"""One-shot commands for collection, delivery and inspection."""

import argparse
import json
import re
import time
from collections.abc import Callable
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from . import __version__
from .collectors import COLLECTORS, Collected, Collector, Cursor, zen_history
from .config import CollectorConfig, load_config
from .dates import parse_date
from .deliver import Delivery, deliver
from .media import MediaFailure, deliver_media, token_for
from .state import State, outside_source, read_json, write_json

MediaResult = tuple[str, dict[str, int], MediaFailure | None]


def _cursor(value: Any, collector: str) -> Cursor | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not all(
        type(v) is int
        or (
            collector == "zen_history"
            and k in {"origins_digest", "bookmarks_digest"}
            and isinstance(v, str)
            and (not v or re.fullmatch(r"[0-9a-f]{64}", v))
        )
        for k, v in value.items()
    ):
        raise ValueError("Invalid cursor")
    return value


def _record(
    state: State, outcome: str, counts: dict[str, Any], receipt: Delivery | None = None
) -> None:
    result: dict[str, Any] = {
        "time": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "counts": counts,
    }
    if receipt is not None and receipt.status is not None:
        result.update(http_status=receipt.status, **receipt.receipt)
    try:
        previous = read_json(state.status)
    except (ValueError, RecursionError):
        previous = None
    for key in ("media", "resend"):
        if previous is not None and key in previous:
            result[key] = previous[key]
    write_json(state.status, result)


def _collect(
    config: CollectorConfig,
    cursor: Cursor | None,
    path: Path,
    *,
    window: tuple[int, int] | None = None,
) -> Collected | None:
    return COLLECTORS[config.collector].collect(
        config.options, cursor, path, time.time(), window=window
    )


def _require_outside_source(config: CollectorConfig, *paths: Path) -> None:
    if config.collector == "screenmemory":
        for path in paths:
            outside_source(path, Path(config.options["data_dir"]))
            if _media_collector(config) is not None:
                outside_source(path, Path(config.options["media_dir"]))
    elif config.collector == "zen_history":
        zen_history.require_outside_source(config.options, *paths)


def _run_rows(
    config: CollectorConfig, state: State, maximum: int
) -> tuple[str, dict[str, Any]]:
    counts: dict[str, Any] = {}
    try:
        if not state.pending:
            state.clear_spool()
        for i in range(maximum):
            if state.pending:
                pending = read_json(state.batch_cursor)
                if (
                    pending is None
                    or (cursor := _cursor(pending.get("cursor"), config.collector))
                    is None
                ):
                    raise ValueError("Invalid pending cursor")
                counts = pending["counts"]
                more = pending["more"]
            else:
                state.batch.parent.mkdir(mode=0o700, exist_ok=True)
                batch = _collect(
                    config,
                    _cursor(read_json(state.cursor), config.collector),
                    state.batch,
                )
                if batch is None:
                    outcome = "delivered" if i else "nothing_new"
                    if not i:
                        _record(state, outcome, {})
                    return outcome, counts
                cursor, counts, more = batch
                write_json(
                    state.batch_cursor,
                    {"cursor": cursor, "counts": counts, "more": more},
                )
            receipt = deliver(config, state.batch)
            if not receipt.acknowledged:
                _record(state, "failed", counts, receipt)
                return "failed", counts
            write_json(state.cursor, cursor)
            state.clear_spool()
            _record(state, "delivered", counts, receipt)
            if not more:
                break
    except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
        _record(state, "failed", counts)
        return "failed", counts
    return "delivered", counts


def _media_collector(config: CollectorConfig) -> Collector | None:
    collector = COLLECTORS[config.collector]
    if config.options.get("media") and collector.media and collector.media_kind:
        return collector
    return None


def _media_cursor(config: CollectorConfig, state: State) -> int:
    value = read_json(state.media_cursor)
    if value is None:
        return int(config.options["since"]) - 1 if "since" in config.options else 0
    ts = value.get("ts")
    if type(ts) is not int:
        raise ValueError("Invalid media cursor")
    return ts


def _media_rows(config: CollectorConfig, state: State) -> Cursor | None:
    saved = _cursor(read_json(state.cursor), config.collector)
    if saved is not None and type(saved.get("frames")) is not int:
        raise ValueError("Invalid frame cursor")
    return saved


def _media_counts() -> dict[str, int]:
    return dict.fromkeys(
        (
            "examined",
            "uploaded",
            "present",
            "withheld",
            "missing",
            "invalid",
            "rejected",
        ),
        0,
    )


def _media_failure_fields(error: MediaFailure | None) -> dict[str, str | int | None]:
    if error is None:
        return {}
    return {
        "step": error.step,
        "reason": error.reason,
        "http_status": error.http_status,
    }


def _print_outcome(
    label: str, outcome: str, counts: dict[str, Any], error: MediaFailure | None = None
) -> None:
    details = "".join(f" {k}={v}" for k, v in _media_failure_fields(error).items())
    print(f"{label} {outcome}{details} {json.dumps(counts)}")


def _media_pages(
    config: CollectorConfig,
    collector: Collector,
    cursor: int,
    rows: Cursor | None,
    counts: dict[str, int],
    maximum: int | None = None,
    save: Callable[[int], None] | None = None,
) -> None:
    collect, kind = collector.media, collector.media_kind
    assert collect is not None and kind is not None
    token = token_for(config)
    while rows is not None and (maximum is None or counts["examined"] < maximum):
        limit = 500 if maximum is None else min(500, maximum - counts["examined"])
        try:
            page = collect(config.options, cursor, rows, limit)
        except Exception:  # noqa: BLE001 - collector errors must not expose source data
            raise MediaFailure("collect", "source") from None
        if page is None:
            break
        for name, count in page.counts.items():
            counts[name] += count
        if page.items:
            deliver_media(config, token, kind, page.items, counts)
        cursor = page.cursor
        if save is not None:
            save(cursor)
        if not page.more:
            break


def _run_media(
    config: CollectorConfig, state: State, collector: Collector, maximum: int
) -> MediaResult:
    counts = _media_counts()
    failure = None
    try:
        _media_pages(
            config,
            collector,
            _media_cursor(config, state),
            _media_rows(config, state),
            counts,
            maximum,
            lambda cursor: write_json(state.media_cursor, {"ts": cursor}),
        )
        outcome = "delivered" if counts["examined"] else "nothing_new"
    except MediaFailure as error:
        failure = error
        outcome = "failed"
    except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
        failure = MediaFailure("collect", "state")
        outcome = "failed"
    result = read_json(state.status) or {}
    result["media"] = {
        "time": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "counts": counts,
        **_media_failure_fields(failure),
    }
    write_json(state.status, result)
    return outcome, counts, failure


def _run(
    config: CollectorConfig, state: State, maximum: int, max_media_files: int
) -> tuple[tuple[str, dict[str, Any]], MediaResult | None]:
    _require_outside_source(
        config, state.directory, state.directory / "lock", state.batch.parent
    )
    collector = _media_collector(config)
    with state.lock() as locked:
        if not locked:
            return ("already_running", {}), (
                "already_running",
                {},
                None,
            ) if collector else None
        rows = _run_rows(config, state, maximum)
        return rows, _run_media(
            config, state, collector, max_media_files
        ) if collector else None


def _preview(
    config: CollectorConfig, state: State, out: Path, maximum: int
) -> tuple[str, dict[str, Any]]:
    _require_outside_source(config, out)
    state_dir = state.directory.parent.resolve()
    out = out.resolve()
    if out.is_relative_to(state_dir):
        raise ValueError("Preview output cannot be inside collector state")
    cursor = _cursor(read_json(state.cursor), config.collector)
    out.mkdir(mode=0o700, parents=True, exist_ok=True)
    counts: dict[str, Any] = {}
    for i in range(1, maximum + 1):
        batch = _collect(config, cursor, out / f"batch-{i:04}.jsonl.gz")
        if batch is None:
            return ("collected" if counts else "nothing_new"), counts
        cursor, counts, more = batch
        if not more:
            break
    return "collected", counts


def _positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def _resend_window(start: str, end: str | None) -> tuple[int, int]:
    lower, _ = parse_date(start)
    upper, through_day = (
        (int(time.time()), False) if end is None else parse_date(end, through_day=True)
    )
    if upper < lower or (through_day and upper == lower):
        raise ValueError("Resend end is before its start")
    return lower, upper


def _add_counts(total: dict[str, Any], counts: dict[str, Any]) -> None:
    for key, value in counts.items():
        if isinstance(value, dict):
            _add_counts(total.setdefault(key, {}), value)
        else:
            total[key] = total.get(key, 0) + value


def _resend(
    config: CollectorConfig,
    state: State,
    window: tuple[int, int],
    out: Path | None,
    maximum: int | None,
    media: Collector | None,
) -> tuple[str, dict[str, Any], MediaFailure | None]:
    _require_outside_source(config, state.directory, state.directory / "lock")
    if out is None:
        _require_outside_source(config, state.status)
    with state.lock() as locked:
        if not locked:
            return "already_running", {}, None
        counts: dict[str, Any] = {}
        failure = None
        media_counts = _media_counts()
        batches = 0
        try:
            if media is not None:
                counts["media"] = media_counts
                token_for(config)
            if out is not None:
                out = out.resolve()
                _require_outside_source(config, out)
                if out.is_relative_to(state.directory.parent.resolve()):
                    raise ValueError("Resend output cannot be inside collector state")
                out.mkdir(mode=0o700, parents=True, exist_ok=True)
            directory = (
                TemporaryDirectory(dir=state.directory)
                if out is None
                else nullcontext(str(out))
            )
            with directory as folder:
                cursor = None
                while maximum is None or batches < maximum:
                    path = Path(folder) / f"batch-{batches + 1:04}.jsonl.gz"
                    _require_outside_source(config, path)
                    try:
                        batch = _collect(config, cursor, path, window=window)
                    except Exception:
                        if media is not None:
                            raise MediaFailure("collect", "source") from None
                        raise
                    if batch is None:
                        break
                    if out is None:
                        if not deliver(config, path).acknowledged:
                            raise ValueError("Resend delivery failed")
                        path.unlink()
                    batches += 1
                    cursor, batch_counts, more = batch
                    _add_counts(counts, batch_counts)
                    if not more:
                        break
            if media is not None:
                saved = _media_rows(config, state)
                upper = saved.get("frames") if saved is not None else None
                if type(upper) is int:
                    _media_pages(
                        config,
                        media,
                        window[0] - 1,
                        {"frames": min(window[1] - 1, upper)},
                        media_counts,
                    )
            outcome = (
                "delivered" if batches or media_counts["examined"] else "nothing_new"
            )
            if out is not None and batches:
                outcome = "collected"
        except MediaFailure as error:
            failure = error
            outcome = "failed"
        except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
            outcome = "failed"
            if media is not None:
                failure = MediaFailure("collect", "state")
        if out is not None:
            return outcome, counts, failure
        try:
            result = read_json(state.status) or {}
        except (ValueError, RecursionError):
            result = {}
        result["resend"] = {
            "time": datetime.now(timezone.utc).isoformat(),
            "outcome": outcome,
            "window": list(window),
            "batches": batches,
            "counts": counts,
            **_media_failure_fields(failure),
        }
        write_json(state.status, result)
        return outcome, counts, failure


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="miroir-agent", description="Collect and deliver local data to Miroir."
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="collect and deliver pending batches")
    preview = commands.add_parser("collect", help="write batches without delivery")
    resend = commands.add_parser(
        "resend", help="re-collect a date range without advancing cursors"
    )
    status = commands.add_parser("status", help="show local delivery state")
    for command in (run, preview, resend, status):
        command.add_argument("--config", required=True, type=Path)
    for command in (run, preview):
        command.add_argument("--collector", required=command is preview)
        command.add_argument("--max-batches", type=_positive, default=1)
    preview.add_argument("--out", required=True, type=Path)
    resend.add_argument("--collector", required=True)
    resend.add_argument("--from", dest="start", required=True)
    resend.add_argument("--to", dest="end")
    resend.add_argument("--max-batches", type=_positive)
    resend_mode = resend.add_mutually_exclusive_group()
    resend_mode.add_argument("--media", action="store_true")
    resend_mode.add_argument("--out", type=Path)
    run.add_argument("--max-media-files", type=_positive, default=2000)
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except (OSError, ValueError, RuntimeError):
        print("Invalid agent configuration")
        return 1
    collectors = config.collectors
    if (selected := getattr(args, "collector", None)) is not None:
        collectors = [item for item in collectors if item.id == selected]
        if not collectors:
            print("Collector not configured")
            return 1
    resend_media = None
    if args.command == "resend":
        item = collectors[0]
        if args.media:
            resend_media = _media_collector(item)
            if resend_media is None:
                print("Media is not enabled for this collector")
                return 1
        try:
            window = _resend_window(args.start, args.end)
        except ValueError as error:
            print(error)
            return 1
    failed = False
    for item in collectors:
        state = State(config.state_dir / item.id)
        media = None
        resend_failure = None
        try:
            if args.command == "status":
                result: dict[str, Any] = {
                    "cursor": _cursor(read_json(state.cursor), item.collector),
                    "pending": state.pending,
                    "status": read_json(state.status),
                }
                if _media_collector(item):
                    try:
                        result["media_cursor"] = {"ts": _media_cursor(item, state)}
                    except (OSError, ValueError, RecursionError):
                        result["media_cursor"] = "unreadable"
                        failed = True
                print(f"{item.id} {json.dumps(result)}")
                continue
            if args.command == "run":
                (outcome, counts), media = _run(
                    item, state, args.max_batches, args.max_media_files
                )
            elif args.command == "resend":
                outcome, counts, resend_failure = _resend(
                    item, state, window, args.out, args.max_batches, resend_media
                )
            else:
                outcome, counts = _preview(item, state, args.out, args.max_batches)
        except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
            outcome, counts = "failed", {}
            if args.command == "run" and _media_collector(item):
                media = "failed", _media_counts(), MediaFailure("collect", "state")
            if args.command == "resend" and resend_media is not None:
                resend_failure = MediaFailure("collect", "state")
        _print_outcome(item.id, outcome, counts, resend_failure)
        failed |= outcome == "failed"
        if media is not None:
            _print_outcome(f"{item.id} media", *media)
            media_outcome = media[0]
            failed |= media_outcome == "failed"
    return int(failed)
