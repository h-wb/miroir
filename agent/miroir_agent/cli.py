"""One-shot commands for collection, delivery and inspection."""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .collectors import COLLECTORS, Collected, Collector, Cursor, zen_history
from .config import CollectorConfig, load_config
from .deliver import Delivery, deliver
from .media import deliver_media, token_for
from .state import State, outside_source, read_json, write_json


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
    if previous is not None and "media" in previous:
        result["media"] = previous["media"]
    write_json(state.status, result)


def _collect(
    config: CollectorConfig, cursor: Cursor | None, path: Path
) -> Collected | None:
    return COLLECTORS[config.collector].collect(
        config.options, cursor, path, time.time()
    )


def _require_outside_source(config: CollectorConfig, *paths: Path) -> None:
    if config.collector == "screenmemory":
        for path in paths:
            outside_source(path, Path(config.options["data_dir"]))
            if config.options.get("media"):
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
        return int(config.options.get("cutoff_ts", 0))
    ts = value.get("ts")
    if type(ts) is not int:
        raise ValueError("Invalid media cursor")
    return ts


def _run_media(
    config: CollectorConfig, state: State, collector: Collector, maximum: int
) -> tuple[str, dict[str, int]]:
    counts = dict.fromkeys(
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
    outcome = "nothing_new"
    collect, kind = collector.media, collector.media_kind
    assert collect is not None and kind is not None
    try:
        token = token_for(config)
        cursor = _media_cursor(config, state)
        rows = _cursor(read_json(state.cursor), config.collector)
        while rows is not None and counts["examined"] < maximum:
            limit = min(500, maximum - counts["examined"])
            page = collect(config.options, cursor, rows, limit)
            if page is None:
                break
            for name, count in page.counts.items():
                counts[name] += count
            if page.items:
                deliver_media(config, token, kind, page.items, counts)
            cursor = page.cursor
            write_json(state.media_cursor, {"ts": cursor})
            outcome = "delivered"
            if not page.more:
                break
    except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
        outcome = "failed"
    result = read_json(state.status) or {}
    result["media"] = {
        "time": datetime.now(timezone.utc).isoformat(),
        "outcome": outcome,
        "counts": counts,
    }
    write_json(state.status, result)
    return outcome, counts


def _run(
    config: CollectorConfig, state: State, maximum: int, max_media_files: int
) -> tuple[tuple[str, dict[str, Any]], tuple[str, dict[str, int]] | None]:
    _require_outside_source(
        config, state.directory, state.directory / "lock", state.batch.parent
    )
    collector = _media_collector(config)
    with state.lock() as locked:
        if not locked:
            return ("already_running", {}), (
                "already_running",
                {},
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
    status = commands.add_parser("status", help="show local delivery state")
    for command in (run, preview, status):
        command.add_argument("--config", required=True, type=Path)
    for command in (run, preview):
        command.add_argument("--collector", required=command is preview)
        command.add_argument("--max-batches", type=_positive, default=1)
    preview.add_argument("--out", required=True, type=Path)
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
    failed = False
    for item in collectors:
        state = State(config.state_dir / item.id)
        media = None
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
            else:
                outcome, counts = _preview(item, state, args.out, args.max_batches)
        except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
            outcome, counts = "failed", {}
            if args.command == "run" and _media_collector(item):
                media = "failed", {}
        print(f"{item.id} {outcome} {json.dumps(counts)}")
        failed |= outcome == "failed"
        if media is not None:
            media_outcome, media_counts = media
            print(f"{item.id} media {media_outcome} {json.dumps(media_counts)}")
            failed |= media_outcome == "failed"
    return int(failed)
