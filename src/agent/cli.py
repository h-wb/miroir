"""One-shot commands for collection, delivery and inspection."""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .collectors import COLLECTORS, Collected
from .config import CollectorConfig, load_config
from .deliver import Delivery, deliver
from .state import State, outside_source, read_json, write_json


def _cursor(value: Any) -> dict[str, int] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not all(type(v) is int for v in value.values()):
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
    write_json(state.status, result)


def _collect(
    config: CollectorConfig, cursor: dict[str, int] | None, path: Path
) -> Collected | None:
    return COLLECTORS[config.collector](config.options, cursor, path, time.time())


def _require_outside_source(config: CollectorConfig, *paths: Path) -> None:
    if config.collector == "screenmemory":
        for path in paths:
            outside_source(path, Path(config.options["data_dir"]))


def _run(
    config: CollectorConfig, state: State, maximum: int
) -> tuple[str, dict[str, Any]]:
    _require_outside_source(
        config, state.directory, state.directory / "lock", state.batch.parent
    )
    with state.lock() as locked:
        if not locked:
            return "already_running", {}
        counts: dict[str, Any] = {}
        try:
            if not state.pending:
                state.clear_spool()
            for i in range(maximum):
                if state.pending:
                    pending = read_json(state.batch_cursor)
                    if (
                        pending is None
                        or (cursor := _cursor(pending.get("cursor"))) is None
                    ):
                        raise ValueError("Invalid pending cursor")
                    counts = pending["counts"]
                    more = pending["more"]
                else:
                    state.batch.parent.mkdir(mode=0o700, exist_ok=True)
                    batch = _collect(
                        config, _cursor(read_json(state.cursor)), state.batch
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


def _preview(
    config: CollectorConfig, state: State, out: Path, maximum: int
) -> tuple[str, dict[str, Any]]:
    _require_outside_source(config, out)
    state_dir = state.directory.parent.resolve()
    out = out.resolve()
    if out.is_relative_to(state_dir):
        raise ValueError("Preview output cannot be inside collector state")
    cursor = _cursor(read_json(state.cursor))
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
        description="Collect and deliver local data to Miroir."
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
        try:
            if args.command == "status":
                result = {
                    "cursor": _cursor(read_json(state.cursor)),
                    "pending": state.pending,
                    "status": read_json(state.status),
                }
                print(f"{item.id} {json.dumps(result)}")
                continue
            if args.command == "run":
                outcome, counts = _run(item, state, args.max_batches)
            else:
                outcome, counts = _preview(item, state, args.out, args.max_batches)
        except (OSError, ValueError, KeyError, RuntimeError, RecursionError):
            outcome, counts = "failed", {}
        print(f"{item.id} {outcome} {json.dumps(counts)}")
        failed |= outcome == "failed"
    return int(failed)
