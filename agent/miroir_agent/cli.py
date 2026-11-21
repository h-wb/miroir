"""One-shot commands for collection, delivery and inspection."""

import argparse
import json
import time
from pathlib import Path

from . import __version__, runner
from .collectors.base import Window
from .config import CollectorConfig, load_config
from .dates import parse_date
from .errors import ConfigError, Failure
from .runner import Outcome, Result
from .state import State, StateError


def _positive(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def _resend_window(start: str, end: str | None) -> Window:
    lower, _ = parse_date(start)
    upper, through_day = (
        (int(time.time()), False) if end is None else parse_date(end, through_day=True)
    )
    if upper < lower or (through_day and upper == lower):
        raise ValueError("Resend end is before its start")
    return lower, upper


def _print_result(label: str, result: Result) -> bool:
    details = ""
    if result.failure is not None:
        details = "".join(
            f" {key}={value}"
            for key, value in result.failure.fields().items()
            if value is not None
        )
    print(f"{label} {result.outcome}{details} {json.dumps(result.counts)}")
    return result.outcome == Outcome.FAILED


def _run(args: argparse.Namespace, config: CollectorConfig, state: State) -> bool:
    rows, media = runner.run(
        config,
        state,
        max_batches=args.max_batches,
        max_media_files=args.max_media_files,
    )
    failed = _print_result(config.id, rows)
    if media is not None:
        failed |= _print_result(f"{config.id} media", media)
    return failed


def _preview(args: argparse.Namespace, config: CollectorConfig, state: State) -> bool:
    return _print_result(
        config.id, runner.preview(config, state, args.out, max_batches=args.max_batches)
    )


def _resend(args: argparse.Namespace, config: CollectorConfig, state: State) -> bool:
    if args.media and config.collector.media(config.options) is None:
        print("Media is not enabled for this collector")
        return True
    try:
        window = _resend_window(args.start, args.end)
    except ValueError as error:
        print(error)
        return True
    return _print_result(
        config.id,
        runner.resend(
            config,
            state,
            window,
            out=args.out,
            max_batches=args.max_batches,
            media=args.media,
        ),
    )


def _status(args: argparse.Namespace, config: CollectorConfig, state: State) -> bool:
    try:
        result = runner.status(config, state)
    except StateError:
        return _print_result(
            config.id, Result(Outcome.FAILED, {}, Failure("collect", "state"))
        )
    print(f"{config.id} {json.dumps(result)}")
    return result.get("media_cursor") == "unreadable"


def _parser() -> argparse.ArgumentParser:
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
    run.set_defaults(handler=_run)
    preview.set_defaults(handler=_preview)
    resend.set_defaults(handler=_resend)
    status.set_defaults(handler=_status)
    return parser


def _dispatch(
    args: argparse.Namespace, config: CollectorConfig, state_dir: Path
) -> bool:
    try:
        handler = args.handler
        return bool(handler(args, config, State(state_dir / config.id)))
    except Exception:  # noqa: BLE001 - never expose a traceback or exception values
        return _print_result(
            config.id, Result(Outcome.FAILED, {}, Failure("collect", "internal"))
        )


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_config(args.config)
    except ConfigError as error:
        print(f"Invalid agent configuration: {error}")
        return 1
    collectors = config.collectors
    if (selected := getattr(args, "collector", None)) is not None:
        collectors = [item for item in collectors if item.id == selected]
        if not collectors:
            print("Collector not configured")
            return 1
    failed = False
    for item in collectors:
        failed |= _dispatch(args, item, config.state_dir)
    return int(failed)
