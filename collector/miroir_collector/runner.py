"""Run the passes of one configured source."""

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from .config import SourceConfig
from .errors import Failure, Reason, Step
from .masking import Masker
from .media import upload
from .sources.base import Collected, Cursor, Media, Window
from .state import Pending, Section, State, StateError, StatusEntry
from .transport import Client, Receipt


class Outcome(StrEnum):
    DELIVERED = "delivered"
    NOTHING_NEW = "nothing_new"
    COLLECTED = "collected"
    FAILED = "failed"
    ALREADY_RUNNING = "already_running"


@dataclass(frozen=True)
class Result:
    outcome: Outcome
    counts: dict[str, Any]
    failure: Failure | None = None


@dataclass
class _Pass:
    step: Step = "collect"
    outcome: Outcome = Outcome.NOTHING_NEW
    counts: dict[str, Any] = field(default_factory=dict)
    failure: Failure | None = None
    batches: int = 0

    def result(self) -> Result:
        return Result(self.outcome, self.counts, self.failure)

    def set_step(self, step: Step) -> None:
        self.step = step

    @contextmanager
    def capture(self) -> Iterator[None]:
        try:
            yield
        except Exception as error:  # noqa: BLE001 - last resort for each pass
            self.outcome = Outcome.FAILED
            if isinstance(error, Failure):
                self.failure = error
            else:
                reason: Reason = (
                    "state" if isinstance(error, (StateError, OSError)) else "internal"
                )
                self.failure = Failure(self.step, reason)


def _client(config: SourceConfig) -> Client:
    return Client(config.receive_url, config.token_env, config.timeout_seconds)


def _guard(config: SourceConfig, *paths: Path) -> None:
    config.source.guard(config.options, *paths)


def _parse_cursor(config: SourceConfig, raw: dict[str, Any]) -> Cursor:
    try:
        return config.source.cursor_model.parse(raw)
    except ValueError:
        raise StateError("Invalid saved cursor") from None


def _cursor(config: SourceConfig, state: State) -> Cursor | None:
    raw = state.read_cursor()
    return _parse_cursor(config, raw) if raw is not None else None


def _collect(
    config: SourceConfig,
    cursor: Cursor | None,
    path: Path,
    masker: Masker,
    window: Window | None = None,
) -> Collected | None:
    return config.source.collect(
        config.options, cursor, path, time.time(), masker=masker, window=window
    )


def _record(state: State, section: Section, progress: _Pass, **fields: Any) -> None:
    if progress.failure is not None:
        fields.update(progress.failure.fields())
    state.record(
        section,
        StatusEntry(
            time=datetime.now(UTC).isoformat(),
            outcome=progress.outcome.value,
            counts=progress.counts,
            **fields,
        ),
    )


def _next_pending(config: SourceConfig, state: State, masker: Masker) -> Pending | None:
    pending = state.pending()
    if pending is not None:
        return pending
    state.prepare_spool()
    batch = _collect(config, _cursor(config, state), state.batch, masker)
    if batch is None:
        return None
    pending = Pending(
        cursor=batch.cursor.model_dump(), counts=batch.counts, more=batch.more
    )
    state.save_pending(pending)
    return pending


def _deliver_pending(
    config: SourceConfig,
    state: State,
    client: Client,
    pending: Pending,
    progress: _Pass,
) -> Receipt:
    cursor = _parse_cursor(config, pending.cursor)
    progress.step = "deliver"
    receipt = client.post_batch(state.batch, config.source.format_id)
    state.save_cursor(cursor.model_dump(exclude_unset=True))
    state.clear_spool()
    return receipt


def _rows(
    config: SourceConfig, state: State, client: Client, masker: Masker, maximum: int
) -> Result:
    progress = _Pass()
    with progress.capture():
        if not state.has_pending():
            state.clear_spool()
        for _ in range(maximum):
            progress.step = "collect"
            pending = _next_pending(config, state, masker)
            if pending is None:
                if not progress.batches:
                    _record(state, "rows", progress)
                break
            progress.counts = pending.counts
            receipt = _deliver_pending(config, state, client, pending, progress)
            progress.batches += 1
            progress.outcome = Outcome.DELIVERED
            _record(
                state, "rows", progress, http_status=receipt.status, **receipt.fields()
            )
            if not pending.more:
                break
    if progress.failure is not None:
        with progress.capture():
            _record(state, "rows", progress)
    return progress.result()


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


def _media_position(state: State, source: Media) -> int:
    saved = state.media_position()
    return source.start() if saved is None else saved


@dataclass(frozen=True)
class _MediaRange:
    after: int
    upto: int | None
    maximum: int | None = None
    save: Callable[[int], None] | None = None


def _media_pages(
    client: Client,
    source: Media,
    span: _MediaRange,
    counts: dict[str, int],
    progress: _Pass,
) -> None:
    progress.step = "links"
    client.require_token("links")
    position = span.after
    while span.upto is not None and (
        span.maximum is None or counts["examined"] < span.maximum
    ):
        progress.step = "collect"
        limit = (
            500 if span.maximum is None else min(500, span.maximum - counts["examined"])
        )
        page = source.page(position, span.upto, limit)
        if page is None:
            break
        for name, count in page.counts.items():
            counts[name] += count
        if page.items:
            upload(client, source.kind, page.items, counts, on_step=progress.set_step)
        position = page.position
        progress.step = "collect"
        if span.save is not None:
            span.save(position)
        if not page.more:
            break


def _run_media(
    config: SourceConfig,
    state: State,
    client: Client,
    source: Media,
    maximum: int,
) -> Result:
    counts = _media_counts()
    progress = _Pass(counts=counts)
    with progress.capture():
        span = _MediaRange(
            _media_position(state, source),
            source.limit(_cursor(config, state)),
            maximum,
            state.save_media_position,
        )
        _media_pages(client, source, span, counts, progress)
        if counts["examined"]:
            progress.outcome = Outcome.DELIVERED
    with progress.capture():
        _record(state, "media", progress)
    return progress.result()


def run(
    config: SourceConfig,
    state: State,
    *,
    masker: Masker,
    max_batches: int,
    max_media_files: int,
) -> tuple[Result, Result | None]:
    source = config.source.media(config.options)
    progress = _Pass()
    with progress.capture():
        _guard(config, *state.paths)
        with state.lock() as locked:
            if not locked:
                result = Result(Outcome.ALREADY_RUNNING, {})
                return result, result if source is not None else None
            with _client(config) as client:
                rows = _rows(config, state, client, masker, max_batches)
                media = (
                    _run_media(config, state, client, source, max_media_files)
                    if source is not None
                    else None
                )
                return rows, media
    media = (
        Result(Outcome.FAILED, _media_counts(), progress.failure)
        if source is not None
        else None
    )
    return progress.result(), media


def _output(config: SourceConfig, state: State, out: Path) -> Path:
    out = out.resolve()
    _guard(config, out)
    if out.is_relative_to(state.directory.parent.resolve()):
        raise StateError("Output cannot be inside collector state")
    return out


def preview(
    config: SourceConfig,
    state: State,
    out: Path,
    *,
    masker: Masker,
    max_batches: int,
) -> Result:
    progress = _Pass()
    with progress.capture():
        out = _output(config, state, out)
        cursor = _cursor(config, state)
        out.mkdir(mode=0o700, parents=True, exist_ok=True)
        for i in range(1, max_batches + 1):
            path = out / f"batch-{i:04}.jsonl.gz"
            _guard(config, path)
            batch = _collect(config, cursor, path, masker)
            if batch is None:
                break
            cursor = batch.cursor
            progress.counts = batch.counts
            progress.outcome = Outcome.COLLECTED
            if not batch.more:
                break
    return progress.result()


def _add_counts(total: dict[str, Any], counts: dict[str, Any]) -> None:
    for key, value in counts.items():
        if isinstance(value, dict):
            _add_counts(total.setdefault(key, {}), value)
        else:
            total[key] = total.get(key, 0) + value


@dataclass(frozen=True)
class Resend:
    window: Window
    out: Path | None
    maximum: int | None
    media: bool


def _resend_rows(
    config: SourceConfig,
    state: State,
    client: Client,
    masker: Masker,
    selection: Resend,
    progress: _Pass,
) -> None:
    out = selection.out
    if out is not None:
        out = _output(config, state, out)
        out.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = (
        TemporaryDirectory(dir=state.directory)
        if out is None
        else nullcontext(str(out))
    )
    cursor = None
    with directory as folder:
        while selection.maximum is None or progress.batches < selection.maximum:
            progress.step = "collect"
            path = Path(folder) / f"batch-{progress.batches + 1:04}.jsonl.gz"
            _guard(config, path)
            batch = _collect(config, cursor, path, masker, selection.window)
            if batch is None:
                break
            if out is None:
                progress.step = "deliver"
                client.post_batch(path, config.source.format_id)
                path.unlink()
            progress.batches += 1
            cursor = batch.cursor
            _add_counts(progress.counts, batch.counts)
            if not batch.more:
                break


def _resend_pass(
    config: SourceConfig,
    state: State,
    client: Client,
    masker: Masker,
    selection: Resend,
    progress: _Pass,
) -> None:
    source = config.source.media(config.options) if selection.media else None
    media_counts = _media_counts()
    if source is not None:
        progress.counts["media"] = media_counts
        progress.step = "links"
        client.require_token("links")
    progress.step = "collect"
    _resend_rows(config, state, client, masker, selection, progress)
    progress.step = "collect"
    if source is not None:
        upper = source.limit(_cursor(config, state))
        if upper is not None:
            start, end = selection.window
            span = _MediaRange(start - 1, min(end - 1, upper))
            _media_pages(client, source, span, media_counts, progress)
    if progress.batches or media_counts["examined"]:
        progress.outcome = (
            Outcome.COLLECTED if selection.out is not None else Outcome.DELIVERED
        )


def resend(
    config: SourceConfig,
    state: State,
    selection: Resend,
    *,
    masker: Masker,
) -> Result:
    progress = _Pass()
    with progress.capture():
        _guard(config, *state.paths)
        with state.lock() as locked:
            if not locked:
                return Result(Outcome.ALREADY_RUNNING, {})
            with _client(config) as client, progress.capture():
                _resend_pass(
                    config,
                    state,
                    client,
                    masker,
                    selection,
                    progress,
                )
            if selection.out is None:
                _record(
                    state,
                    "resend",
                    progress,
                    window=list(selection.window),
                    batches=progress.batches,
                )
    return progress.result()


def status(config: SourceConfig, state: State) -> dict[str, Any]:
    saved = _cursor(config, state)
    result: dict[str, Any] = {
        "cursor": saved.model_dump(exclude_unset=True) if saved is not None else None,
        "pending": state.has_pending(),
        "status": state.status(),
    }
    source = config.source.media(config.options)
    if source is not None:
        try:
            result["media_cursor"] = {"ts": _media_position(state, source)}
        except StateError:
            result["media_cursor"] = "unreadable"
    return result
