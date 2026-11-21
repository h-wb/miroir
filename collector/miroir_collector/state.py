"""The durable state and lock of one source."""

import fcntl
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError

from .errors import Reason, Step
from .files import atomic_path
from .models import Model

Section = Literal["rows", "media", "resend"]


class StateError(Exception):
    """A state file could not be read or had an invalid shape."""


class Pending(Model):
    cursor: dict[str, Any]
    counts: dict[str, Any]
    more: bool


class _Position(Model):
    ts: int


class StatusEntry(Model):
    time: str | None = None
    outcome: (
        Literal["delivered", "nothing_new", "collected", "failed", "already_running"]
        | None
    ) = None
    counts: dict[str, Any] | None = None
    http_status: int | None = None
    artifact_id: str | None = None
    new: bool | None = None
    step: Step | None = None
    reason: Reason | None = None
    window: list[int] | None = None
    batches: int | None = None


def _validate[M: Model](model: type[M], raw: object) -> M:
    try:
        return model.model_validate(raw)
    except ValidationError:
        raise StateError("Invalid collector state") from None


def _read(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError, RecursionError):
        raise StateError("Unreadable collector state") from None
    if not isinstance(data, dict):
        raise StateError("Invalid collector state")
    return data


def _write(path: Path, value: dict[str, Any]) -> None:
    with atomic_path(path) as temporary:
        temporary.write_text(json.dumps(value) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class State:
    directory: Path

    @property
    def _cursor(self) -> Path:
        return self.directory / "cursor.json"

    @property
    def _status(self) -> Path:
        return self.directory / "status.json"

    @property
    def _media(self) -> Path:
        return self.directory / "media_cursor.json"

    @property
    def _report(self) -> Path:
        return self.directory / "report.json"

    @property
    def batch(self) -> Path:
        return self.directory / "spool" / "batch.jsonl.gz"

    @property
    def _batch_cursor(self) -> Path:
        return self.batch.with_name("batch.cursor.json")

    @property
    def _lock(self) -> Path:
        return self.directory / "lock"

    @property
    def paths(self) -> tuple[Path, ...]:
        """Every path this state writes, for the source-folder guard."""
        return (
            self.directory,
            self._lock,
            self.batch.parent,
            self.batch,
            self._batch_cursor,
            self._cursor,
            self._media,
            self._status,
            self._report,
        )

    def read_cursor(self) -> dict[str, Any] | None:
        return _read(self._cursor)

    def save_cursor(self, cursor: dict[str, Any]) -> None:
        _write(self._cursor, cursor)

    def has_pending(self) -> bool:
        return self.batch.is_file() and self._batch_cursor.is_file()

    def pending(self) -> Pending | None:
        if not self.has_pending():
            return None
        return _validate(Pending, _read(self._batch_cursor))

    def prepare_spool(self) -> None:
        self.batch.parent.mkdir(mode=0o700, exist_ok=True)

    def save_pending(self, pending: Pending) -> None:
        _write(self._batch_cursor, pending.model_dump())

    def clear_spool(self) -> None:
        self.batch.unlink(missing_ok=True)
        self._batch_cursor.unlink(missing_ok=True)

    def media_position(self) -> int | None:
        raw = _read(self._media)
        return None if raw is None else _validate(_Position, raw).ts

    def save_media_position(self, position: int) -> None:
        _write(self._media, {"ts": position})

    def status(self) -> dict[str, Any] | None:
        return _read(self._status)

    def unreported_runs(self) -> int:
        value = (_read(self._report) or {}).get("unreported", 0)
        if type(value) is not int or value < 0:
            raise StateError("Invalid report counter")
        return value

    def save_unreported_runs(self, count: int) -> None:
        _write(self._report, {"unreported": count})

    def record_report(self, status: str) -> None:
        try:
            value = self.status() or {}
        except StateError:
            value = {}
        _write(self._status, {**value, "report": status})

    def record(self, section: Section, entry: StatusEntry) -> None:
        try:
            previous = self.status() or {}
        except StateError:
            previous = {}
        value = entry.model_dump(exclude_unset=True)
        if section == "rows":
            value.update(
                {key: previous[key] for key in ("media", "resend") if key in previous}
            )
        else:
            previous[section] = value
            value = previous
        _write(self._status, value)

    @contextmanager
    def lock(self) -> Iterator[bool]:
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self._lock.open("a") as file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
            else:
                yield True
