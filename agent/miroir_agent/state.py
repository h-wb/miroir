"""Atomic file state and a per-collector lock."""

import fcntl
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


@contextmanager
def atomic_path(path: Path) -> Iterator[Path]:
    with NamedTemporaryFile(dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
    try:
        yield temporary
        # The caller has closed the writer, flushing its buffers and any gzip footer.
        with temporary.open("r+b") as file:
            os.fsync(file.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, value: dict[str, Any]) -> None:
    with atomic_path(path) as temporary:
        temporary.write_text(json.dumps(value) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if type(data) is not dict:
        raise ValueError("Invalid agent state")
    return data


def outside_source(path: Path, source: Path) -> None:
    if path.resolve().is_relative_to(source.resolve()):
        raise ValueError("Agent output cannot be inside the source folder")


@dataclass(frozen=True)
class State:
    directory: Path

    @property
    def cursor(self) -> Path:
        return self.directory / "cursor.json"

    @property
    def status(self) -> Path:
        return self.directory / "status.json"

    @property
    def media_cursor(self) -> Path:
        return self.directory / "media_cursor.json"

    @property
    def batch(self) -> Path:
        return self.directory / "spool" / "batch.jsonl.gz"

    @property
    def batch_cursor(self) -> Path:
        return self.batch.with_name("batch.cursor.json")

    @property
    def pending(self) -> bool:
        return self.batch.is_file() and self.batch_cursor.is_file()

    def clear_spool(self) -> None:
        self.batch.unlink(missing_ok=True)
        self.batch_cursor.unlink(missing_ok=True)

    @contextmanager
    def lock(self) -> Iterator[bool]:
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (self.directory / "lock").open("a") as file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
            else:
                yield True
