"""Write one complete batch atomically."""

import gzip
import json
from collections.abc import Mapping
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Self, TextIO

from .errors import Failure
from .files import atomic_path


class BatchWriter:
    def __init__(self, path: Path, format_id: str, version: int, now: float) -> None:
        self.path = path
        self.header = {
            "type": "header",
            "format": format_id,
            "version": version,
            "collected_at": datetime.fromtimestamp(now, UTC)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        self.size = 0
        self.finished = False
        self.stack = ExitStack()
        self.file: TextIO

    def __enter__(self) -> Self:
        with ExitStack() as stack:
            path = stack.enter_context(atomic_path(self.path))
            self.file = stack.enter_context(gzip.open(path, "wt", encoding="utf-8"))
            self.write(self.header)
            self.stack = stack.pop_all()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None and not self.finished:
            failure = Failure("collect", "internal")
            self.stack.__exit__(type(failure), failure, None)
            raise failure
        self.stack.__exit__(exc_type, exc, tb)

    def write(self, record: Mapping[str, Any]) -> None:
        if self.finished:
            raise Failure("collect", "internal")
        try:
            line = (
                json.dumps(dict(record), separators=(",", ":"), allow_nan=False) + "\n"
            )
        except (ValueError, TypeError, RecursionError, OverflowError):
            raise Failure("collect", "source") from None
        self.size += len(line)
        if len(line) > 2 * 1024**2 or self.size > 512 * 1024**2:
            raise Failure("collect", "source")
        self.file.write(line)

    def finish(self, trailer: Mapping[str, Any]) -> None:
        self.write({"type": "trailer", **trailer})
        self.finished = True
