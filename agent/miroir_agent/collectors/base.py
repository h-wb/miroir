"""What a collector is: the contract every source implements.

A collector reads one local source and writes what is new as one batch file.
Everything else in the agent asks a collector through this contract and names
none.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, ClassVar

from pydantic import BeforeValidator, ValidationError

from ..dates import parse_date
from ..errors import Failure
from ..models import Model

# A resend range in Unix seconds: from inclusive, to exclusive.
Window = tuple[int, int]


def nonempty(value: object) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("Expected a nonempty string")
    return value


def _user_path(value: object) -> Path:
    try:
        return Path(nonempty(value)).expanduser()
    except (ValueError, RuntimeError):
        raise ValueError("Expected a valid path") from None


def _nonblank(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected a nonblank string")
    return value


UserPath = Annotated[Path, BeforeValidator(_user_path)]
Since = Annotated[int, BeforeValidator(lambda value: parse_date(nonempty(value))[0])]
Nonblank = Annotated[str, BeforeValidator(_nonblank)]
Nonempty = Annotated[str, BeforeValidator(nonempty)]


class Options(Model):
    """A collector's options. Their defaults live in the model and nowhere else."""


class Cursor(Model):
    """How far a collector's rows have been acknowledged."""

    @classmethod
    def parse(cls, value: object) -> Cursor:
        try:
            return cls.model_validate(value)
        except ValidationError:
            raise ValueError("Invalid saved cursor") from None


@dataclass(frozen=True)
class Collected:
    cursor: Cursor
    counts: dict[str, Any]
    more: bool


@dataclass(frozen=True)
class MediaItem:
    key: str
    path: Path
    size: int


@dataclass(frozen=True)
class MediaPage:
    items: list[MediaItem]
    position: int
    counts: dict[str, int]
    more: bool


class MediaSource(ABC):
    """The files a collector's rows point at, walked by an integer position."""

    kind: ClassVar[str]

    @abstractmethod
    def start(self) -> int:
        """The position to begin after when none is saved."""

    @abstractmethod
    def limit(self, cursor: Cursor | None) -> int | None:
        """The last position whose row was delivered; None when no row was."""

    @abstractmethod
    def page(self, after: int, upto: int, limit: int) -> MediaPage | None:
        """Files for positions in (after, upto], or None when there are none."""


class Collector[O: Options, C: Cursor](ABC):
    name: ClassVar[str]
    format_id: ClassVar[str]
    options_model: ClassVar[type[Options]]
    cursor_model: ClassVar[type[Cursor]]

    @abstractmethod
    def source_dirs(self, options: O) -> list[Path]:
        """Folders the source application owns. The agent never writes in them."""

    @abstractmethod
    def collect(
        self,
        options: O,
        cursor: C | None,
        out: Path,
        now: float,
        *,
        window: Window | None = None,
    ) -> Collected | None:
        """Write rows newer than `cursor` to `out`; None when there are none.

        With a window, collect what is dated inside it instead.
        """

    def media(self, options: O) -> MediaSource | None:
        """The collector's files, when it has any and they are enabled."""
        return None

    def guard(self, options: O, *paths: Path) -> None:
        """Refuse any path that lies inside a source folder."""
        self.guard_paths(self.source_dirs(options), *paths)

    @staticmethod
    def guard_paths(sources: list[Path], *paths: Path) -> None:
        try:
            dirs = [source.resolve() for source in sources]
            if any(
                path.resolve().is_relative_to(source)
                for path in paths
                for source in dirs
            ):
                raise Failure("collect", "state")
        except (OSError, RuntimeError, ValueError):
            raise Failure("collect", "state") from None
