from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

Cursor = dict[str, int | str]


class Collected(NamedTuple):
    cursor: Cursor
    counts: dict[str, Any]
    more: bool


class MediaItem(NamedTuple):
    key: str
    path: Path
    size: int


class MediaPage(NamedTuple):
    items: list[MediaItem]
    cursor: int
    counts: dict[str, int]
    more: bool


class Collector(NamedTuple):
    collect: Callable[..., Collected | None]
    format_id: str
    media: Callable[..., MediaPage | None] | None = None
    media_kind: str | None = None


from . import screenmemory, zen_history

COLLECTORS = {
    "screenmemory": Collector(
        screenmemory.collect,
        "screenmemory_batch",
        screenmemory.collect_media,
        "screenshots",
    ),
    "zen_history": Collector(zen_history.collect, "firefox_places_batch"),
}
