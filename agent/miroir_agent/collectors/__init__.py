from collections.abc import Callable
from typing import Any, NamedTuple

Cursor = dict[str, int | str]


class Collected(NamedTuple):
    cursor: Cursor
    counts: dict[str, Any]
    more: bool


class Collector(NamedTuple):
    collect: Callable[..., Collected | None]
    format_id: str


from . import screenmemory, zen_history

COLLECTORS = {
    "screenmemory": Collector(screenmemory.collect, "screenmemory_batch"),
    "zen_history": Collector(zen_history.collect, "firefox_places_batch"),
}
