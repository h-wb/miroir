from collections.abc import Callable
from typing import Any, NamedTuple


class Collected(NamedTuple):
    cursor: dict[str, int]
    counts: dict[str, Any]
    more: bool


from . import screenmemory

COLLECTORS: dict[str, Callable[..., Collected | None]] = {
    "screenmemory": screenmemory.collect,
}
