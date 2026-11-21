from typing import Any

from .apple_messages import AppleMessages
from .base import Source
from .screenmemory import ScreenMemory
from .zen_history import ZenHistory

SOURCES: dict[str, Source[Any, Any]] = {
    source.name: source for source in (ScreenMemory(), ZenHistory(), AppleMessages())
}
