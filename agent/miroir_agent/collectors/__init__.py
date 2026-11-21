from typing import Any

from .base import Collector
from .screenmemory import ScreenMemory
from .zen_history import ZenHistory

COLLECTORS: dict[str, Collector[Any, Any]] = {
    collector.name: collector for collector in (ScreenMemory(), ZenHistory())
}
