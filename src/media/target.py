"""What media reads of a connection, built by the control plane from its own
configuration (package split, step 5): media never imports that configuration.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass

from src.media.config import MediaConfig
from src.media.kinds import MediaKind

# Holds the connection's own lock, the one its loads take, while it is entered:
# `media_objects` is written to the dataset those loads write.
ConnectionLock = Callable[[], AbstractContextManager[None]]


@dataclass(frozen=True)
class MediaConnection:
    """What upload and view links read of a connection."""

    id: str
    kinds: tuple[MediaKind, ...]


@dataclass(frozen=True)
class MediaTarget:
    """What a derive run reads of a connection and of the media section."""

    miroir_id: str
    connection_id: str
    # The connection's dlt pipeline and dataset: `media_objects` is written to
    # the dataset by a pipeline of its own, `<pipeline_name>_media`.
    pipeline_name: str
    dataset_name: str
    kinds: tuple[MediaKind, ...]
    config: MediaConfig
