"""The object store media is kept in (decision 033): a protocol, and the
errors of a call to it. `src.media.s3` implements it; nothing else talks to the
bucket."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import ParamSpec, Protocol, TypeVar

from src.kernel.errors import OperationError
from src.media.config import MediaConfig


@dataclass(frozen=True)
class StoredObject:
    body: bytes
    content_type: str | None
    etag: str | None


@dataclass(frozen=True)
class ListedObject:
    name: str
    size: int
    modified: datetime


@dataclass(frozen=True)
class Listing:
    objects: list[ListedObject]
    # The store's own word that more objects follow, not a short page.
    truncated: bool


class ObjectTooLarge(OperationError):
    """The object is larger than the caller is prepared to hold in memory."""


class StorageUnavailable(OperationError):
    """The bucket could not be asked."""


class ObjectStore(Protocol):
    def head(self, name: str) -> int | None: ...
    def presign_put(self, name: str, content_type: str, expires: int) -> str: ...
    def presign_get(self, name: str, expires: int) -> str: ...

    # The worker reads originals and writes thumbnails (decision 040). The web
    # process reads thumbnails and small images to copy them to a browser, and
    # never writes.
    def get(self, name: str, max_bytes: int) -> StoredObject | None:
        """The object, or None if absent; ObjectTooLarge past `max_bytes`."""
        ...

    def put(self, name: str, body: bytes, content_type: str) -> None: ...

    def list_objects(self, prefix: str, after: str, limit: int) -> Listing:
        """Up to `limit` objects under `prefix` named after `after`, in name order."""
        ...


_P = ParamSpec("_P")
_T = TypeVar("_T")


def storage_call(call: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> _T:
    try:
        return call(*args, **kwargs)
    except ObjectTooLarge:
        raise
    except Exception as error:
        raise StorageUnavailable("Media storage is unavailable") from error


def configured(
    config: MediaConfig | None, store: ObjectStore | None
) -> tuple[MediaConfig, ObjectStore]:
    if config is None or store is None:
        raise OperationError("Media is not configured")
    return config, store
