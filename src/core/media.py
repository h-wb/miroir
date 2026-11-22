"""Plan, confirm, and sign links for connector media.

The bucket is the record of what is stored (decision 033). Confirmed files of a
kind with thumbnails are also queued for the worker (decision 040).
"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, ParamSpec, Protocol, TypeVar
from unicodedata import category

from src.core.config import MediaConfig, ResolvedConnection
from src.core.operations import OperationError
from src.integrations.contracts import MediaKind

MAX_OBJECTS = 500
# Thumbnail sizes, by width (decision 040).
THUMBNAIL_WIDTHS = (360, 1280)
# Pillow decoders by extension; an image is read only by the decoders of the
# extensions its kind makes thumbnails of, whatever its bytes claim to be.
IMAGE_DECODERS = {
    ".jpeg": "JPEG",
    ".jpg": "JPEG",
    ".png": "PNG",
    ".gif": "GIF",
    ".webp": "WEBP",
    ".bmp": "BMP",
    ".tiff": "TIFF",
    ".tif": "TIFF",
}
# Files one backfill run may walk (decision 040).
MAX_BACKFILL = 1_000_000
# Statuses a backfill may derive again: those a decoder fault can leave behind.
RETRY_STATUSES = ("failed", "unreadable")
CONTENT_TYPES = {
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".heic": "image/heic",
    ".heif": "image/heif",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    ".bmp": "image/bmp",
    ".mov": "video/quicktime",
    ".mp4": "video/mp4",
    ".m4v": "video/x-m4v",
    ".3gp": "video/3gpp",
    ".caf": "audio/x-caf",
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".aac": "audio/aac",
    ".amr": "audio/AMR",
    ".wav": "audio/wav",
    ".pdf": "application/pdf",
    ".vcf": "text/vcard",
    ".txt": "text/plain",
    ".rtf": "application/rtf",
    ".csv": "text/csv",
    ".ics": "text/calendar",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".pages": "application/vnd.apple.pages",
    ".numbers": "application/vnd.apple.numbers",
    ".key": "application/vnd.apple.keynote",
    ".zip": "application/zip",
}


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


class ObjectStore(Protocol):
    def head(self, name: str) -> int | None: ...
    def presign_put(self, name: str, content_type: str, expires: int) -> str: ...
    def presign_get(self, name: str, expires: int) -> str: ...

    # For derived files such as thumbnails, made by the worker (decision 040).
    # The web process signs links and never reads or writes objects itself.
    def get(self, name: str, max_bytes: int) -> StoredObject | None:
        """The object, or None if absent; ObjectTooLarge past `max_bytes`."""
        ...

    def put(self, name: str, body: bytes, content_type: str) -> None: ...

    def list_objects(self, prefix: str, after: str, limit: int) -> Listing:
        """Up to `limit` objects under `prefix` named after `after`, in name order."""
        ...


_P = ParamSpec("_P")
_T = TypeVar("_T")


def _storage_call(call: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> _T:
    try:
        return call(*args, **kwargs)
    except Exception as error:
        raise OperationError("Media storage is unavailable") from error


def valid_key(key: object) -> bool:
    return (
        isinstance(key, str)
        and 1 <= len(key) <= 512
        and "\\" not in key
        and not any(category(char) in {"Cc", "Cs"} for char in key)
        and all(part not in {"", ".", ".."} for part in key.split("/"))
    )


def object_name(prefix: str, connection_id: str, kind: str, key: str) -> str:
    return "/".join(part for part in (prefix, connection_id, kind, key) if part)


def kind_prefix(prefix: str, connection_id: str, kind: str) -> str:
    """What every object of one kind is named after, ending in a slash."""
    return object_name(prefix, connection_id, kind, "") + "/"


def thumbnail_name(
    prefix: str, connection_id: str, kind: str, key: str, width: int
) -> str:
    """A thumbnail beside its original, under a derived kind (decision 040)."""
    return object_name(prefix, connection_id, f"{kind}.w{width}", f"{key}.webp")


def image_decoders(extensions: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({IMAGE_DECODERS[e] for e in extensions if e in IMAGE_DECODERS}))


def derived_kind(connection: ResolvedConnection, name: str) -> MediaKind | None:
    """The kind, if the worker makes thumbnails of it."""
    kind = _media_kind(connection, name)
    return kind if kind.thumbnails else None


def backfill_options(
    connection: ResolvedConnection,
    name: str,
    limit: int,
    retry: Iterable[str] = (),
) -> dict[str, Any]:
    """The options of a backfill run: which kind to walk, how far, and which
    recorded statuses to derive again (after a decoder incident)."""
    if derived_kind(connection, name) is None:
        raise OperationError("The worker makes no thumbnails of this media kind")
    if type(limit) is not int or not 1 <= limit <= MAX_BACKFILL:
        raise OperationError(f"The limit must be between 1 and {MAX_BACKFILL}")
    statuses = sorted(set(retry))
    if not set(statuses) <= set(RETRY_STATUSES):
        raise OperationError(
            f"Only these statuses can be retried: {', '.join(RETRY_STATUSES)}"
        )
    options: dict[str, Any] = {"kind": name, "limit": limit}
    if statuses:
        options["retry"] = statuses
    return options


def _media_kind(connection: ResolvedConnection, name: str) -> MediaKind:
    for kind in connection.integration.media:
        if kind.name == name:
            return kind
    raise OperationError("Unknown media kind")


def _configured(
    config: MediaConfig | None, store: ObjectStore | None
) -> tuple[MediaConfig, ObjectStore]:
    if config is None or store is None:
        raise OperationError("Media is not configured")
    return config, store


def _objects(objects: object) -> list[dict[str, Any]]:
    if not isinstance(objects, list):
        raise OperationError("Media objects must be a list")
    if len(objects) > MAX_OBJECTS:
        raise OperationError("At most 500 media objects are allowed")
    if any(
        not isinstance(item, dict) or item.keys() != {"key", "size"} for item in objects
    ):
        raise OperationError("Each media object must contain a key and size")
    return objects


def request_uploads(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    objects: object,
) -> list[dict[str, Any]]:
    config, store = _configured(config, store)
    kind = _media_kind(connection, name)
    results = []
    for item in _objects(objects):
        key, size = item["key"], item["size"]
        if not valid_key(key):
            results.append(
                {
                    "key": key if isinstance(key, str) else None,
                    "status": "rejected",
                    "reason": "bad_key",
                }
            )
            continue
        extension = PurePosixPath(key).suffix.lower()
        if extension not in kind.extensions:
            results.append({"key": key, "status": "rejected", "reason": "bad_type"})
        elif type(size) is not int or not 1 <= size <= kind.max_bytes:
            results.append({"key": key, "status": "rejected", "reason": "bad_size"})
        else:
            target = object_name(config.prefix, connection.config.id, kind.name, key)
            if _storage_call(store.head, target) == size:
                results.append({"key": key, "status": "present"})
            else:
                content_type = CONTENT_TYPES.get(extension, "application/octet-stream")
                results.append(
                    {
                        "key": key,
                        "status": "upload",
                        "url": _storage_call(
                            store.presign_put,
                            target,
                            content_type,
                            config.upload_ttl_seconds,
                        ),
                        "content_type": content_type,
                    }
                )
    return results


def confirm_uploads(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    objects: object,
) -> list[dict[str, Any]]:
    config, store = _configured(config, store)
    kind = _media_kind(connection, name)
    results = []
    for item in _objects(objects):
        key, size = item["key"], item["size"]
        if not valid_key(key):
            results.append(
                {"key": key if isinstance(key, str) else None, "status": "rejected"}
            )
            continue
        target = object_name(config.prefix, connection.config.id, kind.name, key)
        stored = _storage_call(store.head, target)
        if stored is None:
            status = "missing"
        elif type(size) is int and stored == size:
            status = "stored"
        else:
            status = "size_mismatch"
        results.append({"key": key, "status": status})
    return results


def view_url(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    key: str,
) -> str:
    config, store = _configured(config, store)
    kind = _media_kind(connection, name)
    if not valid_key(key):
        raise OperationError("Invalid media key")
    target = object_name(config.prefix, connection.config.id, kind.name, key)
    return _storage_call(store.presign_get, target, config.view_ttl_seconds)
