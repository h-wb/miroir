"""Plan, confirm, and sign links for connector media without storing metadata."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, ParamSpec, Protocol, TypeVar
from unicodedata import category

from src.core.config import MediaConfig, ResolvedConnection
from src.core.operations import OperationError
from src.integrations.contracts import MediaKind

MAX_OBJECTS = 500
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
