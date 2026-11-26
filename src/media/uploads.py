"""Upload links and confirmations for a connection's media (decision 033).

The bucket is the record of what is stored: a file is present when the bucket
has it at its size. The control plane calls these after the delivery-token
check, then queues confirmed files of a derived kind (decision 040).
"""

from typing import Any

from src.kernel.errors import OperationError
from src.media.config import MediaConfig
from src.media.kinds import kind_named
from src.media.names import content_type, extension, object_name, valid_key
from src.media.store import ObjectStore, configured, storage_call
from src.media.target import MediaConnection

MAX_OBJECTS = 500


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
    connection: MediaConnection,
    name: str,
    objects: object,
) -> list[dict[str, Any]]:
    config, store = configured(config, store)
    kind = kind_named(connection.kinds, name)
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
        if extension(key) not in kind.extensions:
            results.append({"key": key, "status": "rejected", "reason": "bad_type"})
        elif type(size) is not int or not 1 <= size <= kind.max_bytes:
            results.append({"key": key, "status": "rejected", "reason": "bad_size"})
        else:
            target = object_name(config.prefix, connection.id, kind.name, key)
            if storage_call(store.head, target) == size:
                results.append({"key": key, "status": "present"})
            else:
                type_ = content_type(key)
                results.append(
                    {
                        "key": key,
                        "status": "upload",
                        "url": storage_call(
                            store.presign_put,
                            target,
                            type_,
                            config.upload_ttl_seconds,
                        ),
                        "content_type": type_,
                    }
                )
    return results


def confirm_uploads(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: MediaConnection,
    name: str,
    objects: object,
) -> list[dict[str, Any]]:
    config, store = configured(config, store)
    kind = kind_named(connection.kinds, name)
    results = []
    for item in _objects(objects):
        key, size = item["key"], item["size"]
        if not valid_key(key):
            results.append(
                {"key": key if isinstance(key, str) else None, "status": "rejected"}
            )
            continue
        target = object_name(config.prefix, connection.id, kind.name, key)
        stored = storage_call(store.head, target)
        if stored is None:
            status = "missing"
        elif type(size) is int and stored == size:
            status = "stored"
        else:
            status = "size_mismatch"
        results.append({"key": key, "status": status})
    return results
