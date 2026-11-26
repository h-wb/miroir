"""The view side of media: short view links, and the bytes of thumbnails and
small images copied through the server to a browser (decision 040).

Called after the caller's login and withheld checks; nothing here decodes.
"""

from src.core.config import ResolvedConnection
from src.kernel.errors import OperationError
from src.media.config import MediaConfig
from src.media.kinds import MediaKind, kind_named
from src.media.names import (
    extension,
    image_type,
    object_name,
    thumbnail_name,
    valid_key,
)
from src.media.store import (
    ObjectStore,
    ObjectTooLarge,
    StoredObject,
    configured,
    storage_call,
)

# The largest original image the server copies through itself; larger ones, and
# files that are not images, keep the redirect to a signed link.
MAX_STREAMED_IMAGE = 25 * 1024 * 1024


class NotStreamed(OperationError):
    """Not copied through the server (not an image, or too large): use a link."""


def view_url(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    key: str,
) -> str:
    config, store = configured(config, store)
    kind = kind_named(connection.integration.media, name)
    if not valid_key(key):
        raise OperationError("Invalid media key")
    target = object_name(config.prefix, connection.config.id, kind.name, key)
    return storage_call(store.presign_get, target, config.view_ttl_seconds)


def _viewed(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    key: str,
) -> tuple[MediaConfig, ObjectStore, MediaKind]:
    """The checks of the upload path: a kind of this connection (never a derived
    one, which no integration names), a valid key, one of the kind's extensions."""
    config, store = configured(config, store)
    kind = kind_named(connection.integration.media, name)
    if not valid_key(key) or extension(key) not in kind.extensions:
        raise OperationError("Invalid media key")
    return config, store, kind


def read_thumbnail(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    key: str,
    width: int,
) -> StoredObject | None:
    """A thumbnail's bytes, or None if the worker made none."""
    config, store, kind = _viewed(config, store, connection, name, key)
    profile = kind.derive_profile
    if profile is None or width not in profile.widths:
        return None
    target = thumbnail_name(
        config.prefix, connection.config.id, kind.name, key, width, profile.output
    )
    try:
        return storage_call(store.get, target, profile.max_output_bytes)
    except ObjectTooLarge:
        return None  # not one the worker made


def read_image(
    config: MediaConfig | None,
    store: ObjectStore | None,
    connection: ResolvedConnection,
    name: str,
    key: str,
) -> StoredObject | None:
    """An original image's bytes, or None if absent. NotStreamed for a file that
    is not an image or is too large to copy through the server."""
    config, store, kind = _viewed(config, store, connection, name, key)
    if image_type(key) is None:
        raise NotStreamed("Not an image")
    target = object_name(config.prefix, connection.config.id, kind.name, key)
    try:
        return storage_call(store.get, target, min(kind.max_bytes, MAX_STREAMED_IMAGE))
    except ObjectTooLarge:
        raise NotStreamed("Too large to stream") from None
