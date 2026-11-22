"""Thumbnails and the stored-media table, made by the worker (decision 040).

A derive run drains the queue of confirmed files (`media_pending`): for each, it
makes thumbnails where the kind has them and writes one `media_objects` row to
the connection's dataset, also for a file that gets none, so it is not tried
again. A backfill first walks the bucket from where its last walk stopped and
queues what it finds. Only the runner imports this module, inside the run.
"""

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, TypeVar, cast

from src.core.config import MediaConfig, ResolvedConnection, ResolvedMiroir
from src.core.media import (
    CONTENT_TYPES,
    ObjectStore,
    ObjectTooLarge,
    StoredObject,
    kind_prefix,
    object_name,
    thumbnail_name,
    valid_key,
)
from src.core.operations import MetadataStore, OperationError, PendingMedia
from src.core.thumbnails import WIDTHS, Refused, decoders, make_thumbnails
from src.integrations.contracts import MediaKind

if TYPE_CHECKING:
    from dlt.common.schema.typing import TTableSchemaColumns

TABLE = "media_objects"
# Raised when the way thumbnails are made changes: rows of an older version are
# derived again when their file is queued again (a backfill walks it again).
DERIVE_VERSION = 1
# A file whose decoding began this often without an outcome (the process ended
# while decoding it) is recorded as failed instead of tried again.
MAX_ATTEMPTS = 3
LIST_PAGE = 1000

COLUMNS: dict[str, dict[str, Any]] = {
    "connection_id": {"data_type": "text", "nullable": False},
    "kind": {"data_type": "text", "nullable": False},
    "key": {"data_type": "text", "nullable": False},
    "size": {"data_type": "bigint", "nullable": False},
    "content_type": {"data_type": "text", "nullable": False},
    "stored_at": {"data_type": "timestamp", "nullable": False},
    **{
        f"thumbnail_w{width}": {"data_type": "bool", "nullable": False}
        for width in WIDTHS
    },
    "thumbnail_status": {"data_type": "text", "nullable": False},
    "derived_at": {"data_type": "timestamp", "nullable": False},
    "derive_version": {"data_type": "bigint", "nullable": False},
}

_T = TypeVar("_T")


@dataclass
class DeriveSummary:
    # made, unsupported, too_large, unreadable, failed; gone (no longer in the
    # bucket, so no row) and known (already derived at this size and version).
    counts: Counter[str] = field(default_factory=Counter)
    remaining: int = 0
    walked: int = 0
    walk_finished: bool | None = None

    @property
    def progressed(self) -> bool:
        return sum(self.counts.values()) > 0

    def then(self, later: "DeriveSummary") -> "DeriveSummary":
        return DeriveSummary(
            self.counts + later.counts,
            later.remaining,
            self.walked,
            self.walk_finished,
        )

    def detail(self) -> str:
        counts = ", ".join(f"{name} {n}" for name, n in sorted(self.counts.items()))
        text = f"Media: {counts or 'nothing queued'}; {self.remaining} waiting"
        if self.walk_finished is not None:
            state = "walk finished" if self.walk_finished else "walk continues"
            text += f"; backfill walked {self.walked}, {state}"
        return text


def derive_media(
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    *,
    store: MetadataStore,
    object_store: ObjectStore,
    destination: Any,
    pipelines_dir: str | None,
    lock_wait: float,
    backfill: dict[str, Any] | None,
) -> DeriveSummary:
    """One batch per kind with thumbnails. A storage failure ends the run after
    recording what was finished; the rest stays queued."""
    config = miroir.media
    assert config is not None
    summary = DeriveSummary()
    writer = _Writer(connection, destination, pipelines_dir, store, lock_wait)
    for kind in connection.integration.media:
        if not kind.thumbnails:
            continue
        if backfill is not None and backfill["kind"] == kind.name:
            summary.walked, summary.walk_finished = _walk(
                store, object_store, config, connection, kind, backfill["limit"]
            )
        batch = store.pending_media(
            miroir.id, connection.config.id, kind.name, config.derive_batch
        )
        if batch:
            _derive_batch(
                store, object_store, config, connection, kind, batch, writer, summary
            )
    summary.remaining = store.count_pending_media(miroir.id, connection.config.id)
    return summary


def _walk(
    store: MetadataStore,
    object_store: ObjectStore,
    config: MediaConfig,
    connection: ResolvedConnection,
    kind: MediaKind,
    limit: int,
) -> tuple[int, bool]:
    """Queue up to `limit` stored files, continuing the previous walk.

    The bucket is the record (decision 033), so files copied in by other means
    are found too. A walk that reaches the end starts again next time; files
    already derived are then recognised without being read.
    """
    miroir_id, connection_id = connection.miroir_id, connection.config.id
    prefix = kind_prefix(config.prefix, connection_id, kind.name)
    after = store.backfill_after(miroir_id, connection_id, kind.name)
    walked = 0
    while walked < limit:
        asked = min(LIST_PAGE, limit - walked)
        page = _storage(
            object_store.list_objects, prefix, prefix + after if after else "", asked
        )
        found = []
        for listed in page:
            key = listed.name.removeprefix(prefix)
            if listed.name.startswith(prefix) and valid_key(key):
                found.append((key, listed.size, _utc(listed.modified)))
        store.add_pending_media(miroir_id, connection_id, kind.name, found)
        walked += len(page)
        if len(page) < asked:
            store.set_backfill_after(miroir_id, connection_id, kind.name, "")
            return walked, True
        after = page[-1].name.removeprefix(prefix)
        store.set_backfill_after(miroir_id, connection_id, kind.name, after)
    return walked, False


def _derive_batch(
    store: MetadataStore,
    object_store: ObjectStore,
    config: MediaConfig,
    connection: ResolvedConnection,
    kind: MediaKind,
    batch: list[PendingMedia],
    writer: "_Writer",
    summary: DeriveSummary,
) -> None:
    known = writer.known(kind.name, [entry.key for entry in batch])
    rows: list[dict[str, Any]] = []
    done: list[PendingMedia] = []
    failure: OperationError | None = None
    for entry in batch:
        if known.get(entry.key) == entry.size:
            summary.counts["known"] += 1
            done.append(entry)
            continue
        try:
            row = _derive_one(store, object_store, config, connection, kind, entry)
        except OperationError as error:
            failure = error
            break
        if row is None:
            summary.counts["gone"] += 1
        else:
            summary.counts[row["thumbnail_status"]] += 1
            rows.append(row)
        done.append(entry)
    writer.write(rows)
    store.remove_pending_media(
        connection.miroir_id, connection.config.id, kind.name, done
    )
    if failure is not None:
        raise failure


def _derive_one(
    store: MetadataStore,
    object_store: ObjectStore,
    config: MediaConfig,
    connection: ResolvedConnection,
    kind: MediaKind,
    entry: PendingMedia,
) -> dict[str, Any] | None:
    """The file's row, or None when it is no longer stored."""
    extension = PurePosixPath(entry.key).suffix.lower()
    formats = decoders(kind.thumbnails)
    size = entry.size
    thumbnails: dict[int, bytes] = {}
    if extension not in kind.thumbnails or not decoders([extension]):
        status = "unsupported"
    elif entry.attempts >= MAX_ATTEMPTS:
        status = "failed"
    else:
        name = object_name(config.prefix, connection.config.id, kind.name, entry.key)
        try:
            stored = _get(object_store, name, kind.max_bytes)
        except ObjectTooLarge:
            status = "too_large"
        else:
            if stored is None:
                return None
            size = len(stored.body)
            store.begin_media_attempt(
                connection.miroir_id, connection.config.id, kind.name, entry.key
            )
            try:
                thumbnails = make_thumbnails(stored.body, formats)
                status = "made"
            except Refused as refused:
                status = refused.status
        for width, body in thumbnails.items():
            target = thumbnail_name(
                config.prefix, connection.config.id, kind.name, entry.key, width
            )
            _storage(object_store.put, target, body, "image/webp")
    return {
        "connection_id": connection.config.id,
        "kind": kind.name,
        "key": entry.key,
        "size": size,
        "content_type": CONTENT_TYPES.get(extension, "application/octet-stream"),
        "stored_at": entry.stored_at,
        **{f"thumbnail_w{width}": width in thumbnails for width in WIDTHS},
        "thumbnail_status": status,
        "derived_at": datetime.now(timezone.utc),
        "derive_version": DERIVE_VERSION,
    }


def _get(object_store: ObjectStore, name: str, max_bytes: int) -> StoredObject | None:
    try:
        return object_store.get(name, max_bytes)
    except ObjectTooLarge:
        raise
    except Exception as error:
        raise OperationError("Media storage is unavailable") from error


def _storage(call: Callable[..., _T], *args: Any) -> _T:
    try:
        return call(*args)
    except Exception as error:
        raise OperationError("Media storage is unavailable") from error


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class _Writer:
    """`media_objects` in the connection's dataset, through a dlt pipeline of its
    own, so the connection's load state is never touched."""

    def __init__(
        self,
        connection: ResolvedConnection,
        destination: Any,
        pipelines_dir: str | None,
        store: MetadataStore,
        lock_wait: float,
    ):
        import dlt

        self.connection = connection
        self.store = store
        self.lock_wait = lock_wait
        options: dict[str, Any] = (
            {"pipelines_dir": pipelines_dir} if pipelines_dir else {}
        )
        self.pipeline = dlt.pipeline(
            pipeline_name=f"{connection.binding.pipeline_name}_media",
            dataset_name=connection.binding.dataset_name,
            destination=destination,
            **options,
        )

    def known(self, kind: str, keys: list[str]) -> dict[str, int]:
        """Size of each key already derived with this version."""
        from dlt.destinations.exceptions import DatabaseUndefinedRelation

        if not keys:
            return {}
        try:
            with self.pipeline.sql_client() as client:
                table = client.make_qualified_table_name(TABLE)
                key, size = (client.escape_column_name(c) for c in ("key", "size"))
                marks = ", ".join(["%s"] * len(keys))
                rows = client.execute_sql(
                    f"SELECT {key}, {size} FROM {table}"
                    " WHERE connection_id = %s AND kind = %s AND derive_version = %s"
                    f" AND {key} IN ({marks})",
                    self.connection.config.id,
                    kind,
                    DERIVE_VERSION,
                    *keys,
                )
        except DatabaseUndefinedRelation:
            return {}  # nothing was derived into this dataset yet
        return {row[0]: int(row[1]) for row in rows or []}

    def write(self, rows: list[dict[str, Any]]) -> None:
        import dlt

        if not rows:
            return
        resource = dlt.resource(
            rows,
            name=TABLE,
            write_disposition="merge",
            primary_key=("connection_id", "kind", "key"),
            columns=cast("TTableSchemaColumns", COLUMNS),
        )
        # The connection's own loads write the same dataset.
        with self.store.connection_lock(
            self.connection.binding.pipeline_name, self.lock_wait
        ):
            self.pipeline.run(resource)
