"""Thumbnails and the stored-media table, made by the worker (decision 040).

A derive run drains the queue of confirmed files (`media_pending`): for each, it
makes thumbnails where the kind has them and writes one `media_objects` row to
the connection's dataset, also for a file that gets none, so it is not tried
again. A backfill first walks the bucket from where its last walk stopped and
queues what it finds. Only the runner imports this module, inside the run.

Images are decoded in a child process (`Decoder`), never here.
"""

import functools
import json
import os
import select
import struct
import subprocess
import sys
import time
import zlib
from collections import Counter
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import IO, TYPE_CHECKING, Any, TypeVar, cast

from src.core.config import MediaConfig, ResolvedConnection, ResolvedMiroir
from src.core.decoder_checks import check_image
from src.core.media import (
    CONTENT_TYPES,
    IMAGE_DECODERS,
    MAX_THUMBNAIL_BYTES,
    THUMBNAIL_WIDTHS,
    ObjectStore,
    ObjectTooLarge,
    StoredObject,
    image_decoders,
    kind_prefix,
    object_name,
    thumbnail_name,
    valid_key,
)
from src.core.operations import MetadataStore, OperationError, PendingMedia
from src.integrations.contracts import MediaKind

if TYPE_CHECKING:
    from dlt.common.schema.typing import TTableSchemaColumns

TABLE = "media_objects"
# Raised when the way thumbnails are made changes: rows of an older version are
# derived again when their file is queued again (a backfill walks it again).
DERIVE_VERSION = 1
# A file whose decoding began this often without an outcome (the decoder or the
# run ended while it was decoding, or it took too long) is recorded as failed.
MAX_ATTEMPTS = 3
LIST_PAGE = 1000
# Rows are written, and their files leave the queue, this many at a time: a run
# that dies loses at most this much finished work, which the next run redoes.
WRITE_EVERY = 100
# One file's decode, measured at about 1.2 s for the slowest accepted image.
DECODE_TIMEOUT = 30
# A fresh decoder's check (two images, about 1.5 s together).
PROBE_TIMEOUT = 60
DECODE_CPU_SECONDS = 25
# The decoder's address space. The worst accepted image measured 480 MB above
# the child's own 32 MB (a 40-megapixel PNG with grey and alpha).
DECODE_MEMORY = 1024 * 1024 * 1024
WIDTHS = THUMBNAIL_WIDTHS
STATUSES = ("made", "unsupported", "too_large", "unreadable")
REPO = Path(__file__).resolve().parents[2]

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


class DecoderLost(Exception):
    """The decoder ended or took too long on a file, and a fresh one passed its
    check: the file is to blame, and is tried again later."""


class DecoderUnavailable(OperationError):
    """A fresh decoder could not make thumbnails of the built-in images: the fault
    is the decoder's or the machine's, not a file's."""

    def __init__(self) -> None:
        super().__init__(
            "The image decoder could not start or failed its check;"
            " no file was charged and none was recorded"
        )


class _Lost(Exception):
    pass


class Decoder:
    """Thumbnails made by `python -m src.core.thumbnails` in a child process.

    One child serves a run, one file at a time. It has an address-space limit, a
    CPU limit per file, a wall-clock timeout per file enforced here (reading and
    writing), and no credentials in its environment; on Linux the worker also
    makes itself undumpable first, so the child cannot read the worker's memory
    or environment through /proc.

    Every fresh child must first make thumbnails of two built-in images, the
    smallest and the costliest the bounds accept. One that cannot is
    DecoderUnavailable: nothing about a file follows from it. When a child is
    lost on a file and a fresh one passes, the file is DecoderLost.
    """

    def __init__(
        self,
        timeout: float = DECODE_TIMEOUT,
        memory: int = DECODE_MEMORY,
        cpu_seconds: int = DECODE_CPU_SECONDS,
        decoders: tuple[str, ...] = tuple(sorted(set(IMAGE_DECODERS.values()))),
    ):
        self.timeout = timeout
        self.memory = memory
        self.cpu_seconds = cpu_seconds
        # The decoders a fresh child must show it has, one tiny image each.
        self.decoders = decoders
        self._child: subprocess.Popen[bytes] | None = None
        # The child's peak resident memory after the last file, in kilobytes.
        self.peak_kb = 0

    def close(self) -> None:
        child, self._child = self._child, None
        if child is None:
            return
        for stream in (child.stdin, child.stdout):
            if stream is not None:
                stream.close()
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()

    def _start(self) -> subprocess.Popen[bytes]:
        if self._child is not None and self._child.poll() is None:
            return self._child
        self.close()
        try:
            _undumpable()
            child = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "src.core.thumbnails",
                    str(self.memory),
                    str(self.cpu_seconds),
                ],
                cwd=REPO,
                # Nothing of the worker's environment: no bucket key, no passwords.
                env={"PATH": os.environ.get("PATH", "")},
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            assert child.stdin is not None
            os.set_blocking(child.stdin.fileno(), False)
        except OSError:
            # No child at all (no process slot, no interpreter, prctl refused).
            raise DecoderUnavailable() from None
        self._child = child
        try:
            for body, formats in _checks(self.decoders):
                if self._exchange(body, formats, PROBE_TIMEOUT)[0] != "made":
                    raise _Lost()
        except _Lost:
            self._kill()
            raise DecoderUnavailable() from None
        return child

    def _kill(self) -> None:
        if self._child is not None:
            self._child.kill()
        self.close()

    def thumbnails(
        self, body: bytes, formats: tuple[str, ...]
    ) -> tuple[str, dict[int, bytes]]:
        """(status, thumbnails by width), DecoderLost or DecoderUnavailable."""
        self._start()
        try:
            return self._exchange(body, formats, self.timeout)
        except _Lost:
            self._kill()
        # Which was it, the file or the decoder? A fresh child answers.
        self._start()
        raise DecoderLost()

    def _exchange(
        self, body: bytes, formats: tuple[str, ...], timeout: float
    ) -> tuple[str, dict[int, bytes]]:
        child = self._child
        assert child is not None and child.stdin is not None
        assert child.stdout is not None
        request = json.dumps(
            {"formats": formats, "widths": WIDTHS, "size": len(body)}
        ).encode()
        deadline = time.monotonic() + timeout
        try:
            _write(
                child.stdin, struct.pack(">I", len(request)) + request + body, deadline
            )
            (length,) = struct.unpack(">I", _read(child.stdout, 4, deadline))
            if length > 4096:
                raise ValueError("answer header too long")
            answer = json.loads(_read(child.stdout, length, deadline))
            status = answer["status"]
            sizes = [(int(width), int(size)) for width, size in answer["sizes"]]
            if status not in STATUSES or (status == "made") != bool(sizes):
                raise ValueError("unexpected answer")
            if not {width for width, _ in sizes} <= set(WIDTHS) or any(
                not 0 < size <= MAX_THUMBNAIL_BYTES for _, size in sizes
            ):
                raise ValueError("unexpected thumbnails")
            made = {width: _read(child.stdout, size, deadline) for width, size in sizes}
            self.peak_kb = int(answer.get("peak_kb", 0))
        except (OSError, ValueError, KeyError, TypeError, struct.error) as error:
            raise _Lost() from error
        return status, made


def _undumpable() -> None:
    """Linux: other processes of this user, the decoder among them, can no
    longer read this process's memory or environment through /proc."""
    if not sys.platform.startswith("linux"):
        return
    import ctypes

    PR_SET_DUMPABLE = 4
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_DUMPABLE) refused")


@functools.cache
def _checks(decoders: tuple[str, ...]) -> tuple[tuple[bytes, tuple[str, ...]], ...]:
    """A tiny image for each of `decoders`, read by that decoder alone, then the
    costliest image the bounds accept: 40 million grey and alpha pixels (measured
    480 MB to decode), all zero, so it is small. A decoder without its image
    fails the check: one cannot be added and forgotten."""
    tiny = []
    for decoder in decoders:
        image = check_image(decoder)
        if image is None:
            raise _Lost()
        tiny.append((image, (decoder,)))
    return (*tiny, (_png(6324, 6324), ("PNG",)))


def _png(width: int, height: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(kind + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", crc)

    # Grey with alpha, 8 bits: two bytes a pixel, a filter byte a row.
    row = b"\x00" * (1 + 2 * width)
    packer = zlib.compressobj(9)
    data = b"".join(packer.compress(row) for _ in range(height)) + packer.flush()
    header = struct.pack(">IIBBBBB", width, height, 8, 4, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", header)
        + chunk(b"IDAT", data)
        + chunk(b"IEND", b"")
    )


def _write(stream: IO[bytes], data: bytes, deadline: float) -> None:
    """All of `data` to the child's input before the deadline, or _Lost: a child
    that stops reading cannot hold the run."""
    fd, view = stream.fileno(), memoryview(data)
    while view:
        left = deadline - time.monotonic()
        if left <= 0 or not select.select([], [fd], [], left)[1]:
            raise _Lost()
        try:
            view = view[os.write(fd, view[: 1 << 20]) :]
        except BlockingIOError:
            continue


def _read(stream: IO[bytes], size: int, deadline: float) -> bytes:
    """Exactly `size` bytes, or _Lost at the deadline or at the end of input."""
    chunks, wanted = [], size
    fd = stream.fileno()
    while wanted:
        left = deadline - time.monotonic()
        if left <= 0 or not select.select([fd], [], [], left)[0]:
            raise _Lost()
        chunk = os.read(fd, min(wanted, 1 << 20))
        if not chunk:
            raise _Lost()
        chunks.append(chunk)
        wanted -= len(chunk)
    return b"".join(chunks)


@dataclass
class DeriveSummary:
    # made, unsupported, too_large, unreadable, failed; gone (no longer in the
    # bucket, so no row), known (already derived at this size and version) and
    # interrupted (the decoder was lost on it; it stays queued, one try nearer
    # to failed).
    counts: Counter[str] = field(default_factory=Counter)
    remaining: int = 0
    walked: int = 0
    walk_finished: bool | None = None
    # Files queued again by a backfill's --retry, whatever their rows say.
    retried: int = 0

    @property
    def progressed(self) -> bool:
        return sum(self.counts.values()) > 0

    def then(self, later: "DeriveSummary") -> "DeriveSummary":
        return DeriveSummary(
            self.counts + later.counts,
            later.remaining,
            self.walked,
            self.walk_finished,
            self.retried,
        )

    def detail(self) -> str:
        counts = ", ".join(f"{name} {n}" for name, n in sorted(self.counts.items()))
        text = f"Media: {counts or 'nothing queued'}; {self.remaining} waiting"
        if self.walk_finished is not None:
            state = "walk finished" if self.walk_finished else "walk continues"
            text += f"; backfill walked {self.walked}, {state}"
        if self.retried:
            text += f"; {self.retried} queued again"
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
    kinds = [kind for kind in connection.integration.media if kind.thumbnails]
    used = {d for kind in kinds for d in image_decoders(kind.thumbnails)}
    with closing(Decoder(decoders=tuple(sorted(used)))) as decoder:
        for kind in kinds:
            if backfill is not None and backfill["kind"] == kind.name:
                if backfill.get("retry"):
                    # One write carrying the intent: rows stay until replaced.
                    again = writer.with_status(kind.name, backfill["retry"])
                    store.queue_retry(miroir.id, connection.config.id, kind.name, again)
                    summary.retried = len(again)
                summary.walked, summary.walk_finished = _walk(
                    store, object_store, config, connection, kind, backfill["limit"]
                )
            batch = store.pending_media(
                miroir.id, connection.config.id, kind.name, config.derive_batch
            )
            if batch:
                _Batch(
                    store, object_store, config, connection, kind, writer, decoder
                ).run(batch, summary)
    summary.remaining = store.count_pending_media(
        miroir.id, connection.config.id, [kind.name for kind in kinds]
    )
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
        listing = _storage(
            object_store.list_objects, prefix, prefix + after if after else "", asked
        )
        found = []
        for listed in listing.objects:
            key = listed.name.removeprefix(prefix)
            if listed.name.startswith(prefix) and valid_key(key):
                found.append((key, listed.size, _utc(listed.modified)))
        store.add_pending_media(miroir_id, connection_id, kind.name, found)
        walked += len(listing.objects)
        # The store says whether more follow; a short page does not mean the end.
        if not listing.truncated or not listing.objects:
            store.set_backfill_after(miroir_id, connection_id, kind.name, "")
            return walked, True
        # The last name listed, valid or not, so an invalid key cannot stall it.
        after = listing.objects[-1].name.removeprefix(prefix)
        store.set_backfill_after(miroir_id, connection_id, kind.name, after)
    return walked, False


_INTERRUPTED = object()


@dataclass
class _Batch:
    store: MetadataStore
    object_store: ObjectStore
    config: MediaConfig
    connection: ResolvedConnection
    kind: MediaKind
    writer: "_Writer"
    decoder: Decoder

    def run(self, batch: list[PendingMedia], summary: DeriveSummary) -> None:
        """Derive each file; write rows and drop their queue entries in chunks.

        A file's attempt count is raised only while it is decoding, so whatever
        ends the run later never counts against a file already handled.
        """
        known = self.writer.known(self.kind.name, [entry.key for entry in batch])
        rows: list[dict[str, Any]] = []
        done: list[PendingMedia] = []
        try:
            for entry in batch:
                if known.get(entry.key) == entry.size and not entry.retry:
                    summary.counts["known"] += 1
                    done.append(entry)
                else:
                    row = self.derive(entry)
                    if row is _INTERRUPTED:
                        summary.counts["interrupted"] += 1
                        continue
                    if row is None:
                        summary.counts["gone"] += 1
                    else:
                        assert isinstance(row, dict)
                        summary.counts[row["thumbnail_status"]] += 1
                        rows.append(row)
                    done.append(entry)
                if len(done) >= WRITE_EVERY:
                    self.flush(rows, done)
        except Exception:
            # A storage failure ends the run after recording what was finished.
            self.flush(rows, done)
            raise
        self.flush(rows, done)

    def flush(self, rows: list[dict[str, Any]], done: list[PendingMedia]) -> None:
        self.writer.write(rows)
        self.store.remove_pending_media(
            self.connection.miroir_id, self.connection.config.id, self.kind.name, done
        )
        rows.clear()
        done.clear()

    def derive(self, entry: PendingMedia) -> dict[str, Any] | None | object:
        """The file's row; None when it is no longer stored; _INTERRUPTED when
        the decoder was lost on it."""
        config, connection, kind = self.config, self.connection, self.kind
        scope = (connection.miroir_id, connection.config.id, kind.name, entry.key)
        extension = PurePosixPath(entry.key).suffix.lower()
        size = entry.size
        thumbnails: dict[int, bytes] = {}
        if extension not in kind.thumbnails or not image_decoders([extension]):
            status = "unsupported"
        elif entry.attempts >= MAX_ATTEMPTS:
            status = "failed"
        else:
            name = object_name(
                config.prefix, connection.config.id, kind.name, entry.key
            )
            try:
                stored = _get(self.object_store, name, kind.max_bytes)
            except ObjectTooLarge:
                status = "too_large"
            else:
                if stored is None:
                    return None
                size = len(stored.body)
                self.store.begin_media_attempt(*scope)
                try:
                    status, thumbnails = self.decoder.thumbnails(
                        stored.body, image_decoders(kind.thumbnails)
                    )
                except DecoderLost:
                    return _INTERRUPTED
                except DecoderUnavailable:
                    # Not this file's fault: its try is given back, and the run stops.
                    self.store.restore_media_attempt(*scope, entry.attempts)
                    raise
                self.store.end_media_attempt(*scope)
            for width, body in thumbnails.items():
                target = thumbnail_name(
                    config.prefix, connection.config.id, kind.name, entry.key, width
                )
                _storage(self.object_store.put, target, body, "image/webp")
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

    def with_status(
        self, kind: str, statuses: list[str]
    ) -> list[tuple[str, int, datetime]]:
        """(key, size, stored at) of the files of `kind` recorded with a status."""
        marks = ", ".join(["%s"] * len(statuses))
        rows = self._query(
            "SELECT {key}, {size}, stored_at FROM {table}"
            f" WHERE connection_id = %s AND kind = %s AND thumbnail_status IN ({marks})",
            self.connection.config.id,
            kind,
            *statuses,
        )
        return [(row[0], int(row[1]), _utc(row[2])) for row in rows]

    def _query(self, sql: str, *args: Any) -> list[Any]:
        from dlt.destinations.exceptions import DatabaseUndefinedRelation

        try:
            with self.pipeline.sql_client() as client:
                key, size = (client.escape_column_name(c) for c in ("key", "size"))
                table = client.make_qualified_table_name(TABLE)
                rows = client.execute_sql(
                    sql.format(table=table, key=key, size=size), *args
                )
        except DatabaseUndefinedRelation:
            return []  # nothing was derived into this dataset yet
        return list(rows or [])

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
