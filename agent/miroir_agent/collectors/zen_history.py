"""Collect incremental Firefox/Zen history from a private Places copy."""

import hashlib
import json
import shutil
import sqlite3
from collections import Counter
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Annotated, Any

from pydantic import BeforeValidator, Field

from ..batch import BatchWriter
from ..errors import Failure
from ..masking import Masker
from .base import (
    Collected,
    Collector,
    Cursor,
    Options,
    Since,
    UserPath,
    Window,
    nonempty,
)

DEFAULT_PROFILES_DIR = "~/Library/Application Support/zen/Profiles"
_REQUIRED = {
    "moz_places": {"id", "url"},
    "moz_historyvisits": {"id", "place_id", "visit_date"},
}
Row = dict[str, Any]


class ZenHistoryOptions(Options):
    places_path: Annotated[UserPath | None, BeforeValidator(nonempty)] = None
    profiles_dir: UserPath = Field(default=DEFAULT_PROFILES_DIR, validate_default=True)
    max_rows: int = Field(default=20000, ge=1, le=200000)
    mask_urls: bool = True
    since: Annotated[Since | None, BeforeValidator(nonempty)] = None


Digest = Annotated[str, Field(pattern=r"^(?:[0-9a-f]{64})?$")]


class ZenHistoryCursor(Cursor):
    visit_id: int = 0
    visit_date: int = 0
    place_id: int = 0
    metadata_updated_at: int = 0
    origins_digest: Digest = ""
    bookmarks_digest: Digest = ""


class ZenHistory(Collector[ZenHistoryOptions, ZenHistoryCursor]):
    name = "zen_history"
    format_id = "firefox_places_batch"
    options_model = ZenHistoryOptions
    cursor_model = ZenHistoryCursor

    def source_dirs(self, options: ZenHistoryOptions) -> list[Path]:
        return _source_dirs(_source(options))

    def collect(
        self,
        options: ZenHistoryOptions,
        cursor: ZenHistoryCursor | None,
        out: Path,
        now: float,
        *,
        masker: Masker,
        window: Window | None = None,
    ) -> Collected | None:
        return _collect(options, cursor, out, now, masker, window)


def _source_dirs(source: Path) -> list[Path]:
    try:
        return [source.parent, source.resolve().parent]
    except (OSError, ValueError, RuntimeError):
        raise Failure("collect", "source") from None


def _source(options: ZenHistoryOptions) -> Path:
    try:
        source = options.places_path or max(
            options.profiles_dir.glob("*/places.sqlite"),
            key=lambda p: p.stat().st_mtime_ns,
        )
        if not source.is_file():
            raise Failure("collect", "source")
        return source
    except (OSError, ValueError, RuntimeError):
        raise Failure("collect", "source") from None


def _decode(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace")


def _stamp(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_size, stat.st_mtime_ns


@contextmanager
def _stable_copy(source: Path) -> Iterator[Path]:
    temp_root = Path(gettempdir())
    wal = source.with_name(source.name + "-wal")
    for attempt in range(3):
        before = _stamp(source), _stamp(wal)
        if before[0] is None:
            if attempt == 0:
                raise Failure("collect", "source")
            continue
        with TemporaryDirectory(prefix="miroir-zen-", dir=temp_root) as temporary:
            target = Path(temporary) / "places.sqlite"
            try:
                shutil.copyfile(source, target)
                if before[1] is not None:
                    shutil.copyfile(wal, target.with_suffix(".sqlite-wal"))
            except FileNotFoundError:
                if before == (_stamp(source), _stamp(wal)):
                    raise
                continue
            if before == (_stamp(source), _stamp(wal)):
                yield target
                return
    raise Failure("collect", "source")


def _rows(
    db: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()
) -> list[Row]:
    return [
        {k: _decode(v) if isinstance(v, bytes) else v for k, v in dict(row).items()}
        for row in db.execute(query, params)
    ]


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f'PRAGMA table_info("{table}")')}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _visits(
    db: sqlite3.Connection,
    visit_id: int,
    visit_date: int,
    limit: int,
    since: int | None,
    window: Window | None,
) -> tuple[list[Row], int, int, bool]:
    visit_filter = ""
    visit_params: tuple[int, ...] = ()
    if window is not None:
        visit_filter = " AND visit_date >= ? AND visit_date < ?"
        visit_params = (window[0] * 1_000_000, window[1] * 1_000_000)
    elif since is not None:
        visit_filter = " AND visit_date >= ?"
        visit_params = (since * 1_000_000,)
    visits = _rows(
        db,
        f"SELECT * FROM moz_historyvisits WHERE id > ?{visit_filter} ORDER BY id LIMIT ?",
        (visit_id, *visit_params, limit),
    )
    more_visits = len(visits) == limit
    next_id = visits[-1]["id"] if visits else visit_id
    if window is None and not more_visits:
        visits.extend(
            _rows(
                db,
                "SELECT * FROM moz_historyvisits WHERE id <= ? AND visit_date > ?"
                f"{visit_filter} ORDER BY id",
                (visit_id, visit_date, *visit_params),
            )
        )
    next_date = max(
        visit_date,
        max(
            (row["visit_date"] for row in visits if type(row["visit_date"]) is int),
            default=visit_date,
        ),
    )
    return visits, next_id, next_date, more_visits


def _places(
    db: sqlite3.Connection,
    place_id: int,
    limit: int,
    since: int | None,
    window: Window | None,
    visits: list[Row],
) -> tuple[list[Row], int, bool]:
    new_places = []
    if window is None:
        place_filter = ""
        place_params: tuple[int, ...] = ()
        if since is not None and "last_visit_date" in _columns(db, "moz_places"):
            place_filter = " AND (last_visit_date IS NULL OR last_visit_date >= ?)"
            place_params = (since * 1_000_000,)
        new_places = _rows(
            db,
            f"SELECT * FROM moz_places WHERE id > ?{place_filter} ORDER BY id LIMIT ?",
            (place_id, *place_params, limit),
        )
    next_id = new_places[-1]["id"] if new_places else place_id
    places = {row["id"]: row for row in new_places}
    ids = list(
        dict.fromkeys(
            row["place_id"] for row in visits if row["place_id"] not in places
        )
    )
    for i in range(0, len(ids), 500):
        chunk = tuple(ids[i : i + 500])
        placeholders = ",".join("?" for _ in chunk)
        for row in _rows(
            db, f"SELECT * FROM moz_places WHERE id IN ({placeholders})", chunk
        ):
            places[row["id"]] = row
    return list(places.values()), next_id, len(new_places) == limit


def _metadata(
    db: sqlite3.Connection,
    updated_at: int,
    limit: int,
    since: int | None,
    window: Window | None,
) -> tuple[list[Row], int, bool]:
    metadata_filter = ""
    metadata_params: tuple[int, ...] = ()
    if window is not None:
        metadata_filter = " AND updated_at >= ? AND updated_at < ?"
        metadata_params = (window[0] * 1000, window[1] * 1000)
    elif since is not None:
        metadata_filter = " AND updated_at >= ?"
        metadata_params = (since * 1000,)
    metadata = _rows(
        db,
        "SELECT * FROM moz_places_metadata WHERE updated_at > ?"
        f"{metadata_filter} ORDER BY updated_at, id LIMIT ?",
        (updated_at, *metadata_params, limit),
    )
    more = len(metadata) == limit
    if more:
        last = metadata[-1]["updated_at"]
        earlier = [row for row in metadata if row["updated_at"] != last]
        metadata = earlier or _rows(
            db,
            "SELECT * FROM moz_places_metadata WHERE updated_at = ? ORDER BY id",
            (last,),
        )
    return metadata, metadata[-1]["updated_at"] if metadata else updated_at, more


def _changed_snapshot(
    db: sqlite3.Connection, table: str, previous: str
) -> tuple[list[Row] | None, str]:
    items = _rows(db, f"SELECT * FROM {table} ORDER BY id")
    digest = hashlib.sha256(_json(items).encode("ascii")).hexdigest()
    return (items if digest != previous else None), digest


def _pages(
    db: sqlite3.Connection,
    cursor: ZenHistoryCursor,
    limit: int,
    since: int | None,
    window: Window | None,
) -> tuple[dict[str, list[Row]], ZenHistoryCursor, bool]:
    tables = {
        row[0]
        for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")
    }
    if any(
        table not in tables or required - _columns(db, table)
        for table, required in _REQUIRED.items()
    ):
        raise Failure("collect", "source")

    visits, visit_id, visit_date, more_visits = _visits(
        db, cursor.visit_id, cursor.visit_date, limit, since, window
    )
    places, place_id, more_places = _places(
        db, cursor.place_id, limit, since, window, visits
    )
    rows = {"moz_places": places, "moz_historyvisits": visits}
    updated_at = cursor.metadata_updated_at
    more_metadata = False
    if "moz_places_metadata" in tables and "updated_at" in _columns(
        db, "moz_places_metadata"
    ):
        metadata, updated_at, more_metadata = _metadata(
            db, updated_at, limit, since, window
        )
        if metadata:
            rows["moz_places_metadata"] = metadata
    digests = {
        "origins_digest": cursor.origins_digest,
        "bookmarks_digest": cursor.bookmarks_digest,
    }
    for name in ("origins", "bookmarks") if window is None else ():
        table = "moz_" + name
        if table in tables:
            key = name + "_digest"
            items, digests[key] = _changed_snapshot(db, table, digests[key])
            if items is not None:
                rows[table] = items
    return (
        rows,
        ZenHistoryCursor(
            visit_id=visit_id,
            visit_date=visit_date,
            place_id=place_id,
            metadata_updated_at=updated_at,
            **digests,
        ),
        more_visits or more_places or more_metadata,
    )


def _redact(table: str, row: Row, masked: Counter[str], masker: Masker) -> None:
    if table == "moz_places":
        for key in ("url", "preview_image_url"):
            if isinstance(value := row.get(key), str):
                row[key], counts = masker.url(value)
                masked.update(counts)
    titles = {"moz_places": ("title", "description"), "moz_bookmarks": ("title",)}
    for key in titles.get(table, ()):
        if isinstance(value := row.get(key), str):
            row[key], counts = masker.tokens(value)
            masked.update(counts)


def _collect(
    options: ZenHistoryOptions,
    cursor: ZenHistoryCursor | None,
    out_path: Path,
    now: float,
    masker: Masker,
    window: Window | None,
) -> Collected | None:
    source = _source(options)
    # Resolve the source once so the guarded file is the one copied.
    ZenHistory.guard_paths(_source_dirs(source), out_path, Path(gettempdir()))
    since = options.since if window is None else None
    if cursor is None:
        start = window[0] if window is not None else since
        cursor = ZenHistoryCursor(
            metadata_updated_at=start * 1000 - 1 if start is not None else 0
        )
    try:
        with _stable_copy(source) as target, closing(sqlite3.connect(target)) as db:
            db.text_factory = _decode
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            if [tuple(row) for row in db.execute("PRAGMA quick_check")] != [("ok",)]:
                raise Failure("collect", "source")
            rows, next_cursor, more = _pages(
                db,
                cursor,
                options.max_rows,
                since,
                window,
            )
    except (OSError, sqlite3.Error, ValueError, OverflowError):
        raise Failure("collect", "source") from None

    counts = {table: len(items) for table, items in rows.items() if items}
    if not counts:
        return None
    masked: Counter[str] = Counter()
    with BatchWriter(out_path, ZenHistory.format_id, 1, now) as batch:
        for table, items in rows.items():
            for row in items:
                if options.mask_urls:
                    _redact(table, row, masked, masker)
                batch.write({"type": "row", "table": table, "row": row})
        batch.finish({"rows": counts, "masked": dict(masked)})
    return Collected(next_cursor, {"rows": counts, "masked": dict(masked)}, more)
