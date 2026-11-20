"""Collect incremental Firefox/Zen history from a private Places copy."""

import gzip
import hashlib
import json
import shutil
import sqlite3
from collections import Counter
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory, gettempdir
from typing import Any

from ..masking import mask_tokens, mask_url
from ..state import atomic_path, outside_source
from . import Collected, Cursor

DEFAULT_PROFILES_DIR = "~/Library/Application Support/zen/Profiles"
SOURCE_ERROR = "Cannot read Zen Places database or required columns"
CHANGING_SOURCE_ERROR = "Zen Places source kept changing during copy"
_INITIAL_CURSOR: Cursor = {
    "visit_id": 0,
    "visit_date": 0,
    "place_id": 0,
    "metadata_updated_at": 0,
    "origins_digest": "",
    "bookmarks_digest": "",
}
_REQUIRED = {
    "moz_places": {"id", "url"},
    "moz_historyvisits": {"id", "place_id", "visit_date"},
}
Row = dict[str, Any]


class _ChangingSource(ValueError):
    pass


def _source(options: dict[str, Any]) -> Path:
    if "places_path" in options:
        return Path(options["places_path"]).expanduser()
    profiles = Path(options.get("profiles_dir", DEFAULT_PROFILES_DIR)).expanduser()
    return max(profiles.glob("*/places.sqlite"), key=lambda p: p.stat().st_mtime_ns)


def _guard(source: Path, *paths: Path) -> None:
    for path in paths:
        outside_source(path, source.parent)
        outside_source(path, source.resolve().parent)


def require_outside_source(options: dict[str, Any], *paths: Path) -> None:
    try:
        source = _source(options)
        if not source.is_file():
            raise ValueError(SOURCE_ERROR)
    except (OSError, ValueError, RuntimeError):
        raise ValueError(SOURCE_ERROR) from None
    _guard(source, *paths)


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
    _guard(source, temp_root)
    wal = source.with_name(source.name + "-wal")
    for attempt in range(3):
        before = _stamp(source), _stamp(wal)
        if before[0] is None:
            if attempt == 0:
                raise ValueError(SOURCE_ERROR)
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
    raise _ChangingSource(CHANGING_SOURCE_ERROR)


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


def _pages(
    db: sqlite3.Connection,
    cursor: Cursor,
    limit: int,
    since: int | None,
    window: tuple[int, int] | None,
) -> tuple[dict[str, list[Row]], Cursor, bool]:
    tables = {
        row[0]
        for row in db.execute("SELECT name FROM sqlite_schema WHERE type='table'")
    }
    if any(
        table not in tables or required - _columns(db, table)
        for table, required in _REQUIRED.items()
    ):
        raise ValueError(SOURCE_ERROR)

    next_cursor = cursor.copy()
    visit_id = cursor["visit_id"]
    visit_date = cursor.get("visit_date", 0)
    if type(visit_date) is not int:
        raise ValueError(SOURCE_ERROR)
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
    if visits:
        next_cursor["visit_id"] = visits[-1]["id"]
    if window is None and not more_visits:
        visits.extend(
            _rows(
                db,
                "SELECT * FROM moz_historyvisits WHERE id <= ? AND visit_date > ?"
                f"{visit_filter} ORDER BY id",
                (visit_id, visit_date, *visit_params),
            )
        )
    next_cursor["visit_date"] = max(
        visit_date,
        max(
            (row["visit_date"] for row in visits if type(row["visit_date"]) is int),
            default=visit_date,
        ),
    )
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
            (cursor["place_id"], *place_params, limit),
        )
    if new_places:
        next_cursor["place_id"] = new_places[-1]["id"]
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
    rows = {"moz_places": list(places.values()), "moz_historyvisits": visits}
    more = more_visits or len(new_places) == limit

    if "moz_places_metadata" in tables and "updated_at" in _columns(
        db, "moz_places_metadata"
    ):
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
            (cursor["metadata_updated_at"], *metadata_params, limit),
        )
        if len(metadata) == limit:
            more = True
            last = metadata[-1]["updated_at"]
            earlier = [row for row in metadata if row["updated_at"] != last]
            metadata = earlier or _rows(
                db,
                "SELECT * FROM moz_places_metadata WHERE updated_at = ? ORDER BY id",
                (last,),
            )
        if metadata:
            next_cursor["metadata_updated_at"] = metadata[-1]["updated_at"]
            rows["moz_places_metadata"] = metadata

    for name in ("origins", "bookmarks") if window is None else ():
        table = "moz_" + name
        if table in tables:
            items = _rows(db, f"SELECT * FROM {table} ORDER BY id")
            digest = hashlib.sha256(_json(items).encode("ascii")).hexdigest()
            key = name + "_digest"
            if digest != cursor[key]:
                next_cursor[key] = digest
                rows[table] = items
    return rows, next_cursor, more


def _redact(table: str, row: Row, masked: Counter[str]) -> None:
    if table == "moz_places":
        for key in ("url", "preview_image_url"):
            if isinstance(value := row.get(key), str):
                row[key], counts = mask_url(value)
                masked.update(counts)
    titles = {"moz_places": ("title", "description"), "moz_bookmarks": ("title",)}
    for key in titles.get(table, ()):
        if isinstance(value := row.get(key), str):
            row[key], counts = mask_tokens(value)
            masked.update(counts)


def collect(
    options: dict[str, Any],
    cursor: Cursor | None,
    out_path: Path,
    now: float,
    *,
    window: tuple[int, int] | None = None,
) -> Collected | None:
    try:
        source = _source(options)
    except (OSError, ValueError, RuntimeError):
        raise ValueError(SOURCE_ERROR) from None
    _guard(source, out_path)
    since = options.get("since") if window is None else None
    if cursor is None:
        cursor = _INITIAL_CURSOR.copy()
        start = window[0] if window is not None else since
        if start is not None:
            cursor["metadata_updated_at"] = start * 1000 - 1
    try:
        with _stable_copy(source) as target, closing(sqlite3.connect(target)) as db:
            db.text_factory = _decode
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA trusted_schema=OFF")
            if [tuple(row) for row in db.execute("PRAGMA quick_check")] != [("ok",)]:
                raise ValueError(SOURCE_ERROR)
            rows, next_cursor, more = _pages(
                db,
                cursor,
                options.get("max_rows", 20000),
                since,
                window,
            )
    except _ChangingSource:
        raise
    except (OSError, sqlite3.Error, ValueError, OverflowError):
        raise ValueError(SOURCE_ERROR) from None

    counts = {table: len(items) for table, items in rows.items() if items}
    if not counts:
        return None
    masked: Counter[str] = Counter()
    with atomic_path(out_path) as path, gzip.open(path, "wt", encoding="utf-8") as file:
        size = 0

        def write(record: Row) -> None:
            nonlocal size
            try:
                line = _json(record) + "\n"
            except ValueError:
                raise ValueError(SOURCE_ERROR) from None
            size += len(line)
            if len(line) > 2 * 1024**2 or size > 512 * 1024**2:
                raise ValueError("Zen history batch exceeds format size limits")
            file.write(line)

        write(
            {
                "type": "header",
                "format": "firefox_places_batch",
                "version": 1,
                "collected_at": datetime.fromtimestamp(now, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
        )
        for table, items in rows.items():
            for row in items:
                if options.get("mask_urls", True):
                    _redact(table, row, masked)
                write({"type": "row", "table": table, "row": row})
        write({"type": "trailer", "rows": counts, "masked": dict(masked)})
    return Collected(next_cursor, {"rows": counts, "masked": dict(masked)}, more)
