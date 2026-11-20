"""Bounded, read-only preparation of Firefox Places snapshots and batches."""

import gzip
import json
import math
import re
import sqlite3
import tempfile
import zlib
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

from src.integrations.validation import InvalidArtifact

MAX_SNAPSHOT_BYTES = 512 * 1024 * 1024
REQUIRED = {
    "moz_places": {"id", "url"},
    "moz_historyvisits": {"id", "place_id", "visit_date"},
}


def readonly(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(
        f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1", uri=True
    )
    db.execute("PRAGMA query_only=ON")
    db.execute("PRAGMA trusted_schema=OFF")
    return db


@contextmanager
def open_snapshot(path: Path) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="miroir-browser-") as tmp:
        target = Path(tmp) / "places.sqlite"
        with path.open("rb") as raw:
            compressed = raw.read(2) == b"\x1f\x8b"
        opener = gzip.open if compressed else open
        size = 0
        try:
            with opener(path, "rb") as source, target.open("wb") as output:
                while chunk := source.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_SNAPSHOT_BYTES:
                        raise ValueError("Snapshot exceeds uncompressed size limit")
                    output.write(chunk)
        except (EOFError, zlib.error) as error:
            raise ValueError("Incomplete or corrupt gzip snapshot") from error
        try:
            db = readonly(target)
            try:
                objects = dict(
                    db.execute(
                        "select name,type from sqlite_schema where name not like 'sqlite_%'"
                    )
                )
                for table, required in REQUIRED.items():
                    if objects.get(table) != "table":
                        raise ValueError("Missing Places table")
                    columns = {
                        row[1] for row in db.execute(f'PRAGMA table_info("{table}")')
                    }
                    if required - columns:
                        raise ValueError("Missing Places columns")
                if db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                    raise ValueError("Snapshot integrity check failed")
            finally:
                db.close()
        except sqlite3.Error as error:
            raise ValueError("Invalid Places database") from error
        yield target


BATCH_FORMAT = "firefox_places_batch"
BATCH_VERSION = 1
MAX_BATCH_BYTES = 512 * 1024 * 1024
MAX_LINE_BYTES = 2 * 1024 * 1024
BATCH_TABLES = (
    "moz_places",
    "moz_historyvisits",
    "moz_places_metadata",
    "moz_origins",
    "moz_bookmarks",
)
_EXPECTED_BATCH = "Expected a valid Firefox/Zen history batch (gzip JSON Lines)"
_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise InvalidArtifact(_EXPECTED_BATCH)
    return obj


def _count(value: Any) -> bool:
    return type(value) is int and value >= 0


def _counts(value: Any) -> bool:
    return isinstance(value, dict) and all(_count(n) for n in value.values())


def _scalar(value: Any) -> bool:
    return (
        value is None
        or isinstance(value, (bool, int, str))
        or (isinstance(value, float) and math.isfinite(value))
    )


def read_rows(path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield rows; exhaust the iterator to validate the full batch."""
    size = 0
    header_seen = trailer_seen = False
    counts = dict.fromkeys(BATCH_TABLES, 0)
    try:
        with gzip.open(path, "rb") as source:
            while line := source.readline(
                min(MAX_LINE_BYTES, MAX_BATCH_BYTES - size) + 1
            ):
                size += len(line)
                if len(line) > MAX_LINE_BYTES or size > MAX_BATCH_BYTES or trailer_seen:
                    raise InvalidArtifact(_EXPECTED_BATCH)
                record = json.loads(line.decode("utf-8"), object_pairs_hook=_object)
                if not isinstance(record, dict):
                    raise InvalidArtifact(_EXPECTED_BATCH)
                kind = record.get("type")
                if not header_seen:
                    if not (
                        kind == "header"
                        and record.keys()
                        == {"type", "format", "version", "collected_at"}
                        and record["format"] == BATCH_FORMAT
                        and type(record["version"]) is int
                        and record["version"] == BATCH_VERSION
                        and isinstance(record["collected_at"], str)
                    ):
                        raise InvalidArtifact(_EXPECTED_BATCH)
                    header_seen = True
                elif kind == "trailer":
                    if not (
                        record.keys() == {"type", "rows", "masked"}
                        and _counts(record["rows"])
                        and not (record["rows"].keys() - counts.keys())
                        and all(
                            record["rows"].get(table, 0) == n
                            for table, n in counts.items()
                        )
                        and _counts(record["masked"])
                    ):
                        raise InvalidArtifact(_EXPECTED_BATCH)
                    trailer_seen = True
                else:
                    table, row = record.get("table"), record.get("row")
                    if not (
                        kind == "row"
                        and record.keys() == {"type", "table", "row"}
                        and isinstance(table, str)
                        and table in counts
                        and isinstance(row, dict)
                        and len(row) <= 200
                        and _count(row.get("id"))
                        and all(
                            _KEY.fullmatch(k) and _scalar(v) for k, v in row.items()
                        )
                    ):
                        raise InvalidArtifact(_EXPECTED_BATCH)
                    counts[table] += 1
                    yield table, row
    except InvalidArtifact:
        raise
    except (OSError, EOFError, zlib.error, ValueError, RecursionError):
        raise InvalidArtifact(_EXPECTED_BATCH) from None
    if not trailer_seen:
        raise InvalidArtifact(_EXPECTED_BATCH)


def validate_batch(path: Path) -> None:
    for _ in read_rows(path):
        pass
