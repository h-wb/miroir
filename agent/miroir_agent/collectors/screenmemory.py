"""Collect and redact incremental ScreenMemory records."""

import gzip
import json
import math
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ..masking import mask
from ..state import atomic_path, outside_source
from . import Collected

_DROP_BUNDLES = {
    "com.bitwarden.desktop",
    "com.1password.1password",
    "com.agilebits.onepassword7",
    "me.proton.pass.electron",
    "com.apple.Passwords",
    "com.apple.keychainaccess",
    "org.keepassxc.keepassxc",
}
_DROP_MARKERS = {
    "vault.bitwarden.com",
    "vault.bitwarden.eu",
    "pass.proton.me",
    "my.1password.com",
    "my.1password.eu",
    "passwords.google.com",
    "about:logins",
    "chrome://password-manager",
}
SOURCE_ERROR = "Cannot read ScreenMemory databases or required columns"

_FRAMES = """
SELECT epoch_second(CASE WHEN typeof(imagetimestamp) = 'text'
    THEN CAST(imagetimestamp AS BLOB) ELSE imagetimestamp END) AS second,
    imagepath, imagetext FROM ocrresults
"""
_USAGE = """
SELECT epoch_second(CASE WHEN typeof(timestamp) = 'text'
    THEN CAST(timestamp AS BLOB) ELSE timestamp END) AS second,
    identifier FROM usage
"""


def _open(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(
        f"file:{quote(str(path.absolute()))}?mode=ro", uri=True, timeout=5
    )
    db.text_factory = _decode
    db.create_function("epoch_second", 1, _timestamp, deterministic=True)
    return db


def _decode(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def _timestamp(value: Any) -> int | None:
    # SQLite CAST accepts numeric prefixes in malformed strings; float does not.
    try:
        second = int(float(value))
    except (ValueError, TypeError, OverflowError):
        return None
    return second if -(2**63) <= second < 2**63 else None


def _page(
    db: sqlite3.Connection, query: str, limit: int, cursor: int, upper: int
) -> tuple[list[tuple[Any, ...]], int, bool]:
    rows = db.execute(
        f"SELECT * FROM ({query}) WHERE second > ? AND second <= ? ORDER BY second, 2 LIMIT ?",
        (cursor, upper, limit),
    ).fetchall()
    more = len(rows) == limit
    if more:
        last = rows[-1][0]
        split = len(rows) - 1
        while split and rows[split - 1][0] == last:
            split -= 1
        if split:
            rows = rows[:split]
        else:
            rows = db.execute(
                f"SELECT * FROM ({query}) WHERE second = ? ORDER BY 2", (last,)
            ).fetchall()
    # Invalid paths/bundles must not strand the cursor on a rejected row.
    next_cursor: int = rows[-1][0] if rows else cursor
    return rows, next_cursor, more


def _valid_ts(ts: int) -> bool:
    return 978307200 <= ts <= 4102444800


def _valid_path(path: Any) -> bool:
    return (
        isinstance(path, str)
        and 0 < len(path) <= 512
        and not path.startswith("/")
        and ".." not in path.split("/")
    )


def collect(
    options: dict[str, Any], cursor: dict[str, int] | None, out_path: Path, now: float
) -> Collected | None:
    data = Path(options["data_dir"]).expanduser()
    outside_source(out_path, data)
    cutoff = options.get("cutoff_ts", 0)
    frame_cursor = cursor["frames"] if cursor is not None else cutoff
    usage_cursor = cursor["app_usage"] if cursor is not None else cutoff
    upper = math.floor(now - options.get("settle_seconds", 300))
    limit = options.get("max_rows", 20000)
    drop_bundles = _DROP_BUNDLES.union(options.get("drop_bundle_ids", []))
    drop_markers = tuple(
        marker.casefold()
        for marker in _DROP_MARKERS.union(options.get("drop_markers", []))
    )
    try:
        with (
            closing(_open(data / "text.sqlite")) as text_db,
            closing(_open(data / "usage.sqlite")) as usage_db,
        ):
            for db in (text_db, usage_db):
                db.execute("PRAGMA query_only=ON")
                # Keep the page and any follow-up lookups on the same snapshot.
                db.execute("BEGIN")
            frames, next_frame, more_frames = _page(
                text_db, _FRAMES, limit, frame_cursor, upper
            )
            apps, next_app, more_apps = _page(
                usage_db, _USAGE, limit, usage_cursor, upper
            )
            stamps = [ts for ts, _, _ in frames if _valid_ts(ts)]
            excluded = (
                {
                    ts
                    for ts, bundle in usage_db.execute(
                        f"SELECT * FROM ({_USAGE}) WHERE second BETWEEN ? AND ?",
                        (min(stamps), max(stamps)),
                    )
                    if bundle in drop_bundles
                }
                if stamps
                else set()
            )
    except (sqlite3.Error, OverflowError, ValueError):
        raise ValueError(SOURCE_ERROR) from None
    if not frames and not apps:
        return None

    counts: dict[str, Any] = {"frames": 0, "app_usage": 0, "skipped": 0}
    masked: Counter[str] = Counter()
    withheld: Counter[str] = Counter()
    with atomic_path(out_path) as path, gzip.open(path, "wt", encoding="utf-8") as file:
        size = 0

        def write(record: dict[str, Any]) -> None:
            nonlocal size
            line = json.dumps(record, separators=(",", ":")) + "\n"
            size += len(line)  # json.dumps emits ASCII by default.
            if len(line) > 2 * 1024**2 or size > 512 * 1024**2:
                raise ValueError("ScreenMemory batch exceeds format size limits")
            file.write(line)

        write(
            {
                "type": "header",
                "format": "screenmemory_batch",
                "version": 1,
                "collected_at": datetime.fromtimestamp(now, timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
        )
        for ts, image_path, image_text in frames:
            if not _valid_ts(ts) or not _valid_path(image_path):
                counts["skipped"] += 1
                continue
            if isinstance(image_text, bytes):
                raw_text = _decode(image_text)
            else:
                raw_text = str(image_text) if image_text is not None else ""
            text: str | None = None
            reason = None
            replacements = 0
            if ts in excluded:
                reason = "excluded_app"
            elif any(marker in raw_text.casefold() for marker in drop_markers):
                reason = "marker"
            if reason:
                withheld[reason] += 1
            else:
                text, rules = mask(raw_text)
                masked.update(rules)
                replacements = sum(rules.values())
            write(
                {
                    "type": "frame",
                    "ts": ts,
                    "image_path": image_path,
                    "text": text,
                    "withheld": reason,
                    "masked": replacements,
                }
            )
            counts["frames"] += 1
        for ts, bundle in apps:
            if (
                not _valid_ts(ts)
                or not isinstance(bundle, str)
                or not 0 < len(bundle) <= 255
            ):
                counts["skipped"] += 1
                continue
            write({"type": "app_usage", "ts": ts, "bundle_id": bundle})
            counts["app_usage"] += 1
        counts.update(masked=dict(masked), withheld=dict(withheld))
        write(
            {"type": "trailer", **{k: v for k, v in counts.items() if k != "skipped"}}
        )
    return Collected(
        {"frames": next_frame, "app_usage": next_app}, counts, more_frames or more_apps
    )
