"""Collect and redact incremental ScreenMemory records."""

import math
import sqlite3
import stat
from collections import Counter
from contextlib import closing
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote

from pydantic import BeforeValidator, Field

from ..batch import BatchWriter
from ..errors import Failure
from ..masking import mask
from .base import (
    Collected,
    Collector,
    Cursor,
    MediaItem,
    MediaPage,
    MediaSource,
    Nonblank,
    Options,
    Since,
    UserPath,
    Window,
    nonempty,
)

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


class ScreenMemoryOptions(Options):
    data_dir: UserPath
    since: Annotated[Since | None, BeforeValidator(nonempty)] = None
    settle_seconds: int = Field(default=300, ge=0)
    max_rows: int = Field(default=20000, ge=1, le=200000)
    drop_bundle_ids: list[Nonblank] = Field(default_factory=list)
    drop_markers: list[Nonblank] = Field(default_factory=list)
    media: bool = False
    media_dir: Annotated[
        UserPath,
        Field(
            default_factory=lambda data: data.get("data_dir", Path()) / "screenshots"
        ),
    ]


class ScreenMemoryCursor(Cursor):
    frames: int
    app_usage: int


class ScreenMemory(Collector[ScreenMemoryOptions, ScreenMemoryCursor]):
    name = "screenmemory"
    format_id = "screenmemory_batch"
    options_model = ScreenMemoryOptions
    cursor_model = ScreenMemoryCursor

    def source_dirs(self, options: ScreenMemoryOptions) -> list[Path]:
        return (
            [options.data_dir, options.media_dir]
            if options.media
            else [options.data_dir]
        )

    def collect(
        self,
        options: ScreenMemoryOptions,
        cursor: ScreenMemoryCursor | None,
        out: Path,
        now: float,
        *,
        window: Window | None = None,
    ) -> Collected | None:
        self.guard(options, out)
        return _collect(options, cursor, out, now, window=window)

    def media(self, options: ScreenMemoryOptions) -> MediaSource | None:
        return Screenshots(options) if options.media else None


class Screenshots(MediaSource):
    kind = "screenshots"

    def __init__(self, options: ScreenMemoryOptions) -> None:
        self.options = options

    def start(self) -> int:
        return _start(self.options)

    def limit(self, cursor: Cursor | None) -> int | None:
        if cursor is None:
            return None
        assert isinstance(cursor, ScreenMemoryCursor)
        return cursor.frames

    def page(self, after: int, upto: int, limit: int) -> MediaPage | None:
        return _collect_media(self.options, after, upto, limit)


def _start(options: ScreenMemoryOptions) -> int:
    return options.since - 1 if options.since is not None else 0


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


def _withholding(
    options: ScreenMemoryOptions, db: sqlite3.Connection, frames: list[tuple[Any, ...]]
) -> tuple[set[int], tuple[str, ...]]:
    drop_bundles = _DROP_BUNDLES.union(options.drop_bundle_ids)
    markers = tuple(
        marker.casefold() for marker in _DROP_MARKERS.union(options.drop_markers)
    )
    stamps = [ts for ts, _, _ in frames if _valid_ts(ts)]
    excluded = (
        {
            ts
            for ts, bundle in db.execute(
                f"SELECT * FROM ({_USAGE}) WHERE second BETWEEN ? AND ?",
                (min(stamps), max(stamps)),
            )
            if bundle in drop_bundles
        }
        if stamps
        else set()
    )
    return excluded, markers


def _frame_text(value: Any) -> str:
    if isinstance(value, bytes):
        return _decode(value)
    return str(value) if value is not None else ""


def _withheld(
    ts: int, text: str, excluded: set[int], markers: tuple[str, ...]
) -> str | None:
    if ts in excluded:
        return "excluded_app"
    if any(marker in text.casefold() for marker in markers):
        return "marker"
    return None


def _frame(
    ts: int, key: Any, value: Any, excluded: set[int], markers: tuple[str, ...]
) -> tuple[str, str | None] | None:
    if not _valid_ts(ts) or not _valid_path(key):
        return None
    text = _frame_text(value)
    return text, _withheld(ts, text, excluded, markers)


def _snapshot(path: Path) -> sqlite3.Connection:
    db = _open(path)
    try:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
    except sqlite3.Error:
        db.close()
        raise
    return db


def _collect_media(
    options: ScreenMemoryOptions, media_cursor: int, upper: int, limit: int
) -> MediaPage | None:
    if media_cursor >= upper:
        return None
    data = options.data_dir
    try:
        with closing(_snapshot(data / "text.sqlite")) as text_db:
            frames, cursor, more = _page(text_db, _FRAMES, limit, media_cursor, upper)
        if not frames:
            return None
        with closing(_snapshot(data / "usage.sqlite")) as usage_db:
            excluded, markers = _withholding(options, usage_db, frames)
        root = options.media_dir.resolve()
    except (sqlite3.Error, OSError, OverflowError, RuntimeError, ValueError):
        raise Failure("collect", "source") from None
    return _media_items(root, frames, cursor, more, excluded, markers)


def _media_items(
    root: Path,
    frames: list[tuple[Any, ...]],
    cursor: int,
    more: bool,
    excluded: set[int],
    markers: tuple[str, ...],
) -> MediaPage:
    counts = {"examined": len(frames), "withheld": 0, "missing": 0, "invalid": 0}
    items: list[MediaItem] = []
    for ts, key, text in frames:
        frame = _frame(ts, key, text, excluded, markers)
        if frame is None:
            counts["invalid"] += 1
            continue
        if frame[1]:
            counts["withheld"] += 1
            continue
        try:
            path = (root / key).resolve()
            if path == root or not path.is_relative_to(root):
                counts["invalid"] += 1
                continue
        except (OSError, RuntimeError, ValueError):
            counts["invalid"] += 1
            continue
        try:
            info = path.stat()
        except OSError:
            counts["missing"] += 1
            continue
        if stat.S_ISREG(info.st_mode) and info.st_size > 0:
            items.append(MediaItem(key, path, info.st_size))
        else:
            counts["missing"] += 1
    return MediaPage(items, cursor, counts, more)


def _collect(
    options: ScreenMemoryOptions,
    cursor: ScreenMemoryCursor | None,
    out_path: Path,
    now: float,
    *,
    window: Window | None = None,
) -> Collected | None:
    data = options.data_dir
    initial = window[0] - 1 if window is not None else _start(options)
    frame_cursor = cursor.frames if cursor is not None else initial
    usage_cursor = cursor.app_usage if cursor is not None else initial
    upper = math.floor(now - options.settle_seconds)
    if window is not None:
        frame_cursor = max(initial, frame_cursor)
        usage_cursor = max(initial, usage_cursor)
        upper = min(upper, window[1] - 1)
    limit = options.max_rows
    try:
        with (
            closing(_snapshot(data / "text.sqlite")) as text_db,
            closing(_snapshot(data / "usage.sqlite")) as usage_db,
        ):
            frames, next_frame, more_frames = _page(
                text_db, _FRAMES, limit, frame_cursor, upper
            )
            apps, next_app, more_apps = _page(
                usage_db, _USAGE, limit, usage_cursor, upper
            )
            excluded, markers = _withholding(options, usage_db, frames)
    except (sqlite3.Error, OSError, OverflowError, ValueError):
        raise Failure("collect", "source") from None
    if not frames and not apps:
        return None

    counts: dict[str, Any] = {"frames": 0, "app_usage": 0, "skipped": 0}
    masked: Counter[str] = Counter()
    withheld: Counter[str] = Counter()
    with BatchWriter(out_path, ScreenMemory.format_id, 1, now) as batch:
        for ts, image_path, image_text in frames:
            frame = _frame(ts, image_path, image_text, excluded, markers)
            if frame is None:
                counts["skipped"] += 1
                continue
            raw_text, reason = frame
            text: str | None = None
            replacements = 0
            if reason:
                withheld[reason] += 1
            else:
                text, rules = mask(raw_text)
                masked.update(rules)
                replacements = sum(rules.values())
            batch.write(
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
            batch.write({"type": "app_usage", "ts": ts, "bundle_id": bundle})
            counts["app_usage"] += 1
        counts.update(masked=dict(masked), withheld=dict(withheld))
        batch.finish({k: v for k, v in counts.items() if k != "skipped"})
    return Collected(
        ScreenMemoryCursor(frames=next_frame, app_usage=next_app),
        counts,
        more_frames or more_apps,
    )
