"""Collect Apple Messages rows from one read-only snapshot."""

import hashlib
import json
import math
import re
import sqlite3
from collections import Counter
from contextlib import closing
from itertools import batched
from pathlib import Path, PurePosixPath
from typing import Annotated, Any
from urllib.parse import quote

from pydantic import BeforeValidator, Field

from ..batch import BatchWriter
from ..errors import Failure
from ..masking import Masker
from .base import Collected, Cursor, Options, Since, Source, UserPath, Window, nonempty

_EPOCH = 978307200
_MESSAGE = [
    "guid",
    "text",
    "subject",
    "service",
    "is_from_me",
    "date",
    "date_read",
    "date_delivered",
    "date_edited",
    "date_retracted",
    "is_read",
    "is_delivered",
    "is_sent",
    "error",
    "item_type",
    "group_action_type",
    "group_title",
    "associated_message_guid",
    "associated_message_type",
    "thread_originator_guid",
    "reply_to_guid",
    "balloon_bundle_id",
    "expressive_send_style_id",
    "cache_has_attachments",
    "is_audio_message",
    "destination_caller_id",
    "was_downgraded",
    "part_count",
]
_CHAT = [
    "guid",
    "chat_identifier",
    "service_name",
    "display_name",
    "style",
    "room_name",
    "is_archived",
]
_HANDLE = ["id", "service", "country", "uncanonicalized_id", "person_centric_id"]
_ATTACHMENT = [
    "guid",
    "original_guid",
    "transfer_name",
    "mime_type",
    "uti",
    "total_bytes",
    "is_outgoing",
    "is_sticker",
    "created_date",
    "hide_attachment",
]
_KEYS = {
    "message": ("guid",),
    "chat": ("guid",),
    "handle": ("id", "service"),
    "attachment": ("guid",),
    "chat_message": ("chat_guid", "message_guid"),
    "message_attachment": ("message_guid", "attachment_guid"),
    "chat_handle": ("chat_guid", "handle", "handle_service"),
    "contact": ("handle",),
}
_REQUIRED = {
    "message": {"guid", "date"},
    "chat": {"guid"},
    "handle": {"id"},
    "attachment": {"guid"},
    "chat_message_join": {"chat_id", "message_id"},
    "message_attachment_join": {"message_id", "attachment_id"},
    "chat_handle_join": {"chat_id", "handle_id"},
}
_FLAGS = set(
    [
        "is_from_me",
        "is_read",
        "is_delivered",
        "is_sent",
        "cache_has_attachments",
        "is_audio_message",
        "was_downgraded",
        "is_archived",
        "is_outgoing",
        "is_sticker",
        "hide_attachment",
    ]
)
_INTS = set(
    [
        "date",
        "date_read",
        "date_delivered",
        "date_edited",
        "date_retracted",
        "error",
        "item_type",
        "group_action_type",
        "associated_message_type",
        "part_count",
        "style",
        "total_bytes",
        "created_date",
    ]
)
_CHANGES = ("date_edited", "date_retracted")
_LENGTHS = ("text", "attributedBody")
_NS = 10**12  # a date at least this large is in nanoseconds, else in seconds
_MEDIA_GUID = re.compile(r"[A-Za-z0-9_.:-]{1,200}")
_EXTENSION = re.compile(r"\.[A-Za-z0-9]{1,10}")
Row = dict[str, Any]
Digest = Annotated[str, Field(pattern=r"^(?:[0-9a-f]{64})?$")]


def _pair(value: object) -> object:
    # A saved cursor is JSON, where a pair is a list; the strict model wants a tuple.
    return tuple(value) if isinstance(value, list) else value


Anchor = Annotated[tuple[int, Digest], BeforeValidator(_pair)]


class AppleMessagesOptions(Options):
    database: UserPath = Field(
        default="~/Library/Messages/chat.db", validate_default=True
    )
    since: Annotated[Since | None, BeforeValidator(nonempty)] = None
    settle_seconds: int = Field(default=0, ge=0)
    max_rows: int = Field(default=20000, ge=1, le=200000)


class AppleMessagesCursor(Cursor):
    message: int = 0
    # The CLI prints the cursor in status; keep GUIDs out of it.
    anchors: list[Anchor] = Field(default_factory=list, max_length=8)
    chats_digest: Digest = ""
    handles_digest: Digest = ""
    # SHA-256 of (guid, date_edited, date_retracted) of the sent messages that
    # were edited or unsent: when it changes, all of them are sent again.
    edits: Digest = ""
    # SHA-256 of the sorted table.column names the source reads.
    layout: Digest = ""


class AppleMessages(Source[AppleMessagesOptions, AppleMessagesCursor]):
    name = "apple_messages"
    format_id = "apple_messages_batch"
    options_model = AppleMessagesOptions
    cursor_model = AppleMessagesCursor

    def source_dirs(self, options: AppleMessagesOptions) -> list[Path]:
        try:
            return [options.database.parent, options.database.resolve().parent]
        except (OSError, RuntimeError, ValueError):
            raise Failure("collect", "source") from None

    def collect(
        self,
        options: AppleMessagesOptions,
        cursor: AppleMessagesCursor | None,
        out: Path,
        now: float,
        *,
        masker: Masker,
        window: Window | None = None,
    ) -> Collected | None:
        self.guard(options, out)
        try:
            with closing(
                sqlite3.connect(
                    f"file:{quote(str(options.database.absolute()))}?mode=ro",
                    uri=True,
                    timeout=5,
                )
            ) as db:
                db.row_factory = sqlite3.Row
                db.text_factory = _decode
                db.execute("PRAGMA trusted_schema=OFF")
                db.execute("PRAGMA query_only=ON")
                db.execute("BEGIN")
                rows, next_cursor, more, layout_changed = _collect_rows(
                    db, options, cursor, now, window
                )
        except (sqlite3.Error, OSError, OverflowError, ValueError):
            raise Failure("collect", "source") from None
        if not rows:
            return None
        counts = _write(out, now, rows, masker)
        return Collected(
            next_cursor, {**counts, "layout_changed": layout_changed}, more
        )


def media_key(guid: object, transfer_name: object, filename: object) -> str | None:
    """The name an attachment's file is stored under: its guid and extension."""
    if not isinstance(guid, str) or not _MEDIA_GUID.fullmatch(guid):
        return None
    extension = _extension(transfer_name) or _extension(filename)
    return guid + extension if extension else None


def _extension(name: object) -> str | None:
    if not isinstance(name, str):
        return None
    suffix = PurePosixPath(name).suffix
    return suffix.lower() if _EXTENSION.fullmatch(suffix) else None


def attributed_text(blob: bytes) -> str | None:
    """Plain text of an NSAttributedString typedstream, or None."""
    # An NSMutableString archive names NSString too, as its parent class.
    start = blob.find(b"NSString")
    if start < 0:
        return None
    mark = blob.find(b"\x01+", start)
    if mark < 0:
        return None
    pos = mark + 2
    if pos >= len(blob):
        return None
    first = blob[pos]
    if first in (0x81, 0x82):
        size = 2 if first == 0x81 else 4
        if pos + 1 + size > len(blob):
            return None  # the length field itself is cut short
        length = int.from_bytes(blob[pos + 1 : pos + 1 + size], "little")
        pos += 1 + size
    else:
        length, pos = first, pos + 1
    data = blob[pos : pos + length]
    if len(data) != length:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _decode(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def _columns(db: sqlite3.Connection) -> dict[str, set[str]]:
    columns = {
        table: {
            row[0].lower()
            for row in db.execute("SELECT name FROM pragma_table_info(?)", (table,))
        }
        for table in _REQUIRED
    }
    if any(required - columns[table] for table, required in _REQUIRED.items()):
        raise Failure("collect", "source")
    return columns


def _layout(columns: dict[str, set[str]]) -> str:
    names = sorted(
        f"{table}.{column}" for table in columns for column in columns[table]
    )
    return hashlib.sha256("\n".join(names).encode()).hexdigest()


def _column(alias: str, field: str, columns: set[str], name: str) -> str:
    expr = f'{alias}."{field}"' if field.lower() in columns else "NULL"
    return f'{expr} AS "{name}"'


def _fields(alias: str, fields: list[str], columns: set[str]) -> str:
    return ", ".join(_column(alias, field, columns, field) for field in fields)


def _fingerprint(guid: str) -> str:
    return hashlib.sha256(guid.encode()).hexdigest()


def _matches(db: sqlite3.Connection, anchor: tuple[int, str]) -> bool:
    rowid, digest = anchor
    row = db.execute("SELECT guid FROM message WHERE ROWID = ?", (rowid,)).fetchone()
    return (
        row is not None and isinstance(row[0], str) and _fingerprint(row[0]) == digest
    )


def _rebuilt(anchors: list[tuple[int, str]], kept: list[tuple[int, str]]) -> bool:
    """True when none of the three newest anchors is where it was."""
    newest = sorted(anchors)[-3:]
    return bool(newest) and not any(anchor in kept for anchor in newest)


def _anchors(kept: list[tuple[int, str]], rows: list[Row]) -> list[tuple[int, str]]:
    """The anchors that still matched plus the page's, the 8 with the highest ROWID."""
    sent = [
        (row["_rowid"], row["guid"]) for row in rows if isinstance(row["guid"], str)
    ]
    if len(sent) > 8:
        sent = [sent[i * (len(sent) - 1) // 7] for i in range(8)]
    anchors = dict(kept)
    anchors.update((rowid, _fingerprint(guid)) for rowid, guid in sent)
    return sorted(anchors.items())[-8:]


def _select(columns: dict[str, set[str]], *extra: str) -> str:
    fields = _fields("m", [*_MESSAGE, "attributedBody"], columns["message"])
    handle_join = "h.ROWID = m.handle_id" if "handle_id" in columns["message"] else "0"
    service = _column("h", "service", columns["handle"], "handle_service")
    return (
        f"SELECT {', '.join(('m.ROWID AS _rowid', *extra))}, {fields}, "
        f"h.id AS handle, {service} FROM message m LEFT JOIN handle h ON {handle_join}"
    )


def _ns(expr: str) -> str:
    """A date in nanoseconds, whichever unit this row was written in."""
    return (
        f"(CASE WHEN {expr} >= {_NS} OR {expr} <= -{_NS} "
        f"THEN {expr} ELSE {expr} * 1000000000 END)"
    )


def _nanoseconds(unix: float) -> int:
    """A Unix time as Apple nanoseconds, clamped to SQLite's integers."""
    seconds = unix - _EPOCH
    whole = math.floor(seconds)
    value = whole * 10**9 + math.floor((seconds - whole) * 10**9)
    return max(-(2**63), min(value, 2**63 - 1))


def _since(options: AppleMessagesOptions) -> tuple[str, list[int]]:
    if options.since is None:
        return "", []
    return f" AND {_ns('m.date')} >= ?", [_nanoseconds(options.since)]


def _edits(
    db: sqlite3.Connection,
    columns: dict[str, set[str]],
    options: AppleMessagesOptions,
    upto: int,
) -> list[Row]:
    """Sent messages up to `upto` that were edited or unsent, in ROWID order.

    Rows hold ROWID, guid, date_edited and date_retracted.
    """
    present = [field for field in _CHANGES if field in columns["message"]]
    if not present or upto <= 0:
        return []
    changed = " OR ".join(
        f'(typeof(m."{field}") = \'integer\' AND m."{field}" != 0)' for field in present
    )
    since, params = _since(options)
    # Two edits in one second keep the times; the text's length tells them apart.
    lengths = ", ".join(
        f'length(m."{field}") AS "_{field}"'
        if field.lower() in columns["message"]
        else f'NULL AS "_{field}"'
        for field in _LENGTHS
    )
    rows = [
        dict(row)
        for row in db.execute(
            f"SELECT m.ROWID AS _rowid, m.guid AS guid, {lengths}, "
            f"{_fields('m', list(_CHANGES), columns['message'])} FROM message m "
            f"WHERE m.ROWID <= ? AND typeof(m.date) = 'integer' AND ({changed}){since} "
            "ORDER BY m.ROWID",
            (upto, *params),
        )
    ]
    return rows


def _edits_digest(rows: list[Row]) -> str:
    """SHA-256 of each row's guid, two dates and text lengths; empty for none."""
    if not rows:
        return ""
    fields = [*_CHANGES, *(f"_{field}" for field in _LENGTHS)]
    values = [[row["guid"], *(row[field] for field in fields)] for row in rows]
    text = json.dumps(values, separators=(",", ":"), default=_blob_hex)
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def _resent(
    db: sqlite3.Connection, columns: dict[str, set[str]], edited: list[Row]
) -> list[Row]:
    rows: list[Row] = []
    for chunk in batched([row["_rowid"] for row in edited], 500):
        marks = ",".join("?" for _ in chunk)
        rows.extend(
            {**row, "_resent": True}
            for row in db.execute(
                f"{_select(columns)} WHERE m.ROWID IN ({marks}) ORDER BY m.ROWID",
                chunk,
            )
        )
    return rows


def _page(
    db: sqlite3.Connection,
    columns: dict[str, set[str]],
    options: AppleMessagesOptions,
    after: int,
    now: float,
    window: Window | None,
) -> tuple[list[Row], bool]:
    date = _ns("m.date")
    clauses = "m.ROWID > ? AND typeof(m.date) = 'integer'"
    params = [after]
    if window is None:
        since, bound = _since(options)
        clauses += since
        params += bound
    else:
        # A resend is explicit: its range only, with no settle bound.
        clauses += f" AND {date} >= ? AND {date} < ?"
        params += [_nanoseconds(window[0]), _nanoseconds(window[1])]
    page = db.execute(
        f"{_select(columns, f'{date} AS _date')} WHERE {clauses} "
        "ORDER BY m.ROWID LIMIT ?",
        (*params, options.max_rows),
    )
    upper = _nanoseconds(now - options.settle_seconds)
    present = _nanoseconds(now)
    settles = window is None and options.settle_seconds > 0
    rows: list[Row] = []
    with closing(page):
        for row in page:
            # Only a message inside the settle window waits. One dated in the
            # future (a scheduled message, a wrong clock) is sent now, or it
            # would hold back every later row until its date.
            if settles and upper < row["_date"] <= present:
                more = (
                    bool(rows)
                    and db.execute(
                        f"SELECT 1 FROM message m WHERE {clauses} AND m.ROWID > ? "
                        f"AND ({date} <= ? OR {date} > ?) LIMIT 1",
                        (*params, row["_rowid"], upper, present),
                    ).fetchone()
                    is not None
                )
                return rows, more
            rows.append(dict(row))
    return rows, len(rows) == options.max_rows


def _message_links(
    db: sqlite3.Connection,
    columns: dict[str, set[str]],
    messages: list[Row],
) -> dict[str, list[Row]]:
    rows: dict[str, list[Row]] = {
        "chat_message": [],
        "message_attachment": [],
        "attachment": [],
    }
    ids = [row["_rowid"] for row in messages]
    attachments: set[int] = set()
    for chunk in batched(ids, 500):
        marks = ",".join("?" for _ in chunk)
        rows["chat_message"].extend(
            dict(row)
            for row in db.execute(
                "SELECT c.guid AS chat_guid, m.guid AS message_guid FROM chat_message_join j "
                "JOIN message m ON m.ROWID = j.message_id LEFT JOIN chat c ON c.ROWID = j.chat_id "
                f"WHERE j.message_id IN ({marks}) ORDER BY j.ROWID",
                chunk,
            )
        )
        for row in db.execute(
            "SELECT m.guid AS message_guid, a.guid AS attachment_guid, a.ROWID AS attachment_row "
            "FROM message_attachment_join j JOIN message m ON m.ROWID = j.message_id "
            "LEFT JOIN attachment a ON a.ROWID = j.attachment_id "
            f"WHERE j.message_id IN ({marks}) ORDER BY j.ROWID",
            chunk,
        ):
            link = dict(row)
            rowid = link.pop("attachment_row")
            rows["message_attachment"].append(link)
            if rowid is not None:
                attachments.add(rowid)
    fields = _fields("a", _ATTACHMENT, columns["attachment"])
    fields += ", " + _column("a", "filename", columns["attachment"], "_filename")
    for chunk in batched(sorted(attachments), 500):
        marks = ",".join("?" for _ in chunk)
        rows["attachment"].extend(
            dict(row)
            for row in db.execute(
                f"SELECT {fields} FROM attachment a WHERE a.ROWID IN ({marks}) ORDER BY a.ROWID",
                chunk,
            )
        )
    return rows


def _snapshots(
    db: sqlite3.Connection, columns: dict[str, set[str]]
) -> tuple[dict[str, list[Row]], dict[str, list[Row]]]:
    chats = _fields("c", _CHAT, columns["chat"])
    handles = _fields("h", _HANDLE, columns["handle"])
    service = _column("h", "service", columns["handle"], "handle_service")
    return (
        {
            "chat": [
                dict(row)
                for row in db.execute(f"SELECT {chats} FROM chat c ORDER BY c.ROWID")
            ],
            "chat_handle": [
                dict(row)
                for row in db.execute(
                    f"SELECT c.guid AS chat_guid, h.id AS handle, {service} FROM chat_handle_join j "
                    "LEFT JOIN chat c ON c.ROWID = j.chat_id LEFT JOIN handle h ON h.ROWID = j.handle_id "
                    "ORDER BY j.ROWID"
                )
            ],
        },
        {
            "handle": [
                dict(row)
                for row in db.execute(
                    f"SELECT {handles} FROM handle h ORDER BY h.ROWID"
                )
            ]
        },
    )


def _digest(rows: dict[str, list[Row]]) -> str:
    # Non-text keys cannot be sent, but their presence still changes the snapshot.
    value = json.dumps(
        rows, sort_keys=True, separators=(",", ":"), default=_blob_hex, allow_nan=False
    )
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _blob_hex(value: bytes) -> dict[str, str]:
    return {"blob": value.hex()}


def _collect_rows(
    db: sqlite3.Connection,
    options: AppleMessagesOptions,
    cursor: AppleMessagesCursor | None,
    now: float,
    window: Window | None,
) -> tuple[dict[str, list[Row]], AppleMessagesCursor, bool, int]:
    columns = _columns(db)
    layout = _layout(columns)
    saved_layout = cursor.layout if cursor is not None else ""
    kept = [a for a in cursor.anchors if _matches(db, a)] if cursor else []
    if cursor is None or _rebuilt(cursor.anchors, kept):
        cursor = AppleMessagesCursor()
    messages, more = _page(db, columns, options, cursor.message, now, window)
    last = messages[-1]["_rowid"] if messages else cursor.message
    edits, sent = cursor.edits, list(messages)
    if window is None:
        # One query covers both bounds: the rows sent before this pass, and
        # those sent by the end of it, whose digest is saved.
        edited = _edits(db, columns, options, last)
        before = [row for row in edited if row["_rowid"] <= cursor.message]
        if cursor.message > 0 and _edits_digest(before) != cursor.edits:
            sent += _resent(db, columns, before)
        edits = _edits_digest(edited)
    rows = {"message": sent, **_message_links(db, columns, sent)} if sent else {}
    chats_digest, handles_digest = cursor.chats_digest, cursor.handles_digest
    if window is None:
        chats, handles = _snapshots(db, columns)
        chats_digest, handles_digest = _digest(chats), _digest(handles)
        if chats_digest != cursor.chats_digest:
            rows.update(chats)
        if handles_digest != cursor.handles_digest:
            rows.update(handles)
    return (
        rows,
        AppleMessagesCursor(
            message=last,
            anchors=_anchors(kept, messages),
            chats_digest=chats_digest,
            handles_digest=handles_digest,
            edits=edits,
            layout=layout,
        ),
        more,
        int(bool(saved_layout) and saved_layout != layout),
    )


def _value(key: str, value: Any) -> str | int | None:
    if key in _FLAGS:
        return value if type(value) is int and value in (0, 1) else None
    if key in _INTS:
        return value if type(value) is int else None
    return value if isinstance(value, str) else None


def _valid_key(value: object) -> bool:
    # A key is never altered: one holding NUL or undecodable bytes (decoded as
    # U+FFFD) could merge with another row once replaced, so the row is skipped.
    return isinstance(value, str) and not ("\x00" in value or "\ufffd" in value)


def _clean(value: object) -> object:
    # Postgres cannot store U+0000. Masked fields holding one were already masked
    # whole by the scanner; this covers every other string.
    return value.replace("\x00", "\ufffd") if isinstance(value, str) else value


def _text(record: Row, archive: Any) -> str:
    if record["text"] is not None:
        return "text"
    if archive is None:
        return "none"
    record["text"] = attributed_text(archive) if isinstance(archive, bytes) else None
    return "attributed_body" if record["text"] is not None else "unreadable"


def _mask(record: Row, fields: tuple[str, ...], masker: Masker) -> Counter[str]:
    rules: Counter[str] = Counter()
    for field in fields:
        if isinstance(value := record[field], str):
            record[field], counts = masker.text(value)
            rules.update(counts)
    return rules


def _write(out: Path, now: float, rows: dict[str, list[Row]], masker: Masker) -> Row:
    counts: Row = dict.fromkeys(_KEYS, 0)
    counts["unreadable"] = 0
    skipped = resent = 0
    masked: Counter[str] = Counter()
    with BatchWriter(out, AppleMessages.format_id, 1, now) as batch:
        for kind, items in rows.items():
            for row in items:
                if not all(_valid_key(row[key]) for key in _KEYS[kind]):
                    skipped += 1
                    continue
                record = {
                    key: _value(key, value)
                    for key, value in row.items()
                    if not key.startswith("_") and key != "attributedBody"
                }
                # Free text is masked. Identifiers are not, on purpose: guids,
                # chat_identifier, room_name, handles and their services,
                # service names, mime_type, uti, balloon_bundle_id and
                # expressive_send_style_id.
                if kind == "attachment":
                    record["media_key"] = media_key(
                        row["guid"], row["transfer_name"], row["_filename"]
                    )
                    masked.update(_mask(record, ("transfer_name",), masker))
                elif kind == "message":
                    resent += bool(row.get("_resent"))
                    record["text_source"] = _text(record, row["attributedBody"])
                    counts["unreadable"] += record["text_source"] == "unreadable"
                    rules = _mask(record, ("text", "subject", "group_title"), masker)
                    record["masked"] = sum(rules.values())
                    masked.update(rules)
                elif kind == "chat":
                    masked.update(_mask(record, ("display_name",), masker))
                batch.write({"type": kind, **{k: _clean(v) for k, v in record.items()}})
                counts[kind] += 1
        counts["masked"] = dict(masked)
        batch.finish(counts)
    return {**counts, "skipped": skipped, "resent": resent}
