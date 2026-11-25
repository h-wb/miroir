"""Streaming validation of Apple Messages batches, without dlt."""

import gzip
import json
import re
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from src.integrations.validation import InvalidArtifact

FORMAT_ID = "apple_messages_batch"
VERSION = 1
MAX_BATCH_BYTES = 512 * 1024 * 1024
MAX_LINE_BYTES = 2 * 1024 * 1024
EXPECTED = (
    "Expected an Apple Messages batch (apple_messages_batch version 1): gzip UTF-8 "
    "JSON Lines with a header, Apple Messages records, and a matching trailer; "
    "up to 512 MiB uncompressed and 2 MiB per line"
)


def _fields(*, text: str, bigint: str = "", bools: str = "") -> dict[str, str]:
    return {
        **dict.fromkeys(text.split(), "text"),
        **dict.fromkeys(bigint.split(), "bigint"),
        **dict.fromkeys(bools.split(), "bool"),
    }


FIELDS = {
    "message": _fields(
        text="guid text text_source subject service handle handle_service group_title "
        "associated_message_guid thread_originator_guid reply_to_guid balloon_bundle_id "
        "expressive_send_style_id destination_caller_id",
        bigint="masked date date_read date_delivered date_edited date_retracted error "
        "item_type group_action_type associated_message_type part_count",
        bools="is_from_me is_read is_delivered is_sent cache_has_attachments "
        "is_audio_message was_downgraded",
    ),
    "chat": _fields(
        text="guid chat_identifier service_name display_name room_name",
        bigint="style",
        bools="is_archived",
    ),
    "handle": _fields(text="id service country uncanonicalized_id person_centric_id"),
    "attachment": _fields(
        text="guid original_guid transfer_name mime_type uti media_key",
        bigint="total_bytes created_date",
        bools="is_outgoing is_sticker hide_attachment",
    ),
    "chat_message": _fields(text="chat_guid message_guid"),
    "message_attachment": _fields(text="message_guid attachment_guid"),
    "chat_handle": _fields(text="chat_guid handle handle_service"),
    "contact": _fields(text="handle first_name last_name nickname organization"),
}
KEYS = {
    "message": ("guid",),
    "chat": ("guid",),
    "handle": ("id", "service"),
    "attachment": ("guid",),
    "chat_message": ("chat_guid", "message_guid"),
    "message_attachment": ("message_guid", "attachment_guid"),
    "chat_handle": ("chat_guid", "handle", "handle_service"),
    "contact": ("handle",),
}
REQUIRED = {kind: set(keys) for kind, keys in KEYS.items()}
REQUIRED["message"].update(("text_source", "masked"))
# Apple's guids are never empty; an empty one would also collide with keys the
# models reserve.
GUIDS = {
    kind: [key for key in keys if key.endswith("guid")] for kind, keys in KEYS.items()
}


# Postgres cannot store U+0000, and the loader fails on a lone surrogate.
_UNSTORABLE = re.compile("[\x00\ud800-\udfff]")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise InvalidArtifact(EXPECTED)
    return obj


def _count(value: Any) -> bool:
    return type(value) is int and value >= 0


def _valid_value(value: Any, column_type: str) -> bool:
    if column_type == "text":
        return isinstance(value, str) and not _UNSTORABLE.search(value)
    if column_type == "bool":
        return type(value) is bool or (type(value) is int and value in (0, 1))
    return type(value) is int and -(2**63) <= value < 2**63


def _valid_record(kind: str, record: dict[str, Any]) -> bool:
    fields = FIELDS[kind]
    if record.keys() != {"type", *fields}:
        return False
    if not all(
        (record[key] is None and key not in REQUIRED[kind])
        or _valid_value(record[key], column_type)
        for key, column_type in fields.items()
    ):
        return False
    if any(record[key] == "" for key in GUIDS[kind]):
        return False
    if kind == "contact":
        return bool(record["handle"])
    if kind != "message":
        return True
    source = record["text_source"]
    return _count(record["masked"]) and (
        (source in ("text", "attributed_body") and record["text"] is not None)
        or (source in ("none", "unreadable") and record["text"] is None)
    )


def _valid_header(record: dict[str, Any]) -> bool:
    return (
        record.keys() == {"type", "format", "version", "collected_at"}
        and record["type"] == "header"
        and record["format"] == FORMAT_ID
        and type(record["version"]) is int
        and record["version"] == VERSION
        and isinstance(record["collected_at"], str)
    )


def _valid_trailer(
    record: dict[str, Any], counts: dict[str, int], message_masks: int
) -> bool:
    return (
        record.keys() == {"type", "masked", *counts}
        and all(
            _count(record[key]) and record[key] == count
            for key, count in counts.items()
        )
        and isinstance(masked := record["masked"], dict)
        and all(_count(count) for count in masked.values())
        # Chat display names and attachment names add masks without a
        # per-record count; without either, the messages account for them all.
        and (
            sum(masked.values()) >= message_masks
            if counts["chat"] or counts["attachment"]
            else sum(masked.values()) == message_masks
        )
    )


def _records(path: Path) -> Iterator[dict[str, Any]]:
    size = 0
    with gzip.open(path, "rb") as source:
        while line := source.readline(min(MAX_LINE_BYTES, MAX_BATCH_BYTES - size) + 1):
            size += len(line)
            if len(line) > MAX_LINE_BYTES or size > MAX_BATCH_BYTES:
                raise InvalidArtifact(EXPECTED)
            record = json.loads(line.decode("utf-8"), object_pairs_hook=_object)
            if not isinstance(record, dict):
                raise InvalidArtifact(EXPECTED)
            yield record


def read_records(path: Path) -> Iterator[dict[str, Any]]:
    """Yield data records; exhaust the iterator to validate the full batch."""
    counts = dict.fromkeys((*FIELDS, "unreadable"), 0)
    message_masks = 0
    try:
        records = _records(path)
        if not _valid_header(next(records, {})):
            raise InvalidArtifact(EXPECTED)
        for record in records:
            kind = record.get("type")
            if kind == "trailer":
                if (
                    _valid_trailer(record, counts, message_masks)
                    and next(records, None) is None
                ):
                    return
                raise InvalidArtifact(EXPECTED)
            if (
                not isinstance(kind, str)
                or kind not in FIELDS
                or not _valid_record(kind, record)
            ):
                raise InvalidArtifact(EXPECTED)
            counts[kind] += 1
            if kind == "message":
                message_masks += record["masked"]
                counts["unreadable"] += record["text_source"] == "unreadable"
            yield record
    except InvalidArtifact:
        raise
    except (OSError, EOFError, zlib.error, ValueError, RecursionError):
        raise InvalidArtifact(EXPECTED) from None
    raise InvalidArtifact(EXPECTED)


def validate_batch(path: Path) -> None:
    for _ in read_records(path):
        pass
