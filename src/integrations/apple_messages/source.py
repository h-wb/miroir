"""Load Apple Messages rows, merging on Apple's identifiers."""

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dlt
from dlt.extract import DltResource

from src.integrations.apple_messages.formats import FIELDS, KEYS, REQUIRED, read_records

_EPOCH = datetime(2001, 1, 1, tzinfo=timezone.utc)
_TIMESTAMPS = {
    "message": {
        "sent_at": "date",
        "read_at": "date_read",
        "delivered_at": "date_delivered",
        "edited_at": "date_edited",
        "retracted_at": "date_retracted",
    },
    "attachment": {"created_at": "created_date"},
}


def _timestamp(value: int | None, *, seconds: bool) -> datetime | None:
    if not value:
        return None
    try:
        if not seconds and abs(value) >= 10**12:
            return _EPOCH + timedelta(microseconds=value // 1000)
        return _EPOCH + timedelta(seconds=value)
    except OverflowError:
        return None


def _resource(path: Path, kind: str) -> DltResource:
    timestamps = _TIMESTAMPS.get(kind, {})
    columns: dict[str, Any] = {
        key: {"data_type": data_type, "nullable": key not in REQUIRED[kind]}
        for key, data_type in FIELDS[kind].items()
    }
    columns.update(
        {key: {"data_type": "timestamp", "nullable": True} for key in timestamps}
    )

    @dlt.resource(
        name=kind + "s",
        primary_key=list(KEYS[kind]),
        write_disposition="merge",
        columns=columns,
    )
    def rows() -> Iterator[dict[str, Any]]:
        for record in read_records(path):
            if record.pop("type") != kind:
                continue
            for key, data_type in FIELDS[kind].items():
                if data_type == "bool" and record[key] is not None:
                    record[key] = bool(record[key])
            for key, raw_key in timestamps.items():
                record[key] = _timestamp(record[raw_key], seconds=kind == "attachment")
            yield record

    return rows


@dlt.source(name="apple_messages")
def apple_messages(path: Path) -> list[DltResource]:
    return [_resource(path, kind) for kind in FIELDS]
