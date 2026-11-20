"""Extract frames and app usage from a stored ScreenMemory batch."""

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dlt
from dlt.extract import DltResource

from src.integrations.screenmemory.formats import read_records


@dlt.source(name="screenmemory")
def screenmemory(path: Path) -> list[DltResource]:
    @dlt.resource(
        name="frames",
        primary_key="image_path",
        write_disposition="merge",
        columns={
            "image_path": {"data_type": "text", "nullable": False},
            "ts": {"data_type": "bigint", "nullable": False},
            "captured_at": {"data_type": "timestamp", "nullable": False},
            "text": {"data_type": "text", "nullable": True},
            "withheld": {"data_type": "text", "nullable": True},
            "masked": {"data_type": "bigint", "nullable": False},
        },
    )
    def frames() -> Iterator[dict[str, Any]]:
        for record in read_records(path):
            if record.pop("type") == "frame":
                record["captured_at"] = datetime.fromtimestamp(
                    record["ts"], timezone.utc
                )
                yield record

    @dlt.resource(
        name="app_usage",
        primary_key=["ts", "bundle_id"],
        write_disposition="merge",
        columns={
            "ts": {"data_type": "bigint", "nullable": False},
            "bundle_id": {"data_type": "text", "nullable": False},
            "observed_at": {"data_type": "timestamp", "nullable": False},
        },
    )
    def app_usage() -> Iterator[dict[str, Any]]:
        for record in read_records(path):
            if record.pop("type") == "app_usage":
                record["observed_at"] = datetime.fromtimestamp(
                    record["ts"], timezone.utc
                )
                yield record

    return [frames, app_usage]
