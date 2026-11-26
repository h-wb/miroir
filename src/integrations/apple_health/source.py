"""Health Auto Export extraction from one stored file and its import role."""

import hashlib
import json
from collections.abc import Iterator
from functools import lru_cache
from typing import Any

import dlt
from dlt.extract import DltResource, DltSource

TABLES = {
    "metrics": ("metrics", ["metric_name", "date", "source"], "date"),
    "workouts": ("workouts", "id", "start"),
    "stateOfMind": ("state_of_mind", "id", "start"),
    "medications": ("medications", ["displayText", "start"], "start"),
}
# Every other collection under `data`, one row per object, unchanged (decision
# 008 amendment): no schema per collection until one gets a table of its own.
OTHER_RECORDS = "other_records"
# What each import found in those collections: the import's report.
OTHER_COLLECTIONS = "other_collections"
# The fields the known collections are dated by, read the same way.
DATE_FIELDS = ("start", "date")


def record_key(item: dict[str, Any]) -> str:
    """The same object in any file, or a replay of one, has the same key."""
    canonical = json.dumps(item, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _dated(item: dict[str, Any]) -> str | None:
    for field in DATE_FIELDS:
        value = item.get(field)
        if isinstance(value, str) and len(value) >= 10:
            return value[:10]
    return None


def _entries(value: Any) -> list[Any]:
    return value if isinstance(value, list) else [value]


@dlt.source(name="apple_health")
def health_files(files: list[tuple[str, bool]], cutoff: str) -> list[DltResource]:
    @lru_cache(maxsize=1)
    def document(path: str) -> tuple[str, dict[str, Any]]:
        with open(path, "rb") as reader:
            content = reader.read()
        data: dict[str, Any] = json.loads(content)["data"]
        return hashlib.sha256(content).hexdigest(), data

    def in_role(day: str | None, historical: bool) -> bool:
        # An object without a date belongs to no side of the cutoff: every role
        # loads it, and its key merges the copies.
        return day is None or (day < cutoff) == historical

    def others(path: str) -> Iterator[tuple[str, str, list[Any]]]:
        file_sha256, data = document(path)
        for collection, value in data.items():
            if collection not in TABLES:
                yield file_sha256, collection, _entries(value)

    def resource(collection: str, table: str, key: Any, date_field: str) -> DltResource:
        @dlt.resource(name=table, primary_key=key, write_disposition="merge")
        def rows() -> Iterator[dict[str, Any]]:
            for path, historical in files:
                entries = document(path)[1].get(collection, [])
                if collection == "metrics":
                    records = (
                        {
                            "metric_name": metric["name"],
                            "units": metric.get("units"),
                            **point,
                            "source": point.get("source") or "unknown",
                        }
                        for metric in entries
                        for point in metric.get("data", [])
                    )
                else:
                    records = iter(entries)
                for row in records:
                    if (row[date_field][:10] < cutoff) == historical:
                        yield row

        return rows

    @dlt.resource(
        name=OTHER_RECORDS,
        primary_key=["collection", "record_key"],
        write_disposition="merge",
        columns={
            "collection": {"data_type": "text", "nullable": False},
            "record_key": {"data_type": "text", "nullable": False},
            "item": {"data_type": "json", "nullable": False},
            "file_sha256": {"data_type": "text", "nullable": False},
            "role": {"data_type": "text", "nullable": False},
        },
    )
    def other_records() -> Iterator[dict[str, Any]]:
        for path, historical in files:
            for file_sha256, collection, entries in others(path):
                for item in entries:
                    if isinstance(item, dict) and in_role(_dated(item), historical):
                        yield {
                            "collection": collection,
                            "record_key": record_key(item),
                            "item": item,
                            "file_sha256": file_sha256,
                            "role": "historical" if historical else "ongoing",
                        }

    @dlt.resource(
        name=OTHER_COLLECTIONS,
        primary_key=["file_sha256", "role", "collection"],
        write_disposition="merge",
        columns={
            "file_sha256": {"data_type": "text", "nullable": False},
            "role": {"data_type": "text", "nullable": False},
            "collection": {"data_type": "text", "nullable": False},
            "entries": {"data_type": "bigint", "nullable": False},
            "loaded": {"data_type": "bigint", "nullable": False},
            "unreadable": {"data_type": "bigint", "nullable": False},
        },
    )
    def other_collections() -> Iterator[dict[str, Any]]:
        for path, historical in files:
            for file_sha256, collection, entries in others(path):
                objects = [item for item in entries if isinstance(item, dict)]
                yield {
                    "file_sha256": file_sha256,
                    "role": "historical" if historical else "ongoing",
                    "collection": collection,
                    "entries": len(entries),
                    "loaded": sum(in_role(_dated(o), historical) for o in objects),
                    "unreadable": len(entries) - len(objects),
                }

    return [
        *(
            resource(collection, table, key, date_field)
            for collection, (table, key, date_field) in TABLES.items()
        ),
        other_records,
        other_collections,
    ]


def apple_health_artifact(path: str, cutoff: str, historical: bool) -> DltSource:
    return health_files(files=[(path, historical)], cutoff=cutoff)
