"""Streaming validation of ScreenMemory batches, without dlt."""

import gzip
import json
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from src.integrations.validation import InvalidArtifact

FORMAT_ID = "screenmemory_batch"
VERSION = 1
MAX_BATCH_BYTES = 512 * 1024 * 1024
MAX_LINE_BYTES = 2 * 1024 * 1024
MIN_TS = 978307200
MAX_TS = 4102444800
EXPECTED = (
    "Expected a ScreenMemory batch (screenmemory_batch version 1): gzip UTF-8 "
    "JSON Lines with a header, frame or app usage records, and a matching trailer; "
    "up to 512 MiB uncompressed and 2 MiB per line"
)


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise InvalidArtifact(EXPECTED)
    return obj


def _count(value: Any) -> bool:
    return type(value) is int and value >= 0


def _counts(value: Any) -> bool:
    return isinstance(value, dict) and all(_count(count) for count in value.values())


def _valid_frame(record: dict[str, Any]) -> bool:
    if record.keys() != {"type", "ts", "image_path", "text", "withheld", "masked"}:
        return False
    path, text, withheld = record["image_path"], record["text"], record["withheld"]
    return (
        isinstance(path, str)
        and 0 < len(path) <= 512
        and not path.startswith("/")
        and ".." not in path.split("/")
        and (
            withheld is None or (isinstance(withheld, str) and 0 < len(withheld) <= 64)
        )
        # A withheld frame carries no text; any other frame carries its text.
        and (text is None if withheld is not None else isinstance(text, str))
        and _count(record["masked"])
    )


def _valid_app_usage(record: dict[str, Any]) -> bool:
    return (
        record.keys() == {"type", "ts", "bundle_id"}
        and isinstance(record["bundle_id"], str)
        and 0 < len(record["bundle_id"]) <= 255
    )


def read_records(path: Path) -> Iterator[dict[str, Any]]:
    """Yield data records; exhaust the iterator to validate the full batch."""
    size = 0
    header_seen = trailer_seen = False
    counts = {"frame": 0, "app_usage": 0}
    try:
        with gzip.open(path, "rb") as source:
            while line := source.readline(
                min(MAX_LINE_BYTES, MAX_BATCH_BYTES - size) + 1
            ):
                size += len(line)
                if len(line) > MAX_LINE_BYTES or size > MAX_BATCH_BYTES or trailer_seen:
                    raise InvalidArtifact(EXPECTED)
                record = json.loads(line.decode("utf-8"), object_pairs_hook=_object)
                if not isinstance(record, dict):
                    raise InvalidArtifact(EXPECTED)
                kind = record.get("type")
                if not header_seen:
                    if not (
                        kind == "header"
                        and record.keys()
                        == {"type", "format", "version", "collected_at"}
                        and record["format"] == FORMAT_ID
                        and type(record["version"]) is int
                        and record["version"] == VERSION
                        and isinstance(record["collected_at"], str)
                    ):
                        raise InvalidArtifact(EXPECTED)
                    header_seen = True
                elif kind == "trailer":
                    if not (
                        record.keys()
                        == {"type", "frames", "app_usage", "masked", "withheld"}
                        and _count(record["frames"])
                        and record["frames"] == counts["frame"]
                        and _count(record["app_usage"])
                        and record["app_usage"] == counts["app_usage"]
                        and _counts(record["masked"])
                        and _counts(record["withheld"])
                    ):
                        raise InvalidArtifact(EXPECTED)
                    trailer_seen = True
                else:
                    ts = record.get("ts")
                    if type(ts) is not int or not MIN_TS <= ts <= MAX_TS:
                        raise InvalidArtifact(EXPECTED)
                    if kind == "frame" and _valid_frame(record):
                        counts["frame"] += 1
                    elif kind == "app_usage" and _valid_app_usage(record):
                        counts["app_usage"] += 1
                    else:
                        raise InvalidArtifact(EXPECTED)
                    yield record
    except InvalidArtifact:
        raise
    except (OSError, EOFError, zlib.error, ValueError, RecursionError):
        raise InvalidArtifact(EXPECTED) from None
    if not trailer_seen:
        raise InvalidArtifact(EXPECTED)


def validate_batch(path: Path) -> None:
    for _ in read_records(path):
        pass
