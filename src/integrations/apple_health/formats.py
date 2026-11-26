"""Health Auto Export JSON validation. Stdlib only."""

import json
from pathlib import Path

from src.integrations.validation import InvalidArtifact

# The collections with tables of their own; any other is loaded as it is.
HEALTH_COLLECTIONS = ("metrics", "workouts", "stateOfMind", "medications")
# The other collections Health Auto Export's JSON format documents
# (help.healthyapps.dev, "JSON Export Format"). They load into other_records.
DOCUMENTED_COLLECTIONS = (
    *HEALTH_COLLECTIONS,
    "symptoms",
    "ecg",
    "heartRateNotifications",
    "cycleTracking",
)


def validate_export(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            document = json.load(handle)
    except (OSError, UnicodeDecodeError, ValueError, RecursionError) as error:
        raise InvalidArtifact("Not a readable JSON document") from error
    data = document.get("data") if isinstance(document, dict) else None
    if not isinstance(data, dict):
        raise InvalidArtifact("Expected a top-level 'data' object")
    # Another JSON file with a `data` object is not an export: one documented
    # collection is needed. Unknown ones beside it still load (other_records).
    if not any(isinstance(data.get(key), list) for key in DOCUMENTED_COLLECTIONS):
        raise InvalidArtifact("No Health Auto Export collections found")
    present = [key for key in HEALTH_COLLECTIONS if key in data]
    for key in present:
        entries = data[key]
        if not isinstance(entries, list) or not all(
            isinstance(entry, dict) for entry in entries
        ):
            raise InvalidArtifact(f"'{key}' must be a list of objects")
