"""ScreenMemory batches delivered by the Miroir collector."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.core.models import StrictModel
from src.integrations.contracts import (
    FileFormat,
    IntegrationDefinition,
    MediaKind,
    ResourceDefinition,
)
from src.integrations.screenmemory.formats import FORMAT_ID, VERSION, validate_batch


class ScreenMemorySettings(StrictModel):
    """A batch needs no connection settings."""


@contextmanager
def open_batch_source(
    settings: Any, path: str, options: dict[str, Any]
) -> Iterator[Any]:
    from src.integrations.screenmemory.source import screenmemory

    yield screenmemory(path=Path(path))


DEFINITION = IntegrationDefinition(
    name="screenmemory",
    title="ScreenMemory (screen text and app usage)",
    settings_model=ScreenMemorySettings,
    credentials=(),
    resources=(
        ResourceDefinition("frames", "Frames", record_kind="event"),
        ResourceDefinition("app_usage", "App usage", record_kind="event"),
    ),
    formats=(
        FileFormat(
            id=FORMAT_ID,
            version=VERSION,
            label="ScreenMemory batch (Miroir collector)",
            accept=".jsonl.gz,.gz,application/gzip",
            content_type="application/gzip",
            sender_instructions="Send one gzip-compressed JSON Lines batch as the raw HTTP body.",
            help="Batches contain screen text and app usage, not screenshot files.",
            validate=validate_batch,
            open=open_batch_source,
            default_pattern="*.jsonl.gz",
            sender="Miroir collector",
            received_name="screenmemory-batch.jsonl.gz",
        ),
    ),
    analysis=None,
    media=(
        MediaKind(
            "screenshots",
            "frames",
            "image_path",
            (".jpeg", ".jpg", ".png", ".heic"),
            20 * 1024 * 1024,
            # HEIC is accepted, but the collector stores JPEG: a HEIC screenshot
            # is recorded `unsupported` (decision 040).
            thumbnails=(".jpeg", ".jpg", ".png"),
        ),
    ),
)
