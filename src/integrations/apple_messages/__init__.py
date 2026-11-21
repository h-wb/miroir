"""Apple Messages batches delivered by the Miroir collector."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.core.models import StrictModel
from src.integrations.apple_messages.formats import FORMAT_ID, VERSION, validate_batch
from src.integrations.contracts import (
    FileFormat,
    IntegrationDefinition,
    MediaKind,
    ResourceDefinition,
)


class AppleMessagesSettings(StrictModel):
    """A batch needs no connection settings."""


@contextmanager
def open_batch_source(
    settings: Any, path: str, options: dict[str, Any]
) -> Iterator[Any]:
    from src.integrations.apple_messages.source import apple_messages

    yield apple_messages(path=Path(path))


DEFINITION = IntegrationDefinition(
    name="apple_messages",
    title="Apple Messages",
    settings_model=AppleMessagesSettings,
    credentials=(),
    resources=(
        ResourceDefinition("messages", "Messages", record_kind="event"),
        ResourceDefinition("chats", "Chats"),
        ResourceDefinition("handles", "Handles"),
        ResourceDefinition("attachments", "Attachments"),
        ResourceDefinition("chat_messages", "Chat messages"),
        ResourceDefinition("message_attachments", "Message attachments"),
        ResourceDefinition("chat_handles", "Chat handles"),
        ResourceDefinition("contacts", "Contacts"),
    ),
    formats=(
        FileFormat(
            id=FORMAT_ID,
            version=VERSION,
            label="Apple Messages batch (Miroir collector)",
            accept=".jsonl.gz,.gz,application/gzip",
            content_type="application/gzip",
            sender_instructions="Send one gzip-compressed JSON Lines batch as the raw HTTP body.",
            help="Batches contain Messages rows, not attachment files.",
            validate=validate_batch,
            open=open_batch_source,
            default_pattern="*.jsonl.gz",
            sender="Miroir collector",
            received_name="apple-messages-batch.jsonl.gz",
        ),
    ),
    analysis=None,
    media=(
        MediaKind(
            "attachments",
            "attachments",
            "media_key",
            (
                ".heic",
                ".heif",
                ".jpeg",
                ".jpg",
                ".png",
                ".gif",
                ".webp",
                ".tiff",
                ".tif",
                ".bmp",
                ".mov",
                ".mp4",
                ".m4v",
                ".3gp",
                ".caf",
                ".m4a",
                ".mp3",
                ".aac",
                ".amr",
                ".wav",
                ".pdf",
                ".vcf",
                ".txt",
                ".rtf",
                ".csv",
                ".ics",
                ".doc",
                ".docx",
                ".xls",
                ".xlsx",
                ".ppt",
                ".pptx",
                ".pages",
                ".numbers",
                ".key",
                ".zip",
            ),
            5000 * 1024 * 1024,
        ),
    ),
)
