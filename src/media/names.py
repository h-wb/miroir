"""What objects are named in the bucket, and their content types.

An object is `prefix / connection / kind / key`, the key being the value the
row carries; a thumbnail is beside it under a derived kind (decision 040).
"""

from pathlib import PurePosixPath
from unicodedata import category

CONTENT_TYPES = {
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".heic": "image/heic",
    ".heif": "image/heif",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    ".bmp": "image/bmp",
    ".mov": "video/quicktime",
    ".mp4": "video/mp4",
    ".m4v": "video/x-m4v",
    ".3gp": "video/3gpp",
    ".caf": "audio/x-caf",
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".aac": "audio/aac",
    ".amr": "audio/AMR",
    ".wav": "audio/wav",
    ".pdf": "application/pdf",
    ".vcf": "text/vcard",
    ".txt": "text/plain",
    ".rtf": "application/rtf",
    ".csv": "text/csv",
    ".ics": "text/calendar",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".pages": "application/vnd.apple.pages",
    ".numbers": "application/vnd.apple.numbers",
    ".key": "application/vnd.apple.keynote",
    ".zip": "application/zip",
}


def valid_key(key: object) -> bool:
    return (
        isinstance(key, str)
        and 1 <= len(key) <= 512
        and "\\" not in key
        and not any(category(char) in {"Cc", "Cs"} for char in key)
        and all(part not in {"", ".", ".."} for part in key.split("/"))
    )


def extension(key: str) -> str:
    return PurePosixPath(key).suffix.lower()


def content_type(key: str) -> str:
    return CONTENT_TYPES.get(extension(key), "application/octet-stream")


def image_type(key: str) -> str | None:
    """The content type of an image original, by extension; None for other files."""
    found = CONTENT_TYPES.get(extension(key), "")
    return found if found.startswith("image/") else None


def object_name(prefix: str, connection_id: str, kind: str, key: str) -> str:
    return "/".join(part for part in (prefix, connection_id, kind, key) if part)


def kind_prefix(prefix: str, connection_id: str, kind: str) -> str:
    """What every object of one kind is named after, ending in a slash."""
    return object_name(prefix, connection_id, kind, "") + "/"


def thumbnail_name(
    prefix: str, connection_id: str, kind: str, key: str, width: int, output: str
) -> str:
    """A thumbnail beside its original, under a derived kind (decision 040)."""
    return object_name(prefix, connection_id, f"{kind}.w{width}", f"{key}.{output}")
