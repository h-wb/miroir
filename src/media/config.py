"""The `media` section of the configuration, embedded in the root model.

Media is off until the section is present (decision 033). The bucket key is not
here: it comes from `MIROIR_MEDIA_ACCESS_KEY_ID` and
`MIROIR_MEDIA_SECRET_ACCESS_KEY` (`src.media.s3`).
"""

import re
from urllib.parse import urlsplit

from pydantic import Field, field_validator

from src.kernel.models import StrictModel


class MediaConfig(StrictModel):
    endpoint: str
    link_endpoint: str | None = None
    region: str = "garage"
    bucket: str = Field(min_length=1)
    prefix: str = ""
    upload_ttl_seconds: int = Field(default=900, ge=60, le=3600)
    view_ttl_seconds: int = Field(default=300, ge=30, le=3600)
    # Thumbnails (decision 040): confirmations within this many seconds share
    # one derive run, and one run handles at most `derive_batch` files.
    derive_delay_seconds: int = Field(default=300, ge=30, le=3600)
    derive_batch: int = Field(default=500, ge=1, le=5000)

    @field_validator("endpoint", "link_endpoint")
    @classmethod
    def http_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            url = urlsplit(value)
            if (
                url.scheme in {"http", "https"}
                and url.hostname
                and not any(char.isspace() or ord(char) < 32 for char in value)
                and (url.port is None or url.port > 0)
            ):
                return value
        except ValueError:
            pass
        raise ValueError("expected an HTTP or HTTPS URL")

    @field_validator("prefix")
    @classmethod
    def object_prefix(cls, value: str) -> str:
        if (
            not re.fullmatch(r"[A-Za-z0-9._/-]*", value)
            or value.startswith("/")
            or value.endswith("/")
            or ".." in value.split("/")
        ):
            raise ValueError("invalid media prefix")
        return value
