"""How a kind's thumbnails are made: data, not code per kind (decision 040).

A media kind names a profile, or none when it is not derived. A profile says
which decoder reads each extension, which widths are made, what is written and
how large a written thumbnail can be. Each width is a `thumbnail_w<width>`
column of `media_objects` (`src.media.derive.THUMBNAIL_COLUMNS`), so a new width
is a new column beside the others.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

# Pillow decoders by extension; a file is read only by the decoder of its key's
# extension, whatever its bytes claim to be.
_IMAGES = {
    ".jpeg": "JPEG",
    ".jpg": "JPEG",
    ".png": "PNG",
    ".gif": "GIF",
    ".webp": "WEBP",
    ".bmp": "BMP",
    ".tiff": "TIFF",
    ".tif": "TIFF",
    # pillow-heif's format, registered only in the decoder child.
    ".heic": "HEIF",
    ".heif": "HEIF",
}


@dataclass(frozen=True)
class Profile:
    name: str
    # Pillow decoders by extension. A file of any other extension of the kind
    # is recorded `unsupported` and not read.
    decoders: Mapping[str, str]
    # Thumbnail sizes, by width.
    widths: tuple[int, ...] = (360, 1280)
    # What the decoder writes: WebP, the only format it writes.
    output: Literal["webp"] = "webp"
    # A thumbnail larger than this is not one the decoder could have made.
    max_output_bytes: int = 16 * 1024 * 1024

    def decoder(self, extension: str) -> str | None:
        return self.decoders.get(extension)

    @property
    def formats(self) -> tuple[str, ...]:
        """The decoders this profile uses, each once."""
        return tuple(sorted(set(self.decoders.values())))

    @property
    def content_type(self) -> str:
        return f"image/{self.output}"


PROFILES: Mapping[str, Profile] = MappingProxyType(
    {
        # Photos and other images: every decoder, HEIC included.
        "image": Profile("image", MappingProxyType(_IMAGES)),
        # Screens: the collector stores JPEG or PNG, so a HEIC screenshot is
        # recorded `unsupported`.
        "screenshot": Profile(
            "screenshot",
            MappingProxyType(
                {e: d for e, d in _IMAGES.items() if d in ("JPEG", "PNG")}
            ),
        ),
    }
)


def formats(profiles: Iterable[Profile]) -> tuple[str, ...]:
    """Every decoder these profiles use, each once."""
    return tuple(sorted({f for profile in profiles for f in profile.formats}))
