"""WebP thumbnails of untrusted image bytes (decision 040). Worker only.

This is the only module that imports an image library; `tests/test_boundaries.py`
keeps it out of the web process.
"""

import warnings
from collections.abc import Iterable
from io import BytesIO
from typing import Literal

from PIL import Image, ImageOps

# A size is the width of the thumbnail; a narrower image is not enlarged.
WIDTHS = (360, 1280)
# Twice an 8K screen. Decoding allocates about 4 bytes per pixel.
MAX_PIXELS = 40_000_000
# WebP cannot be taller than this.
MAX_HEIGHT = 16383
QUALITY = {360: 70, 1280: 80}
# libwebp's speed for size: 2 encodes about 40% faster than its default 4 for
# about 1% more bytes on a screen of text.
WEBP_METHOD = 2
# Pillow decoders by extension; an image is read only by the decoders of the
# extensions its kind accepts, whatever its bytes claim to be.
DECODERS = {
    ".jpeg": "JPEG",
    ".jpg": "JPEG",
    ".png": "PNG",
    ".gif": "GIF",
    ".webp": "WEBP",
    ".bmp": "BMP",
    ".tiff": "TIFF",
    ".tif": "TIFF",
}

Refusal = Literal["unsupported", "too_large", "unreadable"]


class Refused(Exception):
    """No thumbnail can be made of this file; `status` says why."""

    def __init__(self, status: Refusal):
        super().__init__(status)
        self.status: Refusal = status


def decoders(extensions: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({DECODERS[e] for e in extensions if e in DECODERS}))


def make_thumbnails(body: bytes, formats: tuple[str, ...]) -> dict[int, bytes]:
    """One WebP per width in WIDTHS, or Refused.

    The caller bounds `body`. The header is read first and the pixel count
    checked before anything is decoded; only the first frame is used.
    """
    if not formats:
        raise Refused("unsupported")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(body), formats=formats) as image:
                width, height = image.size
                if width < 1 or height < 1:
                    raise Refused("unreadable")
                if width * height > MAX_PIXELS:
                    raise Refused("too_large")
                # JPEG decodes at a reduced scale no smaller than the largest size.
                largest = max(WIDTHS)
                if width > largest:
                    image.draft("RGB", (largest, max(1, height * largest // width)))
                image.load()
                return _encode(_prepared(image))
    except Refused:
        raise
    except (Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise Refused("too_large") from None
    except Exception:  # noqa: BLE001 - any decoder failure means no thumbnail
        raise Refused("unreadable") from None


def _prepared(image: Image.Image) -> Image.Image:
    ImageOps.exif_transpose(image, in_place=True)
    if image.mode in ("RGB", "RGBA"):
        return image
    alpha = image.mode in ("LA", "PA", "RGBa", "La") or (
        image.mode == "P" and "transparency" in image.info
    )
    return image.convert("RGBA" if alpha else "RGB")


def _encode(image: Image.Image) -> dict[int, bytes]:
    result = {}
    # Each size from the next larger one: fewer pixels to filter.
    source = image
    for width in sorted(WIDTHS, reverse=True):
        source = _fit(source, width)
        out = BytesIO()
        # No `exif` or `xmp` is passed, so no metadata (a photo's place) is kept.
        source.save(out, "WEBP", quality=QUALITY[width], method=WEBP_METHOD)
        result[width] = out.getvalue()
    return result


def _fit(image: Image.Image, width: int) -> Image.Image:
    scale = min(1.0, width / image.width, MAX_HEIGHT / image.height)
    if scale >= 1.0:
        return image
    size = (max(1, round(image.width * scale)), max(1, round(image.height * scale)))
    return image.resize(size, Image.Resampling.LANCZOS, reducing_gap=3.0)
