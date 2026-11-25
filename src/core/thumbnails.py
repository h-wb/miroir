"""WebP thumbnails of untrusted image bytes (decision 040).

This is the only module that imports an image library (Pillow, and pillow-heif
for HEIC), and nothing in `src`
imports it: the derive run starts it as its own process (`python -m
src.core.thumbnails`), with an address-space limit and no credentials in its
environment, and sends it one file at a time. A crash, a kill or a decode that
takes too long then costs that file, not the run. It imports nothing of Miroir,
so the child starts quickly and holds nothing else.
"""

import json
import resource
import struct
import sys
import warnings
from io import BytesIO
from typing import BinaryIO, Literal

from PIL import Image, ImageOps
from pillow_heif import register_heif_opener

# HEIC and HEIF (photos from an iPhone), decoded by libheif. Registered here, in
# the child, and nowhere else (decision 040). Only the primary image is read: no
# embedded thumbnails, depth maps or auxiliary images. libheif applies the file's
# own rotation and mirroring (`irot`, `imir`) and pillow-heif then reports EXIF
# orientation 1, so the image is turned once.
register_heif_opener(
    thumbnails=False, depth_images=False, aux_images=False, decode_threads=1
)

# About an 8K screen with room to spare. A JPEG decodes at a reduced scale.
MAX_PIXELS = 40_000_000
# Pillow keeps a pointer and an allocation per row, so a thin image costs far
# more than its pixel count: both sides and the ratio between them are bounded.
# The bounds are the same either way round, so an EXIF rotation changes nothing.
MAX_SIDE = 16384
MAX_ASPECT = 16
# WebP cannot be taller than this.
MAX_HEIGHT = 16383
QUALITY = {360: 70, 1280: 80}
# libwebp's speed for size: 2 encodes about 40% faster than its default 4 for
# about 1% more bytes on a screen of text.
WEBP_METHOD = 2
ORIENTATION = 0x0112
# EXIF orientations that swap width and height.
TRANSPOSED = (5, 6, 7, 8)

Refusal = Literal["unsupported", "too_large", "unreadable"]


class Refused(Exception):
    """No thumbnail can be made of this file; `status` says why."""

    def __init__(self, status: Refusal):
        super().__init__(status)
        self.status: Refusal = status


def make_thumbnails(
    body: bytes, formats: tuple[str, ...], widths: tuple[int, ...]
) -> dict[int, bytes]:
    """One WebP per width, or Refused.

    The caller bounds `body`. The header is read first and the shape checked
    before anything is decoded; only the first frame is used.
    """
    if not formats:
        raise Refused("unsupported")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(body), formats=formats) as image:
                _check_shape(*image.size)
                _draft(image, max(widths))
                image.load()
                return _encode(_prepared(image), widths)
    except Refused:
        raise
    except (Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise Refused("too_large") from None
    except MemoryError:
        # The child's address-space limit, reached by a shape the checks allowed.
        raise Refused("too_large") from None
    except Exception:  # noqa: BLE001 - any decoder failure means no thumbnail
        raise Refused("unreadable") from None


def _check_shape(width: int, height: int) -> None:
    if width < 1 or height < 1:
        raise Refused("unreadable")
    if (
        width * height > MAX_PIXELS
        or max(width, height) > MAX_SIDE
        or max(width, height) > MAX_ASPECT * min(width, height)
    ):
        raise Refused("too_large")


def _draft(image: Image.Image, largest: int) -> None:
    """Let a JPEG decode at a reduced scale, no smaller than `largest` wide once
    it is turned the way its EXIF orientation says."""
    width, height = image.size
    try:
        orientation = image.getexif().get(ORIENTATION, 1)
    except Exception:  # noqa: BLE001 - unreadable EXIF: as stored
        orientation = 1
    shown_width = height if orientation in TRANSPOSED else width
    if shown_width > largest:
        image.draft(
            "RGB",
            (
                max(1, -(-width * largest // shown_width)),
                max(1, -(-height * largest // shown_width)),
            ),
        )


def _prepared(image: Image.Image) -> Image.Image:
    ImageOps.exif_transpose(image, in_place=True)
    if image.mode in ("RGB", "RGBA"):
        return image
    alpha = image.mode in ("LA", "PA", "RGBa", "La") or (
        image.mode == "P" and "transparency" in image.info
    )
    return image.convert("RGBA" if alpha else "RGB")


def _encode(image: Image.Image, widths: tuple[int, ...]) -> dict[int, bytes]:
    result = {}
    # Each size from the next larger one: fewer pixels to filter.
    source = image
    for width in sorted(widths, reverse=True):
        source = _fit(source, width)
        out = BytesIO()
        # No `exif` or `xmp` is passed, so no metadata (a photo's place) is kept.
        source.save(out, "WEBP", quality=QUALITY.get(width, 80), method=WEBP_METHOD)
        result[width] = out.getvalue()
    return result


def _fit(image: Image.Image, width: int) -> Image.Image:
    scale = min(1.0, width / image.width, MAX_HEIGHT / image.height)
    if scale >= 1.0:
        return image
    size = (max(1, round(image.width * scale)), max(1, round(image.height * scale)))
    return image.resize(size, Image.Resampling.LANCZOS, reducing_gap=3.0)


# ---- The child process ------------------------------------------------------
#
# Request: a 4-byte length, a JSON header {"formats", "widths", "size"}, then
# `size` bytes of the original. Answer: a 4-byte length, a JSON header
# {"status", "sizes": [[width, length], ...], "peak_kb"}, then the thumbnails in
# that order.


def _read(stream: BinaryIO, size: int) -> bytes | None:
    data = stream.read(size)
    return data if len(data) == size else None


def _limit_cpu(seconds: int) -> None:
    """At most `seconds` more of CPU for this file, behind the parent's timeout."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    used = int(usage.ru_utime + usage.ru_stime) + 1
    hard = resource.getrlimit(resource.RLIMIT_CPU)[1]
    resource.setrlimit(resource.RLIMIT_CPU, (used + seconds, hard))


def serve(source: BinaryIO, sink: BinaryIO, cpu_seconds: int = 0) -> None:
    while (head := _read(source, 4)) is not None:
        request = json.loads(_read(source, struct.unpack(">I", head)[0]) or b"")
        body = _read(source, int(request["size"])) or b""
        if cpu_seconds:
            _limit_cpu(cpu_seconds)
        try:
            made = make_thumbnails(
                body, tuple(request["formats"]), tuple(request["widths"])
            )
            status = "made"
        except Refused as refused:
            made, status = {}, refused.status
        del body
        answer = json.dumps(
            {
                "status": status,
                "sizes": [[width, len(data)] for width, data in made.items()],
                "peak_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            }
        ).encode()
        sink.write(struct.pack(">I", len(answer)) + answer + b"".join(made.values()))
        sink.flush()


def main(argv: list[str]) -> int:
    memory, cpu_seconds = int(argv[1]), int(argv[2])
    # No core file: it would hold the decoded image and whatever else was mapped.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory, memory))
    except (ValueError, OSError):
        # macOS (development) does not take it; elsewhere, refuse to run unbounded.
        if sys.platform != "darwin":
            raise
    serve(sys.stdin.buffer, sys.stdout.buffer, cpu_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
