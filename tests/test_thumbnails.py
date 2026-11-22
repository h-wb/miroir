"""Thumbnails of untrusted bytes (decision 040). Every image is made here."""

import struct
import unittest
import zlib
from io import BytesIO

from PIL import Image, features

from src.core.thumbnails import (
    MAX_HEIGHT,
    MAX_PIXELS,
    Refused,
    decoders,
    make_thumbnails,
)

JPEG = ("JPEG",)


def encoded(image: Image.Image, fmt: str, **options) -> bytes:
    out = BytesIO()
    image.save(out, fmt, **options)
    return out.getvalue()


def screen(width: int = 2880, height: int = 1800) -> bytes:
    image = Image.linear_gradient("L").resize((width, height)).convert("RGB")
    return encoded(image, "JPEG", quality=80)


def opened(body: bytes) -> Image.Image:
    image = Image.open(BytesIO(body))
    image.load()
    return image


def forged_png(width: int, height: int) -> bytes:
    """A valid 1x1 PNG whose header claims another size: nothing to decode."""
    body = encoded(Image.new("RGB", (1, 1)), "PNG")
    header = struct.pack(">II", width, height) + body[24:29]
    crc = struct.pack(">I", zlib.crc32(b"IHDR" + header))
    return body[:16] + header + crc + body[33:]


class ThumbnailTests(unittest.TestCase):
    def refusal(self, body: bytes, formats=JPEG) -> str:
        with self.assertRaises(Refused) as caught:
            make_thumbnails(body, formats)
        return caught.exception.status

    def test_every_kind_with_thumbnails_can_decode_them(self):
        from src.integrations.registry import INTEGRATIONS

        kinds = [k for i in INTEGRATIONS.values() for k in i.media if k.thumbnails]
        self.assertEqual([k.name for k in kinds], ["screenshots"])
        for kind in kinds:
            self.assertLessEqual(set(kind.thumbnails), set(kind.extensions))
            for extension in kind.thumbnails:
                self.assertTrue(decoders([extension]), extension)

    def test_pillow_has_webp(self):
        self.assertTrue(features.check("webp"))

    def test_two_webp_sizes_of_a_screen(self):
        made = make_thumbnails(screen(), JPEG)
        self.assertEqual(sorted(made), [360, 1280])
        large, small = opened(made[1280]), opened(made[360])
        self.assertEqual((large.format, large.size), ("WEBP", (1280, 800)))
        self.assertEqual((small.format, small.size), ("WEBP", (360, 225)))

    def test_a_narrow_image_is_not_enlarged(self):
        made = make_thumbnails(encoded(Image.new("RGB", (300, 120)), "PNG"), ("PNG",))
        self.assertEqual(opened(made[360]).size, (300, 120))
        self.assertEqual(opened(made[1280]).size, (300, 120))
        between = make_thumbnails(screen(800, 400), JPEG)
        self.assertEqual(opened(between[1280]).size, (800, 400))
        self.assertEqual(opened(between[360]).size, (360, 180))

    def test_modes_become_rgb_or_keep_their_alpha(self):
        rgba = encoded(Image.new("RGBA", (500, 500), (1, 2, 3, 100)), "PNG")
        self.assertEqual(opened(make_thumbnails(rgba, ("PNG",))[360]).mode, "RGBA")
        cases = {
            "L": encoded(Image.new("L", (500, 500)), "PNG"),
            "P": encoded(Image.new("P", (500, 500)), "PNG"),
            "I;16": encoded(Image.new("I;16", (500, 500)), "PNG"),
            "CMYK": encoded(Image.new("CMYK", (500, 500)), "JPEG"),
        }
        for mode, body in cases.items():
            with self.subTest(mode=mode):
                made = make_thumbnails(body, ("JPEG", "PNG"))
                self.assertEqual(opened(made[360]).mode, "RGB")

    def test_metadata_is_dropped_and_orientation_applied(self):
        exif = Image.Exif()
        exif[0x0112] = 6  # rotated: shown 200 wide and 400 tall
        exif[0x010F] = "A camera maker"
        body = encoded(Image.new("RGB", (400, 200)), "JPEG", exif=exif.tobytes())
        made = opened(make_thumbnails(body, JPEG)[360])
        self.assertEqual(made.size, (200, 400))
        self.assertEqual(dict(made.getexif()), {})
        self.assertNotIn("exif", made.info)

    def test_only_the_decoders_of_the_kind_are_used(self):
        png = encoded(Image.new("RGB", (50, 50)), "PNG")
        self.assertEqual(self.refusal(png, JPEG), "unreadable")
        self.assertEqual(sorted(make_thumbnails(png, ("JPEG", "PNG"))), [360, 1280])
        self.assertEqual(self.refusal(png, ()), "unsupported")
        self.assertEqual(decoders((".jpeg", ".jpg", ".png", ".heic")), ("JPEG", "PNG"))

    def test_broken_bytes_are_unreadable(self):
        body = screen(1600, 1000)
        cases = {
            "truncated": body[: len(body) // 2],
            "header only": body[:200],
            "random": bytes(range(256)) * 40,
            "empty": b"",
        }
        for name, broken in cases.items():
            with self.subTest(name):
                self.assertEqual(self.refusal(broken), "unreadable")

    def test_too_many_pixels_is_refused_before_decoding(self):
        side = int(MAX_PIXELS**0.5) + 1
        self.assertEqual(self.refusal(forged_png(side, side), ("PNG",)), "too_large")
        # Past Pillow's own bomb limit too, which raises instead of warning.
        self.assertEqual(self.refusal(forged_png(30000, 30000), ("PNG",)), "too_large")

    def test_a_compressed_bomb_under_the_byte_limit_is_refused(self):
        # A few hundred kilobytes that would decode to 200 MB.
        huge = Image.new("L", (8000, 6500))
        body = encoded(huge, "PNG", optimize=True)
        self.assertLess(len(body), 1024 * 1024)
        self.assertEqual(self.refusal(body, ("PNG",)), "too_large")

    def test_a_very_tall_image_fits_webp(self):
        tall = encoded(Image.new("RGB", (400, 40000)), "PNG")
        made = opened(make_thumbnails(tall, ("PNG",))[1280])
        self.assertEqual(made.height, MAX_HEIGHT)
        self.assertLessEqual(made.width, 400)

    def test_only_the_first_frame_of_an_animation(self):
        frames = [Image.new("RGB", (64, 64), (i, 0, 0)) for i in range(0, 250, 50)]
        body = encoded(frames[0], "GIF", save_all=True, append_images=frames[1:])
        made = opened(make_thumbnails(body, ("GIF",))[360])
        self.assertEqual((made.size, getattr(made, "n_frames", 1)), ((64, 64), 1))


if __name__ == "__main__":
    unittest.main()
