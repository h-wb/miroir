"""Thumbnails of untrusted bytes (decision 040). Every image is made here."""

import struct
import unittest
import zlib
from io import BytesIO
from unittest.mock import patch

from PIL import Image, features

from src.media.decoder import (
    MAX_ASPECT,
    MAX_HEIGHT,
    MAX_PIXELS,
    MAX_SIDE,
    Refused,
    make_thumbnails,
)
from src.media.profiles import PROFILES
from src.media.profiles import formats as decoders_of

JPEG = ("JPEG",)
HEIF = ("HEIF",)


def make(body: bytes, formats: tuple[str, ...]) -> dict[int, bytes]:
    return make_thumbnails(body, formats, PROFILES["image"].widths)


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
            make(body, formats)
        return caught.exception.status

    def test_every_derived_kind_has_a_profile_it_can_decode(self):
        from src.integrations.registry import INTEGRATIONS

        kinds = [k for i in INTEGRATIONS.values() for k in i.media if k.profile]
        self.assertEqual(
            {k.name: k.profile for k in kinds},
            {"screenshots": "screenshot", "attachments": "image"},
        )
        for kind in kinds:
            profile = kind.derive_profile
            assert profile is not None
            self.assertLessEqual(set(profile.decoders), set(kind.extensions))
        # What each kind decoded before profiles, and how much of a file it reads.
        screenshots, attachments = kinds
        self.assertEqual(
            sorted(PROFILES[screenshots.profile].decoders), [".jpeg", ".jpg", ".png"]
        )
        self.assertEqual(
            sorted(PROFILES[attachments.profile].decoders),
            sorted(
                (".heic", ".heif", ".jpeg", ".jpg", ".png")
                + (".gif", ".webp", ".tiff", ".tif", ".bmp")
            ),
        )
        self.assertEqual(screenshots.derive_bound, 20 * 1024 * 1024)
        self.assertEqual(attachments.derive_bound, 50 * 1024 * 1024)

    def test_a_profile_makes_only_widths_media_objects_has_columns_for(self):
        from src.media.derive import COLUMNS, THUMBNAIL_COLUMNS

        self.assertEqual(
            THUMBNAIL_COLUMNS, {360: "thumbnail_w360", 1280: "thumbnail_w1280"}
        )
        self.assertLessEqual(set(THUMBNAIL_COLUMNS.values()), set(COLUMNS))
        for profile in PROFILES.values():
            with self.subTest(profile.name):
                # A new width needs a new column (additive), not just a profile.
                self.assertEqual(profile.widths, tuple(THUMBNAIL_COLUMNS))
                self.assertEqual(
                    (profile.output, profile.content_type), ("webp", "image/webp")
                )

    def test_a_kind_cannot_name_an_unknown_profile(self):
        from src.media.kinds import MediaKind

        with self.assertRaises(ValueError):
            MediaKind("k", "r", "c", (".png",), 10, profile="video")

    def test_every_decoder_has_a_check_image_that_it_alone_reads(self):
        from src.media.decoder_checks import check_image

        every = decoders_of(PROFILES.values())
        for decoder in every:
            with self.subTest(decoder):
                image = check_image(decoder)
                self.assertIsNotNone(image)
                self.assertEqual(sorted(make(image, (decoder,))), [360, 1280])
                others = tuple(d for d in every if d != decoder)
                self.assertEqual(self.refusal(image, others), "unreadable")

    def test_pillow_has_webp(self):
        self.assertTrue(features.check("webp"))

    def test_two_webp_sizes_of_a_screen(self):
        made = make(screen(), JPEG)
        self.assertEqual(sorted(made), [360, 1280])
        large, small = opened(made[1280]), opened(made[360])
        self.assertEqual((large.format, large.size), ("WEBP", (1280, 800)))
        self.assertEqual((small.format, small.size), ("WEBP", (360, 225)))

    def test_a_narrow_image_is_not_enlarged(self):
        made = make(encoded(Image.new("RGB", (300, 120)), "PNG"), ("PNG",))
        self.assertEqual(opened(made[360]).size, (300, 120))
        self.assertEqual(opened(made[1280]).size, (300, 120))
        between = make(screen(800, 400), JPEG)
        self.assertEqual(opened(between[1280]).size, (800, 400))
        self.assertEqual(opened(between[360]).size, (360, 180))

    def test_modes_become_rgb_or_keep_their_alpha(self):
        rgba = encoded(Image.new("RGBA", (500, 500), (1, 2, 3, 100)), "PNG")
        self.assertEqual(opened(make(rgba, ("PNG",))[360]).mode, "RGBA")
        cases = {
            "L": encoded(Image.new("L", (500, 500)), "PNG"),
            "P": encoded(Image.new("P", (500, 500)), "PNG"),
            "I;16": encoded(Image.new("I;16", (500, 500)), "PNG"),
            "CMYK": encoded(Image.new("CMYK", (500, 500)), "JPEG"),
        }
        for mode, body in cases.items():
            with self.subTest(mode=mode):
                made = make(body, ("JPEG", "PNG"))
                self.assertEqual(opened(made[360]).mode, "RGB")

    def test_metadata_is_dropped_and_orientation_applied(self):
        exif = Image.Exif()
        exif[0x0112] = 6  # rotated: shown 200 wide and 400 tall
        exif[0x010F] = "A camera maker"
        body = encoded(Image.new("RGB", (400, 200)), "JPEG", exif=exif.tobytes())
        made = opened(make(body, JPEG)[360])
        self.assertEqual(made.size, (200, 400))
        self.assertEqual(dict(made.getexif()), {})
        self.assertNotIn("exif", made.info)

    def test_a_heic_becomes_two_webp_sizes(self):
        image = Image.linear_gradient("L").resize((4032, 3024)).convert("RGB")
        body = encoded(image, "HEIF", quality=60)
        self.assertEqual(body[4:12], b"ftypheic")
        made = make(body, HEIF)
        large, small = opened(made[1280]), opened(made[360])
        self.assertEqual((large.format, large.size), ("WEBP", (1280, 960)))
        self.assertEqual((small.format, small.size), ("WEBP", (360, 270)))
        # Read by the HEIF decoder alone, and it alone reads it.
        self.assertEqual(self.refusal(body, ("JPEG", "PNG")), "unreadable")
        self.assertEqual(self.refusal(screen(), HEIF), "unreadable")

    def test_a_heic_rotation_is_applied_exactly_once(self):
        # Stored 400 x 200 with a blue corner; pillow-heif writes EXIF orientation
        # 6 as an `irot` box and keeps the EXIF too, as an iPhone does.
        image = Image.new("RGB", (400, 200), (200, 0, 0))
        image.paste((0, 0, 200), (0, 0, 40, 40))
        exif = Image.Exif()
        exif[0x0112] = 6
        exif[0x010F] = "A camera maker"
        body = encoded(image, "HEIF", exif=exif.tobytes(), quality=90)
        self.assertIn(b"irot", body)
        jpeg = encoded(image, "JPEG", exif=exif.tobytes(), quality=90)
        for name, made in (
            ("heic", opened(make(body, HEIF)[1280])),
            ("jpeg", opened(make(jpeg, JPEG)[1280])),
        ):
            with self.subTest(name):
                # Shown 200 x 400, the blue corner turned to the top right.
                self.assertEqual(made.size, (200, 400))
                red, _, blue = made.convert("RGB").getpixel((190, 10))
                self.assertGreater(blue, 150)
                self.assertLess(red, 60)
                red, _, blue = made.convert("RGB").getpixel((10, 10))
                self.assertGreater(red, 150)
                self.assertLess(blue, 60)
                self.assertEqual(dict(made.getexif()), {})
                self.assertNotIn("exif", made.info)
                self.assertNotIn("xmp", made.info)

    def test_only_the_decoders_of_the_kind_are_used(self):
        png = encoded(Image.new("RGB", (50, 50)), "PNG")
        self.assertEqual(self.refusal(png, JPEG), "unreadable")
        self.assertEqual(sorted(make(png, ("JPEG", "PNG"))), [360, 1280])
        self.assertEqual(self.refusal(png, ()), "unsupported")
        self.assertEqual(PROFILES["screenshot"].formats, ("JPEG", "PNG"))
        self.assertEqual(
            PROFILES["image"].formats,
            ("BMP", "GIF", "HEIF", "JPEG", "PNG", "TIFF", "WEBP"),
        )

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
        tall = encoded(Image.new("L", (1024, MAX_SIDE)), "PNG")
        made = opened(make(tall, ("PNG",))[1280])
        self.assertEqual(made.size, (round(1024 * MAX_HEIGHT / MAX_SIDE), MAX_HEIGHT))

    def test_extreme_shapes_are_refused_before_decoding(self):
        # Pillow keeps a pointer and an allocation per row: thin is expensive.
        cases = {
            "a side too long": (MAX_SIDE + 1, 1200),
            "too thin": (MAX_SIDE, MAX_SIDE // MAX_ASPECT - 1),
            "the reviewer's 1 x 40M": (1, 40_000_000),
        }
        for name, (width, height) in cases.items():
            for size in ((width, height), (height, width)):
                with self.subTest(name, size=size):
                    self.assertEqual(
                        self.refusal(forged_png(*size), ("PNG",)), "too_large"
                    )
        thinnest = Image.new("L", (MAX_SIDE, MAX_SIDE // MAX_ASPECT))
        made = make(encoded(thinnest, "PNG"), ("PNG",))
        self.assertEqual(opened(made[1280]).size, (1280, 80))

    def test_a_rotated_jpeg_is_drafted_for_its_shown_width(self):
        exif = Image.Exif()
        exif[0x0112] = 6
        body = encoded(Image.new("RGB", (8000, 1000)), "JPEG", exif=exif.tobytes())
        made = make(body, JPEG)
        self.assertEqual(opened(made[1280]).size, (1000, 8000))
        self.assertEqual(opened(made[360]).size, (360, 2880))

    def test_running_out_of_memory_is_too_large(self):
        with patch("src.media.decoder._prepared", side_effect=MemoryError):
            self.assertEqual(self.refusal(screen(), JPEG), "too_large")

    def test_only_the_first_frame_of_an_animation(self):
        frames = [Image.new("RGB", (64, 64), (i, 0, 0)) for i in range(0, 250, 50)]
        body = encoded(frames[0], "GIF", save_all=True, append_images=frames[1:])
        made = opened(make(body, ("GIF",))[360])
        self.assertEqual((made.size, getattr(made, "n_frames", 1)), ((64, 64), 1))


if __name__ == "__main__":
    unittest.main()
