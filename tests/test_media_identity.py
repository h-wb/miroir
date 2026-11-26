"""Thumbnails and `media_objects` rows, byte for byte as before profiles.

`fixtures/media_identity.json` was written by this test (`--write`) on the code
before media moved into `src/media` and decoders and widths became profiles
(package split, step 4). A screenshot and an attachment of every type a kind
names must give the same reads, thumbnails and rows. Every input is generated.

The fixture also holds each input's digest. If those differ, the image
libraries changed (a Pillow, libjpeg, libwebp or libheif upgrade), not the
code: write it again from a commit whose output is known good.
"""

import hashlib
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image
from pillow_heif import register_heif_opener

from src.control.runner import request_derive
from tests.test_derive import DeriveFixture, encoded, jpeg

FIXTURE = Path(__file__).parent / "fixtures" / "media_identity.json"
STORED_AT = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

register_heif_opener()


def inputs() -> dict[str, dict[str, bytes]]:
    """Files by kind: what each kind decodes, refuses and does not read."""
    exif = Image.Exif()
    exif[0x0112] = 6  # stored 400 x 200, shown 200 x 400
    photo = Image.linear_gradient("L").resize((2016, 1512)).convert("RGB")
    frames = [Image.new("RGB", (64, 48), (i, 0, 0)) for i in (250, 0, 120)]
    gradient = Image.linear_gradient("L").resize((1600, 1200))
    rgba = Image.merge("RGBA", (gradient, gradient.rotate(90), gradient, gradient))
    return {
        "screenshots": {
            "s/screen.jpeg": jpeg(2880, 1800),
            "s/screen.jpg": jpeg(1440, 900),
            "s/narrow.png": encoded(Image.new("RGB", (300, 120), (9, 9, 9)), "PNG"),
            "s/alpha.png": encoded(rgba, "PNG"),
            # Accepted, not decoded: a HEIC screenshot stays unsupported.
            "s/screen.heic": encoded(photo, "HEIF", quality=60),
            "s/named.png": jpeg(),
            # Over the kind's 20 MB: too large, not read.
            "s/huge.png": b"\x89PNG" + b"\x00" * (20 * 1024 * 1024 - 3),
        },
        "attachments": {
            "a/photo.heic": encoded(photo, "HEIF", quality=60),
            "a/turned.heif": encoded(
                Image.new("RGB", (400, 200), (0, 0, 200)), "HEIF", exif=exif.tobytes()
            ),
            "a/rotated.jpeg": encoded(
                Image.new("RGB", (400, 200), (200, 0, 0)), "JPEG", exif=exif.tobytes()
            ),
            "a/photo.jpg": jpeg(4032, 3024),
            "a/plain.png": encoded(rgba, "PNG"),
            "a/animated.gif": encoded(
                frames[0], "GIF", save_all=True, append_images=frames[1:]
            ),
            "a/still.webp": encoded(photo.resize((1600, 1200)), "WEBP", quality=90),
            "a/scan.tiff": encoded(photo.resize((1500, 1000)), "TIFF"),
            "a/scan.tif": encoded(gradient.resize((900, 600)), "TIFF"),
            "a/icon.bmp": encoded(Image.new("RGB", (1400, 700), (1, 2, 3)), "BMP"),
            "a/clip.mov": b"\x00\x00\x00\x14ftypqt  synthetic video",
            "a/letter.pdf": b"%PDF-1.4 synthetic",
            "a/named.png": jpeg(),
            # Over the kind's 50 MB read bound, under its 5,000 MB.
            "a/big.jpeg": b"\xff" * (50 * 1024 * 1024 + 1),
        },
    }


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


class MediaIdentityTests(DeriveFixture):
    def observed(self) -> dict[str, Any]:
        files = inputs()
        connections = {"screenshots": "screen", "attachments": "messages"}
        for kind, kind_files in files.items():
            connection = connections[kind]
            for key, body in kind_files.items():
                self.objects.add(f"site/{connection}/{kind}/{key}", body)
            self.store.media_queue.add_pending_media(
                "test",
                connection,
                kind,
                [(key, len(body), STORED_AT) for key, body in kind_files.items()],
            )
        originals = set(self.objects.bodies)
        reads: list[list[Any]] = []
        get = self.objects.get

        def recorded(name: str, max_bytes: int) -> Any:
            reads.append([name, max_bytes])
            return get(name, max_bytes)

        self.objects.get = recorded  # type: ignore[method-assign]
        details = {}
        for kind, connection in connections.items():
            operation_id = request_derive(
                self.miroir(),
                connection,
                store=self.store,
                origin="request",
                submit=lambda operation: None,
                join=False,
            )
            assert operation_id is not None
            self.derive(operation_id)
            details[kind] = self.detail(operation_id)
        rows = {}
        for index, kind in enumerate(connections):
            for key, row in self.rows_of(index).items():
                # When it was written, and dlt's own: different on every run.
                for column in ("derived_at", "_dlt_id", "_dlt_load_id"):
                    row.pop(column)
                rows[f"{kind}/{key}"] = {k: plain(v) for k, v in sorted(row.items())}
        return {
            "inputs": {
                f"{kind}/{key}": digest(body)
                for kind, kind_files in files.items()
                for key, body in kind_files.items()
            },
            "reads": reads,
            "thumbnails": {
                name: [digest(body), content_type]
                for name, (body, content_type) in sorted(self.objects.bodies.items())
                if name not in originals
            },
            "rows": rows,
            "details": details,
        }

    def rows_of(self, index: int) -> dict[str, dict[str, Any]]:
        import duckdb

        dataset = self.miroir().connections[index].binding.dataset_name
        with duckdb.connect(str(self.warehouse)) as db:
            cursor = db.execute(f"SELECT * FROM {dataset}.media_objects")
            names = [c[0] for c in cursor.description]
            return {row[2]: dict(zip(names, row)) for row in cursor.fetchall()}

    def test_thumbnails_and_rows_are_as_before(self):
        observed = self.observed()
        if "--write" in sys.argv:
            FIXTURE.parent.mkdir(exist_ok=True)
            FIXTURE.write_text(json.dumps(observed, indent=1, sort_keys=True) + "\n")
        expected = json.loads(FIXTURE.read_text())
        self.maxDiff = None
        self.assertEqual(observed["inputs"], expected["inputs"], "inputs changed")
        self.assertEqual(observed["reads"], expected["reads"])
        self.assertEqual(observed["thumbnails"], expected["thumbnails"])
        self.assertEqual(observed["rows"], expected["rows"])
        self.assertEqual(observed["details"], expected["details"])
        # Both kinds made thumbnails, refused some files and did not read others.
        statuses = {row["thumbnail_status"] for row in observed["rows"].values()}
        self.assertEqual(statuses, {"made", "unsupported", "too_large", "unreadable"})


if __name__ == "__main__":
    unittest.main(argv=[a for a in sys.argv if a != "--write"])
