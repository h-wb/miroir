import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.core import media
from src.core.config import MediaConfig, load_miroir
from src.core.operations import OperationError
from tests.fakes import FakeObjectStore


class MediaTests(unittest.TestCase):
    def setUp(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "miroir.json"
            path.write_text(
                '{"schema_version":1,"id":"test","connections":['
                '{"id":"screen","integration":"screenmemory","settings":{}},'
                '{"id":"browser","integration":"browser_history","settings":{}},'
                '{"id":"messages","integration":"apple_messages","settings":{}}]}'
            )
            self.screen, self.browser, self.messages = load_miroir(path).connections
        self.config = MediaConfig(
            endpoint="http://internal.test", bucket="media", prefix="site"
        )
        self.store = FakeObjectStore()
        self.kind = "screenshots"

    def plan(self, objects):
        return media.request_uploads(
            self.config, self.store, self.screen, self.kind, objects
        )

    def test_apple_messages_attachments(self):
        (kind,) = self.messages.integration.media
        self.assertEqual(
            (kind.name, kind.resource, kind.column),
            ("attachments", "attachments", "media_key"),
        )
        self.assertEqual(kind.max_bytes, 5000 * 1024 * 1024)
        self.assertTrue(set(kind.extensions) <= media.CONTENT_TYPES.keys())
        mov, pdf, unsupported, oversize = media.request_uploads(
            self.config,
            self.store,
            self.messages,
            kind.name,
            [
                {"key": "clip.mov", "size": 3},
                {"key": "print.pdf", "size": kind.max_bytes},
                {"key": "unsupported.exe", "size": 3},
                {"key": "large.pdf", "size": kind.max_bytes + 1},
            ],
        )
        self.assertEqual(
            (mov["status"], mov["content_type"]), ("upload", "video/quicktime")
        )
        self.assertEqual(
            (pdf["status"], pdf["content_type"]), ("upload", "application/pdf")
        )
        self.assertEqual(
            (unsupported["status"], unsupported["reason"]), ("rejected", "bad_type")
        )
        self.assertEqual(
            (oversize["status"], oversize["reason"]), ("rejected", "bad_size")
        )

    def test_keys(self):
        for key in (
            "a",
            "x" * 512,
            "file.jpg",
            "files/.hidden/foo ..~.jpg",
            "caf\u00e9.jpg",
        ):
            with self.subTest(key=key):
                self.assertTrue(media.valid_key(key))
        for key in (
            None,
            123,
            "",
            "x" * 513,
            "/abs",
            "trailing/",
            "a//b",
            "a/./b",
            "a/../b",
            "a\\b",
            "a\n",
            "a\x7f",
            "a\x85",
        ):
            with self.subTest(key=key):
                self.assertFalse(media.valid_key(key))

    def test_object_name(self):
        self.assertEqual(
            media.object_name("a/b", "screen", self.kind, "c/d.png"),
            "a/b/screen/screenshots/c/d.png",
        )
        self.assertEqual(
            media.object_name("", "screen", self.kind, "c/d.png"),
            "screen/screenshots/c/d.png",
        )

    def test_plan_in_order(self):
        self.store.objects = {
            "site/screen/screenshots/dir/a.JPG": 1,
            "site/screen/screenshots/b.PNG": 3,
        }
        objects = [
            ("dir/a.JPG", 1),
            ("b.PNG", 2),
            ("a.heic", 2),
            ("../private.jpg", 1),
            ("a.exe", 1),
            ("a.jpg", True),
            ("a.jpg", 0),
            ("a.jpg", 20 * 1024 * 1024 + 1),
            ("a.jpg", 1.0),
            (123, 1),
        ]
        result = self.plan([{"key": key, "size": size} for key, size in objects])
        self.assertEqual(
            result[:3],
            [
                {"key": "dir/a.JPG", "status": "present"},
                {
                    "key": "b.PNG",
                    "status": "upload",
                    "url": "https://objects.test/site/screen/screenshots/b.PNG?put=1&type=image/png&expires=900",
                    "content_type": "image/png",
                },
                {
                    "key": "a.heic",
                    "status": "upload",
                    "url": "https://objects.test/site/screen/screenshots/a.heic?put=1&type=image/heic&expires=900",
                    "content_type": "image/heic",
                },
            ],
        )
        self.assertEqual(
            result[3:],
            [
                {"key": key, "status": "rejected", "reason": reason}
                for key, reason in [
                    ("../private.jpg", "bad_key"),
                    ("a.exe", "bad_type"),
                    *(("a.jpg", "bad_size"),) * 4,
                    (None, "bad_key"),
                ]
            ],
        )

    def test_confirm_and_view(self):
        self.store.objects["site/screen/screenshots/a.JPG"] = 2
        objects = [("a.JPG", 2), ("a.JPG", 3), ("b.png", 3), ("../private.jpg", 1)]
        results = media.confirm_uploads(
            self.config,
            self.store,
            self.screen,
            self.kind,
            [{"key": key, "size": size} for key, size in objects],
        )
        self.assertEqual(
            results,
            [
                {"key": key, "status": status}
                for (key, _), status in zip(
                    objects,
                    ("stored", "size_mismatch", "missing", "rejected"),
                    strict=True,
                )
            ],
        )
        with patch.object(
            self.store, "head", side_effect=AssertionError("head called")
        ):
            self.assertEqual(
                media.view_url(
                    self.config, self.store, self.screen, self.kind, "dir/no.png"
                ),
                "https://objects.test/site/screen/screenshots/dir/no.png?get=1&expires=300",
            )

    def test_limits_and_refusals(self):
        item = {"key": "private.jpg", "size": 1}
        self.assertEqual(len(self.plan([item] * 500)), 500)
        for operation in (media.request_uploads, media.confirm_uploads):
            for args in (
                (None, self.store, self.screen, self.kind, [item]),
                (self.config, None, self.screen, self.kind, [item]),
                (self.config, self.store, self.browser, self.kind, [item]),
                (self.config, self.store, self.screen, "private.jpg", [item]),
                (self.config, self.store, self.screen, self.kind, [item] * 501),
                (self.config, self.store, self.screen, self.kind, item),
                (
                    self.config,
                    self.store,
                    self.screen,
                    self.kind,
                    [{"key": "private.jpg"}],
                ),
            ):
                with self.assertRaises(OperationError) as ctx:
                    operation(*args)
                self.assertNotIn("private.jpg", str(ctx.exception))
        with self.assertRaises(OperationError) as ctx:
            media.view_url(
                self.config, self.store, self.screen, self.kind, "../private.jpg"
            )
        self.assertNotIn("private.jpg", str(ctx.exception))
