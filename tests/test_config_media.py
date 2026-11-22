import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.core.config import ConfigurationError, load_miroir


class MediaConfigTests(unittest.TestCase):
    def load(self, **sections):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "miroir.json"
            path.write_text(json.dumps({"schema_version": 1, "id": "test", **sections}))
            return load_miroir(path)

    def test_defaults(self):
        self.assertIsNone(self.load().media)
        config = self.load(
            media={"endpoint": "http://garage.storage.svc:3900", "bucket": "media"}
        ).media
        self.assertEqual(
            config.model_dump(),
            {
                "endpoint": "http://garage.storage.svc:3900",
                "link_endpoint": "http://garage.storage.svc:3900",
                "bucket": "media",
                "region": "garage",
                "prefix": "",
                "upload_ttl_seconds": 900,
                "view_ttl_seconds": 300,
                "derive_delay_seconds": 300,
                "derive_batch": 500,
            },
        )

    def test_overrides(self):
        fields = {
            "endpoint": "http://garage.storage.svc:3900",
            "link_endpoint": "https://s3.example.org",
            "bucket": "media",
            "region": "other",
            "prefix": "site_1/camera.old",
            "upload_ttl_seconds": 3600,
            "view_ttl_seconds": 30,
            "derive_delay_seconds": 30,
            "derive_batch": 5000,
        }
        self.assertEqual(self.load(media=fields).media.model_dump(), fields)

    def test_invalid(self):
        base = {"endpoint": "http://garage.storage.svc:3900", "bucket": "media"}
        for overrides in (
            {"endpoint": "ftp://storage.test"},
            {"endpoint": "http://"},
            {"link_endpoint": "file:///local"},
            {"bucket": ""},
            {"prefix": "/leading"},
            {"prefix": "trailing/"},
            {"prefix": "a/../b"},
            {"prefix": "a b"},
            {"upload_ttl_seconds": 59},
            {"upload_ttl_seconds": True},
            {"view_ttl_seconds": 3601},
            {"derive_delay_seconds": 29},
            {"derive_delay_seconds": 3601},
            {"derive_batch": 0},
            {"derive_batch": 5001},
            {"derive_batch": "500"},
            {"unexpected": "field"},
        ):
            with (
                self.subTest(overrides=overrides),
                self.assertRaises(ConfigurationError),
            ):
                self.load(media={**base, **overrides})
