import io
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from src.adapters.s3 import S3ObjectStore, from_environment
from src.core.config import MediaConfig
from src.core.media import ListedObject, Listing, ObjectTooLarge, StoredObject
from tests.fakes import FakeObjectStore


def streamed(data: bytes) -> StreamingBody:
    return StreamingBody(io.BytesIO(data), len(data))


class S3AdapterTests(unittest.TestCase):
    def setUp(self):
        self.store = S3ObjectStore(
            "http://internal.test:3900",
            "https://s3.example.org",
            "garage",
            "media",
            "fake",
            "fake",
        )

    def test_head(self):
        params = {"Bucket": "media", "Key": "screen/screenshots/dir/a.jpg"}
        self.assertEqual(
            self.store.client.meta.endpoint_url, "http://internal.test:3900"
        )
        with Stubber(self.store.client) as stub:
            stub.add_response("head_object", {"ContentLength": 12}, params)
            self.assertEqual(self.store.head(params["Key"]), 12)
            for code in ("404", "NoSuchKey", "NotFound"):
                stub.add_client_error(
                    "head_object", code, http_status_code=404, expected_params=params
                )
                self.assertIsNone(self.store.head(params["Key"]))
            stub.add_client_error(
                "head_object", "403", http_status_code=403, expected_params=params
            )
            with self.assertRaises(ClientError):
                self.store.head(params["Key"])
            stub.assert_no_pending_responses()

    def test_get_and_put(self):
        name = "screen/screenshots.w360/dir/a.webp"
        params = {"Bucket": "media", "Key": name}
        with Stubber(self.store.client) as stub:
            stub.add_response(
                "put_object",
                {"ETag": '"abc"'},
                {**params, "Body": b"thumb", "ContentType": "image/webp"},
            )
            self.store.put(name, b"thumb", "image/webp")
            stub.add_response(
                "get_object",
                {
                    "Body": streamed(b"thumb"),
                    "ContentLength": 5,
                    "ContentType": "image/webp",
                    "ETag": '"abc"',
                },
                params,
            )
            self.assertEqual(
                self.store.get(name, 5), StoredObject(b"thumb", "image/webp", '"abc"')
            )
            for code in ("404", "NoSuchKey", "NotFound"):
                stub.add_client_error(
                    "get_object", code, http_status_code=404, expected_params=params
                )
                self.assertIsNone(self.store.get(name, 5))
            stub.add_client_error(
                "get_object", "403", http_status_code=403, expected_params=params
            )
            with self.assertRaises(ClientError):
                self.store.get(name, 5)
            # A declared length past the limit is refused before reading.
            stub.add_response(
                "get_object", {"Body": streamed(b"thumb"), "ContentLength": 5}, params
            )
            with self.assertRaises(ObjectTooLarge):
                self.store.get(name, 4)
            # So is a body longer than it declared.
            stub.add_response(
                "get_object",
                {"Body": streamed(b"thumbnail"), "ContentLength": 3},
                params,
            )
            with self.assertRaises(ObjectTooLarge):
                self.store.get(name, 4)
            stub.assert_no_pending_responses()

    def test_list_objects(self):
        modified = datetime(2026, 11, 20, tzinfo=timezone.utc)
        prefix = "screen/screenshots/"
        with Stubber(self.store.client) as stub:
            stub.add_response(
                "list_objects_v2",
                {
                    "Contents": [
                        {"Key": f"{prefix}a.jpg", "Size": 3, "LastModified": modified}
                    ],
                    "IsTruncated": True,
                },
                {"Bucket": "media", "Prefix": prefix, "MaxKeys": 1000},
            )
            self.assertEqual(
                self.store.list_objects(prefix, "", 5000),
                Listing([ListedObject(f"{prefix}a.jpg", 3, modified)], True),
            )
            stub.add_response(
                "list_objects_v2",
                {},
                {
                    "Bucket": "media",
                    "Prefix": prefix,
                    "MaxKeys": 10,
                    "StartAfter": f"{prefix}a.jpg",
                },
            )
            self.assertEqual(
                self.store.list_objects(prefix, f"{prefix}a.jpg", 10),
                Listing([], False),
            )
            stub.assert_no_pending_responses()

    def test_the_key_comes_from_the_environment(self):
        config = MediaConfig(endpoint="http://internal.test:3900", bucket="media")
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(from_environment(config))
        keys = {
            "MIROIR_MEDIA_ACCESS_KEY_ID": "fake",
            "MIROIR_MEDIA_SECRET_ACCESS_KEY": "fake",
        }
        with patch.dict(os.environ, keys, clear=True):
            store = from_environment(config)
        self.assertEqual(store.bucket, "media")
        self.assertEqual(store.link_client.meta.endpoint_url, config.endpoint)

    def test_the_test_store_behaves_like_the_adapter(self):
        store = FakeObjectStore()
        self.assertIsNone(store.get("a/b.webp", 10))
        store.put("a/b.webp", b"thumb", "image/webp")
        self.assertEqual(store.head("a/b.webp"), 5)
        self.assertEqual(store.get("a/b.webp", 5).body, b"thumb")
        with self.assertRaises(ObjectTooLarge):
            store.get("a/b.webp", 4)

    def test_both_clients_bound_storage_attempts(self):
        for client in (self.store.client, self.store.link_client):
            config = client.meta.config
            self.assertEqual(config.connect_timeout, 5)
            self.assertEqual(config.read_timeout, 10)
            self.assertEqual(config.retries["total_max_attempts"], 3)

    def test_presigned_links(self):
        key = "screen/screenshots/dir/a b.jpg"
        for link, ttl, headers in (
            (self.store.presign_put(key, "image/jpeg", 99), "99", "content-type;host"),
            (self.store.presign_get(key, 123), "123", "host"),
        ):
            url = urlsplit(link)
            self.assertEqual(url.scheme, "https")
            self.assertEqual(url.netloc, "s3.example.org")
            self.assertEqual(url.path, "/media/screen/screenshots/dir/a%20b.jpg")
            query = parse_qs(url.query)
            self.assertIn("X-Amz-Signature", query)
            self.assertEqual(query["X-Amz-Expires"], [ttl])
            self.assertEqual(query["X-Amz-SignedHeaders"], [headers])
