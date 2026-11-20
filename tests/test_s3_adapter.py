import unittest
from urllib.parse import parse_qs, urlsplit

from botocore.exceptions import ClientError
from botocore.stub import Stubber

from src.adapters.s3 import S3ObjectStore


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
