import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.core.operations import MetadataStore
from src.core.service import InlineExecutor, MiroirService
from src.web.app import create_app
from tests.fakes import FakeObjectStore

ADMIN = "test-admin-token-0123456"
UPLOAD = "/api/connections/screen/media/uploads"
CONFIRM = "/api/connections/screen/media/confirm"
VIEW = "/media/screen/screenshots/dir/a.jpg"
OBJECT = "screen/screenshots/dir/a.jpg"


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


class WebMediaTests(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        config = root / "miroir.json"
        config.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "test",
                    "media": {"endpoint": "https://s3.example.org", "bucket": "media"},
                    "connections": [
                        {"id": "screen", "integration": "screenmemory", "settings": {}},
                        {
                            "id": "browser",
                            "integration": "browser_history",
                            "settings": {},
                        },
                    ],
                }
            )
        )
        store = MetadataStore(f"sqlite:///{root / 'meta.sqlite'}")
        self.addCleanup(store.engine.dispose)
        store.migrate()
        self.objects = FakeObjectStore()
        self.service = MiroirService(
            config,
            store,
            root / "artifacts",
            InlineExecutor(self.fail),
            object_store=self.objects,
        )
        self.client = TestClient(
            create_app(self.service, ADMIN), follow_redirects=False
        )
        self.screen = bearer(self.service.create_delivery_token("screen"))
        self.browser = bearer(self.service.create_delivery_token("browser"))
        self.body = {
            "kind": "screenshots",
            "objects": [{"key": "dir/a.jpg", "size": 2}],
        }

    def test_deliver_and_view(self):
        upload = self.client.post(UPLOAD, json=self.body, headers=self.screen)
        self.assertEqual(upload.status_code, 200)
        self.assertEqual(
            upload.json(),
            {
                "objects": [
                    {
                        "key": "dir/a.jpg",
                        "status": "upload",
                        "content_type": "image/jpeg",
                        "url": f"https://objects.test/{OBJECT}?put=1&type=image/jpeg&expires=900",
                    }
                ]
            },
        )
        self.objects.objects[OBJECT] = 2
        confirm = self.client.post(CONFIRM, json=self.body, headers=self.screen)
        self.assertEqual(confirm.status_code, 200)
        self.assertEqual(
            confirm.json(), {"objects": [{"key": "dir/a.jpg", "status": "stored"}]}
        )
        self.assertEqual(self.client.get(VIEW).status_code, 303)
        self.assertEqual(self.client.get(VIEW, headers=self.screen).status_code, 303)
        self.assertEqual(
            self.client.post("/login", data={"token": ADMIN}).status_code, 303
        )
        view = self.client.get(VIEW)
        self.assertEqual(view.status_code, 302)
        self.assertEqual(
            view.headers["Location"], f"https://objects.test/{OBJECT}?get=1&expires=300"
        )
        self.assertEqual(view.headers["Cache-Control"], "private, no-store")

    def test_requires_delivery_auth_before_body(self):
        oversized = b"x" * (1024 * 1024 + 1)
        for url in (UPLOAD, CONFIRM):
            for headers in ({}, self.browser, bearer(ADMIN)):
                response = self.client.post(url, content=oversized, headers=headers)
                self.assertEqual(response.status_code, 401)

    def test_storage_failure_is_a_refusal_and_stops_the_batch(self):
        body = {
            **self.body,
            "objects": [*self.body["objects"], {"key": "dir/b.jpg", "size": 2}],
        }
        for url in (UPLOAD, CONFIRM):
            with (
                self.subTest(url=url),
                patch.object(
                    self.objects,
                    "head",
                    side_effect=OSError("storage detail for dir/a.jpg"),
                ) as head,
            ):
                response = self.client.post(url, json=body, headers=self.screen)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(
                    response.json(), {"error": "Media storage is unavailable"}
                )
                head.assert_called_once_with(OBJECT)

    def test_size_and_shape(self):
        limit = 1024 * 1024
        body = json.dumps(self.body).encode()
        response = self.client.post(
            UPLOAD, content=body.ljust(limit), headers=self.screen
        )
        self.assertEqual(response.status_code, 200)
        response = self.client.post(
            UPLOAD, content=iter([body.ljust(limit), b" "]), headers=self.screen
        )
        self.assertEqual(response.status_code, 413)
        for bad_body in (
            b"{",
            b"[]",
            b'{"kind":0,"objects":[]}',
            b'{"kind":"screenshots","objects":{}}',
        ):
            self.assertEqual(
                self.client.post(
                    UPLOAD, content=bad_body, headers=self.screen
                ).status_code,
                400,
            )

    def test_view_refusals(self):
        for path in (
            "/media/unknown/screenshots/a.jpg",
            "/media/screen/other/a.jpg",
            "/media/screen/screenshots/dir/%2e%2e/a.jpg",
        ):
            self.assertEqual(
                self.client.get(path, headers=bearer(ADMIN)).status_code, 404
            )

    def test_unconfigured_store_and_connector_without_media(self):
        for connection, headers in (("browser", self.browser), ("screen", self.screen)):
            if connection == "screen":
                self.service.object_store = None
            for url in (UPLOAD, CONFIRM):
                response = self.client.post(
                    url.replace("/screen/", f"/{connection}/"),
                    json=self.body,
                    headers=headers,
                )
                self.assertEqual(response.status_code, 400)
            view = self.client.get(
                VIEW.replace("/screen/", f"/{connection}/"), headers=bearer(ADMIN)
            )
            self.assertEqual(view.status_code, 404)


class WebMediaBytesTests(WebMediaTests):
    """Thumbnails and image originals copied through the server (decision 040)."""

    def setUp(self):
        super().setUp()
        config = json.loads(self.service.config_path.read_text())
        config["connections"].append(
            {"id": "phone", "integration": "apple_messages", "settings": {}}
        )
        self.service.config_path.write_text(json.dumps(config))
        self.admin = bearer(ADMIN)
        self.objects.add("screen/screenshots.w360/dir/a.jpg.webp", b"small", "x/y")
        self.objects.add("screen/screenshots.w1280/dir/a.jpg.webp", b"larger")
        self.objects.add(OBJECT, b"original", "text/html")

    def test_thumbnails_are_copied_with_an_immutable_private_cache(self):
        for size, body in (("360", b"small"), ("1280", b"larger")):
            with self.subTest(size=size):
                response = self.client.get(f"{VIEW}?size={size}", headers=self.admin)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, body)
                # Its own type, whatever the bucket says; browsers must not sniff.
                self.assertEqual(response.headers["content-type"], "image/webp")
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertEqual(
                    response.headers["cache-control"],
                    "private, max-age=31536000, immutable",
                )
                tag = response.headers["etag"]
                self.assertEqual(tag, f'"{len(body)}"')
                for header in (tag, f"W/{tag}", f'"other", {tag}'):
                    again = self.client.get(
                        f"{VIEW}?size={size}",
                        headers={**self.admin, "If-None-Match": header},
                    )
                    self.assertEqual((again.status_code, again.content), (304, b""))
                    self.assertEqual(again.headers["etag"], tag)
                changed = self.client.get(
                    f"{VIEW}?size={size}",
                    headers={**self.admin, "If-None-Match": '"other"'},
                )
                self.assertEqual(changed.status_code, 200)
        self.assertEqual(
            self.objects.gets,
            ["screen/screenshots.w360/dir/a.jpg.webp"] * 5
            + ["screen/screenshots.w1280/dir/a.jpg.webp"] * 5,
        )

    def test_attachment_thumbnails_are_copied_as_screenshots_are(self):
        self.objects.add("phone/attachments.w360/m/photo.jpeg.webp", b"small")
        self.objects.add("phone/attachments.w1280/m/photo.jpeg.webp", b"larger")
        for size, body in (("360", b"small"), ("1280", b"larger")):
            with self.subTest(size=size):
                response = self.client.get(
                    f"/media/phone/attachments/m/photo.jpeg?size={size}",
                    headers=self.admin,
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.content, body)
                self.assertEqual(response.headers["content-type"], "image/webp")
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertEqual(
                    response.headers["cache-control"],
                    "private, max-age=31536000, immutable",
                )
                self.assertEqual(response.headers["etag"], f'"{len(body)}"')
        self.assertEqual(
            self.objects.gets,
            [
                "phone/attachments.w360/m/photo.jpeg.webp",
                "phone/attachments.w1280/m/photo.jpeg.webp",
            ],
        )

    def test_a_missing_thumbnail_is_404_and_bad_requests_are_refused(self):
        for url, status in (
            ("/media/screen/screenshots/dir/b.jpg?size=360", 404),
            # An attachment the worker made no thumbnail of (a video, say).
            ("/media/phone/attachments/clip.mov?size=360", 404),
            # The derived kind is not a kind: neither read nor written by name.
            ("/media/screen/screenshots.w360/dir/a.jpg.webp", 404),
            ("/media/screen/screenshots.w360/dir/a.jpg.webp?size=360", 404),
            ("/media/screen/screenshots/dir/%2e%2e/a.jpg?size=360", 404),
            ("/media/screen/screenshots/dir/a.txt?size=360", 404),
            ("/media/unknown/screenshots/dir/a.jpg?size=360", 404),
            (f"{VIEW}?size=720", 400),
            (f"{VIEW}?size=360&size=1280", 400),
            (f"{VIEW}?size=360&x=1", 400),
        ):
            with self.subTest(url=url):
                response = self.client.get(url, headers=self.admin)
                self.assertEqual(response.status_code, status)
                if "size=" in url:  # the redirect route's 404 is as it was
                    self.assertEqual(
                        response.headers["cache-control"], "private, no-store"
                    )
        for url in (UPLOAD, CONFIRM):
            response = self.client.post(
                url,
                json={**self.body, "kind": "screenshots.w360"},
                headers=self.screen,
            )
            self.assertEqual(response.status_code, 400)
            self.assertEqual(response.json(), {"error": "Unknown media kind"})
        with patch.object(self.objects, "get", side_effect=OSError("detail")):
            response = self.client.get(f"{VIEW}?size=360", headers=self.admin)
        self.assertEqual(response.status_code, 503)

    def test_image_originals_are_copied_and_other_files_keep_the_link(self):
        original = self.client.get(f"{VIEW}?size=original", headers=self.admin)
        self.assertEqual(original.status_code, 200)
        self.assertEqual(original.content, b"original")
        # The type comes from the extension, never from what the bucket stored.
        self.assertEqual(original.headers["content-type"], "image/jpeg")
        self.assertEqual(
            original.headers["cache-control"], "private, max-age=31536000, immutable"
        )
        self.objects.add("phone/attachments/clip.mov", b"video")
        video = self.client.get(
            "/media/phone/attachments/clip.mov?size=original", headers=self.admin
        )
        self.assertEqual(video.status_code, 302)
        self.assertEqual(
            video.headers["location"],
            "https://objects.test/phone/attachments/clip.mov?get=1&expires=300",
        )
        self.assertEqual(self.objects.gets, [OBJECT])
        with patch("src.core.media.MAX_STREAMED_IMAGE", 4):
            large = self.client.get(f"{VIEW}?size=original", headers=self.admin)
        self.assertEqual(large.status_code, 302)
        self.assertEqual(
            large.headers["location"],
            f"https://objects.test/{OBJECT}?get=1&expires=300",
        )
        absent = self.client.get(
            "/media/screen/screenshots/dir/b.jpg?size=original", headers=self.admin
        )
        self.assertEqual(absent.status_code, 404)

    def test_without_size_the_route_still_redirects_and_bytes_need_the_login(self):
        view = self.client.get(VIEW, headers=self.admin)
        self.assertEqual(view.status_code, 302)
        self.assertEqual(
            view.headers["location"], f"https://objects.test/{OBJECT}?get=1&expires=300"
        )
        self.assertEqual(view.headers["cache-control"], "private, no-store")
        for size in ("360", "1280", "original"):
            for headers in ({}, self.screen):
                response = self.client.get(f"{VIEW}?size={size}", headers=headers)
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/login")
        self.assertEqual(self.objects.gets, [])

    def test_logging_out_clears_the_browsers_cache(self):
        self.client.post("/login", data={"token": ADMIN})
        response = self.client.post("/logout")
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["clear-site-data"], '"cache", "storage"')

    def test_paths_cannot_reach_another_connection_kind_or_derived_file(self):
        self.objects.add(OBJECT, b"<html><script>x</script>")
        self.objects.add("browser/screenshots/dir/a.jpg", b"other")
        for url in (
            "/media/screen/screenshots/..%2F..%2Fbrowser/screenshots/dir/a.jpg?size=original",
            "/media/screen/screenshots/%2Fscreen/screenshots/dir/a.jpg?size=original",
            "/media/screen/screenshots.w360/dir/a.jpg.webp?size=original",
            "/media/screen/screenshots%2F..%2Fx/dir/a.jpg?size=360",
            "/media/browser/screenshots/dir/a.jpg?size=original",
            "/media/screen/screenshots/dir/a.jpg?size=original&size=360",
        ):
            with self.subTest(url=url):
                response = self.client.get(url, headers=self.admin)
                self.assertIn(response.status_code, (400, 404))
        # Markup stored under an image key is still sent as that image's type.
        response = self.client.get(f"{VIEW}?size=original", headers=self.admin)
        self.assertEqual(response.headers["content-type"], "image/jpeg")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
