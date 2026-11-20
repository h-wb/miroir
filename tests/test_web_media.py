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
