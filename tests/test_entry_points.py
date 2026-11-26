"""The viewer's own process (package split, step 10): `serve`, `serve
--without-viewer` and `view`, what each loads and what each answers.

One image runs the control plane alone and the viewer alone, behind one
hostname that routes by path; `serve` with no option still runs both."""

import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from src.cli.compose import admin_app, web_app
from src.cli.view import viewer_app, viewer_settings
from src.control.config import load_miroir
from src.control.operations import MetadataStore
from src.control.service import InlineExecutor, MiroirService
from src.kernel.errors import OperationError
from src.web.shell import SESSION_COOKIE
from tests.fakes import FakeObjectStore
from tests.test_timeline_api import ROUTES, TimelineApiFixture, call
from tests.test_timeline_models import path
from tests.test_web import TOKEN
from tests.test_web_static import FakeTimeline

ROOT = Path(__file__).resolve().parents[1]
MEDIA = {"endpoint": "https://s3.example.org", "bucket": "media"}
CONFIG = {
    "schema_version": 1,
    "id": "split",
    "media": MEDIA,
    "connections": [
        {"id": "screen", "integration": "screenmemory", "settings": {}},
        {"id": "messages", "integration": "apple_messages", "settings": {}},
    ],
    "analyses": [
        {
            "id": "timeline",
            "package": "timeline",
            "inputs": {"screen": "screen", "messages": "messages"},
        }
    ],
}
# The header both processes draw: the admin pages', then the viewer's.
HEADER = [
    ("/", "Connections"),
    ("/config", "Configuration"),
    ("/timeline", "Timeline"),
    ("/timeline/conversations", "Conversations"),
]
READER_URL = "postgresql+psycopg2://reader:synthetic@db.miroir.example/warehouse"
# Runs an entry point to the moment it would serve, then names what it loaded.
PROBE = """
import json, sys
import uvicorn
served = []
uvicorn.run = lambda app, **options: served.append(app)
from src.cli.main import main
code = main(sys.argv[1:])
print(json.dumps({"code": code, "served": len(served), "modules": sorted(sys.modules)}))
"""
# What neither the viewer alone nor its reading of the configuration loads.
NOT_IN_VIEW = (
    "src.control.service",
    "src.control.runner",
    "src.control.operations",
    "src.control.secrets",
    "src.adapters.prefect",
    "src.web.app",
    "src.media.derive",
    "src.media.decoder",
)
LIBRARIES = ("dlt", "prefect", "PIL", "pillow_heif")


def header(html: str) -> list[tuple[str, str]]:
    """The header's links, in order."""
    found = re.search(r"<header>(.*?)</header>", html, re.DOTALL)
    assert found is not None
    return re.findall(r'<a href="([^"]+)">([^<]+)</a>', found.group(1))


class EntryPointProbeTests(unittest.TestCase):
    """Each entry point in a fresh interpreter, as the container runs it."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.config = self.root / "miroir.json"
        self.config.write_text(json.dumps(CONFIG))
        self.env = {
            k: v for k, v in os.environ.items() if not k.startswith("MIROIR_")
        } | {
            "MIROIR_API_TOKEN": TOKEN,
            "MIROIR_TIMELINE_DATABASE_URL": READER_URL,
            "MIROIR_MEDIA_ACCESS_KEY_ID": "synthetic-id",
            "MIROIR_MEDIA_SECRET_ACCESS_KEY": "synthetic-secret",
        }

    def probe(self, *argv: str, **env: str) -> dict:
        result = subprocess.run(
            [sys.executable, "-c", PROBE, "--config", str(self.config), *argv],
            cwd=ROOT,
            env=self.env | env,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout.strip().splitlines()[-1])

    def serve(self, *options: str, **env: str) -> dict:
        return self.probe(
            "serve",
            "--duckdb",
            str(self.root / "w.duckdb"),
            "--metadata",
            f"sqlite:///{self.root / 'metadata.sqlite'}",
            "--artifacts",
            str(self.root / "artifacts"),
            *options,
            **env,
        )

    def test_view_loads_only_what_the_viewer_needs_and_writes_nothing(self):
        before = {p: p.stat().st_mtime_ns for p in self.root.rglob("*")}
        found = self.probe("view")
        self.assertEqual((found["code"], found["served"]), (0, 1))
        modules = set(found["modules"])
        self.assertIn("src.viewer.part", modules)
        self.assertEqual(sorted({m.split(".")[0] for m in modules} & {*LIBRARIES}), [])
        self.assertEqual(sorted(modules & {*NOT_IN_VIEW}), [])
        # Reading the configuration loads the connector registry, never a
        # connector's source (the dlt side).
        sources = sorted(
            m for m in modules if re.fullmatch(r"src\.integrations\..*source.*", m)
        )
        self.assertEqual(sources, [])
        self.assertEqual(
            {p: p.stat().st_mtime_ns for p in self.root.rglob("*")}, before
        )

    def test_view_needs_the_reader_database_and_the_token_not_the_master_key(self):
        self.assertEqual(self.probe("view", MIROIR_TIMELINE_DATABASE_URL="")["code"], 2)
        self.assertEqual(self.probe("view", MIROIR_API_TOKEN="short")["code"], 2)
        # No configuration is not one to create: `view` writes nothing.
        self.config.unlink()
        self.assertEqual(self.probe("view")["code"], 2)
        self.assertFalse(self.config.exists())

    def test_serve_without_the_viewer_never_reads_the_reader_settings(self):
        # A reader URL `serve` refuses: never read, it stops nothing.
        found = self.serve("--without-viewer", MIROIR_TIMELINE_DATABASE_URL="not a url")
        self.assertEqual((found["code"], found["served"]), (0, 1))
        viewer = [m for m in found["modules"] if m.startswith("src.viewer")]
        # Its header's links come from the viewer's declaration, and nothing else.
        self.assertEqual(viewer, ["src.viewer", "src.viewer.navigation"])
        self.assertNotIn("PIL", {m.split(".")[0] for m in found["modules"]})

    def test_serve_with_no_option_still_serves_the_viewer_and_reads_its_settings(self):
        refused = self.serve(MIROIR_TIMELINE_DATABASE_URL="not a url")
        self.assertEqual((refused["code"], refused["served"]), (2, 0))
        found = self.serve()
        self.assertEqual((found["code"], found["served"]), (0, 1))
        self.assertIn("src.viewer.part", found["modules"])


class SplitTests(unittest.TestCase):
    """Both processes' applications side by side, over one configuration."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        self.config = root / "miroir.json"
        self.config.write_text(json.dumps(CONFIG))
        store = MetadataStore(f"sqlite:///{root / 'meta.sqlite'}")
        self.addCleanup(store.engine.dispose)
        store.migrate()
        self.objects = FakeObjectStore()
        self.service = MiroirService(
            self.config,
            store,
            root / "artifacts",
            InlineExecutor(self.fail),
            object_store=self.objects,
        )
        down = FakeTimeline(error=OperationError("The timeline database is down."))
        self.both = TestClient(web_app(self.service, TOKEN, timeline=down))
        self.admin = TestClient(admin_app(self.service, TOKEN))
        settings = viewer_settings(
            down, lambda: load_miroir(self.config), lambda: self.objects
        )
        self.view = TestClient(viewer_app(TOKEN, settings), follow_redirects=False)

    def login(self, client: TestClient) -> str:
        response = client.post("/login", data={"token": TOKEN}, follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        return response.cookies[SESSION_COOKIE]

    def test_a_login_on_either_process_is_a_login_on_both(self):
        cookie = self.login(self.admin)
        self.assertEqual(self.login(self.view), cookie)
        self.assertEqual(self.login(self.both), cookie)
        anonymous = TestClient(self.view.app, follow_redirects=False)
        self.assertEqual(anonymous.get("/api/timeline/health").status_code, 401)
        page = anonymous.get("/timeline")
        self.assertEqual((page.status_code, page.headers["location"]), (303, "/login"))
        # A login on `view` lands on its own page; on `serve`, on the admin's.
        landing = anonymous.post("/login", data={"token": TOKEN})
        self.assertEqual(landing.headers["location"], "/timeline")
        self.assertEqual(
            self.admin.post(
                "/login", data={"token": TOKEN}, follow_redirects=False
            ).headers["location"],
            "/",
        )
        logout = self.view.post("/logout")
        self.assertEqual(logout.headers["location"], "/login")
        self.assertIn("cache", logout.headers["clear-site-data"])

    def test_both_processes_draw_the_same_header(self):
        for client in (self.admin, self.view, self.both):
            self.login(client)
        self.assertEqual(header(self.admin.get("/").text), HEADER)
        self.assertEqual(header(self.both.get("/").text), HEADER)
        page = self.view.get("/timeline")
        self.assertEqual(page.status_code, 503)
        self.assertIn("The timeline database is down.", page.text)
        self.assertEqual(header(page.text), HEADER)
        # Without a timeline analysis neither links the viewer.
        plain = {**CONFIG, "analyses": []}
        self.config.write_text(json.dumps(plain))
        self.assertEqual(header(self.admin.get("/").text), HEADER[:2])
        self.assertEqual(header(self.view.get("/timeline").text), HEADER[:2])

    def test_view_answers_the_viewer_and_refuses_the_admin_routes(self):
        self.login(self.view)
        health = TestClient(self.view.app).get("/healthz")
        self.assertEqual((health.status_code, health.json()), (200, {"ok": True}))
        for name in ("viewer/timeline.js", "viewer/conversations.css", "miroir.css"):
            with self.subTest(name=name):
                self.assertEqual(self.view.get(f"/static/{name}").status_code, 200)
        self.objects.add("screen/screenshots/2026/11/20/a.jpeg", b"original")
        # The reader is down: the view route fails closed, but it is the viewer's.
        media = self.view.get("/media/screen/screenshots/2026/11/20/a.jpeg")
        self.assertEqual(media.status_code, 503)
        for method, url in (
            ("GET", "/"),
            ("GET", "/config"),
            ("GET", "/connections/screen"),
            ("GET", "/api/overview"),
            ("GET", "/api/connections/screen"),
            ("GET", "/metrics"),
            ("POST", "/api/connections/screen/receive"),
            ("POST", "/api/connections/screen/media/uploads"),
            ("POST", "/api/connections/screen/media/confirm"),
        ):
            with self.subTest(url=url):
                self.assertEqual(self.view.request(method, url).status_code, 404)

    def test_serve_without_the_viewer_refuses_the_viewer_routes(self):
        self.login(self.admin)
        self.assertEqual(self.admin.get("/").status_code, 200)
        self.assertEqual(self.admin.get("/healthz").status_code, 200)
        self.assertEqual(self.admin.get("/static/miroir.css").status_code, 200)
        for url in (
            "/timeline",
            "/timeline/2026-11-20",
            "/timeline/conversations",
            "/api/timeline/health",
            "/media/screen/screenshots/2026/11/20/a.jpeg",
            "/static/viewer/timeline.js",
        ):
            with self.subTest(url=url):
                response = self.admin.get(url, follow_redirects=False)
                self.assertEqual(response.status_code, 404)


class SplitTimelineTests(TimelineApiFixture):
    """`view` over the built synthetic day, with a login made by `serve`."""

    def test_view_serves_pages_api_and_media_with_serves_session(self):
        config = self.root / "split-miroir.json"
        config.write_text(json.dumps({**self.raw, "media": MEDIA}))
        service = MiroirService(
            config,
            self.store,
            self.root / "artifacts",
            InlineExecutor(self.fail),
            object_store=self.objects,
        )
        serve = TestClient(admin_app(service, TOKEN), follow_redirects=False)
        cookie = serve.post("/login", data={"token": TOKEN}).cookies[SESSION_COOKIE]
        settings = viewer_settings(
            self.database, lambda: load_miroir(config), lambda: self.objects
        )
        app = viewer_app(TOKEN, settings)
        anonymous = TestClient(app, follow_redirects=False)
        view = TestClient(app, follow_redirects=False, cookies={SESSION_COOKIE: cookie})
        self.stored("screen", path(0))
        self.stored("screen", path(3700))
        media = f"/media/screen/screenshots/{path(0)}?size=360"
        self.assertEqual(anonymous.get("/timeline/2026-11-20").status_code, 303)
        self.assertEqual(anonymous.get(media).status_code, 303)
        for route in ROUTES:
            with self.subTest(route=route):
                self.assertEqual(call(anonymous, route).status_code, 401)
                self.assertEqual(call(view, route).status_code, 200)
        home = view.get("/timeline")
        self.assertEqual(
            (home.status_code, home.headers["location"]), (303, "/timeline/2026-11-21")
        )
        day = view.get("/timeline/2026-11-20")
        self.assertEqual(day.status_code, 200)
        self.assertIn("7 screen frames, 6 browser visits.", day.text)
        self.assertIn('src="/static/viewer/timeline.js"', day.text)
        self.assertEqual(header(day.text), HEADER[:3])
        self.assertEqual(header(serve.get("/").text), HEADER[:3])
        self.assertEqual(view.get(media).status_code, 200)
        withheld = view.get(f"/media/screen/screenshots/{path(3700)}?size=360")
        self.assertEqual(withheld.status_code, 404)
        original = view.get(f"/media/screen/screenshots/{path(0)}")
        self.assertEqual(original.status_code, 302)


if __name__ == "__main__":
    unittest.main()
