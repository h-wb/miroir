"""Static files, the content security policy, and the timeline routes (decision 040)."""

import hashlib
import json
import re
from pathlib import Path

from fastapi.testclient import TestClient

from src.cli.compose import web_app
from src.viewer import TEMPLATES_DIR as VIEWER_TEMPLATES
from src.web.app import TEMPLATES_DIR as ADMIN_TEMPLATES
from src.web.app import create_app
from src.web.shell import SECURITY_HEADERS, STATIC_DIR
from src.web.shell import TEMPLATES_DIR as TEMPLATES
from tests.test_web import BEARER, TOKEN, WebFixture

# What the policy refuses: style elements and attributes, inline handlers and
# script, javascript: links.
INLINE = re.compile(
    r"<style|\sstyle=|\son[a-z]+=|javascript:|<script(?![^>]*\ssrc=)", re.IGNORECASE
)


class StaticFileTests(WebFixture):
    def test_pages_carry_the_policy_and_load_the_stylesheet_from_a_file(self):
        self.login()
        for path in ("/", "/config", "/connections/music", "/analyses/music"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                for name, value in SECURITY_HEADERS.items():
                    self.assertEqual(response.headers[name], value)
                self.assertIn(
                    '<link rel="stylesheet" href="/static/miroir.css">', response.text
                )
                self.assertIsNone(INLINE.search(response.text), path)
        policy = SECURITY_HEADERS["Content-Security-Policy"]
        self.assertIn("default-src 'self'", policy)
        self.assertNotIn("unsafe", policy)

    def test_only_the_viewer_widens_the_policy_and_only_on_its_own_paths(self):
        # The timeline makes images from fetched frames (decision 040): blobs are
        # allowed on the viewer's pages, nowhere else (package split, step 8).
        strict = (
            "default-src 'self'; img-src 'self'; object-src 'none'; "
            "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
        )
        viewer = strict.replace("img-src 'self'", "img-src 'self' blob:")
        self.assertEqual(SECURITY_HEADERS["Content-Security-Policy"], strict)
        self.login()
        for path in ("/", "/config", "/login", "/static/miroir.css", "/timelinex"):
            with self.subTest(path=path):
                policy = self.client.get(path).headers["Content-Security-Policy"]
                self.assertEqual(policy, strict)
                self.assertNotIn("blob:", policy)
        for path in (
            "/timeline",
            "/timeline/conversations",
            "/timeline/search",
            "/api/timeline/health",
            "/media/screen/screenshots/a.png",
        ):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.headers["Content-Security-Policy"], viewer)
        # The admin part alone widens nothing.
        alone = TestClient(create_app(self.service, TOKEN))
        self.assertEqual(
            alone.get("/timeline", headers=BEARER).headers["Content-Security-Policy"],
            strict,
        )

    def test_every_response_carries_the_headers_even_redirects_and_errors(self):
        client = TestClient(create_app(self.service, TOKEN))
        for response in (
            client.get("/", follow_redirects=False),
            client.get("/login"),
            client.get("/api/overview"),
            client.get("/healthz"),
            client.get("/api/connections/missing", headers=BEARER),
        ):
            with self.subTest(url=str(response.url), status=response.status_code):
                self.assertEqual(response.headers["Referrer-Policy"], "same-origin")
                self.assertIn("Content-Security-Policy", response.headers)

    def test_static_files_need_no_login_and_revalidate(self):
        for name, kind in (
            ("miroir.css", "text/css"),
            ("miroir.js", "javascript"),
            ("htmx.min.js", "javascript"),
            ("conversations.css", "text/css"),
            ("conversations.js", "javascript"),
            ("conversations-core.js", "javascript"),
        ):
            with self.subTest(name=name):
                response = self.client.get(f"/static/{name}")
                self.assertEqual(response.status_code, 200)
                self.assertIn(kind, response.headers["content-type"])
                self.assertEqual(response.headers["cache-control"], "no-cache")
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertIn("etag", response.headers)
        for path in (
            "/static/../__init__.py",
            "/static/%2e%2e/__init__.py",
            "/static/missing",
        ):
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)
                self.assertNotEqual(response.status_code, 200)
                self.assertNotIn("Shell", response.text)
        # The static prefix opens nothing else.
        self.assertEqual(self.client.get("/staticx").status_code, 200)
        self.assertIn("Log in", self.client.get("/staticx").text)

    def test_templates_hold_no_inline_style_or_script(self):
        templates = sorted(
            [
                *TEMPLATES.glob("*.html"),
                *ADMIN_TEMPLATES.glob("*.html"),
                *VIEWER_TEMPLATES.glob("*.html"),
            ]
        )
        self.assertIn(TEMPLATES / "base.html", templates)
        self.assertIn(ADMIN_TEMPLATES / "overview.html", templates)
        self.assertIn(VIEWER_TEMPLATES / "timeline_day.html", templates)
        for template in templates:
            with self.subTest(template=template.name):
                self.assertIsNone(INLINE.search(template.read_text()))

    def test_vendored_htmx_matches_its_record_and_is_configured_safely(self):
        record = (Path(STATIC_DIR) / "VENDORED.md").read_text()
        row = re.search(
            r"^\| `htmx\.min\.js` \| htmx\.org \| (2\.\d+\.\d+) \| `([0-9a-f]{64})` \|",
            record,
            re.MULTILINE,
        )
        assert row is not None
        body = (Path(STATIC_DIR) / "htmx.min.js").read_bytes()
        self.assertEqual(hashlib.sha256(body).hexdigest(), row.group(2))
        self.assertIn(f'version:"{row.group(1)}"'.encode(), body)
        base = (TEMPLATES / "base.html").read_text()
        config = re.search(r"<meta name=\"htmx-config\" content='([^']*)'>", base)
        assert config is not None
        for setting in (
            '"allowEval":false',
            '"allowScriptTags":false',
            '"includeIndicatorStyles":false',
            '"selfRequestsOnly":true',
            '"historyEnabled":false',
            '"historyCacheSize":0',
        ):
            self.assertIn(setting, config.group(1))

    def test_the_moved_handlers_are_wired_to_their_elements(self):
        script = (Path(STATIC_DIR) / "miroir.js").read_text()
        self.assertIn("button[data-copy-previous]", script)
        # Logging out empties the page's storage (htmx keeps an address there).
        self.assertIn('matches("form[data-logout]")', script)
        self.assertIn("sessionStorage.clear()", script)
        self.login()
        self.assertIn(
            '<form method="post" action="/logout" data-logout>',
            self.client.get("/").text,
        )
        self.assertIn("a[data-history-back]", script)
        self.login()
        error = self.client.get("/connections/missing")
        self.assertIn('<a href="/" data-history-back>Back</a>', error.text)
        self.assertIn('<script src="/static/miroir.js" defer></script>', error.text)
        setup = self.client.post(
            "/connections/health/setup", data={"method": "webhook"}
        )
        for label in ("Copy URL", "Copy header"):
            self.assertIn(
                f'<button type="button" data-copy-previous>{label}', setup.text
            )


class FakeTimeline:
    def __init__(self, health=None, error=None):
        self.result, self.error = health, error

    def health(self):
        if self.error:
            raise self.error
        return self.result


class TimelineRouteTests(WebFixture):
    def test_without_the_setting_the_timeline_says_it_is_not_configured(self):
        self.assertEqual(self.client.get("/api/timeline/health").status_code, 401)
        self.assertEqual(
            self.client.get("/timeline", follow_redirects=False).status_code, 303
        )
        health = self.client.get("/api/timeline/health", headers=BEARER)
        self.assertEqual(health.status_code, 503)
        self.assertEqual(health.json()["configured"], False)
        self.assertIn("MIROIR_TIMELINE_DATABASE_URL", health.json()["message"])
        self.login()
        page = self.client.get("/timeline")
        self.assertEqual(page.status_code, 503)
        self.assertIn("The timeline is not configured", page.text)
        # Nothing else changes: the rest of the interface does not mention it.
        self.assertNotIn("/timeline", self.client.get("/").text)

    def test_a_configured_timeline_reports_its_database(self):
        from src.kernel.errors import OperationError

        # Without a timeline analysis the database alone is not a timeline.
        ready = FakeTimeline({"read_only": True, "statement_timeout": "5s"})
        client = TestClient(web_app(self.service, TOKEN, timeline=ready))
        health = client.get("/api/timeline/health", headers=BEARER)
        self.assertEqual(health.status_code, 503)
        self.assertIn("no analysis of the timeline package", health.json()["message"])
        config = json.loads(self.config_path.read_text())
        config["connections"][1].pop("dashboards")  # the fixture's, not valid here
        config["connections"].append(
            {"id": "screen", "integration": "screenmemory", "settings": {}}
        )
        config.setdefault("analyses", []).append(
            {"id": "timeline", "package": "timeline", "inputs": {"screen": "screen"}}
        )
        self.config_path.write_text(json.dumps(config))
        ready = FakeTimeline({"read_only": True, "statement_timeout": "5s"})
        client = TestClient(web_app(self.service, TOKEN, timeline=ready))
        health = client.get("/api/timeline/health", headers=BEARER)
        self.assertEqual(health.status_code, 200, health.text)
        self.assertEqual(health.json()["ready"], True)
        self.assertEqual(health.json()["statement_timeout"], "5s")
        writable = FakeTimeline({"read_only": False, "statement_timeout": "5s"})
        client = TestClient(web_app(self.service, TOKEN, timeline=writable))
        self.assertEqual(
            client.get("/api/timeline/health", headers=BEARER).status_code, 503
        )
        down = FakeTimeline(error=OperationError("The timeline database is down."))
        client = TestClient(web_app(self.service, TOKEN, timeline=down))
        client.post("/login", data={"token": TOKEN})
        page = client.get("/timeline")
        self.assertEqual(page.status_code, 503)
        self.assertIn("The timeline database is down.", page.text)


class AccessLogTests(WebFixture):
    def test_access_lines_keep_the_path_and_drop_the_query(self):
        import socket
        import threading
        import time
        import urllib.request

        import uvicorn

        app = web_app(self.service, TOKEN)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        # Configured after the app, as `serve` does: uvicorn's own logging
        # configuration must not drop the filter.
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="info")
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(setattr, server, "should_exit", True)
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.05)
        base = f"http://127.0.0.1:{port}"
        with self.assertLogs("uvicorn.access", "INFO") as logs:
            urllib.request.urlopen(f"{base}/healthz?q=secret+words&x=1").read()
            request = urllib.request.Request(
                f"{base}/static/miroir.css?q=secret", method="GET"
            )
            urllib.request.urlopen(request).read()
        lines = "\n".join(logs.output)
        self.assertIn('"GET /healthz HTTP/1.1" 200', lines)
        self.assertIn('"GET /static/miroir.css HTTP/1.1" 200', lines)
        self.assertNotIn("secret", lines)
        self.assertNotIn("?", lines)


class RouteTableTests(WebFixture):
    def test_no_path_and_method_is_registered_twice(self):
        app = web_app(self.service, TOKEN)

        def walk(routes):
            # Included routers are kept as wrappers of the router itself.
            for route in routes:
                inner = getattr(route, "original_router", None)
                if inner is not None:
                    yield from walk(inner.routes)
                else:
                    yield route

        seen: dict[tuple[str, str], int] = {}
        for route in walk(app.routes):
            for method in getattr(route, "methods", None) or ():
                key = (route.path, method)
                seen[key] = seen.get(key, 0) + 1
        self.assertGreater(len(seen), 40)
        self.assertIn(("/api/timeline/search", "POST"), seen)
        self.assertEqual({k: n for k, n in seen.items() if n > 1}, {})
