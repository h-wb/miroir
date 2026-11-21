"""Static files, the content security policy, and the timeline routes (decision 040)."""

import hashlib
import re
from pathlib import Path

from fastapi.testclient import TestClient

from src.web.app import SECURITY_HEADERS, STATIC_DIR, create_app
from tests.test_web import BEARER, TOKEN, WebFixture

TEMPLATES = Path(STATIC_DIR).parent / "templates"
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
        ):
            with self.subTest(name=name):
                response = self.client.get(f"/static/{name}")
                self.assertEqual(response.status_code, 200)
                self.assertIn(kind, response.headers["content-type"])
                self.assertEqual(response.headers["cache-control"], "no-cache")
                self.assertEqual(response.headers["x-content-type-options"], "nosniff")
                self.assertIn("etag", response.headers)
        for path in ("/static/../app.py", "/static/%2e%2e/app.py", "/static/missing"):
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)
                self.assertNotEqual(response.status_code, 200)
                self.assertNotIn("create_app", response.text)
        # The static prefix opens nothing else.
        self.assertEqual(self.client.get("/staticx").status_code, 200)
        self.assertIn("Log in", self.client.get("/staticx").text)

    def test_templates_hold_no_inline_style_or_script(self):
        for template in sorted(TEMPLATES.glob("*.html")):
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
        ):
            self.assertIn(setting, config.group(1))

    def test_the_moved_handlers_are_wired_to_their_elements(self):
        script = (Path(STATIC_DIR) / "miroir.js").read_text()
        self.assertIn("button[data-copy-previous]", script)
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
        from src.core.operations import OperationError

        ready = FakeTimeline({"read_only": True, "statement_timeout": "5s"})
        client = TestClient(create_app(self.service, TOKEN, timeline=ready))
        health = client.get("/api/timeline/health", headers=BEARER)
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["ready"], True)
        self.assertEqual(health.json()["statement_timeout"], "5s")
        writable = FakeTimeline({"read_only": False, "statement_timeout": "5s"})
        client = TestClient(create_app(self.service, TOKEN, timeline=writable))
        self.assertEqual(
            client.get("/api/timeline/health", headers=BEARER).status_code, 503
        )
        down = FakeTimeline(error=OperationError("The timeline database is down."))
        client = TestClient(create_app(self.service, TOKEN, timeline=down))
        client.post("/login", data={"token": TOKEN})
        page = client.get("/timeline")
        self.assertEqual(page.status_code, 503)
        self.assertIn("The timeline database is down.", page.text)
