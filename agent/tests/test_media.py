import io
import json
import os
import sqlite3
import unittest
from contextlib import closing, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch
from urllib.parse import urlsplit

from miroir_agent.cli import main
from miroir_agent.collectors import MediaItem
from miroir_agent.config import load_config
from miroir_agent.media import _NoRedirect, _put, deliver_media

TS = 1700000000
KEY = "source-image.jpg"
TEXT = "fixture screen text"


class MediaTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.data = self.root / "source"
        self.images = self.data / "screenshots"
        self.images.mkdir(parents=True)
        self.state = self.root / "state" / "screen"
        self.config = self.root / "agent.json"
        self.plans: list[list[dict]] = []
        self.confirms: list[list[dict]] = []
        self.puts: list[tuple[str, str, bytes, str | None]] = []
        self.queries: list[str] = []
        self.mode = "upload"
        self.row_posts = 0
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                url = urlsplit(self.path)
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if self.headers.get("Authorization") != "Bearer testing":
                    self.send_response(401)
                    self.end_headers()
                    return
                if url.path.endswith("/receive"):
                    fixture.row_posts += 1
                    result: dict = {}
                else:
                    fixture.queries.append(url.query)
                    items = json.loads(body)["objects"]
                    if url.path.endswith("/uploads"):
                        fixture.plans.append(items)
                        result = {
                            "objects": [fixture.plan(item["key"]) for item in items]
                        }
                    else:
                        fixture.confirms.append(items)
                        result = {
                            "objects": [
                                {
                                    "key": item["key"],
                                    "status": "missing"
                                    if fixture.mode == "missing"
                                    else "stored",
                                }
                                for item in items
                            ]
                        }
                self.send_response(200)
                self.end_headers()
                self.wfile.write(json.dumps(result).encode())

            def do_PUT(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                fixture.puts.append(
                    (
                        self.path,
                        self.headers["Content-Type"],
                        body,
                        self.headers.get("Authorization"),
                    )
                )
                status = {"put_500": 500, "redirect": 307}.get(fixture.mode, 201)
                self.send_response(status)
                if status == 307:
                    self.send_header("Location", fixture.signed_url)
                self.end_headers()

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(self.server.server_close)
        thread = Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.signed_url = self.url + "/put?fixture=private-link"
        self.configure()

    def plan(self, key: str) -> dict[str, str]:
        if self.mode == "wrong_key":
            return {"key": "different.jpg", "status": "present"}
        if self.mode == "mixed":
            if key == KEY:
                return {"key": key, "status": "present"}
            return {"key": key, "status": "rejected", "reason": "bad_type"}
        return {
            "key": key,
            "status": "upload",
            "url": "http://objects.test/put"
            if self.mode == "non_loopback"
            else self.signed_url,
            "content_type": "image/test",
        }

    def configure(
        self,
        *,
        path: str = "/receive?format=old",
        collector: str = "screenmemory",
        **options: object,
    ) -> None:
        self.config.write_text(
            json.dumps(
                {
                    "state_dir": str(self.state.parent),
                    "collectors": [
                        {
                            "id": "screen",
                            "collector": collector,
                            "token_env": "TEST_TOKEN",
                            "receive_url": self.url + path,
                            "options": {"data_dir": str(self.data), **options}
                            if collector == "screenmemory"
                            else options,
                        }
                    ],
                }
            )
        )

    def source(self, frames: list[tuple] | None = None, apps: tuple = ()) -> None:
        with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
            db.execute("CREATE TABLE ocrresults(imagetimestamp, imagepath, imagetext)")
            db.executemany(
                "INSERT INTO ocrresults VALUES (?, ?, ?)", frames or [(TS, KEY, TEXT)]
            )
        with closing(sqlite3.connect(self.data / "usage.sqlite")) as db, db:
            db.execute("CREATE TABLE usage(timestamp, identifier)")
            db.executemany("INSERT INTO usage VALUES (?, ?)", apps)

    def run_agent(
        self, *extra: str, command: str = "run", token: str = "testing"
    ) -> tuple[int, str]:
        with (
            patch.dict(os.environ, TEST_TOKEN=token),
            redirect_stdout(io.StringIO()) as out,
        ):
            code = main([command, "--config", str(self.config), *extra])
        return code, out.getvalue()

    def media_status(self) -> dict:
        return json.loads((self.state / "status.json").read_text())["media"]

    def cursor(self) -> int:
        return json.loads((self.state / "media_cursor.json").read_text())["ts"]

    def test_delivers_and_filters_frames(self) -> None:
        self.configure(media=True)
        self.source(
            [
                (TS, KEY, TEXT),
                (TS + 1, "app.jpg", TEXT),
                (TS + 2, "marker.jpg", "ABOUT:logins"),
                (TS + 3, "missing.jpg", TEXT),
                (TS + 4, "empty.jpg", TEXT),
                (TS + 5, "../escape.jpg", TEXT),
                (TS + 6, "last.jpg", TEXT),
            ],
            ((TS + 1, "com.apple.Passwords"),),
        )
        for key, body in [
            (KEY, b"first"),
            ("app.jpg", b"app"),
            ("marker.jpg", b"marker"),
            ("empty.jpg", b""),
            ("last.jpg", b"last"),
        ]:
            (self.images / key).write_bytes(body)
        code, output = self.run_agent()
        self.assertEqual(code, 0)
        self.assertEqual(self.row_posts, 1)
        expected = [{"key": KEY, "size": 5}, {"key": "last.jpg", "size": 4}]
        self.assertEqual(self.plans, [expected])
        self.assertEqual(self.confirms, [expected])
        self.assertEqual(self.queries, ["", ""])
        self.assertEqual(
            self.puts,
            [
                ("/put?fixture=private-link", "image/test", body, None)
                for body in (b"first", b"last")
            ],
        )
        self.assertEqual(self.cursor(), TS + 6)
        self.assertEqual(
            self.media_status()["counts"],
            {
                "examined": 7,
                "uploaded": 2,
                "present": 0,
                "withheld": 2,
                "missing": 2,
                "invalid": 1,
                "rejected": 0,
            },
        )
        self.assertIn("media delivered", output)
        code, status = self.run_agent(command="status")
        self.assertEqual(code, 0)
        self.assertIn(f'"media_cursor": {{"ts": {TS + 6}}}', status)
        for text in (self.signed_url, "private-link", KEY, TEXT):
            self.assertNotIn(
                text, output + status + (self.state / "status.json").read_text()
            )

    def test_present_and_rejected(self) -> None:
        self.configure(media=True)
        self.source([(TS, KEY, TEXT), (TS + 1, "bad.txt", TEXT)])
        for key in (KEY, "bad.txt"):
            (self.images / key).write_bytes(b"fixture")
        self.mode = "mixed"
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.cursor(), TS + 1)
        self.assertEqual(self.media_status()["counts"]["present"], 1)
        self.assertEqual(self.media_status()["counts"]["rejected"], 1)
        self.assertFalse(self.puts or self.confirms)

    def test_failure_retries_same_page(self) -> None:
        self.configure(media=True, cutoff_ts=TS - 1)
        self.source()
        file = self.images / KEY
        file.write_bytes(b"fixture")
        for mode in ("put_500", "missing", "redirect", "non_loopback", "wrong_key"):
            with self.subTest(mode=mode):
                self.mode = mode
                before = len(self.puts)
                code, output = self.run_agent()
                self.assertEqual(code, 1)
                self.assertIn("media failed", output)
                self.assertEqual(self.media_status()["outcome"], "failed")
                self.assertEqual(self.media_status()["counts"]["uploaded"], 0)
                self.assertFalse((self.state / "media_cursor.json").exists())
                self.assertEqual(
                    len(self.puts) - before,
                    int(mode not in {"non_loopback", "wrong_key"}),
                )
        self.mode = "redirect"
        with self.assertRaisesRegex(ValueError, "^Media delivery failed$"):
            deliver_media(
                load_config(self.config).collectors[0],
                "testing",
                "screenshots",
                [MediaItem(KEY, file, 7)],
                {"uploaded": 0},
            )
        self.mode = "upload"
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.cursor(), TS)
        self.assertEqual(self.plans, [[{"key": KEY, "size": 7}]] * 7)

    def test_status_preserves_rows_when_media_cursor_is_corrupt(self) -> None:
        self.configure(media=True)
        self.state.mkdir(parents=True)
        rows = {"frames": TS, "app_usage": TS}
        status = {"outcome": "delivered", "media": {"outcome": "delivered"}}
        (self.state / "cursor.json").write_text(json.dumps(rows))
        (self.state / "status.json").write_text(json.dumps(status))
        (self.state / "media_cursor.json").write_text("{invalid fixture")
        _, output = self.run_agent(command="status")
        self.assertEqual(
            json.loads(output.removeprefix("screen ")),
            {
                "cursor": rows,
                "pending": False,
                "status": status,
                "media_cursor": "unreadable",
            },
        )

    def test_put_uses_default_proxy_handling_without_redirects(self) -> None:
        file = self.images / KEY
        file.write_bytes(b"x")
        with patch("miroir_agent.media.build_opener") as opener:
            opener.return_value.open.return_value.__enter__.return_value.status = 204
            _put(MediaItem(KEY, file, 1), self.signed_url, "image/test", 10)
        opener.assert_called_once()
        self.assertEqual(opener.call_args.kwargs, {})
        (handler,) = opener.call_args.args
        self.assertIsInstance(handler, _NoRedirect)

    def test_max_media_files_resumes_from_media_cursor(self) -> None:
        self.configure(media=True)
        keys = [f"{i}.jpg" for i in range(3)]
        self.source([(TS + i, key, TEXT) for i, key in enumerate(keys)])
        for key in keys:
            (self.images / key).write_bytes(b"x")
        self.assertEqual(self.run_agent("--max-media-files", "1")[0], 0)
        self.assertEqual(self.media_status()["counts"]["examined"], 1)
        self.assertEqual(self.cursor(), TS)
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.cursor(), TS + 2)
        self.assertEqual(self.media_status()["counts"]["examined"], 2)

    def test_media_never_runs_ahead_of_rows(self) -> None:
        self.configure(media=True, max_rows=1)
        self.source([(TS, KEY, TEXT), (TS + 1, "new.jpg", TEXT)])
        for key in (KEY, "new.jpg"):
            (self.images / key).write_bytes(b"x")
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.cursor(), TS)
        self.assertEqual(self.media_status()["counts"]["examined"], 1)
        self.assertEqual(self.plans, [[{"key": KEY, "size": 1}]])

    def test_timestamp_group_stays_whole(self) -> None:
        self.configure(media=True)
        self.source([(TS + 0.9, "b.jpg", TEXT), (TS + 0.1, KEY, TEXT)])
        for key in (KEY, "b.jpg"):
            (self.images / key).write_bytes(b"x")
        self.assertEqual(self.run_agent("--max-media-files", "1")[0], 0)
        self.assertEqual(self.cursor(), TS)
        self.assertEqual(
            self.plans, [[{"key": key, "size": 1} for key in ("b.jpg", KEY)]]
        )
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.media_status()["outcome"], "nothing_new")
        self.assertEqual(len(self.puts), 2)

    def test_missing_token_fails_and_preview_does_not_upload(self) -> None:
        self.configure(media=True)
        self.source()
        self.assertEqual(
            self.run_agent(
                "--collector",
                "screen",
                "--out",
                str(self.root / "preview"),
                command="collect",
            )[0],
            0,
        )
        self.assertEqual(self.run_agent(token="")[0], 1)
        self.assertEqual(self.media_status()["outcome"], "failed")
        self.assertFalse(self.row_posts or self.plans or self.puts)

    def test_disabled_by_default(self) -> None:
        self.source()
        (self.images / KEY).write_bytes(b"x")
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.row_posts, 1)
        self.assertFalse(self.plans or self.puts or self.confirms)
        self.assertFalse((self.state / "media_cursor.json").exists())

    def test_config(self) -> None:
        self.configure(media=True, media_dir="~/images")
        options = load_config(self.config).collectors[0].options
        self.assertIs(options["media"], True)
        self.assertEqual(options["media_dir"], str(Path.home() / "images"))
        for kwargs in (
            {"path": "/wrong?then=/receive", "media": True},
            {"collector": "zen_history", "media": False},
            {"media": 1},
        ):
            with self.subTest(kwargs=kwargs):
                self.configure(**kwargs)
                with self.assertRaises(ValueError):
                    load_config(self.config)
