import gzip
import io
import json
import os
import sqlite3
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socket import socket
from tempfile import TemporaryDirectory
from threading import Event, Lock, Thread
from time import sleep
from unittest.mock import patch
from urllib.parse import urlsplit

from miroir_agent.cli import main
from miroir_agent.collectors.base import MediaItem
from miroir_agent.config import load_config
from miroir_agent.errors import ConfigError, Failure
from miroir_agent.media import upload
from miroir_agent.state import State
from miroir_agent.transport import Client

from .masking_fixtures import MASKER
from .test_dates import local_zone

TS = 1700000000
START = "2023-11-14T22:13:20Z"
END = "2023-11-14T22:13:23Z"
KEY = "source-image.jpg"
TEXT = "fixture screen text"
PRIVATE = "private-link"


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
        self.put_connections = set()
        self.puts: list[tuple[str, str, bytes, str | None]] = []
        self.queries: list[str] = []
        self.media_requests: list[str] = []
        self.stored: set[str] = set()
        self.active_puts = 0
        self.peak_puts = 0
        self.put_lock = Lock()
        self.release_timeout = Event()
        self.addCleanup(self.release_timeout.set)
        self.mode = "upload"
        self.row_posts = 0
        self.row_batches: list[list[dict]] = []
        self.row_queries: list[str] = []
        self.fail_row_post = 0
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def end_headers(self) -> None:
                if self.close_connection:
                    self.send_header("Connection", "close")
                super().end_headers()

            def log_message(self, format: str, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                url = urlsplit(self.path)
                body = self.rfile.read(int(self.headers["Content-Length"]))
                if self.headers.get("Authorization") != "Bearer testing":
                    self.send_response(401)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if url.path.endswith("/receive"):
                    fixture.row_posts += 1
                    if fixture.row_posts == fixture.fail_row_post:
                        self.send_response(500)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    fixture.row_batches.append(
                        [
                            json.loads(line)
                            for line in gzip.decompress(body).splitlines()
                        ]
                    )
                    fixture.row_queries.append(url.query)
                    result: dict = {}
                else:
                    fixture.queries.append(url.query)
                    items = json.loads(body)["objects"]
                    if url.path.endswith("/uploads"):
                        fixture.media_requests.append("uploads")
                        fixture.plans.append(items)
                        if fixture.mode == "timeout":
                            fixture.release_timeout.wait(3)
                            self.close_connection = True
                            return
                        if fixture.mode == "bad_json":
                            self.send_response(200)
                            self.send_header("Content-Length", str(len(TEXT.encode())))
                            self.end_headers()
                            self.wfile.write(TEXT.encode())
                            return
                        result = {
                            "objects": [fixture.plan(item["key"]) for item in items]
                        }
                    else:
                        fixture.media_requests.append("confirm")
                        fixture.confirms.append(items)
                        if fixture.mode != "missing":
                            fixture.stored.update(item["key"] for item in items)
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
                encoded = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def do_PUT(self) -> None:
                with fixture.put_lock:
                    fixture.put_connections.add(self.client_address)
                    fixture.active_puts += 1
                    fixture.peak_puts = max(fixture.peak_puts, fixture.active_puts)
                try:
                    status = self.put_body()
                finally:
                    with fixture.put_lock:
                        fixture.active_puts -= 1
                self.send_response(status)
                self.send_header("Content-Length", "0")
                if status == 307:
                    self.send_header("Location", fixture.signed_url)
                self.end_headers()

            def put_body(self) -> int:
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
                if fixture.mode in {"grouped", "fail_second_group"}:
                    sleep(0.03)
                if fixture.mode == "fail_second_group" and len(fixture.plans) == 2:
                    return 403 if body == b"frame-25.jpg" else 500
                return status

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(self.server.server_close)
        thread = Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.shutdown)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.signed_url = self.url + "/put?fixture=" + PRIVATE
        self.configure()

    def plan(self, key: str) -> dict[str, str]:
        if self.mode == "wrong_key":
            return {"key": "different.jpg", "status": "present"}
        if self.mode == "mixed":
            if key == KEY:
                return {"key": key, "status": "present"}
            return {"key": key, "status": "rejected", "reason": "bad_type"}
        if self.mode in {"grouped", "fail_second_group"} and key in self.stored:
            return {"key": key, "status": "present"}
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
        timeout_seconds: int = 10,
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
                            "timeout_seconds": timeout_seconds,
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
            patch("miroir_agent.cli.Masker", return_value=MASKER),
            patch.dict(os.environ, TEST_TOKEN=token),
            redirect_stdout(io.StringIO()) as out,
        ):
            code = main([command, "--config", str(self.config), *extra])
        return code, out.getvalue()

    def media_status(self) -> dict:
        return json.loads((self.state / "status.json").read_text())["media"]

    def cursor(self) -> int:
        return json.loads((self.state / "media_cursor.json").read_text())["ts"]

    def assert_failure(
        self,
        output: str,
        step: str,
        reason: str,
        status: int | None = None,
        *,
        key: str = "media",
    ) -> dict:
        raw = (self.state / "status.json").read_text()
        result = json.loads(raw)[key]
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(
            (result["step"], result["reason"], result["http_status"]),
            (step, reason, status),
        )
        self.assertIn(
            f"failed step={step} reason={reason}"
            + (f" http_status={status}" if status is not None else "")
            + " ",
            output,
        )
        for secret in (self.url, self.signed_url, PRIVATE, KEY, TEXT, str(self.images)):
            self.assertNotIn(secret, output + raw)
        return result

    def resend(
        self, *extra: str, end: str = END, token: str = "testing"
    ) -> tuple[int, str]:
        return self.run_agent(
            "--collector",
            "screen",
            "--from",
            START,
            "--to",
            end,
            *extra,
            command="resend",
            token=token,
        )

    def test_since_covers_initial_rows_and_media(self) -> None:
        self.configure(media=True, since=START)
        self.source(
            [(TS - 1, "old.jpg", TEXT), (TS, KEY, TEXT)], ((TS - 1, "old"), (TS, "new"))
        )
        for key in ("old.jpg", KEY):
            (self.images / key).write_bytes(b"fixture")
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual([row["ts"] for row in self.row_batches[0][1:-1]], [TS, TS])
        self.assertEqual(self.plans, [[{"key": KEY, "size": 7}]])

    def test_resend_pages_retry_and_preserves_state(self) -> None:
        self.configure(since="2025-01-01", max_rows=1)
        self.source(
            [(TS + i, f"frame{i}.jpg", TEXT) for i in range(-1, 4)],
            tuple((TS + i, "app") for i in range(-1, 4)),
        )
        state = State(self.state)
        self.assertEqual(self.resend()[0], 0)
        for path in (
            (state.directory / "cursor.json"),
            (state.directory / "media_cursor.json"),
            state.batch.parent,
        ):
            self.assertFalse(path.exists())
        state.batch.parent.mkdir()
        saved = (
            (state.directory / "cursor.json"),
            (state.directory / "media_cursor.json"),
            state.batch,
            (state.directory / "spool/batch.cursor.json"),
        )
        for i, path in enumerate(saved):
            path.write_bytes(f"fixed fixture {i}".encode())
        before = [path.read_bytes() for path in saved]
        (state.directory / "status.json").write_text(
            json.dumps({"outcome": "failed", "media": {"outcome": "nothing_new"}})
        )
        self.assertEqual(self.resend()[0], 0)
        sent = [batch[1:-1] for batch in self.row_batches]
        self.assertEqual(sent[:3], sent[3:])
        for kind in ("frame", "app_usage"):
            self.assertEqual(
                [row["ts"] for page in sent[:3] for row in page if row["type"] == kind],
                [TS, TS + 1, TS + 2],
            )
        self.assertEqual(self.row_queries, ["format=screenmemory_batch"] * 6)
        status = json.loads((state.directory / "status.json").read_text())
        self.assertEqual(
            (status["outcome"], status["media"]), ("failed", {"outcome": "nothing_new"})
        )
        result = status["resend"]
        self.assertEqual(
            (result["batches"], result["counts"]["frames"], result["window"]),
            (3, 3, [TS, TS + 3]),
        )
        self.fail_row_post = 8
        exit_code, output = self.resend()
        self.assertEqual(exit_code, 1)
        self.assertIn("screen failed", output)
        result = json.loads((state.directory / "status.json").read_text())["resend"]
        self.assertEqual(
            (result["outcome"], result["batches"], result["counts"]["frames"]),
            ("failed", 1, 1),
        )
        self.assertEqual(self.row_posts, 8)
        self.assertEqual(self.row_batches[-1][1:-1], sent[0])
        self.assertEqual([path.read_bytes() for path in saved], before)
        self.assertEqual(
            {path.name for path in state.directory.iterdir()},
            {"lock", "cursor.json", "media_cursor.json", "spool", "status.json"},
        )

    def test_resend_bare_to_and_out_without_token(self) -> None:
        midnight = 1700006400
        self.configure(max_rows=1)
        self.source([(ts, KEY, TEXT) for ts in (TS, midnight - 1, midnight)])
        out = self.root / "output"
        status = self.state / "status.json"
        with local_zone("UTC0"):
            code, output = self.resend("--out", str(out), end="2023-11-14", token="")
            self.assertEqual(code, 0)
            self.assertIn("screen collected ", output)
            self.assertFalse(status.exists())
            previous = b'{ "outcome": "delivered" }\n'
            status.write_bytes(previous)
            self.assertEqual(
                self.resend("--out", str(out), end="2023-11-14", token="")[0], 0
            )
            self.assertEqual(status.read_bytes(), previous)
            self.assertEqual(self.resend(end="2023-11-13")[0], 1)
        self.assertEqual(self.row_posts, 0)
        records = [
            json.loads(gzip.decompress(path.read_bytes()).splitlines()[1])
            for path in sorted(out.iterdir())
        ]
        self.assertEqual([row["ts"] for row in records], [TS, midnight - 1])
        self.assertEqual(
            self.resend("--max-batches", "1", end="2023-11-15T00:00:01Z")[0], 0
        )
        self.assertEqual(self.row_posts, 1)

    def test_resend_media_window_and_saved_frames_bound(self) -> None:
        self.configure(media=True)
        self.source(
            [
                (TS - 1, "old.jpg", TEXT),
                (TS, KEY, TEXT),
                (TS + 1, "masked.jpg", "ABOUT:logins"),
                (TS + 2, "unsaved.jpg", TEXT),
                (TS + 3, "end.jpg", TEXT),
            ]
        )
        for key in ("old.jpg", KEY, "masked.jpg", "unsaved.jpg", "end.jpg"):
            (self.images / key).write_bytes(b"fixture")
        self.state.mkdir(parents=True)
        state = State(self.state)
        (state.directory / "cursor.json").write_text(
            json.dumps({"frames": TS + 1, "app_usage": TS})
        )
        (state.directory / "media_cursor.json").write_text("unreadable fixture")
        code, output = self.resend("--media")
        self.assertEqual(code, 0)
        self.assertEqual(len(output.splitlines()), 1)
        self.assertEqual(self.plans, [[{"key": KEY, "size": 7}]])
        self.assertEqual(self.confirms, self.plans)
        self.assertEqual(len(self.puts), 1)
        self.assertEqual(
            (state.directory / "media_cursor.json").read_text(), "unreadable fixture"
        )
        (state.directory / "media_cursor.json").unlink()
        (state.directory / "cursor.json").write_text(
            json.dumps({"frames": TS + 3, "app_usage": TS})
        )
        self.mode = "put_500"
        code, failure_output = self.resend("--media")
        self.assertEqual(code, 1)
        self.assert_failure(failure_output, "put", "http", 500, key="resend")
        self.assertEqual(
            self.plans[-1], [{"key": KEY, "size": 7}, {"key": "unsaved.jpg", "size": 7}]
        )
        _, status = self.run_agent(command="status")
        self.assertIn('"resend":', status)
        for value in (KEY, TEXT, self.signed_url):
            self.assertNotIn(value, output + status)

    def test_resend_rejects_media_modes_and_respects_lock(self) -> None:
        with State(self.state).lock() as locked:
            self.assertTrue(locked)
            self.assertEqual(self.resend(), (0, "screen already_running {}\n"))
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.resend("--media", "--out", str(self.root / "output"))
        self.assertEqual(error.exception.code, 2)
        self.configure(collector="zen_history")
        self.assertEqual(self.resend("--media")[0], 1)
        self.assertEqual(self.row_posts, 0)

    def test_resend_rejects_media_when_not_enabled(self) -> None:
        self.source()
        self.state.mkdir(parents=True)
        (self.state / "cursor.json").write_text(
            json.dumps({"frames": TS, "app_usage": TS})
        )
        for options in ({}, {"media": False}):
            with self.subTest(options=options):
                self.configure(**options)
                code, output = self.resend("--media")
                self.assertEqual(code, 1)
                self.assertIn("Media is not enabled", output)
                self.assertEqual(self.row_posts, 0)
                self.assertFalse(self.plans or self.puts or self.confirms)

    def test_collect_only_checks_custom_media_dir_when_enabled(self) -> None:
        self.source()
        directory = self.root / "custom-media"
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                self.configure(media=enabled, media_dir=str(directory))
                out = directory / str(enabled)
                code, _ = self.run_agent(
                    "--collector", "screen", "--out", str(out), command="collect"
                )
                self.assertEqual(code, int(enabled))
                self.assertEqual((out / "batch-0001.jsonl.gz").exists(), not enabled)

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
        self.assertCountEqual(
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
        self.configure(media=True, since=START)
        self.source()
        file = self.images / KEY
        file.write_bytes(b"fixture")
        failures = {
            "put_500": ("put", "http", 500),
            "missing": ("confirm", "not_stored", None),
            "redirect": ("put", "link", None),
            "non_loopback": ("put", "link", None),
            "wrong_key": ("links", "reply", None),
            "bad_json": ("links", "reply", None),
        }
        for mode, (step, reason, status) in failures.items():
            with self.subTest(mode=mode):
                self.mode = mode
                before = len(self.puts)
                code, output = self.run_agent()
                self.assertEqual(code, 1)
                self.assert_failure(output, step, reason, status)
                self.assertEqual(self.media_status()["counts"]["uploaded"], 0)
                self.assertFalse((self.state / "media_cursor.json").exists())
                self.assertEqual(
                    len(self.puts) - before,
                    int(mode in {"put_500", "missing", "redirect"}),
                )
        self.mode = "redirect"
        with (
            patch.dict(
                os.environ,
                {load_config(self.config).collectors[0].token_env: "testing"},
            ),
            Client(
                self.url, load_config(self.config).collectors[0].token_env, 10
            ) as client,
            self.assertRaisesRegex(Failure, "^put: link$"),
        ):
            upload(
                client,
                "screenshots",
                [MediaItem(KEY, file, 7)],
                {"uploaded": 0},
            )
        self.mode = "upload"
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.cursor(), TS)
        self.assertEqual(self.plans, [[{"key": KEY, "size": 7}]] * 8)

    def group_source(self, count: int = 60) -> list[bytes]:
        self.configure(media=True)
        keys = [f"frame-{i:02}.jpg" for i in range(count)]
        self.source([(TS + i, key, TEXT) for i, key in enumerate(keys)])
        bodies = [key.encode() for key in keys]
        for key, body in zip(keys, bodies, strict=True):
            (self.images / key).write_bytes(body)
        return bodies

    def test_upload_groups_and_put_concurrency(self) -> None:
        bodies = self.group_source()
        self.mode = "grouped"
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual([len(plan) for plan in self.plans], [25, 25, 10])
        self.assertEqual(self.confirms, self.plans)
        self.assertEqual(self.media_requests, ["uploads", "confirm"] * 3)
        self.assertCountEqual([body for _, _, body, _ in self.puts], bodies)
        self.assertGreater(self.peak_puts, 1)
        self.assertLessEqual(self.peak_puts, 4)
        self.assertEqual(self.media_status()["counts"]["uploaded"], 60)
        self.assertEqual(self.cursor(), TS + 59)

    def test_reuses_put_connections_across_groups(self) -> None:
        self.group_source(30)
        self.mode = "grouped"
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(len(self.puts), 30)
        self.assertLessEqual(len(self.put_connections), 4)

    def test_links_extra_field_does_not_leak_in_failure(self) -> None:
        self.configure(media=True)
        self.source()
        (self.images / KEY).write_bytes(b"fixture")
        marker = "unexpected-reply-value"
        with patch.object(
            self,
            "plan",
            return_value={"key": KEY, "status": "present", "extra": marker},
        ):
            code, output = self.run_agent()
        self.assertEqual(code, 1)
        self.assert_failure(output, "links", "reply")
        self.assertNotIn(marker, output + (self.state / "status.json").read_text())

    def test_continues_from_existing_state_files(self) -> None:
        self.configure(media=True)
        self.source()
        (self.images / KEY).write_bytes(b"fixture")
        spool = self.state / "spool"
        spool.mkdir(parents=True)
        (self.state / "cursor.json").write_text(
            json.dumps({"frames": TS - 1, "app_usage": 0})
        )
        (self.state / "media_cursor.json").write_text(json.dumps({"ts": TS - 1}))
        (spool / "batch.jsonl.gz").write_bytes(gzip.compress(b'{"type":"header"}\n'))
        (spool / "batch.cursor.json").write_text(
            json.dumps(
                {
                    "cursor": {"frames": TS, "app_usage": 0},
                    "counts": {"frames": 1},
                    "more": False,
                }
            )
        )
        resend = {
            "outcome": "nothing_new",
            "window": [TS - 1, TS],
            "counts": {},
            "batches": 0,
        }
        (self.state / "status.json").write_text(
            json.dumps(
                {
                    "outcome": "failed",
                    "resend": resend,
                    "media": {"outcome": "nothing_new"},
                }
            )
        )
        code, output = self.run_agent(command="status")
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output.split(" ", 1)[1])["pending"])
        self.assertEqual(self.run_agent()[0], 0)
        self.assertEqual(self.row_batches, [[{"type": "header"}]])
        self.assertEqual(
            json.loads((self.state / "cursor.json").read_text()),
            {"frames": TS, "app_usage": 0},
        )
        self.assertEqual(
            json.loads((self.state / "media_cursor.json").read_text()), {"ts": TS}
        )
        self.assertEqual(list(spool.iterdir()), [])
        status = json.loads((self.state / "status.json").read_text())
        self.assertEqual(status["outcome"], "delivered")
        self.assertEqual(status["media"]["counts"]["uploaded"], 1)
        self.assertEqual(status["resend"], resend)

    def test_later_group_failure_keeps_counts_and_retries_page(self) -> None:
        bodies = self.group_source()
        self.state.mkdir(parents=True)
        (self.state / "media_cursor.json").write_text(json.dumps({"ts": TS - 1}))
        self.mode = "fail_second_group"
        code, output = self.run_agent()
        self.assertEqual(code, 1)
        result = self.assert_failure(output, "put", "http", 403)
        self.assertNotIn(
            "frame-25.jpg", output + (self.state / "status.json").read_text()
        )
        self.assertEqual(result["counts"]["uploaded"], 25)
        self.assertEqual(self.cursor(), TS - 1)
        self.assertEqual(self.media_requests, ["uploads", "confirm", "uploads"])
        self.assertCountEqual([body for _, _, body, _ in self.puts], bodies[:50])

        self.mode = "grouped"
        self.puts.clear()
        self.assertEqual(self.run_agent()[0], 0)
        self.assertCountEqual([body for _, _, body, _ in self.puts], bodies[25:])
        self.assertEqual(self.media_status()["counts"]["present"], 25)
        self.assertEqual(self.media_status()["counts"]["uploaded"], 35)
        self.assertEqual(self.cursor(), TS + 59)

    def test_link_request_timeout(self) -> None:
        self.configure(media=True, timeout_seconds=1)
        self.source()
        (self.images / KEY).write_bytes(b"fixture")
        self.mode = "timeout"
        try:
            code, output = self.run_agent()
        finally:
            self.release_timeout.set()
        self.assertEqual(code, 1)
        self.assert_failure(output, "links", "timeout")

    def test_refused_connection(self) -> None:
        self.source()
        (self.images / KEY).write_bytes(b"fixture")
        self.assertEqual(self.run_agent()[0], 0)
        # Reserve a port without listening so the connection is refused.
        with socket() as endpoint:
            endpoint.bind(("127.0.0.1", 0))
            self.url = f"http://127.0.0.1:{endpoint.getsockname()[1]}"
            self.configure(media=True)
            code, output = self.run_agent()
        self.assertEqual(code, 1)
        self.assert_failure(output, "links", "network")

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

    def test_run_reports_corrupt_media_cursor(self) -> None:
        self.configure(media=True)
        self.source()
        self.state.mkdir(parents=True)
        cursor = self.state / "media_cursor.json"
        cursor.write_text(TEXT)
        code, output = self.run_agent()
        self.assertEqual(code, 1)
        self.assert_failure(output, "collect", "state")
        self.assertEqual(cursor.read_text(), TEXT)
        self.assertFalse(self.media_requests)

    def test_resend_reports_corrupt_saved_row_cursor(self) -> None:
        self.configure(media=True)
        self.source()
        self.state.mkdir(parents=True)
        (self.state / "cursor.json").write_text(TEXT)
        code, output = self.resend("--media")
        self.assertEqual(code, 1)
        self.assert_failure(output, "collect", "state", key="resend")
        self.assertFalse(self.media_requests)

    def test_run_and_resend_report_media_source_errors(self) -> None:
        self.configure(media=True)
        self.source()
        self.state.mkdir(parents=True)
        (self.state / "cursor.json").write_text(
            json.dumps({"frames": TS, "app_usage": TS})
        )
        for error in (OSError, sqlite3.OperationalError):
            with self.subTest(error=error):
                for key, run in (("media", self.run_agent), ("resend", self.resend)):
                    # Both passes open the two databases for rows before reading media.
                    connect = sqlite3.connect
                    reads = iter(range(2))

                    def open_db(
                        *args, calls=reads, failure=error, original=connect, **kwargs
                    ):
                        if next(calls, None) is None:
                            raise failure(TEXT)
                        return original(*args, **kwargs)

                    with patch.object(sqlite3, "connect", side_effect=open_db):
                        code, output = run(*(["--media"] if key == "resend" else []))
                    self.assertEqual(code, 1)
                    self.assert_failure(output, "collect", "source", key=key)
                self.assertFalse(self.media_requests)

    def test_upload_reply_rejects_invalid_content_type(self) -> None:
        self.configure(media=True)
        self.source()
        (self.images / KEY).write_bytes(b"fixture")
        for fields in ({}, {"content_type": 42}, {"content_type": "\n" + TEXT}):
            with (
                self.subTest(fields=fields),
                patch.object(
                    self,
                    "plan",
                    return_value={
                        "key": KEY,
                        "status": "upload",
                        "url": self.signed_url,
                        **fields,
                    },
                ),
            ):
                code, output = self.run_agent()
                self.assertEqual(code, 1)
                self.assert_failure(output, "links", "reply")
                self.assertFalse(self.puts or self.confirms)

    def test_put_reports_missing_file_and_changed_size(self) -> None:
        file = self.images / KEY
        for exists in (False, True):
            with self.subTest(exists=exists):
                if exists:
                    file.write_bytes(b"changed")
                with (
                    Client(self.url, "UNUSED", 10) as client,
                    self.assertRaises(Failure) as failure,
                ):
                    client.put(file, 1, self.signed_url, "image/test")
                self.assertEqual(
                    (failure.exception.step, failure.exception.reason), ("put", "file")
                )
                self.assertIsNone(failure.exception.http_status)
        self.assertFalse(self.puts)

    def test_put_distinguishes_read_failure_from_network_failure(self) -> None:
        file = self.images / KEY
        file.write_bytes(b"x")
        with file.open("rb") as body:
            with (
                patch.object(Path, "open", return_value=body),
                patch.object(body, "read", side_effect=OSError(TEXT)),
                patch("miroir_agent.transport.PoolManager") as opener,
                Client(self.url, "UNUSED", 10) as client,
                self.assertRaises(Failure) as failure,
            ):
                opener.return_value.request.side_effect = lambda *args, **kwargs: (
                    kwargs["body"].read(1)
                )
                client.put(file, 1, self.signed_url, "image/test")
            self.assertEqual(
                (failure.exception.step, failure.exception.reason), ("put", "file")
            )
            self.assertIsNone(failure.exception.http_status)

        with patch("miroir_agent.transport.PoolManager") as opener:
            opener.return_value.request.side_effect = OSError(TEXT)
            with (
                Client(self.url, "UNUSED", 10) as client,
                self.assertRaises(Failure) as failure,
            ):
                client.put(file, 1, self.signed_url, "image/test")
            self.assertEqual(
                (failure.exception.step, failure.exception.reason), ("put", "network")
            )
            self.assertIsNone(failure.exception.http_status)

    def test_put_uses_a_pool_without_proxies_retries_or_redirects(self) -> None:
        file = self.images / KEY
        file.write_bytes(b"x")
        with patch("miroir_agent.transport.PoolManager") as pool_factory:
            pool = pool_factory.return_value
            pool.request.return_value.status = 204
            pool.request.return_value.length_remaining = 0
            with Client(self.url, "UNUSED", 10) as client:
                client.put(file, 1, self.signed_url, "image/test")
        pool_factory.assert_called_once()
        self.assertEqual(pool_factory.call_args.args, ())
        self.assertNotIn("proxy_url", pool_factory.call_args.kwargs)
        self.assertIs(pool_factory.call_args.kwargs["retries"], False)
        self.assertIs(pool.request.call_args.kwargs["redirect"], False)

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
        code, output = self.run_agent(token="")
        self.assertEqual(code, 1)
        self.assert_failure(output, "links", "token")
        code, output = self.resend("--media", token="")
        self.assertEqual(code, 1)
        self.assert_failure(output, "links", "token", key="resend")
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
        self.assertIs(options.media, True)
        self.assertEqual(options.media_dir, Path.home() / "images")
        for kwargs in (
            {"path": "/wrong?then=/receive", "media": True},
            {"collector": "zen_history", "media": False},
            {"media": 1},
        ):
            with self.subTest(kwargs=kwargs):
                self.configure(**kwargs)
                with self.assertRaises(ConfigError):
                    load_config(self.config)
