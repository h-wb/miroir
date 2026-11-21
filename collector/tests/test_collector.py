import base64
import gzip
import io
import json
import os
import sqlite3
import stat
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socket import socket
from tempfile import TemporaryDirectory
from threading import Event, Thread
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from kingfisher_sdk import Rules, Scanner

from miroir_collector import __version__
from miroir_collector.cli import main
from miroir_collector.config import load_config
from miroir_collector.errors import ConfigError, Failure
from miroir_collector.masking import Masker
from miroir_collector.sources import SOURCES
from miroir_collector.sources.base import Collected
from miroir_collector.sources.screenmemory import ScreenMemoryCursor
from miroir_collector.state import State
from miroir_collector.transport import Client

from .masking_fixtures import MASKER, github_token


class NativePanic(BaseException):
    pass


class MaskingTests(unittest.TestCase):
    def test_interrupts_propagate_during_start_and_scan(self) -> None:
        for error in (KeyboardInterrupt, SystemExit):
            with (
                self.subTest(error=error),
                patch("kingfisher_sdk.Rules", side_effect=error),
                self.assertRaises(error),
            ):
                Masker().text("ordinary")
            MASKER.text("ordinary")
            with (
                self.subTest(error=error),
                patch.object(Scanner, "scan", side_effect=error),
                self.assertRaises(error),
            ):
                MASKER.text("ordinary")

    def test_scanner_preserves_unicode_and_ignores_allow_comments(self) -> None:
        for comment in ("", " # gitleaks:allow", " # kingfisher:ignore"):
            with self.subTest(comment=comment):
                prefix = "caf\u00e9 \U0001f600 "
                suffix = " \U0001f680 d\u00e9j\u00e0" + comment
                self.assertEqual(
                    MASKER.text(prefix + github_token() + suffix),
                    (prefix + "[masked:token]" + suffix, {"token": 1}),
                )

    def test_scanner_masks_base64(self) -> None:
        encoded = base64.b64encode(github_token().encode()).decode()
        self.assertEqual(MASKER.tokens(encoded), ("[masked:scanner]", {"scanner": 1}))

    def test_custom_rule_and_lazy_reuse(self) -> None:
        token = "exm_" + "u9g4LmCHp0MaQxYoI8r1VTkZ"
        with TemporaryDirectory() as directory:
            rules = Path(directory) / "internal.yml"
            rules.write_text(
                "rules:\n"
                "  - name: Example internal token\n"
                "    id: custom.example-internal\n"
                "    pattern: '\\b(exm_[A-Za-z0-9]{24})\\b'\n"
                "    min_entropy: 3.0\n"
                "    confidence: low\n"
            )
            with patch("kingfisher_sdk.Scanner", wraps=Scanner) as start:
                masker = Masker([rules])
                self.assertEqual(masker.text(" \n"), (" \n", {}))
                start.assert_not_called()
                expected = ("[masked:scanner]", {"scanner": 1})
                self.assertEqual(masker.text(token), expected)
                self.assertEqual(masker.tokens(token), expected)
                start.assert_called_once()
        self.assertEqual(MASKER.text(token), (token, {}))

    def test_mask_tokens_skips_assignments_and_long_random_strings(self) -> None:
        token = "gh" + "p_" + "a1B2" * 9
        opaque = "aA91bX87zD0" * 4
        self.assertEqual(
            MASKER.tokens(f"password={opaque} {token}"),
            (
                f"password={opaque} [masked:token]",
                {"token": 1},
            ),
        )
        self.assertEqual(MASKER.tokens(opaque), (opaque, {}))

    def test_mask_url_preserves_encoding_and_masks_by_parameter_name(self) -> None:
        token = "gh" + "p_" + "a1B2" * 9
        url = (
            "https://reader:p%40ss@[::1]/a%2Fb?q=a+b&SAMLResponse=x&client-Signature=y#/route?access_key=z&other="
            + token
        )
        self.assertEqual(
            MASKER.url(url),
            (
                (
                    "https://reader:[masked:url_userinfo]@[::1]/a%2Fb?q=a+b&SAMLResponse=[masked:url_param]"
                    "&client-Signature=[masked:url_param]#/route?access_key=[masked:url_param]&other=[masked:token]"
                ),
                {"url_userinfo": 1, "url_param": 3, "token": 1},
            ),
        )
        self.assertEqual(
            MASKER.url("https://example.org/#token=a?b"),
            (
                "https://example.org/#token=[masked:url_param]",
                {"url_param": 1},
            ),
        )
        public = "https://reader@example.org/a%2Fb?q=a+b#anchor"
        self.assertEqual(MASKER.url(public), (public, {}))

    def test_prefixed_assignments(self) -> None:
        for prefix, value in (
            ("POSTGRES_PASSWORD=", "S3cret" + "pass9"),
            ("db_password: ", "hunter2" * 2),
            ("access_token=", "Ab12cd" + "34ef56"),
            ("AWS_SECRET_ACCESS_KEY=", "abc/" * 10),
        ):
            self.assertEqual(
                MASKER.text(prefix + value),
                (prefix + "[masked:assignment]", {"assignment": 1}),
            )

    def test_private_key_on_one_line(self) -> None:
        following = "a+/=" * 5 + "\n-----END RSA " + "PRIVATE KEY-----"
        inline = (
            "-----BEGIN RSA "
            + "PRIVATE KEY----- "
            + "b+/=" * 5
            + " -----END RSA "
            + "PRIVATE KEY-----"
        )
        self.assertEqual(
            MASKER.text(f"{inline}\n{following}"),
            (f"[masked:private_key]\n{following}", {"private_key": 1}),
        )

    def test_exact_length_tokens_reject_continuations(self) -> None:
        for value in (
            "AI" + "za" + "0" * 35 + "-",
            "np" + "m_" + "a" * 37,
            "AK" + "IA" + "A" * 17,
            "AS" + "IA" + "A" * 17,
        ):
            self.assertNotIn("token", MASKER.text(value)[1])

    def test_card_is_not_part_of_a_longer_number(self) -> None:
        card = "4" + "1" * 15
        for separator in ("", " ", "-"):
            for value in (f"{card}{separator}2", f"2{separator}{card}"):
                self.assertEqual(MASKER.text(value), (value, {}))

    def test_token_shapes(self) -> None:
        tokens = [
            *("gh" + letter + "_" + "a" * 30 for letter in "pousr"),
            "github_" + "pat_" + "a_" * 20,
            "gl" + "pat-" + "a_" * 10,
            "s" + "k-" + "a_" * 10,
            *(prefix + "IA" + "A" * 16 for prefix in ("AK", "AS")),
            *("xo" + "x" + letter + "-" + "a" * 10 for letter in "abprs"),
            "AI" + "za" + "0" * 35,
            "np" + "m_" + "a" * 36,
            *(
                f"{prefix}k_{env}_" + "b" * 16
                for prefix in "sr"
                for env in ("live", "test")
            ),
            "ts" + "key-" + "a" * 20,
            "AGE-" + "SECRET-KEY-1" + "B" * 50,
            "ey" + "J" + ".".join(["b" * 8] * 3),
            "0."
            + "aabbccdd-1234-4321-9876-abcdef012345."
            + "a+/=" * 5
            + ":"
            + "b+/=" * 5,
        ]
        for i, token in enumerate(tokens):
            with self.subTest(case=i):
                self.assertEqual(
                    MASKER.text(f"<{token}>"), ("<[masked:token]>", {"token": 1})
                )
        bearer = "bEaReR " + "x+/=" * 5
        self.assertEqual(MASKER.text(bearer), ("bEaReR [masked:token]", {"token": 1}))

    def test_other_rules_and_determinism(self) -> None:
        key = "-----BEGIN RSA " + "PRIVATE KEY-----\n" + "a+/=" * 5
        with_end = key + "\n-----END RSA " + "PRIVATE KEY-----"
        self.assertEqual(
            MASKER.text(key + "\nnext line"),
            ("[masked:private_key]\nnext line", {"private_key": 1}),
        )
        assignment = 'POSTGRES_PASSWORD="' + "unusual" + "2" + '"'
        random = "Ab3_dEf6" + "GhIj9kLm-NoPqRsTuVwXyZ012345"
        card = "4" + "1" * 3 + "-" + "1111-" * 2 + "1" * 4
        text = f"{with_end}\n{assignment}\n{random}\n{card}"
        expected = (
            '[masked:private_key]\nPOSTGRES_PASSWORD="[masked:assignment]"\n[masked:random]\n[masked:card]',
            {"private_key": 1, "assignment": 1, "random": 1, "card": 1},
        )
        self.assertEqual(MASKER.text(text), expected)
        self.assertEqual(MASKER.text(text), expected)

    def test_preserves_ordinary_text(self) -> None:
        for value in (
            "abcde01234" * 4,
            "ABCDabcd01234567" * 4,
            "e7c8a52f54bde093c751ac3db713651611fc06ba",
            "aabbccdd-1234-4321-9876-abcdef012345",
            "a-long-kebab-case-name-for-a-local-application",
            "token: counting",
            "password: required",
            "1234567890123",
        ):
            self.assertEqual(MASKER.text(value), (value, {}))


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path.endswith("/report"):
            self.server.reports.append(
                (self.headers, self.rfile.read(int(self.headers["Content-Length"])))
            )
            self.send_response(self.server.report_code)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.server.posts.append(
            (
                self.path,
                self.headers,
                self.rfile.read(int(self.headers["Content-Length"])),
            )
        )
        if self.server.wait_for_reply:
            self.server.release_reply.wait(3)
            return
        if self.server.stall_receipt:
            self.send_response(201)
            self.send_header("Content-Length", "100")
            self.end_headers()
            self.wfile.flush()
            self.server.release_reply.wait(3)
            return
        self.send_response(self.server.response_code)
        self.send_header("Location", "/elsewhere")
        self.end_headers()
        self.wfile.write(
            b'{"new":true,"artifact_id":"artifact-one","ignored":"fixture text"}'
        )

    def do_GET(self) -> None:
        self.server.gets.append(self.path)
        self.send_response(200)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        pass


class CollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        server = HTTPServer(("127.0.0.1", 0), _Handler)
        self.server = server
        server.posts = []
        server.reports = []
        server.report_code = 204
        server.gets = []
        server.response_code = 201
        server.wait_for_reply = False
        server.stall_receipt = False
        server.release_reply = Event()
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        self.addCleanup(server.release_reply.set)
        self.token = "demo" + "-receive"
        env = patch.dict(
            os.environ, {"MIROIR_TEST_TOKEN": self.token, "no_proxy": "127.0.0.1"}
        )
        env.start()
        self.addCleanup(env.stop)
        self.path = self.root / "config.json"
        self.item = {
            "id": "screen_macbook",
            "source": "screenmemory",
            "receive_url": f"http://127.0.0.1:{server.server_port}/receive",
            "token_env": "MIROIR_TEST_TOKEN",
            "options": {"data_dir": "~/ScreenMemoryData"},
        }
        self.raw = {"state_dir": str(self.root / "state"), "sources": [self.item]}
        self.path.write_text(json.dumps(self.raw))
        self.state = State(self.root / "state" / "screen_macbook")
        self.calls = []

    def _cli(self, *args: str) -> tuple[int, str]:
        output = io.StringIO()
        with redirect_stdout(output):
            code = main([*args, "--config", str(self.path)])
        return code, output.getvalue()

    def _fake(
        self,
        options: dict,
        cursor: ScreenMemoryCursor | None,
        out: Path,
        now: float,
        *,
        masker: Masker,
        window: tuple[int, int] | None = None,
    ) -> Collected:
        self.calls.append(cursor.model_dump() if cursor else None)
        step = (cursor.frames if cursor else 0) + 1
        out.write_bytes(f"fixture text {step}".encode())
        return Collected(
            ScreenMemoryCursor(frames=step, app_usage=0), {"frames": 1}, True
        )

    def _source(self, *texts: str) -> Path:
        data = self.root / "source"
        data.mkdir()
        self.item["options"]["data_dir"] = str(data)
        self.path.write_text(json.dumps(self.raw))
        with closing(sqlite3.connect(data / "text.sqlite")) as db, db:
            db.execute("CREATE TABLE ocrresults(imagetimestamp, imagepath, imagetext)")
            db.executemany(
                "INSERT INTO ocrresults VALUES (?, ?, ?)",
                [(1795132100 + i, f"{i}.jpeg", text) for i, text in enumerate(texts)],
            )
        with closing(sqlite3.connect(data / "usage.sqlite")) as db, db:
            db.execute("CREATE TABLE usage(timestamp, identifier)")
        return data

    def test_status_and_no_new_rows_do_not_start_scanner(self) -> None:
        self._source()
        with patch("kingfisher_sdk.Scanner", wraps=Scanner) as start:
            self.assertEqual(self._cli("status")[0], 0)
            code, output = self._cli("run")
            self.assertEqual(code, 0)
            self.assertIn("nothing_new", output)
            start.assert_not_called()

    def test_rule_paths_are_validated_and_may_be_inside_source(self) -> None:
        data = self._source()
        rules = data / "private-rules.yml"
        self.raw["masking"] = {"rules": ["~/source/private-rules.yml"]}
        self.path.write_text(json.dumps(self.raw))
        with patch.dict(os.environ, HOME=str(self.root)):
            self.assertEqual(
                self._cli("status"),
                (
                    1,
                    "Invalid collector configuration: masking.rules.0: file not found\n",
                ),
            )
            rules.touch()
            self.assertEqual(load_config(self.path).masking_rules, (rules,))
            self.assertEqual(self._cli("run")[0], 0)

    def test_bad_rule_fails_closed_without_leaking_details(self) -> None:
        self._source("source fixture text")
        rules = self.root / "private-rules.yml"
        pattern = "(private fixture"
        rules.write_text(
            "rules:\n  - name: Broken rule\n    id: custom.broken\n"
            f"    pattern: '{pattern}'\n    confidence: medium\n"
        )
        self.raw["masking"] = {"rules": [str(rules)]}
        self.path.write_text(json.dumps(self.raw))
        self.state.directory.mkdir(parents=True)
        cursor = self.state.directory / "cursor.json"
        cursor.write_text('{"frames": 1795132099, "app_usage": 0}')
        before = cursor.read_bytes()
        with redirect_stderr(io.StringIO()) as stderr:
            code, stdout = self._cli("run")
        self.assertEqual(code, 1)
        self.assertIn("step=collect reason=masking", stdout)
        status = (self.state.directory / "status.json").read_text()
        self.assertEqual(json.loads(status)["reason"], "masking")
        for value in (str(rules), pattern, "source fixture text"):
            self.assertNotIn(value, stdout + status + stderr.getvalue())
        self.assertEqual(cursor.read_bytes(), before)
        self.assertEqual(list(self.state.batch.parent.iterdir()), [])
        self.assertFalse(self.server.posts)

    def test_non_custom_rule_id_fails_collection(self) -> None:
        self._source("ordinary text")
        rules = self.root / "internal.yml"
        rules.write_text(
            "rules:\n  - name: Example rule\n    id: betterleaks.github-pat\n"
            "    pattern: '(example_[A-Za-z0-9]{24})'\n    confidence: low\n"
        )
        self.raw["masking"] = {"rules": [str(rules)]}
        self.path.write_text(json.dumps(self.raw))
        with patch("kingfisher_sdk.Rules", wraps=Rules) as load:
            code, output = self._cli("run")
        self.assertEqual(code, 1)
        self.assertIn("step=collect reason=masking", output)
        load.assert_called_once_with(
            paths=(rules,), builtins=False, confidence="low", cache=False
        )
        self.assertFalse(self.server.posts)

    def test_start_panic_is_sanitized_and_remembered_across_sources(self) -> None:
        self._source("ordinary text")
        self.raw["sources"].append(self.item | {"id": "second"})
        self.path.write_text(json.dumps(self.raw))
        with (
            patch(
                "kingfisher_sdk.Rules", side_effect=NativePanic("private panic")
            ) as load,
            redirect_stderr(io.StringIO()) as stderr,
        ):
            code, stdout = self._cli("run")
        self.assertEqual(code, 1)
        for source in (self.item["id"], "second"):
            self.assertIn(f"{source} failed step=collect reason=masking", stdout)
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn("private panic", stdout)
        load.assert_called_once()
        self.assertFalse(self.server.posts)

    def test_scan_errors_mask_whole_frame_and_allow_delivery(self) -> None:
        private_text = "private source text"
        key = "-----BEGIN PRIVATE KEY-----\n" + "a+/=" * 5
        self._source(
            github_token(),
            key + "\n" + private_text,
            "timeout source text",
            "panic source text",
        )
        scan = Scanner.scan

        def flaky(scanner, text, *, timeout):
            self.assertEqual(timeout, 5)
            if private_text in text:
                raise RuntimeError("private fixture")
            if text == "timeout source text":
                raise TimeoutError("timeout fixture")
            if text == "panic source text":
                raise NativePanic("panic fixture")
            return scan(scanner, text, timeout=timeout)

        with (
            patch.object(Scanner, "scan", flaky),
            patch("miroir_collector.cli.Masker", return_value=MASKER),
            redirect_stderr(io.StringIO()) as stderr,
        ):
            code, stdout = self._cli("run")
        self.assertEqual(code, 0)
        self.assertIn("delivered", stdout)
        self.assertEqual(len(self.server.posts), 1)
        raw = gzip.decompress(self.server.posts[0][2]).decode()
        document = [json.loads(line) for line in raw.splitlines()]
        self.assertEqual(
            [frame["text"] for frame in document[1:-1]],
            ["[masked:token]"] + ["[masked:unscanned]"] * 3,
        )
        self.assertEqual(document[-1]["masked"], {"token": 1, "unscanned": 3})
        self.assertEqual(stderr.getvalue(), "")
        status = (self.state.directory / "status.json").read_text()
        for value in (
            private_text,
            "timeout source text",
            "panic source text",
            "private fixture",
            "timeout fixture",
            "panic fixture",
            key,
        ):
            self.assertNotIn(value, raw + stdout + status + stderr.getvalue())
        self.assertEqual(
            self.state.read_cursor(), {"frames": 1795132103, "app_usage": 0}
        )

    def test_config(self) -> None:
        self.item["receive_url"] = (
            "https://miroir.example/api/connections/screen_macbook/receive"
        )
        example = {
            **self.raw,
            "state_dir": "~/Library/Application Support/miroir-collector",
        }
        self.path.write_text(json.dumps(example))
        config = load_config(self.path)
        self.assertEqual(
            config.state_dir,
            Path.home() / "Library/Application Support/miroir-collector",
        )
        self.assertEqual(config.sources[0].timeout_seconds, 120)
        self.assertEqual(
            config.sources[0].options.data_dir,
            Path.home() / "ScreenMemoryData",
        )
        for changes in (
            {"wat": True},
            {"id": "Bad-id"},
            {"source": "unknown"},
            {"receive_url": "http://miroir.example/receive"},
            {"timeout_seconds": True},
            {"options": {"data_dir": "data", "max_rows": 200001}},
        ):
            self.path.write_text(
                json.dumps({**self.raw, "sources": [{**self.item, **changes}]})
            )
            with self.assertRaises(ConfigError):
                load_config(self.path)
        self.path.write_text(
            json.dumps({**self.raw, "sources": [self.item, self.item]})
        )
        with self.assertRaises(ConfigError):
            load_config(self.path)

    def test_config_rejects_blank_drop_list_entries(self) -> None:
        for field in ("drop_bundle_ids", "drop_markers"):
            for entry in ("", " \t\n"):
                self.item["options"] = {"data_dir": "data", field: [entry]}
                self.path.write_text(json.dumps(self.raw))
                with (
                    self.subTest(field=field, entry=entry),
                    self.assertRaises(ConfigError),
                ):
                    load_config(self.path)

    def test_zen_delivery_retries_and_persists_digest_cursor(self) -> None:
        profile = self.root / "profile"
        profile.mkdir()
        source = profile / "places.sqlite"
        with closing(sqlite3.connect(source)) as db, db:
            db.executescript("""
                CREATE TABLE moz_places(id INTEGER PRIMARY KEY, url TEXT);
                CREATE TABLE moz_historyvisits(id INTEGER PRIMARY KEY, place_id INTEGER, visit_date INTEGER);
                CREATE TABLE moz_origins(id INTEGER PRIMARY KEY, host TEXT);
                INSERT INTO moz_places VALUES (1, 'https://example.org/');
                INSERT INTO moz_origins VALUES (1, 'example.org');
            """)
        self.item.update(source="zen_history", options={"places_path": str(source)})
        self.path.write_text(json.dumps(self.raw))

        self.server.response_code = 503
        self.assertEqual(self._cli("run")[0], 1)
        self.assertTrue(self.state.has_pending())
        self.assertFalse((self.state.directory / "cursor.json").exists())
        self.server.response_code = 201
        self.assertEqual(self._cli("run")[0], 0)
        self.assertFalse(self.state.has_pending())
        cursor = json.loads((self.state.directory / "cursor.json").read_text())
        self.assertRegex(cursor["origins_digest"], r"^[0-9a-f]{64}$")
        self.assertEqual(self._cli("status")[0], 0)
        code, output = self._cli("run")
        self.assertEqual(code, 0)
        self.assertIn("nothing_new", output)
        self.assertEqual(len(self.server.posts), 2)
        self.assertEqual(self.server.posts[0][2], self.server.posts[1][2])
        for path, _, _ in self.server.posts:
            self.assertEqual(
                parse_qs(urlsplit(path).query), {"format": ["firefox_places_batch"]}
            )

        self.raw["state_dir"] = str(profile / "state")
        self.path.write_text(json.dumps(self.raw))
        self.assertEqual(self._cli("run")[0], 1)
        self.assertFalse((profile / "state").exists())

    def test_atomic_write_syncs_file_then_directory(self) -> None:
        path = self.root / "cursor.json"
        replace = os.replace
        steps: list[str] = []

        def sync(fd: int) -> None:
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                self.assertEqual(
                    json.loads(path.read_text()), {"frames": 1, "app_usage": 0}
                )
                steps.append("directory")
            else:
                self.assertFalse(path.exists())
                self.assertGreater(os.fstat(fd).st_size, 0)
                steps.append("file")

        def rename(source: Path, destination: Path) -> None:
            steps.append("replace")
            replace(source, destination)

        with (
            patch("miroir_collector.files.os.fsync", side_effect=sync),
            patch("miroir_collector.files.os.replace", side_effect=rename),
        ):
            State(path.parent).save_cursor({"frames": 1, "app_usage": 0})
        self.assertEqual(steps, ["file", "replace", "directory"])

    def test_delivery(self) -> None:
        self.item["receive_url"] += (
            "?format=wrong&filter=a%2Fb&f%6Frmat=also-wrong&filter="
        )
        self.path.write_text(json.dumps(self.raw))
        config = load_config(self.path).sources[0]
        body = self.root / "batch.gz"
        body.write_bytes(b"batch body")
        for code in (201, 500, 302):
            self.server.response_code = code
            with Client(
                config.receive_url, config.token_env, config.timeout_seconds
            ) as client:
                if code == 201:
                    self.assertEqual(
                        client.post_batch(body, "screenmemory_batch").status, code
                    )
                else:
                    with self.assertRaises(Failure) as caught:
                        client.post_batch(body, "screenmemory_batch")
                    self.assertEqual(caught.exception.http_status, code)
        self.assertEqual(len(self.server.posts), 3)
        self.assertFalse(self.server.gets)
        for path, headers, contents in self.server.posts:
            self.assertEqual(
                urlsplit(path).query, "filter=a%2Fb&filter=&format=screenmemory_batch"
            )
            self.assertEqual(headers["Authorization"], f"Bearer {self.token}")
            self.assertEqual(headers["Content-Type"], "application/gzip")
            self.assertRegex(headers["User-Agent"], r"^miroir-collector/\d")
            self.assertEqual(contents, body.read_bytes())
        self.assertEqual(config.receive_url, self.item["receive_url"])

    def test_retry_lock_limit_and_status(self) -> None:
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            self.state.batch.parent.mkdir(parents=True)
            self.state.batch.write_bytes(b"incomplete batch")
            self.server.response_code = 500
            code, output = self._cli("run")
            self.assertEqual(code, 1)
            self.assertIn("failed", output)
            self.assertTrue(self.state.has_pending())
            self.assertFalse((self.state.directory / "cursor.json").exists())
            pending = self.state.batch.read_bytes()
            self.assertNotEqual(pending, b"incomplete batch")
            with self.state.lock() as locked:
                self.assertTrue(locked)
                code, output = self._cli("run")
                self.assertEqual(code, 0)
                self.assertIn("already_running", output)
            self.server.response_code = 200
            self.assertEqual(self._cli("run")[0], 0)
            self.assertEqual(self.calls, [None])
            self.assertEqual(
                [body for _, _, body in self.server.posts], [pending, pending]
            )
            self.assertFalse(self.state.has_pending())
            self.assertEqual(
                json.loads((self.state.directory / "cursor.json").read_text()),
                {"frames": 1, "app_usage": 0},
            )
            self.assertEqual(self._cli("run", "--max-batches", "2")[0], 0)
            self.assertEqual(
                self.calls,
                [None, {"frames": 1, "app_usage": 0}, {"frames": 2, "app_usage": 0}],
            )
            code, output = self._cli("status")
            self.assertEqual(code, 0)
            self.assertNotIn("fixture text", output)
            self.assertNotIn(self.token, output)
            status = json.loads(output.split(" ", 1)[1])
            self.assertEqual(status["cursor"], {"frames": 3, "app_usage": 0})
            self.assertFalse(status["pending"])
            self.assertEqual(status["status"]["outcome"], "delivered")
            self.assertEqual(status["status"]["artifact_id"], "artifact-one")

    def test_missing_token_preserves_pending_batch(self) -> None:
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            del os.environ["MIROIR_TEST_TOKEN"]
            self.assertEqual(self._cli("run")[0], 1)
            self.assertTrue(self.state.has_pending())
            self.assertFalse((self.state.directory / "cursor.json").exists())
            self.assertFalse(self.server.posts)
            self.assertEqual(self.calls, [None])

    def test_row_failures_are_reported_without_advancing_the_cursor(self) -> None:
        cursor_path = self.state.directory / "cursor.json"
        saved = '{"frames": 10, "app_usage": 0}\n'
        cursor_path.parent.mkdir(parents=True)
        cursor_path.write_text(saved)
        url = self.item["receive_url"]
        with (
            closing(socket()) as endpoint,
            patch.object(SOURCES["screenmemory"], "collect", self._fake),
        ):
            endpoint.bind(("127.0.0.1", 0))
            refused_url = f"http://127.0.0.1:{endpoint.getsockname()[1]}/receive"
            for reason, endpoint_url, token, http_status in (
                ("network", refused_url, self.token, None),
                ("token", url, "", None),
                ("http", url, self.token, 500),
                ("timeout", url, self.token, None),
            ):
                with (
                    self.subTest(reason=reason),
                    patch.dict(os.environ, MIROIR_TEST_TOKEN=token),
                ):
                    self.server.response_code = 500
                    self.server.wait_for_reply = reason == "timeout"
                    self.item.update(receive_url=endpoint_url, timeout_seconds=1)
                    self.path.write_text(json.dumps(self.raw))
                    code, output = self._cli("run")
                    self.assertEqual(code, 1)
                    detail = f"failed step=deliver reason={reason}"
                    if http_status is not None:
                        detail += f" http_status={http_status}"
                    self.assertIn(detail + " ", output)
                    raw_status = (self.state.directory / "status.json").read_text()
                    status = json.loads(raw_status)
                    self.assertEqual(
                        (
                            status["outcome"],
                            status["step"],
                            status["reason"],
                            status["http_status"],
                        ),
                        ("failed", "deliver", reason, http_status),
                    )
                    self.assertNotIn(self.token, output + raw_status)
                    self.assertTrue(self.state.has_pending())
                    self.assertEqual(cursor_path.read_text(), saved)
            self.assertEqual(self.calls, [json.loads(saved)])
            self.assertEqual(len(self.server.posts), 2)
            self.assertEqual(self.server.posts[0][2], self.server.posts[1][2])

    def test_receipt_that_never_arrives_is_not_an_acknowledgement(self) -> None:
        self.server.stall_receipt = True
        self.item["timeout_seconds"] = 1
        self.path.write_text(json.dumps(self.raw))
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            code, output = self._cli("run")
        self.assertEqual(code, 1)
        self.assertIn("failed step=deliver reason=timeout ", output)
        self.assertTrue(self.state.has_pending())
        self.assertFalse((self.state.directory / "cursor.json").exists())

    def test_timeout_while_sending_the_body_is_a_timeout(self) -> None:
        body = self.root / "large"
        size = 64 * 1024**2
        with body.open("wb") as file:
            file.truncate(size)
        with closing(socket()) as stuck:
            # Connections are accepted by the kernel and never read.
            stuck.bind(("127.0.0.1", 0))
            stuck.listen()
            url = f"http://127.0.0.1:{stuck.getsockname()[1]}/receive"
            with Client(url, "MIROIR_TEST_TOKEN", 1) as client:
                for step, send in (
                    ("put", lambda: client.put(body, size, url, "image/jpeg")),
                    ("deliver", lambda: client.post_batch(body, "screenmemory_batch")),
                ):
                    with self.subTest(step=step), self.assertRaises(Failure) as caught:
                        send()
                    self.assertEqual(
                        (caught.exception.step, caught.exception.reason),
                        (step, "timeout"),
                    )

    def test_unknown_cursor_key_and_unexpected_source_error_are_distinct(
        self,
    ) -> None:
        self.state.directory.mkdir(parents=True)
        cursor = self.state.directory / "cursor.json"
        cursor.write_text(json.dumps({"frames": 1, "app_usage": 2, "unexpected": 3}))
        code, output = self._cli("run")
        self.assertEqual(code, 1)
        self.assertIn("step=collect reason=state ", output)
        cursor.unlink()
        with patch.object(
            SOURCES["screenmemory"],
            "collect",
            side_effect=RuntimeError("private fixture"),
        ):
            code, output = self._cli("run")
        self.assertEqual(code, 1)
        self.assertIn("step=collect reason=internal ", output)
        self.assertNotIn("private fixture", output)

    def test_preview_leaves_state_unchanged(self) -> None:
        self.state.batch.parent.mkdir(parents=True)
        (self.state.directory / "cursor.json").write_text(
            json.dumps({"frames": 10, "app_usage": 0})
        )
        (self.state.directory / "status.json").write_text(
            json.dumps({"outcome": "delivered"})
        )
        (self.state.directory / "spool/batch.cursor.json").write_text(
            json.dumps({"cursor": {"frames": 11, "app_usage": 0}})
        )
        self.state.batch.write_bytes(b"pending data")
        before = {
            p: p.read_bytes() for p in self.state.directory.rglob("*") if p.is_file()
        }
        out = self.root / "out"
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            del os.environ["MIROIR_TEST_TOKEN"]
            self.assertEqual(
                self._cli(
                    "collect",
                    "--source",
                    "screen_macbook",
                    "--out",
                    str(out),
                    "--max-batches",
                    "2",
                )[0],
                0,
            )
        self.assertEqual(
            self.calls, [{"frames": 10, "app_usage": 0}, {"frames": 11, "app_usage": 0}]
        )
        self.assertEqual(len(list(out.glob("*.jsonl.gz"))), 2)
        self.assertEqual(
            before,
            {p: p.read_bytes() for p in self.state.directory.rglob("*") if p.is_file()},
        )
        self.assertFalse(self.server.posts)

    # ---- Reports (decision 039) ------------------------------------------------

    def _reports(self) -> list[dict]:
        return [json.loads(body) for _, body in self.server.reports]

    def _status_file(self) -> dict:
        return json.loads((self.state.directory / "status.json").read_text())

    def _unreported(self) -> int:
        return json.loads((self.state.directory / "report.json").read_text())[
            "unreported"
        ]

    def _nothing(self, *args: object, **kwargs: object) -> None:
        return None

    def test_each_run_sends_one_report(self) -> None:
        cases = (
            ("delivered", self._fake, 201, 0),
            ("nothing_new", self._nothing, 201, 0),
            ("failed", self._fake, 500, 1),
        )
        for outcome, collect, code, exit_code in cases:
            with (
                self.subTest(outcome=outcome),
                patch.object(SOURCES["screenmemory"], "collect", collect),
            ):
                self.server.reports.clear()
                self.server.response_code = code
                self.assertEqual(self._cli("run")[0], exit_code)
                (report,) = self._reports()
                headers, _ = self.server.reports[0]
                self.assertEqual(headers["Authorization"], f"Bearer {self.token}")
                self.assertEqual(headers["Content-Type"], "application/json")
                self.assertEqual(
                    set(report),
                    {
                        "version",
                        "collector",
                        "source",
                        "started_at",
                        "finished_at",
                        "rows",
                        "media",
                        "pending",
                        "unreported",
                    },
                )
                self.assertEqual(
                    (report["version"], report["collector"], report["source"]),
                    (1, __version__, "screenmemory"),
                )
                for key in ("started_at", "finished_at"):
                    self.assertRegex(report[key], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
                self.assertLessEqual(report["started_at"], report["finished_at"])
                self.assertEqual(
                    set(report["rows"]),
                    {"outcome", "counts", "step", "reason", "http_status"},
                )
                self.assertEqual(report["rows"]["outcome"], outcome)
                self.assertIsNone(report["media"])
                self.assertEqual(report["unreported"], 0)
                self.assertEqual(report["pending"], outcome == "failed")
                status = self._status_file()
                self.assertEqual(status["report"], "sent")
                self.assertEqual(status["outcome"], outcome)
        self.assertEqual(
            self._reports()[0]["rows"],
            {
                "outcome": "failed",
                "counts": {"frames": 1},
                "step": "deliver",
                "reason": "http",
                "http_status": 500,
            },
        )

    def test_report_shapes_of_delivered_and_nothing_new(self) -> None:
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            self.assertEqual(self._cli("run")[0], 0)
        with patch.object(SOURCES["screenmemory"], "collect", self._nothing):
            self.assertEqual(self._cli("run")[0], 0)
        self.assertEqual(
            [report["rows"] for report in self._reports()],
            [
                {
                    "outcome": "delivered",
                    "counts": {"frames": 1},
                    "step": None,
                    "reason": None,
                    "http_status": 201,
                },
                {
                    "outcome": "nothing_new",
                    "counts": {},
                    "step": None,
                    "reason": None,
                    "http_status": None,
                },
            ],
        )

    def test_unsent_reports_do_not_change_the_run_and_are_counted(self) -> None:
        cursor = self.state.directory / "cursor.json"
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            for unreported in (1, 2):
                self.server.report_code = 500
                code, output = self._cli("run")
                self.assertEqual(code, 0)
                self.assertRegex(output, r"^screen_macbook delivered \{")
                self.assertEqual(self._status_file()["report"], "failed")
                self.assertEqual(self._status_file()["outcome"], "delivered")
                self.assertFalse(self.state.has_pending())
                self.assertEqual(
                    json.loads(cursor.read_text()),
                    {"frames": unreported, "app_usage": 0},
                )
                self.assertEqual(self._unreported(), unreported)
            self.assertEqual(
                [report["unreported"] for report in self._reports()], [0, 1]
            )
            # Something unexpected inside reporting is still only a failed report.
            with patch.object(Client, "report", side_effect=RuntimeError("fixture")):
                code, output = self._cli("run")
            self.assertEqual(code, 0)
            self.assertNotIn("fixture", output)
            self.assertEqual(self._status_file()["report"], "failed")
            self.assertEqual(self._status_file()["outcome"], "delivered")
            self.assertEqual(json.loads(cursor.read_text())["frames"], 3)
            # It is still a run that could not be reported.
            self.assertEqual(self._unreported(), 3)
            self.server.report_code = 204
            self.assertEqual(self._cli("run")[0], 0)
            self.assertEqual(self._reports()[-1]["unreported"], 3)
            self.assertEqual(self._status_file()["report"], "sent")
            self.assertEqual(self._unreported(), 0)
            self.assertEqual(self._cli("run")[0], 0)
            self.assertEqual(self._reports()[-1]["unreported"], 0)

    def test_server_down_keeps_the_failed_run_as_it_was(self) -> None:
        with (
            closing(socket()) as endpoint,
            patch.object(SOURCES["screenmemory"], "collect", self._fake),
        ):
            endpoint.bind(("127.0.0.1", 0))
            self.item["receive_url"] = (
                f"http://127.0.0.1:{endpoint.getsockname()[1]}/receive"
            )
            self.path.write_text(json.dumps(self.raw))
            code, output = self._cli("run")
        self.assertEqual(code, 1)
        self.assertIn("failed step=deliver reason=network ", output)
        self.assertNotIn("report", output)
        status = self._status_file()
        self.assertEqual(
            (status["outcome"], status["step"], status["reason"], status["report"]),
            ("failed", "deliver", "network", "failed"),
        )
        self.assertTrue(self.state.has_pending())
        self.assertFalse((self.state.directory / "cursor.json").exists())
        self.assertEqual(self._unreported(), 1)

    def test_a_server_without_reports_is_not_counted(self) -> None:
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            for code in (401, 404, 405):
                with self.subTest(code=code):
                    self.server.report_code = code
                    self.assertEqual(self._cli("run")[0], 0)
                    self.assertEqual(self._status_file()["report"], "unsupported")
                    self.assertFalse((self.state.directory / "report.json").exists())
        self.assertEqual(len(self.server.reports), 3)

    def test_no_report_outside_a_run_or_without_an_address(self) -> None:
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            out = self.root / "out"
            self.assertEqual(
                self._cli("collect", "--source", "screen_macbook", "--out", str(out))[
                    0
                ],
                0,
            )
            self.assertEqual(self._cli("status")[0], 0)
            self.assertEqual(
                self._cli(
                    "resend",
                    "--source",
                    "screen_macbook",
                    "--from",
                    "2026-01-01",
                    "--max-batches",
                    "1",
                )[0],
                0,
            )
            with self.state.lock():
                code, output = self._cli("run")
            self.assertEqual(code, 0)
            self.assertIn("already_running", output)
            self.assertFalse(self.server.reports)
            self.assertEqual(len(self.server.posts), 1)
            self.item["receive_url"] = self.item["receive_url"].replace(
                "/receive", "/ingest"
            )
            self.path.write_text(json.dumps(self.raw))
            self.assertEqual(self._cli("run")[0], 0)
            self.assertEqual(self._status_file()["outcome"], "delivered")
            self.assertEqual(self._status_file()["report"], "skipped")
            self.assertEqual(urlsplit(self.server.posts[-1][0]).path, "/ingest")
            # Without a token, at the usual address: no request at all.
            self.item["receive_url"] = self.item["receive_url"].replace(
                "/ingest", "/receive"
            )
            self.path.write_text(json.dumps(self.raw))
            posts = len(self.server.posts)
            del os.environ["MIROIR_TEST_TOKEN"]
            self.assertEqual(self._cli("run")[0], 1)
            self.assertEqual(self._status_file()["report"], "skipped")
            self.assertEqual(len(self.server.posts), posts)
        self.assertFalse(self.server.reports)
        self.assertFalse((self.state.directory / "report.json").exists())

    def test_report_holds_no_source_text_token_or_address(self) -> None:
        marker = "report-marker-4d1f"
        data = self._source(f"{marker} visible text", f"{marker} more")
        self.assertEqual(self._cli("run")[0], 0)
        ((headers, body),) = self.server.reports
        self.assertEqual(headers["Authorization"], f"Bearer {self.token}")
        text = body.decode()
        rows = json.loads(body)["rows"]
        self.assertEqual(rows["outcome"], "delivered")
        self.assertEqual(rows["counts"]["frames"], 2)
        for value in (
            marker,
            self.token,
            self.item["receive_url"],
            "127.0.0.1",
            str(self.server.server_port),
            str(data),
            "jpeg",
        ):
            self.assertNotIn(value, text)
        self.assertIn(marker, gzip.decompress(self.server.posts[0][2]).decode())

    def test_a_state_folder_from_before_reports_works(self) -> None:
        self.state.directory.mkdir(parents=True)
        (self.state.directory / "cursor.json").write_text(
            json.dumps({"frames": 4, "app_usage": 0})
        )
        (self.state.directory / "status.json").write_text(
            json.dumps({"time": "2026-11-01T00:00:00+00:00", "outcome": "delivered"})
        )
        with patch.object(SOURCES["screenmemory"], "collect", self._fake):
            self.assertEqual(self._cli("run")[0], 0)
            code, output = self._cli("status")
        self.assertEqual(code, 0)
        self.assertEqual(self.calls, [{"frames": 4, "app_usage": 0}])
        (report,) = self._reports()
        self.assertEqual(report["unreported"], 0)
        self.assertEqual(
            json.loads(output.split(" ", 1)[1])["status"]["report"], "sent"
        )
        self.assertEqual(self._unreported(), 0)


class ConfigurationMessageTests(unittest.TestCase):
    def run_status(self, document: object) -> str:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "collector.json"
            path.write_text(
                document if isinstance(document, str) else json.dumps(document)
            )
            output = io.StringIO()
            with redirect_stdout(output):
                code = main(["status", "--config", str(path)])
            self.assertEqual(code, 1)
            return output.getvalue()

    def source(self, **changes: object) -> dict[str, object]:
        base: dict[str, object] = {
            "id": "screen",
            "source": "screenmemory",
            "receive_url": "https://miroir.example/api/connections/screen/receive",
            "token_env": "MIROIR_TOKEN",
            "options": {"data_dir": "/nowhere"},
        }
        return base | changes

    def test_legacy_configuration_keys_are_rejected(self) -> None:
        entry = self.source()
        entry["collector"] = entry.pop("source")
        self.assertEqual(
            self.run_status({"state_dir": "/tmp/x", "collectors": [entry]}),
            "Invalid collector configuration: top level: unknown key collectors\n",
        )
        self.assertIn(
            "sources.0.collector: unknown key",
            self.run_status(
                {
                    "state_dir": "/tmp/x",
                    "sources": [self.source(collector="screenmemory")],
                }
            ),
        )

    def test_the_message_says_what_is_wrong_and_never_echoes_a_value(self):
        cases = {
            "sources.0.media: unknown key": {
                "state_dir": "/tmp/x",
                "sources": [self.source(media="marker-value")],
            },
            "sources.0.options.cutoff: unknown key": {
                "state_dir": "/tmp/x",
                "sources": [self.source(options={"data_dir": "/nowhere", "cutoff": 0})],
            },
            "source: unknown key": {
                "state_dir": "/tmp/x",
                "source": [],
                "sources": [],
            },
            "sources.0.options.media: expected a boolean": {
                "state_dir": "/tmp/x",
                "sources": [
                    self.source(options={"data_dir": "/nowhere", "media": "true"})
                ],
            },
            "sources.0.receive_url: Invalid receiving URL": {
                "state_dir": "/tmp/x",
                "sources": [self.source(receive_url="ftp://private-host/x")],
            },
            "sources.0.source: unknown source": {
                "state_dir": "/tmp/x",
                "sources": [self.source(source="marker-value")],
            },
            "sources.0.options.max_rows: expected an integer": {
                "state_dir": "/tmp/x",
                "sources": [
                    self.source(
                        options={"data_dir": "/nowhere", "max_rows": "marker-value"}
                    )
                ],
            },
            "sources.0.options.max_rows: number out of range": {
                "state_dir": "/tmp/x",
                "sources": [
                    self.source(options={"data_dir": "/nowhere", "max_rows": 200001})
                ],
            },
            "sources.0.options.since: Invalid date or offset-qualified instant": {
                "state_dir": "/tmp/x",
                "sources": [
                    self.source(
                        options={"data_dir": "/nowhere", "since": "marker-value"}
                    )
                ],
            },
            "not valid JSON": '{"state_dir": "/tmp/x", "sources": [',
        }
        for expected, document in cases.items():
            output = self.run_status(document)
            self.assertIn("Invalid collector configuration: ", output, expected)
            self.assertIn(expected, output)
            self.assertNotIn("private-host", output)
            self.assertNotIn("/nowhere", output)
            self.assertNotIn("marker-value", output)
            self.assertNotIn("200001", output)
