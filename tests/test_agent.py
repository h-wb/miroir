import ast
import io
import json
import os
import stat
import sys
import unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch

from src.agent.cli import main
from src.agent.collectors import COLLECTORS, Collected
from src.agent.config import load_config
from src.agent.deliver import deliver
from src.agent.masking import mask
from src.agent.state import State, write_json


class MaskingTests(unittest.TestCase):
    def test_prefixed_assignments(self) -> None:
        for prefix, value in (
            ("POSTGRES_PASSWORD=", "S3cret" + "pass9"),
            ("db_password: ", "hunter2" * 2),
            ("access_token=", "Ab12cd" + "34ef56"),
            ("AWS_SECRET_ACCESS_KEY=", "abc/" * 10),
        ):
            self.assertEqual(
                mask(prefix + value),
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
            mask(f"{inline}\n{following}"),
            (f"[masked:private_key]\n{following}", {"private_key": 1}),
        )

    def test_exact_length_tokens_reject_continuations(self) -> None:
        for value in (
            "AI" + "za" + "0" * 35 + "-",
            "np" + "m_" + "a" * 37,
            "AK" + "IA" + "A" * 17,
            "AS" + "IA" + "A" * 17,
        ):
            self.assertNotIn("token", mask(value)[1])

    def test_card_is_not_part_of_a_longer_number(self) -> None:
        card = "4" + "1" * 15
        for separator in ("", " ", "-"):
            for value in (f"{card}{separator}2", f"2{separator}{card}"):
                self.assertEqual(mask(value), (value, {}))

    def test_standard_library_only(self) -> None:
        for path in Path("src/agent").rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom) and node.level == 0:
                    names = [node.module or ""]
                elif isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                else:
                    continue
                for name in names:
                    self.assertTrue(
                        name.split(".")[0] in sys.stdlib_module_names
                        or name == "src.agent"
                        or name.startswith("src.agent."),
                        (path, name),
                    )

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
                self.assertEqual(mask(f"<{token}>"), ("<[masked:token]>", {"token": 1}))
        bearer = "bEaReR " + "x+/=" * 5
        self.assertEqual(mask(bearer), ("bEaReR [masked:token]", {"token": 1}))

    def test_other_rules_and_determinism(self) -> None:
        key = "-----BEGIN RSA " + "PRIVATE KEY-----\n" + "a+/=" * 5
        with_end = key + "\n-----END RSA " + "PRIVATE KEY-----"
        self.assertEqual(
            mask(key + "\nnext line"),
            ("[masked:private_key]\nnext line", {"private_key": 1}),
        )
        assignment = 'client-secret: "' + "unusual" + "2" + '"'
        random = "Ab3_dEf6" + "GhIj9kLm-NoPqRsTuVwXyZ012345"
        card = "4" + "1" * 3 + "-" + "1111-" * 2 + "1" * 4
        text = f"{with_end}\n{assignment}\n{random}\n{card}"
        expected = (
            '[masked:private_key]\nclient-secret: "[masked:assignment]"\n[masked:random]\n[masked:card]',
            {"private_key": 1, "assignment": 1, "random": 1, "card": 1},
        )
        self.assertEqual(mask(text), expected)
        self.assertEqual(mask(text), expected)

    def test_preserves_ordinary_text(self) -> None:
        for value in (
            "abcde01234" * 4,
            "ABCDabcd01234567" * 4,
            "aabbccdd-1234-4321-9876-abcdef012345",
            "a-long-kebab-case-name-for-a-local-application",
            "token: counting",
            "password: required",
            "1234567890123",
        ):
            self.assertEqual(mask(value), (value, {}))


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.server.posts.append(
            (self.headers, self.rfile.read(int(self.headers["Content-Length"])))
        )
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


class AgentTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        server = HTTPServer(("127.0.0.1", 0), _Handler)
        self.server = server
        server.posts = []
        server.gets = []
        server.response_code = 201
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        self.token = "demo" + "-receive"
        env = patch.dict(
            os.environ, {"MIROIR_TEST_TOKEN": self.token, "no_proxy": "127.0.0.1"}
        )
        env.start()
        self.addCleanup(env.stop)
        self.path = self.root / "config.json"
        self.item = {
            "id": "screen_macbook",
            "collector": "screenmemory",
            "receive_url": f"http://127.0.0.1:{server.server_port}/receive",
            "token_env": "MIROIR_TEST_TOKEN",
            "options": {"data_dir": "~/ScreenMemoryData"},
        }
        self.raw = {"state_dir": str(self.root / "state"), "collectors": [self.item]}
        self.path.write_text(json.dumps(self.raw))
        self.state = State(self.root / "state" / "screen_macbook")
        self.calls = []

    def _cli(self, *args: str) -> tuple[int, str]:
        output = io.StringIO()
        with redirect_stdout(output):
            code = main([*args, "--config", str(self.path)])
        return code, output.getvalue()

    def _fake(
        self, options: dict, cursor: dict | None, out: Path, now: float
    ) -> Collected:
        self.calls.append(cursor)
        step = (cursor or {}).get("frames", 0) + 1
        out.write_bytes(f"fixture text {step}".encode())
        return Collected({"frames": step}, {"frames": 1}, True)

    def test_config(self) -> None:
        self.item["receive_url"] = (
            "https://miroir.example/api/connections/screen_macbook/receive"
        )
        example = {
            **self.raw,
            "state_dir": "~/Library/Application Support/miroir-agent",
        }
        self.path.write_text(json.dumps(example))
        config = load_config(self.path)
        self.assertEqual(
            config.state_dir, Path.home() / "Library/Application Support/miroir-agent"
        )
        self.assertEqual(config.collectors[0].timeout_seconds, 120)
        self.assertEqual(
            config.collectors[0].options["data_dir"],
            str(Path.home() / "ScreenMemoryData"),
        )
        for changes in (
            {"wat": True},
            {"id": "Bad-id"},
            {"collector": "unknown"},
            {"receive_url": "http://miroir.example/receive"},
            {"timeout_seconds": True},
            {"options": {"data_dir": "data", "max_rows": 200001}},
        ):
            self.path.write_text(
                json.dumps({**self.raw, "collectors": [{**self.item, **changes}]})
            )
            with self.assertRaises(ValueError):
                load_config(self.path)
        self.path.write_text(
            json.dumps({**self.raw, "collectors": [self.item, self.item]})
        )
        with self.assertRaises(ValueError):
            load_config(self.path)

    def test_config_rejects_blank_drop_list_entries(self) -> None:
        for field in ("drop_bundle_ids", "drop_markers"):
            for entry in ("", " \t\n"):
                self.item["options"] = {"data_dir": "data", field: [entry]}
                self.path.write_text(json.dumps(self.raw))
                with (
                    self.subTest(field=field, entry=entry),
                    self.assertRaises(ValueError),
                ):
                    load_config(self.path)

    def test_atomic_write_syncs_file_then_directory(self) -> None:
        path = self.root / "atomic.json"
        replace = os.replace
        steps: list[str] = []

        def sync(fd: int) -> None:
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                self.assertEqual(json.loads(path.read_text()), {"frames": 1})
                steps.append("directory")
            else:
                self.assertFalse(path.exists())
                self.assertGreater(os.fstat(fd).st_size, 0)
                steps.append("file")

        def rename(source: Path, destination: Path) -> None:
            steps.append("replace")
            replace(source, destination)

        with (
            patch("src.agent.state.os.fsync", side_effect=sync),
            patch("src.agent.state.os.replace", side_effect=rename),
        ):
            write_json(path, {"frames": 1})
        self.assertEqual(steps, ["file", "replace", "directory"])

    def test_delivery(self) -> None:
        config = load_config(self.path).collectors[0]
        body = self.root / "batch.gz"
        body.write_bytes(b"batch body")
        for code in (201, 500, 302):
            self.server.response_code = code
            observed = deliver(config, body)
            self.assertEqual(observed.status, code)
            self.assertEqual(observed.acknowledged, code == 201)
        self.assertEqual(len(self.server.posts), 3)
        self.assertFalse(self.server.gets)
        for headers, contents in self.server.posts:
            self.assertEqual(headers["Authorization"], f"Bearer {self.token}")
            self.assertEqual(headers["Content-Type"], "application/gzip")
            self.assertEqual(contents, body.read_bytes())

    def test_retry_lock_limit_and_status(self) -> None:
        with patch.dict(COLLECTORS, {"screenmemory": self._fake}):
            self.state.batch.parent.mkdir(parents=True)
            self.state.batch.write_bytes(b"incomplete batch")
            self.server.response_code = 500
            code, output = self._cli("run")
            self.assertEqual(code, 1)
            self.assertIn("failed", output)
            self.assertTrue(self.state.pending)
            self.assertFalse(self.state.cursor.exists())
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
                [body for _, body in self.server.posts], [pending, pending]
            )
            self.assertFalse(self.state.pending)
            self.assertEqual(json.loads(self.state.cursor.read_text()), {"frames": 1})
            self.assertEqual(self._cli("run", "--max-batches", "2")[0], 0)
            self.assertEqual(self.calls, [None, {"frames": 1}, {"frames": 2}])
            code, output = self._cli("status")
            self.assertEqual(code, 0)
            self.assertNotIn("fixture text", output)
            self.assertNotIn(self.token, output)
            status = json.loads(output.split(" ", 1)[1])
            self.assertEqual(status["cursor"], {"frames": 3})
            self.assertFalse(status["pending"])
            self.assertEqual(status["status"]["outcome"], "delivered")
            self.assertEqual(status["status"]["artifact_id"], "artifact-one")

    def test_missing_token_preserves_pending_batch(self) -> None:
        with patch.dict(COLLECTORS, {"screenmemory": self._fake}):
            del os.environ["MIROIR_TEST_TOKEN"]
            self.assertEqual(self._cli("run")[0], 1)
            self.assertTrue(self.state.pending)
            self.assertFalse(self.state.cursor.exists())
            self.assertFalse(self.server.posts)
            self.assertEqual(self.calls, [None])

    def test_preview_leaves_state_unchanged(self) -> None:
        self.state.batch.parent.mkdir(parents=True)
        write_json(self.state.cursor, {"frames": 10})
        write_json(self.state.status, {"outcome": "delivered"})
        write_json(self.state.batch_cursor, {"cursor": {"frames": 11}})
        self.state.batch.write_bytes(b"pending data")
        before = {
            p: p.read_bytes() for p in self.state.directory.rglob("*") if p.is_file()
        }
        out = self.root / "out"
        with patch.dict(COLLECTORS, {"screenmemory": self._fake}):
            del os.environ["MIROIR_TEST_TOKEN"]
            self.assertEqual(
                self._cli(
                    "collect",
                    "--collector",
                    "screen_macbook",
                    "--out",
                    str(out),
                    "--max-batches",
                    "2",
                )[0],
                0,
            )
        self.assertEqual(self.calls, [{"frames": 10}, {"frames": 11}])
        self.assertEqual(len(list(out.glob("*.jsonl.gz"))), 2)
        self.assertEqual(
            before,
            {p: p.read_bytes() for p in self.state.directory.rglob("*") if p.is_file()},
        )
        self.assertFalse(self.server.posts)
