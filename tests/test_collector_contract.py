import gzip
import json
import os
import socket
import sqlite3
import sys
import time
import unittest
from contextlib import closing, redirect_stdout
from importlib import import_module
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any
from unittest.mock import patch

import uvicorn

from src.core.operations import MetadataStore
from src.core.service import InlineExecutor, MiroirService
from src.integrations.apple_messages import formats as apple_formats
from src.integrations.browser_history.formats import read_rows
from src.integrations.browser_history.formats import (
    validate_batch as validate_zen_batch,
)
from src.integrations.screenmemory.formats import read_records, validate_batch
from src.web.app import create_app
from tests.fakes import ServedObjectStore
from tests.test_browser_history_batch import snapshot

_TS = 1795132100


@unittest.skipIf(sys.version_info < (3, 12), "collector requires Python 3.12")
class AppleMessagesContractTests(unittest.TestCase):
    def test_collected_batch_matches_connector_format(self):
        collector = Path(__file__).resolve().parents[1] / "collector"
        with patch.object(sys, "path", [str(collector), *sys.path]):
            from miroir_collector.masking import Masker
            from miroir_collector.sources.apple_messages import (
                AppleMessages,
                AppleMessagesOptions,
            )

        fixture = import_module("collector.tests.test_apple_messages_source")
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "Messages"
            data.mkdir()
            database = data / "chat.db"
            with closing(sqlite3.connect(database)) as db:
                db.executescript(
                    fixture.SCHEMA
                    + """
                    INSERT INTO message (ROWID, guid, text, date, handle_id) VALUES (91, 'message-1', 'hello', 800000000, 81);
                    INSERT INTO chat (ROWID, guid) VALUES (71, 'chat-1');
                    INSERT INTO handle (ROWID, id, service) VALUES (81, 'handle-1', 'iMessage');
                    INSERT INTO attachment (ROWID, guid, transfer_name) VALUES (61, 'attachment-1', 'IMG_1.HEIC');
                    INSERT INTO chat_message_join VALUES (71, 91);
                    INSERT INTO message_attachment_join VALUES (91, 61);
                    INSERT INTO chat_handle_join VALUES (71, 81);
                    INSERT INTO message (ROWID, guid, text, subject, date, handle_id)
                        VALUES (92, 'message-2', CAST(X'48ff69' AS TEXT),
                        CAST(X'ff' AS TEXT), 800000001, 82);
                    INSERT INTO handle (ROWID, id, service)
                        VALUES (82, CAST(X'74ff' AS TEXT), 'SMS');
                    INSERT INTO handle (ROWID, id, service)
                        VALUES (83, 'a' || char(0) || 'b', 'SMS');
                    INSERT INTO chat (ROWID, guid, chat_identifier)
                        VALUES (72, 'chat-2', 'c' || char(0) || 'd');
                    INSERT INTO chat_handle_join VALUES (72, 83);
                """
                )
            path = root / "batch.gz"
            batch = AppleMessages().collect(
                AppleMessagesOptions.model_validate({"database": str(database)}),
                None,
                path,
                _TS,
                masker=Masker(),
            )
            self.assertIsNotNone(batch)
            apple_formats.validate_batch(path)
            records = list(apple_formats.read_records(path))
            # Contacts belong to the format; this version of the source sends none.
            self.assertEqual(
                {row["type"] for row in records},
                {kind for kind in apple_formats.FIELDS if kind != "contact"},
            )
            self.assertEqual(
                [row["text"] for row in records if row["type"] == "message"],
                ["hello", "H\ufffdi"],
            )
            # Keys holding NUL or undecodable bytes are skipped, not altered.
            self.assertEqual(
                [row["id"] for row in records if row["type"] == "handle"],
                ["handle-1"],
            )
            self.assertEqual(batch.counts["skipped"], 3)
            attachment = next(row for row in records if row["type"] == "attachment")
            self.assertEqual(attachment["media_key"], "attachment-1.heic")
            self.assertEqual(batch.counts["contact"], 0)
            self.assertEqual(
                (batch.counts["resent"], batch.counts["layout_changed"]), (0, 0)
            )

            # Masks outside messages, in a batch with no chat row.
            token = fixture.github_token()
            with closing(sqlite3.connect(database)) as db, db:
                db.executescript(
                    f"""
                    ALTER TABLE message ADD COLUMN group_title TEXT;
                    INSERT INTO message (ROWID, guid, text, date, group_title)
                        VALUES (93, 'message-3', 'hi', 800000002, 'PASSWORD=GroupTitle42');
                    INSERT INTO attachment (ROWID, guid, transfer_name)
                        VALUES (62, 'attachment-2', '{token}.JPG');
                    INSERT INTO message_attachment_join VALUES (93, 62);
                    INSERT INTO message (ROWID, guid, text, date)
                        VALUES (94, 'message-4', 'hi', 800000003);
                    INSERT INTO attachment (ROWID, guid, transfer_name)
                        VALUES (63, 'attachment-3', '{token}.PNG');
                    INSERT INTO message_attachment_join VALUES (94, 63);
                    """
                )
            path.unlink()
            # Chats are unchanged, so this batch holds no chat row.
            second = AppleMessages().collect(
                AppleMessagesOptions.model_validate({"database": str(database)}),
                batch.cursor,
                path,
                _TS,
                masker=Masker(),
            )
            self.assertIsNotNone(second)
            apple_formats.validate_batch(path)
            records = list(apple_formats.read_records(path))
            self.assertNotIn("chat", {row["type"] for row in records})
            messages = {r["guid"]: r for r in records if r["type"] == "message"}
            self.assertEqual(
                messages["message-3"]["group_title"], "PASSWORD=[masked:assignment]"
            )
            self.assertEqual(
                {r["media_key"] for r in records if r["type"] == "attachment"},
                {"attachment-2.jpg", "attachment-3.png"},
            )
            self.assertEqual(second.counts["masked"], {"assignment": 1, "token": 2})
            self.assertNotIn(token, gzip.decompress(path.read_bytes()).decode())


@unittest.skipIf(sys.version_info < (3, 12), "collector requires Python 3.12")
class ZenHistoryContractTests(unittest.TestCase):
    def test_collected_batch_matches_connector_format(self):
        collector = Path(__file__).resolve().parents[1] / "collector"
        with patch.object(sys, "path", [str(collector), *sys.path]):
            from miroir_collector.masking import Masker
            from miroir_collector.sources.zen_history import (
                ZenHistory,
                ZenHistoryOptions,
            )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            profile = root / "profile"
            profile.mkdir()
            source = snapshot(profile / "places.sqlite")
            out = root / "batch.jsonl.gz"
            self.assertIsNotNone(
                ZenHistory().collect(
                    ZenHistoryOptions.model_validate({"places_path": str(source)}),
                    None,
                    out,
                    _TS,
                    masker=Masker(),
                )
            )
            validate_zen_batch(out)
            with gzip.open(out, "rt", encoding="utf-8") as file:
                document = [json.loads(line) for line in file]
            rows = list(read_rows(out))
            self.assertEqual(
                rows, [(item["table"], item["row"]) for item in document[1:-1]]
            )
            self.assertEqual(
                [(table, row["id"]) for table, row in rows],
                [
                    ("moz_places", 1),
                    ("moz_places", 2),
                    ("moz_historyvisits", 1),
                ],
            )


@unittest.skipIf(sys.version_info < (3, 12), "collector requires Python 3.12")
class ScreenMemoryContractTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.data = self.root / "local source #1"
        self.data.mkdir()
        self.out = self.root / "batch.jsonl.gz"
        self.options: dict[str, Any] = {"data_dir": str(self.data)}
        self.now = _TS + 340

    def _source(self, frames: list[tuple], apps: list[tuple]) -> None:
        with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
            db.execute("CREATE TABLE ocrresults(imagetimestamp, imagepath, imagetext)")
            db.executemany("INSERT INTO ocrresults VALUES (?, ?, ?)", frames)
        with closing(sqlite3.connect(self.data / "usage.sqlite")) as db, db:
            db.execute("CREATE TABLE usage(timestamp, identifier)")
            db.executemany("INSERT INTO usage VALUES (?, ?)", apps)

    def test_uploads_media_to_real_app(self) -> None:
        collector = Path(__file__).resolve().parents[1] / "collector"
        with patch.object(sys, "path", [str(collector), *sys.path]):
            from miroir_collector.cli import main

        self._source(
            [
                (_TS, "a.jpg", "ordinary"),
                (_TS + 1, "b.jpeg", "ordinary"),
                (_TS + 2, "withheld.jpg", "ABOUT:logins"),
            ],
            [],
        )
        images = self.data / "screenshots"
        images.mkdir()
        for name, body in [
            ("a.jpg", b"abc"),
            ("b.jpeg", b"defg"),
            ("withheld.jpg", b"abcde"),
        ]:
            (images / name).write_bytes(body)

        config = self.root / "miroir.json"
        config.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "test",
                    "media": {"endpoint": "https://objects.test", "bucket": "media"},
                    "connections": [
                        {"id": "screen", "integration": "screenmemory", "settings": {}}
                    ],
                }
            )
        )
        metadata = MetadataStore(f"sqlite:///{self.root / 'meta.sqlite'}")
        self.addCleanup(metadata.engine.dispose)
        metadata.migrate()
        objects = ServedObjectStore()
        self.addCleanup(objects.close)
        service = MiroirService(
            config,
            metadata,
            self.root / "artifacts",
            InlineExecutor(lambda _: None),
            object_store=objects,
        )
        token = service.create_delivery_token("screen")
        listener = socket.socket()
        self.addCleanup(listener.close)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(create_app(service, "unused" * 3), log_level="critical")
        )
        thread = Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)

        def stop() -> None:
            server.should_exit = True
            thread.join(5)

        thread.start()
        self.addCleanup(stop)
        deadline = time.monotonic() + 5
        while not server.started and thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(server.started)

        config = self.root / "collector.json"
        config.write_text(
            json.dumps(
                {
                    "state_dir": str(self.root / "state"),
                    "sources": [
                        {
                            "id": "screen",
                            "source": "screenmemory",
                            "token_env": "TEST_TOKEN",
                            "receive_url": f"http://127.0.0.1:{port}/api/connections/screen/receive",
                            "options": {
                                **self.options,
                                "media": True,
                                "settle_seconds": 0,
                            },
                        }
                    ],
                }
            )
        )
        expected = {"screen/screenshots/a.jpg": 3, "screen/screenshots/b.jpeg": 4}
        with patch.dict(os.environ, TEST_TOKEN=token), redirect_stdout(StringIO()):
            self.assertEqual(main(["run", "--config", str(config)]), 0)
            self.assertEqual(objects.objects, expected)
            self.assertEqual(
                objects.content_types, dict.fromkeys(expected, "image/jpeg")
            )
            self.assertEqual(objects.put_count, 2)
            self.assertEqual(main(["run", "--config", str(config)]), 0)
            self.assertEqual(objects.put_count, 2)

    def test_mask_exclude_and_connector_contract(self) -> None:
        collector = Path(__file__).resolve().parents[1] / "collector"
        with patch.object(sys, "path", [str(collector), *sys.path]):
            from miroir_collector.masking import Masker
            from miroir_collector.sources.screenmemory import (
                ScreenMemory,
                ScreenMemoryOptions,
            )

        # Standard base64 can overlap the collector's random-string rule. Use a
        # scanner-only URI credential instead to exercise the scanner label.
        token = "https://reader:p%40ss@[::1]/"
        paths = [f"date/{i}.jpeg" for i in range(6)]
        unscannable = "private unscannable text"
        self._source(
            [
                (_TS + i, paths[i], text)
                for i, text in enumerate(
                    (
                        "ordinary",
                        token,
                        "app fixture text",
                        "ABOUT:logins",
                        unscannable,
                        "too recent",
                    )
                )
            ][:-1]
            + [(_TS + 41, paths[5], "too recent")],
            [
                (_TS, "app.example.browser"),
                (_TS + 2, "com.apple.Passwords"),
                (_TS + 41, "app.example.new"),
            ],
        )
        masker = Masker()
        from kingfisher_sdk import Scanner

        scan = Scanner.scan

        def scan_fixture(scanner, text, *, timeout):
            if text == unscannable:
                raise TimeoutError("private fixture")
            return scan(scanner, text, timeout=timeout)

        with patch.object(Scanner, "scan", scan_fixture):
            batch = ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                None,
                self.out,
                self.now,
                masker=masker,
            )
        assert batch is not None
        validate_batch(self.out)
        with gzip.open(self.out, "rt", encoding="utf-8") as gz:
            raw = gz.read()
        self.assertNotIn(token, raw)
        self.assertNotIn("app fixture text", raw)
        self.assertNotIn("ABOUT:logins", raw)
        self.assertNotIn("too recent", raw)
        self.assertNotIn(unscannable, raw)
        lines = [json.loads(line) for line in raw.splitlines()]
        records = list(read_records(self.out))
        self.assertEqual(records, lines[1:-1])
        frames = records[:5]
        self.assertEqual([row["image_path"] for row in frames], paths[:5])
        self.assertEqual(
            [row["text"] for row in frames],
            [
                "ordinary",
                "https://reader:[masked:scanner]@[::1]/",
                None,
                None,
                "[masked:unscanned]",
            ],
        )
        self.assertEqual(
            [row["withheld"] for row in frames],
            [None, None, "excluded_app", "marker", None],
        )
        self.assertEqual([row["masked"] for row in frames], [0, 1, 0, 0, 1])
        self.assertEqual(
            [row["bundle_id"] for row in records[5:]],
            ["app.example.browser", "com.apple.Passwords"],
        )
        self.assertEqual(
            lines[-1],
            {
                "type": "trailer",
                "frames": 5,
                "app_usage": 2,
                "masked": {"scanner": 1, "unscanned": 1},
                "withheld": {"excluded_app": 1, "marker": 1},
            },
        )
        self.assertEqual(
            batch.cursor.model_dump(), {"frames": _TS + 4, "app_usage": _TS + 2}
        )
        self.assertIsNone(
            ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                batch.cursor,
                self.root / "second.gz",
                self.now,
                masker=masker,
            )
        )
        self.assertFalse((self.root / "second.gz").exists())
