import gzip
import json
import sqlite3
import sys
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from src.integrations.screenmemory.formats import read_records, validate_batch

_TS = 1795132100


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

    def test_mask_exclude_and_connector_contract(self) -> None:
        agent = Path(__file__).resolve().parents[1] / "agent"
        with patch.object(sys, "path", [str(agent), *sys.path]):
            from miroir_agent.collectors.screenmemory import collect

        token = "gh" + "p_" + "a1B2" * 9
        paths = [f"date/{i}.jpeg" for i in range(5)]
        self._source(
            [
                (_TS + i, paths[i], text)
                for i, text in enumerate(
                    (
                        "ordinary",
                        token,
                        "app fixture text",
                        "ABOUT:logins",
                        "too recent",
                    )
                )
            ][:-1]
            + [(_TS + 41, paths[4], "too recent")],
            [
                (_TS, "app.example.browser"),
                (_TS + 2, "com.apple.Passwords"),
                (_TS + 41, "app.example.new"),
            ],
        )
        batch = collect(self.options, None, self.out, self.now)
        assert batch is not None
        validate_batch(self.out)
        with gzip.open(self.out, "rt", encoding="utf-8") as gz:
            raw = gz.read()
        self.assertNotIn(token, raw)
        self.assertNotIn("app fixture text", raw)
        self.assertNotIn("ABOUT:logins", raw)
        self.assertNotIn("too recent", raw)
        lines = [json.loads(line) for line in raw.splitlines()]
        records = list(read_records(self.out))
        self.assertEqual(records, lines[1:-1])
        frames = records[:4]
        self.assertEqual([row["image_path"] for row in frames], paths[:4])
        self.assertEqual(
            [row["text"] for row in frames], ["ordinary", "[masked:token]", None, None]
        )
        self.assertEqual(
            [row["withheld"] for row in frames], [None, None, "excluded_app", "marker"]
        )
        self.assertEqual([row["masked"] for row in frames], [0, 1, 0, 0])
        self.assertEqual(
            [row["bundle_id"] for row in records[4:]],
            ["app.example.browser", "com.apple.Passwords"],
        )
        self.assertEqual(
            lines[-1],
            {
                "type": "trailer",
                "frames": 4,
                "app_usage": 2,
                "masked": {"token": 1},
                "withheld": {"excluded_app": 1, "marker": 1},
            },
        )
        self.assertEqual(batch.cursor, {"frames": _TS + 3, "app_usage": _TS + 2})
        self.assertIsNone(
            collect(self.options, batch.cursor, self.root / "second.gz", self.now)
        )
        self.assertFalse((self.root / "second.gz").exists())
