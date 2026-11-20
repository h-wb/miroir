import gzip
import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from src.agent.collectors.screenmemory import SOURCE_ERROR, collect
from src.integrations.screenmemory.formats import read_records, validate_batch

_TS = 1795132100


class ScreenMemoryCollectorTests(unittest.TestCase):
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

    def test_paging_keeps_timestamp_group_together(self) -> None:
        self._source(
            [
                (_TS + offset, f"{i}.jpeg", None)
                for i, offset in enumerate((0, 1, 2, 2, 3))
            ],
            [],
        )
        self.options["max_rows"] = 3
        # The second page is also exactly full, so conservatively advertises more.
        first = collect(self.options, None, self.out, self.now)
        assert first is not None
        self.assertTrue(first.more)
        self.assertEqual(first.cursor, {"frames": _TS + 1, "app_usage": 0})
        records = list(read_records(self.out))
        second = collect(self.options, first.cursor, self.out, self.now)
        assert second is not None
        self.assertTrue(second.more)
        self.assertEqual(second.cursor, {"frames": _TS + 2, "app_usage": 0})
        records.extend(read_records(self.out))
        third = collect(self.options, second.cursor, self.out, self.now)
        assert third is not None
        self.assertFalse(third.more)
        self.assertEqual(third.cursor, {"frames": _TS + 3, "app_usage": 0})
        records.extend(read_records(self.out))
        self.assertEqual(
            [row["image_path"] for row in records], [f"{i}.jpeg" for i in range(5)]
        )
        self.assertTrue(all(row["text"] == "" for row in records))

    def test_invalid_rows_are_skipped(self) -> None:
        self._source(
            [
                (_TS, "../bad", "fixture"),
                (1, "early.jpg", "fixture"),
                (str(_TS + 1), "ok.jpeg", None),
            ],
            [(_TS, ""), (_TS + 2, "app.example.browser")],
        )
        batch = collect(self.options, None, self.out, self.now)
        assert batch is not None
        self.assertEqual(batch.counts["skipped"], 3)
        validate_batch(self.out)
        self.assertIsNone(collect(self.options, batch.cursor, self.out, self.now))

    def test_fractional_timestamps_use_whole_seconds(self) -> None:
        self._source(
            [
                (_TS + 0.2, "a.jpeg", "test"),
                (_TS + 0.5, "b.jpeg", "test"),
                (_TS + 1, "c.jpeg", "later"),
            ],
            [(_TS + 0.8, "com.apple.Passwords"), (_TS + 1, "app.example.later")],
        )
        now = _TS + 300.3
        batch = collect(self.options, None, self.out, now)
        assert batch is not None
        self.assertEqual(batch.cursor, {"frames": _TS, "app_usage": _TS})
        records = list(read_records(self.out))
        self.assertEqual([row["ts"] for row in records], [_TS] * 3)
        self.assertEqual(
            [row["image_path"] for row in records[:2]], ["a.jpeg", "b.jpeg"]
        )
        self.assertEqual([row["withheld"] for row in records[:2]], ["excluded_app"] * 2)
        self.assertIsNone(collect(self.options, batch.cursor, self.out, now))

    def test_decodes_invalid_utf8_and_blob_text(self) -> None:
        self._source([(_TS, "blob.jpeg", b"blob \xff")], [])
        with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
            db.execute(
                "INSERT INTO ocrresults VALUES (?, ?, CAST(? AS TEXT))",
                (_TS + 1, "text.jpeg", b"text \xfe"),
            )
        self.assertIsNotNone(collect(self.options, None, self.out, self.now))
        self.assertEqual(
            [r["text"] for r in read_records(self.out)], ["blob \ufffd", "text \ufffd"]
        )

    def test_fetches_entire_second_when_page_is_full(self) -> None:
        self.options["max_rows"] = 2
        self._source(
            [(_TS + i / 10, f"{i}.jpeg", None) for i in range(3)],
            [(_TS + i / 10, f"app.example.{i}") for i in range(3)],
        )
        batch = collect(self.options, None, self.out, self.now)
        assert batch is not None
        self.assertTrue(batch.more)
        self.assertEqual(batch.cursor, {"frames": _TS, "app_usage": _TS})
        records = list(read_records(self.out))
        self.assertEqual(
            [r["image_path"] for r in records[:3]], [f"{i}.jpeg" for i in range(3)]
        )
        self.assertEqual(
            [r["bundle_id"] for r in records[3:]],
            [f"app.example.{i}" for i in range(3)],
        )
        self.assertIsNone(collect(self.options, batch.cursor, self.out, self.now))

    def test_queries_ignore_unreadable_timestamps(self) -> None:
        invalid = [None, f"{_TS}bad", b"\xff", "nan"]
        self._source(
            [(ts, "bad.jpeg", None) for ts in invalid],
            [(ts, "app.example.bad") for ts in invalid],
        )
        with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
            db.execute(
                "INSERT INTO ocrresults VALUES (CAST(? AS TEXT), ?, NULL)",
                (b"\xff", "bad.jpeg"),
            )
        self.assertIsNone(collect(self.options, None, self.out, self.now))
        self.assertFalse(self.out.exists())

    def test_missing_file_table_and_column(self) -> None:
        with closing(sqlite3.connect(self.data / "usage.sqlite")) as db, db:
            db.execute("CREATE TABLE usage(timestamp, identifier)")
        for case in ("file", "table", "column"):
            with self.subTest(case=case), self.assertRaises(ValueError) as raised:
                collect(self.options, None, self.out, self.now)
            self.assertEqual(str(raised.exception), SOURCE_ERROR)
            self.assertNotIn(str(self.data), str(raised.exception))
            if case != "column":
                with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
                    if case == "table":
                        db.execute("CREATE TABLE ocrresults(imagetimestamp, imagepath)")
        self.assertFalse(self.out.exists())
