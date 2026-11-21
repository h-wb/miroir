import gzip
import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from miroir_collector.errors import Failure
from miroir_collector.sources.screenmemory import (
    ScreenMemory,
    ScreenMemoryCursor,
    ScreenMemoryOptions,
)

from .masking_fixtures import MASKER

_TS = 1795132100


class ScreenMemorySourceTests(unittest.TestCase):
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

    def _records(self) -> list[dict[str, Any]]:
        with gzip.open(self.out, "rt", encoding="utf-8") as gz:
            lines = [json.loads(line) for line in gz]
        return lines[1:-1]  # Skip the batch header and trailer.

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
        first = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            None,
            self.out,
            self.now,
            masker=MASKER,
        )
        assert first is not None
        self.assertTrue(first.more)
        self.assertEqual(first.cursor, ScreenMemoryCursor(frames=_TS + 1, app_usage=0))
        records = list(self._records())
        second = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            first.cursor,
            self.out,
            self.now,
            masker=MASKER,
        )
        assert second is not None
        self.assertTrue(second.more)
        self.assertEqual(second.cursor, ScreenMemoryCursor(frames=_TS + 2, app_usage=0))
        records.extend(self._records())
        third = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            second.cursor,
            self.out,
            self.now,
            masker=MASKER,
        )
        assert third is not None
        self.assertFalse(third.more)
        self.assertEqual(third.cursor, ScreenMemoryCursor(frames=_TS + 3, app_usage=0))
        records.extend(self._records())
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
        batch = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            None,
            self.out,
            self.now,
            masker=MASKER,
        )
        assert batch is not None
        self.assertEqual(batch.counts["skipped"], 3)
        self.assertEqual(
            [row["image_path"] for row in self._records() if row["type"] == "frame"],
            ["ok.jpeg"],
        )
        self.assertIsNone(
            ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                batch.cursor,
                self.out,
                self.now,
                masker=MASKER,
            )
        )

    def test_window_intersects_settle_bound(self) -> None:
        self._source(
            [(_TS + i, f"{i}.jpg", None) for i in range(-1, 4)],
            [(_TS + i, "app.example") for i in range(-1, 4)],
        )
        batch = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            ScreenMemoryCursor(frames=_TS - 2, app_usage=_TS),
            self.out,
            _TS + 301,
            window=(_TS, _TS + 3),
            masker=MASKER,
        )
        assert batch is not None
        self.assertEqual(
            [(row["type"], row["ts"]) for row in self._records()],
            [("frame", _TS), ("frame", _TS + 1), ("app_usage", _TS + 1)],
        )

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
        batch = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            None,
            self.out,
            now,
            masker=MASKER,
        )
        assert batch is not None
        self.assertEqual(batch.cursor, ScreenMemoryCursor(frames=_TS, app_usage=_TS))
        records = list(self._records())
        self.assertEqual([row["ts"] for row in records], [_TS] * 3)
        self.assertEqual(
            [row["image_path"] for row in records[:2]], ["a.jpeg", "b.jpeg"]
        )
        self.assertEqual([row["withheld"] for row in records[:2]], ["excluded_app"] * 2)
        self.assertIsNone(
            ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                batch.cursor,
                self.out,
                now,
                masker=MASKER,
            )
        )

    def test_decodes_invalid_utf8_and_blob_text(self) -> None:
        self._source([(_TS, "blob.jpeg", b"blob \xff")], [])
        with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
            db.execute(
                "INSERT INTO ocrresults VALUES (?, ?, CAST(? AS TEXT))",
                (_TS + 1, "text.jpeg", b"text \xfe"),
            )
        self.assertIsNotNone(
            ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                None,
                self.out,
                self.now,
                masker=MASKER,
            )
        )
        self.assertEqual(
            [r["text"] for r in self._records()], ["blob \ufffd", "text \ufffd"]
        )

    def test_fetches_entire_second_when_page_is_full(self) -> None:
        self.options["max_rows"] = 2
        self._source(
            [(_TS + i / 10, f"{i}.jpeg", None) for i in range(3)],
            [(_TS + i / 10, f"app.example.{i}") for i in range(3)],
        )
        batch = ScreenMemory().collect(
            ScreenMemoryOptions.model_validate(self.options),
            None,
            self.out,
            self.now,
            masker=MASKER,
        )
        assert batch is not None
        self.assertTrue(batch.more)
        self.assertEqual(batch.cursor, ScreenMemoryCursor(frames=_TS, app_usage=_TS))
        records = list(self._records())
        self.assertEqual(
            [r["image_path"] for r in records[:3]], [f"{i}.jpeg" for i in range(3)]
        )
        self.assertEqual(
            [r["bundle_id"] for r in records[3:]],
            [f"app.example.{i}" for i in range(3)],
        )
        self.assertIsNone(
            ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                batch.cursor,
                self.out,
                self.now,
                masker=MASKER,
            )
        )

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
        self.assertIsNone(
            ScreenMemory().collect(
                ScreenMemoryOptions.model_validate(self.options),
                None,
                self.out,
                self.now,
                masker=MASKER,
            )
        )
        self.assertFalse(self.out.exists())

    def test_missing_file_table_and_column(self) -> None:
        with closing(sqlite3.connect(self.data / "usage.sqlite")) as db, db:
            db.execute("CREATE TABLE usage(timestamp, identifier)")
        for case in ("file", "table", "column"):
            with self.subTest(case=case), self.assertRaises(Failure) as raised:
                ScreenMemory().collect(
                    ScreenMemoryOptions.model_validate(self.options),
                    None,
                    self.out,
                    self.now,
                    masker=MASKER,
                )
            self.assertEqual(
                (raised.exception.step, raised.exception.reason), ("collect", "source")
            )
            self.assertNotIn(str(self.data), str(raised.exception))
            if case != "column":
                with closing(sqlite3.connect(self.data / "text.sqlite")) as db, db:
                    if case == "table":
                        db.execute("CREATE TABLE ocrresults(imagetimestamp, imagepath)")
        self.assertFalse(self.out.exists())
