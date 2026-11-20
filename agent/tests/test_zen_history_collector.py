import gzip
import json
import os
import shutil
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from miroir_agent.collectors.zen_history import (
    CHANGING_SOURCE_ERROR,
    SOURCE_ERROR,
    collect,
)
from miroir_agent.config import load_config

_REQUIRED = """
CREATE TABLE moz_places(id INTEGER PRIMARY KEY, url TEXT, title TEXT, description TEXT, preview_image_url TEXT);
CREATE TABLE moz_historyvisits(id INTEGER PRIMARY KEY, place_id INTEGER, visit_date INTEGER);
"""
_OPTIONAL = """
CREATE TABLE moz_places_metadata(id INTEGER PRIMARY KEY, updated_at INTEGER);
CREATE TABLE moz_origins(id INTEGER PRIMARY KEY, host TEXT);
CREATE TABLE moz_bookmarks(id INTEGER PRIMARY KEY, title TEXT);
"""


class ZenHistoryTests(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.profile = self.root / "profile"
        self.profile.mkdir()
        self.source = self.profile / "places.sqlite"
        self.out = self.root / "batch.jsonl.gz"
        self.options = {"places_path": str(self.source)}

    def sql(self, script):
        with closing(sqlite3.connect(self.source)) as db, db:
            db.executescript(script)

    def run_collect(self, cursor=None, **options):
        self.out.unlink(missing_ok=True)
        return collect(self.options | options, cursor, self.out, 1795186800)

    def batch(self):
        with gzip.open(self.out, "rt", encoding="utf-8") as file:
            document = [json.loads(line) for line in file]
        rows = {}
        for record in document[1:-1]:
            rows.setdefault(record["table"], []).append(record["row"])
        return rows, document[-1]

    def test_first_batch_then_unchanged_and_new_visit(self):
        self.sql(
            _REQUIRED
            + _OPTIONAL
            + """
            INSERT INTO moz_places(id, url) VALUES (1, 'https://example.org/');
            INSERT INTO moz_historyvisits VALUES (5, 1, 20);
            INSERT INTO moz_places_metadata VALUES (2, 30);
            INSERT INTO moz_origins VALUES (3, 'example.org');
            INSERT INTO moz_bookmarks VALUES (4, 'Bookmark');
        """
        )
        batch = self.run_collect()
        rows, trailer = self.batch()
        self.assertEqual(
            {table: [r["id"] for r in items] for table, items in rows.items()},
            {
                "moz_places": [1],
                "moz_historyvisits": [5],
                "moz_places_metadata": [2],
                "moz_origins": [3],
                "moz_bookmarks": [4],
            },
        )
        self.assertEqual(
            trailer, {"type": "trailer", "rows": dict.fromkeys(rows, 1), "masked": {}}
        )
        self.assertEqual(batch.counts, {"rows": trailer["rows"], "masked": {}})
        self.assertEqual(batch.cursor["visit_id"], 5)
        self.assertEqual(batch.cursor["visit_date"], 20)
        self.assertEqual(batch.cursor["place_id"], 1)
        self.assertEqual(batch.cursor["metadata_updated_at"], 30)
        for name in ("origins_digest", "bookmarks_digest"):
            self.assertRegex(batch.cursor[name], r"^[0-9a-f]{64}$")
        self.assertFalse(batch.more)
        self.assertIsNone(self.run_collect(batch.cursor))
        self.assertFalse(self.out.exists())

        self.sql("INSERT INTO moz_historyvisits VALUES (6, 1, 40);")
        next_batch = self.run_collect(batch.cursor)
        rows, _ = self.batch()
        self.assertEqual(
            {table: [r["id"] for r in items] for table, items in rows.items()},
            {
                "moz_places": [1],
                "moz_historyvisits": [6],
            },
        )
        self.assertEqual(
            next_batch.cursor, batch.cursor | {"visit_id": 6, "visit_date": 40}
        )

        self.sql("UPDATE moz_origins SET host = 'example.net';")
        changed = self.run_collect(next_batch.cursor)
        self.assertEqual(
            self.batch()[0], {"moz_origins": [{"id": 3, "host": "example.net"}]}
        )
        self.assertNotEqual(
            changed.cursor["origins_digest"], batch.cursor["origins_digest"]
        )

    def test_collects_visits_whose_ids_were_reused(self):
        self.sql(
            _REQUIRED
            + """
            INSERT INTO moz_places(id, url) VALUES (1, 'a'), (2, 'b'), (3, 'c');
            INSERT INTO moz_historyvisits(place_id, visit_date) VALUES (1, 10), (2, 20), (3, 30);
        """
        )
        initial = self.run_collect()
        with closing(sqlite3.connect(self.source)) as db, db:
            removed = db.execute(
                "SELECT id FROM moz_historyvisits WHERE id > 1 ORDER BY id"
            ).fetchall()
            db.execute("DELETE FROM moz_historyvisits WHERE id > 1")
            reused = [
                db.execute(
                    "INSERT INTO moz_historyvisits(place_id, visit_date) VALUES (?, ?)",
                    row,
                ).lastrowid
                for row in ((3, 40), (2, 50))
            ]
        self.assertEqual(reused, [row[0] for row in removed])

        next_batch = self.run_collect(initial.cursor, max_rows=1)
        rows, _ = self.batch()
        self.assertEqual([r["id"] for r in rows["moz_historyvisits"]], reused)
        self.assertEqual([r["visit_date"] for r in rows["moz_historyvisits"]], [40, 50])
        self.assertCountEqual([r["id"] for r in rows["moz_places"]], [2, 3])
        self.assertEqual(next_batch.cursor, initial.cursor | {"visit_date": 50})
        self.assertFalse(next_batch.more)
        self.assertIsNone(self.run_collect(next_batch.cursor))

        legacy = {
            key: value for key, value in initial.cursor.items() if key != "visit_date"
        }
        self.assertEqual(self.run_collect(legacy).cursor, next_batch.cursor)
        self.assertEqual(
            [r["id"] for r in self.batch()[0]["moz_historyvisits"]], [1, *reused]
        )

    def test_visit_and_place_pages_with_interaction_ties(self):
        self.sql(
            _REQUIRED
            + _OPTIONAL
            + """
            INSERT INTO moz_places(id, url) VALUES (1, 'a'), (2, 'b'), (3, 'c');
            INSERT INTO moz_historyvisits VALUES (1, 3, 1), (2, 3, 2), (3, 2, 3), (4, 2, 4), (5, 1, 5);
            INSERT INTO moz_places_metadata VALUES (1, 10), (2, 20), (3, 20), (4, 20), (5, 30);
        """
        )
        cursor = None
        expected = [
            ([1, 2], [1, 2, 3], [1]),
            ([3, 4], [3, 2], [2, 3, 4]),
            ([5], [1], [5]),
        ]
        for i, (visits, places, metadata) in enumerate(expected):
            batch = self.run_collect(cursor, max_rows=2)
            rows, _ = self.batch()
            self.assertEqual([r["id"] for r in rows["moz_historyvisits"]], visits)
            self.assertCountEqual([r["id"] for r in rows["moz_places"]], places)
            self.assertEqual([r["id"] for r in rows["moz_places_metadata"]], metadata)
            self.assertEqual(batch.more, i < 2)
            self.assertEqual(batch.cursor["place_id"], 2 if i == 0 else 3)
            cursor = batch.cursor
        self.assertIsNone(self.run_collect(cursor, max_rows=2))

    def test_wal_is_applied_without_changing_source_files(self):
        self.sql(_REQUIRED)
        with closing(sqlite3.connect(self.source)) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode=WAL").fetchone(), ("wal",))
            db.execute("PRAGMA wal_autocheckpoint=0")
            with db:
                db.execute(
                    "INSERT INTO moz_places(id, url, description) VALUES (1, 'example', x'78FF')"
                )
                db.execute("INSERT INTO moz_historyvisits VALUES (1, 1, 1)")
            before = {path.name: path.read_bytes() for path in self.profile.iterdir()}
            self.assertIn("places.sqlite-wal", before)
            self.run_collect()
            rows, _ = self.batch()
            self.assertEqual(set(rows), {"moz_places", "moz_historyvisits"})
            self.assertEqual(rows["moz_places"][0]["description"], "x\ufffd")
            self.assertEqual(rows["moz_historyvisits"][0]["id"], 1)
            self.assertEqual(
                before,
                {path.name: path.read_bytes() for path in self.profile.iterdir()},
            )

    def test_missing_required_table_or_file_fails_with_fixed_error(self):
        for exists in (False, True):
            if exists:
                self.sql("CREATE TABLE moz_places(id, url);")
            with self.assertRaises(ValueError) as caught:
                self.run_collect()
            self.assertEqual(str(caught.exception), SOURCE_ERROR)
            self.assertNotIn(str(self.source), str(caught.exception))
            self.assertFalse(self.out.exists())

    def test_retries_changed_copy_and_cleans_up_each_attempt(self):
        self.sql(_REQUIRED + "INSERT INTO moz_places(id, url) VALUES (1, 'a');")
        temp = self.root / "temp"
        temp.mkdir()
        copyfile = shutil.copyfile
        for changes, expected_attempts in ((1, 2), (3, 3)):
            with self.subTest(changes=changes):
                attempts = []

                def copy_and_touch(source, target, attempts=attempts, changes=changes):
                    attempts.append(target.parent)
                    copyfile(source, target)
                    if len(attempts) <= changes:
                        stat = source.stat()
                        os.utime(
                            source,
                            ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000),
                        )

                with (
                    patch(
                        "miroir_agent.collectors.zen_history.gettempdir",
                        return_value=str(temp),
                    ),
                    patch(
                        "miroir_agent.collectors.zen_history.shutil.copyfile",
                        side_effect=copy_and_touch,
                    ),
                ):
                    if changes == 1:
                        self.assertIsNotNone(self.run_collect())
                        self.assertEqual(self.batch()[0]["moz_places"][0]["id"], 1)
                    else:
                        with self.assertRaises(ValueError) as caught:
                            self.run_collect()
                        self.assertEqual(str(caught.exception), CHANGING_SOURCE_ERROR)
                        self.assertFalse(self.out.exists())
                self.assertEqual(len(set(attempts)), expected_attempts)
                self.assertEqual(list(temp.iterdir()), [])

    def test_selects_newest_profile_and_refuses_output_inside_it(self):
        self.sql(_REQUIRED + "INSERT INTO moz_places(id, url) VALUES (1, 'older');")
        older = self.source
        new_profile = self.root / "newer"
        new_profile.mkdir()
        self.source = new_profile / "places.sqlite"
        self.sql(_REQUIRED + "INSERT INTO moz_places(id, url) VALUES (2, 'newer');")
        os.utime(older, (1, 1))
        os.utime(self.source, (2, 2))
        self.options = {"profiles_dir": str(self.root)}
        self.run_collect()
        self.assertEqual(self.batch()[0]["moz_places"][0]["id"], 2)
        with self.assertRaisesRegex(ValueError, "output cannot be inside"):
            collect(self.options, None, new_profile / "batch.gz", 1795186800)

    def test_masks_urls_and_titles_and_can_disable_masking(self):
        self.sql(_REQUIRED + _OPTIONAL)
        token = "gh" + "p_" + "a1B2" * 9
        unchanged = "https://example.org/" + "aA91bX87zD0" * 4 + "?q=ordinary"
        with closing(sqlite3.connect(self.source)) as db, db:
            db.executemany(
                "INSERT INTO moz_places VALUES (?, ?, ?, ?, ?)",
                [
                    (1, "https://example.org/?API_Key=private&q=ok", token, None, None),
                    (2, "https://example.org/#access-token=private", None, token, None),
                    (
                        3,
                        "https://reader:private@example.org/",
                        None,
                        None,
                        "https://example.org/?code=private",
                    ),
                    (4, unchanged, None, None, None),
                ],
            )
            db.execute("INSERT INTO moz_bookmarks VALUES (1, ?)", (token,))
        self.run_collect()
        rows, trailer = self.batch()
        self.assertEqual(
            [r["url"] for r in rows["moz_places"]],
            [
                "https://example.org/?API_Key=[masked:url_param]&q=ok",
                "https://example.org/#access-token=[masked:url_param]",
                "https://reader:[masked:url_userinfo]@example.org/",
                unchanged,
            ],
        )
        self.assertEqual(rows["moz_places"][0]["title"], "[masked:token]")
        self.assertEqual(rows["moz_places"][1]["description"], "[masked:token]")
        self.assertEqual(
            rows["moz_places"][2]["preview_image_url"],
            "https://example.org/?code=[masked:url_param]",
        )
        self.assertEqual(rows["moz_bookmarks"][0]["title"], "[masked:token]")
        self.assertEqual(
            trailer["masked"], {"url_param": 3, "url_userinfo": 1, "token": 3}
        )
        self.run_collect(mask_urls=False)
        rows, trailer = self.batch()
        self.assertEqual(
            rows["moz_places"][0]["url"], "https://example.org/?API_Key=private&q=ok"
        )
        self.assertEqual(rows["moz_bookmarks"][0]["title"], token)
        self.assertEqual(trailer["masked"], {})

    def test_config_validates_options(self):
        path = self.root / "agent.json"

        def config(options):
            path.write_text(
                json.dumps(
                    {
                        "state_dir": str(self.root),
                        "collectors": [
                            {
                                "id": "zen",
                                "collector": "zen_history",
                                "receive_url": "https://example.org/receive?format=firefox_places_batch",
                                "token_env": "ZEN_TOKEN",
                                "options": options,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            return load_config(path).collectors[0].options

        options = self.options | {
            "profiles_dir": str(self.root),
            "max_rows": 1,
            "mask_urls": False,
        }
        self.assertEqual(config(options), options)
        defaults = config({})
        self.assertNotIn("places_path", defaults)
        self.assertEqual(defaults["max_rows"], 20000)
        self.assertIs(defaults["mask_urls"], True)
        for bad in (
            {"unknown": 1},
            {"max_rows": 0},
            {"max_rows": 200001},
            {"max_rows": True},
            {"mask_urls": 1},
            {"places_path": ""},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                config(bad)
