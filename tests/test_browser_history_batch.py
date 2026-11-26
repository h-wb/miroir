"""Firefox/Zen batch validation and merge with a Places snapshot."""

import gzip
import json
import sqlite3
import unittest
from collections import Counter
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import dlt
import duckdb

from src.control.artifacts import submit_artifact
from src.control.config import parse_miroir
from src.control.operations import MetadataStore
from src.control.runner import import_artifact
from src.integrations.browser_history.formats import read_rows, validate_batch
from src.integrations.validation import InvalidArtifact

FORMAT = "firefox_places_batch"
HEADER = {
    "type": "header",
    "format": FORMAT,
    "version": 1,
    "collected_at": "2026-11-20T15:00:00Z",
}


def record(table, **row):
    return {"type": "row", "table": table, "row": row}


def batch(*records):
    return [
        HEADER,
        *records,
        {
            "type": "trailer",
            "rows": dict(Counter(r["table"] for r in records)),
            "masked": {},
        },
    ]


def compress(records):
    return gzip.compress("".join(json.dumps(row) + "\n" for row in records).encode())


def snapshot(path):
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
            CREATE TABLE moz_places(id INTEGER PRIMARY KEY, url TEXT, title TEXT);
            CREATE TABLE moz_historyvisits(id INTEGER PRIMARY KEY, place_id INTEGER, visit_date INTEGER);
            INSERT INTO moz_places VALUES (1, 'https://example.org/one', 'Original'),
                                          (2, 'https://example.org/two', 'Unchanged');
            INSERT INTO moz_historyvisits VALUES (1, 2, 100);
        """)
    return path


class BrowserHistoryBatchTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.path = self.root / "batch.jsonl.gz"

    def test_accepts_normal_and_empty_batches(self):
        row = record(
            "moz_places",
            id=0,
            url="https://example.org/",
            score=0.25,
            flag=True,
            title=None,
        )
        self.path.write_bytes(compress(batch(row)))
        validate_batch(self.path)
        self.assertEqual(list(read_rows(self.path)), [("moz_places", row["row"])])
        document = batch()
        document[-1]["rows"] = {"moz_places": 0}
        self.path.write_bytes(compress(document))
        validate_batch(self.path)
        self.assertEqual(list(read_rows(self.path)), [])

    def test_rejections_never_echo_contents(self):
        marker = "PRIVATE_FIXTURE_MARKER"
        row = record("moz_places", id=1, title=marker)
        header, _, trailer = batch(row)
        document = compress(batch(row))
        invalid = {
            "not gzip": marker.encode(),
            "truncated": document[:-4],
            "missing header": compress([row, trailer]),
            "missing trailer": compress([header, row]),
            "wrong count": compress(
                [header, row, trailer | {"rows": {"moz_places": 2}}]
            ),
            "unknown table": compress(batch(row | {"table": marker})),
            "nested value": compress(batch(record("moz_places", id=1, title=[marker]))),
            "missing id": compress(batch(record("moz_places", title=marker))),
            "bad key": compress(
                batch(record("moz_places", id=1, **{"bad-key": marker}))
            ),
            "long line": compress(
                batch(record("moz_places", id=1, title=marker + "x" * (2 * 1024**2)))
            ),
            "extra field": compress([header, row, trailer | {"extra": marker}]),
            "row after trailer": compress([header, row, trailer, row]),
            "boolean id": compress(batch(record("moz_places", id=True, title=marker))),
        }
        for case, content in invalid.items():
            with self.subTest(case=case), self.assertRaises(InvalidArtifact) as caught:
                self.path.write_bytes(content)
                validate_batch(self.path)
            self.assertNotIn(marker, str(caught.exception))

    def test_enforces_uncompressed_limit(self):
        data = compress(batch())
        self.path.write_bytes(data)
        size = len(gzip.decompress(data))
        with patch("src.integrations.browser_history.formats.MAX_BATCH_BYTES", size):
            validate_batch(self.path)
        with (
            patch("src.integrations.browser_history.formats.MAX_BATCH_BYTES", size - 1),
            self.assertRaises(InvalidArtifact),
        ):
            validate_batch(self.path)

    def test_snapshot_then_batch_merges_same_tables(self):
        miroir = parse_miroir(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "tests",
                    "connections": [
                        {
                            "id": "browser",
                            "integration": "browser_history",
                            "settings": {},
                        }
                    ],
                }
            )
        )
        connection = miroir.connections[0]
        dataset = connection.binding.dataset_name
        store = MetadataStore(f"sqlite:///{self.root / 'meta.sqlite'}")
        self.addCleanup(store.engine.dispose)
        store.migrate()
        database = self.root / "warehouse.duckdb"

        def load(path, input_format=None):
            artifact, created = submit_artifact(
                miroir,
                connection,
                path,
                store=store,
                root=self.root / "artifacts",
                input_format=input_format,
            )
            self.assertTrue(created)
            import_artifact(
                miroir,
                artifact["connection_id"],
                artifact["id"],
                store=store,
                destination=dlt.destinations.duckdb(credentials=str(database)),
                resolve_credentials=lambda connection: {},
                origin="local",
                pipelines_dir=str(self.root / "state"),
            )

        load(snapshot(self.root / "places.sqlite"))
        self.path.write_bytes(
            compress(
                batch(
                    record(
                        "moz_places",
                        id=1,
                        url="https://example.org/updated",
                        title="Updated",
                    ),
                    record(
                        "moz_places", id=3, url="https://example.org/three", title="New"
                    ),
                    record("moz_historyvisits", id=2, place_id=1, visit_date=200),
                    record("moz_historyvisits", id=3, place_id=3, visit_date=300),
                )
            )
        )
        load(self.path, FORMAT)
        with closing(duckdb.connect(str(database))) as db:
            self.assertEqual(
                db.execute(
                    f'SELECT id, url, title FROM "{dataset}".zen_places ORDER BY id'
                ).fetchall(),
                [
                    (1, "https://example.org/updated", "Updated"),
                    (2, "https://example.org/two", "Unchanged"),
                    (3, "https://example.org/three", "New"),
                ],
            )
            self.assertEqual(
                db.execute(
                    f'SELECT id, place_id, visit_date FROM "{dataset}".zen_historyvisits ORDER BY id'
                ).fetchall(),
                [
                    (1, 2, 100),
                    (2, 1, 200),
                    (3, 3, 300),
                ],
            )
