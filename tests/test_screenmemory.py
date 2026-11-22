"""ScreenMemory batch validation and merge loads with invented data."""

import gzip
import json
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import dlt
import duckdb

from src.core.artifacts import submit_artifact
from src.core.config import parse_miroir
from src.core.operations import MetadataStore
from src.core.runner import import_artifact
from src.integrations.registry import INTEGRATIONS
from src.integrations.screenmemory.formats import validate_batch
from src.integrations.validation import InvalidArtifact

TS = 1795132101
HEADER = {
    "type": "header",
    "format": "screenmemory_batch",
    "version": 1,
    "collected_at": "2026-11-20T15:00:00Z",
}
FRAME = {
    "type": "frame",
    "ts": TS,
    "image_path": "2026/11/19/18/1795132101.jpeg",
    "text": "Invented screen text",
    "withheld": None,
    "masked": 0,
}
USAGE = {"type": "app_usage", "ts": TS, "bundle_id": "app.zen-browser.zen"}


def batch(*records):
    return [
        HEADER,
        *records,
        {
            "type": "trailer",
            "frames": sum(r["type"] == "frame" for r in records),
            "app_usage": sum(r["type"] == "app_usage" for r in records),
            "masked": {},
            "withheld": {},
        },
    ]


class BatchTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

    def write(self, records, name="batch.jsonl.gz"):
        path = self.root / name
        with gzip.open(path, "wt", encoding="utf-8") as writer:
            for record in records:
                writer.write(json.dumps(record) + "\n")
        return path

    def test_registry_definition(self):
        definition = INTEGRATIONS["screenmemory"]
        self.assertEqual(
            [r.name for r in definition.resources], ["frames", "app_usage"]
        )
        self.assertEqual([f.id for f in definition.formats], ["screenmemory_batch"])
        self.assertIsNone(definition.analysis)
        self.assertIsNone(definition.fetch)

    def test_accepts_normal_withheld_and_empty_batches(self):
        validate_batch(self.write(batch(FRAME, USAGE)))
        withheld = FRAME | {"text": None, "withheld": "password_manager"}
        document = batch(withheld)
        document[-1]["withheld"] = {"password_manager": 1}
        validate_batch(self.write(document))
        validate_batch(self.write(batch()))

    def test_rejects_invalid_gzip_and_utf8(self):
        path = self.write(batch())
        truncated = path.read_bytes()[:-4]
        for content in (b"not gzip", truncated, gzip.compress(b"\xff")):
            with self.subTest(content=content), self.assertRaises(InvalidArtifact):
                path.write_bytes(content)
                validate_batch(path)

    def test_rejects_invalid_documents(self):
        trailer = batch(FRAME)[-1]
        invalid = {
            "missing header": [FRAME, trailer],
            "missing trailer": [HEADER, FRAME],
            "mismatched count": [HEADER, FRAME, trailer | {"frames": 2}],
            "unknown type": [HEADER, FRAME | {"type": "unknown"}, trailer],
            "wrong version": [HEADER | {"version": 2}, FRAME, trailer],
            "extra field": batch(FRAME | {"extra": 0}),
            "absolute path": batch(FRAME | {"image_path": "/" + FRAME["image_path"]}),
            "parent segment": batch(FRAME | {"image_path": "2026/../image.jpeg"}),
            "null text": batch(FRAME | {"text": None}),
            "withheld with text": batch(FRAME | {"withheld": "excluded_app"}),
            "long line": batch(FRAME | {"text": "x" * (2 * 1024 * 1024)}),
            "repeated header": [HEADER, HEADER, trailer],
            "repeated trailer": [HEADER, FRAME, trailer, trailer],
            "wrong integer type": batch(FRAME | {"masked": True}),
            "non-object": [HEADER, [], trailer],
        }
        for reason, document in invalid.items():
            with self.subTest(reason=reason), self.assertRaises(InvalidArtifact):
                validate_batch(self.write(document))

    def test_rejects_uncompressed_size_over_limit(self):
        path = self.write(batch(FRAME))
        size = len(gzip.decompress(path.read_bytes()))
        with patch("src.integrations.screenmemory.formats.MAX_BATCH_BYTES", size):
            validate_batch(path)
        with (
            patch("src.integrations.screenmemory.formats.MAX_BATCH_BYTES", size - 1),
            self.assertRaises(InvalidArtifact),
        ):
            validate_batch(path)

    def test_error_does_not_echo_text(self):
        marker = "PRIVATE_SCREEN_TEXT_MARKER"
        path = self.write(batch(FRAME | {"text": marker, "masked": -1}))
        with self.assertRaises(InvalidArtifact) as caught:
            validate_batch(path)
        self.assertNotIn(marker, str(caught.exception))

    def test_submit_import_and_merge_overlapping_batches(self):
        miroir = parse_miroir(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "tests",
                    "connections": [
                        {"id": "screen", "integration": "screenmemory", "settings": {}}
                    ],
                }
            )
        )
        self.assertFalse(miroir.analyses)
        connection = miroir.connections[0]
        dataset = connection.binding.dataset_name
        store = MetadataStore(f"sqlite:///{self.root / 'meta.sqlite'}")
        self.addCleanup(store.engine.dispose)
        store.migrate()
        database = self.root / "warehouse.duckdb"

        def load(document, name):
            artifact, created = submit_artifact(
                miroir,
                connection,
                self.write(document, name),
                {},
                store=store,
                root=self.root / "artifacts",
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

        def rows():
            with closing(duckdb.connect(str(database))) as db:
                db.execute("SET TimeZone = 'UTC'")
                frames = db.execute(
                    f'SELECT image_path, ts, captured_at, text, withheld, masked FROM "{dataset}".frames ORDER BY image_path'
                ).fetchall()
                usage = db.execute(
                    f'SELECT ts, bundle_id, observed_at FROM "{dataset}".app_usage ORDER BY ts, bundle_id'
                ).fetchall()
            return frames, usage

        second_path = "2026/11/19/18/1795132102.jpeg"
        third_path = "2026/11/19/18/1795132103.jpeg"
        first = FRAME | {"text": None, "withheld": "password_manager"}
        second = first | {"image_path": second_path, "ts": TS + 1}
        first_frames = [
            (
                first["image_path"],
                TS,
                datetime.fromtimestamp(TS, timezone.utc),
                None,
                "password_manager",
                0,
            ),
            (
                second_path,
                TS + 1,
                datetime.fromtimestamp(TS + 1, timezone.utc),
                None,
                "password_manager",
                0,
            ),
        ]
        first_usage = [
            (TS, USAGE["bundle_id"], datetime.fromtimestamp(TS, timezone.utc))
        ]
        load(batch(first, USAGE, second), "first.jsonl.gz")
        self.assertEqual(rows(), (first_frames, first_usage))

        third = FRAME | {"image_path": third_path, "ts": TS + 2, "masked": 1}
        other_app = USAGE | {"bundle_id": "example.invented-app"}
        load(batch(first, second, third, USAGE, other_app), "second.jsonl.gz")
        self.assertEqual(
            rows(),
            (
                first_frames
                + [
                    (
                        third_path,
                        TS + 2,
                        datetime.fromtimestamp(TS + 2, timezone.utc),
                        FRAME["text"],
                        None,
                        1,
                    )
                ],
                first_usage
                + [
                    (
                        TS,
                        other_app["bundle_id"],
                        datetime.fromtimestamp(TS, timezone.utc),
                    )
                ],
            ),
        )
