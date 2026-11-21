import gzip
import json
import unittest
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import dlt
import duckdb

from src.integrations.apple_messages.formats import EXPECTED, FIELDS, validate_batch
from src.integrations.apple_messages.source import _timestamp
from src.integrations.registry import INTEGRATIONS
from src.integrations.validation import InvalidArtifact

HEADER = {
    "type": "header",
    "format": "apple_messages_batch",
    "version": 1,
    "collected_at": "2026-11-21T10:00:00Z",
}
STAMP = 800_000_000
ROWS = {
    "message": {
        "guid": "message-1",
        "text_source": "text",
        "text": "hello",
        "date": STAMP,
        "masked": 0,
        "is_from_me": 1,
    },
    "chat": {"guid": "chat-1"},
    "handle": {"id": "handle-1", "service": "iMessage"},
    "attachment": {
        "guid": "attachment-1",
        "created_date": 4321,
        "media_key": "attachment-1.heic",
    },
    "chat_message": {"chat_guid": "chat-1", "message_guid": "message-1"},
    "message_attachment": {
        "message_guid": "message-1",
        "attachment_guid": "attachment-1",
    },
    "chat_handle": {
        "chat_guid": "chat-1",
        "handle": "handle-1",
        "handle_service": "iMessage",
    },
    "contact": {"handle": "handle-1", "first_name": "Test", "organization": "Example"},
}
TABLE_KEYS = {
    "messages": ["guid"],
    "chats": ["guid"],
    "handles": ["id", "service"],
    "attachments": ["guid"],
    "chat_messages": ["chat_guid", "message_guid"],
    "message_attachments": ["message_guid", "attachment_guid"],
    "chat_handles": ["chat_guid", "handle", "handle_service"],
    "contacts": ["handle"],
}


def record(kind, **changes):
    return {"type": kind, **dict.fromkeys(FIELDS[kind]), **ROWS[kind], **changes}


def batch(*records):
    counts = Counter(row["type"] for row in records)
    return [
        HEADER,
        *records,
        {
            "type": "trailer",
            **dict.fromkeys(ROWS, 0),
            **counts,
            "masked": {},
            "unreadable": 0,
        },
    ]


class AppleMessagesBatchTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def write(self, document):
        path = self.root / "batch.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as gz:
            for row in document:
                gz.write(json.dumps(row) + "\n")
        return path

    def test_validate_accepts_good_batches(self):
        validate_batch(self.write(batch(*(record(kind) for kind in ROWS))))
        validate_batch(self.write(batch()))
        validate_batch(
            self.write(batch(record("contact"), record("contact", handle="handle-2")))
        )
        validate_batch(self.write(batch(record("attachment", media_key=None))))
        # A masked attachment name counts in the total, without a chat row.
        document = batch(record("message"), record("attachment"))
        document[-1]["masked"] = {"token": 1}
        validate_batch(self.write(document))

    def test_fixed_validation_errors(self):
        message = record("message")
        trailer = batch(message)[-1]
        cases = {
            "missing header": [message, trailer],
            "missing trailer": [HEADER, message],
            "wrong count": [HEADER, message, trailer | {"message": 2}],
            "wrong unreadable count": [HEADER, message, trailer | {"unreadable": 1}],
            "wrong masked count": [HEADER, message, trailer | {"masked": {"token": 1}}],
            "masked below the messages' sum": [
                *batch(message | {"masked": 2}, record("attachment"))[:-1],
                batch(message, record("attachment"))[-1] | {"masked": {"token": 1}},
            ],
            "unknown type": [HEADER, message | {"type": "unknown"}, trailer],
            "missing key": batch({k: v for k, v in message.items() if k != "subject"}),
            "extra key": batch(message | {"ROWID": 12345}),
            "wrong value type": batch(message | {"date": "private text"}),
            "wrong flag value": batch(message | {"is_from_me": 2}),
            "null key": batch(record("handle", service=None)),
            "attachment without media key": batch(
                {k: v for k, v in record("attachment").items() if k != "media_key"}
            ),
            "contact missing key": batch(
                {k: v for k, v in record("contact").items() if k != "nickname"}
            ),
            "contact extra key": batch(record("contact") | {"phone": "private"}),
            "contact empty handle": batch(record("contact", handle="")),
            "contact null handle": batch(record("contact", handle=None)),
            "contact wrong type": batch(record("contact", first_name=1)),
            "NUL in text": batch(message | {"text": "a\x00b"}),
            "NUL in key": batch(record("handle", id="a\x00b")),
            "lone surrogate": batch(message | {"subject": "a\ud800b"}),
            "missing contact count": [
                HEADER,
                message,
                {k: v for k, v in trailer.items() if k != "contact"},
            ],
            "long line": batch(message | {"text": "x" * (2 * 1024**2)}),
            "extra trailer": [*batch(message), trailer],
        }
        for reason, document in cases.items():
            with (
                self.subTest(reason=reason),
                self.assertRaises(InvalidArtifact) as caught,
            ):
                validate_batch(self.write(document))
            self.assertEqual(str(caught.exception), EXPECTED)

    def test_invalid_gzip_and_size_limit(self):
        path = self.write(batch())
        original = path.read_bytes()
        size = len(gzip.decompress(original))
        with patch("src.integrations.apple_messages.formats.MAX_BATCH_BYTES", size):
            validate_batch(path)
        with (
            patch("src.integrations.apple_messages.formats.MAX_BATCH_BYTES", size - 1),
            self.assertRaises(InvalidArtifact),
        ):
            validate_batch(path)
        for data in (b"invalid gzip", original[:-4], gzip.compress(b"\xff")):
            path.write_bytes(data)
            with self.assertRaises(InvalidArtifact) as caught:
                validate_batch(path)
            self.assertEqual(str(caught.exception), EXPECTED)

    def test_timestamp_units(self):
        epoch = datetime(2001, 1, 1, tzinfo=UTC)
        for value, expected in (
            (STAMP, epoch + timedelta(seconds=STAMP)),
            (STAMP * 10**9, epoch + timedelta(seconds=STAMP)),
            (-STAMP, epoch - timedelta(seconds=STAMP)),
            (-STAMP * 10**9, epoch - timedelta(seconds=STAMP)),
            (0, None),
            (None, None),
        ):
            with self.subTest(value=value):
                self.assertEqual(_timestamp(value, seconds=False), expected)
        self.assertEqual(
            _timestamp(STAMP * 10**9, seconds=True), None
        )  # attachment dates are seconds, and this one is out of range

    def test_merge_load(self):
        definition = INTEGRATIONS["apple_messages"]
        self.assertEqual([r.name for r in definition.resources], list(TABLE_KEYS))
        self.assertIsNone(definition.analysis)
        self.assertFalse(definition.media)
        (fmt,) = definition.formats
        database = self.root / "warehouse.duckdb"
        pipeline = dlt.pipeline(
            pipeline_name="apple_messages_test",
            pipelines_dir=str(self.root / "pipelines"),
            destination=dlt.destinations.duckdb(credentials=str(database)),
            dataset_name="apple_rows",
        )

        def load(path):
            fmt.validate(path)
            with fmt.open(definition.settings_model(), str(path), {}) as source:
                pipeline.run(source)

        def snapshot():
            with closing(duckdb.connect(str(database))) as db:
                return {
                    name: db.execute(
                        f"SELECT * EXCLUDE (_dlt_load_id, _dlt_id) FROM apple_rows.{name} ORDER BY ALL"
                    ).fetchall()
                    for name in TABLE_KEYS
                }

        path = self.write(
            batch(
                *(record(kind) for kind in ROWS),
                record(
                    "message",
                    guid="message-2",
                    date=(STAMP + 1) * 10**9,
                    date_read=STAMP + 2,
                    date_edited=(STAMP + 3) * 10**9 + 123000,
                    date_delivered=0,
                ),
            )
        )
        load(path)
        original = snapshot()
        self.assertEqual(
            [len(rows) for rows in original.values()], [2, 1, 1, 1, 1, 1, 1, 1]
        )
        for name, keys in TABLE_KEYS.items():
            columns = pipeline.default_schema.tables[name]["columns"]
            self.assertCountEqual(
                [key for key, info in columns.items() if info.get("primary_key")], keys
            )
        tables = pipeline.default_schema.tables
        for table, column in [
            ("attachments", "media_key"),
            *(("contacts", name) for name in FIELDS["contact"] if name != "handle"),
        ]:
            info = tables[table]["columns"][column]
            self.assertEqual((info["data_type"], info["nullable"]), ("text", True))
        load(path)
        self.assertEqual(snapshot(), original)
        load(self.write(batch(record("message", text="edited"))))
        self.assertEqual(snapshot()["contacts"], original["contacts"])
        load(self.write(batch(record("contact", first_name=None, nickname="Tess"))))
        self.assertEqual(
            snapshot()["contacts"], [("handle-1", None, None, "Tess", "Example")]
        )

        epoch = datetime(2001, 1, 1, tzinfo=UTC)
        with closing(duckdb.connect(str(database))) as db:
            db.execute("SET TimeZone = 'UTC'")
            self.assertEqual(
                db.execute(
                    "SELECT guid, text, date, sent_at, read_at, edited_at, delivered_at, retracted_at, is_from_me "
                    "FROM apple_rows.messages ORDER BY guid"
                ).fetchall(),
                [
                    (
                        "message-1",
                        "edited",
                        STAMP,
                        epoch + timedelta(seconds=STAMP),
                        None,
                        None,
                        None,
                        None,
                        True,
                    ),
                    (
                        "message-2",
                        "hello",
                        (STAMP + 1) * 10**9,
                        epoch + timedelta(seconds=STAMP + 1),
                        epoch + timedelta(seconds=STAMP + 2),
                        epoch + timedelta(seconds=STAMP + 3, microseconds=123),
                        None,
                        None,
                        True,
                    ),
                ],
            )
            self.assertEqual(
                db.execute(
                    "SELECT created_date, created_at, media_key FROM apple_rows.attachments"
                ).fetchall(),
                [(4321, epoch + timedelta(seconds=4321), "attachment-1.heic")],
            )
