import gzip
import hashlib
import json
import os
import sqlite3
import unittest
from contextlib import closing, redirect_stdout
from http.server import HTTPServer
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch

from miroir_collector.cli import main
from miroir_collector.errors import Failure
from miroir_collector.sources.apple_messages import (
    AppleMessages,
    AppleMessagesCursor,
    AppleMessagesOptions,
    attributed_text,
    media_key,
)
from miroir_collector.state import State

from .masking_fixtures import MASKER, github_token
from .test_collector import _Handler

EPOCH = 978307200
DAY = 86400
STAMP = 800_000_000
NOW = EPOCH + STAMP + 2000
ROW = 71021
SCHEMA = """
CREATE TABLE message (
    guid TEXT, text TEXT, attributedBody BLOB, message_summary_info BLOB,
    payload_data BLOB, handle_id INTEGER, date INTEGER, subject TEXT,
    is_from_me INTEGER, thread_originator_guid TEXT, date_edited INTEGER,
    date_retracted INTEGER, reply_to_guid TEXT, part_count INTEGER
);
CREATE TABLE chat (guid TEXT, display_name TEXT, chat_identifier TEXT);
CREATE TABLE handle (id TEXT, service TEXT);
CREATE TABLE attachment (
    guid TEXT, filename TEXT, transfer_name TEXT, transfer_state INTEGER,
    created_date INTEGER
);
CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
"""


def archive(text):
    value = text.encode()
    size = len(value)
    length = (
        bytes([size])
        if size <= 127
        else b"\x81" + size.to_bytes(2, "little")
        if size <= 65535
        else b"\x82" + size.to_bytes(4, "little")
    )
    # A typedstream names the class and then its superclass, NSString.
    return (
        b"archive-private-data NSMutableString\x00\x84\x84\x08NSString\x01\x94\x84\x01+"
        + length
        + value
    )


class AppleMessagesTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.db = self.root / "Messages" / "chat.db"
        self.db.parent.mkdir()
        self.out = self.root / "batch.gz"
        self.executescript(
            SCHEMA
            + f"""
            INSERT INTO chat (ROWID, guid, chat_identifier) VALUES ({ROW + 1}, 'chat-example', 'test-group');
            INSERT INTO handle (ROWID, id, service) VALUES ({ROW + 2}, 'test@example.org', 'iMessage');
            INSERT INTO attachment (guid, filename, transfer_state, created_date)
                VALUES ('attachment-example', '~/private/Image.JPG', 1, 4321);
            INSERT INTO chat_handle_join VALUES ({ROW + 1}, {ROW + 2});
        """
        )

    def executescript(self, sql):
        with closing(sqlite3.connect(self.db)) as db:
            db.executescript(sql)

    def message(
        self, rowid=ROW, *, guid="message-example", date=STAMP, text="Hello", body=None
    ):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute(
                "INSERT INTO message (ROWID, guid, text, date, handle_id, attributedBody) VALUES (?, ?, ?, ?, ?, ?)",
                (rowid, guid, text, date, ROW + 2, body),
            )

    def collect(self, cursor=None, *, now=NOW, window=None, **options):
        self.out.unlink(missing_ok=True)
        result = AppleMessages().collect(
            AppleMessagesOptions.model_validate({"database": str(self.db), **options}),
            cursor,
            self.out,
            now,
            masker=MASKER,
            window=window,
        )
        if result is None:
            self.assertFalse(self.out.exists())
            return None, [], {}
        with gzip.open(self.out, "rt", encoding="utf-8") as file:
            lines = [json.loads(line) for line in file]
        return result, lines[1:-1], lines[-1]

    def test_first_pass_and_increment(self):
        self.message()
        self.executescript(f"""
            UPDATE attachment SET ROWID = {ROW + 3};
            INSERT INTO chat_message_join VALUES ({ROW + 1}, {ROW});
            INSERT INTO message_attachment_join VALUES ({ROW}, {ROW + 3});
        """)
        first, records, trailer = self.collect()
        rows = {row["type"]: row for row in records}
        self.assertEqual(len(rows), 7)
        self.assertEqual(rows["message"]["handle"], "test@example.org")
        self.assertEqual(rows["message"]["handle_service"], "iMessage")
        self.assertEqual(
            rows["chat_message"],
            {
                "type": "chat_message",
                "chat_guid": "chat-example",
                "message_guid": "message-example",
            },
        )
        self.assertEqual(
            rows["message_attachment"],
            {
                "type": "message_attachment",
                "message_guid": "message-example",
                "attachment_guid": "attachment-example",
            },
        )
        self.assertEqual(
            rows["chat_handle"],
            {
                "type": "chat_handle",
                "chat_guid": "chat-example",
                "handle": "test@example.org",
                "handle_service": "iMessage",
            },
        )
        self.assertEqual(rows["attachment"]["created_date"], 4321)
        self.assertEqual(rows["attachment"]["media_key"], "attachment-example.jpg")
        self.assertEqual(trailer["contact"], 0)
        self.assertEqual(
            (first.counts["resent"], first.counts["layout_changed"]), (0, 0)
        )
        self.assertNotIn("private", json.dumps(records))
        for record in records:
            self.assertEqual(trailer[record["type"]], 1)
            self.assertNotIn("rowid", {key.lower() for key in record})
            self.assertTrue(set(range(ROW, ROW + 4)).isdisjoint(record.values()))
        self.assertNotIn("filename", rows["attachment"])
        self.assertNotIn("transfer_state", rows["attachment"])
        self.assertIsNone(self.collect(first.cursor)[0])
        self.message(ROW + 4, guid="new-message")
        second, records, _ = self.collect(first.cursor)
        self.assertEqual(
            [(r["type"], r["guid"]) for r in records], [("message", "new-message")]
        )
        self.executescript(f"DELETE FROM chat_handle_join WHERE handle_id = {ROW + 2};")
        third, records, _ = self.collect(second.cursor)
        self.assertEqual([r["type"] for r in records], ["chat"])
        self.assertIsNone(self.collect(third.cursor)[0])

    def test_text_and_masking(self):
        token = github_token()
        values = ["short", "a " * 70, "a " * 33000, "caf\u00e9 \U0001f600"]
        self.message(text="", body=archive("ignore this"))
        for i, value in enumerate(values, 1):
            self.message(ROW + i, text=None, body=archive(value))
        self.message(ROW + 5, text=None)
        self.message(ROW + 6, text=None, body=archive("truncated")[:-1])
        self.message(ROW + 7, text=None, body=archive(f"{token} PASSWORD=Secret42"))
        self.executescript(f"""
            UPDATE message SET message_summary_info = 'private-summary', payload_data = 'private-payload';
            UPDATE message SET subject = 'PASSWORD=Subject42' WHERE ROWID = {ROW + 7};
            UPDATE chat SET display_name = 'PASSWORD=Display42';
        """)
        _, records, trailer = self.collect()
        messages = [r for r in records if r["type"] == "message"]
        self.assertEqual(
            [r["text"] for r in messages],
            ["", *values, None, None, "[masked:token] PASSWORD=[masked:assignment]"],
        )
        self.assertEqual(
            [r["text_source"] for r in messages],
            ["text", *["attributed_body"] * 4, "none", "unreadable", "attributed_body"],
        )
        self.assertEqual(messages[-1]["masked"], 3)
        self.assertEqual(trailer["unreadable"], 1)
        self.assertEqual(trailer["masked"], {"token": 1, "assignment": 3})
        raw = gzip.decompress(self.out.read_bytes()).decode()
        for value in (
            token,
            "Secret42",
            "Subject42",
            "Display42",
            "ignore this",
            "archive-private-data",
            "private-summary",
            "private-payload",
            "attributedBody",
            "message_summary_info",
            "payload_data",
        ):
            self.assertNotIn(value, raw)

    def test_deleted_last_anchor_and_rebuilt_database(self):
        for i in range(12):
            self.message(ROW + i, guid=f"test-{i}")
        first, _, _ = self.collect()
        self.assertEqual(
            [rowid for rowid, _ in first.cursor.anchors],
            [ROW + i for i in (0, 1, 3, 4, 6, 7, 9, 11)],
        )
        # Deleting the newest anchor messages, one or two, is not a rebuild.
        self.executescript(f"DELETE FROM message WHERE ROWID = {ROW + 11}")
        self.assertIsNone(self.collect(first.cursor)[0])
        self.executescript(f"DELETE FROM message WHERE ROWID = {ROW + 9}")
        self.assertIsNone(self.collect(first.cursor)[0])
        # A page of one keeps the anchors that still match beside its own.
        self.message(ROW + 20, guid="test-new")
        second, _, _ = self.collect(first.cursor)
        self.assertEqual(
            [rowid for rowid, _ in second.cursor.anchors],
            [ROW + i for i in (0, 1, 3, 4, 6, 7, 20)],
        )
        self.executescript(f"DELETE FROM message WHERE ROWID = {ROW + 20}")
        self.assertIsNone(self.collect(second.cursor)[0])
        new = self.db.with_name("rebuilt.db")
        with (
            closing(sqlite3.connect(self.db)) as original,
            closing(sqlite3.connect(new)) as rebuilt,
        ):
            original.backup(rebuilt)
        self.db = new
        self.executescript("UPDATE message SET ROWID = ROWID - 20000")
        _, records, _ = self.collect(second.cursor)
        self.assertEqual(
            [r["guid"] for r in records if r["type"] == "message"],
            [f"test-{i}" for i in (*range(9), 10)],
        )

    def test_replaced_latest_page_is_a_rebuild(self):
        for i in range(12):
            self.message(ROW + i, guid=f"test-{i}")
        first, _, _ = self.collect()
        # The oldest anchors still match; the three newest do not.
        self.executescript(
            f"UPDATE message SET guid = 'other-' || (ROWID - {ROW}) "
            f"WHERE ROWID >= {ROW + 6}"
        )
        _, records, _ = self.collect(first.cursor)
        self.assertEqual(
            [r["guid"] for r in records if r["type"] == "message"],
            [*(f"test-{i}" for i in range(6)), *(f"other-{i}" for i in range(6, 12))],
        )

    def test_settle_blocks_later_rows_in_both_units(self):
        for scale in (1, 10**9):
            with self.subTest(scale=scale):
                self.executescript("DELETE FROM message")
                self.message(date=STAMP * scale)
                self.message(ROW + 1, guid="unsettled", date=(STAMP + 801) * scale)
                self.message(ROW + 2, guid="settled", date=(STAMP + 800) * scale)
                first, records, _ = self.collect(settle_seconds=1200)
                self.assertEqual(records[0]["date"], STAMP * scale)
                self.assertEqual(first.cursor.message, ROW)
                self.assertTrue(first.more)
                self.assertIsNone(self.collect(first.cursor, settle_seconds=1200)[0])
                later, records, _ = self.collect(
                    first.cursor, now=NOW + 1, settle_seconds=1200
                )
                self.assertEqual([r["guid"] for r in records], ["unsettled", "settled"])
                self.assertFalse(later.more)

    def test_future_dated_message_does_not_hold_back_later_rows(self):
        for scale in (1, 10**9):
            for settle in (0, 1200):
                with self.subTest(scale=scale, settle=settle):
                    self.executescript("DELETE FROM message")
                    self.message(date=STAMP * scale)
                    self.message(
                        ROW + 1, guid="scheduled", date=(STAMP + 30 * DAY) * scale
                    )
                    self.message(ROW + 2, guid="after", date=(STAMP + 1) * scale)
                    result, records, _ = self.collect(settle_seconds=settle)
                    self.assertEqual(
                        [r["guid"] for r in records if r["type"] == "message"][1:],
                        ["scheduled", "after"],
                    )
                    self.assertEqual(result.cursor.message, ROW + 2)
                    self.assertFalse(result.more)

    def test_since_and_resend_page(self):
        for i, date in enumerate((STAMP - DAY, STAMP, STAMP + 1, STAMP + 2)):
            self.message(ROW + i, date=date, guid=f"test-{i}")
        since = "2026-05-09T06:13:20Z"  # Apple epoch plus STAMP
        _, records, _ = self.collect(since=since)
        self.assertEqual(
            [r["guid"] for r in records if r["type"] == "message"],
            ["test-1", "test-2", "test-3"],
        )
        window = (EPOCH + STAMP - DAY, EPOCH + STAMP + 2)
        cursor = None
        for i in range(3):
            result, records, _ = self.collect(
                cursor, since=since, window=window, max_rows=1
            )
            self.assertEqual(
                [(r["type"], r["guid"]) for r in records], [("message", f"test-{i}")]
            )
            self.assertTrue(result.more)
            cursor = result.cursor
        self.assertIsNone(self.collect(cursor, window=window)[0])

    def test_missing_columns_and_tables(self):
        self.message()
        optional = (
            "thread_originator_guid",
            "date_edited",
            "date_retracted",
            "reply_to_guid",
            "part_count",
        )
        self.executescript(
            ";".join(f"ALTER TABLE message DROP COLUMN {field}" for field in optional)
        )
        first, records, _ = self.collect()
        self.assertEqual(
            [records[0][field] for field in optional], [None] * len(optional)
        )
        self.message(ROW + 1, guid="later")
        second, records, _ = self.collect(first.cursor)
        self.assertEqual([r["guid"] for r in records], ["later"])
        self.assertEqual(second.counts["resent"], 0)
        self.assertIsNone(self.collect(second.cursor)[0])
        for sql in (
            "ALTER TABLE message RENAME COLUMN date TO old_date",
            "ALTER TABLE message RENAME COLUMN guid TO old_guid",
            "DROP TABLE chat",
        ):
            with self.subTest(sql=sql):
                original = self.db.read_bytes()
                self.executescript(sql)
                with self.assertRaises(Failure) as caught:
                    self.collect()
                self.assertEqual(
                    (caught.exception.step, caught.exception.reason),
                    ("collect", "source"),
                )
                self.db.write_bytes(original)

    def test_skipped_rows(self):
        self.message()
        self.message(ROW + 1, guid=None)
        self.executescript(
            f"INSERT INTO message_attachment_join VALUES ({ROW}, 999999)"
        )
        batch, _, trailer = self.collect()
        self.assertEqual(batch.counts["skipped"], 2)
        self.assertEqual(trailer["message"], 1)
        self.assertEqual(trailer["message_attachment"], 0)
        self.assertNotIn("skipped", trailer)
        self.assertIsNone(self.collect(batch.cursor)[0])

    def test_values_of_the_wrong_kind_are_sent_as_null_and_counted(self):
        self.message()
        self.executescript(f"""
            UPDATE message SET is_from_me = 2, date_edited = 'soon', part_count = 1.5,
                subject = X'00ff' WHERE ROWID = {ROW};
            UPDATE chat SET display_name = X'01';
            UPDATE attachment SET ROWID = {ROW + 3}, created_date = 'later';
            INSERT INTO chat_message_join VALUES ({ROW + 1}, {ROW});
            INSERT INTO message_attachment_join VALUES ({ROW}, {ROW + 3});
        """)
        batch, records, trailer = self.collect()
        rows = {row["type"]: row for row in records}
        self.assertEqual(
            [
                rows[kind][column]
                for kind, column in (
                    ("message", "is_from_me"),
                    ("message", "date_edited"),
                    ("message", "part_count"),
                    ("message", "subject"),
                    ("chat", "display_name"),
                    ("attachment", "created_date"),
                )
            ],
            [None] * 6,
        )
        self.assertEqual(
            batch.counts["blanked"],
            {
                "message_is_from_me": 1,
                "message_date_edited": 1,
                "message_part_count": 1,
                "message_subject": 1,
                "chat_display_name": 1,
                "attachment_created_date": 1,
            },
        )
        # A null is not blanked, and the batch's trailer does not change.
        self.assertNotIn("message_date_retracted", batch.counts["blanked"])
        self.assertNotIn("blanked", trailer)
        self.executescript(f"""
            UPDATE message SET is_from_me = 1, date_edited = NULL, part_count = 1,
                subject = NULL WHERE ROWID = {ROW};
            UPDATE chat SET display_name = 'Group';
            UPDATE attachment SET created_date = 4321;
        """)
        self.out.unlink()
        self.assertEqual(self.collect()[0].counts["blanked"], {})

    def test_media_key(self):
        cases = [
            (("guid-1", "IMG_1.HEIC", None), "guid-1.heic"),
            (("at_0:G.1", "IMG_1", "~/Library/Messages/x/IMG_1.Mov"), "at_0:G.1.mov"),
            (("guid-1", None, "~/Library/Messages/x/IMG_1.png"), "guid-1.png"),
            (("guid-1", "a.tar.GZ", None), "guid-1.gz"),
            (("guid-1", "IMG_1", "~/Library/Messages/x/IMG_1"), None),
            (("guid-1", None, None), None),
            (("guid-1", "a.abcdefghijk", None), None),
            (("guid-1", "a.he-ic", "/x/b.caf\u00e9"), None),
            (("guid-1", "a.abcdefghijk", "/x/b.JPEG"), "guid-1.jpeg"),
            (("guid/1", "IMG_1.HEIC", None), None),
            (("", "IMG_1.HEIC", None), None),
            (("g" * 201, "IMG_1.HEIC", None), None),
            (("guid 1", "IMG_1.HEIC", None), None),
            ((None, "IMG_1.HEIC", None), None),
        ]
        for args, expected in cases:
            with self.subTest(args=args):
                self.assertEqual(media_key(*args), expected)

    def edit(self, rowid, text, **dates):
        values = ", ".join(f"{column} = {value}" for column, value in dates.items())
        self.executescript(
            f"UPDATE message SET text = '{text}', {values} WHERE ROWID = {rowid}"
        )

    def resend(self, cursor, expected, **options):
        result, records, trailer = self.collect(cursor, **options)
        if expected is None:
            self.assertIsNone(result)
            return cursor
        messages = [r for r in records if r["type"] == "message"]
        self.assertEqual([(r["guid"], r["text"]) for r in messages], expected)
        self.assertEqual(result.counts["resent"], trailer["message"])
        self.assertFalse(result.more)
        return result.cursor

    def test_sent_at_once_and_sent_again_when_edited(self):
        self.message(date=NOW - EPOCH)
        for i in range(1, 4):
            self.message(ROW + i, guid=f"test-{i}")
        self.executescript(f"INSERT INTO chat_message_join VALUES ({ROW + 1}, {ROW})")
        first, records, _ = self.collect()
        self.assertEqual(
            [r["guid"] for r in records if r["type"] == "message"],
            ["message-example", "test-1", "test-2", "test-3"],
        )
        self.assertEqual(first.cursor.edits, "")
        cursor = self.resend(first.cursor, None)
        late = (STAMP + 100) * 10**9
        self.edit(ROW, "one", date_edited=late)
        result, records, _ = self.collect(cursor)
        self.assertIn(
            {
                "type": "chat_message",
                "chat_guid": "chat-example",
                "message_guid": "message-example",
            },
            records,
        )
        cursor = self.resend(cursor, [("message-example", "one")])
        # Synced late from another device, with an earlier time than the last.
        self.edit(ROW + 1, "two", date_edited=late - 50 * 10**9)
        cursor = self.resend(cursor, [("message-example", "one"), ("test-1", "two")])
        # The same second as an edit already sent again.
        self.edit(ROW + 2, "three", date_edited=late - 50 * 10**9)
        cursor = self.resend(
            cursor,
            [("message-example", "one"), ("test-1", "two"), ("test-2", "three")],
        )
        self.edit(ROW + 3, "", date_retracted=late)
        unsent = [("message-example", "one"), ("test-1", "two")]
        unsent += [("test-2", "three"), ("test-3", "")]
        cursor = self.resend(cursor, unsent)
        self.edit(ROW, "again", date_edited=late + 10**9)
        cursor = self.resend(cursor, [("message-example", "again"), *unsent[1:]])
        # A second edit in the same second: only the text's length changed.
        self.edit(ROW, "again, later", date_edited=late + 10**9)
        cursor = self.resend(cursor, [("message-example", "again, later"), *unsent[1:]])
        cursor = self.resend(cursor, None)
        # Already edited when first sent: sent once, as a new message.
        self.message(ROW + 9, guid="new", text="new")
        self.edit(ROW + 9, "new", date_edited=late)
        result, records, _ = self.collect(cursor)
        self.assertEqual(
            [r["guid"] for r in records if r["type"] == "message"], ["new"]
        )
        self.assertEqual(result.counts["resent"], 0)
        self.resend(result.cursor, None)

    def test_since_bounds_what_is_sent_again(self):
        self.message(ROW, guid="old", date=STAMP - DAY)
        self.message(ROW + 1, guid="kept", date=STAMP)
        since = "2026-05-09T06:13:20Z"  # Apple epoch plus STAMP
        result, records, _ = self.collect(since=since)
        self.assertEqual(
            [r["guid"] for r in records if r["type"] == "message"], ["kept"]
        )
        self.edit(ROW, "late edit", date_edited=STAMP * 10**9)
        cursor = self.resend(result.cursor, None, since=since)
        self.edit(ROW + 1, "edited", date_edited=STAMP * 10**9)
        cursor = self.resend(cursor, [("kept", "edited")], since=since)
        self.resend(cursor, None, since=since)

    def test_masks_group_title_and_transfer_name(self):
        token = github_token()
        self.message()
        self.executescript(f"""
            ALTER TABLE message ADD COLUMN group_title TEXT;
            UPDATE message SET group_title = 'PASSWORD=GroupTitle42';
            UPDATE attachment SET ROWID = {ROW + 3}, transfer_name = '{token}.HEIC';
            INSERT INTO message_attachment_join VALUES ({ROW}, {ROW + 3});
        """)
        _, records, trailer = self.collect()
        rows = {row["type"]: row for row in records}
        self.assertEqual(rows["message"]["group_title"], "PASSWORD=[masked:assignment]")
        self.assertEqual(rows["message"]["masked"], 1)
        self.assertIn("[masked:token]", rows["attachment"]["transfer_name"])
        self.assertEqual(rows["attachment"]["media_key"], "attachment-example.heic")
        self.assertEqual(trailer["masked"], {"assignment": 1, "token": 1})
        raw = gzip.decompress(self.out.read_bytes()).decode()
        self.assertNotIn(token, raw)
        self.assertNotIn("GroupTitle42", raw)

    def test_resend_window_ignores_settle(self):
        for i in range(3):
            self.message(ROW + i, guid=f"test-{i}", date=STAMP + 1990 + i)
        window = (EPOCH + STAMP, EPOCH + STAMP + 3000)
        result, records, _ = self.collect(window=window, settle_seconds=1200)
        self.assertEqual([r["guid"] for r in records], ["test-0", "test-1", "test-2"])
        self.assertFalse(result.more)
        _, records, _ = self.collect(settle_seconds=1200)
        self.assertNotIn("message", [r["type"] for r in records])

    def test_mixed_date_units(self):
        self.message(ROW, guid="seconds", date=STAMP)
        self.message(ROW + 1, guid="nanoseconds", date=STAMP * 10**9)
        self.message(ROW + 2, guid="earlier", date=STAMP * 10**9 - 1)
        since = "2026-05-09T06:13:20Z"  # Apple epoch plus STAMP
        for options in (
            {"since": since},
            {"window": (EPOCH + STAMP, EPOCH + STAMP + 1)},
        ):
            with self.subTest(options=options):
                _, records, _ = self.collect(**options)
                self.assertEqual(
                    [r["guid"] for r in records if r["type"] == "message"],
                    ["seconds", "nanoseconds"],
                )
                self.assertEqual(
                    [r["date"] for r in records if r["type"] == "message"],
                    [STAMP, STAMP * 10**9],
                )

    def test_invalid_utf8_and_nul(self):
        self.message()
        self.message(ROW + 1, guid=None)
        self.executescript(f"""
            UPDATE message SET guid = 'm' || char(0) WHERE ROWID = {ROW + 1};
            INSERT INTO message (ROWID, guid, date) VALUES ({ROW + 2}, CAST(X'6dff' AS TEXT), {STAMP});
            UPDATE message SET text = CAST(X'48ff69' AS TEXT), subject = 'x' || char(0),
                handle_id = {ROW + 4} WHERE ROWID = {ROW};
            INSERT INTO handle (ROWID, id, service) VALUES ({ROW + 4}, CAST(X'74ff' AS TEXT), 'SMS');
            INSERT INTO handle (ROWID, id, service) VALUES ({ROW + 5}, 'a' || char(0) || 'b', 'SMS');
            UPDATE chat SET chat_identifier = 'c' || char(0) || 'd';
            INSERT INTO chat_handle_join VALUES ({ROW + 1}, {ROW + 5});
        """)
        batch, records, _ = self.collect()
        # Keys are never altered: a key with NUL or undecodable bytes is skipped.
        messages = [r for r in records if r["type"] == "message"]
        self.assertEqual([r["guid"] for r in messages], ["message-example"])
        self.assertEqual(
            [r["id"] for r in records if r["type"] == "handle"], ["test@example.org"]
        )
        self.assertEqual(
            [r["handle"] for r in records if r["type"] == "chat_handle"],
            ["test@example.org"],
        )
        self.assertEqual(batch.counts["skipped"], 5)
        # Other strings are sent, with U+FFFD in place; a masked field holding
        # a NUL cannot be scanned, so it is masked whole.
        self.assertEqual(
            (messages[0]["text"], messages[0]["subject"], messages[0]["handle"]),
            ("H\ufffdi", "[masked:unscanned]", "t\ufffd"),
        )
        chat = next(r for r in records if r["type"] == "chat")
        self.assertEqual(chat["chat_identifier"], "c\ufffdd")
        self.assertNotIn("\\u0000", gzip.decompress(self.out.read_bytes()).decode())

    def test_length_field_cut_short_is_unreadable(self):
        head = archive("x")[:-2]
        self.assertEqual(attributed_text(archive("x" * 300)), "x" * 300)
        for i, tail in enumerate((b"\x81", b"\x81\x05", b"\x82", b"\x82\x05\x00\x00")):
            self.message(ROW + i, guid=f"test-{i}", text=None, body=head + tail)
        _, records, trailer = self.collect()
        self.assertEqual(
            [r["text_source"] for r in records if r["type"] == "message"],
            ["unreadable"] * 4,
        )
        self.assertEqual(trailer["unreadable"], 4)

    def test_layout_fingerprint(self):
        self.message()
        result, _, _ = self.collect()
        seen = [result.counts["layout_changed"]]
        steps = [
            "",
            "ALTER TABLE message ADD COLUMN new_field TEXT",
            "",
            "ALTER TABLE handle ADD COLUMN other_field TEXT",
        ]
        for i, sql in enumerate(steps, 1):
            self.executescript(sql)
            if i == 4:
                # A layout change alone sends nothing; the next batch reports it.
                self.assertIsNone(self.collect(result.cursor)[0])
            self.message(ROW + i, guid=f"test-{i}")
            result, _, _ = self.collect(result.cursor)
            seen.append(result.counts["layout_changed"])
        self.assertEqual(seen, [0, 0, 1, 0, 1])

    def test_earlier_cursor_shape_loads(self):
        self.message()
        digest = hashlib.sha256(b"message-example").hexdigest()
        cursor = AppleMessagesCursor.parse(
            {
                "message": ROW,
                "anchors": [[ROW, digest]],
                "chats_digest": "",
                "handles_digest": "",
            }
        )
        self.assertEqual((cursor.edits, cursor.layout), ("", ""))
        result, records, _ = self.collect(cursor)
        self.assertNotIn("message", [r["type"] for r in records])
        self.assertEqual(result.counts["layout_changed"], 0)
        self.assertEqual(
            AppleMessagesCursor.parse(result.cursor.model_dump(mode="json")),
            result.cursor,
        )

    def test_cli_delivery_status_and_write_guard(self):
        self.message()
        server = HTTPServer(("127.0.0.1", 0), _Handler)
        server.posts = []
        server.response_code = 201
        server.wait_for_reply = server.stall_receipt = False
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        config = self.root / "config.json"
        state = self.root / "state"
        config.write_text(
            json.dumps(
                {
                    "state_dir": str(state),
                    "sources": [
                        {
                            "id": "test",
                            "source": "apple_messages",
                            "token_env": "TEST_MESSAGES_TOKEN",
                            "receive_url": f"http://127.0.0.1:{server.server_port}/receive",
                            "options": {"database": str(self.db)},
                        }
                    ],
                }
            )
        )

        def cli(*args):
            output = StringIO()
            with (
                patch.dict(
                    os.environ, TEST_MESSAGES_TOKEN="test-token", no_proxy="127.0.0.1"
                ),
                redirect_stdout(output),
            ):
                code = main([*args, "--config", str(config)])
            return code, output.getvalue()

        code, output = cli("run")
        self.assertEqual(code, 0, output)
        self.assertIn("delivered", output)
        self.assertEqual(State(state / "test").read_cursor()["message"], ROW)
        self.assertEqual(len(server.posts), 1)
        self.assertIn("apple_messages_batch", server.posts[0][0])
        self.assertIn(b"message-example", gzip.decompress(server.posts[0][2]))
        self.assertIn("nothing_new", cli("run")[1])
        self.assertEqual(len(server.posts), 1)
        self.assertNotIn("message-example", cli("status")[1])
        blocked = self.db.parent / "blocked"
        code, output = cli("collect", "--source", "test", "--out", str(blocked))
        self.assertNotEqual(code, 0)
        self.assertIn("reason=state", output)
        self.assertFalse(blocked.exists())
