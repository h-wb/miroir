import gzip
import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from miroir_collector.errors import Failure
from miroir_collector.sources.apple_messages import AppleMessages, AppleMessagesCursor

from .masking_fixtures import MASKER
from .test_apple_messages_source import NOW, SCHEMA

BOOK_SCHEMA = """
CREATE TABLE ZABCDRECORD (Z_PK INTEGER PRIMARY KEY, ZFIRSTNAME TEXT,
    ZLASTNAME TEXT, ZNICKNAME TEXT, ZORGANIZATION TEXT);
CREATE TABLE ZABCDPHONENUMBER (ZOWNER INTEGER, ZFULLNUMBER TEXT);
CREATE TABLE ZABCDEMAILADDRESS (ZOWNER INTEGER, ZADDRESS TEXT, ZADDRESSNORMALIZED TEXT);
"""
SOURCE = "miroir_collector.sources.apple_messages"
SECOND = 1700000000


def address_book(home, source=None):
    folder = home / "Library/Application Support/AddressBook"
    if source:
        folder = folder / "Sources" / source
    folder.mkdir(parents=True, exist_ok=True)
    return folder / "AddressBook-v22.abcddb"


class AppleMessagesPart2Tests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.home = self.root / "restored home"
        self.attachments = self.home / "Library/Messages/Attachments"
        self.attachments.mkdir(parents=True)
        self.db = self.attachments.parent / "chat.db"
        self.out = self.root / "batch.gz"
        self.source = AppleMessages()
        self.script(SCHEMA)

    def script(self, sql):
        with closing(sqlite3.connect(self.db)) as db:
            db.executescript(sql)

    def options(self, **options):
        return self.source.options_model.model_validate(
            {"database": str(self.db), "home": str(self.home), **options}
        )

    def attachment(self, key, filename, contents=None):
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute(
                "INSERT INTO attachment(guid, filename) VALUES (?, ?)", (key, filename)
            )
        if contents is not None:
            (self.attachments / Path(filename).name).write_bytes(contents)

    def test_attachment_paths_and_counts(self):
        inside = "~/Library/Messages/Attachments/"
        self.attachment("have", inside + "Clip.MOV", b"movie")
        self.attachment("have", inside + "Clip.MOV")
        self.attachment("missing", inside + "missing.jpg")
        self.attachment("null-key", inside + "noextension", b"data")
        self.attachment("empty", inside + "empty.jpg", b"")
        self.attachment("folder", inside + "folder.jpg")
        (self.attachments / "folder.jpg").mkdir()
        outside = self.attachments.parent / "outside.jpg"
        outside.write_bytes(b"not an attachment")
        (self.attachments / "link.jpg").symlink_to(outside)
        self.attachment("absolute", str(outside))
        self.attachment("traversal", inside + "../outside.jpg")
        self.attachment("symlink", inside + "link.jpg")
        options = self.options(media=True)
        self.assertIn(self.attachments, self.source.source_dirs(options))
        media = self.source.media(options)
        self.assertIsNone(media.limit(None))
        with patch(
            f"{SOURCE}.time.time",
            return_value=(self.attachments / "Clip.MOV").stat().st_ctime + 120,
        ):
            page = media.page(media.start(), media.limit(AppleMessagesCursor()), 500)
            self.assertEqual(
                page.counts, {"examined": 5, "missing": 1, "invalid": 6, "too_large": 0}
            )
            self.assertEqual(
                [(i.key, i.path, i.size) for i in page.items],
                [("have.mov", self.attachments / "Clip.MOV", 5)],
            )
            self.assertIsNone(media.page(page.position, 2**62, 500))

    def test_attachment_position_ties_settling_and_late_files(self):
        stamps = {
            "z.jpg": SECOND,
            "a.jpg": SECOND,
            "big.jpg": SECOND + 1,
            "new.jpg": SECOND + 50,
        }
        stat = Path.stat

        def timed_stat(path, *args, **kwargs):
            info = stat(path, *args, **kwargs)
            if path.parent == self.attachments and path.name in stamps:
                return SimpleNamespace(
                    st_ctime=stamps[path.name],
                    st_size=info.st_size,
                    st_mode=info.st_mode,
                )
            return info

        for key in ("z", "a", "big", "new", "late"):
            self.attachment(
                key,
                str(self.attachments / f"{key}.jpg"),
                None if key == "late" else b"x" * (1048577 if key == "big" else 1),
            )
        media = self.source.media(self.options(media=True, media_max_mb=1))
        with (
            patch.object(Path, "stat", timed_stat),
            patch(f"{SOURCE}.time.time", return_value=SECOND + 61) as clock,
        ):
            page = media.page(0, 2**62, 1)
            self.assertEqual([i.key for i in page.items], ["a.jpg", "z.jpg"])
            self.assertEqual((page.position, page.more), (SECOND, True))
            page = media.page(page.position, 2**62, 1)
            self.assertEqual(
                (page.position, page.more, page.items), (SECOND + 1, False, [])
            )
            self.assertEqual(page.counts["too_large"], 1)
            self.assertIsNone(media.page(page.position, 2**62, 1))
            clock.return_value = SECOND + 110
            page = media.page(page.position, 2**62, 1)
            self.assertEqual([i.key for i in page.items], ["new.jpg"])
            self.assertIsNone(media.page(page.position, 2**62, 1))
            (self.attachments / "late.jpg").write_bytes(b"late")
            stamps["late.jpg"] = SECOND + 60
            clock.return_value += 10
            self.assertEqual(
                [i.key for i in media.page(page.position, 2**62, 1).items], ["late.jpg"]
            )

    def test_unreadable_messages_database_for_media(self):
        self.db.write_text("not SQLite")
        media = self.source.media(self.options(media=True))
        with self.assertRaises(Failure) as error:
            media.page(0, 2**62, 1)
        self.assertEqual(
            (error.exception.step, error.exception.reason), ("collect", "source")
        )

    def collect(self, cursor=None, *, window=None, **options):
        self.out.unlink(missing_ok=True)
        batch = self.source.collect(
            self.options(**options), cursor, self.out, NOW, masker=MASKER, window=window
        )
        if batch is None:
            return None, [], {}
        with gzip.open(self.out, "rt") as file:
            records = [json.loads(line) for line in file]
        return batch, [r for r in records if r["type"] == "contact"], records[-1]

    def test_matches_only_messaged_handles_and_uses_deterministic_winner(self):
        self.script("""
            INSERT INTO handle VALUES ('4155550100', 'SMS'), ('+1 (415) 555-0100', 'iMessage'),
                ('WORK@EXAMPLE.ORG', 'iMessage'), ('other@example.org', 'iMessage'),
                ('blank@example.org', 'iMessage'), ('nobody@example.org', 'iMessage'),
                ('4155550100', 'iMessage');
        """)
        local = address_book(self.home)
        remote = address_book(self.home, "account")
        with closing(sqlite3.connect(remote)) as db:
            db.executescript(
                BOOK_SCHEMA
                + """
                INSERT INTO ZABCDRECORD VALUES (1, 'Cloud', NULL, NULL, NULL);
                INSERT INTO ZABCDPHONENUMBER VALUES (1, '4155550100');
            """
            )
        with closing(sqlite3.connect(local)) as db:
            db.executescript(
                BOOK_SCHEMA
                + """
                INSERT INTO ZABCDRECORD VALUES (3, 'Second', NULL, NULL, NULL),
                    (2, 'Alice' || char(0) || 'A', NULL, ' ', NULL),
                    (4, NULL, '', NULL, 'Example Inc.'), (5, ' ', NULL, '', NULL),
                    (6, 'Never Messaged', NULL, NULL, NULL), (7, 'Other', NULL, NULL, NULL);
                INSERT INTO ZABCDPHONENUMBER VALUES (2, '+1 (415) 555-0100'),
                    (2, '+14155550100'), (3, '4155550100');
                INSERT INTO ZABCDEMAILADDRESS VALUES (4, 'ignored@example.org', 'work@example.org'),
                    (5, 'blank@example.org', NULL), (6, 'private@example.org', NULL),
                    (7, 'OTHER@EXAMPLE.ORG', NULL);
            """
            )
        self.assertTrue(
            {local.parent, remote.parent}.issubset(
                self.source.source_dirs(self.options())
            )
        )
        batch, contacts, trailer = self.collect()
        self.assertEqual(
            {r["handle"] for r in contacts},
            {
                "4155550100",
                "+1 (415) 555-0100",
                "WORK@EXAMPLE.ORG",
                "other@example.org",
            },
        )
        by_key = {r["handle"]: r for r in contacts}
        self.assertEqual(by_key["4155550100"]["first_name"], "Alice\ufffdA")
        self.assertIsNone(by_key["4155550100"]["nickname"])
        self.assertEqual(by_key["WORK@EXAMPLE.ORG"]["organization"], "Example Inc.")
        self.assertNotIn(b"Never Messaged", gzip.decompress(self.out.read_bytes()))
        self.assertEqual(
            (batch.counts["names_unavailable"], batch.counts["names_ambiguous"]), (0, 2)
        )
        self.assertEqual(batch.counts["contact"], 4)
        self.assertNotIn("names_ambiguous", trailer)

    def test_names_unavailable_without_failing_rows(self):
        for reason in ("absent", "not_db", "no_tables", "no_columns"):
            with self.subTest(reason=reason):
                if reason == "not_db":
                    book = address_book(self.home)
                    book.write_text("not SQLite")
                elif reason == "no_tables":
                    book.unlink()
                    with closing(sqlite3.connect(book)) as db:
                        db.execute("CREATE TABLE unrelated (id)")
                elif reason == "no_columns":
                    with closing(sqlite3.connect(book)) as db:
                        db.executescript(
                            BOOK_SCHEMA
                            + "ALTER TABLE ZABCDEMAILADDRESS DROP COLUMN ZADDRESSNORMALIZED;"
                        )
                batch, contacts, _ = self.collect()
                self.assertEqual(contacts, [])
                self.assertEqual(batch.counts["names_unavailable"], 1)

    def test_name_only_updates_old_cursor_disabled_resend_and_invalid_keys(self):
        self.script("""
            INSERT INTO handle VALUES ('test@example.org', 'iMessage'),
                ('415' || char(0) || '5550100', 'SMS'),
                ('415' || CAST(X'ff' AS TEXT) || '5550100', 'SMS');
            INSERT INTO message(guid, date, text) VALUES ('first', 800000000, 'hi');
        """)
        book = address_book(self.home)
        with closing(sqlite3.connect(book)) as db:
            db.executescript(
                BOOK_SCHEMA
                + """
                INSERT INTO ZABCDRECORD VALUES (1, 'First', NULL, NULL, NULL);
                INSERT INTO ZABCDEMAILADDRESS VALUES (1, 'test@example.org', NULL);
                INSERT INTO ZABCDPHONENUMBER VALUES (1, '4155550100');
            """
            )
        with patch(
            f"{SOURCE}._address_books", side_effect=AssertionError("read address book")
        ):
            first, contacts, _ = self.collect(names=False)
            self.assertEqual(contacts, [])
        with patch(f"{SOURCE}._contacts", side_effect=AssertionError("read names")):
            self.assertEqual(self.collect(window=(0, int(NOW) + 1))[1], [])
        old = AppleMessagesCursor.parse(
            first.cursor.model_dump(exclude={"contacts_digest"})
        )
        batch, contacts, _ = self.collect(old)
        self.assertEqual([r["handle"] for r in contacts], ["test@example.org"])
        self.assertEqual((batch.counts["message"], batch.counts["skipped"]), (0, 2))
        self.assertIsNone(self.collect(batch.cursor)[0])
        with closing(sqlite3.connect(book)) as db, db:
            db.execute("UPDATE ZABCDRECORD SET ZFIRSTNAME = 'Changed'")
        batch, contacts, _ = self.collect(batch.cursor)
        self.assertEqual([r["first_name"] for r in contacts], ["Changed"])
        with closing(sqlite3.connect(book)) as db, db:
            db.execute("UPDATE ZABCDRECORD SET ZFIRSTNAME = ' '")
        self.script("INSERT INTO message(guid, date) VALUES ('second', 800000001)")
        final, contacts, _ = self.collect(batch.cursor)
        self.assertEqual(
            (final.counts["message"], final.counts["contact"], contacts), (1, 0, [])
        )
