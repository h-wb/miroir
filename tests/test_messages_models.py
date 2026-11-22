"""The messages analysis package, built by dbt on a disposable Postgres.

Every row is invented: example.com addresses and 555 numbers. Set
MIROIR_TEST_POSTGRES_BIN to an installed Postgres bin directory, or put
initdb/pg_ctl on PATH.
"""

import getpass
import gzip
import json
import os
import tempfile
import unittest
from collections import Counter
from contextlib import closing, nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import dlt
import psycopg2

from src.core.analysis import analysis_plan
from src.core.artifacts import submit_artifact
from src.core.config import parse_miroir
from src.core.operations import MetadataStore
from src.core.runner import import_artifact
from src.integrations.apple_messages.formats import FIELDS
from src.pipelines.transform import build_connection_models
from tests.postgres import start_postgres

ALEX, SAM, ROBIN, KAI, LEE = (
    "+15555550101",
    "sam@example.com",
    "+15555550102",
    "+15555550103",
    "+15555550104",
)
NOBODY = "+15555550199"
JORDAN, TAYLOR = "+15555550105", "+15555550113"
QUINN, QUINN_LOWER = "Quinn@Example.com", "quinn@example.com"
# January: Toronto is five hours behind UTC.
T0 = datetime(2026, 1, 10, 4, 30, tzinfo=UTC)
APPLE_EPOCH = datetime(2001, 1, 1, tzinfo=UTC)


def apple(moment):
    return int((moment - APPLE_EPOCH).total_seconds()) * 10**9


def at(minutes):
    return T0 + timedelta(minutes=minutes)


def row(kind, **fields):
    return {"type": kind, **dict.fromkeys(FIELDS[kind]), **fields}


def message(guid, minutes, chat, *, me=False, handle=None, text="hi", **fields):
    record = row(
        "message",
        guid=guid,
        date=apple(at(minutes)),
        is_from_me=me,
        handle=handle,
        service=fields.pop("service", "iMessage"),
        text=text,
        text_source=fields.pop("text_source", "text" if text is not None else "none"),
        masked=0,
        **({"item_type": 0, "associated_message_type": 0} | fields),
    )
    links = [row("chat_message", chat_guid=chat, message_guid=guid)] if chat else []
    return [record, *links]


def reaction(guid, minutes, chat, code, target, *, me=False, handle=None):
    return message(
        guid,
        minutes,
        chat,
        me=me,
        handle=handle,
        text=None,
        associated_message_type=code,
        associated_message_guid=target,
    )


def chat(guid, identifier, *handles, style=45, title=None, room=None, service=None):
    return [
        row(
            "chat",
            guid=guid,
            chat_identifier=identifier,
            style=style,
            display_name=title,
            room_name=room,
            service_name=service or guid.split(";")[0],
        ),
        *(
            row("chat_handle", chat_guid=guid, handle=h, handle_service="iMessage")
            for h in handles
        ),
    ]


def attachment(guid, message_guid, **fields):
    return [
        row("attachment", guid=guid, **fields),
        row("message_attachment", message_guid=message_guid, attachment_guid=guid),
    ]


def contact(handle, **names):
    return row("contact", handle=handle, **names)


def undated(records, date):
    records[0]["date"] = date
    return records


def key(handle):
    """A direct conversation's key: the collector's matching rule."""
    if "@" in handle:
        return handle.lower()
    digits = "".join(c for c in handle if c.isdigit())
    return digits[1:] if len(digits) == 11 and digits.startswith("1") else digits


def write_batch(path, records):
    counts = Counter(r["type"] for r in records)
    trailer = {
        "type": "trailer",
        **dict.fromkeys(FIELDS, 0),
        **counts,
        "masked": {},
        "unreadable": sum(r.get("text_source") == "unreadable" for r in records),
    }
    header = {
        "type": "header",
        "format": "apple_messages_batch",
        "version": 1,
        "collected_at": "2026-01-20T00:00:00Z",
    }
    with gzip.open(path, "wt", encoding="utf-8") as out:
        for record in (header, *records, trailer):
            out.write(json.dumps(record) + "\n")
    return path


# One person over iMessage, SMS and a second Mac's renamed chat guid.
DIRECT = ("iMessage;-;" + ALEX, "SMS;-;" + ALEX, "any;-;" + ALEX)
BOOK, UNTITLED, UNSTYLED = "iMessage;+;chat111", "iMessage;+;chat222", "any;+;chat333"
FIXTURE = [
    *chat(DIRECT[0], ALEX, ALEX),
    *chat(DIRECT[1], ALEX, ALEX),
    *chat(DIRECT[2], ALEX, ALEX, service="iMessage"),
    *chat("iMessage;-;" + SAM, SAM, SAM),
    *chat("SMS;-;" + NOBODY, NOBODY),  # a chat with no handles
    *chat(BOOK, "chat111", ALEX, ROBIN, KAI, style=43, title="Book Club", room="r1"),
    *chat(UNTITLED, "chat222", ROBIN, KAI, LEE, style=43, room="r2"),
    *chat(
        UNSTYLED, "chat333", ALEX, ROBIN, KAI, LEE, SAM, style=None, service="iMessage"
    ),
    contact(ALEX, first_name="Alex", last_name="Example"),
    contact(ROBIN, first_name="Robin", last_name="Sample", organization="Example Co"),
    contact(KAI, nickname="Kai"),
    # Direct with Alex: 23:30 local on the 9th, then after midnight on the 10th.
    *message("a1", 0, DIRECT[0], handle=ALEX, text="hello there"),
    *message("a2", 40, DIRECT[1], me=True, handle=ALEX, service="SMS", text="￼"),
    *message("a3", 42, DIRECT[1], me=True, handle=ALEX, service="SMS", text="ok"),
    *message("a4", 102, DIRECT[2], handle=ALEX, text="see this"),
    # Two days later: beyond the reply cap.
    *message("a5", 102 + 48 * 60, DIRECT[0], me=True, handle=ALEX, text="sorry"),
    # Sam replies to me after ten minutes.
    *message("s1", 200, "iMessage;-;" + SAM, me=True, handle=SAM, text="are you in?"),
    *message("s2", 210, "iMessage;-;" + SAM, handle=SAM, text="yes"),
    *message("s3", 215, "iMessage;-;" + SAM, handle=SAM, text=None),
    *message("n1", 300, "SMS;-;" + NOBODY, me=True, service="SMS", text="code?"),
    # Groups.
    *message("g1", 400, BOOK, handle=ROBIN, text="chapter 3", is_audio_message=True),
    *message("g2", 401, BOOK, me=True, text="done", thread_originator_guid="g1"),
    *message("g3", 402, BOOK, handle=KAI, text="edited", date_edited=apple(at(403))),
    *message(
        "g4",
        404,
        BOOK,
        handle=KAI,
        text="",
        date_edited=apple(at(405)),
        date_retracted=apple(at(405)),
    ),
    *message("u1", 500, UNTITLED, handle=LEE, text=None, text_source="unreadable"),
    *message("v1", 600, UNSTYLED, me=True, text="hi all"),
    # Not messages: group events and a poll vote.
    *message("e1", 401, BOOK, handle=ROBIN, text=None, item_type=1),
    *message("e2", 402, BOOK, me=True, text=None, item_type=2, group_title="Club"),
    *message("p1", 403, BOOK, handle=KAI, text=None, associated_message_type=4000),
    # A message in no chat is kept, without a conversation.
    *message("o1", 700, None, handle=SAM, text="orphan"),
    # Reactions.
    *reaction("r1", 41, DIRECT[0], 2000, "p:0/a2", handle=ALEX),
    *reaction("r2", 103, DIRECT[0], 2001, "p:0/a4", me=True, handle=ALEX),
    *reaction("r3", 104, DIRECT[0], 3001, "p:0/a4", me=True, handle=ALEX),
    *reaction("r4", 105, DIRECT[0], 2003, "bp:a1", me=True, handle=ALEX),
    *reaction("r5", 106, None, 2006, "p:1/a4", handle=ALEX),
    *reaction("r6", 107, DIRECT[0], 3000, "p:0/a1", handle=ALEX),
    *reaction("r7", 410, BOOK, 2004, "p:0/g2", handle=ROBIN),
    *reaction("r8", 411, BOOK, 3004, "p:0/g2", handle=ROBIN),
    *reaction("r9", 412, BOOK, 2004, "p:0/g2", handle=ROBIN),
    *reaction("r10", 413, BOOK, 2005, "p:0/g2", handle=KAI),
    *reaction("r11", 414, BOOK, 2007, "p:0/g1", me=True),
    # Attachments; a4's link preview is left out.
    *attachment(
        "att1",
        "a2",
        mime_type="image/jpeg",
        media_key="att1.jpeg",
        total_bytes=1000,
        transfer_name="IMG_0001.JPG",
    ),
    *attachment(
        "att2",
        "a4",
        transfer_name="ABCD.pluginPayloadAttachment",
        total_bytes=50,
        media_key=None,
    ),
    *attachment("att3", "g2", transfer_name="notes.pdf", media_key="att3.pdf"),
    *attachment("att4", "g1", transfer_name="Audio Message.caf", media_key="att4.caf"),
    *attachment("att5", "v1", mime_type="video/quicktime", media_key=None),
    *attachment("att6", "v1", transfer_name="thing.xyz", media_key="att6.xyz"),
    *attachment("att7", "r11", mime_type="image/png", media_key="att7.png"),
    # One person written two ways: an address in two cases, a number with and
    # without its country code.
    *chat("iMessage;-;" + QUINN, QUINN, QUINN),
    *chat("iMessage;-;" + QUINN_LOWER, QUINN_LOWER, QUINN_LOWER),
    contact(QUINN, first_name="Quinn", last_name="Sample"),
    *message("q1", 800, "iMessage;-;" + QUINN, handle=QUINN),
    *message("q2", 805, "iMessage;-;" + QUINN_LOWER, me=True, handle=QUINN_LOWER),
    *chat("SMS;-;5555550113", "5555550113", "5555550113"),
    *chat("iMessage;-;" + TAYLOR, TAYLOR, TAYLOR),
    *message("t1", 900, "SMS;-;5555550113", handle="5555550113", service="SMS"),
    *message("t2", 903, "iMessage;-;" + TAYLOR, me=True, handle=TAYLOR),
    # Jordan sends three in a row (the first unsent); I answer, twice.
    *chat("iMessage;-;" + JORDAN, JORDAN, JORDAN),
    *message(
        "j1",
        1000,
        "iMessage;-;" + JORDAN,
        handle=JORDAN,
        text="",
        date_retracted=apple(at(1001)),
    ),
    *message("j2", 1010, "iMessage;-;" + JORDAN, handle=JORDAN),
    *message("j3", 1020, "iMessage;-;" + JORDAN, handle=JORDAN),
    *message("j4", 1030, "iMessage;-;" + JORDAN, me=True, handle=JORDAN),
    *message("j5", 1031, "iMessage;-;" + JORDAN, me=True, handle=JORDAN),
    *message("j6", 1040, "iMessage;-;" + JORDAN, handle=JORDAN),
    # Undated: no date, and a date at Apple's epoch.
    *undated(message("z1", 0, "iMessage;-;" + SAM, handle=SAM), None),
    *undated(message("z2", 0, "iMessage;-;" + SAM, me=True, handle=SAM), 1),
    # Stickers are each their own reaction; a sticker removal cancels none.
    *reaction("k1", 420, BOOK, 1000, "p:0/g1", handle=ROBIN),
    *reaction("k2", 421, BOOK, 2007, "p:0/g1", handle=ROBIN),
    *reaction("k3", 422, BOOK, 3007, "p:0/g1", handle=ROBIN),
    # A tapback changed from love to like with no removal row: only like stands.
    *reaction("k4", 430, BOOK, 2000, "p:0/g3", handle=KAI),
    *reaction("k5", 431, BOOK, 2001, "p:0/g3", handle=KAI),
    # An untitled group with nobody listed, and a chat with no identifier.
    *chat("iMessage;+;chat444", "chat444", style=43),
    *message("w1", 1100, "iMessage;+;chat444", me=True),
    *chat("iMessage;-;", ""),
]


class MessagesModelsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root, cls.port = start_postgres(cls, "miroir-messages-")
        environment = patch.dict(
            os.environ,
            {
                "DESTINATION__POSTGRES__CREDENTIALS__HOST": "127.0.0.1",
                "DESTINATION__POSTGRES__CREDENTIALS__PORT": str(cls.port),
                "DESTINATION__POSTGRES__CREDENTIALS__USERNAME": getpass.getuser(),
                "DESTINATION__POSTGRES__CREDENTIALS__PASSWORD": "",
                "DESTINATION__POSTGRES__CREDENTIALS__DATABASE": "postgres",
            },
        )
        environment.start()
        cls.addClassCleanup(environment.stop)
        concurrency = patch(
            "src.pipelines.transform.concurrency",
            side_effect=lambda *a, **kw: nullcontext(),
        )
        concurrency.start()
        cls.addClassCleanup(concurrency.stop)
        cls.store = MetadataStore(
            f"postgresql+psycopg2://{getpass.getuser()}@127.0.0.1:{cls.port}/postgres"
        )
        cls.store.migrate()
        cls.addClassCleanup(cls.store.engine.dispose)
        cls.miroir = cls.configuration("messages", "messages_unnamed")
        cls.plan = analysis_plan(cls.miroir.analyses[0])
        cls.outcome = cls.load("messages", FIXTURE)

    @classmethod
    def configuration(cls, *ids):
        return parse_miroir(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "messages_tests",
                    "connections": [
                        {"id": i, "integration": "apple_messages", "settings": {}}
                        for i in ids
                    ],
                    "analyses": [
                        {
                            "id": i,
                            "package": "messages",
                            "inputs": {"messages": i},
                            "timezone": "America/Toronto",
                        }
                        for i in ids
                    ],
                }
            )
        )

    @classmethod
    def load(cls, connection_id, records):
        path = write_batch(
            Path(tempfile.mkdtemp(dir=cls.root)) / "batch.jsonl.gz", records
        )
        connection = next(
            c for c in cls.miroir.connections if c.config.id == connection_id
        )
        artifact, _ = submit_artifact(
            cls.miroir,
            connection,
            path,
            store=cls.store,
            root=cls.root / "artifacts",
        )
        return import_artifact(
            cls.miroir,
            connection_id,
            artifact["id"],
            origin="local",
            store=cls.store,
            destination=dlt.destinations.postgres(
                credentials={
                    "host": "127.0.0.1",
                    "port": cls.port,
                    "database": "postgres",
                    "username": getpass.getuser(),
                    "password": "",
                }
            ),
            resolve_credentials=lambda c: {},
            pipelines_dir=str(cls.root / "dlt"),
            transform=build_connection_models,
        )

    def query(self, sql):
        with (
            closing(
                psycopg2.connect(
                    host="127.0.0.1",
                    port=self.port,
                    user=getpass.getuser(),
                    dbname="postgres",
                )
            ) as db,
            db,
            db.cursor() as cursor,
        ):
            cursor.execute(sql)
            return cursor.fetchall() if cursor.description else []

    def relation(self, name, plan=None):
        plan = plan or self.plan
        return next(r for r in plan.relations if r.endswith("." + name))

    def test_a_new_connection_gets_the_package_and_the_import_builds_it(self):
        self.assertEqual(
            (self.outcome.ingestion, self.outcome.transformation),
            ("succeeded", "succeeded"),
        )
        upgraded = parse_miroir(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "messages_tests",
                    "connections": [
                        {"id": "m", "integration": "apple_messages", "settings": {}}
                    ],
                }
            )
        )
        self.assertEqual(upgraded.analyses[0].package.id, "messages")

    def test_conversations_are_keyed_by_person_and_named(self):
        rows = self.query(
            "select conversation_key, display_name, is_group, participants, "
            "messages_sent, messages_received, attachments, services "
            f"from {self.relation('conversations')} order by conversation_key"
        )
        self.assertEqual(
            rows,
            [
                (key(ALEX), "Alex Example", False, 1, 3, 2, 1, "SMS, iMessage"),
                (key(JORDAN), JORDAN, False, 1, 2, 4, 0, "iMessage"),
                # As written in the chat with the latest message.
                (key(TAYLOR), TAYLOR, False, 1, 1, 1, 0, "SMS, iMessage"),
                (key(NOBODY), NOBODY, False, 0, 1, 0, 0, "SMS"),
                ("chat111", "Book Club", True, 3, 1, 3, 2, "iMessage"),
                ("chat222", f"{LEE}, Kai, Robin Sample", True, 3, 0, 1, 0, "iMessage"),
                (
                    "chat333",
                    f"{LEE}, Alex Example, Kai, Robin Sample +1",
                    True,
                    5,
                    1,
                    0,
                    2,
                    "iMessage",
                ),
                ("chat444", "Group", True, 0, 1, 0, 0, "iMessage"),
                # No identifier and nothing after the guid's prefix: the guid.
                ("iMessage;-;", "iMessage;-;", False, 0, 0, 0, 0, "iMessage"),
                (QUINN_LOWER, "Quinn Sample", False, 1, 1, 1, 0, "iMessage"),
                # Undated messages count here, but not in the first and last times.
                (SAM, SAM, False, 1, 2, 3, 0, "iMessage"),
            ],
        )
        self.assertEqual(
            self.query(
                "select conversation_key, first_message_at, last_message_at "
                f"from {self.relation('conversations')} "
                f"where conversation_key in ('{key(ALEX)}', '{SAM}') order by 1"
            ),
            [(key(ALEX), at(0), at(102 + 48 * 60)), (SAM, at(200), at(215))],
        )

    def test_messages_exclude_reactions_and_events_and_carry_no_text(self):
        relation = self.relation("messages")
        guids = [g for (g,) in self.query(f"select guid from {relation} order by guid")]
        self.assertEqual(
            guids,
            ["a1", "a2", "a3", "a4", "a5", "g1", "g2", "g3", "g4"]
            + ["j1", "j2", "j3", "j4", "j5", "j6", "n1", "o1", "q1", "q2"]
            + ["s1", "s2", "s3", "t1", "t2", "u1", "v1", "w1", "z1", "z2"],
        )
        columns = {
            c
            for (c,) in self.query(
                "select column_name from information_schema.columns "
                f"where table_schema || '.' || table_name = '{relation}'"
            )
        }
        self.assertFalse({"text", "subject", "group_title"} & columns)
        self.assertEqual(
            self.query(
                "select guid, conversation_key, is_from_me, sender_handle, sender_name, "
                "has_text, text_length, attachments, is_thread_reply, is_edited, "
                f"is_unsent, is_audio from {relation} "
                "where guid in ('a1', 'a2', 'g1', 'g2', 'g3', 'g4', 'u1', 'o1', 's3') "
                "order by guid"
            ),
            [
                ("a1", key(ALEX), False, key(ALEX), "Alex Example", True, 11, 0)
                + (False, False, False, False),
                ("a2", key(ALEX), True, None, None, False, 0, 1)
                + (False, False, False, False),
                ("g1", "chat111", False, key(ROBIN), "Robin Sample", True, 9, 1)
                + (False, False, False, True),
                ("g2", "chat111", True, None, None, True, 4, 1)
                + (True, False, False, False),
                ("g3", "chat111", False, key(KAI), "Kai", True, 6, 0)
                + (False, True, False, False),
                ("g4", "chat111", False, key(KAI), "Kai", False, 0, 0)
                + (False, False, True, False),
                ("o1", None, False, SAM, SAM, True, 6, 0)
                + (False, False, False, False),
                ("s3", SAM, False, SAM, SAM, False, 0, 0)
                + (False, False, False, False),
                # No contact: the handle as written.
                ("u1", "chat222", False, key(LEE), LEE, None, None, 0)
                + (False, False, False, False),
            ],
        )

    def test_local_day_and_hour_cross_midnight_in_the_timezone(self):
        self.assertEqual(
            self.query(
                "select guid, day::text, hour, weekday "
                f"from {self.relation('messages')} where guid in ('a1', 'a2') "
                "order by guid"
            ),
            [("a1", "2026-01-09", 23, 5), ("a2", "2026-01-10", 0, 6)],
        )

    def test_a_missing_or_epoch_date_is_no_time(self):
        self.assertEqual(
            self.query(
                "select guid, conversation_key, sent_at, day, hour, reply_minutes "
                f"from {self.relation('messages')} where guid in ('z1', 'z2') "
                "order by guid"
            ),
            [("z1", SAM, None, None, None, None), ("z2", SAM, None, None, None, None)],
        )
        self.assertEqual(
            self.query(f"select min(day)::text from {self.relation('daily')}"),
            [("2026-01-09",)],
        )

    def test_reply_minutes_count_from_the_first_message_of_the_other_run(self):
        rows = self.query(
            "select guid, reply_minutes from "
            f"{self.relation('messages')} where guid in "
            "('a1', 'a2', 'a3', 'a4', 'a5', 's1', 's2', 's3', 'g1', 'g2', 'o1', "
            "'j1', 'j2', 'j3', 'j4', 'j5', 'j6', 'q1', 'q2', 't1', 't2') "
            "order by guid"
        )
        self.assertEqual(
            rows,
            [
                ("a1", None),  # nothing before it
                ("a2", 40.0),  # mine, answering Alex
                ("a3", None),  # mine again
                ("a4", 62.0),  # Alex, answering my run that began with a2
                ("a5", None),  # two days later: beyond the cap
                ("g1", None),  # groups have no reply time
                ("g2", None),
                ("j1", None),
                ("j2", None),
                ("j3", None),
                ("j4", 30.0),  # since j1, the first of Jordan's three (unsent)
                ("j5", None),
                ("j6", 10.0),
                ("o1", None),
                ("q1", None),
                ("q2", 5.0),  # across two ways of writing one address
                ("s1", None),
                ("s2", 10.0),
                ("s3", None),
                ("t1", None),
                ("t2", 3.0),  # across a number with and without its country code
            ],
        )

    def test_reactions_added_removed_changed_and_stickers(self):
        self.assertEqual(
            self.query(
                "select guid, conversation_key, kind, is_from_me, sender_handle, "
                "sender_name, target_guid, target_part "
                f"from {self.relation('reactions')} order by guid"
            ),
            [
                (
                    "k1",
                    "chat111",
                    "sticker",
                    False,
                    key(ROBIN),
                    "Robin Sample",
                    "g1",
                    0,
                ),
                (
                    "k2",
                    "chat111",
                    "sticker",
                    False,
                    key(ROBIN),
                    "Robin Sample",
                    "g1",
                    0,
                ),
                ("k5", "chat111", "like", False, key(KAI), "Kai", "g3", 0),
                ("r1", key(ALEX), "love", False, key(ALEX), "Alex Example", "a2", 0),
                ("r10", "chat111", "question", False, key(KAI), "Kai", "g2", 0),
                ("r11", "chat111", "sticker", True, None, None, "g1", 0),
                ("r4", key(ALEX), "laugh", True, None, None, "a1", 0),
                # In no chat: the conversation of the message it targets.
                ("r5", key(ALEX), "emoji", False, key(ALEX), "Alex Example", "a4", 1),
                (
                    "r9",
                    "chat111",
                    "emphasis",
                    False,
                    key(ROBIN),
                    "Robin Sample",
                    "g2",
                    0,
                ),
            ],
        )

    def test_participants_are_people_whatever_the_service_or_spelling(self):
        self.assertEqual(
            self.query(
                "select conversation_key, handle, display_name, has_contact_name, "
                f"messages_sent from {self.relation('participants')} "
                f"where conversation_key in ('{key(ALEX)}', 'chat111', "
                f"'{key(NOBODY)}', '{key(TAYLOR)}', '{QUINN_LOWER}') "
                "order by 1, 2"
            ),
            [
                (key(ALEX), key(ALEX), "Alex Example", True, 2),
                (key(TAYLOR), key(TAYLOR), TAYLOR, False, 1),
                ("chat111", key(ALEX), "Alex Example", True, 0),
                ("chat111", key(ROBIN), "Robin Sample", True, 1),
                ("chat111", key(KAI), "Kai", True, 2),
                (QUINN_LOWER, QUINN_LOWER, "Quinn Sample", True, 1),
            ],
        )

    def test_attachments_leave_out_link_previews_and_reactions(self):
        self.assertEqual(
            self.query(
                "select attachment_guid, message_guid, conversation_key, is_from_me, "
                f"kind, bytes, has_media_key from {self.relation('attachments')} "
                "order by 1"
            ),
            [
                ("att1", "a2", key(ALEX), True, "image", 1000, True),
                ("att3", "g2", "chat111", True, "document", None, True),
                ("att4", "g1", "chat111", False, "audio", None, True),
                ("att5", "v1", "chat333", True, "video", None, False),
                ("att6", "v1", "chat333", True, "other", None, True),
            ],
        )

    def test_daily_counts_every_local_day(self):
        self.assertEqual(
            self.query(
                "select day::text, messages_sent, messages_received, "
                "conversations_active, attachments, reactions "
                f"from {self.relation('daily')} order by day"
            ),
            [
                ("2026-01-09", 0, 1, 1, 0, 0),
                ("2026-01-10", 11, 14, 10, 5, 9),
                ("2026-01-11", 0, 0, 0, 0, 0),
                ("2026-01-12", 1, 0, 1, 0, 0),
            ],
        )

    def test_the_package_builds_at_most_every_ten_minutes(self):
        self.assertEqual(
            self.miroir.analyses[0].package.min_build_interval, timedelta(minutes=10)
        )

    def test_builds_without_contacts_handles_or_chats(self):
        records = [
            *chat("iMessage;-;" + ALEX, ALEX),
            *message("x1", 0, "iMessage;-;" + ALEX, handle=ALEX),
            *message("x2", 5, "iMessage;-;" + ALEX, me=True, handle=ALEX),
        ]
        outcome = self.load("messages_unnamed", records)
        self.assertEqual(outcome.transformation, "succeeded")
        plan = analysis_plan(self.miroir.analyses[1])
        raw = self.miroir.connections[1].binding.dataset_name
        self.assertEqual(
            self.query(
                "select table_name from information_schema.tables "
                f"where table_schema = '{raw}' and table_name = 'contacts'"
            ),
            [],
        )
        expected = [(key(ALEX), ALEX, False, 1, 1, 1)]
        conversations = (
            "select conversation_key, display_name, is_group, participants, "
            f"messages_sent, messages_received from {self.relation('conversations', plan)}"
        )
        self.assertEqual(self.query(conversations), expected)
        # An empty contacts table, as when names were never matched.
        self.query(f"create table {raw}.contacts (handle text, first_name text)")
        build_connection_models(plan)
        self.assertEqual(self.query(conversations), expected)
        self.assertEqual(
            self.query(
                f"select guid, sender_name, reply_minutes from "
                f"{self.relation('messages', plan)} order by guid"
            ),
            [("x1", ALEX, None), ("x2", None, 5.0)],
        )


if __name__ == "__main__":
    unittest.main()
