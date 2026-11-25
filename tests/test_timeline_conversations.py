"""Conversations in the timeline's queries and JSON API (decision 040, phase 2c,
part 1), on a disposable Postgres (MIROIR_TEST_POSTGRES_BIN, see
tests/postgres.py).

Synthetic Apple Messages batches, with example.com addresses and 555 numbers,
are loaded and built by the real models; the server reads them as the reader
role, through its read-only, time-limited sessions.
"""

import base64
import json
from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import patch

import sqlalchemy as sa
from fastapi.testclient import TestClient

from src.core.analysis import analysis_plan
from src.core.config import parse_miroir
from src.core.operations import OperationError
from src.core.service import InlineExecutor, MiroirService
from src.core.timeline import (
    MAX_EVENT_TEXT,
    MAX_TEXT,
    NO_MESSAGES,
    NOT_BUILT,
    TimelineDatabase,
    TimelineReader,
    TimelineSource,
    TimelineTimeout,
    TimelineUnavailable,
    timeline_source,
)
from src.web.app import create_app
from tests.fakes import FakeObjectStore
from tests.test_messages_models import (
    ALEX,
    BOOK,
    DIRECT,
    KAI,
    LEE,
    NOBODY,
    ROBIN,
    SAM,
    apple,
    at,
    attachment,
    chat,
    contact,
    message,
    reaction,
    undated,
)
from tests.test_timeline_messages import TimelineMessagesFixture
from tests.test_timeline_models import READER
from tests.test_web import BEARER, TOKEN

MIROIR_ID = "timeline_conversations"
ZONE = "America/Toronto"
SAM_CHAT = "iMessage;-;" + SAM
NOBODY_CHAT = "SMS;-;" + NOBODY
LONG = "long " + "y" * (MAX_TEXT + 100)


def raw_configuration(inputs=None):
    inputs = inputs or {"messages": "m"}
    connections = [{"id": "m", "integration": "apple_messages", "settings": {}}]
    if "browser" in inputs:
        connections.append(
            {"id": "b", "integration": "browser_history", "settings": {}}
        )
    return {
        "schema_version": 1,
        "id": MIROIR_ID,
        "connections": connections,
        "analyses": [
            {
                "id": "timeline",
                "package": "timeline",
                "inputs": inputs,
                "timezone": ZONE,
                "parameters": {"reader_role": READER},
            }
        ],
    }


def alex_messages():
    """21 messages with Alex, three to a minute so paging meets ties on sent_at,
    their guids out of the order they were sent in."""
    records = []
    for n in range(21):
        minutes = n // 3 * 20  # 23:30 local on the 9th, past midnight from n=6
        guid = f"a{n * 8 % 21:02d}"  # a permutation of a00 to a20
        fields = {}
        text = f"note {n}"
        if n == 6:
            fields["thread_originator_guid"] = "a19"  # n=5
        if n == 7:
            fields["date_edited"] = apple(at(minutes + 1))
        if n == 8:
            text = ""
            fields["date_retracted"] = apple(at(minutes + 1))
        if n == 9:
            fields["is_audio_message"] = True
        if n == 10:
            text = LONG
        if n == 11:
            text = "the invoice for Alex"
        records += message(
            guid, minutes, DIRECT[0], me=n % 2 == 0, handle=ALEX, text=text, **fields
        )
    return records


def history():
    return [
        *chat(DIRECT[0], ALEX, ALEX),
        *chat(BOOK, "chat111", ALEX, ROBIN, KAI, style=43, title="Book Club"),
        *chat(SAM_CHAT, SAM, SAM),
        *chat(NOBODY_CHAT, NOBODY, NOBODY),
        *chat("iMessage;-;" + LEE, LEE, LEE),  # no message: not listed
        contact(ALEX, first_name="Alex", last_name="Example"),
        contact(ROBIN, first_name="Robin", last_name="Sample"),
        contact(KAI, nickname="Kai"),
        *alex_messages(),
        *undated(message("az", 0, DIRECT[0], handle=ALEX, text="no date"), None),
        # n=1 (a08): a thumbnail made; n=2 (a16): none could be made; n=3 (a03):
        # no record yet; n=4 (a11): a file with no key.
        *attachment("p1", "a08", mime_type="image/jpeg", media_key="p1.jpeg"),
        *attachment("p2", "a16", mime_type="image/png", media_key="p2.png"),
        *attachment("p3", "a03", mime_type="image/jpeg", media_key="p3.jpeg"),
        *attachment("p4", "a11", mime_type="video/quicktime", media_key=None),
        # On n=5 (a19): Alex loves it, I like it.
        *reaction("ra", 81, DIRECT[0], 2000, "p:0/a19", handle=ALEX),
        *reaction("rb", 82, DIRECT[0], 2001, "p:0/a19", me=True, handle=ALEX),
        *message("g1", 200, BOOK, handle=ROBIN, text="chapter four invoice"),
        *message("g2", 201, BOOK, me=True, text="done", thread_originator_guid="g1"),
        *message("g3", 202, BOOK, handle=KAI, text="invoice paid"),
        *reaction("rk", 203, BOOK, 2004, "p:0/g2", handle=KAI),
        *message("s1", 300, SAM_CHAT, handle=SAM, text="invoice due " + "z" * 300),
        *message("s2", 301, SAM_CHAT, me=True, handle=SAM, text="ok"),
        *message("s3", 302, SAM_CHAT, handle=SAM, text=None),
        *undated(message("n1", 0, NOBODY_CHAT, handle=NOBODY, service="SMS"), 1),
    ]


class ConversationsFixture(TimelineMessagesFixture):
    built = False

    def setUp(self):
        super().setUp()
        self.query(f"alter role {READER} login")
        self.raw = raw_configuration()
        self.miroir = parse_miroir(json.dumps(self.raw))
        self.schema_name = self.schema(self.miroir, "timeline")
        if not type(self).built:
            self.deliver(self.miroir, history())
            self.media_record(self.miroir, "p1.jpeg")
            self.media_record(self.miroir, "p2.png", status="failed")
            self.build(analysis_plan(self.miroir.analyses[0]))
            type(self).built = True
        self.source = timeline_source(self.miroir)
        self.database = self.reader_database()
        self.reader = TimelineReader(self.database, self.source)
        self.objects = FakeObjectStore()
        self.ids = {
            name: conversation_id
            for conversation_id, name in self.query(
                "select conversation_id, display_name "
                f"from {self.schema_name}.conversations"
            )
        }
        self.alex, self.book = self.ids["Alex Example"], self.ids["Book Club"]
        self.sam = self.ids[SAM]

    def reader_database(self, timeout=None):
        url = f"postgresql://{READER}@127.0.0.1:{self.port}/postgres"
        options = {} if timeout is None else {"statement_timeout_ms": timeout}
        database = TimelineDatabase(url, **options)
        self.addCleanup(database.close)
        return database

    def client(self, raw=None, database: object = True):
        self.clients = getattr(self, "clients", 0) + 1
        config = self.root / f"conversations-{self.clients}.json"
        media = {"endpoint": "https://s3.example.org", "bucket": "media"}
        config.write_text(json.dumps({**(raw or self.raw), "media": media}))
        service = MiroirService(
            config,
            self.store,
            self.root / "artifacts",
            InlineExecutor(self.fail),
            object_store=self.objects,
        )
        if database is True:
            database = self.database
        return TestClient(create_app(service, TOKEN, timeline=database or None))

    def dated(self, conversation_id):
        """The conversation's dated messages in order, read as the owner."""
        return [
            guid
            for (guid,) in self.query(
                f"select guid from {self.schema_name}.messages "
                f"where conversation_id = '{conversation_id}' and not deleted "
                "and sent_at is not null order by sent_at, guid"
            )
        ]

    def page(self, client, conversation_id, **parameters):
        response = client.get(
            f"/api/timeline/conversations/{conversation_id}/messages",
            params=parameters,
            headers=BEARER,
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def index(self, definition):
        """The name of the index of `messages` whose definition holds this."""
        (name,) = [
            name
            for name, made in self.query(
                "select indexname, indexdef from pg_indexes "
                f"where schemaname = '{self.schema_name}' and tablename = 'messages'"
            )
            if definition in made
        ]
        return f'"{name}"' if name[0].isdigit() else name

    @contextmanager
    def explained(self, marker):
        """Each statement holding `marker` explained as the reader runs it."""
        plans = []
        session = TimelineReader._session

        @contextmanager
        def recording(reader, lookup=False):
            with session(reader, lookup) as db:
                execute = db.execute

                def explain(statement, parameters=None):
                    if marker in str(statement):
                        # A table this small is read whole otherwise.
                        execute(sa.text("SET LOCAL enable_seqscan = off"))
                        explained = execute(
                            sa.text(f"EXPLAIN {statement}"), parameters
                        ).all()
                        plans.append("\n".join(line for (line,) in explained))
                    return execute(statement, parameters)

                db.execute = explain
                yield db

        with patch.object(TimelineReader, "_session", recording):
            yield plans


class ConversationQueryTests(ConversationsFixture):
    # ---- messages as a source ------------------------------------------------

    def test_messages_are_a_source_only_where_linked(self):
        self.assertEqual(self.source.sources, ("screen", "browser", "messages"))
        self.assertEqual(self.source.messages, "m")
        unlinked = TimelineSource("other", self.source.schema, ZONE)
        self.assertEqual(unlinked.sources, ("screen", "browser"))
        with self.assertRaisesRegex(TimelineUnavailable, NO_MESSAGES):
            TimelineReader(self.database, unlinked).conversations(10, None)
        with self.assertRaisesRegex(OperationError, "Sources are screen, browser"):
            TimelineReader(self.database, unlinked).events(
                at(0), at(400), ("messages",), 10, None
            )

    def test_days_count_messages_and_the_pages_ask_for_screen_and_browser(self):
        first, last = at(0).date() - timedelta(days=1), at(400).date()
        days = self.reader.days(first, last, self.source.sources)["days"]
        self.assertEqual(
            days,
            [
                {"day": "2026-01-09", "messages": {"events": 6}},
                {"day": "2026-01-10", "messages": {"events": 21}},
            ],
        )
        # What the home page reads: no day of messages alone (the Day view
        # reads every source of the analysis, phase 2c part 2).
        self.assertEqual(self.reader.days(first, last)["days"], [])

    def test_a_message_event_names_its_conversation_and_sender(self):
        found = self.reader.events(at(0), at(400), ("messages",), 500, None)
        events = {e["payload"]["guid"]: e for e in found["events"]}
        self.assertEqual(len(events), 27)  # dated only
        self.assertNotIn("az", events)
        robin = events["g1"]
        self.assertEqual(
            {k: robin[k] for k in ("source", "kind", "title", "group", "media")},
            {
                "source": "messages",
                "kind": "message",
                "title": "chapter four invoice",
                "group": self.book,
                "media": None,
            },
        )
        self.assertEqual(
            {k: robin["payload"][k] for k in ("display_name", "sender", "from_me")},
            {"display_name": "Book Club", "sender": "Robin Sample", "from_me": False},
        )
        self.assertNotIn(ROBIN, json.dumps(robin))  # the handle stays out
        self.assertEqual(events["g2"]["payload"]["sender"], None)
        self.assertEqual(events["g2"]["payload"]["reactions"], 1)
        self.assertEqual(events["s1"]["payload"]["sender"], SAM)  # as written
        self.assertEqual(events["a08"]["payload"]["attachments"], 1)
        self.assertEqual(
            events["a08"]["media"],
            {
                "connection": "m",
                "kind": "attachments",
                "key": "p1.jpeg",
                "state": "thumbnails",
                "status": "made",
            },
        )
        self.assertEqual(len(events["a17"]["title"]), MAX_EVENT_TEXT)

    # ---- the conversation list -------------------------------------------------

    def test_conversations_newest_first_with_a_preview(self):
        listed = self.reader.conversations(50, None)
        names = [c["display_name"] for c in listed["conversations"]]
        # The one with only undated messages last; the one with none left out.
        self.assertEqual(names, [SAM, "Book Club", "Alex Example", NOBODY])
        self.assertIsNone(listed["next"])
        sam, book, alex, nobody = listed["conversations"]
        self.assertEqual(
            sam,
            {
                "conversation_id": self.sam,
                "display_name": SAM,
                "is_group": False,
                "participant_count": 1,
                "last_message_at": "2026-01-10T04:32:00-05:00",
                "message_count": 3,
                # Its last message has no text.
                "preview": None,
                "preview_from_me": False,
            },
        )
        self.assertEqual(
            (book["preview"], book["preview_from_me"], book["is_group"]),
            ("invoice paid", False, True),
        )
        self.assertEqual(alex["message_count"], 22)  # undated included
        # The last of a tie of three, by guid: n=18.
        self.assertEqual(alex["preview"], "note 18")
        self.assertTrue(alex["preview_from_me"])
        self.assertEqual(
            (nobody["last_message_at"], nobody["preview"], nobody["message_count"]),
            (None, None, 1),
        )

    def test_the_list_pages_by_keyset_to_its_end(self):
        everything = [
            c["conversation_id"]
            for c in self.reader.conversations(50, None)["conversations"]
        ]
        for limit in (1, 2, 3):
            with self.subTest(limit=limit):
                seen, after = [], None
                while True:
                    found = self.client().get(
                        "/api/timeline/conversations",
                        params={"limit": limit, **({"after": after} if after else {})},
                        headers=BEARER,
                    )
                    self.assertEqual(found.status_code, 200, found.text)
                    body = found.json()
                    self.assertLessEqual(len(body["conversations"]), limit)
                    seen += [c["conversation_id"] for c in body["conversations"]]
                    after = body["next"]
                    if after is None:
                        break
                self.assertEqual(seen, everything)

    def test_a_preview_is_the_first_200_characters(self):
        self.query(
            f"update {self.schema_name}.messages set text = 'x' || repeat('é', 300) "
            "where guid = 's3'"
        )
        self.addCleanup(
            self.query,
            f"update {self.schema_name}.messages set text = null where guid = 's3'",
        )
        sam = self.reader.conversations(1, None)["conversations"][0]
        self.assertEqual(sam["preview"], "x" + "é" * 199)

    # ---- one conversation ------------------------------------------------------

    def test_a_conversation_has_its_people_and_its_undated_count(self):
        book = self.reader.conversation(self.book)
        self.assertEqual(book["title"], "Book Club")
        self.assertEqual(book["first_message_at"], "2026-01-10T02:50:00-05:00")
        self.assertEqual(
            book["participants"],
            [
                {"handle": "5555550101", "display_name": "Alex Example"},
                {"handle": "5555550103", "display_name": "Kai"},
                {"handle": "5555550102", "display_name": "Robin Sample"},
            ],
        )
        self.assertEqual(book["undated"], 0)
        alex = self.reader.conversation(self.alex)
        self.assertEqual((alex["undated"], alex["title"]), (1, None))
        self.assertEqual(alex["analysis"], "timeline")
        self.assertEqual(alex["timezone"], ZONE)
        self.assertIsNone(self.reader.conversation("0" * 32))

    # ---- its messages ----------------------------------------------------------

    def test_a_message_carries_its_sender_files_and_reactions(self):
        messages = {
            m["guid"]: m
            for m in self.reader.conversation_messages(self.alex, 200)["messages"]
        }
        loved = messages["a19"]
        self.assertEqual(
            loved,
            {
                "guid": "a19",
                "sent_at": "2026-01-09T23:50:00-05:00",
                "day": "2026-01-09",
                "from_me": False,
                "sender": {"handle": "5555550101", "display_name": "Alex Example"},
                "service": "iMessage",
                "text": "note 5",
                "text_truncated": False,
                "text_unreadable": False,
                "edited": False,
                "edited_at": None,
                "unsent": False,
                "audio": False,
                "reply_to": None,
                "attachments": [],
                "reactions": [
                    {
                        "kind": "love",
                        "from_me": False,
                        "sender": "Alex Example",
                        "part": 0,
                        "at": "2026-01-10T00:51:00-05:00",
                    },
                    {
                        "kind": "like",
                        "from_me": True,
                        "sender": None,
                        "part": 0,
                        "at": "2026-01-10T00:52:00-05:00",
                    },
                ],
            },
        )
        self.assertIsNone(messages["a00"]["sender"])  # mine
        self.assertEqual(messages["a06"]["reply_to"], "a19")
        self.assertTrue(messages["a14"]["edited"])
        self.assertEqual(messages["a14"]["edited_at"], "2026-01-10T00:11:00-05:00")
        self.assertTrue(messages["a01"]["unsent"])
        self.assertTrue(messages["a09"]["audio"])
        self.assertEqual(len(messages["a17"]["text"]), MAX_TEXT)
        self.assertTrue(messages["a17"]["text_truncated"])
        states = {
            guid: [a["media"] and a["media"]["state"] for a in m["attachments"]]
            for guid, m in messages.items()
            if m["attachments"]
        }
        self.assertEqual(
            states,
            {
                "a08": ["thumbnails"],
                "a16": ["stored"],
                "a03": ["unknown"],
                "a11": [None],  # no key: no file
            },
        )
        self.assertEqual(
            messages["a16"]["attachments"][0],
            {
                "guid": "p2",
                "kind": "image",
                "mime_type": "image/png",
                "bytes": None,
                "transfer_name": None,
                "media": {
                    "connection": "m",
                    "kind": "attachments",
                    "key": "p2.png",
                    "state": "stored",
                    "status": "failed",
                },
            },
        )
        self.assertNotIn("az", messages)  # undated

    def test_paging_both_ways_covers_a_conversation_without_gap_or_repeat(self):
        expected = self.dated(self.alex)
        self.assertEqual(len(expected), 21)
        client = self.client()
        for limit in (1, 2, 3, 4, 5, 7, 20, 21, 200):
            with self.subTest(limit=limit, way="back"):
                body = self.page(client, self.alex, limit=limit)
                self.assertIsNone(body["after"])
                seen = [m["guid"] for m in body["messages"]]
                while body["before"]:
                    body = self.page(
                        client, self.alex, limit=limit, before=body["before"]
                    )
                    self.assertIsNotNone(body["after"])
                    seen = [m["guid"] for m in body["messages"]] + seen
                self.assertEqual(seen, expected)
            with self.subTest(limit=limit, way="forward"):
                body = self.page(client, self.alex, limit=limit, at="2026-01-01")
                self.assertIsNone(body["before"])
                seen = [m["guid"] for m in body["messages"]]
                while body["after"]:
                    body = self.page(
                        client, self.alex, limit=limit, after=body["after"]
                    )
                    self.assertIsNotNone(body["before"])
                    seen += [m["guid"] for m in body["messages"]]
                self.assertEqual(seen, expected)

    def test_around_a_message_puts_it_in_the_middle_and_pages_on(self):
        expected = self.dated(self.alex)
        client = self.client()
        for limit in (1, 4, 5):
            for position, guid in enumerate(expected):
                with self.subTest(limit=limit, guid=guid):
                    body = self.page(client, self.alex, limit=limit, around=guid)
                    page = [m["guid"] for m in body["messages"]]
                    self.assertEqual(len(page), limit)
                    start = expected.index(page[0])
                    self.assertEqual(page, expected[start : start + limit])
                    # In the middle, unless an end is nearer.
                    middle = min(max(position - (limit - 1) // 2, 0), 21 - limit)
                    self.assertEqual(start, middle)
                    seen = list(page)
                    older, newer = body["before"], body["after"]
                    while older:
                        back = self.page(client, self.alex, limit=limit, before=older)
                        seen = [m["guid"] for m in back["messages"]] + seen
                        older = back["before"]
                    while newer:
                        on = self.page(client, self.alex, limit=limit, after=newer)
                        seen += [m["guid"] for m in on["messages"]]
                        newer = on["after"]
                    self.assertEqual(seen, expected)

    def test_at_a_day_or_a_time_starts_the_page_there(self):
        expected = self.dated(self.alex)
        client = self.client()
        # Local midnight of the 10th: past the six of the 9th.
        body = self.page(client, self.alex, limit=4, at="2026-01-10")
        self.assertEqual([m["guid"] for m in body["messages"]], expected[6:10])
        self.assertEqual(body["messages"][0]["day"], "2026-01-10")
        back = self.page(client, self.alex, limit=10, before=body["before"])
        self.assertEqual([m["guid"] for m in back["messages"]], expected[:6])
        # The time of a tie starts at its first message.
        body = self.page(client, self.alex, limit=2, at="2026-01-09T23:50:00-05:00")
        self.assertEqual([m["guid"] for m in body["messages"]], expected[3:5])
        # After the last message: an empty page, whose before is the last page.
        late = self.page(client, self.alex, limit=5, at="2026-02-01T00:00:00+00:00")
        self.assertEqual((late["messages"], late["after"]), ([], None))
        last = self.page(client, self.alex, limit=5, before=late["before"])
        self.assertEqual([m["guid"] for m in last["messages"]], expected[-5:])
        self.assertIsNone(last["after"])

    def test_empty_pages_at_either_end_point_back(self):
        expected = self.dated(self.alex)
        client = self.client()
        # Past either end: nothing, and a cursor back to the end it passed.
        older = self.reader.conversation_messages(self.alex, 3, before=(at(-60), "a"))
        self.assertEqual((older["messages"], older["before"]), ([], None))
        again = self.page(client, self.alex, limit=3, after=older["after"])
        self.assertEqual([m["guid"] for m in again["messages"]], expected[:3])
        newer = self.reader.conversation_messages(self.alex, 3, after=(at(999), "a"))
        self.assertEqual((newer["messages"], newer["after"]), ([], None))
        again = self.page(client, self.alex, limit=3, before=newer["before"])
        self.assertEqual([m["guid"] for m in again["messages"]], expected[-3:])

    def test_deleted_and_undated_messages_are_not_paged_or_found(self):
        # Kept with its conversation and time, to see the condition alone work.
        self.query(
            f"update {self.schema_name}.messages set deleted = true where guid = 'a18'"
        )
        self.addCleanup(
            self.query,
            f"update {self.schema_name}.messages set deleted = false "
            "where guid = 'a18'",
        )
        self.assertNotIn("a18", self.dated(self.alex))
        guids = [
            m["guid"]
            for m in self.reader.conversation_messages(self.alex, 200)["messages"]
        ]
        self.assertEqual(guids, self.dated(self.alex))
        self.assertNotIn("az", guids)
        self.assertIsNone(self.reader.conversation_messages(self.alex, 5, around="a18"))
        self.assertIsNone(self.reader.conversation_messages(self.alex, 5, around="az"))
        # n=18 (a18) was the last: the preview is the one before it.
        alex = self.reader.conversation(self.alex)
        self.assertEqual(alex["preview"], "note 20")
        found = self.reader.search_messages("note", self.alex, None, None, 50, None)
        self.assertNotIn("a18", [m["guid"] for m in found["messages"]])
        self.assertEqual(
            self.reader.search_messages("date", None, None, None, 50, None)["messages"],
            [],
        )

    def test_the_paging_uses_the_conversations_index(self):
        self.query(f"analyze {self.schema_name}.messages")
        with self.explained("LIMIT :count") as plans:
            self.reader.conversation_messages(self.alex, 5, around="a19")
        self.assertTrue(plans)
        for plan in plans:
            self.assertIn(
                f"using {self.index('(conversation_id, sent_at, guid)')}", plan
            )
            self.assertNotIn("Sort", plan)

    # ---- search ----------------------------------------------------------------

    def test_message_search_is_newest_first_and_filters(self):
        found = self.reader.search_messages("invoice", None, None, None, 50, None)
        self.assertEqual(
            [m["guid"] for m in found["messages"]], ["s1", "g3", "g1", "a04"]
        )
        self.assertEqual(found["kind"], "messages")
        self.assertIsNone(found["next"])
        self.assertEqual(
            found["messages"][1],
            {
                "guid": "g3",
                "conversation_id": self.book,
                "display_name": "Book Club",
                "sent_at": "2026-01-10T02:52:00-05:00",
                "day": "2026-01-10",
                "from_me": False,
                "sender": "Kai",
                "snippet": ["", "invoice", " paid"],
            },
        )
        alex = self.reader.search_messages("invoice", self.alex, None, None, 50, None)
        self.assertEqual([m["guid"] for m in alex["messages"]], ["a04"])
        self.assertFalse(alex["messages"][0]["from_me"])
        self.assertEqual(alex["messages"][0]["sender"], "Alex Example")
        day = date(2026, 1, 9)
        before = self.reader.search_messages("invoice", None, day, day, 50, None)
        self.assertEqual(before["messages"], [])  # 23:30 on the 9th has none
        later = self.reader.search_messages(
            "invoice", None, day + timedelta(days=1), None, 50, None
        )
        self.assertEqual(len(later["messages"]), 4)

    def test_message_search_pages_newest_first(self):
        client = self.client()
        seen, before = [], None
        while True:
            body = {"q": "note", "kind": "messages", "limit": 4}
            if before:
                body["before"] = before
            found = client.post("/api/timeline/search", json=body, headers=BEARER)
            self.assertEqual(found.status_code, 200, found.text)
            seen += [m["guid"] for m in found.json()["messages"]]
            before = found.json()["next"]
            if before is None:
                break
        dated = self.dated(self.alex)
        notes = [g for g in dated if g not in ("a17", "a04", "a01")]
        self.assertEqual(seen, notes[::-1])

    def test_message_search_reads_the_text_index(self):
        self.query(f"analyze {self.schema_name}.messages")
        for conversation in (None, self.alex):
            with self.subTest(conversation=conversation):
                with self.explained(
                    "websearch_to_tsquery('simple', :q) AS query"
                ) as plans:
                    self.reader.search_messages(
                        "invoice", conversation, None, None, 20, None
                    )
                (plan,) = plans
                text = self.index("gin (to_tsvector")
                self.assertIn(f"Bitmap Index Scan on {text}", plan)
                self.assertNotIn(self.index("(conversation_id, sent_at, guid)"), plan)
                self.assertNotIn("Seq Scan", plan)


class ConversationRouteTests(ConversationsFixture):
    ROUTES = (
        "/api/timeline/conversations",
        "/api/timeline/conversations/{alex}",
        "/api/timeline/conversations/{alex}/messages",
        "/api/timeline/events?from=2026-01-09&to=2026-01-10&source=messages",
        ("/api/timeline/search", {"q": "invoice", "kind": "messages"}),
    )

    def routes(self):
        return [
            (r[0], r[1]) if isinstance(r, tuple) else r.format(alex=self.alex)
            for r in self.ROUTES
        ]

    def call(self, client, route, **options):
        if isinstance(route, tuple):
            return client.post(route[0], json=route[1], **options)
        return client.get(route, **options)

    def test_every_route_needs_the_login_and_is_not_stored(self):
        client = self.client()
        for route in self.routes():
            with self.subTest(route=route):
                self.assertEqual(self.call(client, route).status_code, 401)
                response = self.call(client, route, headers=BEARER)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.headers["content-type"], "application/json")
                self.assertEqual(response.headers["cache-control"], "private, no-store")
                body = response.json()
                self.assertEqual(
                    (body["analysis"], body["timezone"]), ("timeline", ZONE)
                )

    def test_large_answers_are_compressed(self):
        response = self.client().get(
            f"/api/timeline/conversations/{self.alex}/messages",
            headers={**BEARER, "Accept-Encoding": "gzip"},
        )
        self.assertEqual(response.headers["content-encoding"], "gzip")
        small = self.client().get(
            f"/api/timeline/conversations/{self.alex}/messages?limit=1&around=a00",
            headers={**BEARER, "Accept-Encoding": "gzip"},
        )
        self.assertNotIn("content-encoding", small.headers)
        self.assertLess(len(small.content), 1024)

    def test_a_conversation_id_that_is_not_one_is_404_before_any_query(self):
        client = self.client()
        with patch.object(
            TimelineReader, "_session", side_effect=AssertionError("queried")
        ) as session:
            for value in ("A" * 32, "0" * 31, "0" * 33, "g" * 32, "+15555550101"):
                for suffix in ("", "/messages"):
                    with self.subTest(value=value, suffix=suffix):
                        response = client.get(
                            f"/api/timeline/conversations/{value}{suffix}",
                            headers=BEARER,
                        )
                        self.assertEqual(response.status_code, 404)
                        self.assertEqual(
                            response.json(), {"error": "Unknown conversation"}
                        )
                        self.assertEqual(
                            response.headers["cache-control"], "private, no-store"
                        )
            self.assertFalse(session.called)
        for suffix in ("", "/messages"):
            with self.subTest(suffix=suffix, known=False):
                response = client.get(
                    f"/api/timeline/conversations/{'0' * 32}{suffix}", headers=BEARER
                )
                self.assertEqual(response.status_code, 404)
        response = client.get(
            f"/api/timeline/conversations/{self.alex}/messages?around=s1",
            headers=BEARER,
        )
        self.assertEqual(
            (response.status_code, response.json()["error"]),
            (404, "Unknown message in this conversation"),
        )

    def test_parameters_are_validated(self):
        client = self.client()
        messages = f"/api/timeline/conversations/{self.alex}/messages"
        cursor = self.page(client, self.alex, limit=1)["before"]
        for route, expected in (
            ("/api/timeline/conversations?limit=0", "limit"),
            ("/api/timeline/conversations?limit=201", "limit"),
            ("/api/timeline/conversations?after=nonsense", "cursor"),
            ("/api/timeline/conversations?limit=1&limit=2", "more than once"),
            ("/api/timeline/conversations?q=x", "Unknown parameter"),
            (f"/api/timeline/conversations/{self.alex}?limit=1", "Unknown parameter"),
            (f"{messages}?limit=0", "limit"),
            (f"{messages}?limit=201", "limit"),
            (f"{messages}?before={cursor}&after={cursor}", "at most one"),
            (f"{messages}?around=a00&at=2026-01-10", "at most one"),
            (f"{messages}?at=yesterday", "a date"),
            (f"{messages}?at=2026-01-10T00:00:00", "offset"),
            (f"{messages}?around=a%00b", "control characters"),
            (f"{messages}?around={'a' * 256}", "guid"),
            (f"{messages}?before=bm90IGEgY3Vyc29y", "cursor"),
            # The last representable times: refused, not an overflow (500).
            (
                f"{messages}?after={_cursor(['9999-12-31T23:59:59.999999+00:00', 'x'])}",
                "cursor",
            ),
            (
                f"{messages}?after={_cursor(['9999-12-31T23:00:00-05:00', 'x'])}",
                "cursor",
            ),
            (f"{messages}?q=invoice", "Unknown parameter"),
            (
                "/api/timeline/events?from=2026-01-09&to=2026-01-10&source=chat",
                "source",
            ),
        ):
            with self.subTest(route=route):
                response = client.get(route, headers=BEARER)
                self.assertEqual(response.status_code, 400, response.text)
                self.assertIn(expected, response.json()["error"])
        for body, expected in (
            ({"q": "invoice", "kind": "messages", "app": "x"}, "app"),
            (
                {"q": "invoice", "kind": "messages", "conversation": "x"},
                "conversation id",
            ),
            ({"q": "invoice", "conversation": self.alex}, "message results only"),
            ({"q": "invoice", "kind": "browser", "conversation": self.alex}, "only"),
            ({"q": "invoice", "kind": "messages", "before": "x"}, "cursor"),
            ({"q": "invoice", "kind": "chat"}, "screen, browser or messages"),
            ({"q": "-invoice", "kind": "messages"}, "at least one word"),
            ({"q": "invoice", "kind": "messages", "limit": 51}, "limit"),
        ):
            with self.subTest(body=body):
                response = client.post(
                    "/api/timeline/search", json=body, headers=BEARER
                )
                self.assertEqual(response.status_code, 400, response.text)
                self.assertIn(expected, response.json()["error"])

    def test_events_default_to_screen_and_browser(self):
        client = self.client()
        default = client.get(
            "/api/timeline/events?from=2026-01-09&to=2026-01-10", headers=BEARER
        ).json()
        self.assertEqual(default["events"], [])
        messages = client.get(
            "/api/timeline/events?from=2026-01-09&to=2026-01-10&source=messages"
            "&limit=5",
            headers=BEARER,
        ).json()
        self.assertEqual(len(messages["events"]), 5)
        rest = client.get(
            "/api/timeline/events?from=2026-01-09&to=2026-01-10&source=messages"
            f"&limit=500&after={messages['next']}",
            headers=BEARER,
        ).json()
        self.assertEqual(len(messages["events"]) + len(rest["events"]), 27)
        days = client.get(
            "/api/timeline/days?from=2026-01-09&to=2026-01-10", headers=BEARER
        ).json()
        self.assertEqual(
            days["days"][1], {"day": "2026-01-10", "messages": {"events": 21}}
        )

    def test_without_messages_or_tables_the_routes_say_why(self):
        without = raw_configuration({"browser": "b"})
        client = self.client(without)
        for route in self.routes()[:3] + [self.routes()[4]]:
            with self.subTest(route=route):
                response = self.call(client, route, headers=BEARER)
                self.assertEqual(response.status_code, 503, response.text)
                self.assertEqual(response.json(), {"error": NO_MESSAGES})
        events = self.call(client, self.routes()[3], headers=BEARER)
        self.assertEqual(events.status_code, 400)
        unconfigured = self.client(database=None)
        for route in self.routes():
            with self.subTest(route=route, database=None):
                self.assertEqual(
                    self.call(unconfigured, route, headers=BEARER).status_code, 503
                )
        missing = TimelineReader(
            self.database, TimelineSource("other", "miroir_not_built", ZONE, (), "m")
        )
        for call in (
            lambda: missing.conversations(10, None),
            lambda: missing.conversation(self.alex),
            lambda: missing.conversation_messages(self.alex, 10),
            lambda: missing.search_messages("invoice", None, None, None, 10, None),
        ):
            with self.assertRaisesRegex(TimelineUnavailable, NOT_BUILT):
                call()

    def test_the_time_limit_answers_504(self):
        client = self.client()
        with patch.object(
            TimelineReader,
            "conversation_messages",
            side_effect=TimelineTimeout("Too slow"),
        ):
            response = client.get(
                f"/api/timeline/conversations/{self.alex}/messages", headers=BEARER
            )
        self.assertEqual(
            (response.status_code, response.json()), (504, {"error": "Too slow"})
        )

    def test_an_attachment_is_served_as_a_screenshot_is(self):
        message = next(
            m
            for m in self.page(self.client(), self.alex, limit=200)["messages"]
            if m["guid"] == "a08"
        )
        media = message["attachments"][0]["media"]
        self.objects.add(f"m/attachments/{media['key']}", b"jpeg bytes")
        client = self.client()
        url = f"/media/{media['connection']}/{media['kind']}/{media['key']}"
        link = client.get(url, headers=BEARER, follow_redirects=False)
        self.assertEqual(link.status_code, 302)
        original = client.get(f"{url}?size=original", headers=BEARER)
        self.assertEqual((original.status_code, original.content), (200, b"jpeg bytes"))

    def test_text_is_data(self):
        self.query(
            f"update {self.schema_name}.messages "
            "set text = '<b>invoice</b> & <script>x</script>' where guid = 'g2'"
        )
        self.addCleanup(
            self.query,
            f"update {self.schema_name}.messages set text = 'done' where guid = 'g2'",
        )
        page = self.page(self.client(), self.book)
        self.assertEqual(
            page["messages"][1]["text"], "<b>invoice</b> & <script>x</script>"
        )
        raw = self.client().get(
            f"/api/timeline/conversations/{self.book}/messages", headers=BEARER
        )
        self.assertIn("<b>invoice</b>", raw.text)  # JSON, not escaped markup


def _cursor(values: list[str]) -> str:
    """A paging cursor in the server's own encoding, for crafted values."""
    raw = json.dumps(values).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")
