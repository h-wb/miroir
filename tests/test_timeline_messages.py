"""The timeline's Apple Messages data (decision 040, phase 2a), on a disposable
Postgres (MIROIR_TEST_POSTGRES_BIN, see tests/postgres.py).

Every row is invented: the messages package's fixture
(tests/test_messages_models.py) and rows made up here, with example.com
addresses and 555 numbers.
"""

import hashlib
import json
import os
import random
import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import mkdtemp

import psycopg2.errors

from src.analyses.catalog import PACKAGES
from src.core.analysis import analysis_plan
from src.core.artifacts import submit_artifact
from src.core.config import parse_miroir
from src.core.derive import DERIVE_VERSION, _Writer
from src.core.runner import import_artifact
from src.core.timeline import SEARCH_CHARACTERS, search_vector
from tests.test_messages_models import (
    ALEX,
    BOOK,
    DIRECT,
    FIXTURE,
    KAI,
    ROBIN,
    SAM,
    apple,
    at,
    attachment,
    chat,
    contact,
    key,
    message,
    reaction,
    row,
    write_batch,
)
from tests.test_timeline_models import (
    READER,
    SAFARI,
    TimelineBuildFixture,
    browser_batch,
    frame,
    place,
    screen,
    screen_batch,
    usage,
    visit,
)

MIROIR = "timeline_messages_tests"
ZONE = "America/Toronto"
OUTPUTS = {
    "screen_frames",
    "events",
    "days",
    "apps",
    "conversations",
    "messages",
    "people",
}
# The timeline's own message columns, without the build's bookkeeping.
MESSAGE_COLUMNS = (
    "guid, conversation_id, sent_at, day, timezone, is_from_me, sender_handle, "
    "sender_written, service, text, text_unreadable, is_edited, is_unsent, "
    "edited_at, is_audio, reply_to_guid, attachments, reactions, deleted, connection"
)


def configuration(analyses, connections=("m",), others=(), miroir_id=MIROIR):
    return parse_miroir(
        json.dumps(
            {
                "schema_version": 1,
                "id": miroir_id,
                "connections": [
                    *(
                        {"id": c, "integration": "apple_messages", "settings": {}}
                        for c in connections
                    ),
                    *(
                        {"id": c, "integration": integration, "settings": {}}
                        for c, integration in others
                    ),
                ],
                "analyses": analyses,
            }
        )
    )


def timeline(analysis_id, inputs=None, reader_role=None, zone=ZONE):
    return {
        "id": analysis_id,
        "package": "timeline",
        "inputs": inputs or {"messages": "m"},
        "timezone": zone,
        "parameters": {"reader_role": reader_role},
    }


def messages_package(analysis_id, connection="m"):
    return {
        "id": analysis_id,
        "package": "messages",
        "inputs": {"messages": connection},
        "timezone": ZONE,
    }


def tapback(guid, minutes, chat_guid, code, target, **sender):
    return reaction(guid, minutes, chat_guid, code, target, **sender)


class TimelineMessagesFixture(TimelineBuildFixture):
    def configure(self, analyses, **options):
        """A configuration of this test's own, so its raw dataset is its own."""
        name = self._testMethodName.removeprefix("test_")[:20].rstrip("_")
        return configuration(analyses, miroir_id=f"tm_{name}", **options)

    def deliver(self, miroir, records, connection_id="m"):
        """One Apple Messages batch, loaded and built as a delivery is."""
        path = write_batch(Path(mkdtemp(dir=self.root)) / "batch.jsonl.gz", records)
        self.send(miroir, connection_id, path.read_bytes())

    def load(self, miroir, records):
        """Load a batch without building, as several loads before one build."""
        path = write_batch(Path(mkdtemp(dir=self.root)) / "batch.jsonl.gz", records)
        connection = next(c for c in miroir.connections if c.config.id == "m")
        artifact, _ = submit_artifact(
            miroir, connection, path, store=self.store, root=self.root / "artifacts"
        )
        outcome = import_artifact(
            miroir,
            "m",
            artifact["id"],
            store=self.store,
            origin="local",
            **{**self.execution, "transform": lambda plan: None},
        )
        self.assertEqual(outcome.ingestion, "succeeded")

    def relation(self, miroir, analysis_id, name):
        analysis = next(a for a in miroir.analyses if a.id == analysis_id)
        plan = analysis_plan(analysis)
        return f"{plan.relations[0].split('.')[0]}.{name}"

    def keys(self, schema):
        """Conversation id to conversation key, read as the owner."""
        return dict(
            self.query(
                "select distinct conversation_id, conversation_key "
                f"from {schema}.timeline_message_chats"
            )
        )

    def snapshot(self, schema):
        """Every message table, with conversation ids written as their keys so
        two analyses (two keys of their own) compare."""
        ids = self.keys(schema)

        def keyed(value):
            return ids.get(value, value)

        messages = [
            (r[0], keyed(r[1]), *r[2:])
            for r in self.query(
                f"select {MESSAGE_COLUMNS} from {schema}.messages order by guid"
            )
        ]
        conversations = sorted(
            (keyed(r[0]), *r[1:])
            for r in self.query(f"select * from {schema}.conversations")
        )
        events = [
            (
                *r[:9],
                keyed(r[9]),
                {**r[10], "conversation": keyed(r[10]["conversation"])},
                r[11],
            )
            for r in self.query(
                "select source, connection, event_id, started_at, ended_at, kind, "
                "title, body, media_key, group_key, payload, search::text "
                f"from {schema}.events where source = 'messages' order by event_id"
            )
        ]
        return {
            "chats": self.query(
                "select guid, identifier, conversation_key, is_group, title, service, "
                f"members, connection from {schema}.timeline_message_chats order by guid"
            ),
            "messages": messages,
            "conversations": conversations,
            "people": self.query(f"select * from {schema}.people order by handle"),
            "events": events,
            "days": self.query(
                "select source, day, events, frames, timezone "
                f"from {schema}.days where source = 'messages' order by day"
            ),
        }

    def media_record(self, miroir, media_key, status="made", connection_id="m"):
        """One media_objects row for an attachment, as the derive worker writes it."""
        connection = next(c for c in miroir.connections if c.config.id == "m")
        writer = _Writer(
            connection,
            self.execution["destination"],
            self.execution["pipelines_dir"],
            self.store,
            5,
        )
        now = datetime.now(timezone.utc)
        made = status == "made"
        writer.write(
            [
                {
                    "connection_id": connection_id,
                    "kind": "attachments",
                    "key": media_key,
                    "size": 1000,
                    "content_type": "image/jpeg",
                    "stored_at": now,
                    "thumbnail_w360": made,
                    "thumbnail_w1280": made,
                    "thumbnail_status": status,
                    "derived_at": now,
                    "derive_version": DERIVE_VERSION,
                }
            ]
        )


class TimelineMessagesPackageTests(unittest.TestCase):
    def test_messages_is_a_third_optional_input_with_three_readable_tables(self):
        package = PACKAGES["timeline"]
        self.assertEqual(package.inputs["messages"].integrations, ("apple_messages",))
        self.assertFalse(package.inputs["messages"].required)
        self.assertEqual(set(package.relations), OUTPUTS)
        self.assertEqual(package.relations[0], "screen_frames")
        others = (("s", "screenmemory"), ("b", "browser_history"))
        for inputs in (
            {"messages": "m"},
            {"screen": "s", "messages": "m"},
            {"browser": "b", "messages": "m"},
            {"screen": "s", "browser": "b", "messages": "m"},
        ):
            with self.subTest(inputs=inputs):
                miroir = configuration([timeline("t", inputs)], others=others)
                variables = analysis_plan(miroir.analyses[0]).variables
                self.assertEqual(variables["miroir_connections"]["messages"], "m")


class TimelineMessagesTests(TimelineMessagesFixture):
    def test_both_packages_agree_on_conversations_messages_and_reactions(self):
        miroir = self.configure([timeline("t"), messages_package("p")])
        self.deliver(miroir, FIXTURE)
        schema = self.schema(miroir, "t")
        package = self.schema(miroir, "p")
        ids = self.keys(schema)
        self.assertEqual(
            set(ids.values()),
            {
                k
                for (k,) in self.query(
                    f"select conversation_key from {package}.conversations"
                )
            },
        )
        # Each message in the same conversation, the same person, the same flags.
        self.assertEqual(
            [
                (r[0], ids.get(r[1]), *r[2:])
                for r in self.query(
                    "select guid, conversation_id, sent_at, day, is_from_me, "
                    "sender_handle, is_edited, is_unsent, is_audio, "
                    "reply_to_guid is not null, jsonb_array_length(attachments) "
                    f"from {schema}.messages order by guid"
                )
            ],
            self.query(
                "select guid, conversation_key, sent_at, day, is_from_me, "
                "sender_handle, is_edited, is_unsent, is_audio, is_thread_reply, "
                f"attachments from {package}.messages order by guid"
            ),
        )
        self.assertEqual(
            self.query(
                # A message's sender: the contact's name, else the handle as
                # that message writes it.
                f"select m.guid, coalesce(n.name, m.sender_written) from {schema}.messages m "
                f"join {schema}.people n on n.handle = m.sender_handle order by 1"
            ),
            self.query(
                f"select guid, sender_name from {package}.messages "
                "where sender_handle is not null order by 1"
            ),
        )
        # The same reactions stand.
        self.assertEqual(
            sorted(
                self.query(
                    "select m.guid, r->>'kind', (r->>'from_me')::boolean, r->>'sender', "
                    f"(r->>'part')::int from {schema}.messages m, "
                    "jsonb_array_elements(m.reactions) r"
                )
            ),
            sorted(
                self.query(
                    "select target_guid, kind, is_from_me, sender_handle, target_part "
                    f"from {package}.reactions"
                )
            ),
        )
        self.assertEqual(
            sorted(
                self.query(
                    "select m.guid, a->>'guid', a->>'kind' from "
                    f"{schema}.messages m, jsonb_array_elements(m.attachments) a"
                )
            ),
            sorted(
                self.query(
                    "select message_guid, attachment_guid, kind "
                    f"from {package}.attachments"
                )
            ),
        )
        # Conversations: the same shape, people, totals and names.
        self.assertEqual(
            sorted(
                (ids[r[0]], *r[1:])
                for r in self.query(
                    "select conversation_id, display_name, is_group, "
                    "participant_count, first_message_at, last_message_at, "
                    f"message_count from {schema}.conversations"
                )
            ),
            self.query(
                "select conversation_key, display_name, is_group, participants, "
                "first_message_at, last_message_at, messages_sent + messages_received "
                f"from {package}.conversations order by 1"
            ),
        )
        self.assertEqual(
            sorted(
                (ids[c], h)
                for c, handles in self.query(
                    f"select conversation_id, participants from {schema}.conversations"
                )
                for h in handles
            ),
            self.query(
                f"select conversation_key, handle from {package}.participants order by 1, 2"
            ),
        )

    def test_messages_carry_text_attachments_reactions_and_opaque_ids(self):
        miroir = self.configure([timeline("t", reader_role=READER), timeline("other")])
        self.deliver(miroir, FIXTURE)
        schema = self.schema(miroir, "t")
        rows = {
            r[0]: r[1:]
            for r in self.query(
                "select guid, text, text_unreadable, is_edited, is_unsent, is_audio, "
                f"reply_to_guid, sender_handle, day::text from {schema}.messages"
            )
        }
        # The text as delivered: the attachment placeholder stays.
        self.assertEqual(rows["a1"][:2], ("hello there", False))
        self.assertEqual(rows["a2"][0], "￼")
        self.assertEqual(rows["u1"][:2], (None, True))
        self.assertEqual(rows["g3"][2:4], (True, False))
        self.assertEqual(rows["g4"][2:4], (False, True))
        self.assertEqual(rows["g1"][4], True)
        self.assertEqual(rows["g2"][5], "g1")
        self.assertEqual(rows["a1"][6:], (key(ALEX), "2026-01-09"))
        self.assertEqual(rows["a2"][6:], (None, "2026-01-10"))
        self.assertEqual(rows["z1"][7], None)

        def reactions(guid):
            return self.query(
                "select r->>'kind', (r->>'from_me')::boolean, r->>'sender', "
                f"(r->>'part')::int, r->>'at' from {schema}.messages m, "
                f"jsonb_array_elements(m.reactions) r where m.guid = '{guid}'"
            )

        def stamp(minutes):
            return (at(minutes)).strftime("%Y-%m-%dT%H:%M:%S.000000Z")

        # Removed, then added again; a removal of what was never there; changed.
        self.assertEqual(
            reactions("g2"),
            [
                ("emphasis", False, key(ROBIN), 0, stamp(412)),
                ("question", False, key(KAI), 0, stamp(413)),
            ],
        )
        self.assertEqual(reactions("a4"), [("emoji", False, key(ALEX), 1, stamp(106))])
        self.assertEqual(reactions("a1"), [("laugh", True, None, 0, stamp(105))])
        self.assertEqual(reactions("g3"), [("like", False, key(KAI), 0, stamp(431))])
        self.assertEqual(
            [r[:2] for r in reactions("g1")],
            [("sticker", True), ("sticker", False), ("sticker", False)],
        )
        # Attachments in order, link previews left out, no record: unknown.
        self.assertEqual(
            self.query(
                f"select guid, attachments from {schema}.messages "
                "where guid in ('a2', 'a4', 'v1') order by guid"
            ),
            [
                (
                    "a2",
                    [
                        {
                            "guid": "att1",
                            "media_key": "att1.jpeg",
                            "kind": "image",
                            "mime_type": "image/jpeg",
                            "bytes": 1000,
                            "transfer_name": "IMG_0001.JPG",
                            "media_status": None,
                            "thumbnail_w360": None,
                            "thumbnail_w1280": None,
                        }
                    ],
                ),
                ("a4", []),
                (
                    "v1",
                    [
                        {
                            "guid": "att6",
                            "media_key": "att6.xyz",
                            "kind": "other",
                            "mime_type": None,
                            "bytes": None,
                            "transfer_name": "thing.xyz",
                            "media_status": None,
                            "thumbnail_w360": None,
                            "thumbnail_w1280": None,
                        },
                        {
                            "guid": "att5",
                            "media_key": None,
                            "kind": "video",
                            "mime_type": "video/quicktime",
                            "bytes": None,
                            "transfer_name": None,
                            "media_status": None,
                            "thumbnail_w360": None,
                            "thumbnail_w1280": None,
                        },
                    ],
                ),
            ],
        )

        # Opaque ids: 128 bits, not a hash of the key alone, one key per analysis.
        ids = self.keys(schema)
        alex = next(i for i, k in ids.items() if k == key(ALEX))
        self.assertRegex(alex, "^[0-9a-f]{32}$")
        for guess in (key(ALEX), ALEX, "conversation:" + key(ALEX)):
            for digest in (hashlib.sha256, hashlib.md5, hashlib.sha1):
                self.assertNotIn(alex, digest(guess.encode()).hexdigest())
        self.assertNotEqual(self.keys(self.schema(miroir, "other")), ids)
        self.assertEqual(
            set(self.keys(self.schema(miroir, "other")).values()), set(ids.values())
        )
        columns = {
            table: {
                c
                for (c,) in self.query(
                    "select column_name from information_schema.columns "
                    f"where table_schema = '{schema}' and table_name = '{table}'"
                )
            }
            for table in ("conversations", "messages", "people")
        }
        self.assertNotIn(
            "conversation_key", columns["conversations"] | columns["messages"]
        )
        self.assertEqual(
            self.query(
                f"select display_name, is_group from {schema}.conversations "
                f"where conversation_id = '{alex}'"
            ),
            [("Alex Example", False)],
        )

        # The lane: one event per dated message; text searched in messages only.
        self.assertEqual(
            self.query(
                "select title, body, group_key, media_kind, media_key, "
                "payload->>'sender', (payload->>'attachments')::int, search::text "
                f"from {schema}.events where event_id = 'messages:a2'"
            ),
            [("￼", None, alex, "attachments", "att1.jpeg", None, 1, "")],
        )
        self.assertEqual(
            self.query(
                f"select count(*) from {schema}.events where source = 'messages'"
            ),
            [(27,)],  # 29 messages, two undated
        )
        self.assertEqual(
            self.query(
                f"select day::text, events, frames from {schema}.days "
                "where source = 'messages' order by day"
            ),
            [
                ("2026-01-09", 1, None),
                ("2026-01-10", 25, None),
                ("2026-01-12", 1, None),
            ],
        )
        search = (
            f"select guid, conversation_id, sent_at from {schema}.messages "
            f"where {search_vector('text')} @@ websearch_to_tsquery('simple', '{{q}}')"
        )
        self.assertEqual(
            self.query(search.format(q="chapter")),
            [
                (
                    "g1",
                    self.query(
                        f"select conversation_id from {schema}.messages where guid = 'g1'"
                    )[0][0],
                    at(400),
                )
            ],
        )
        with self.subTest("the search uses the text index"):
            plan = self.query(
                "set enable_seqscan = off; explain " + search.format(q="chapter")
            )
            self.assertTrue(any("Bitmap Index Scan" in line for (line,) in plan), plan)

    def test_incremental_builds_equal_a_rebuild_from_scratch_at_every_step(self):
        # No margin: each build reads only the loads completed since the last.
        self.assertEqual(self.margin, "0 seconds")
        miroir = self.configure([timeline("t"), timeline("fresh")])
        deliveries = self.configure([timeline("t")])
        schema, fresh = self.schema(miroir, "t"), self.schema(miroir, "fresh")
        fresh_plan = analysis_plan(next(a for a in miroir.analyses if a.id == "fresh"))
        dataset = miroir.connections[0].binding.dataset_name

        def step(records=None, note=""):
            if records is not None:
                self.deliver(deliveries, records)
            else:
                self.build(analysis_plan(deliveries.analyses[0]))
            self.build(replace(fresh_plan, full_refresh=True))
            self.assertEqual(self.snapshot(schema), self.snapshot(fresh), note)

        def row_of(guid, columns="text, is_edited, is_unsent, conversation_id"):
            found = self.query(
                f"select {columns} from {schema}.messages where guid = '{guid}'"
            )
            return found[0] if found else None

        def stamps():
            return dict(self.query(f"select guid, _stamp from {schema}.messages"))

        alex, sam = DIRECT[0], "iMessage;-;" + SAM
        step(
            [
                *chat(alex, ALEX, ALEX),
                *chat(sam, SAM, SAM),
                *chat(BOOK, "chat111", ALEX, ROBIN, style=43, title="Book Club"),
                contact(ALEX, first_name="Alex", last_name="Example"),
                *message("x1", 0, alex, handle=ALEX, text="first draft"),
                *message("x2", 5, alex, me=True, handle=ALEX, text="oops"),
                *message("x3", 10, alex, handle=ALEX, text="look"),
                *message("x4", 20, BOOK, handle=ROBIN, text="in the group"),
                *message("x5", 30, sam, handle=SAM, text="hello sam"),
            ],
            "first delivery",
        )

        # Edited and unsent: the message is sent again and replaces its row.
        step(
            [
                *message(
                    "x1", 0, alex, handle=ALEX, text="final", date_edited=apple(at(1))
                ),
                *message(
                    "x2",
                    5,
                    alex,
                    me=True,
                    handle=ALEX,
                    text="",
                    date_retracted=apple(at(6)),
                ),
            ],
            "edit and unsend",
        )
        self.assertEqual(row_of("x1")[:3], ("final", True, False))
        self.assertEqual(row_of("x2")[:3], ("", False, True))

        # A reaction, then its removal with another kind added: the change stands.
        step(
            [*tapback("rx1", 11, alex, 2000, "p:0/x3", me=True, handle=ALEX)],
            "reaction",
        )
        step(
            [
                *tapback("rx2", 12, alex, 3000, "p:0/x3", me=True, handle=ALEX),
                *tapback("rx3", 12, alex, 2001, "p:0/x3", me=True, handle=ALEX),
                *tapback("rx4", 13, alex, 3002, "p:0/x1", handle=ALEX),
            ],
            "reaction changed",
        )
        self.assertEqual(
            self.query(
                f"select r->>'kind' from {schema}.messages m, "
                "jsonb_array_elements(m.reactions) r where m.guid = 'x3'"
            ),
            [("like",)],
        )

        # A reaction sent again aimed at another message leaves the first; one
        # sent again as an ordinary message leaves its target too.
        def kinds(guid):
            return self.query(
                f"select r->>'kind' from {schema}.messages m, "
                f"jsonb_array_elements(m.reactions) r where m.guid = '{guid}'"
            )

        step(
            [*tapback("rx3", 12, alex, 2001, "p:0/x1", me=True, handle=ALEX)],
            "reaction aimed elsewhere",
        )
        self.assertEqual((kinds("x3"), kinds("x1")), ([], [("like",)]))
        step(
            [*message("rx3", 12, alex, me=True, handle=ALEX, text="now a message")],
            "reaction becomes a message",
        )
        self.assertEqual(kinds("x1"), [])

        # Messages before their chat: no conversation, then the chat's.
        late = "iMessage;-;" + KAI
        step(
            [
                *message("y1", 40, late, handle=KAI),
                *message("y2", 41, late, me=True, handle=KAI),
            ],
            "chat missing",
        )
        self.assertIsNone(row_of("y1")[3])
        step(chat(late, KAI, KAI), "chat arrives")
        self.assertIsNotNone(row_of("y1")[3])

        # A chat sent again unchanged, or with a new title, reads no message.
        settled = stamps()
        step(
            [
                *chat(alex, ALEX, ALEX),
                *chat(BOOK, "chat111", ALEX, ROBIN, style=43, title="Book Club 2"),
            ],
            "chats sent again",
        )
        self.assertEqual({g: s for g, s in stamps().items() if g in settled}, settled)
        self.assertIn(
            ("Book Club 2",),
            self.query(f"select display_name from {schema}.conversations"),
        )

        # A chat whose conversation changes: an unstyled chat that lists a second
        # handle becomes a group, keyed by its identifier.
        # An unstyled chat whose identifier is a number.
        shared, number = "any;-;+15555550177", "+15555550177"
        step(
            [
                *chat(shared, number, ROBIN, style=None),
                *message("w1", 50, shared, handle=ROBIN),
            ],
            "direct chat",
        )
        direct_id = row_of("w1")[3]
        step(
            [
                row(
                    "chat_handle",
                    chat_guid=shared,
                    handle=KAI,
                    handle_service="iMessage",
                )
            ],
            "becomes a group",
        )
        self.assertNotEqual(row_of("w1")[3], direct_id)
        self.assertEqual(
            self.query(
                f"select is_group from {schema}.conversations "
                f"where conversation_id = '{row_of('w1')[3]}'"
            ),
            [(True,)],
        )

        # Attachments: the link before the file's metadata, then the metadata;
        # a link arriving after its message; a media record, then another.
        step(
            [
                *message("v1", 60, alex, me=True, handle=ALEX, text="￼￼"),
                row("message_attachment", message_guid="v1", attachment_guid="f1"),
            ],
            "link without metadata",
        )
        self.assertEqual(row_of("v1", "attachments"), ([],))
        step(
            [
                row(
                    "attachment",
                    guid="f1",
                    media_key="f1.jpeg",
                    mime_type="image/jpeg",
                    created_date=apple(at(60)) // 10**9,
                ),
                row(
                    "attachment",
                    guid="f2",
                    media_key="f2.png",
                    mime_type="image/png",
                    created_date=apple(at(61)) // 10**9,
                ),
            ],
            "metadata arrives",
        )
        step(
            [row("message_attachment", message_guid="v1", attachment_guid="f2")],
            "second link",
        )
        self.assertEqual(
            row_of("v1", "jsonb_path_query_array(attachments, '$[*].guid')"),
            (["f1", "f2"],),
        )
        self.media_record(deliveries, "f2.png", status="failed")
        self.media_record(deliveries, "f9.png")  # no attachment has this key
        self.media_record(deliveries, "f1.jpeg", connection_id="another")  # not ours
        step(None, "a media record")
        self.media_record(deliveries, "f2.png")
        step(None, "a replaced media record")
        self.assertEqual(
            row_of("v1", "jsonb_path_query_array(attachments, '$[*].media_status')"),
            ([None, "made"],),
        )
        self.assertEqual(
            self.query(
                f"select media_key, media_status, thumbnail_w360 from {schema}.events "
                "where event_id = 'messages:v1'"
            ),
            [("f1.jpeg", None, None)],
        )

        # A renamed contact renames old messages' sender without rewriting them.
        settled = stamps()
        step([contact(ALEX, first_name="Alexandra", last_name="Example")], "renamed")
        self.assertEqual(stamps(), settled)
        self.assertEqual(
            self.query(
                f"select display_name from {schema}.people where handle = '{key(ALEX)}'"
            ),
            [("Alexandra Example",)],
        )
        self.assertIn(
            ("Alexandra Example",),
            self.query(f"select display_name from {schema}.conversations"),
        )

        # A message that moves to another day leaves its old day; one sent again
        # as something else leaves the tables.
        step(
            [*message("x5", 30 + 3 * 24 * 60, sam, handle=SAM, text="hello sam")],
            "moved",
        )
        step(
            [*tapback("x4", 21, BOOK, 2000, "p:0/x3", handle=ROBIN)],
            "no longer a message",
        )
        self.assertIsNone(row_of("x4"))
        self.assertEqual(
            self.query(
                f"select count(*) from {schema}.events where event_id = 'messages:x4'"
            ),
            [(0,)],
        )

        # A load that has not completed is not read; when it completes, it is.
        pending = "1000000000.000001"
        self.query(
            f"insert into {dataset}.messages (guid, text, text_source, masked, "
            "is_from_me, handle, sent_at, item_type, associated_message_type, "
            f"_dlt_load_id, _dlt_id) values ('p1', 'pending', 'text', 0, false, "
            f"'{SAM}', '{at(70).isoformat()}', 0, 0, '{pending}', 'invented-p1')"
        )
        self.query(
            f"insert into {dataset}.chat_messages (chat_guid, message_guid, "
            f"_dlt_load_id, _dlt_id) values ('{sam}', 'p1', '{pending}', 'invented-l1')"
        )
        step(None, "pending load")
        self.assertIsNone(row_of("p1"))
        self.query(
            f"insert into {dataset}._dlt_loads (load_id, schema_name, status, "
            f"inserted_at) values ('{pending}', 'apple_messages', 0, now())"
        )
        step(None, "the load completes")
        self.assertEqual(row_of("p1")[0], "pending")

        # A new timezone gives every message the day of the new one.
        utc = self.configure([timeline("t", zone="UTC"), timeline("fresh", zone="UTC")])
        deliveries = self.configure([timeline("t", zone="UTC")])
        fresh_plan = analysis_plan(utc.analyses[1])
        step(None, "timezone")
        self.assertEqual(
            self.query(f"select distinct timezone from {schema}.messages"), [("UTC",)]
        )
        self.assertEqual(row_of("x1", "day::text"), ("2026-01-10",))

        # And a rebuild from scratch of the same analysis keeps its ids.
        ids = self.keys(schema)
        snapshot = self.snapshot(schema)
        self.build(replace(analysis_plan(utc.analyses[0]), full_refresh=True))
        self.assertEqual(self.keys(schema), ids)
        self.assertEqual(self.snapshot(schema), snapshot)

    def test_a_very_long_text_is_stored_whole_and_searched_at_its_start(self):
        # 1.36 MB of distinct words, under the batches' 2 MB line: whole, it
        # is more than a tsvector holds and failed every build after it.
        words = " ".join(f"t{n:06x}" for n in range(170_000))
        text = "needle " + words + " beyond"
        others = (("s", "screenmemory"),)
        inputs = {"screen": "s", "messages": "m"}
        miroir = self.configure([timeline("t", inputs, READER)], others=others)
        schema = self.schema(miroir, "t")
        self.send(miroir, "s", screen_batch(frame(0, text=text), usage(0, SAFARI)))
        alex = DIRECT[0]
        self.deliver(
            miroir,
            [*chat(alex, ALEX, ALEX), *message("x1", 0, alex, handle=ALEX, text=text)],
        )
        for table in ("screen_frames", "messages"):
            with self.subTest(table=table):
                self.assertEqual(
                    self.query(f"select length(text) from {schema}.{table}"),
                    [(len(text),)],
                )
                found = (
                    f"select count(*) from {schema}.{table} where "
                    f"{search_vector('text')} @@ websearch_to_tsquery('simple', '{{}}')"
                )
                self.assertEqual(self.query(found.format("needle")), [(1,)])
                self.assertEqual(self.query(found.format("beyond")), [(0,)])
                plan = self.query(
                    "set enable_seqscan = off; explain " + found.format("needle")
                )
                self.assertTrue(
                    any("Bitmap Index Scan" in line for (line,) in plan), plan
                )
        self.assertGreater(len(text), SEARCH_CHARACTERS)
        self.assertEqual(
            self.query(f"select count(*) from {schema}.events where source = 'screen'"),
            [(1,)],
        )

    def test_a_screen_table_indexed_on_the_whole_text_gets_the_bounded_index(self):
        others = (("s", "screenmemory"),)
        miroir = self.configure([timeline("t", {"screen": "s"})], others=others)
        schema = self.schema(miroir, "t")
        self.send(miroir, "s", screen_batch(*screen(0, SAFARI)))
        indexes = (
            f"select indexdef from pg_indexes where schemaname = '{schema}' "
            "and tablename = 'screen_frames' and indexdef like '%to_tsvector%'"
        )
        (bounded,) = self.query(indexes)
        # As tables built before the bound are indexed.
        self.query(
            f"drop index {schema}.{bounded[0].split()[2]}; create index "
            f"on {schema}.screen_frames using gin (to_tsvector('simple', text))"
        )
        self.send(miroir, "s", screen_batch(*screen(10, SAFARI)))
        (after,) = self.query(indexes)
        self.assertIn('"left"(text, 100000)', after[0])

    def test_a_build_that_fails_at_its_end_changes_nothing(self):
        # The raw-index hook runs outside the model's transaction and opens it
        # again (decision 040, the media upgrade): a build that fails in a
        # post-hook keeps none of its changes, and the next build makes them.
        miroir = self.configure([timeline("t", reader_role=READER)])
        schema = self.schema(miroir, "t")
        records = [
            *chat(DIRECT[0], ALEX, ALEX),
            *message("x1", 0, DIRECT[0], handle=ALEX, text="before"),
        ]
        self.deliver(miroir, records)
        snapshot = self.snapshot(schema)
        edit = message(
            "x1", 0, DIRECT[0], handle=ALEX, text="after", date_edited=apple(at(1))
        )
        self.load(miroir, [*edit, *message("x2", 5, DIRECT[0], me=True, handle=ALEX)])
        failing = self.configure([timeline("t", reader_role="no_such_reader")])
        with self.assertRaises(RuntimeError):
            self.build(analysis_plan(failing.analyses[0]))
        self.assertEqual(self.snapshot(schema), snapshot)
        self.build(analysis_plan(miroir.analyses[0]))
        self.assertEqual(
            self.query(f"select guid, text from {schema}.messages order by guid"),
            [("x1", "after"), ("x2", "hi")],
        )

    def test_the_reader_reads_the_outputs_and_nothing_else(self):
        miroir = self.configure(
            [timeline("t", reader_role=READER), messages_package("p")]
        )
        self.deliver(miroir, FIXTURE)
        schema = self.schema(miroir, "t")
        readable = {
            name
            for (name,) in self.query(
                "select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace "
                f"where n.nspname = '{schema}' and c.relkind in ('r', 'v', 'm') "
                f"and has_table_privilege('{READER}', c.oid, 'select')"
            )
        }
        self.assertEqual(readable, OUTPUTS)
        for table in ("conversations", "messages", "people"):
            self.assertTrue(self.query(f"select 1 from {schema}.{table}", READER))
        dataset = miroir.connections[0].binding.dataset_name
        for relation in (
            f"{schema}.timeline_secret",
            f"{schema}.timeline_message_chats",
            f"{dataset}.messages",
            f"{dataset}.contacts",
            f"{self.schema(miroir, 'p')}.messages",
        ):
            with (
                self.subTest(relation=relation),
                self.assertRaises(psycopg2.errors.InsufficientPrivilege),
            ):
                self.query(f"select 1 from {relation}", READER)

    def test_a_replaced_input_leaves_none_of_its_rows_and_keeps_the_ids(self):
        both = ("m", "m2")
        miroir = self.configure([timeline("t", reader_role=READER)], connections=both)
        schema = self.schema(miroir, "t")
        records = [
            *chat(DIRECT[0], ALEX, ALEX),
            *message("x1", 0, DIRECT[0], handle=ALEX, text="from m"),
        ]
        self.deliver(miroir, records)
        ids = self.keys(schema)
        replaced = self.configure(
            [timeline("t", {"messages": "m2"}, READER)], connections=both
        )
        self.deliver(
            replaced,
            [
                *chat(DIRECT[0], ALEX, ALEX),
                *message("x2", 5, DIRECT[0], me=True, handle=ALEX, text="from m2"),
            ],
            connection_id="m2",
        )
        self.assertEqual(
            self.query(f"select guid, text, connection from {schema}.messages"),
            [("x2", "from m2", "m2")],
        )
        self.assertEqual(
            self.query(f"select event_id, connection from {schema}.events order by 1"),
            [("messages:x2", "m2")],
        )
        self.assertEqual(self.keys(schema), ids)
        unlinked = self.configure(
            [timeline("t", {"screen": "s"}, READER)],
            connections=both,
            others=(("s", "screenmemory"),),
        )
        self.build(analysis_plan(unlinked.analyses[0]))
        for table in ("messages", "conversations", "people", "timeline_message_chats"):
            self.assertEqual(
                self.query(f"select count(*) from {schema}.{table}"), [(0,)]
            )
        self.assertEqual(
            self.query(
                f"select count(*) from {schema}.events where source = 'messages'"
            ),
            [(0,)],
        )

    def test_all_three_inputs_share_events_and_days(self):
        others = (("s", "screenmemory"), ("b", "browser_history"))
        three = {"screen": "s", "browser": "b", "messages": "m"}
        miroir = self.configure([timeline("t", three)], others=others)
        schema = self.schema(miroir, "t")
        self.send(miroir, "s", screen_batch(*screen(0, SAFARI), *screen(10, SAFARI)))
        self.send(
            miroir,
            "b",
            browser_batch(
                place(1, "https://example.org/a", "Page A"), visit(1, 1, 100, 1)
            ),
            "firefox_places_batch",
        )
        self.deliver(miroir, FIXTURE)
        self.assertEqual(
            self.query(
                f"select source, count(*) from {schema}.events group by 1 order by 1"
            ),
            [("browser", 1), ("messages", 27), ("screen", 1)],
        )
        self.assertEqual(
            self.query(
                f"select source, sum(events)::int from {schema}.days group by 1 order by 1"
            ),
            [("browser", 1), ("messages", 27), ("screen", 1)],
        )
        # The browser's search does not find message text.
        self.assertEqual(
            self.query(
                f"select count(*) from {schema}.events "
                "where search @@ websearch_to_tsquery('simple', 'chapter')"
            ),
            [(0,)],
        )


LARGE = int(os.environ.get("MIROIR_TIMELINE_MESSAGES", "20000"))
WORDS = [f"w{n}" for n in range(5000)]


def synthetic_chat(c):
    """Conversation c's number, chat guid and whether it is a group."""
    number = f"+1555{c:07d}"
    group = c % 10 == 0
    return number, f"iMessage;+;chat{c}" if group else f"iMessage;-;{number}", group


def synthetic(count, chats=320, seed=7):
    """`count` messages in `chats` conversations, a quarter of them about 4 KB of
    text (long enough to be stored out of line), one reaction in
    ten, one attachment in twenty-five, delivered as several batches."""
    rng = random.Random(seed)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    records = []
    for c in range(chats):
        number, guid, group = synthetic_chat(c)
        members = [number] + ([f"+1555{(c + 1) % chats:07d}"] if group else [])
        records += chat(
            guid, f"chat{c}" if group else number, *members, style=43 if group else 45
        )
        records.append(contact(number, first_name=f"Person{c}", last_name="Example"))
    for n in range(count):
        c = int(rng.paretovariate(1.2)) % chats
        number, guid, group = synthetic_chat(c)
        moment = start + timedelta(seconds=n * 170 + rng.randint(0, 60))
        text = " ".join(rng.choices(WORDS, k=rng.choice((5, 20, 80, 700))))
        me = rng.random() < 0.45
        sent = message(
            f"m{n}", 0, guid, me=me, handle=None if me and group else number, text=text
        )
        sent[0]["date"] = apple(moment)
        records += sent
        if n % 10 == 9:
            tap = reaction(
                f"r{n}", 0, guid, 2000 + n % 6, f"p:0/m{n - 1}", handle=number
            )
            tap[0]["date"] = apple(moment + timedelta(seconds=5))
            records += tap
        if n % 25 == 0:
            records += attachment(
                f"a{n}",
                f"m{n}",
                media_key=f"a{n}.jpeg",
                mime_type="image/jpeg",
                total_bytes=100_000,
                transfer_name=f"IMG_{n}.JPG",
            )
    return records


class TimelineMessagesLargeTests(TimelineMessagesFixture):
    def test_a_large_history_builds_and_then_builds_incrementally(self):
        """Tens of thousands of messages with long text (MIROIR_TIMELINE_MESSAGES,
        default 20,000): the first build reads them all; a later build reads one
        new message; a build with nothing new; and a build that must read all of
        them again into tables that exist, as after a replaced input. The timings
        are printed. No margin: each build reads only the loads completed since
        the last, so the incremental timings are an incremental build's."""
        # With the smallest temp_buffers: PostgreSQL 18 fails with "no empty local
        # buffer available" where a build stages many rows in a temporary table
        # (decision 040, the media upgrade). The timeline stages in an unlogged
        # table of its schema instead.
        self.query("alter database postgres set temp_buffers = 100")
        self.addCleanup(self.query, "alter database postgres reset temp_buffers")
        miroir = self.configure([timeline("t", reader_role=READER)])
        schema = self.schema(miroir, "t")
        plan = analysis_plan(miroir.analyses[0])
        records = synthetic(LARGE)
        timings = {}

        def timed(name, action):
            begun = time.monotonic()
            action()
            timings[name] = time.monotonic() - begun

        timed("load", lambda: [self.load(miroir, records[n::4]) for n in range(4)])
        timed("first build", lambda: self.build(plan))
        expected = sum(r["type"] == "message" and r["guid"][0] == "m" for r in records)
        count = f"select count(*) from {schema}.messages"
        self.assertEqual(self.query(count), [(expected,)])
        number, guid, _ = synthetic_chat(3)
        late = message("late", 0, guid, handle=number, text="one more")
        timed("load one", lambda: self.load(miroir, late))
        timed("one more", lambda: self.build(plan))
        self.assertEqual(self.query(count), [(expected + 1,)])
        timed("nothing new", lambda: self.build(plan))
        # Every message again, into the existing tables: the path a replaced
        # input takes, through dbt's temporary relation.
        for table, where in (
            ("messages", "true"),
            ("timeline_message_chats", "true"),
            ("events", "source = 'messages'"),
            ("days", "source = 'messages'"),
        ):
            self.query(f"update {schema}.{table} set _input = 'replaced' where {where}")
        timed("read again", lambda: self.build(plan))
        self.assertEqual(self.query(count), [(expected + 1,)])
        self.assertEqual(
            self.query(
                f"select count(*) from {schema}.events where source = 'messages'"
            ),
            [(expected + 1,)],
        )
        self.assertEqual(
            self.query(f"select count(*) from {schema}.conversations"), [(320,)]
        )
        version = self.query("show server_version")[0][0]
        print(
            f"\n{LARGE} messages, Postgres {version}: "
            + ", ".join(f"{k} {v:.1f} s" for k, v in timings.items())
        )


if __name__ == "__main__":
    unittest.main()
