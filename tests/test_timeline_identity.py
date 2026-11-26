"""The timeline package's tables, row for row as before lanes came from
declarations (package split, step 11).

`fixtures/timeline_identity.json` was written by this test (`--write`) on the
code before `media_ref`, the lane variable and `contract` (the base of step 11).
The same synthetic screen, browser and Messages deliveries, with media records
written before and after the rows they point at, must build the same tables:
every column (name, type, position), every index and every row. What differs
on every run is left out: the builds' clocks (`CLOCKS`) and the
analysis's random key (`timeline_secret`), so conversation ids are written as
the conversation keys they hash. `contract`, new in step 11, is not compared;
`timeline_messages`, also new, is a view, not a table.

Needs Postgres (MIROIR_TEST_POSTGRES_BIN, see tests/postgres.py).
"""

import json
import re
import sys
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from tests.test_messages_models import ALEX, DIRECT, FIXTURE, chat, message
from tests.test_timeline_messages import (
    TimelineMessagesFixture,
    configuration,
    timeline,
)
from tests.test_timeline_models import (
    NOTES,
    READER,
    SAFARI,
    TERMINAL,
    TimelineBuildFixture,
    browser_batch,
    frame,
    place,
    screen,
    screen_batch,
    usage,
    visit,
)

EXPECTED = Path(__file__).parent / "fixtures" / "timeline_identity.json"
# Written at each run's own time: build stamps (`_stamp`, and `_chats_seen`,
# the chats' stamp messages last read), load cursors (`_cursor`) and the load
# ID a chat was read from (`loaded`, dlt's clock).
CLOCKS = {"_stamp", "_cursor", "loaded", "_chats_seen"}
LEFT_OUT = {"timeline_secret", "contract"}
DBT_INDEX = re.compile(r'"?\b[0-9a-f]{32}\b"?')


def plain(value: Any) -> Any:
    """A value as JSON, the same on every run."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if isinstance(value, dict):
        return {k: plain(v) for k, v in sorted(value.items())}
    return value


def keyed(value: Any, ids: dict[str, str]) -> Any:
    """Every conversation id in a value written as its conversation key."""
    if isinstance(value, str):
        return ids.get(value, value)
    if isinstance(value, list):
        return [keyed(v, ids) for v in value]
    if isinstance(value, dict):
        return {k: keyed(v, ids) for k, v in value.items()}
    return value


def names_of(found: dict[str, Any], table: str) -> list[str]:
    return [c[0] for c in found[table]["columns"] if c[0] not in CLOCKS]


class TimelineIdentityTests(TimelineMessagesFixture):
    def deliveries(self):
        others = (("screen", "screenmemory"), ("browser", "browser_history"))
        inputs = {"screen": "screen", "browser": "browser", "messages": "m"}
        miroir = configuration(
            [timeline("t", inputs, reader_role=READER)],
            others=others,
            miroir_id="timeline_identity",
        )
        # Records before their rows: found when the rows arrive.
        TimelineMessagesFixture.media_record(self, miroir, "att1.jpeg")
        self.screen_record(miroir, 0)
        self.send(
            miroir,
            "screen",
            screen_batch(
                *screen(0, SAFARI),
                *screen(10, SAFARI),
                *screen(400, TERMINAL),
                frame(410, withheld="password_manager"),
                usage(410, TERMINAL),
                frame(900),  # no app
            ),
        )
        self.send(
            miroir,
            "browser",
            browser_batch(
                place(1, "https://example.org/a", "Page A"),
                place(2, "https://www.example.net/b", ""),
                visit(1, 1, 100, 1),
                visit(2, 2, 200, 1),
                visit(3, 1, 300, 5),  # a redirect, not a visit
            ),
            "firefox_places_batch",
        )
        self.deliver(miroir, FIXTURE)
        # Records after their rows, a failure and another connection's.
        self.screen_record(miroir, 400, status="failed")
        self.screen_record(miroir, 10, connection_id="laptop")
        TimelineMessagesFixture.media_record(self, miroir, "att3.pdf")
        TimelineMessagesFixture.media_record(self, miroir, "att4.caf", "failed")
        TimelineMessagesFixture.media_record(
            self, miroir, "att6.xyz", connection_id="another"
        )
        # Incremental builds: more frames, a late app, visits and a message.
        self.send(
            miroir,
            "screen",
            screen_batch(*screen(20, SAFARI), *screen(2000, NOTES), usage(900, NOTES)),
        )
        self.send(
            miroir,
            "browser",
            browser_batch(
                place(1, "https://example.org/a", "Page A, renamed"),
                visit(4, 1, 2100, 1),
            ),
            "firefox_places_batch",
        )
        self.deliver(
            miroir,
            [
                *chat(DIRECT[0], ALEX, ALEX),
                *message("late1", 900, DIRECT[0], handle=ALEX, text="a later one"),
            ],
        )
        return miroir

    def screen_record(self, miroir, offset, status="made", connection_id="screen"):
        TimelineBuildFixture.media_record(
            self, miroir, offset, status=status, connection_id=connection_id
        )

    def observed(self, schema: str) -> dict[str, Any]:
        ids = dict(
            self.query(
                "select distinct conversation_id, conversation_key "
                f"from {schema}.timeline_message_chats"
            )
        )
        tables = [
            name
            for (name,) in self.query(
                f"select tablename from pg_tables where schemaname = '{schema}' "
                "order by tablename"
            )
            if name not in LEFT_OUT
        ]
        found: dict[str, Any] = {}
        for table in tables:
            columns = self.query(
                "select column_name, data_type, ordinal_position "
                "from information_schema.columns "
                f"where table_schema = '{schema}' and table_name = '{table}' "
                "order by ordinal_position"
            )
            names = [name for name, _, _ in columns if name not in CLOCKS]
            quoted = ", ".join(f'"{name}"' for name in names)
            rows = self.query(f"select {quoted} from {schema}.{table}")
            values = [
                json.dumps(keyed(plain(list(row)), ids), sort_keys=True) for row in rows
            ]
            indexes = self.query(
                "select indexname, indexdef from pg_indexes "
                f"where schemaname = '{schema}' and tablename = '{table}' "
                "order by indexname"
            )
            found[table] = {
                "columns": [list(c) for c in columns],
                # dbt names its indexes by a hash of the time it made them.
                "indexes": sorted(DBT_INDEX.sub('"dbt"', d) for _, d in indexes),
                "rows": [json.loads(v) for v in sorted(values)],
            }
        return found

    def test_every_table_is_as_before(self):
        miroir = self.deliveries()
        observed = self.observed(self.schema(miroir, "t"))
        if "--write" in sys.argv:
            EXPECTED.parent.mkdir(exist_ok=True)
            EXPECTED.write_text(json.dumps(observed, indent=1, sort_keys=True) + "\n")
        expected = json.loads(EXPECTED.read_text())
        self.maxDiff = None
        self.assertEqual(sorted(observed), sorted(expected), "tables changed")
        for table in expected:
            with self.subTest(table=table):
                self.assertEqual(observed[table], expected[table])
        # The deliveries reach every source and both kinds of media.
        events = expected["events"]["rows"]
        self.assertEqual({row[0] for row in events}, {"screen", "browser", "messages"})
        status = names_of(expected, "screen_frames").index("media_status")
        found = {row[status] for row in expected["screen_frames"]["rows"]}
        self.assertTrue({"made", "failed", None} <= found)
        attachments = [
            item
            for row in expected["messages"]["rows"]
            for item in row[names_of(expected, "messages").index("attachments")]
        ]
        self.assertTrue(
            {"made", "failed", None} <= {a["media_status"] for a in attachments}
        )


if __name__ == "__main__":
    unittest.main(argv=[a for a in sys.argv if a != "--write"])
