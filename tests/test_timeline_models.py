"""The timeline analysis package (decision 040): catalogue entry and its models.

The model tests build against a disposable real Postgres (MIROIR_TEST_POSTGRES_BIN,
see tests/postgres.py) from invented ScreenMemory and browser batches.
"""

import getpass
import gzip
import json
import os
import tempfile
import unittest
from collections import Counter
from contextlib import closing, nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import dlt
import psycopg2
import psycopg2.errors
from dbt.cli.main import dbtRunner

from src.analyses.catalog import PACKAGES
from src.core.analysis import analysis_plan
from src.core.artifacts import submit_artifact
from src.core.config import ConfigurationError, parse_miroir
from src.core.derive import DERIVE_VERSION, _Writer
from src.core.operations import MetadataStore
from src.core.runner import import_artifact
from src.pipelines.transform import build_connection_models
from tests.postgres import start_postgres
from tests.test_miroir import example

READER = "timeline_reader"
# Local midnight of 2026-11-20 in America/Toronto (EST, UTC-5).
MIDNIGHT = int(datetime(2026, 11, 20, 5, tzinfo=timezone.utc).timestamp())
SAFARI, TERMINAL, NOTES, MAIL = (
    "com.apple.Safari",
    "com.apple.Terminal",
    "com.apple.Notes",
    "com.apple.mail",
)
UNLISTED = "com.example.unlisted-app"
LINKS = "timeline_links"  # a miroir of its own: other raw datasets
MOVES = "timeline_moves"
MEDIA = "timeline_media"


def raw_configuration(analyses, miroir_id="timeline_tests"):
    return {
        "schema_version": 1,
        "id": miroir_id,
        "connections": [
            {"id": "screen", "integration": "screenmemory", "settings": {}},
            {"id": "browser", "integration": "browser_history", "settings": {}},
            {"id": "laptop", "integration": "screenmemory", "settings": {}},
        ],
        "analyses": analyses,
    }


def configuration(analyses, miroir_id="timeline_tests"):
    return parse_miroir(json.dumps(raw_configuration(analyses, miroir_id)))


def timeline(analysis_id, inputs, reader_role=None):
    return {
        "id": analysis_id,
        "package": "timeline",
        "inputs": inputs,
        "timezone": "America/Toronto",
        "parameters": {"reader_role": reader_role},
    }


def path(offset):
    return f"2026/11/20/{MIDNIGHT + offset}.jpeg"


def frame(offset, text=None, withheld=None):
    return {
        "type": "frame",
        "ts": MIDNIGHT + offset,
        "image_path": path(offset),
        "text": None if withheld else (text or f"Invented text {offset}"),
        "withheld": withheld,
        "masked": 0,
    }


def usage(offset, bundle):
    return {"type": "app_usage", "ts": MIDNIGHT + offset, "bundle_id": bundle}


def screen_batch(*records):
    lines = [
        {
            "type": "header",
            "format": "screenmemory_batch",
            "version": 1,
            "collected_at": "2026-11-20T15:00:00Z",
        },
        *records,
        {
            "type": "trailer",
            "frames": sum(r["type"] == "frame" for r in records),
            "app_usage": sum(r["type"] == "app_usage" for r in records),
            "masked": {},
            "withheld": {},
        },
    ]
    return gzip.compress("".join(json.dumps(line) + "\n" for line in lines).encode())


def screen(offset, bundle):
    return [frame(offset), usage(offset, bundle)]


def browser_batch(*rows):
    lines = [
        {
            "type": "header",
            "format": "firefox_places_batch",
            "version": 1,
            "collected_at": "2026-11-20T15:00:00Z",
        },
        *({"type": "row", "table": table, "row": row} for table, row in rows),
        {
            "type": "trailer",
            "rows": dict(Counter(table for table, _ in rows)),
            "masked": {},
        },
    ]
    return gzip.compress("".join(json.dumps(line) + "\n" for line in lines).encode())


def place(place_id, url, title):
    return ("moz_places", {"id": place_id, "url": url, "title": title})


def visit(visit_id, place_id, offset, visit_type):
    return (
        "moz_historyvisits",
        {
            "id": visit_id,
            "place_id": place_id,
            "visit_date": (MIDNIGHT + offset) * 1_000_000,
            "from_visit": 0,
            "visit_type": visit_type,
        },
    )


class TimelinePackageTests(unittest.TestCase):
    def test_package_reads_optional_screen_browser_and_messages_every_five_minutes(
        self,
    ):
        package = PACKAGES["timeline"]
        self.assertEqual(package.min_build_interval, timedelta(minutes=5))
        self.assertEqual(
            {role: (s.integrations, s.required) for role, s in package.inputs.items()},
            {
                "screen": (("screenmemory",), False),
                "browser": (("browser_history",), False),
                "messages": (("apple_messages",), False),
            },
        )
        self.assertEqual(
            package.relations,
            (
                "screen_frames",
                "events",
                "days",
                "apps",
                "conversations",
                "messages",
                "people",
            ),
        )
        for inputs in ({"screen": "screen"}, {"browser": "browser"}):
            configuration([timeline("t", inputs)])
        with self.assertRaisesRegex(ConfigurationError, "link at least one input"):
            configuration([timeline("t", {})])

    def test_reader_role_is_a_plain_role_name_or_nothing(self):
        def plan(role):
            miroir = configuration([timeline("t", {"screen": "screen"}, role)])
            return analysis_plan(miroir.analyses[0]).variables

        self.assertIsNone(plan(None)["timeline_reader_role"])
        self.assertIsNone(plan("  ")["timeline_reader_role"])
        self.assertEqual(plan(READER)["timeline_reader_role"], READER)
        for role in ("public", "pg_read_all_data", "Reader", "a b", "x;drop", "1x"):
            with self.subTest(role=role), self.assertRaises(ConfigurationError):
                plan(role)

    def test_only_the_timeline_receives_connection_ids(self):
        miroir = configuration([timeline("t", {"browser": "browser"})])
        variables = analysis_plan(miroir.analyses[0]).variables
        self.assertEqual(variables["miroir_connections"], {"browser": "browser"})
        self.assertEqual(
            variables["miroir_unlinked_sources"], ["screenmemory", "apple_messages"]
        )
        music = analysis_plan(parse_miroir(json.dumps(example())).analyses[0])
        self.assertNotIn("miroir_connections", music.variables)

    def test_builds_without_a_miroir_analysis_leave_the_timeline_out(self):
        # dbt reads every model when it parses the project without an analysis.
        dbt = Path(__file__).parents[1] / "dbt"
        env = {
            f"DESTINATION__POSTGRES__CREDENTIALS__{part}": "unused"
            for part in ("HOST", "USERNAME", "PASSWORD", "DATABASE")
        }
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, env):
            result = dbtRunner().invoke(
                [
                    "ls",
                    *("--project-dir", str(dbt), "--profiles-dir", str(dbt)),
                    *("--target-path", f"{tmp}/target", "--log-path", f"{tmp}/logs"),
                    *("--resource-type", "model", "--output", "path", "--quiet"),
                ]
            )
        self.assertTrue(result.success)
        self.assertTrue(result.result)
        self.assertFalse([p for p in result.result if "timeline" in p])


class TimelineBuildFixture(unittest.TestCase):
    """A disposable warehouse, loads through the real runner, builds by dbt."""

    @classmethod
    def setUpClass(cls):
        cls.root, cls.port = start_postgres(cls, "miroir-timeline-")
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

    def setUp(self):
        self.store = MetadataStore(
            f"postgresql+psycopg2://{getpass.getuser()}@127.0.0.1:{self.port}/postgres"
        )
        self.store.migrate()
        self.addCleanup(self.store.engine.dispose)
        self.query(
            f"do $$ begin create role {READER} nologin; "
            "exception when duplicate_object then null; end $$"
        )
        self.execution = {
            "destination": dlt.destinations.postgres(
                credentials={
                    "host": "127.0.0.1",
                    "port": self.port,
                    "database": "postgres",
                    "username": getpass.getuser(),
                    "password": "",
                }
            ),
            "resolve_credentials": lambda c: {},
            "pipelines_dir": str(self.root / "dlt"),
            "transform": self.build,
        }
        # No margin: each build reads only loads completed since the last, so
        # the narrow incremental paths are the ones tested.
        self.margin = "0 seconds"
        self.sent = 0

    def build(self, plan):
        variables = {**plan.variables, "timeline_cursor_margin": self.margin}
        build_connection_models(replace(plan, variables=variables))

    def query(self, sql, role=None):
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
            if role:
                cursor.execute(f"set local role {role}")
            cursor.execute(sql)
            return cursor.fetchall() if cursor.description else []

    def send(self, miroir, connection_id, body, input_format=None):
        self.sent += 1
        source = self.root / f"batch-{self.sent}.jsonl.gz"
        source.write_bytes(body)
        connection = next(c for c in miroir.connections if c.config.id == connection_id)
        artifact, _ = submit_artifact(
            miroir,
            connection,
            source,
            store=self.store,
            root=self.root / "artifacts",
            input_format=input_format,
        )
        outcome = import_artifact(
            miroir,
            connection_id,
            artifact["id"],
            store=self.store,
            origin="local",
            **self.execution,
        )
        self.assertEqual(
            (outcome.ingestion, outcome.transformation), ("succeeded", "succeeded")
        )

    def schema(self, miroir, analysis_id):
        analysis = next(a for a in miroir.analyses if a.id == analysis_id)
        return analysis_plan(analysis).relations[0].split(".")[0]

    def sessions(self, schema):
        return self.query(
            "select substr(event_id, 8), title, started_at, ended_at, "
            "(payload->>'frames')::int "
            f"from {schema}.events where source = 'screen' order by started_at"
        )

    def snapshot(self, schema):
        return {
            "screen_frames": self.query(
                "select image_path, captured_at, bundle_id, text, withheld, masked, "
                "connection, media_status, thumbnail_w360, thumbnail_w1280, "
                f"media_withheld from {schema}.screen_frames order by image_path"
            ),
            "events": self.query(
                "select source, connection, event_id, started_at, ended_at, kind, "
                "title, body, media_kind, media_key, media_status, thumbnail_w360, "
                "thumbnail_w1280, media_withheld, group_key, payload, "
                f"search::text from {schema}.events order by event_id"
            ),
            "days": self.query(
                "select source, day, events, frames, timezone "
                f"from {schema}.days order by source, day"
            ),
            "apps": self.query(f"select * from {schema}.apps order by bundle_id"),
        }

    def media_record(
        self, miroir, offset, status="made", connection_id="screen", key=None
    ):
        """One media_objects row, written as the derive worker writes it."""
        connection = next(c for c in miroir.connections if c.config.id == "screen")
        made = status == "made"
        writer = _Writer(
            connection,
            self.execution["destination"],
            self.execution["pipelines_dir"],
            self.store,
            5,
        )
        now = datetime.now(timezone.utc)
        writer.write(
            [
                {
                    "connection_id": connection_id,
                    "kind": "screenshots",
                    "key": key or path(offset),
                    "size": 1000 + offset,
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

    def media(self, schema):
        return (
            self.query(
                "select image_path, media_status, thumbnail_w360, thumbnail_w1280 "
                f"from {schema}.screen_frames order by image_path"
            ),
            self.query(
                "select event_id, media_key, media_status, thumbnail_w360, "
                f"thumbnail_w1280 from {schema}.events order by event_id"
            ),
        )


class TimelineModelTests(TimelineBuildFixture):
    def test_incremental_builds_handle_late_rows_and_match_a_full_build(self):
        miroir = configuration(
            [timeline("timeline", {"screen": "screen", "browser": "browser"}, READER)]
        )
        schema = self.schema(miroir, "timeline")
        batch_format = "firefox_places_batch"
        self.send(
            miroir,
            "screen",
            screen_batch(
                # A Safari session over local midnight, then Terminal, still open.
                *screen(-120, SAFARI),
                *screen(-60, SAFARI),
                *screen(0, SAFARI),
                *screen(10, TERMINAL),
                frame(20),  # its app usage arrives in the next batch
                frame(30, withheld="excluded_app"),
                usage(30, TERMINAL),
                # Two Safari sessions six minutes apart, later joined.
                *screen(1000, SAFARI),
                *screen(1400, SAFARI),
                # Untouched sessions: two separate windows are rebuilt.
                *screen(2000, UNLISTED),
                *screen(2100, TERMINAL),
                # A Safari session later split by another app.
                *screen(3000, SAFARI),
                *screen(3010, SAFARI),
                *screen(3020, SAFARI),
                # A Notes session whose start moves earlier.
                *screen(5000, NOTES),
                *screen(5010, NOTES),
            ),
        )
        self.assertEqual(
            [(s[0], s[4]) for s in self.sessions(schema)],
            [
                (path(-120), 3),
                (path(10), 2),
                (path(1000), 1),
                (path(1400), 1),
                (path(2000), 1),
                (path(2100), 1),
                (path(3000), 3),
                (path(5000), 2),
            ],
        )
        self.send(
            miroir,
            "browser",
            browser_batch(
                place(1, "https://www.example.org/a", "Page A"),
                place(2, "https://example.net/b", "Page B"),
                visit(1, 1, 100, 1),
                visit(2, 1, 110, 9),  # a reload is not a visit
                visit(4, 2, 200 - 2 * 86400, 2),  # two days earlier
            ),
            batch_format,
        )
        self.send(
            miroir,
            "screen",
            screen_batch(
                *screen(40, TERMINAL),  # the open session grows
                usage(20, TERMINAL),  # a late app for a frame already loaded
                frame(-60, text="Resent text"),  # a resent frame
                *screen(1200, SAFARI),
                *screen(3015, MAIL),
                *screen(4900, NOTES),
            ),
        )
        self.send(
            miroir,
            "browser",
            browser_batch(
                place(1, "https://www.example.org/a", "Page A, renamed"),
                visit(3, 2, 300, 1),
                # A cleared visit's id, reused by an embed: its old day loses it.
                visit(4, 2, 400, 4),
            ),
            batch_format,
        )

        at = lambda offset: datetime.fromtimestamp(MIDNIGHT + offset, timezone.utc)
        self.assertEqual(
            self.sessions(schema),
            [
                (path(-120), "Safari", at(-120), at(0), 3),
                (path(10), "Terminal", at(10), at(40), 4),
                (path(1000), "Safari", at(1000), at(1400), 3),
                (path(2000), "Unlisted App", at(2000), at(2000), 1),
                (path(2100), "Terminal", at(2100), at(2100), 1),
                (path(3000), "Safari", at(3000), at(3010), 2),
                (path(3015), "Mail", at(3015), at(3015), 1),
                (path(3020), "Safari", at(3020), at(3020), 1),
                (path(4900), "Notes", at(4900), at(5010), 3),
            ],
        )
        self.assertEqual(
            self.query(
                "select substr(event_id, 8) from "
                f"{schema}.timeline_screen where deleted order by 1"
            ),
            [(path(1400),), (path(5000),)],
        )
        frames = {
            row[0]: row[1:]
            for row in self.query(
                f"select image_path, bundle_id, text, withheld from {schema}.screen_frames"
            )
        }
        self.assertEqual(len(frames), 19)
        self.assertEqual(frames[path(20)][0], TERMINAL)
        self.assertEqual(frames[path(30)], (TERMINAL, None, "excluded_app"))
        self.assertEqual(frames[path(-60)][1], "Resent text")
        self.assertEqual(
            self.query(
                "select event_id, title, group_key, body from "
                f"{schema}.events where source = 'browser' order by event_id"
            ),
            [
                (
                    "browser:1",
                    "Page A, renamed",
                    "example.org",
                    "https://www.example.org/a",
                ),
                ("browser:3", "Page B", "example.net", "https://example.net/b"),
            ],
        )
        self.assertEqual(
            self.query(
                f"select source, day::text, events, frames from {schema}.days "
                "order by source, day"
            ),
            [
                ("browser", "2026-11-20", 2, None),
                ("screen", "2026-11-19", 1, 2),
                ("screen", "2026-11-20", 8, 17),
            ],
        )
        self.assertEqual(
            self.query(
                f"select name, known from {schema}.apps where bundle_id = '{UNLISTED}'"
            ),
            [("Unlisted App", False)],
        )
        self.assertEqual(
            self.query(
                f"select count(*) from {schema}.events "
                "where search @@ websearch_to_tsquery('simple', 'renamed')"
            ),
            [(1,)],
        )
        self.assertEqual(
            self.query(
                f"select image_path from {schema}.screen_frames where "
                "to_tsvector('simple', text) @@ websearch_to_tsquery('simple', 'resent')"
            ),
            [(path(-60),)],
        )

        # Rows of a load still running are not read, in lookups neither. A load
        # with an older ID that completes later is read at the next build. One
        # that never completes is never read.
        dataset = miroir.connections[0].binding.dataset_name

        def raw_frame(load_id, offset, text="Late", withheld="null"):
            self.query(
                f"insert into {dataset}.frames (image_path, ts, captured_at, text, "
                "withheld, masked, _dlt_load_id, _dlt_id) values "
                f"('{path(offset)}', {MIDNIGHT + offset}, "
                f"to_timestamp({MIDNIGHT + offset}), '{text}', {withheld}, 0, "
                f"'{load_id}', 'invented-frame-{offset}')"
            )

        def raw_usage(load_id, offset, bundle):
            self.query(
                f"insert into {dataset}.app_usage (ts, bundle_id, observed_at, "
                f"_dlt_load_id, _dlt_id) values ({MIDNIGHT + offset}, '{bundle}', "
                f"to_timestamp({MIDNIGHT + offset}), '{load_id}', "
                f"'invented-usage-{offset}-{load_id}')"
            )

        late, never = "1000000000.000001", "1000000000.000002"
        # Withheld, yet with text: the model keeps none.
        raw_frame(late, 50, text="Should not stay", withheld="'marker'")
        raw_usage(late, 50, TERMINAL)
        raw_frame(never, 60)
        raw_usage(never, 60, TERMINAL)
        raw_usage(never, 40, "com.aaa.first")  # would be the frame at 40's app
        plan = analysis_plan(miroir.analyses[0])
        self.build(plan)
        self.assertEqual(self.sessions(schema)[1][3:], (at(40), 4))
        self.query(
            f"insert into {dataset}._dlt_loads (load_id, schema_name, status, "
            f"inserted_at) values ('{late}', 'screenmemory', 0, now())"
        )
        self.build(plan)
        self.assertEqual(self.sessions(schema)[1][3:], (at(50), 5))
        frames = {
            row[0]: row[1:]
            for row in self.query(
                f"select image_path, bundle_id, text, withheld from {schema}.screen_frames"
            )
        }
        self.assertEqual(frames[path(50)], (TERMINAL, None, "marker"))
        self.assertNotIn(path(60), frames)
        self.assertEqual(frames[path(40)][0], TERMINAL)

        # A frame resent with another time leaves its old session and day: the
        # second frame of a Safari session moves to the evening before, and a
        # frame without an app, alone on its day, moves there next.
        self.send(
            miroir,
            "screen",
            screen_batch(
                frame(3010) | {"ts": MIDNIGHT - 7200},
                usage(-7200, SAFARI),
                frame(3 * 86400),
            ),
        )
        sessions = self.sessions(schema)
        self.assertEqual(sessions[0], (path(3010), "Safari", at(-7200), at(-7200), 1))
        self.assertIn((path(3000), "Safari", at(3000), at(3000), 1), sessions)
        self.send(
            miroir, "screen", screen_batch(frame(3 * 86400) | {"ts": MIDNIGHT - 7300})
        )

        # A first build over the same rows gives the same tables.
        both = {"screen": "screen", "browser": "browser"}
        miroir = configuration(
            [
                timeline("timeline", both, READER),
                timeline("fresh", both),
                timeline("screen_only", {"screen": "screen"}),
                timeline("browser_only", {"browser": "browser"}),
            ]
        )
        for analysis in miroir.analyses[1:]:
            self.build(analysis_plan(analysis))
        incremental = self.snapshot(schema)
        self.assertEqual(self.snapshot(self.schema(miroir, "fresh")), incremental)
        self.assertEqual(
            self.query(
                f"select source, day::text, events, frames from {schema}.days "
                "order by source, day"
            ),
            [
                ("browser", "2026-11-20", 2, None),
                ("screen", "2026-11-19", 2, 4),
                ("screen", "2026-11-20", 8, 17),
            ],
        )
        # A rebuild from scratch gives the same tables again.
        self.build(replace(analysis_plan(miroir.analyses[0]), full_refresh=True))
        self.assertEqual(self.snapshot(schema), incremental)
        screen_only = self.schema(miroir, "screen_only")
        browser_only = self.schema(miroir, "browser_only")
        self.assertEqual(
            self.query(f"select source, count(*) from {screen_only}.events group by 1"),
            [("screen", 10)],
        )
        self.assertEqual(
            self.query(
                f"select source, count(*) from {browser_only}.events group by 1"
            ),
            [("browser", 2)],
        )
        self.assertEqual(
            self.query(f"select count(*) from {browser_only}.screen_frames"), [(0,)]
        )
        # A changed timezone recounts every day and keeps none of the old ones.
        fresh = next(a for a in miroir.analyses if a.id == "fresh")
        utc = replace(fresh, config=fresh.config.model_copy(update={"timezone": "UTC"}))
        self.build(analysis_plan(utc))
        self.assertEqual(
            self.query(
                "select source, day::text, events, frames, timezone from "
                f"{self.schema(miroir, 'fresh')}.days order by source, day"
            ),
            [
                ("browser", "2026-11-20", 2, None, "UTC"),
                ("screen", "2026-11-20", 10, 21, "UTC"),
            ],
        )

        # The reader selects the timeline's tables and nothing else.
        for table in ("screen_frames", "events", "days", "apps"):
            self.assertTrue(self.query(f"select 1 from {schema}.{table}", READER))
        for relation in (
            f"{schema}.timeline_screen",
            f"{self.schema(miroir, 'fresh')}.events",
            f"{dataset}.frames",
            f"{miroir.connections[1].binding.dataset_name}.zen_places",
        ):
            with (
                self.subTest(relation=relation),
                self.assertRaises(psycopg2.errors.InsufficientPrivilege),
            ):
                self.query(f"select 1 from {relation}", READER)
        # Unsetting the role revokes what an earlier build granted, even where
        # default privileges grant the reader every new table.
        self.query(f"alter default privileges grant select on tables to {READER}")
        self.addCleanup(
            self.query,
            f"alter default privileges revoke select on tables from {READER}",
        )
        miroir = configuration([timeline("timeline", both)])
        self.build(analysis_plan(miroir.analyses[0]))
        for table in (
            "screen_frames",
            "events",
            "days",
            "apps",
            "conversations",
            "messages",
            "people",
            "timeline_screen",
        ):
            with (
                self.subTest(table=table),
                self.assertRaises(psycopg2.errors.InsufficientPrivilege),
            ):
                self.query(f"select 1 from {schema}.{table}", READER)
        self.assertTrue(
            {
                "miroir_frames_dlt_load_id",
                "miroir_frames_ts",
                "miroir_frames_image_path",
                "miroir__dlt_loads_load_id",
                "miroir__dlt_loads_inserted_at",
            }
            <= {
                row[0]
                for row in self.query(
                    f"select indexname from pg_indexes where schemaname = '{dataset}'"
                )
            }
        )

    def test_unlinking_or_replacing_an_input_leaves_none_of_its_rows(self):
        self.margin = "10 minutes"  # the installation's
        both = {"screen": "screen", "browser": "browser"}
        miroir = configuration([timeline("timeline", both, READER)], LINKS)
        schema = self.schema(miroir, "timeline")
        self.send(
            miroir, "screen", screen_batch(*screen(0, SAFARI), *screen(10, UNLISTED))
        )
        self.send(
            miroir,
            "browser",
            browser_batch(
                place(1, "https://example.org/a", "Page A"), visit(1, 1, 100, 1)
            ),
            "firefox_places_batch",
        )

        def screen_rows():
            return {
                table: self.query(
                    f"select count(*) from {schema}.{table}"
                    + (
                        " where source = 'screen'"
                        if table in ("events", "days")
                        else ""
                    )
                )[0][0]
                for table in ("screen_frames", "timeline_screen", "events", "days")
            }

        def browser_rows():
            return (
                self.query(
                    "select event_id, title, started_at from "
                    f"{schema}.events where source = 'browser'"
                ),
                self.query(
                    f"select day, events from {schema}.days where source = 'browser'"
                ),
            )

        browser = browser_rows()
        self.assertEqual(
            screen_rows(),
            {"screen_frames": 2, "timeline_screen": 2, "events": 2, "days": 1},
        )
        # Unlinked: its rows go at the next build, wherever they were.
        miroir = configuration(
            [timeline("timeline", {"browser": "browser"}, READER)], LINKS
        )
        self.build(analysis_plan(miroir.analyses[0]))
        self.assertEqual(
            screen_rows(),
            {"screen_frames": 0, "timeline_screen": 0, "events": 0, "days": 0},
        )
        self.assertEqual(
            self.query(f"select count(*) from {schema}.apps where not known"), [(0,)]
        )
        self.assertEqual(browser_rows(), browser)

        # Replaced by another connection: only that connection's rows.
        replaced = {"screen": "laptop", "browser": "browser"}
        miroir = configuration([timeline("timeline", replaced, READER)], LINKS)
        self.send(miroir, "laptop", screen_batch(*screen(500, NOTES)))
        self.assertEqual(
            self.query(f"select image_path, connection from {schema}.screen_frames"),
            [(path(500), "laptop")],
        )
        self.assertEqual(
            self.query(
                f"select group_key from {schema}.events where source = 'screen'"
            ),
            [(NOTES,)],
        )
        # And back: the first connection's rows again, read from the start.
        miroir = configuration([timeline("timeline", both, READER)], LINKS)
        self.build(analysis_plan(miroir.analyses[0]))
        self.assertEqual(
            self.query(
                f"select image_path, connection from {schema}.screen_frames order by 1"
            ),
            [(path(0), "screen"), (path(10), "screen")],
        )
        self.assertEqual(
            screen_rows(),
            {"screen_frames": 2, "timeline_screen": 2, "events": 2, "days": 1},
        )
        self.assertEqual(browser_rows(), browser)

    def test_a_build_that_fails_after_reading_a_move_loses_no_old_position(self):
        self.margin = "10 minutes"  # the installation's: the next build reads again
        both = {"screen": "screen", "browser": "browser"}
        miroir = configuration(
            [timeline("timeline", both), timeline("fresh", both)], MOVES
        )
        schema = self.schema(miroir, "timeline")
        datasets = [c.binding.dataset_name for c in miroir.connections[:2]]
        only_first = miroir.analyses[:1]
        loading = configuration([timeline("timeline", both)], MOVES)
        # A Safari session over local midnight, a Notes session after it; a visit.
        self.send(
            loading,
            "screen",
            screen_batch(
                *screen(-20, SAFARI),
                *screen(-10, SAFARI),
                *screen(0, SAFARI),
                *screen(1000, NOTES),
            ),
        )
        self.send(
            loading,
            "browser",
            browser_batch(
                place(1, "https://example.org/a", "Page A"), visit(1, 1, 100, 1)
            ),
            "firefox_places_batch",
        )
        for dataset in datasets:  # long done: outside the margin from now on
            self.query(
                f"update {dataset}._dlt_loads "
                "set inserted_at = inserted_at - interval '1 hour'"
            )

        # The moves: a frame leaves for the Notes session, across midnight; the
        # visit's id is reused the next day. Each build stops after its first
        # model, as one that fails there would.
        def first_model_only(name):
            def transform(plan):
                variables = {**plan.variables, "timeline_cursor_margin": self.margin}
                build_connection_models(
                    replace(
                        plan,
                        selector=f"path:models/timeline/{name}.sql",
                        variables=variables,
                        ready=None,
                    )
                )

            return transform

        transform = self.execution["transform"]
        self.execution["transform"] = first_model_only("screen_frames")
        self.send(
            loading,
            "screen",
            screen_batch(frame(-10) | {"ts": MIDNIGHT + 1010}, usage(1010, NOTES)),
        )
        self.execution["transform"] = first_model_only("timeline_browser")
        self.send(
            loading,
            "browser",
            browser_batch(visit(1, 1, 100 + 86400, 1)),
            "firefox_places_batch",
        )
        self.execution["transform"] = transform

        # The next build, inside the margin, reads both loads again.
        self.build(analysis_plan(only_first[0]))
        self.build(analysis_plan(miroir.analyses[1]))
        self.assertEqual(
            self.snapshot(self.schema(miroir, "fresh")), self.snapshot(schema)
        )
        self.assertEqual(
            self.query(
                f"select source, day::text, events, frames from {schema}.days "
                "order by source, day"
            ),
            [
                ("browser", "2026-11-21", 1, None),
                ("screen", "2026-11-19", 1, 1),
                ("screen", "2026-11-20", 1, 3),
            ],
        )

    def test_stored_media_reaches_frames_and_sessions_whenever_it_is_recorded(self):
        both = {"screen": "screen", "browser": "browser"}
        miroir = configuration(
            [timeline("timeline", both, READER), timeline("fresh", both)], MEDIA
        )
        schema = self.schema(miroir, "timeline")
        plan = analysis_plan(miroir.analyses[0])
        loading = configuration([timeline("timeline", both, READER)], MEDIA)
        # No media_objects table yet: every frame is unknown.
        self.send(
            loading,
            "screen",
            screen_batch(*screen(0, SAFARI), *screen(10, SAFARI), *screen(1000, NOTES)),
        )
        self.send(
            loading,
            "browser",
            browser_batch(
                place(1, "https://example.org/a", "Page A"), visit(1, 1, 100, 1)
            ),
            "firefox_places_batch",
        )
        self.assertEqual(
            self.media(schema)[0],
            [(path(0), None, None, None), (path(10), None, None, None)]
            + [(path(1000), None, None, None)],
        )
        # Recorded after their frames were built: the next build reads them.
        self.media_record(miroir, 0)
        self.media_record(miroir, 1000, "failed")
        # Another connection's record in the same dataset is not this one's.
        self.media_record(miroir, 10, connection_id="laptop")
        self.build(plan)
        frames, events = self.media(schema)
        self.assertEqual(
            frames,
            [
                (path(0), "made", True, True),
                (path(10), None, None, None),
                (path(1000), "failed", False, False),
            ],
        )
        self.assertEqual(
            events,
            [
                ("browser:1", None, None, None, None),
                (f"screen:{path(0)}", path(0), "made", True, True),
                (f"screen:{path(1000)}", path(1000), "failed", False, False),
            ],
        )
        # Recorded before its frame arrives: the frame's own build finds it.
        self.media_record(miroir, 2000)
        self.build(plan)
        self.send(loading, "screen", screen_batch(*screen(2000, TERMINAL)))
        frames, events = self.media(schema)
        self.assertIn((path(2000), "made", True, True), frames)
        self.assertIn((f"screen:{path(2000)}", path(2000), "made", True, True), events)
        # A record replaced by a later derive run (the merge, failed to made)
        # replaces the frame's, and its session's in both screen models.
        self.media_record(miroir, 1000)
        self.build(plan)
        frames, events = self.media(schema)
        self.assertIn((path(1000), "made", True, True), frames)
        self.assertIn((f"screen:{path(1000)}", path(1000), "made", True, True), events)
        self.assertEqual(
            self.query(
                "select media_status, thumbnail_w360, thumbnail_w1280 from "
                f"{schema}.timeline_screen where event_id = 'screen:{path(1000)}'"
            ),
            [("made", True, True)],
        )

        # A frame withheld after its screenshot was stored and derived (a newer
        # rule; the file stays): withheld wins in the frame and its session.
        def withheld():
            return (
                self.query(
                    f"select image_path, media_withheld from {schema}.screen_frames"
                    " order by 1"
                ),
                self.query(
                    f"select event_id, media_withheld from {schema}.events order by 1"
                ),
            )

        self.assertEqual(
            withheld()[1],
            [("browser:1", None)]
            + [(f"screen:{path(o)}", False) for o in (0, 1000, 2000)],
        )
        self.send(
            loading,
            "screen",
            screen_batch(frame(0, withheld="excluded_app"), usage(0, SAFARI)),
        )
        frames, events = withheld()
        self.assertEqual(
            frames,
            [(path(0), True), (path(10), False), (path(1000), False)]
            + [(path(2000), False)],
        )
        self.assertIn((f"screen:{path(0)}", True), events)
        self.assertIn(
            (f"screen:{path(0)}", path(0), "made", True, True), self.media(schema)[1]
        )

        # The reader reads the timeline's tables, never the raw media records.
        dataset = miroir.connections[0].binding.dataset_name
        self.assertTrue(self.query(f"select 1 from {schema}.screen_frames", READER))
        for relation in (f"{dataset}.media_objects", f"{dataset}.frames"):
            with (
                self.subTest(relation=relation),
                self.assertRaises(psycopg2.errors.InsufficientPrivilege),
            ):
                self.query(f"select 1 from {relation}", READER)

        # A first build over the same rows gives the same tables.
        self.build(analysis_plan(miroir.analyses[1]))
        fresh = self.schema(miroir, "fresh")
        self.assertEqual(self.media(fresh), self.media(schema))
        self.assertEqual(self.snapshot(fresh), self.snapshot(schema))

        # Tables built before the media columns gain them, and read every frame
        # and session once.
        expected = self.media(schema)
        for table in ("screen_frames", "timeline_screen", "events", "timeline_browser"):
            self.query(
                f"alter table {schema}.{table} drop column media_status, "
                "drop column thumbnail_w360, drop column thumbnail_w1280, "
                "drop column media_withheld"
            )
        self.build(plan)
        self.assertEqual(self.media(schema), expected)
        self.assertEqual(self.snapshot(schema), self.snapshot(fresh))
        # A table that has some of the media columns gains the rest.
        self.query(f"alter table {schema}.events drop column media_withheld")
        self.build(plan)
        self.assertEqual(self.snapshot(schema), self.snapshot(fresh))
        self.assertIn(
            ("miroir_media_objects_key",),
            self.query(
                f"select indexname from pg_indexes where schemaname = '{dataset}'"
            ),
        )
