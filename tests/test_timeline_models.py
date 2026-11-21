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


def configuration(analyses):
    return parse_miroir(
        json.dumps(
            {
                "schema_version": 1,
                "id": "timeline_tests",
                "connections": [
                    {"id": "screen", "integration": "screenmemory", "settings": {}},
                    {"id": "browser", "integration": "browser_history", "settings": {}},
                ],
                "analyses": analyses,
            }
        )
    )


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
    def test_package_reads_optional_screen_and_browser_inputs_every_five_minutes(self):
        package = PACKAGES["timeline"]
        self.assertEqual(package.min_build_interval, timedelta(minutes=5))
        self.assertEqual(
            {role: (s.integrations, s.required) for role, s in package.inputs.items()},
            {
                "screen": (("screenmemory",), False),
                "browser": (("browser_history",), False),
            },
        )
        self.assertEqual(package.relations, ("screen_frames", "events", "days", "apps"))
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
        self.assertEqual(variables["miroir_unlinked_sources"], ["screenmemory"])
        music = analysis_plan(parse_miroir(json.dumps(example())).analyses[0])
        self.assertNotIn("miroir_connections", music.variables)

    def test_builds_without_a_miroir_analysis_leave_the_timeline_out(self):
        # The nightly legacy build reads every model; the timeline has no legacy source.
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


class TimelineModelTests(unittest.TestCase):
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
        self.query(f"create role {READER} nologin")
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
            "transform": build_connection_models,
        }
        self.sent = 0

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
                f"connection from {schema}.screen_frames order by image_path"
            ),
            "events": self.query(
                "select source, connection, event_id, started_at, ended_at, kind, "
                "title, body, media_kind, media_key, group_key, payload, "
                f"search::text from {schema}.events order by event_id"
            ),
            "days": self.query(
                "select source, day, events, frames, timezone "
                f"from {schema}.days order by source, day"
            ),
            "apps": self.query(f"select * from {schema}.apps order by bundle_id"),
        }

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
                visit(4, 2, 200, 2),
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
                visit(4, 2, 400, 4),  # a cleared visit's id, reused by an embed
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

        # A load still running is not read; once listed as complete, it is.
        dataset = miroir.connections[0].binding.dataset_name
        self.query(
            f"insert into {dataset}.frames (image_path, ts, captured_at, text, masked, "
            "_dlt_load_id, _dlt_id) values "
            f"('{path(50)}', {MIDNIGHT + 50}, to_timestamp({MIDNIGHT + 50}), 'Late', 0, "
            "'9999999999.000001', 'invented-row-1')"
        )
        self.query(
            f"insert into {dataset}.app_usage (ts, bundle_id, observed_at, "
            f"_dlt_load_id, _dlt_id) values ({MIDNIGHT + 50}, '{TERMINAL}', "
            f"to_timestamp({MIDNIGHT + 50}), '9999999999.000001', 'invented-row-2')"
        )
        plan = analysis_plan(miroir.analyses[0])
        build_connection_models(plan)
        self.assertEqual(self.sessions(schema)[1][3:], (at(40), 4))
        self.query(
            f"insert into {dataset}._dlt_loads (load_id, schema_name, status, "
            "inserted_at) values ('9999999999.000001', 'screenmemory', 0, now())"
        )
        build_connection_models(plan)
        self.assertEqual(self.sessions(schema)[1][3:], (at(50), 5))

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
            build_connection_models(analysis_plan(analysis))
        self.assertEqual(
            self.snapshot(self.schema(miroir, "fresh")), self.snapshot(schema)
        )
        screen_only = self.schema(miroir, "screen_only")
        browser_only = self.schema(miroir, "browser_only")
        self.assertEqual(
            self.query(f"select source, count(*) from {screen_only}.events group by 1"),
            [("screen", 9)],
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
        build_connection_models(analysis_plan(utc))
        self.assertEqual(
            self.query(
                "select source, day::text, events, frames, timezone from "
                f"{self.schema(miroir, 'fresh')}.days order by source, day"
            ),
            [
                ("browser", "2026-11-20", 2, None, "UTC"),
                ("screen", "2026-11-20", 9, 20, "UTC"),
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
        # Unsetting the role revokes what an earlier build granted.
        miroir = configuration([timeline("timeline", both)])
        build_connection_models(analysis_plan(miroir.analyses[0]))
        with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
            self.query(f"select 1 from {schema}.events", READER)
        self.assertTrue(
            {
                "miroir_frames_dlt_load_id",
                "miroir_frames_ts",
                "miroir_frames_image_path",
                "miroir__dlt_loads_load_id",
            }
            <= {
                row[0]
                for row in self.query(
                    f"select indexname from pg_indexes where schemaname = '{dataset}'"
                )
            }
        )
