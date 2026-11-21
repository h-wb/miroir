"""The timeline's read-only warehouse connection (decision 040).

The database tests start a disposable Postgres (see tests/postgres.py) and create
synthetic tables there; no real warehouse is used.
"""

import getpass
import os
import time
import unittest
from unittest.mock import patch

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from src.core import timeline
from src.core.config import ConfigurationError
from src.core.timeline import (
    NOT_CONFIGURED,
    STATEMENT_TIMEOUT_MS,
    TimelineDatabase,
    timeline_database_url,
)
from tests.postgres import start_postgres


class TimelineSettingTests(unittest.TestCase):
    def test_url_wins_then_parts_compose_and_partial_parts_are_absent(self):
        parts = {
            "MIROIR_TIMELINE_DATABASE__HOST": "warehouse.test",
            "MIROIR_TIMELINE_DATABASE__USERNAME": "reader",
            "MIROIR_TIMELINE_DATABASE__PASSWORD": "p@ss word+",
        }
        with patch.dict(os.environ, parts, clear=True):
            self.assertEqual(
                timeline_database_url(),
                "postgresql+psycopg2://reader:p%40ss%20word%2B@warehouse.test:5432/reader",
            )
        with patch.dict(
            os.environ,
            {
                **parts,
                "MIROIR_TIMELINE_DATABASE__PORT": "6543",
                "MIROIR_TIMELINE_DATABASE__DATABASE": "warehouse",
            },
            clear=True,
        ):
            self.assertTrue(timeline_database_url().endswith(":6543/warehouse"))
        with patch.dict(
            os.environ,
            {**parts, "MIROIR_TIMELINE_DATABASE_URL": "postgresql://x@y/z"},
            clear=True,
        ):
            self.assertEqual(timeline_database_url(), "postgresql://x@y/z")
        for missing in parts:
            partial = {k: v for k, v in parts.items() if k != missing}
            with patch.dict(os.environ, partial, clear=True):
                self.assertIsNone(timeline_database_url())

    def test_only_postgres_urls_are_accepted_without_echoing_them(self):
        for url in (
            "sqlite:///SECRET.db",
            "mysql://u:SECRET@h/d",
            "postgresql+psycopg://u:SECRET@h/d",
            "not a url SECRET",
            "postgresql://u:SECRET@h:port/d",
        ):
            with self.subTest(url=url), self.assertRaises(ConfigurationError) as caught:
                TimelineDatabase(url)
            self.assertNotIn("SECRET", str(caught.exception))
        database = TimelineDatabase("postgresql://reader@warehouse.test/warehouse")
        self.addCleanup(database.close)
        self.assertEqual(database.engine.url.drivername, "postgresql+psycopg2")

    def test_status_without_a_setting_says_so(self):
        self.assertEqual(
            timeline.status(None),
            {"configured": False, "ready": False, "message": NOT_CONFIGURED},
        )

    def test_an_unreachable_database_is_a_fixed_message(self):
        # Nothing listens on port 1; the URL's password must not reach the answer.
        database = TimelineDatabase("postgresql://reader:SECRET@127.0.0.1:1/warehouse")
        self.addCleanup(database.close)
        with self.assertLogs("src.core.timeline", "WARNING") as logs:
            state = timeline.status(database)
        self.assertEqual(
            state,
            {
                "configured": True,
                "ready": False,
                "message": "The timeline database is unavailable.",
            },
        )
        self.assertNotIn("SECRET", "".join(logs.output))
        self.assertNotIn("127.0.0.1", "".join(logs.output))


class TimelinePostgresTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root, cls.port = start_postgres(cls, "miroir-timeline-")
        owner = sa.create_engine(
            f"postgresql+psycopg2://{getpass.getuser()}@127.0.0.1:{cls.port}/postgres"
        )
        cls.addClassCleanup(owner.dispose)
        with owner.begin() as db:
            # The role as the installation is expected to create it, minus the
            # role-level time limit: the server must bound sessions on its own.
            db.execute(sa.text("CREATE ROLE timeline_reader LOGIN"))
            db.execute(sa.text("CREATE SCHEMA timeline"))
            db.execute(sa.text("CREATE TABLE timeline.frames (id int, text text)"))
            db.execute(sa.text("INSERT INTO timeline.frames VALUES (1, 'synthetic')"))
            db.execute(sa.text("GRANT USAGE ON SCHEMA timeline TO timeline_reader"))
            db.execute(sa.text("GRANT SELECT ON timeline.frames TO timeline_reader"))
            db.execute(sa.text("CREATE TABLE public.private_rows (id int)"))
            # Even a role that could write must not, through the server.
            db.execute(sa.text("CREATE ROLE careless_reader LOGIN"))
            db.execute(sa.text("GRANT ALL ON SCHEMA timeline TO careless_reader"))
            db.execute(sa.text("GRANT ALL ON timeline.frames TO careless_reader"))

    def database(self, role="timeline_reader", query=""):
        database = TimelineDatabase(
            f"postgresql://{role}@127.0.0.1:{self.port}/postgres{query}"
        )
        self.addCleanup(database.close)
        return database

    def test_health_reports_a_read_only_time_limited_session(self):
        state = timeline.status(self.database())
        self.assertEqual(
            state,
            {
                "configured": True,
                "ready": True,
                "message": "The timeline database is reachable and read-only.",
                "read_only": True,
                "statement_timeout": f"{STATEMENT_TIMEOUT_MS // 1000}s",
            },
        )

    def test_sessions_read_only_what_the_role_grants_and_never_write(self):
        for role in ("timeline_reader", "careless_reader"):
            with self.subTest(role=role), self.database(role).engine.connect() as db:
                self.assertEqual(
                    db.execute(sa.text("SELECT text FROM timeline.frames")).scalar(),
                    "synthetic",
                )
                with self.assertRaises(DBAPIError) as caught:
                    db.execute(sa.text("INSERT INTO timeline.frames VALUES (2, 'x')"))
                self.assertIn("read-only transaction", str(caught.exception))
                db.rollback()
        with self.database().engine.connect() as db, self.assertRaises(DBAPIError):
            db.execute(sa.text("SELECT * FROM public.private_rows"))

    def test_the_time_limit_holds_and_the_url_cannot_lift_it(self):
        lifted = "?options=-c%20statement_timeout%3D0"
        for database in (self.database(), self.database(query=lifted)):
            with database.engine.connect() as db:
                begun = time.monotonic()
                with self.assertRaises(DBAPIError) as caught:
                    db.execute(sa.text("SELECT pg_sleep(30)"))
                self.assertLess(time.monotonic() - begun, 15)
                self.assertIn("statement timeout", str(caught.exception))
