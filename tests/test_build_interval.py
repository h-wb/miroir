"""A package's minimum interval between builds (decision 040).

Loads run in-process against DuckDB and SQLite, with ListenBrainz intercepted
(see WebFixture). Time is moved by shifting what the store recorded back.
"""

import json
from dataclasses import replace
from datetime import datetime, timedelta

import dlt
import sqlalchemy as sa

from src.core.config import load_miroir
from src.core.operations import attempts, operations
from src.core.runner import (
    Execution,
    _deferred,
    _new_operation,
    execute_operation,
    request_models,
)
from src.core.service import InlineExecutor
from tests.test_web import WebFixture

INTERVAL = timedelta(minutes=5)


class BuildIntervalTests(WebFixture):
    def setUp(self):
        super().setUp()
        self.intervals = {"music": INTERVAL}
        self.built: list[str] = []
        self.scheduled: list[tuple[str, datetime]] = []
        self.defer = self.schedule
        self.service.executor = InlineExecutor(self.run_operation)

    def miroir(self):
        miroir = load_miroir(self.config_path)
        return replace(
            miroir,
            analyses=tuple(
                replace(
                    a,
                    package=replace(
                        a.package, min_build_interval=self.intervals.get(a.id)
                    ),
                )
                for a in miroir.analyses
            ),
        )

    def schedule(self, operation_id, start_at):
        # As the flow does: the scheduled build is bound to its own run.
        self.scheduled.append((operation_id, start_at))
        self.store.bind_backend_run(operation_id, f"run-{operation_id}")

    def retime(self, timezone):
        self.service.update_analysis(
            "music", {"timezone": timezone}, self.service._raw_config()[1]
        )

    def run_operation(self, operation_id, reload=None):
        return execute_operation(
            self.miroir(),
            operation_id,
            store=self.store,
            destination=dlt.destinations.duckdb(
                credentials=str(self.root / "w.duckdb")
            ),
            resolve_credentials=lambda c: {"access_token": "fake"},
            pipelines_dir=str(self.root / "state"),
            transform=lambda plan: self.built.append(plan.title),
            defer=self.defer,
            reload=reload,
        )

    def add_crossovers(self):
        self.service.create_analysis(
            "combined",
            "crossovers",
            {"health": "health", "music": "music"},
            self.service._raw_config()[1],
        )

    def load(self):
        operation_id = self.service.request_sync("music")
        self.assertEqual(
            self.store.attempts(operation_id)[-1]["ingestion"], "succeeded"
        )
        return operation_id

    def unstarted(self):
        return self.store.unstarted_builds("personal", "music")

    def shift(self, by):
        """Move everything recorded so far `by` into the past."""
        with self.store.engine.begin() as db:
            for row in db.execute(sa.select(attempts.c.id, attempts.c.started_at)):
                db.execute(
                    attempts.update()
                    .where(attempts.c.id == row.id)
                    .values(started_at=row.started_at - by)
                )
            for row in db.execute(sa.select(operations.c.id, operations.c.options)):
                options = json.loads(row.options or "{}")
                if "not_before" in options:
                    due = datetime.fromisoformat(options["not_before"]) - by
                    options["not_before"] = due.isoformat()
                    db.execute(
                        operations.update()
                        .where(operations.c.id == row.id)
                        .values(options=json.dumps(options))
                    )

    def test_loads_inside_the_interval_coalesce_into_one_build_at_its_end(self):
        self.load()
        self.assertEqual(self.built, ["Music analysis"])
        first = self.store.last_build_started("personal", "music")
        second = self.load()
        third = self.load()
        self.assertEqual(self.built, ["Music analysis"])
        self.assertEqual(len(self.scheduled), 1)
        scheduled, due = self.scheduled[0]
        self.assertEqual(due, first + INTERVAL)
        self.assertEqual([o.id for o in self.unstarted()], [scheduled])
        build = self.store.get_operation(scheduled)
        self.assertEqual(
            (build.kind, build.analysis_id, build.parent_operation_id),
            ("models", "music", second),
        )
        for load in (second, third):
            attempt = self.store.attempts(load)[-1]
            self.assertEqual(attempt["transformation"], "skipped")
            self.assertEqual(
                attempt["transformation_detail"], "Analyses: music deferred"
            )
        self.assertEqual(self.service.analysis("music")["status"]["state"], "running")

        # The scheduled run at the interval's end builds once, for all of them.
        self.run_operation(scheduled)
        self.assertEqual(self.built, ["Music analysis"] * 2)
        self.assertEqual(self.unstarted(), [])
        self.assertEqual(self.service.analysis("music")["status"]["state"], "current")

    def test_a_load_after_the_interval_builds_again(self):
        self.load()
        self.shift(INTERVAL + timedelta(seconds=1))
        self.load()
        self.assertEqual(self.built, ["Music analysis"] * 2)
        self.assertEqual(self.scheduled, [])
        # A load inside the next interval is deferred again, from that build.
        self.load()
        self.assertEqual(len(self.scheduled), 1)
        self.assertEqual(
            self.scheduled[0][1],
            self.store.last_build_started("personal", "music") + INTERVAL,
        )

    def test_packages_without_an_interval_build_after_every_load(self):
        self.add_crossovers()
        for _ in range(3):
            load = self.load()
            self.assertEqual(
                self.store.attempts(load)[-1]["transformation"], "succeeded"
            )
        self.assertEqual(self.built.count("Across sources"), 3)
        self.assertEqual(self.built.count("Music analysis"), 1)
        self.assertEqual(
            self.store.attempts(load)[-1]["transformation_detail"],
            "Analyses: music deferred, combined succeeded",
        )
        self.assertEqual(len(self.scheduled), 1)

        self.intervals = {}
        self.built.clear()
        for _ in range(2):
            self.load()
        self.assertEqual(self.built, ["Music analysis", "Across sources"] * 2)
        self.assertEqual(len(self.scheduled), 1)

    def test_without_a_way_to_schedule_every_load_builds(self):
        self.defer = None
        for _ in range(3):
            self.load()
        self.assertEqual(self.built, ["Music analysis"] * 3)
        self.assertEqual(self.unstarted(), [])

    def test_a_build_that_cannot_be_scheduled_is_built_with_the_load(self):
        def refuse(operation_id, start_at):
            self.scheduled.append((operation_id, start_at))
            raise RuntimeError("synthetic orchestrator outage")

        self.defer = refuse
        self.load()
        with self.assertLogs("src.core.runner", "WARNING"):
            load = self.load()
        self.assertEqual(self.built, ["Music analysis"] * 2)
        self.assertEqual(self.store.attempts(load)[-1]["transformation"], "succeeded")
        self.assertEqual(self.unstarted(), [])
        blocked = self.store.attempts(self.scheduled[0][0])
        self.assertEqual(
            [(a["ingestion"], a["error_type"]) for a in blocked],
            [("blocked", "NotScheduled")],
        )
        # The failed schedule is not a build: the next load tries to defer again.
        with self.assertLogs("src.core.runner", "WARNING"):
            self.load()
        self.assertEqual(len(self.scheduled), 2)

    def test_a_due_build_still_starting_is_joined_and_a_stale_one_is_not(self):
        self.load()
        self.load()
        self.assertEqual(len(self.scheduled), 1)
        # Due half a minute ago and not started: its run may be starting.
        self.shift(INTERVAL + timedelta(seconds=30))
        self.load()
        self.assertEqual(self.built, ["Music analysis"])
        self.assertEqual(len(self.scheduled), 1)
        # Due longer than an interval ago: it may never run, so the load builds.
        self.shift(INTERVAL)
        self.load()
        self.assertEqual(self.built, ["Music analysis"] * 2)

    def test_a_build_planned_for_other_settings_is_not_joined(self):
        self.load()
        self.load()
        stale = self.unstarted()[0].id
        with self.store.engine.begin() as db:
            options = json.loads(
                db.execute(
                    sa.select(operations.c.options).where(operations.c.id == stale)
                ).scalar_one()
            )
            options["analysis_receipt"] = "Music analysis v0 (older)"
            db.execute(
                operations.update()
                .where(operations.c.id == stale)
                .values(options=json.dumps(options))
            )
        self.load()
        self.assertEqual(len(self.scheduled), 2)
        self.assertEqual(
            {o.id for o in self.unstarted()}, {stale, self.scheduled[1][0]}
        )
        # Nor one requested under an older revision of its connection: it
        # would be blocked when it starts.
        outdated = self.scheduled[1][0]
        with self.store.engine.begin() as db:
            db.execute(
                operations.update()
                .where(operations.c.id == outdated)
                .values(config_revision="older")
            )
        self.load()
        self.assertEqual(len(self.scheduled), 3)
        self.assertEqual(
            {o.id for o in self.unstarted()}, {stale, outdated, self.scheduled[2][0]}
        )

    def test_a_build_with_no_run_of_its_own_is_not_joined(self):
        # Recorded, but the worker stopped before a flow run was bound to it.
        self.defer = lambda operation_id, start_at: self.scheduled.append(
            (operation_id, start_at)
        )
        self.load()
        self.load()
        self.assertEqual(len(self.scheduled), 1)
        self.load()
        self.assertEqual(len(self.scheduled), 2)
        self.assertEqual(
            {o.id for o in self.unstarted()}, {s for s, _ in self.scheduled}
        )

    def test_a_build_due_now_is_registered_before_the_next_load_decides(self):
        miroir = self.miroir()
        connection = next(c for c in miroir.connections if c.config.id == "music")
        analysis = next(a for a in miroir.analyses if a.id == "music")
        run = Execution(self.store, None, lambda c: {}, defer=self.defer)
        loads = [_new_operation(self.store, miroir, connection, "local") for _ in "ab"]
        # Both loads decide before either builds: the first is registered and
        # started under the lock, so the second defers to the interval's end.
        first = _deferred(run, loads[0], miroir, connection, analysis)
        self.assertIsInstance(first, tuple)
        self.assertEqual(self.store.attempts(first[0].id)[0]["ingestion"], "not_run")
        self.assertIs(_deferred(run, loads[1], miroir, connection, analysis), True)
        self.assertEqual(len(self.scheduled), 1)

    def requested(self, **options):
        """A rebuild recorded but not run, and the configuration it was read with."""
        miroir = self.miroir()
        operation = request_models(
            miroir, "music", store=self.store, origin="local", **options
        )
        return miroir, operation.id

    def execute(self, miroir, operation_id, transform):
        return execute_operation(
            miroir,
            operation_id,
            store=self.store,
            destination=None,
            resolve_credentials=lambda c: {},
            transform=transform,
            reload=self.miroir,
        )

    def test_a_build_whose_settings_changed_while_it_waited_is_blocked(self):
        self.load()
        built = []
        # Read before it waited for the connection lock; saved meanwhile.
        miroir, rebuild = self.requested()
        self.retime("Europe/Paris")
        self.execute(miroir, rebuild, built.append)
        self.assertEqual(built, [])
        attempt = self.store.attempts(rebuild)[-1]
        self.assertEqual(
            (attempt["ingestion"], attempt["transformation"], attempt["error_type"]),
            ("blocked", "not_run", "Blocked"),
        )
        self.assertIn("Analysis settings changed", attempt["error_message"])

        # The same check runs again once the build holds the dbt slot.
        miroir, rebuild = self.requested()

        def transform(plan):
            self.retime("Asia/Tokyo")  # saved while it waited for the dbt slot
            plan.ready()
            built.append(plan)

        self.execute(miroir, rebuild, transform)
        self.assertEqual(built, [])
        self.assertEqual(self.store.attempts(rebuild)[-1]["ingestion"], "blocked")

        # Unchanged, it builds, and a requested full refresh reaches the plan.
        miroir, rebuild = self.requested(full_refresh=True)
        self.execute(miroir, rebuild, built.append)
        self.assertEqual([plan.full_refresh for plan in built], [True])
        self.assertTrue(
            json.loads(self.store.get_operation(rebuild).options)["full_refresh"]
        )
        self.assertEqual(
            self.store.attempts(rebuild)[-1]["transformation"], "succeeded"
        )
