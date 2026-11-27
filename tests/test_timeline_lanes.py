"""A source joins the timeline with one model and one lane (package split,
step 11): nothing of the viewer changes.

A made-up source, walks, is declared here only: an input role of the timeline
package (linking a ListenBrainz connection, whose data it does not read), a
tiny model emitting `events` rows from values, written into a copy of the dbt
project, and a `Lane`. The build reads it through the lane variable; the Day
view and the API carry its lane, marks, day totals and search from `events`
alone. Needs Postgres (MIROIR_TEST_POSTGRES_BIN, see tests/postgres.py).
"""

import json
import re
import shutil
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.analyses import views
from src.analyses.catalog import PACKAGES, InputSpec
from src.analyses.views import LANES, Lane
from src.cli.compose import web_app
from src.control.analysis import analysis_plan
from src.control.config import parse_miroir
from src.control.service import InlineExecutor, MiroirService
from src.flows import transform
from src.viewer.database import TimelineDatabase
from tests.test_timeline_models import (
    READER,
    SAFARI,
    TimelineBuildFixture,
    browser_batch,
    place,
    screen,
    screen_batch,
    visit,
)
from tests.test_web import BEARER, TOKEN
from tests.test_web_static import INLINE

ROOT = Path(__file__).resolve().parents[1]
WALKS = Lane(
    "walks",
    "walks",
    "Walks",
    "group",
    units=("walk", "walks"),
    search_in="in walks",
)
# Local times on 2026-11-20 in America/Toronto (UTC-5).
MODEL = """{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='table',
    grants={'select': []},
) }}
-- A made-up source of the lane tests: walks, two on one day, one the next.
select
    'walks'::text as source,
    {{ timeline_connection('walks') }} as connection,
    'walks:' || w.id as event_id,
    w.started_at,
    w.started_at + interval '40 minutes' as ended_at,
    'walk'::text as kind,
    w.title,
    w.body,
    null::text as media_kind,
    null::text as media_key,
    null::text as media_status,
    null::boolean as thumbnail_w360,
    null::boolean as thumbnail_w1280,
    null::boolean as media_withheld,
    w.route as group_key,
    jsonb_build_object('km', w.km) as payload,
    false as deleted,
    null::timestamptz[] as previous_started_at,
    {{ timeline_literal(timeline_input('walks')) }} as _input,
    {{ timeline_literal(timeline_new_stamp()) }} as _stamp
from (values
    (1, '2026-11-20T12:00:00Z'::timestamptz, 'Riverside loop', 'Along the canal', 'river', 4.2),
    (2, '2026-11-20T21:30:00Z'::timestamptz, 'Evening errand', 'To the bakery', 'town', 1.1),
    (3, '2026-11-21T13:00:00Z'::timestamptz, 'Hill climb', 'Up the lookout', 'hill', 6.0)
) w (id, started_at, title, body, route, km)
"""


class FourthLaneTests(TimelineBuildFixture):
    def setUp(self):
        super().setUp()
        self.query(f"alter role {READER} login")
        dbt = self.root / f"dbt-{self._testMethodName}"
        shutil.copytree(
            transform.DBT_DIR,
            dbt,
            ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"),
        )
        (dbt / "models" / "timeline" / "timeline_walks.sql").write_text(MODEL)
        package = PACKAGES["timeline"]
        walking = InputSpec("Walks", ("listenbrainz",), "listenbrainz", False)
        for patcher in (
            patch.object(transform, "DBT_DIR", dbt),
            patch.dict(
                PACKAGES,
                {
                    "timeline": replace(
                        package, inputs={**package.inputs, "walks": walking}
                    )
                },
            ),
            patch.dict(LANES, {"timeline": (*LANES["timeline"], WALKS)}),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.raw = {
            "schema_version": 1,
            "id": "timeline_lanes",
            "connections": [
                {"id": "screen", "integration": "screenmemory", "settings": {}},
                {"id": "browser", "integration": "browser_history", "settings": {}},
                {
                    "id": "walker",
                    "integration": "listenbrainz",
                    "settings": {
                        "username": "walker",
                        "start_date": "2026-01-01T00:00:00Z",
                    },
                    # Never read: the walks model reads no raw data.
                    "credentials": {
                        "access_token": {"provider": "env", "key": "UNUSED"}
                    },
                },
            ],
            "analyses": [
                {
                    "id": "t",
                    "package": "timeline",
                    "inputs": {
                        "screen": "screen",
                        "browser": "browser",
                        "walks": "walker",
                    },
                    "timezone": "America/Toronto",
                    "parameters": {"reader_role": READER},
                }
            ],
        }
        self.miroir = parse_miroir(json.dumps(self.raw))
        self.send(self.miroir, "screen", screen_batch(*screen(0, SAFARI)))
        self.send(
            self.miroir,
            "browser",
            browser_batch(
                place(1, "https://example.org/a", "A walk map"), visit(1, 1, 600, 1)
            ),
            "firefox_places_batch",
        )
        config = self.root / f"lanes-{self._testMethodName}.json"
        config.write_text(json.dumps(self.raw))
        service = MiroirService(
            config, self.store, self.root / "artifacts", InlineExecutor(self.fail)
        )
        database = TimelineDatabase(
            f"postgresql://{READER}@127.0.0.1:{self.port}/postgres"
        )
        self.addCleanup(database.close)
        self.client = TestClient(web_app(service, TOKEN, timeline=database))

    def get(self, url):
        response = self.client.get(url, headers=BEARER, follow_redirects=False)
        self.assertEqual(response.status_code, 200, response.text[:2000])
        return response

    def test_a_declared_lane_reaches_the_build_and_every_page(self):
        plan = analysis_plan(self.miroir.analyses[0])
        self.assertIn(
            ["walks", "walks"],
            [
                [lane["role"], lane["source"]]
                for lane in plan.variables["timeline_lanes"]
            ],
        )
        schema = plan.relations[0].split(".")[0]
        ((lanes,),) = self.query(f"select lanes from {schema}.contract", READER)
        self.assertEqual([lane["source"] for lane in lanes][-1], "walks")

        # The API: its source, its days and its search.
        days = self.get("/api/timeline/days?from=2026-11-20&to=2026-11-21").json()
        self.assertEqual(
            [(d["day"], d.get("walks")) for d in days["days"]],
            [("2026-11-20", {"events": 2}), ("2026-11-21", {"events": 1})],
        )
        events = self.get(
            "/api/timeline/events?from=2026-11-20&to=2026-11-20&source=walks"
        ).json()["events"]
        self.assertEqual(
            [(e["title"], e["group"], e["payload"]) for e in events],
            [
                ("Riverside loop", "river", {"km": 4.2}),
                ("Evening errand", "town", {"km": 1.1}),
            ],
        )
        # Not read where no source is asked for: the lanes shown always are.
        default = self.get("/api/timeline/events?from=2026-11-20&to=2026-11-21")
        self.assertNotIn("walks", {e["source"] for e in default.json()["events"]})
        found = self.client.post(
            "/api/timeline/search",
            json={"q": "canal", "kind": "walks"},
            headers=BEARER,
        ).json()
        self.assertEqual(found["kind"], "walks")
        self.assertEqual([v["title"] for v in found["visits"]], ["Riverside loop"])

        # The Day view: its lane, its marks' colouring, its total, its list.
        day = self.get("/timeline/2026-11-20").text
        self.assertIsNone(INLINE.search(day))
        self.assertIn(
            '<canvas id="lane-walks" tabindex="0" aria-labelledby="lane-walks-name"'
            ' aria-describedby="lane-help" data-lane="walks" data-plugin=""'
            ' data-colour="group">',
            day,
        )
        self.assertIn('<span class="lane-name" id="lane-walks-name">Walks</span>', day)
        self.assertIn("1 screen frame, 1 browser visit, 2 walks. Times in", day)
        self.assertIn('<h2 id="near-walks-heading">Walks around this time</h2>', day)
        self.assertIn('<ul id="walks-near" class="visits"></ul>', day)
        self.assertIn('<div class="near-pair">', day)
        # The calendar counts it: a frame, a visit and two walks.
        self.assertIn('title="2026-11-20: 4"', day)
        self.assertIn('title="2026-11-21: 1"', day)
        self.assertIn('<input type="radio" name="kind" value="walks" > Walks', day)

        # Search: a kind of its own, its words, its results.
        form = self.get("/timeline/search").text
        self.assertIn('placeholder="Words on screen, in page titles or in walks"', form)
        results = self.client.post(
            "/timeline/search", data={"q": "bakery", "kind": "walks"}, headers=BEARER
        )
        self.assertEqual(results.status_code, 200)
        self.assertIn("Evening errand", results.text)
        self.assertIn(
            '<input type="radio" name="kind" value="walks" checked>', results.text
        )
        nothing = self.client.post(
            "/timeline/search", data={"q": "nowhere", "kind": "walks"}, headers=BEARER
        )
        self.assertIn("Nothing found in walks.", nothing.text)


class ViewerSourceTests(unittest.TestCase):
    def test_the_viewer_names_no_source_or_media_kind(self):
        # What the lane test relies on (step 11): nothing in the viewer names
        # walks, the screen, the browser or a media kind. `messages` and
        # `attachments` stay only as the thread's field names, DOM ids and
        # table, never to choose what a source does.
        named = re.compile(r"[\"'](screen|browser|walks|screenshots)[\"']")
        for path in sorted((ROOT / "src" / "viewer").rglob("*")):
            if path.suffix in (".py", ".js", ".html"):
                with self.subTest(path=path.name):
                    text = path.read_text()
                    self.assertNotIn("walks", text)
                    self.assertEqual(named.findall(text), [])
        self.assertIs(views.LANES, LANES)
