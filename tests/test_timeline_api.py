"""The timeline's queries and JSON API (decision 040, phase 1c).

Synthetic ScreenMemory and browser batches are loaded and built by the real
models in a disposable Postgres (tests/postgres.py); the server reads them as the
reader role, through its read-only, time-limited sessions.
"""

import getpass
import gzip
import json
from contextlib import closing
from datetime import datetime, timezone
from unittest.mock import patch

import psycopg2
from fastapi.testclient import TestClient

from src.core.analysis import analysis_plan
from src.core.config import parse_miroir
from src.core.operations import OperationError
from src.core.service import InlineExecutor, MiroirService
from src.core.timeline import (
    MEDIA_NO_THUMBNAILS,
    MEDIA_THUMBNAILS,
    MEDIA_UNKNOWN,
    MEDIA_WITHHELD,
    NO_ANALYSIS,
    NOT_BUILT,
    TimelineDatabase,
    TimelineReader,
    TimelineSource,
    TimelineTimeout,
    TimelineUnavailable,
    read_cursor,
    snippet,
    status,
    timeline_source,
)
from src.web.app import create_app
from tests.fakes import FakeObjectStore
from tests.test_timeline_models import (
    MIDNIGHT,
    NOTES,
    READER,
    SAFARI,
    TERMINAL,
    TimelineBuildFixture,
    browser_batch,
    frame,
    path,
    place,
    raw_configuration,
    screen_batch,
    timeline,
    usage,
    visit,
)
from tests.test_web import BEARER, TOKEN

MAIL = "com.apple.mail"
MIROIR_ID = "timeline_api"
BOTH = {"screen": "screen", "browser": "browser"}
SAME_A, SAME_B = "2026/11/20/same-a.jpeg", "2026/11/20/same-b.jpeg"
ROUTES = (
    "/api/timeline/days?from=2026-11-19&to=2026-11-21",
    "/api/timeline/screen/2026-11-20",
    f"/api/timeline/frames/{path(0)}",
    "/api/timeline/events?from=2026-11-19&to=2026-11-21",
    "/api/timeline/search?q=invoice",
)


def at(offset):
    return datetime.fromtimestamp(MIDNIGHT + offset, timezone.utc)


def text_frame(offset, text, bundle):
    return [frame(offset, text=text), usage(offset, bundle)]


class TimelineQueryTests(TimelineBuildFixture):
    built = False

    def setUp(self):
        super().setUp()
        self.query(f"alter role {READER} login")
        self.raw = raw_configuration([timeline("timeline", BOTH, READER)], MIROIR_ID)
        self.miroir = parse_miroir(json.dumps(self.raw))
        if not TimelineQueryTests.built:
            self.load()
            TimelineQueryTests.built = True
        self.source = timeline_source(self.miroir)
        self.database = self.reader_database()
        self.reader = TimelineReader(self.database, self.source)

    def reader_database(self, timeout=None):
        url = f"postgresql://{READER}@127.0.0.1:{self.port}/postgres"
        database = (
            TimelineDatabase(url)
            if timeout is None
            else TimelineDatabase(url, statement_timeout_ms=timeout)
        )
        self.addCleanup(database.close)
        return database

    def load(self):
        self.send(
            self.miroir,
            "screen",
            screen_batch(
                # The evening before, one moment of Safari over midnight.
                *text_frame(-10, "Evening invoice draft", SAFARI),
                *text_frame(0, "Morning invoice review", SAFARI),
                *text_frame(60, "invoice totals", SAFARI),
                # Another app in the same five minutes: another moment.
                *text_frame(120, "invoice script", TERMINAL),
                *text_frame(3600, "meeting notes", NOTES),
                frame(3700, withheld="excluded_app"),
                usage(3700, NOTES),
                # Two frames of one second.
                frame(7200, text="invoice paid") | {"image_path": SAME_A},
                frame(7200, text="invoice paid again") | {"image_path": SAME_B},
                usage(7200, MAIL),
                *text_frame(86400 + 100, "invoice next day", SAFARI),
            ),
        )
        self.send(
            self.miroir,
            "browser",
            browser_batch(
                place(1, "https://billing.example.com/invoices", "Invoice portal"),
                place(2, "https://weather.example.org/", "Weather"),
                # Five visits of one microsecond: keyset paging meets ties.
                *(visit(n, 1, 500, 1) for n in range(1, 6)),
                visit(6, 2, 900, 1),
                visit(7, 1, -100, 1),
            ),
            "firefox_places_batch",
        )
        self.media_record(self.miroir, 0)
        self.media_record(self.miroir, 60, "unreadable")
        self.build(analysis_plan(self.miroir.analyses[0]))

    # ---- finding the tables -------------------------------------------------

    def test_the_first_timeline_analysis_is_read_and_others_are_named(self):
        self.assertEqual(
            self.source.schema,
            analysis_plan(self.miroir.analyses[0]).relations[0][:-14],
        )
        self.assertEqual(self.source.timezone, "America/Toronto")
        self.assertNotIn("warning", self.source.describe())
        several = parse_miroir(
            json.dumps(
                raw_configuration(
                    [timeline("timeline", BOTH), timeline("second", BOTH)], MIROIR_ID
                )
            )
        )
        found = timeline_source(several)
        self.assertEqual((found.analysis_id, found.ignored), ("timeline", ("second",)))
        self.assertIn("only the first, timeline", found.describe()["warning"])
        none = parse_miroir(json.dumps(raw_configuration([], MIROIR_ID)))
        self.assertIsNone(timeline_source(none))
        self.assertEqual(
            status(self.database, none),
            {"configured": False, "ready": False, "message": NO_ANALYSIS},
        )
        state = status(self.database, self.miroir)
        self.assertTrue(state["ready"])
        self.assertEqual(state["analysis"], "timeline")

    def test_tables_not_built_yet_are_a_clear_error(self):
        missing = TimelineReader(
            self.database, TimelineSource("other", "miroir_not_built", "UTC")
        )
        with self.assertRaisesRegex(TimelineUnavailable, NOT_BUILT):
            missing.screen_day(datetime(2026, 11, 20).date())

    def test_a_query_past_the_time_limit_is_a_clear_error(self):
        reader = TimelineReader(self.reader_database(timeout=300), self.source)
        owner = psycopg2.connect(
            host="127.0.0.1", port=self.port, user=getpass.getuser(), dbname="postgres"
        )
        with closing(owner):
            with owner.cursor() as cursor:
                cursor.execute(
                    f'lock table "{self.source.schema}".screen_frames'
                    " in access exclusive mode"
                )
            with self.assertRaises(TimelineTimeout) as caught:
                reader.screen_day(datetime(2026, 11, 20).date())
            owner.rollback()
        self.assertIn("took longer than", str(caught.exception))

    # ---- days ---------------------------------------------------------------

    def test_days_count_each_source_per_local_day_within_bounds(self):
        days = self.reader.days(
            datetime(2026, 11, 19).date(), datetime(2026, 11, 21).date()
        )
        self.assertEqual(days["timezone"], "America/Toronto")
        self.assertEqual(
            days["days"],
            [
                {
                    "day": "2026-11-19",
                    "browser": {"events": 1},
                    "screen": {"events": 1, "frames": 1},
                },
                {
                    "day": "2026-11-20",
                    "browser": {"events": 6},
                    "screen": {"events": 3, "frames": 7},
                },
                {"day": "2026-11-21", "screen": {"events": 1, "frames": 1}},
            ],
        )
        one = self.reader.days(
            datetime(2026, 11, 21).date(), datetime(2026, 11, 21).date()
        )
        self.assertEqual([d["day"] for d in one["days"]], ["2026-11-21"])
        for start, end in (("2026-11-20", "2026-11-19"), ("2025-01-01", "2026-11-21")):
            with self.subTest(start=start), self.assertRaises(OperationError):
                self.reader.days(
                    datetime.fromisoformat(start).date(),
                    datetime.fromisoformat(end).date(),
                )

    # ---- a day's screen index -----------------------------------------------

    def test_a_days_index_is_columns_from_local_midnight(self):
        index = self.reader.screen_day(datetime(2026, 11, 20).date())
        self.assertEqual(index["start"], "2026-11-20T00:00:00-05:00")
        self.assertEqual(index["end"], "2026-11-21T00:00:00-05:00")
        self.assertEqual((index["count"], index["truncated"]), (7, False))
        self.assertEqual(index["connection"], "screen")
        frames = index["frames"]
        self.assertEqual(frames["t"], [0, 60, 120, 3600, 3700, 7200, 7200])
        self.assertEqual(
            frames["key"],
            [path(0), path(60), path(120), path(3600), path(3700), SAME_A, SAME_B],
        )
        apps = [index["apps"][i]["name"] for i in frames["app"]]
        self.assertEqual(
            apps, ["Safari", "Safari", "Terminal", "Notes", "Notes", "Mail", "Mail"]
        )
        self.assertEqual(index["apps"][0]["colour"], "#2196f3")
        self.assertEqual(
            frames["media"],
            [MEDIA_THUMBNAILS, MEDIA_NO_THUMBNAILS, MEDIA_UNKNOWN, MEDIA_UNKNOWN]
            + [MEDIA_WITHHELD, MEDIA_UNKNOWN, MEDIA_UNKNOWN],
        )
        # The frame ten seconds before midnight is the evening before's last.
        evening = self.reader.screen_day(datetime(2026, 11, 19).date())
        self.assertEqual(evening["frames"]["t"], [86390])
        self.assertEqual(evening["frames"]["key"], [path(-10)])
        empty = self.reader.screen_day(datetime(2026, 1, 1).date())
        self.assertEqual(
            (empty["count"], empty["apps"], empty["frames"]["t"]), (0, [], [])
        )
        with patch("src.core.timeline.MAX_DAY_FRAMES", 3):
            cut = self.reader.screen_day(datetime(2026, 11, 20).date())
        self.assertEqual(
            (cut["count"], cut["truncated"], len(cut["frames"]["key"])), (3, True, 3)
        )

    def test_a_frame_has_its_time_app_text_and_media(self):
        found = self.reader.frame(path(0))
        self.assertEqual(
            {k: v for k, v in found.items() if k not in ("analysis", "timezone")},
            {
                "key": path(0),
                "captured_at": "2026-11-20T00:00:00-05:00",
                "day": "2026-11-20",
                "t": 0,
                "app": {"bundle_id": SAFARI, "name": "Safari", "colour": "#2196f3"},
                "text": "Morning invoice review",
                "text_truncated": False,
                "withheld": None,
                "masked": 0,
                "media": {
                    "connection": "screen",
                    "kind": "screenshots",
                    "key": path(0),
                    "state": "thumbnails",
                    "status": "made",
                },
            },
        )
        withheld = self.reader.frame(path(3700))
        self.assertEqual(
            (withheld["text"], withheld["withheld"]), (None, "excluded_app")
        )
        self.assertEqual(withheld["media"]["state"], "withheld")
        self.assertEqual(self.reader.frame(path(-10))["t"], 86390)
        self.assertIsNone(self.reader.frame("2026/11/20/unknown.jpeg"))
        with patch("src.core.timeline.MAX_TEXT", 7):
            cut = self.reader.frame(path(0))
        self.assertEqual((cut["text"], cut["text_truncated"]), ("Morning", True))

    # ---- events -------------------------------------------------------------

    def test_events_page_by_keyset_without_gaps_or_repeats(self):
        day19, day21 = datetime(2026, 11, 19).date(), datetime(2026, 11, 21).date()
        whole = self.reader.events(day19, day21, ("screen", "browser"), 500, None)
        self.assertIsNone(whole["next"])
        keys = [(e["started_at"], e["event_id"]) for e in whole["events"]]
        self.assertEqual(len(keys), 12)  # 7 visits, 5 sessions
        self.assertEqual(
            keys, sorted(keys, key=lambda k: (datetime.fromisoformat(k[0]), k[1]))
        )
        for size in (1, 2, 3, 5):
            with self.subTest(size=size):
                pages, after = [], None
                while True:
                    page = self.reader.events(
                        day19, day21, ("screen", "browser"), size, after
                    )
                    pages += [(e["started_at"], e["event_id"]) for e in page["events"]]
                    if page["next"] is None:
                        break
                    stamp, event_id = read_cursor(page["next"], (str, str))
                    after = (datetime.fromisoformat(stamp), event_id)
                self.assertEqual(pages, keys)
        session = whole["events"][1]
        self.assertEqual(session["event_id"], f"screen:{path(-10)}")
        self.assertEqual(session["started_at"], "2026-11-19T23:59:50-05:00")
        self.assertEqual(session["ended_at"], "2026-11-20T00:01:00-05:00")
        self.assertEqual(session["title"], "Safari")
        self.assertEqual(session["payload"]["frames"], 3)
        self.assertEqual(session["media"]["state"], "unknown")
        visits = self.reader.events(at(0), at(1000), ("browser",), 500, None)
        self.assertEqual(
            [e["event_id"] for e in visits["events"]],
            [f"browser:{n}" for n in range(1, 7)],
        )
        self.assertEqual(
            visits["events"][0]["body"], "https://billing.example.com/invoices"
        )
        self.assertIsNone(visits["events"][0]["media"])
        sessions = self.reader.events(at(0), at(200), ("screen",), 500, None)
        self.assertEqual(
            [e["event_id"] for e in sessions["events"]], [f"screen:{path(120)}"]
        )
        with self.assertRaises(OperationError):
            self.reader.events(at(10), at(0), ("screen",), 10, None)

    # ---- search -------------------------------------------------------------

    def test_screen_search_groups_moments_newest_first_and_pages(self):
        found = self.reader.search_screen("invoice", None, None, None, 50, None)
        moments = [
            (m["key"], found["apps"][m["app"]]["name"], m["matches"])
            for m in found["moments"]
        ]
        self.assertEqual(
            moments,
            [
                (path(86500), "Safari", 1),
                (SAME_A, "Mail", 2),
                (path(0), "Safari", 2),
                (path(120), "Terminal", 1),
                (path(-10), "Safari", 1),
            ],
        )
        safari = found["moments"][2]
        self.assertEqual(safari["snippet"], ["Morning ", "invoice", " review"])
        self.assertEqual(
            (safari["at"], safari["until"], safari["day"]),
            ("2026-11-20T00:00:00-05:00", "2026-11-20T00:01:00-05:00", "2026-11-20"),
        )
        self.assertEqual(safari["media"], MEDIA_THUMBNAILS)
        self.assertIsNone(found["next"])
        pages, before = [], None
        while True:
            page = self.reader.search_screen("invoice", None, None, None, 2, before)
            pages += [m["key"] for m in page["moments"]]
            if page["next"] is None:
                break
            before = tuple(read_cursor(page["next"], (int, str)))
        self.assertEqual(pages, [m[0] for m in moments])
        terminal = self.reader.search_screen("invoice", TERMINAL, None, None, 50, None)
        self.assertEqual([m["key"] for m in terminal["moments"]], [path(120)])
        day = datetime(2026, 11, 20).date()
        that_day = self.reader.search_screen("invoice", None, day, day, 50, None)
        self.assertEqual(
            [m["key"] for m in that_day["moments"]], [SAME_A, path(0), path(120)]
        )
        phrase = self.reader.search_screen('"invoice paid"', None, None, None, 50, None)
        self.assertEqual([m["matches"] for m in phrase["moments"]], [2])
        none = self.reader.search_screen("absent", None, None, None, 50, None)
        self.assertEqual((none["moments"], none["next"]), ([], None))
        for words in ("", "  ", "x" * 201):
            with self.subTest(words=words), self.assertRaises(OperationError):
                self.reader.search_screen(words, None, None, None, 20, None)

    def test_browser_search_finds_titles_newest_first(self):
        found = self.reader.search_browser("invoice", None, None, 50, None)
        self.assertEqual(
            [v["event_id"] for v in found["visits"]],
            [f"browser:{n}" for n in (5, 4, 3, 2, 1, 7)],
        )
        self.assertEqual(found["visits"][0]["snippet"], ["", "Invoice", " portal"])
        self.assertEqual(found["visits"][0]["site"], "billing.example.com")
        pages, before = [], None
        while True:
            page = self.reader.search_browser("invoice", None, None, 4, before)
            pages += [v["event_id"] for v in page["visits"]]
            if page["next"] is None:
                break
            stamp, event_id = read_cursor(page["next"], (str, str))
            before = (datetime.fromisoformat(stamp), event_id)
        self.assertEqual(pages, [v["event_id"] for v in found["visits"]])
        day = datetime(2026, 11, 19).date()
        evening = self.reader.search_browser("invoice", day, day, 50, None)
        self.assertEqual([v["event_id"] for v in evening["visits"]], ["browser:7"])

    def test_snippets_split_marks_and_cursors_refuse_forgeries(self):
        self.assertEqual(snippet("a b c"), ["a ", "b", " c"])
        self.assertEqual(snippet("b"), ["", "b"])
        self.assertEqual(snippet(None), [""])
        for cursor in ("", "%%%", "W10", "WyJhIl0", "WzEsMl0", "eyJhIjoxfQ"):
            with self.subTest(cursor=cursor), self.assertRaises(OperationError):
                read_cursor(cursor, (str, str))

    # ---- the API --------------------------------------------------------------

    def client(self, raw=None, database=True):
        self.clients = getattr(self, "clients", 0) + 1
        config = self.root / f"api-miroir-{self.clients}.json"
        config.write_text(
            json.dumps(
                {
                    **(raw or self.raw),
                    "media": {"endpoint": "https://s3.example.org", "bucket": "media"},
                }
            )
        )
        service = MiroirService(
            config,
            self.store,
            self.root / "artifacts",
            InlineExecutor(self.fail),
            object_store=FakeObjectStore(),
        )
        app = create_app(service, TOKEN, timeline=self.database if database else None)
        return TestClient(app)

    def test_every_route_needs_the_login(self):
        client = self.client()
        for route in (
            *ROUTES,
            "/api/timeline/health",
            f"/media/screen/screenshots/{path(0)}?size=360",
        ):
            # Pages outside /api send the browser to the login page.
            refused = 401 if route.startswith("/api/") else 303
            with self.subTest(route=route):
                self.assertEqual(
                    client.get(route, follow_redirects=False).status_code, refused
                )
                self.assertEqual(
                    client.get(
                        route,
                        headers={"Authorization": "Bearer wrong"},
                        follow_redirects=False,
                    ).status_code,
                    refused,
                )

    def test_routes_answer_json_that_is_not_stored(self):
        client = self.client()
        for route in ROUTES:
            with self.subTest(route=route):
                response = client.get(route, headers=BEARER)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.headers["content-type"], "application/json")
                expected = (
                    "private, no-cache" if "/screen/" in route else "private, no-store"
                )
                self.assertEqual(response.headers["cache-control"], expected)
        days = client.get(ROUTES[0], headers=BEARER).json()
        self.assertEqual(len(days["days"]), 3)
        events = client.get(
            "/api/timeline/events?from=2026-11-20T00:00:00%2B00:00"
            "&to=2026-11-20T05:05:00Z&source=browser&limit=2",
            headers=BEARER,
        ).json()
        self.assertEqual([e["event_id"] for e in events["events"]], ["browser:7"])
        unencoded = client.get(
            "/api/timeline/events?from=2026-11-20T00:00:00+00:00&to=2026-11-21",
            headers=BEARER,
        )
        self.assertEqual(unencoded.status_code, 200)
        page = client.get(
            "/api/timeline/events?from=2026-11-20&to=2026-11-20&limit=2", headers=BEARER
        ).json()
        rest = client.get(
            f"/api/timeline/events?from=2026-11-20&to=2026-11-20&limit=2&after={page['next']}",
            headers=BEARER,
        ).json()
        self.assertEqual(
            [e["event_id"] for e in page["events"] + rest["events"]],
            [f"screen:{path(120)}", "browser:1", "browser:2", "browser:3"],
        )
        browser = client.get(
            "/api/timeline/search?q=invoice&kind=browser&limit=1", headers=BEARER
        ).json()
        self.assertEqual(browser["visits"][0]["event_id"], "browser:5")
        self.assertIsNotNone(browser["next"])
        frame = client.get(f"/api/timeline/frames/{path(0)}", headers=BEARER).json()
        self.assertEqual(frame["text"], "Morning invoice review")

    def test_a_days_index_revalidates_by_etag_and_compresses(self):
        client = self.client()
        route = "/api/timeline/screen/2026-11-20"
        first = client.get(route, headers=BEARER)
        tag = first.headers["etag"]
        self.assertTrue(tag.startswith('W/"'))
        again = client.get(route, headers={**BEARER, "If-None-Match": tag})
        self.assertEqual(again.status_code, 304)
        self.assertEqual(again.content, b"")
        self.assertEqual(again.headers["etag"], tag)
        self.assertEqual(again.headers["cache-control"], "private, no-cache")
        other = client.get(route, headers={**BEARER, "If-None-Match": 'W/"other"'})
        self.assertEqual(other.status_code, 200)
        # Compressed for a client that accepts it; the tag is the content's.
        compress = patch("src.web.timeline.COMPRESS_FROM", 1)
        compress.start()
        self.addCleanup(compress.stop)
        raw = client.get(
            route,
            headers={**BEARER, "Accept-Encoding": "gzip"},
        )
        self.assertEqual(raw.headers["content-encoding"], "gzip")
        self.assertEqual(raw.headers["etag"], tag)
        self.assertEqual(raw.headers["vary"], "Accept-Encoding")
        plain = client.get(route, headers={**BEARER, "Accept-Encoding": "identity"})
        self.assertNotIn("content-encoding", plain.headers)
        self.assertEqual(plain.json(), raw.json())
        self.assertEqual(
            json.loads(gzip.decompress(gzip.compress(plain.content))), plain.json()
        )

    def test_parameters_are_validated(self):
        client = self.client()
        cases = (
            "/api/timeline/days?from=2026-11-19",
            "/api/timeline/days?from=2026-11-19&to=tomorrow",
            "/api/timeline/days?from=2026-11-19&to=2026-11-21&extra=1",
            "/api/timeline/days?from=2026-11-19&from=2026-11-18&to=2026-11-21",
            "/api/timeline/days?from=2024-01-01&to=2026-11-21",
            "/api/timeline/days?from=2026-02-30&to=2026-03-01",
            "/api/timeline/screen/20261120",
            "/api/timeline/screen/2026-11-20?size=1",
            "/api/timeline/events?from=2026-11-20",
            "/api/timeline/events?from=2026-11-20T00:00:00&to=2026-11-21",
            "/api/timeline/events?from=2026-11-20&to=2026-11-21&source=music",
            "/api/timeline/events?from=2026-11-20&to=2026-11-21&source=screen,screen",
            "/api/timeline/events?from=2026-11-20&to=2026-11-21&limit=0",
            "/api/timeline/events?from=2026-11-20&to=2026-11-21&limit=501",
            "/api/timeline/events?from=2026-11-20&to=2026-11-21&limit=1e3",
            "/api/timeline/events?from=2026-11-20&to=2026-11-21&after=forged",
            "/api/timeline/events?from=2026-11-21&to=2026-11-20T00:00:00Z",
            "/api/timeline/search",
            "/api/timeline/search?q=",
            "/api/timeline/search?q=invoice&kind=music",
            "/api/timeline/search?q=invoice&kind=browser&app=com.apple.Safari",
            "/api/timeline/search?q=invoice&before=WyJhIiwiYiJd",
            "/api/timeline/search?q=invoice&limit=51",
            "/api/timeline/search?q=invoice&from=2026-11-21&to=2026-11-20",
        )
        for route in cases:
            with self.subTest(route=route):
                response = client.get(route, headers=BEARER)
                self.assertEqual(response.status_code, 400, response.text)
                self.assertIn("error", response.json())
                self.assertEqual(response.headers["cache-control"], "private, no-store")
        unknown = client.get(
            "/api/timeline/frames/2026/11/20/unknown.jpeg", headers=BEARER
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.headers["cache-control"], "private, no-store")

    def test_without_an_analysis_or_a_database_every_route_says_so(self):
        for client, message in (
            (
                self.client(raw_configuration([], MIROIR_ID)),
                "no analysis of the timeline",
            ),
            (self.client(database=False), "MIROIR_TIMELINE_DATABASE_URL"),
        ):
            for route in (*ROUTES, "/api/timeline/health"):
                with self.subTest(route=route, message=message):
                    response = client.get(route, headers=BEARER)
                    self.assertEqual(response.status_code, 503)
                    body = response.json()
                    self.assertIn(message, body.get("error") or body.get("message"))
                    self.assertEqual(
                        response.headers["cache-control"], "private, no-store"
                    )

    def test_the_time_limit_answers_504(self):
        client = self.client()
        with patch.object(
            TimelineReader, "search_screen", side_effect=TimelineTimeout("Too slow")
        ):
            response = client.get("/api/timeline/search?q=invoice", headers=BEARER)
        self.assertEqual(
            (response.status_code, response.json()), (504, {"error": "Too slow"})
        )
