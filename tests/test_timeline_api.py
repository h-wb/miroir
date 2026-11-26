"""The timeline's queries and JSON API (decision 040, phase 1c).

Synthetic ScreenMemory and browser batches are loaded and built by the real
models in a disposable Postgres (tests/postgres.py); the server reads them as the
reader role, through its read-only, time-limited sessions.
"""

import getpass
import gzip
import html
import json
import re
import tempfile
import threading
import time
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

import psycopg2
import sqlalchemy as sa
from fastapi.testclient import TestClient

from src.core.analysis import analysis_plan
from src.core.config import parse_miroir
from src.core.service import InlineExecutor, MiroirService
from src.kernel.errors import OperationError
from src.viewer import STATIC_DIR
from src.viewer import reader as timeline_module
from src.viewer.reader import (
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
)
from src.viewer.timeline_pages import MEDIA_LABELS
from src.web.app import create_app
from tests import test_messages_models as apple_messages
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
from tests.test_web_static import INLINE
from tests.viewer_settings import status, timeline_source

MAIL = "com.apple.mail"
MIROIR_ID = "timeline_api"
BOTH = {"screen": "screen", "browser": "browser"}
SAME_A, SAME_B = "2026/11/20/same-a.jpeg", "2026/11/20/same-b.jpeg"
ROUTES = (
    "/api/timeline/days?from=2026-11-19&to=2026-11-21",
    "/api/timeline/screen/2026-11-20",
    f"/api/timeline/frames/{path(0)}",
    "/api/timeline/events?from=2026-11-19&to=2026-11-21",
    # Search words go in a POST body, never in an address.
    ("/api/timeline/search", {"q": "invoice"}),
)


def call(client, route, **options):
    """A route of ROUTES: an address to GET, or (address, body) to POST as JSON."""
    options.setdefault("follow_redirects", False)
    if isinstance(route, tuple):
        return client.post(route[0], json=route[1], **options)
    return client.get(route, **options)


def at(offset):
    return datetime.fromtimestamp(MIDNIGHT + offset, timezone.utc)


def text_frame(offset, text, bundle):
    return [frame(offset, text=text), usage(offset, bundle)]


class TimelineApiFixture(TimelineBuildFixture):
    """The API tests' synthetic day, built once per class by the real models."""

    built = False

    def setUp(self):
        super().setUp()
        self.query(f"alter role {READER} login")
        self.raw = raw_configuration([timeline("timeline", BOTH, READER)], MIROIR_ID)
        self.miroir = parse_miroir(json.dumps(self.raw))
        if not type(self).built:
            self.load()
            type(self).built = True
        self.source = timeline_source(self.miroir)
        self.database = self.reader_database()
        self.reader = TimelineReader(self.database, self.source)
        self.objects = FakeObjectStore()

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
                # Withheld, and the first frame of a Mail session.
                frame(3700, withheld="excluded_app"),
                usage(3700, MAIL),
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
        # Stored and derived before a newer rule withheld it: withheld wins.
        self.media_record(self.miroir, 3700)
        self.build(analysis_plan(self.miroir.analyses[0]))

    def client(self, raw=None, database: object = True):
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
            object_store=self.objects,
        )
        if database is True:
            database = self.database
        app = create_app(service, TOKEN, timeline=database or None)
        return TestClient(app)

    def stored(self, connection, key):
        self.objects.add(f"{connection}/screenshots/{key}", b"original")
        for width in (360, 1280):
            self.objects.add(f"{connection}/screenshots.w{width}/{key}.webp", b"thumb")

    def get_media(self, client, connection, key, size=None):
        url = f"/media/{connection}/screenshots/{key}" + (
            f"?size={size}" if size else ""
        )
        return client.get(url, headers=BEARER, follow_redirects=False)


class TimelineQueryTests(TimelineApiFixture):
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
                    "screen": {"events": 4, "frames": 7},
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
            apps, ["Safari", "Safari", "Terminal", "Notes", "Mail", "Mail", "Mail"]
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
        with patch("src.viewer.reader.MAX_DAY_FRAMES", 3):
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
        with patch("src.viewer.reader.MAX_TEXT", 7):
            cut = self.reader.frame(path(0))
        self.assertEqual((cut["text"], cut["text_truncated"]), ("Morning", True))

    # ---- events -------------------------------------------------------------

    def test_events_page_by_keyset_without_gaps_or_repeats(self):
        day19, day21 = datetime(2026, 11, 19).date(), datetime(2026, 11, 21).date()
        whole = self.reader.events(day19, day21, ("screen", "browser"), 500, None)
        self.assertIsNone(whole["next"])
        keys = [(e["started_at"], e["event_id"]) for e in whole["events"]]
        self.assertEqual(len(keys), 13)  # 7 visits, 6 sessions
        # A session whose first frame is withheld says so, whatever was stored.
        withheld = next(
            e for e in whole["events"] if e["event_id"] == f"screen:{path(3700)}"
        )
        self.assertEqual(withheld["media"]["key"], path(3700))
        self.assertEqual(
            (withheld["media"]["state"], withheld["media"]["status"]),
            ("withheld", "made"),
        )
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

    def test_every_route_needs_the_login(self):
        client = self.client()
        for route in (
            *ROUTES,
            "/api/timeline/health",
            f"/media/screen/screenshots/{path(0)}?size=360",
        ):
            # Pages outside /api send the browser to the login page.
            url = route[0] if isinstance(route, tuple) else route
            refused = 401 if url.startswith("/api/") else 303
            with self.subTest(route=route):
                self.assertEqual(call(client, route).status_code, refused)
                self.assertEqual(
                    call(
                        client, route, headers={"Authorization": "Bearer wrong"}
                    ).status_code,
                    refused,
                )

    def test_routes_answer_json_that_is_not_stored(self):
        client = self.client()
        for route in ROUTES:
            with self.subTest(route=route):
                response = call(client, route, headers=BEARER)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.headers["content-type"], "application/json")
                expected = (
                    "private, no-cache"
                    if "/screen/" in str(route)
                    else "private, no-store"
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
        browser = client.post(
            "/api/timeline/search",
            json={"q": "invoice", "kind": "browser", "limit": 1},
            headers=BEARER,
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
        compress = patch("src.viewer.timeline.COMPRESS_FROM", 1)
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
            ("/api/timeline/search", {}),
            ("/api/timeline/search", {"q": ""}),
            ("/api/timeline/search", {"q": "invoice", "kind": "music"}),
            ("/api/timeline/search", {"q": "invoice", "kind": "browser", "app": "x"}),
            ("/api/timeline/search", {"q": "invoice", "before": "WyJhIiwiYiJd"}),
            ("/api/timeline/search", {"q": "invoice", "limit": 51}),
            ("/api/timeline/search", {"q": "invoice", "limit": "5"}),
            ("/api/timeline/search", {"q": 3}),
            ("/api/timeline/search", {"q": "invoice", "extra": "1"}),
            ("/api/timeline/search", ["invoice"]),
            (
                "/api/timeline/search",
                {"q": "invoice", "from": "2026-11-21", "to": "2026-11-20"},
            ),
        )
        for route in cases:
            with self.subTest(route=route):
                response = call(client, route, headers=BEARER)
                self.assertEqual(response.status_code, 400, response.text)
                self.assertIn("error", response.json())
                self.assertEqual(response.headers["cache-control"], "private, no-store")
        unknown = client.get(
            "/api/timeline/frames/2026/11/20/unknown.jpeg", headers=BEARER
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.headers["cache-control"], "private, no-store")

    def test_search_words_are_never_taken_from_an_address(self):
        client = self.client()
        for url in (
            "/api/timeline/search?q=invoice",
            "/api/timeline/search",
            "/api/timeline/search?kind=browser",
        ):
            with self.subTest(url=url):
                response = client.get(url, headers=BEARER)
                self.assertEqual(response.status_code, 400)
                self.assertIn("Search with POST", response.json()["error"])
                self.assertNotIn("invoice", response.text)
        # A POST with the words in its address is refused too: nothing quietly
        # goes back to sending them where logs keep them.
        posted = client.post(
            "/api/timeline/search?q=invoice", json={"q": "invoice"}, headers=BEARER
        )
        self.assertEqual(posted.status_code, 400)
        self.assertIn("Search with POST", posted.json()["error"])
        found = client.post(
            "/api/timeline/search",
            json={"q": "invoice", "app": TERMINAL, "limit": 5, "before": None},
            headers=BEARER,
        )
        self.assertEqual(found.status_code, 200)
        self.assertEqual(found.headers["cache-control"], "private, no-store")
        self.assertEqual([m["key"] for m in found.json()["moments"]], [path(120)])

    def test_the_search_body_is_json_with_each_field_once(self):
        client = self.client()
        url = "/api/timeline/search"
        body = '{"q": "invoice"}'
        for content_type in ("text/plain", "application/x-www-form-urlencoded", ""):
            with self.subTest(content_type=content_type):
                response = client.post(
                    url,
                    content=body,
                    headers={**BEARER, "Content-Type": content_type},
                )
                self.assertEqual(response.status_code, 415)
                self.assertIn("application/json", response.json()["error"])
                self.assertEqual(response.headers["cache-control"], "private, no-store")
        for content_type in ("application/json", "Application/JSON; charset=utf-8"):
            with self.subTest(content_type=content_type):
                response = client.post(
                    url, content=body, headers={**BEARER, "Content-Type": content_type}
                )
                self.assertEqual(response.status_code, 200)
        repeated = client.post(
            url,
            content='{"q": "absent", "q": "invoice"}',
            headers={**BEARER, "Content-Type": "application/json"},
        )
        self.assertEqual(repeated.status_code, 400)
        self.assertEqual(repeated.json(), {"error": "A field is given more than once"})

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
                    response = call(client, route, headers=BEARER)
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
            response = client.post(
                "/api/timeline/search", json={"q": "invoice"}, headers=BEARER
            )
        self.assertEqual(
            (response.status_code, response.json()), (504, {"error": "Too slow"})
        )

    # ---- a withheld frame's files ----------------------------------------------

    def test_a_withheld_frames_file_is_not_served_at_any_size(self):
        unknown = "2026/11/20/not-a-frame-yet.jpeg"
        for key in (path(0), path(3700), unknown):
            self.stored("screen", key)
        self.stored("laptop", path(3700))
        client = self.client()
        for size, served in (
            ("360", 200),
            ("1280", 200),
            ("original", 200),
            (None, 302),
        ):
            with self.subTest(size=size):
                withheld = self.get_media(client, "screen", path(3700), size)
                self.assertEqual(withheld.status_code, 404)
                self.assertEqual(withheld.headers["cache-control"], "private, no-store")
                self.assertEqual(
                    self.get_media(client, "screen", path(0), size).status_code, served
                )
                # A key the timeline does not know is not withheld.
                self.assertEqual(
                    self.get_media(client, "screen", unknown, size).status_code, served
                )
                # No timeline analysis reads this connection: nothing to ask.
                self.assertEqual(
                    self.get_media(client, "laptop", path(3700), size).status_code,
                    served,
                )
        self.assertNotIn(
            "screen/screenshots.w1280/" + path(3700) + ".webp", self.objects.gets
        )

    def test_the_media_route_fails_closed_when_the_timeline_cannot_answer(self):
        self.stored("screen", path(0))
        unbuilt = raw_configuration([timeline("unbuilt", BOTH, READER)], MIROIR_ID)
        cases = (
            (self.client(database=False), 503, "MIROIR_TIMELINE_DATABASE_URL"),
            (self.client(unbuilt), 503, "not been built yet"),
        )
        for client, code, message in cases:
            for size in ("360", "original", None):
                with self.subTest(message=message, size=size):
                    response = self.get_media(client, "screen", path(0), size)
                    self.assertEqual(response.status_code, code)
                    self.assertIn(message, response.json()["error"])
                    self.assertEqual(
                        response.headers["cache-control"], "private, no-store"
                    )
        client = self.client(database=self.reader_database(timeout=300))
        owner = psycopg2.connect(
            host="127.0.0.1", port=self.port, user=getpass.getuser(), dbname="postgres"
        )
        with closing(owner):
            with owner.cursor() as cursor:
                cursor.execute(
                    f'lock table "{self.source.schema}".screen_frames'
                    " in access exclusive mode"
                )
            response = self.get_media(client, "screen", path(0), "1280")
            owner.rollback()
        self.assertEqual(response.status_code, 504)
        self.assertNotIn(b"thumb", response.content)

    # ---- searches the index cannot answer ---------------------------------------

    def test_a_search_without_a_word_to_look_for_is_refused(self):
        for words, message in (
            ("-zzzz", "at least one word"),
            ("-a -b", "at least one word"),
            ('-"invoice paid"', "at least one word"),
            ("invoice or -zzzz", "at least one word"),
            ("invoice paid or -zzzz", "at least one word"),
            ("-", "no words"),
            ("!!!", "no words"),
            ('""', "no words"),
        ):
            for search in (
                lambda w: self.reader.search_screen(w, None, None, None, 20, None),
                lambda w: self.reader.search_browser(w, None, None, 20, None),
            ):
                with (
                    self.subTest(words=words),
                    self.assertRaisesRegex(OperationError, message),
                ):
                    search(words)
        # A word with exclusions, a phrase, alternatives: each has a word to find.
        for words, found in (
            ("invoice -paid", 4),
            ('"invoice paid"', 1),
            ("paid or totals", 2),
            ("the", 0),  # the simple configuration has no stop words
        ):
            with self.subTest(words=words):
                moments = self.reader.search_screen(words, None, None, None, 20, None)
                self.assertEqual(len(moments["moments"]), found)
        response = self.client().post(
            "/api/timeline/search", json={"q": "-zzzz"}, headers=BEARER
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("at least one word", response.json()["error"])

    # ---- review of #49, second look -----------------------------------------

    def test_only_analyses_that_can_answer_are_asked(self):
        for key in (path(0), path(3700)):
            self.stored("screen", key)
        for others in (
            [timeline("unbuilt", BOTH, READER)],  # no tables yet
            [timeline("unread", BOTH)],  # no reader role
        ):
            raw = raw_configuration(
                [timeline("timeline", BOTH, READER), *others], MIROIR_ID
            )
            client = self.client(raw)
            for size in ("360", "original", None):
                with self.subTest(others=others[0]["id"], size=size):
                    served = self.get_media(client, "screen", path(0), size)
                    self.assertIn(served.status_code, (200, 302))
                    withheld = self.get_media(client, "screen", path(3700), size)
                    self.assertEqual(withheld.status_code, 404)
        # None that can answer: closed.
        for analyses in (
            [timeline("unbuilt", BOTH, READER)],
            [timeline("unread", BOTH)],
        ):
            client = self.client(raw_configuration(analyses, MIROIR_ID))
            with self.subTest(analyses=analyses[0]["id"]):
                response = self.get_media(client, "screen", path(0), "360")
                self.assertEqual(response.status_code, 503)
                self.assertIn("not been built yet", response.json()["error"])

    def test_control_characters_are_refused_not_a_server_error(self):
        client = self.client()
        cursor = read_cursor  # the routes write cursors; this one is forged
        forged = timeline_module._cursor(["2026-11-20T00:00:00+00:00", "a\x00b"])
        self.assertTrue(forged)
        with self.assertRaises(OperationError):
            cursor(forged, (str, str))
        for route, code in (
            ("/media/screen/screenshots/a%00b.jpeg?size=360", 404),
            ("/media/screen/screenshots/a%00b.jpeg", 404),
            (("/api/timeline/search", {"q": "a\x00b"}), 400),
            (("/api/timeline/search", {"q": "invoice", "app": "a\x00b"}), 400),
            (("/api/timeline/search", {"q": "a\x01b"}), 400),
            ("/api/timeline/frames/a%00b", 400),
            (
                "/api/timeline/events?from=2026-11-20&to=2026-11-21&after=" + forged,
                400,
            ),
            (
                (
                    "/api/timeline/search",
                    {"q": "invoice", "kind": "browser", "before": forged},
                ),
                400,
            ),
        ):
            with self.subTest(route=route):
                response = call(client, route, headers=BEARER)
                self.assertEqual(response.status_code, code, response.text)

    def test_an_image_answers_while_searches_hold_the_pool(self):
        self.stored("screen", path(0))
        client = self.client()
        holding = threading.Barrier(5)

        def slow():
            with self.database.engine.connect() as db:
                holding.wait()
                db.execute(sa.text("SELECT pg_sleep(3)"))

        threads = [threading.Thread(target=slow) for _ in range(4)]
        for thread in threads:
            thread.start()
        holding.wait()  # all four of the queries' connections are taken
        time.sleep(0.2)
        begun = time.monotonic()
        response = self.get_media(client, "screen", path(0), "360")
        took = time.monotonic() - begun
        for thread in threads:
            thread.join()
        self.assertEqual(response.status_code, 200)
        self.assertLess(took, 1.0)
        # And the queries' pool was indeed full: a search waits for it.
        self.assertEqual(self.database.engine.pool.size(), 2)


class TimelineDayBoundTests(TimelineBuildFixture):
    """One rule for a time's day everywhere: the day whose bounds hold it."""

    def test_days_index_frames_and_moments_agree_where_clocks_go_back(self):
        self.query(f"alter role {READER} login")
        havana = timeline("havana", BOTH, READER) | {"timezone": "America/Havana"}
        raw = raw_configuration(
            [timeline("toronto", BOTH, READER), havana], "day_bounds"
        )
        miroir = parse_miroir(json.dumps(raw))

        def utc(*parts):
            return int(datetime(*parts, tzinfo=timezone.utc).timestamp())

        # Havana goes back from 01:00 CDT to 00:00 CST on 2026-11-01: local
        # midnight comes twice, and the day starts at the later one.
        first, repeat = utc(2026, 11, 1, 4, 30), utc(2026, 11, 1, 5, 30)
        # Toronto's 25-hour and 23-hour days.
        long_day, short_day = utc(2026, 11, 2, 4, 30), utc(2026, 3, 9, 3, 30)
        records = []
        for ts in (first, repeat, long_day, short_day):
            records += [
                frame(0, text="Some text")
                | {"ts": ts, "image_path": f"bounds/{ts}.jpeg"},
                usage(ts - MIDNIGHT, SAFARI),
            ]
        self.send(miroir, "screen", screen_batch(*records))
        for analysis in miroir.analyses:
            self.build(analysis_plan(analysis))
        database = TimelineDatabase(
            f"postgresql://{READER}@127.0.0.1:{self.port}/postgres"
        )
        self.addCleanup(database.close)

        def reader(index):
            analysis = miroir.analyses[index]
            schema = analysis_plan(analysis).relations[0].split(".")[0]
            source = TimelineSource(analysis.id, schema, analysis.config.timezone)
            return TimelineReader(database, source)

        def where(reader, ts):
            """Each place that names the day of a frame."""
            key = f"bounds/{ts}.jpeg"
            found = reader.frame(key)
            listed = [
                d
                for d in ("2026-03-08", "2026-03-09", "2026-10-31", "2026-11-01")
                if key in reader.screen_day(date.fromisoformat(d))["frames"]["key"]
            ]
            moments = reader.search_screen("some", None, None, None, 50, None)
            moment = next(m["day"] for m in moments["moments"] if m["key"] == key)
            return found["day"], found["t"], listed, moment

        cuba = reader(1)
        self.assertEqual(
            where(cuba, first), ("2026-10-31", 88200, ["2026-10-31"], "2026-10-31")
        )
        self.assertEqual(
            where(cuba, repeat), ("2026-11-01", 1800, ["2026-11-01"], "2026-11-01")
        )
        days = cuba.days(date(2026, 10, 31), date(2026, 11, 1))["days"]
        self.assertEqual(
            [(d["day"], d["screen"]["frames"]) for d in days],
            [("2026-10-31", 1), ("2026-11-01", 2)],
        )
        toronto = reader(0)
        self.assertEqual(
            where(toronto, long_day),
            ("2026-11-01", 88200, ["2026-11-01"], "2026-11-01"),
        )
        self.assertEqual(
            where(toronto, short_day),
            ("2026-03-08", 81000, ["2026-03-08"], "2026-03-08"),
        )
        index = toronto.screen_day(date(2026, 11, 1))
        self.assertEqual(
            (index["start"], index["end"]),
            ("2026-11-01T00:00:00-04:00", "2026-11-02T00:00:00-05:00"),
        )


class TimelinePageTests(TimelineApiFixture):
    """The Day view and search pages (phase 1c, part 2), on the same built data."""

    def page(self, client, url, **headers):
        response = client.get(
            url, headers={**BEARER, **headers}, follow_redirects=False
        )
        if response.status_code < 300:
            self.assertEqual(response.headers["cache-control"], "private, no-store")
            self.assertIsNone(INLINE.search(response.text), url)
        return response

    def test_the_latest_day_opens_and_a_day_lists_its_calendar(self):
        client = self.client()
        home = self.page(client, "/timeline")
        self.assertEqual(
            (home.status_code, home.headers["location"]), (303, "/timeline/2026-11-21")
        )
        day = self.page(client, "/timeline/2026-11-20")
        self.assertEqual(day.status_code, 200)
        self.assertIn('data-day="2026-11-20"', day.text)
        self.assertIn('data-timezone="America/Toronto"', day.text)
        self.assertIn(
            '<script type="module" src="/static/timeline.js"></script>', day.text
        )
        self.assertIn("7 screen frames, 6 browser visits.", day.text)
        self.assertIn('id="scrubber"', day.text)
        for code, label in MEDIA_LABELS.items():
            self.assertIn(f'data-label-{code}="{label}"', day.text)
        # The month around it, shaded by activity; the day itself marked.
        self.assertIn(
            '<a href="/timeline/2026-11-20" class="heat heat-2 selected" aria-current="date"',
            day.text,
        )
        self.assertIn('href="/timeline/2026-11-19" class="heat heat-1"', day.text)
        self.assertIn('href="/timeline/2026-11-01" class="heat heat-0"', day.text)
        self.assertIn('href="/timeline/2026-11-19" rel="prev"', day.text)
        # The header now links the timeline.
        self.assertIn('<a href="/timeline">Timeline</a>', day.text)

    def test_an_empty_day_says_so_and_loads_no_scrubber(self):
        empty = self.page(self.client(), "/timeline/2026-01-01")
        self.assertEqual(empty.status_code, 200)
        self.assertIn("Nothing was recorded on this day.", empty.text)
        self.assertNotIn('id="scrubber"', empty.text)

    def test_a_frame_shows_its_image_or_a_tile_for_each_media_code(self):
        client = self.client()
        cases = (
            (path(0), MEDIA_THUMBNAILS),
            (path(60), MEDIA_NO_THUMBNAILS),
            (path(120), MEDIA_UNKNOWN),
            (path(3700), MEDIA_WITHHELD),
        )
        for key, code in cases:
            with self.subTest(key=key, code=code):
                found = self.page(client, f"/timeline/2026-11-20?frame={key}")
                self.assertEqual(found.status_code, 200)
                self.assertIn(f'data-frame="{key}"', found.text)
                original = f'href="/media/screen/screenshots/{key}?size=original"'
                if code in (MEDIA_THUMBNAILS, MEDIA_NO_THUMBNAILS):
                    self.assertIn(original, found.text)
                else:
                    self.assertNotIn(original, found.text)
                if code == MEDIA_THUMBNAILS:
                    self.assertIn(
                        f'<img id="frame-image" src="/media/screen/screenshots/{key}?size=1280"',
                        found.text,
                    )
                else:
                    self.assertIn(
                        f'<span id="frame-tile" class="tile tile-{code}" >{MEDIA_LABELS[code]}</span>',
                        found.text,
                    )
                    # No image: neither thumbnail size is asked for.
                    self.assertNotIn("?size=360", found.text)
                    self.assertNotIn("?size=1280", found.text)
        withheld = self.page(client, f"/timeline/2026-11-20?frame={path(3700)}")
        self.assertIn("Withheld (excluded_app): no text, no screenshot.", withheld.text)
        self.assertIn('<pre id="frame-text" class="frame-text"></pre>', withheld.text)
        shown = self.page(client, f"/timeline/2026-11-20?frame={path(0)}")
        self.assertIn(">Morning invoice review</pre>", shown.text)
        self.assertIn('data-colour="#2196f3">Safari</span>', shown.text)
        # A frame opens on its own day; an unknown one says so.
        moved = self.page(client, f"/timeline/2026-11-20?frame={path(-10)}")
        self.assertEqual(moved.status_code, 303)
        self.assertEqual(
            moved.headers["location"],
            f"/timeline/2026-11-19?frame={quote(path(-10), safe='')}",
        )
        unknown = self.page(client, "/timeline/2026-11-20?frame=2026/11/20/none.jpeg")
        self.assertIn("That frame is not in the timeline.", unknown.text)
        for url in (
            "/timeline/20261120",
            "/timeline/2026-13-01",
            "/timeline/2026-11-20?frame=a%00b",
        ):
            with self.subTest(url=url):
                self.assertEqual(self.page(client, url).status_code, 400)

    def search(self, client, fields, **headers):
        """The search form, posted: its words are never in an address."""
        response = client.post(
            "/timeline/search",
            data=fields,
            headers={**BEARER, **headers},
            follow_redirects=False,
        )
        self.assertEqual(response.headers["cache-control"], "private, no-store")
        self.assertIsNone(INLINE.search(response.text))
        return response

    def test_search_shows_moments_visits_nothing_or_why_it_refused(self):
        client = self.client()
        found = self.search(client, {"q": "invoice", "kind": "screen"})
        self.assertEqual(found.status_code, 200)
        self.assertIn('<script src="/static/htmx.min.js" defer></script>', found.text)
        self.assertEqual(found.text.count('<article class="moment">'), 5)
        self.assertIn(
            f'<img class="thumb" src="/media/screen/screenshots/{path(0)}?size=360"',
            found.text,
        )
        self.assertIn(
            '<span class="tile tile-0">Image state not known yet</span>', found.text
        )
        self.assertIn(
            f'href="/timeline/2026-11-20?frame={quote(path(0), safe="")}"', found.text
        )
        self.assertIn("Morning <mark>invoice</mark> review", found.text)
        self.assertIn("2 frames, until 00:01:00", found.text)
        # The words are shown back in the form, which posts again, but are not
        # in the title: browsers keep titles in their history.
        self.assertIn('name="q" value="invoice"', found.text)
        self.assertIn("<title>Timeline search</title>", found.text)
        # The results' history entry is made an ordinary one, so Back shows the
        # empty form rather than offering to send the words again.
        self.assertIn(
            '<script type="module" src="/static/timeline.js"></script>', found.text
        )
        script = (Path(STATIC_DIR) / "timeline.js").read_text()
        self.assertIn('history.replaceState(null, "", location.pathname)', script)
        self.assertIn(
            '<form class="timeline-search" method="post" action="/timeline/search"',
            found.text,
        )
        browser = self.search(client, {"q": "invoice", "kind": "browser"})
        self.assertEqual(browser.text.count('<article class="visit">'), 6)
        self.assertIn("<mark>Invoice</mark> portal", browser.text)
        self.assertIn(
            '<a href="https://billing.example.com/invoices" rel="noreferrer noopener"',
            browser.text,
        )
        terminal = self.search(client, {"q": "invoice", "app": TERMINAL})
        self.assertEqual(terminal.text.count('<article class="moment">'), 1)
        none = self.search(client, {"q": "absent"})
        self.assertIn("Nothing found on screen.", none.text)
        refused = self.search(client, {"q": "-zzzz"})
        self.assertEqual(refused.status_code, 400)
        self.assertIn(
            '<p class="error" id="search-refused">The search must look for',
            refused.text,
        )
        empty = self.page(client, "/timeline/search")
        self.assertEqual(empty.status_code, 200)
        self.assertIn('method="post" action="/timeline/search"', empty.text)
        self.assertNotIn('class="results"', empty.text)

    def test_the_form_is_read_with_small_limits_and_long_words_are_not_echoed(self):
        client = self.client()
        long_words = "invoice " * 40  # 320 characters: refused, and not shown back
        refused = self.search(client, {"q": long_words})
        self.assertEqual(refused.status_code, 400)
        self.assertIn("1 to 200 characters", refused.text)
        self.assertIn('name="q" value=""', refused.text)
        self.assertNotIn("invoice invoice", refused.text)
        for fields in (
            {"q": "x" * 9000},  # one part over 8 KB
            {"q": "invoice", **{f"extra{n}": "1" for n in range(10)}},  # 11 fields
        ):
            with self.subTest(fields=len(fields)):
                response = self.search(client, fields)
                self.assertEqual(response.status_code, 400)
                self.assertIn("The search form was too large", response.text)
                self.assertNotIn("xxxx", response.text)
                self.assertIn('name="q" value=""', response.text)
        multipart = client.post(
            "/timeline/search",
            files={"q": ("q.txt", b"invoice")},
            headers=BEARER,
        )
        self.assertEqual(multipart.status_code, 400)
        self.assertIn("The search form was too large", multipart.text)

    def test_search_words_in_an_address_are_refused_and_not_echoed(self):
        client = self.client()
        for url in (
            "/timeline/search?q=invoice",
            "/timeline/search?q=invoice&kind=browser",
        ):
            with self.subTest(url=url):
                response = self.page(client, url)
                self.assertEqual(response.status_code, 400)
                self.assertIn("Search with POST", response.text)
                self.assertNotIn("invoice", response.text)
                self.assertNotIn('class="results"', response.text)
        posted = client.post(
            "/timeline/search?q=invoice", data={"q": "invoice"}, headers=BEARER
        )
        self.assertEqual(posted.status_code, 400)
        self.assertNotIn('class="results"', posted.text)

    def test_older_results_post_their_fields_in_the_body(self):
        client = self.client()
        with patch("src.viewer.timeline_pages.SEARCH_PAGE", 2):
            first = self.search(client, {"q": "invoice", "kind": "screen"})
            more = re.search(
                r'<form class="more"([^>]*)>(.*?)</form>', first.text, re.DOTALL
            )
            assert more is not None
            attributes, inner = more.groups()
            # Neither the form's address nor any hx- attribute carries the words.
            self.assertIn('method="post" action="/timeline/search"', attributes)
            self.assertIn('hx-post="/timeline/search"', attributes)
            self.assertIn('hx-push-url="false"', attributes)
            self.assertNotIn("invoice", attributes)
            self.assertNotIn("?", attributes)
            fields = {
                name: html.unescape(value)
                for name, value in re.findall(
                    r'<input type="hidden" name="([^"]+)" value="([^"]*)">', inner
                )
            }
            self.assertEqual(fields["q"], "invoice")
            self.assertIn("before", fields)
            fragment = self.search(client, fields, **{"HX-Request": "true"})
            plain = self.search(client, fields)
        self.assertNotIn("<html", fragment.text)
        self.assertEqual(fragment.text.count('<article class="moment">'), 2)
        self.assertIn("<html", plain.text)
        self.assertIn(f"frame={quote(path(0), safe='')}", fragment.text)

    def test_an_htmx_request_without_a_login_reloads_the_login_page(self):
        client = self.client()
        anonymous = {"HX-Request": "true"}
        for response in (
            client.post(
                "/timeline/search",
                data={"q": "invoice"},
                headers=anonymous,
                follow_redirects=False,
            ),
            client.get(
                "/timeline/2026-11-20", headers=anonymous, follow_redirects=False
            ),
        ):
            with self.subTest(url=str(response.url)):
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.headers["hx-redirect"], "/login")
                self.assertEqual(response.content, b"")
        # Without htmx, the browser is sent to the login page as before.
        plain = client.post(
            "/timeline/search", data={"q": "invoice"}, follow_redirects=False
        )
        self.assertEqual(
            (plain.status_code, plain.headers["location"]), (303, "/login")
        )

    def test_data_is_text_never_markup(self):
        from src.web.app import templates

        rendered = templates.get_template("_timeline_results.html").render(
            kind="screen",
            labels=MEDIA_LABELS,
            more=None,
            results=[
                {
                    "open": "/timeline/2026-11-20",
                    "code": 0,
                    "thumbnail": None,
                    "clock": "10:00:00",
                    "at": "x",
                    "day": "2026-11-20",
                    "app": {"name": "<img src=x onerror=alert(1)>", "colour": '"><b>'},
                    "matches": 1,
                    "snippet": ["<script>", "<b>hit</b>", "&"],
                }
            ],
        )
        self.assertNotIn("<script>", rendered)
        self.assertNotIn("<img src=x", rendered)
        self.assertIn(
            "&lt;script&gt;<mark>&lt;b&gt;hit&lt;/b&gt;</mark>&amp;", rendered
        )
        self.assertIn('data-colour="&#34;&gt;&lt;b&gt;"', rendered)

    def test_pages_say_when_there_is_no_timeline_or_it_is_not_built(self):
        unbuilt = raw_configuration([timeline("unbuilt", BOTH, READER)], MIROIR_ID)
        for client, message in (
            (self.client(database=False), "MIROIR_TIMELINE_DATABASE_URL"),
            (
                self.client(raw_configuration([], MIROIR_ID)),
                "no analysis of the timeline",
            ),
            (self.client(unbuilt), "not been built yet"),
        ):
            for url in (
                "/timeline",
                "/timeline/2026-11-20",
                "/timeline/search",
                "POST /timeline/search",
            ):
                if url == "/timeline/search" and "built" in message:
                    continue  # the empty form reads no table; its search says so
                with self.subTest(url=url, message=message):
                    response = (
                        self.search(client, {"q": "invoice"})
                        if url.startswith("POST")
                        else self.page(client, url)
                    )
                    self.assertEqual(response.status_code, 503)
                    self.assertIn(message, response.text)
                    self.assertEqual(
                        response.headers["cache-control"], "private, no-store"
                    )
        anonymous = self.client().get("/timeline/2026-11-20", follow_redirects=False)
        self.assertEqual(
            (anonymous.status_code, anonymous.headers["location"]), (303, "/login")
        )


class TimelineWithMessagesTests(TimelineApiFixture):
    """Linking messages changes nothing the screen and browser routes and the
    home page answer (decision 040, phases 2a and 2c): the days answer only
    gains `messages` counts. The Day view gains its Messages lane (phase 2c,
    part 2) and keeps everything else."""

    def answers(self, client):
        answers = {}
        for route in (
            *ROUTES,
            "/api/timeline/days?from=2026-11-01&to=2026-11-30",
        ):
            response = call(client, route, headers=BEARER)
            answers[str(route)] = (response.status_code, response.text)
        home = client.get("/timeline", headers=BEARER, follow_redirects=False)
        answers["/timeline"] = (home.status_code, home.headers.get("location"))
        for day in ("2026-11-20", "2026-11-22"):
            page = client.get(f"/timeline/{day}", headers=BEARER)
            answers[day] = (page.status_code, page.text)
        return answers

    def test_messages_change_no_page_or_route_of_screen_and_browser(self):
        before = self.answers(self.client())
        raw = json.loads(json.dumps(self.raw))
        raw["connections"].append(
            {"id": "messages", "integration": "apple_messages", "settings": {}}
        )
        raw["analyses"][0]["inputs"]["messages"] = "messages"
        miroir = parse_miroir(json.dumps(raw))
        number = "+15555550101"
        guid = "iMessage;-;" + number
        records = list(apple_messages.chat(guid, number, number))
        for name, day in (("x1", 20), ("x2", 22)):
            sent = apple_messages.message(
                name, 0, guid, handle=number, text="invoice from a message"
            )
            sent[0]["date"] = apple_messages.apple(
                datetime(2026, 11, day, 15, tzinfo=timezone.utc)
            )
            records += sent
        batch = apple_messages.write_batch(
            Path(tempfile.mkdtemp(dir=self.root)) / "b.jsonl.gz", records
        )
        self.send(miroir, "messages", batch.read_bytes())
        self.assertEqual(
            self.query(
                f"select count(*) from {self.schema(miroir, 'timeline')}.days "
                "where source = 'messages'"
            ),
            [(2,)],
        )
        after = self.answers(self.client(raw))
        # The Day view: as before, plus the Messages lane, the messages near the
        # frame, their count and the Messages kind of search.
        pages = {}
        for day in ("2026-11-20", "2026-11-22"):
            pages[day] = before.pop(day)[1], after.pop(day)[1]
            for added in ("lane-messages", "messages-near", 'value="messages"'):
                self.assertNotIn(added, pages[day][0])
                self.assertIn(added, pages[day][1])
        self.assertIn("7 screen frames, 6 browser visits.", pages["2026-11-20"][0])
        self.assertIn(
            "7 screen frames, 6 browser visits, 1 message.", pages["2026-11-20"][1]
        )
        # A day with a message alone is not empty any more.
        self.assertIn("Nothing was recorded", pages["2026-11-22"][0])
        self.assertNotIn("Nothing was recorded", pages["2026-11-22"][1])
        counted = {}
        for route in [r for r in after if r.startswith("/api/timeline/days")]:
            status, text = after[route]
            body = json.loads(text)
            kept = []
            for day in body["days"]:
                if "messages" in day:
                    counted[day["day"]] = day.pop("messages")
                if set(day) != {"day"}:
                    kept.append(day)
            after[route] = (status, json.dumps({**body, "days": kept}))
            before[route] = (before[route][0], json.dumps(json.loads(before[route][1])))
        self.assertEqual(after, before)
        self.assertEqual(
            counted, {"2026-11-20": {"events": 1}, "2026-11-22": {"events": 1}}
        )
