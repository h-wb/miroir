"""Timings of the timeline's queries on a year of synthetic frames (decision 040).

Skipped unless MIROIR_TIMELINE_BENCHMARK=1: it fills a disposable Postgres with
some 300,000 invented frames (a few minutes) and prints what it measured. The
tables are built by the real models first, then filled in bulk, so the queries
run against the models' own columns and indexes.
"""

import gzip
import json
import os
import statistics
import time
import unittest
from datetime import date

from fastapi.testclient import TestClient

from src.core.analysis import analysis_plan
from src.core.config import parse_miroir
from src.core.service import InlineExecutor, MiroirService
from src.core.timeline import TimelineDatabase, TimelineReader, timeline_source
from src.web.app import create_app
from src.web.timeline import _encoded
from tests.test_timeline_models import (
    READER,
    SAFARI,
    TimelineBuildFixture,
    raw_configuration,
    screen,
    screen_batch,
    timeline,
)
from tests.test_web import BEARER, TOKEN

YEAR_FRAMES = 300_000
DAY = date(2026, 11, 20)


@unittest.skipUnless(
    os.environ.get("MIROIR_TIMELINE_BENCHMARK") == "1",
    "set MIROIR_TIMELINE_BENCHMARK=1 to measure",
)
class TimelineBenchmark(TimelineBuildFixture):
    def test_measure(self):
        self.query(f"alter role {READER} login")
        raw = raw_configuration(
            [timeline("timeline", {"screen": "screen"}, READER)], "timeline_bench"
        )
        miroir = parse_miroir(json.dumps(raw))
        self.send(miroir, "screen", screen_batch(*screen(0, SAFARI)))
        schema = analysis_plan(miroir.analyses[0]).relations[0].split(".")[0]
        frames = f'"{schema}".screen_frames'
        apps = ", ".join(
            f"('com.example.app{n}', 'App {n}', '#4e79a7')" for n in range(12)
        )
        self.query(
            f'insert into "{schema}".apps values {apps.replace(")", ", false)")}'
        )
        # Words drawn with a skew, like text on screens: a few very common, a
        # long tail of rare ones. 'common' is on about 30% of frames, 'medium'
        # on 1%, 'rare' on 0.01%.
        fill = """
            insert into {frames} (image_path, captured_at, bundle_id, text, withheld,
                masked, media_status, thumbnail_w360, thumbnail_w1280, connection,
                _input, _stamp)
            select
                to_char(at, 'YYYY/MM/DD/') || n || '.jpeg', at,
                'com.example.app' || (n % 12), words, null, 0,
                case when n % 10 = 0 then null else 'made' end,
                n % 10 <> 0, n % 10 <> 0, 'screen', 'bench', 'bench'
            from (
                select n, at, (
                    select string_agg('w' || floor(power(5000, random()))::int, ' ')
                    from generate_series(1, 80 + (n % 3) * 0)
                ) || case when random() < 0.3 then ' common' else '' end
                  || case when random() < 0.01 then ' medium' else '' end
                  || case when random() < 0.0001 then ' rare' else '' end as words
                from (
                    select n, timestamptz '2025-11-21 05:00Z' + n * interval '{SPAN}' as at
                    from generate_series(1, {count}) n
                ) s
            ) t
        """
        fill = fill.replace("{frames}", frames)
        begun = time.monotonic()
        self.query(
            fill.replace("{SPAN}", "100 seconds").replace("{count}", str(YEAR_FRAMES))
        )
        # One busy day: 3,000 frames, one every 20 seconds from 06:00 local.
        self.query(
            fill.replace("{SPAN}", "20 seconds")
            .replace("{count}", "3000")
            .replace(
                "timestamptz '2025-11-21 05:00Z'", "timestamptz '2026-11-20 11:00Z'"
            )
            .replace("|| n ||", "|| 'busy' || n ||")
        )
        self.query(f"analyze {frames}")
        print(f"\nfilled in {time.monotonic() - begun:.0f} s")

        database = TimelineDatabase(
            f"postgresql://{READER}@127.0.0.1:{self.port}/postgres"
        )
        self.addCleanup(database.close)
        reader = TimelineReader(database, timeline_source(miroir))

        def timed(call, runs=7):
            call()  # warm
            times = []
            for _ in range(runs):
                start = time.perf_counter()
                result = call()
                times.append((time.perf_counter() - start) * 1000)
            return result, statistics.median(times), max(times)

        index, median, worst = timed(lambda: reader.screen_day(DAY))
        body = _encoded(index)
        print(
            f"day index: {index['count']} frames, {median:.0f} ms median, {worst:.0f} ms"
            f" worst; {len(body) / 1024:.0f} KB, {len(gzip.compress(body, 6)) / 1024:.1f}"
            " KB gzip"
        )
        config = self.root / "bench-miroir.json"
        config.write_text(json.dumps(raw))
        service = MiroirService(
            config, self.store, self.root / "artifacts", InlineExecutor(self.fail)
        )
        client = TestClient(create_app(service, TOKEN, timeline=database))
        headers = {**BEARER, "Accept-Encoding": "gzip"}
        response, median, worst = timed(
            lambda: client.get(f"/api/timeline/screen/{DAY}", headers=headers)
        )
        print(
            f"day index route: {median:.0f} ms median, {worst:.0f} ms worst,"
            f" {response.headers['content-encoding']}"
            f" {response.num_bytes_downloaded / 1024:.1f} KB on the wire"
        )
        for words in ("common", "medium", "rare", "w2 w3", "common medium"):
            found, median, worst = timed(
                lambda w=words: reader.search_screen(w, None, None, None, 20, None)
            )
            print(
                f"search {words!r}: {len(found['moments'])} moments, {median:.0f} ms"
                f" median, {worst:.0f} ms worst"
            )
        month = (date(2026, 10, 1), date(2026, 10, 31))
        found, median, worst = timed(
            lambda: reader.search_screen("common", None, *month, 20, None)
        )
        print(
            f"search 'common' in a month: {median:.0f} ms median, {worst:.0f} ms worst"
        )
        found, median, worst = timed(
            lambda: reader.search_screen(
                "common", "com.example.app3", None, None, 20, None
            )
        )
        print(
            f"search 'common' in one app: {median:.0f} ms median, {worst:.0f} ms worst"
        )
