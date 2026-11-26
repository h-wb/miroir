"""Timings of the timeline's queries on a year of synthetic frames, and of the
conversation routes on 200,000 synthetic messages (decision 040).

Skipped unless MIROIR_TIMELINE_BENCHMARK=1: it fills a disposable Postgres with
some 300,000 invented frames or 200,000 invented messages (a few minutes each)
and prints what it measured. The tables are built by the real models first, then
filled in bulk, so the queries run against the models' own columns and indexes.
"""

import gzip
import json
import os
import statistics
import time
import unittest
from datetime import date, datetime

from fastapi.testclient import TestClient

from src.cli.compose import web_app
from src.core.analysis import analysis_plan
from src.core.config import parse_miroir
from src.core.service import InlineExecutor, MiroirService
from src.viewer.reader import TimelineDatabase, TimelineReader
from src.viewer.timeline import _encoded
from tests.test_messages_models import chat, message
from tests.test_timeline_messages import (
    TimelineMessagesFixture,
    configuration,
)
from tests.test_timeline_messages import timeline as messages_timeline
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
from tests.viewer_settings import timeline_source

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
        client = TestClient(web_app(service, TOKEN, timeline=database))
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


MESSAGES = 200_000
CONVERSATIONS = 300


@unittest.skipUnless(
    os.environ.get("MIROIR_TIMELINE_BENCHMARK") == "1",
    "set MIROIR_TIMELINE_BENCHMARK=1 to measure",
)
class TimelineMessagesBenchmark(TimelineMessagesFixture):
    """The conversation routes on 200,000 invented messages in 300 conversations
    (decision 040, phase 2c): the tables are built by the real models, then
    filled in bulk."""

    def test_measure(self):
        self.query(f"alter role {READER} login")
        miroir = configuration(
            [messages_timeline("timeline", reader_role=READER)],
            miroir_id="timeline_messages_bench",
        )
        number = "+15555550101"
        self.deliver(
            miroir,
            [
                *chat("iMessage;-;" + number, number, number),
                *message("seed", 0, "iMessage;-;" + number, handle=number),
            ],
        )
        schema = analysis_plan(miroir.analyses[0]).relations[0].split(".")[0]
        table = f'"{schema}".'.__add__
        begun = time.monotonic()
        # Conversations drawn with a skew, as people are written to: a few take
        # most messages. Words as on screens: 'common' on about 30% of
        # messages, 'medium' on 1%, 'rare' on 0.01%; one in ten is long.
        self.query(
            f"""
            insert into {table("messages")} (guid, conversation_id, sent_at, day,
                timezone, is_from_me, sender_handle, sender_written, service, text,
                text_unreadable, is_edited, is_unsent, is_audio, attachments,
                reactions, deleted, connection, _input, _stamp)
            select 'b' || n, md5('c' || c), at,
                (at at time zone 'America/Toronto')::date, 'America/Toronto',
                me, case when not me then '555' || lpad(c::text, 7, '0') end,
                case when not me then '+1555' || lpad(c::text, 7, '0') end,
                'iMessage', words, false, false, false, false,
                case when n % 25 = 0 then jsonb_build_array(jsonb_build_object(
                    'guid', 'f' || n, 'media_key', 'f' || n || '.jpeg',
                    'kind', 'image', 'mime_type', 'image/jpeg', 'bytes', 100000,
                    'transfer_name', 'IMG_' || n || '.JPG', 'media_status', 'made',
                    'thumbnail_w360', true, 'thumbnail_w1280', true))
                else '[]'::jsonb end,
                case when n % 10 = 0 then jsonb_build_array(jsonb_build_object(
                    'kind', 'love', 'from_me', false,
                    'sender', '555' || lpad(c::text, 7, '0'), 'part', 0,
                    'at', to_char(at at time zone 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')))
                else '[]'::jsonb end,
                false, 'm', 'bench', 'bench'
            from (
                select n, c, at, random() < 0.45 as me, (
                    select string_agg('w' || floor(power(5000, random()))::int, ' ')
                    from generate_series(1, case when n % 10 = 0 then 200 else 20 end)
                    where n > 0
                ) || case when random() < 0.3 then ' common' else '' end
                  || case when random() < 0.01 then ' medium' else '' end
                  || case when random() < 0.0001 then ' rare' else '' end as words
                from (
                    select n, floor({CONVERSATIONS} * power(random(), 3))::int as c,
                        timestamptz '2025-11-01 12:00Z' + n * interval '170 seconds'
                        + (random() * 60) * interval '1 second' as at
                    from generate_series(1, {MESSAGES}) n
                ) s
            ) t
            """
        )
        self.query(
            f"""
            insert into {table("people")} (handle, name, written, display_name)
            select '555' || lpad(c::text, 7, '0'), 'Person ' || c,
                '+1555' || lpad(c::text, 7, '0'), 'Person ' || c
            from generate_series(0, {CONVERSATIONS - 1}) c
            on conflict do nothing
            """
        )
        self.query(
            f"""
            insert into {table("conversations")} (conversation_id, display_name,
                is_group, title, participants, participant_count, first_message_at,
                last_message_at, message_count, connection)
            select md5('c' || c), 'Person ' || c, false, null,
                array['555' || lpad(c::text, 7, '0')], 1, min(m.sent_at),
                max(m.sent_at), count(m.guid), 'm'
            from generate_series(0, {CONVERSATIONS - 1}) c
            left join {table("messages")} m on m.conversation_id = md5('c' || c)
            group by c
            """
        )
        for name in ("messages", "people", "conversations"):
            self.query(f"analyze {table(name)}")
        counts = self.query(
            f"select count(*), count(distinct conversation_id) from {table('messages')}"
        )
        print(f"\nfilled {counts[0]} in {time.monotonic() - begun:.0f} s")

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

        def report(name, call):
            result, median, worst = timed(call)
            print(f"{name}: {median:.1f} ms median, {worst:.1f} ms worst")
            return result

        listed = report(
            "list, first page of 50", lambda: reader.conversations(50, None)
        )
        busiest = self.query(
            f"select conversation_id, message_count from {table('conversations')}"
            " order by message_count desc limit 1"
        )[0]
        quiet = self.query(
            f"select conversation_id, message_count from {table('conversations')}"
            " where message_count > 0 order by message_count limit 1"
        )[0]
        print(f"busiest conversation {busiest[1]} messages, quietest {quiet[1]}")
        report("list, all 300 in one page", lambda: reader.conversations(200, None))
        report("one conversation", lambda: reader.conversation(busiest[0]))
        page = report(
            "latest page of 100, busiest",
            lambda: reader.conversation_messages(busiest[0], 100),
        )
        middle = self.query(
            f"select guid from {table('messages')} where conversation_id = "
            f"'{busiest[0]}' order by sent_at, guid offset {busiest[1] // 2} limit 1"
        )[0][0]
        report(
            "page around a message in the middle, busiest",
            lambda: reader.conversation_messages(busiest[0], 100, around=middle),
        )
        report(
            "page at a day, busiest",
            lambda: reader.conversation_messages(busiest[0], 100, at=date(2026, 3, 1)),
        )
        before = (
            datetime.fromisoformat(page["messages"][0]["sent_at"]),
            page["messages"][0]["guid"],
        )
        report(
            "page before, busiest",
            lambda: reader.conversation_messages(busiest[0], 100, before=before),
        )
        report(
            "latest page of 100, quietest",
            lambda: reader.conversation_messages(quiet[0], 100),
        )
        for words in ("common", "medium", "rare", "w2 w3", "common medium"):
            found = report(
                f"search {words!r}",
                lambda w=words: reader.search_messages(w, None, None, None, 20, None),
            )
            print(f"  {len(found['messages'])} results")
        month = (date(2026, 3, 1), date(2026, 3, 31))
        report(
            "search 'common' in a month",
            lambda: reader.search_messages("common", None, *month, 20, None),
        )
        for name, conversation in (("busiest", busiest[0]), ("quietest", quiet[0])):
            for words in ("common", "rare"):
                report(
                    f"search {words!r} in the {name} conversation",
                    lambda w=words, c=conversation: reader.search_messages(
                        w, c, None, None, 20, None
                    ),
                )
        config = self.root / "bench-messages.json"
        raw = {
            "schema_version": 1,
            "id": "timeline_messages_bench",
            "connections": [
                {"id": "m", "integration": "apple_messages", "settings": {}}
            ],
            "analyses": [messages_timeline("timeline", reader_role=READER)],
        }
        config.write_text(json.dumps(raw))
        service = MiroirService(
            config, self.store, self.root / "artifacts", InlineExecutor(self.fail)
        )
        client = TestClient(web_app(service, TOKEN, timeline=database))
        headers = {**BEARER, "Accept-Encoding": "gzip"}
        for name, route in (
            ("route: list of 50", "/api/timeline/conversations"),
            (
                "route: latest page of 100, busiest",
                f"/api/timeline/conversations/{busiest[0]}/messages",
            ),
        ):
            response, median, worst = timed(
                lambda r=route: client.get(r, headers=headers)
            )
            self.assertEqual(response.status_code, 200)
            print(
                f"{name}: {median:.1f} ms median, {worst:.1f} ms worst,"
                f" {len(response.content) / 1024:.0f} KB,"
                f" {response.num_bytes_downloaded / 1024:.1f} KB on the wire"
            )
        self.assertEqual(len(listed["conversations"]), 50)
