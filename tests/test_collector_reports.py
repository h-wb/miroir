"""Collector reports (decision 039): endpoint, storage, status, pages, metrics."""

import copy
import hashlib
import json
import re
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch

import sqlalchemy as sa
from fastapi.testclient import TestClient

from src.core.operations import (
    SCHEMA_VERSION,
    MetadataStore,
    collector_reports,
    collector_summaries,
    schema_info,
)
from src.core.service import InlineExecutor, MiroirService
from src.web.app import SESSION_COOKIE, create_app
from src.web.metrics import render_metrics

ADMIN = "test-admin-token-0123456"
METRICS = "test-metrics-token-6543210"
REPORT = "/api/connections/screen/report"
MARKER = "echo-marker-7c1e"


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def report(**changes) -> dict:
    value = {
        "version": 1,
        "collector": "0.1.0",
        "source": "screenmemory",
        "started_at": "2026-11-21T04:00:00Z",
        "finished_at": "2026-11-21T04:00:07Z",
        "rows": {
            "outcome": "delivered",
            "counts": {"frames": 189, "masked": {"token": 2}},
            "step": None,
            "reason": None,
            "http_status": 201,
        },
        "media": {
            "outcome": "delivered",
            "counts": {"examined": 4, "uploaded": 3},
            "step": None,
            "reason": None,
            "http_status": None,
        },
        "pending": False,
        "unreported": 0,
    }
    value.update(changes)
    return value


def failed_rows(reason: str = "network") -> dict:
    return {
        "outcome": "failed",
        "counts": {"frames": 2},
        "step": "deliver",
        "reason": reason,
        "http_status": None,
    }


_SAMPLE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)\{((?:[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\\n]|\\["\\n])*",?)*)\} (\S+)$'
)
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\\n]|\\["\\n])*)"')


def _unescape(match: re.Match[str]) -> str:
    return "\n" if match.group(1) == "n" else match.group(1)


def parse_metrics(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """A small parser of the Prometheus text format, strict about this output."""
    assert text.endswith("\n"), "the exposition ends with a newline"
    helped: set[str] = set()
    typed: dict[str, str] = {}
    samples: dict[str, list[tuple[dict[str, str], float]]] = {}
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    for line in text.splitlines():
        if line.startswith("# HELP "):
            name, _, help_text = line[7:].partition(" ")
            assert help_text and name not in helped, line
            helped.add(name)
        elif line.startswith("# TYPE "):
            name, _, kind = line[7:].partition(" ")
            assert kind in {"gauge", "counter", "untyped"}, line
            assert name in helped and name not in typed, line
            typed[name] = kind
            samples[name] = []
        else:
            match = _SAMPLE.fullmatch(line)
            assert match, f"not a sample line: {line!r}"
            name, labels, value = match.groups()
            assert name in typed, f"sample before its TYPE: {line!r}"
            parsed = {
                key: re.sub(r"\\(.)", _unescape, raw)
                for key, raw in _LABEL.findall(labels)
            }
            identity = (name, tuple(sorted(parsed.items())))
            assert identity not in seen, f"duplicate sample: {line!r}"
            seen.add(identity)
            samples[name].append((parsed, float(value)))
    assert helped == set(typed)
    return samples


class ReportFixture(unittest.TestCase):
    metrics_token: str | None = None

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.config = self.root / "miroir.json"
        self.config.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "test",
                    "media": {"endpoint": "https://s3.example.org", "bucket": "media"},
                    "connections": [
                        {"id": "screen", "integration": "screenmemory", "settings": {}},
                        {
                            "id": "browser",
                            "integration": "browser_history",
                            "settings": {},
                        },
                    ],
                }
            )
        )
        self.store = MetadataStore(f"sqlite:///{self.root / 'meta.sqlite'}")
        self.addCleanup(self.store.engine.dispose)
        self.store.migrate()
        self.service = MiroirService(
            self.config, self.store, self.root / "artifacts", InlineExecutor(self.fail)
        )
        self.client = TestClient(
            create_app(self.service, ADMIN, metrics_token=self.metrics_token),
            follow_redirects=False,
        )
        self.tokens = {
            name: self.service.create_delivery_token(name)
            for name in ("screen", "browser")
        }
        self.screen = bearer(self.tokens["screen"])
        self.browser = bearer(self.tokens["browser"])

    def record(self, connection_id: str, body: dict) -> bool:
        return self.service.record_collector_report(
            connection_id, body, self.tokens[connection_id]
        )

    def stored(self, connection_id: str = "screen") -> list[dict]:
        with self.store.engine.connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    collector_reports.select()
                    .where(collector_reports.c.connection_id == connection_id)
                    .order_by(collector_reports.c.id)
                ).mappings()
            ]

    def send(self, body: dict, connection_id: str = "screen"):
        headers = self.screen if connection_id == "screen" else self.browser
        response = self.client.post(
            f"/api/connections/{connection_id}/report", json=body, headers=headers
        )
        self.assertEqual(response.status_code, 204, response.text)
        return response


class EndpointTests(ReportFixture):
    def test_authorisation(self):
        for headers in ({}, bearer("wrong"), self.browser, bearer(ADMIN)):
            with self.subTest(headers=headers):
                response = self.client.post(REPORT, json=report(), headers=headers)
                self.assertEqual(response.status_code, 401)
        self.assertFalse(self.stored())

    def test_valid_report_is_stored_and_answered_without_a_body(self):
        # Delivery-token routes are not subject to the admin origin check.
        response = self.client.post(
            REPORT,
            json=report(),
            headers={**self.screen, "Origin": "https://elsewhere.example"},
        )
        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")
        (row,) = self.stored()
        self.assertEqual(
            {
                key: row[key]
                for key in (
                    "miroir_id",
                    "collector",
                    "source",
                    "rows_outcome",
                    "media_outcome",
                    "failed",
                    "pending",
                    "unreported",
                )
            },
            {
                "miroir_id": "test",
                "collector": "0.1.0",
                "source": "screenmemory",
                "rows_outcome": "delivered",
                "media_outcome": "delivered",
                "failed": False,
                "pending": False,
                "unreported": 0,
            },
        )
        self.assertEqual(json.loads(row["report"]), report())
        self.assertIsNotNone(row["received_at"])

    def test_size_limit(self):
        body = json.dumps(report()).encode()
        body = body[:-1] + b" " * (16 * 1024 - len(body)) + b"}"
        self.assertEqual(len(body), 16 * 1024)
        headers = {**self.screen, "Content-Type": "application/json"}
        self.assertEqual(
            self.client.post(REPORT, content=body, headers=headers).status_code, 204
        )
        body = body[:-1] + b" }"
        response = self.client.post(REPORT, content=body, headers=headers)
        self.assertEqual(response.status_code, 413)
        self.assertEqual(len(self.stored()), 1)

    def test_invalid_bodies_are_refused_without_echo(self):
        def rows(**changes):
            value = report()["rows"]
            value.update(changes)
            return {"rows": value}

        cases = {
            "unknown key": report(**{MARKER: 1}),
            "unknown key in a pass": report(**rows(**{MARKER: 1})),
            "wrong outcome word": report(**rows(outcome=MARKER)),
            "string in counts": report(**rows(counts={"frames": MARKER})),
            "boolean in counts": report(**rows(counts={"frames": True})),
            "two levels deep": report(
                **rows(counts={"masked": {"token": {MARKER: 1}}})
            ),
            "markup in a counts key": report(**rows(counts={f"<b>{MARKER}": 1})),
            "a dot in a counts key": report(
                **rows(counts={"masked.token": 1, "masked": {"token": 2}})
            ),
            "a dot in a nested counts key": report(
                **rows(counts={"masked": {"a.b": 1}})
            ),
            "a count over 2**53": report(**rows(counts={"frames": 2**53 + 1})),
            "a huge count": report(**rows(counts={"frames": 10**400})),
            "a huge nested count": report(**rows(counts={"masked": {"t": 10**30}})),
            "a negative count": report(**rows(counts={"frames": -1})),
            "a time that overflows": report(started_at="9999-12-31T23:59:59-01:00"),
            "a time before 2000": report(started_at="1999-12-31T23:59:59Z"),
            "a time from 2100": report(finished_at="2100-01-01T00:00:00Z"),
            "a bad time": report(started_at=MARKER),
            "a time without offset": report(started_at="2026-11-21T04:00:00"),
            "uppercase step": report(**rows(step=MARKER.upper())),
            "http status out of range": report(**rows(http_status=99)),
            "version": report(version=2),
            "too many counts": report(**rows(counts={f"c{i}": i for i in range(41)})),
            "long collector": report(collector="v" * 41),
            "unreported too large": report(unreported=100_001),
            "pending not a boolean": report(pending=MARKER),
            "not an object": [MARKER],
        }
        for name, body in cases.items():
            with self.subTest(name=name):
                response = self.client.post(REPORT, json=body, headers=self.screen)
                self.assertEqual(response.status_code, 422)
                self.assertNotIn(MARKER, response.text)
                self.assertNotIn(MARKER.upper(), response.text)
                self.assertEqual(
                    response.json(),
                    {"error": "The report does not have the expected shape"},
                )
        response = self.client.post(
            REPORT,
            content=b"{" + MARKER.encode(),
            headers={**self.screen, "Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 422)
        self.assertNotIn(MARKER, response.text)
        self.assertFalse(self.stored())

    def test_a_newer_collector_word_is_accepted(self):
        self.send(report(rows=failed_rows("brand_new_reason")))
        self.assertEqual(self.stored()[0]["rows_outcome"], "failed")


class LimitTests(ReportFixture):
    def test_counts_and_times_at_the_limits_are_accepted(self):
        self.send(
            report(
                started_at="2000-01-01T00:00:00Z",
                finished_at="2099-12-31T23:59:59Z",
                rows={**report()["rows"], "counts": {"frames": 2**53, "zero": 0}},
            )
        )
        self.assertEqual(len(self.stored()), 1)


class RetentionTests(ReportFixture):
    def test_newest_500_are_kept_per_connection_and_removal_clears(self):
        for i in range(3):
            self.record("browser", report(unreported=i))
        for i in range(501):
            self.record("screen", report(unreported=i))
        kept = self.stored()
        self.assertEqual(len(kept), 500)
        self.assertEqual([row["unreported"] for row in kept], list(range(1, 501)))
        self.assertEqual(len(self.stored("browser")), 3)
        self.service.remove_connection("screen")
        self.assertFalse(self.stored())
        self.assertIsNone(self.store.collector_status("test", "screen"))
        self.assertEqual(len(self.stored("browser")), 3)

    def test_a_long_failure_streak_outlives_retention(self):
        self.record("screen", report())
        success = self.service.delivery_status("screen")["collector"]
        for _ in range(501):
            self.record("screen", report(rows=failed_rows()))
        self.assertEqual(len(self.stored()), 500)
        self.assertTrue(all(row["failed"] for row in self.stored()))
        status = self.service.delivery_status("screen")["collector"]
        self.assertEqual(status["consecutive_failures"], 501)
        self.assertEqual(
            status["last_success_at"], success["last_report"]["received_at"]
        )
        self.record("screen", report())
        status = self.service.delivery_status("screen")["collector"]
        self.assertEqual(status["consecutive_failures"], 0)
        self.assertEqual(
            status["last_success_at"], status["last_report"]["received_at"]
        )

    def test_a_late_report_does_not_reach_a_new_connection_of_the_same_id(self):
        self.send(report())
        old = self.tokens["screen"]
        # Authorised, then the connection is removed, then the report lands.
        self.assertTrue(self.service.verify_delivery_token("screen", old))
        self.service.remove_connection("screen")
        self.assertFalse(self.record("screen", report(unreported=9)))
        self.service.create_connection("screenmemory", "screen", {}, {})
        self.assertIsNone(self.service.delivery_status("screen")["collector"])
        self.assertFalse(self.stored())

    def test_a_report_in_flight_across_removal_and_re_creation_is_dropped(self):
        old = self.tokens["screen"]
        # Authorised, removal, re-creation with a new token, then the insert.
        self.assertTrue(self.service.verify_delivery_token("screen", old))
        self.service.remove_connection("screen")
        self.service.create_connection("screenmemory", "screen", {}, {})
        self.tokens["screen"] = self.service.create_delivery_token("screen")
        self.assertFalse(
            self.service.record_collector_report("screen", report(unreported=9), old)
        )
        self.assertFalse(self.stored())
        self.assertIsNone(self.service.delivery_status("screen")["collector"])
        self.assertTrue(self.record("screen", report()))
        self.assertEqual(len(self.stored()), 1)

    def test_the_endpoint_refuses_a_report_whose_token_went_away_meanwhile(self):
        def revoke_first(connection_id, body, token):
            self.service.revoke_delivery_token(connection_id)
            return recorder(connection_id, body, token)

        recorder = self.service.record_collector_report
        with patch.object(self.service, "record_collector_report", revoke_first):
            response = self.client.post(REPORT, json=report(), headers=self.screen)
        self.assertEqual(response.status_code, 401)
        self.assertFalse(self.stored())


class OrderTests(ReportFixture):
    def test_latest_is_the_last_written_whatever_its_timestamp(self):
        later = datetime(2026, 11, 21, 5, 0, tzinfo=timezone.utc)
        earlier = later - timedelta(seconds=1)
        # Two writers whose clocks were read in the opposite order to their writes.
        with patch("src.core.operations._now", side_effect=[later, earlier]):
            self.record("screen", report())
            self.record("screen", report(rows=failed_rows(), unreported=7))
        status = self.service.delivery_status("screen")["collector"]
        self.assertEqual(status["last_report"]["unreported"], 7)
        self.assertTrue(status["last_report"]["failed"])
        self.assertEqual(status["consecutive_failures"], 1)
        self.assertEqual(status["last_report"]["received_at"], earlier.isoformat())
        self.assertEqual(status["last_success_at"], later.isoformat())
        self.assertEqual([row["unreported"] for row in self.stored()], [0, 7])

    def test_two_threads_record_one_consistent_history(self):
        errors = []

        def write(i):
            try:
                self.record("screen", report(rows=failed_rows(), unreported=i))
            except Exception as error:  # noqa: BLE001 - collected for the assertion
                errors.append(error)

        threads = [Thread(target=write, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(30)
        self.assertFalse(errors)
        rows = self.stored()
        status = self.service.delivery_status("screen")["collector"]
        self.assertEqual(status["consecutive_failures"], 8)
        self.assertEqual(status["last_report"]["unreported"], rows[-1]["unreported"])


class HiddenParameterTests(ReportFixture):
    def test_store_errors_do_not_carry_bound_values(self):
        token_hash = hashlib.sha256(self.tokens["screen"].encode()).hexdigest()
        body = report(collector=None, source=MARKER)
        with self.assertRaises(sa.exc.IntegrityError) as caught:
            self.store.record_collector_report("test", "screen", token_hash, body)
        text = str(caught.exception) + repr(caught.exception)
        self.assertNotIn(MARKER, text)
        self.assertNotIn(token_hash, text)
        with self.store.engine.begin() as db:
            collector_summaries.drop(db)
        with self.assertRaises(sa.exc.OperationalError) as caught:
            self.store.record_collector_report(
                "test", "screen", token_hash, report(source=MARKER)
            )
        text = str(caught.exception) + repr(caught.exception)
        self.assertNotIn(MARKER, text)
        self.assertNotIn(token_hash, text)
        self.assertFalse(self.stored())


class StatusTests(ReportFixture):
    def test_last_report_last_success_and_consecutive_failures(self):
        self.assertIsNone(self.service.delivery_status("screen")["collector"])
        sequence = [
            report(),
            report(rows=failed_rows(), pending=True),
            report(media=None, rows=failed_rows("timeout"), unreported=3),
            report(rows={**report()["rows"], "outcome": "nothing_new"}),
        ]
        seen = []
        for body in sequence:
            self.send(body)
            status = self.client.get("/api/connections/screen", headers=bearer(ADMIN))
            self.assertEqual(status.status_code, 200)
            collector = status.json()["delivery"]["collector"]
            seen.append(collector)
        received = [item["last_report"]["received_at"] for item in seen]
        self.assertEqual([item["consecutive_failures"] for item in seen], [0, 1, 2, 0])
        self.assertEqual(
            [item["last_success_at"] for item in seen],
            [received[0], received[0], received[0], received[3]],
        )
        self.assertEqual(
            [item["last_report"]["failed"] for item in seen],
            [False, True, True, False],
        )
        third = seen[2]["last_report"]
        self.assertEqual(
            (
                third["collector"],
                third["source"],
                third["rows"]["step"],
                third["rows"]["reason"],
                third["rows"]["counts"],
                third["media"],
                third["pending"],
                third["unreported"],
            ),
            (
                "0.1.0",
                "screenmemory",
                "deliver",
                "timeout",
                {"frames": 2},
                None,
                False,
                3,
            ),
        )
        overview = self.client.get("/api/overview", headers=bearer(ADMIN)).json()
        by_id = {c["id"]: c["delivery"]["collector"] for c in overview["connections"]}
        self.assertIsNone(by_id["browser"])
        self.assertEqual(by_id["screen"]["last_report"]["received_at"], received[3])


class PageTests(ReportFixture):
    def page(self, path: str) -> str:
        response = self.client.get(path, headers=bearer(ADMIN))
        self.assertEqual(response.status_code, 200)
        return response.text

    def test_connection_page_block(self):
        self.assertNotIn("<h2>Collector</h2>", self.page("/connections/screen"))
        self.assertNotIn("collector reported", self.page("/"))
        self.send(report())
        text = self.page("/connections/screen")
        self.assertIn("<h2>Collector</h2>", text)
        self.assertIn("version 0.1.0", text)
        self.assertIn("frames 189, masked.token 2", text)
        self.assertIn("examined 4, uploaded 3", text)
        self.assertNotIn("Stopped while", text)
        self.assertNotIn("could not be reported", text)
        self.assertNotIn("in a row", text)
        self.send(report(rows=failed_rows()))
        self.send(report(rows=failed_rows("http") | {"http_status": 502}, unreported=3))
        text = self.page("/connections/screen")
        self.assertIn("Stopped while delivering rows: http (HTTP 502).", text)
        self.assertIn("3 runs could not be reported before this one.", text)
        self.assertIn("2 failed runs in a row.", text)
        self.assertNotIn("<h2>Collector</h2>", self.page("/connections/browser"))

    def test_overview_shows_the_age_and_the_mark(self):
        self.send(report())
        text = self.page("/")
        self.assertIn("collector reported <time", text)
        self.assertNotIn("collector run failed", text)
        self.send(report(rows=failed_rows()))
        self.assertIn("collector run failed", self.page("/"))

    def test_report_values_are_escaped(self):
        self.send(report(source="<b>x</b>", collector='"><script>'))
        text = self.page("/connections/screen")
        self.assertIn("&lt;b&gt;x&lt;/b&gt;", text)
        self.assertNotIn("<script>", text)


class MetricsDisabledTests(ReportFixture):
    def test_not_found_without_the_variable(self):
        for headers in ({}, bearer(ADMIN), bearer(METRICS)):
            self.assertEqual(
                self.client.get("/metrics", headers=headers).status_code, 404
            )


class MetricsTests(ReportFixture):
    metrics_token = METRICS

    def test_refuses_other_credentials(self):
        self.assertEqual(self.client.get("/metrics").status_code, 401)
        for headers in (bearer("wrong"), bearer(ADMIN), self.screen):
            self.assertEqual(
                self.client.get("/metrics", headers=headers).status_code, 401
            )
        login = self.client.post("/login", data={"token": ADMIN})
        self.assertEqual(login.status_code, 303)
        self.assertIn(SESSION_COOKIE, self.client.cookies)
        self.assertEqual(self.client.get("/metrics").status_code, 401)

    def test_scrape(self):
        received, _ = self.store.register_artifact(
            miroir_id="test",
            connection_id="browser",
            sha256="a" * 64,
            size=1,
            input_format="zen_history_batch",
            options="{}",
            original_name="batch.jsonl.gz",
            storage_path="/unused/batch",
            retention="keep",
        )
        self.send(report())
        self.send(report(rows=failed_rows(), pending=True, unreported=4))
        status = self.service.delivery_status("screen")["collector"]
        response = self.client.get("/metrics", headers=bearer(METRICS))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/plain; version=0.0.4")
        samples = parse_metrics(response.text)

        def one(name):
            return {labels["connection"]: value for labels, value in samples[name]}

        from datetime import datetime

        def epoch(value):
            return datetime.fromisoformat(value).timestamp()

        self.assertEqual(
            one("miroir_connection_last_received_timestamp_seconds"),
            {"browser": epoch(received["last_received_at"])},
        )
        self.assertEqual(
            one("miroir_collector_last_report_timestamp_seconds"),
            {"screen": epoch(status["last_report"]["received_at"])},
        )
        self.assertEqual(
            one("miroir_collector_last_success_timestamp_seconds"),
            {"screen": epoch(status["last_success_at"])},
        )
        self.assertEqual(one("miroir_collector_consecutive_failures"), {"screen": 1})
        self.assertEqual(one("miroir_collector_pending"), {"screen": 1})
        self.assertEqual(one("miroir_collector_unreported_runs"), {"screen": 4})
        self.assertEqual(
            sorted(
                (labels["connection"], labels["part"], labels["name"], value)
                for labels, value in samples["miroir_collector_last_count"]
            ),
            [
                ("screen", "media", "examined", 4),
                ("screen", "media", "uploaded", 3),
                ("screen", "rows", "frames", 2),
            ],
        )

    def test_an_odd_stored_report_does_not_break_the_scrape(self):
        self.send(report(), "browser")
        # Stored past the validator, as an older or damaged record might be.
        self.record(
            "screen", report(rows={**report()["rows"], "counts": {"frames": 10**400}})
        )
        response = self.client.get("/metrics", headers=bearer(METRICS))
        self.assertEqual(response.status_code, 200)
        samples = parse_metrics(response.text)
        reported = {
            labels["connection"]
            for labels, _ in samples["miroir_collector_last_report_timestamp_seconds"]
        }
        self.assertEqual(reported, {"browser"})
        for odd in (
            {"frames": "many"},
            {"frames": [1]},
            {"frames": float("inf")},
            {"masked": {"token": True}},
        ):
            status = {
                "last_report": {
                    **report(rows={**report()["rows"], "counts": odd}),
                    "received_at": "2026-11-21T04:00:08+00:00",
                    "failed": False,
                },
                "last_success_at": None,
                "consecutive_failures": 0,
            }
            with self.subTest(counts=odd):
                text = render_metrics(
                    [
                        ("odd", {"last_received_at": None, "collector": status}),
                        (
                            "fine",
                            {
                                "last_received_at": "2026-11-21T04:00:08+00:00",
                                "collector": None,
                            },
                        ),
                    ]
                )
                samples = parse_metrics(text)
                self.assertEqual(
                    samples["miroir_connection_last_received_timestamp_seconds"],
                    [({"connection": "fine"}, 1795233608.0)],
                )
                self.assertEqual(samples["miroir_collector_last_count"], [])

    def test_nested_counts_and_label_escaping(self):
        status = {
            "last_report": {
                **report(),
                "received_at": "2026-11-21T04:00:08+00:00",
                "failed": False,
            },
            "last_success_at": None,
            "consecutive_failures": 0,
        }
        name = 'odd"name\\with\nnewline'
        samples = parse_metrics(
            render_metrics([(name, {"last_received_at": None, "collector": status})])
        )
        self.assertEqual(samples["miroir_collector_last_success_timestamp_seconds"], [])
        self.assertEqual(
            samples["miroir_collector_last_report_timestamp_seconds"],
            [({"connection": name}, 1795233608.0)],
        )
        self.assertIn(
            ({"connection": name, "part": "rows", "name": "masked.token"}, 2.0),
            samples["miroir_collector_last_count"],
        )


class MigrationTests(unittest.TestCase):
    def test_version_6_store_gains_the_table_and_keeps_its_data(self):
        # Version 7 added these tables; later versions upgrade through it.
        self.assertGreaterEqual(SCHEMA_VERSION, 7)
        with TemporaryDirectory() as tmp:
            store = MetadataStore(f"sqlite:///{Path(tmp) / 'meta.sqlite'}")
            self.addCleanup(store.engine.dispose)
            store.migrate()
            store.set_delivery_token("test", "screen", "a" * 64)
            with store.engine.begin() as db:
                collector_reports.drop(db)
                collector_summaries.drop(db)
                db.execute(schema_info.update().values(version=6))
            self.assertFalse(sa.inspect(store.engine).has_table("collector_reports"))
            store.migrate()
            for table in ("collector_reports", "collector_summaries"):
                self.assertTrue(sa.inspect(store.engine).has_table(table))
            indexes = {
                tuple(index["column_names"])
                for index in sa.inspect(store.engine).get_indexes("collector_reports")
            }
            self.assertIn(("miroir_id", "connection_id", "received_at"), indexes)
            self.assertEqual(
                store.delivery_token("test", "screen")["token_hash"], "a" * 64
            )
            with store.engine.connect() as db:
                self.assertEqual(
                    db.execute(sa.select(schema_info.c.version)).scalar(),
                    SCHEMA_VERSION,
                )
            self.assertTrue(
                store.record_collector_report(
                    "test", "screen", "a" * 64, copy.deepcopy(report())
                )
            )
            self.assertEqual(
                store.collector_status("test", "screen")["consecutive_failures"], 0
            )


if __name__ == "__main__":
    unittest.main()
