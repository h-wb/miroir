"""Thumbnails and `media_objects`, made by a derive run (decision 040).

The server queues confirmed files and requests a run; the run is executed here
in-process against DuckDB and a fake bucket. Every image is generated.
"""

import contextlib
import getpass
import io
import json
import os
import signal
import subprocess
import threading
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, ClassVar
from unittest.mock import patch

import dlt
import duckdb
import psycopg2
import sqlalchemy as sa
from fastapi.testclient import TestClient
from PIL import Image

from src.cli.main import main
from src.core.config import load_miroir
from src.core.derive import Decoder, DecoderLost
from src.core.media import ObjectTooLarge
from src.core.operations import (
    ConnectionBusy,
    MetadataStore,
    OperationError,
    media_pending,
    operations,
)
from src.core.runner import IngestionFailed, execute_operation, request_derive
from src.core.service import MiroirService
from src.web.app import create_app
from tests.fakes import FakeObjectStore
from tests.postgres import start_postgres

ADMIN = "test-admin-token-0123456"
CONFIRM = "/api/connections/screen/media/confirm"
UPLOADS = "/api/connections/screen/media/uploads"


def jpeg(width: int = 1600, height: int = 1000) -> bytes:
    out = BytesIO()
    image = Image.linear_gradient("L").resize((width, height)).convert("RGB")
    image.save(out, "JPEG", quality=80)
    return out.getvalue()


class Recorder:
    """An executor that records what it was handed and binds a backend run to
    it, as the Prefect adapter does."""

    def __init__(self):
        self.submitted: list[str] = []
        self.refuse = False
        self.store: MetadataStore | None = None

    def submit(self, operation_id: str) -> None:
        if self.refuse:
            raise OperationError("Prefect is down")
        self.submitted.append(operation_id)
        if self.store is not None:
            self.store.bind_backend_run(operation_id, f"run-{operation_id}")


class Killed(BaseException):
    """Stands in for the run's own process dying (a kill, out of memory)."""


class DeriveFixture(unittest.TestCase):
    media: ClassVar[dict[str, Any]] = {}

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.config_path = self.root / "miroir.json"
        self.config_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "id": "test",
                    "media": {
                        "endpoint": "https://s3.example.org",
                        "bucket": "media",
                        "prefix": "site",
                        **self.media,
                    },
                    "connections": [
                        {"id": "screen", "integration": "screenmemory", "settings": {}},
                        {
                            "id": "messages",
                            "integration": "apple_messages",
                            "settings": {},
                        },
                    ],
                }
            )
        )
        self.store = MetadataStore(self.metadata_url())
        self.addCleanup(self.store.engine.dispose)
        self.store.migrate()
        self.objects = FakeObjectStore()
        self.executor = Recorder()
        self.executor.store = self.store
        self.service = MiroirService(
            self.config_path,
            self.store,
            self.root / "artifacts",
            self.executor,
            object_store=self.objects,
        )
        self.client = TestClient(
            create_app(self.service, ADMIN), follow_redirects=False
        )
        self.token = {
            "Authorization": f"Bearer {self.service.create_delivery_token('screen')}"
        }
        self.scheduled: list[tuple[str, datetime]] = []
        self.warehouse = self.root / "warehouse.duckdb"

    def metadata_url(self) -> str:
        return f"sqlite:///{self.root / 'meta.sqlite'}"

    def destination(self) -> Any:
        return dlt.destinations.duckdb(credentials=str(self.warehouse))

    def miroir(self):
        return load_miroir(self.config_path)

    def upload(self, files: dict[str, bytes]) -> None:
        """What a device does: store each file, then confirm them."""
        for key, body in files.items():
            self.objects.add(f"site/screen/screenshots/{key}", body)
        objects = [{"key": key, "size": len(body)} for key, body in files.items()]
        response = self.client.post(
            CONFIRM,
            json={"kind": "screenshots", "objects": objects},
            headers=self.token,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual({o["status"] for o in response.json()["objects"]}, {"stored"})

    def derive(self, operation_id: str, defer=None, object_store="fake"):
        return execute_operation(
            self.miroir(),
            operation_id,
            store=self.store,
            destination=self.destination(),
            resolve_credentials=lambda c: {},
            pipelines_dir=str(self.root / "dlt"),
            object_store=self.objects if object_store == "fake" else object_store,
            defer=defer,
        )

    def request(self, backfill=None) -> str:
        """A derive run due now, as a backfill or the next batch records one."""
        operation_id = request_derive(
            self.miroir(),
            "screen",
            store=self.store,
            origin="request",
            submit=lambda operation: None,
            backfill=backfill,
            join=False,
        )
        assert operation_id is not None
        return operation_id

    def queue(self, files: dict[str, bytes]) -> None:
        """Files stored and queued without the web request."""
        now = datetime.now(timezone.utc)
        for key, body in files.items():
            self.objects.add(f"site/screen/screenshots/{key}", body)
        self.store.add_pending_media(
            "test",
            "screen",
            "screenshots",
            [(key, len(body), now) for key, body in files.items()],
        )

    def schedule(self, operation_id: str, start_at: datetime) -> None:
        self.scheduled.append((operation_id, start_at))

    def rows(self) -> dict[str, dict]:
        dataset = self.miroir().connections[0].binding.dataset_name
        with duckdb.connect(str(self.warehouse)) as db:
            cursor = db.execute(f"SELECT * FROM {dataset}.media_objects")
            names = [c[0] for c in cursor.description]
            return {row[2]: dict(zip(names, row)) for row in cursor.fetchall()}

    def pending(self) -> list[str]:
        with self.store.engine.connect() as db:
            return sorted(db.execute(sa.select(media_pending.c.key)).scalars())

    def attempts(self) -> dict[str, int]:
        queued = self.store.pending_media("test", "screen", "screenshots", 100)
        return {entry.key: entry.attempts for entry in queued}

    def detail(self, operation_id: str) -> str:
        return self.store.attempts(operation_id)[-1]["transformation_detail"]


class DeriveRequestTests(DeriveFixture):
    def test_confirmations_share_one_debounced_run(self):
        before = datetime.now(timezone.utc)
        self.upload({"a.jpeg": jpeg()})
        self.upload({"b.jpeg": jpeg()})
        self.upload({"c.png": b"x"})
        self.assertEqual(self.pending(), ["a.jpeg", "b.jpeg", "c.png"])
        self.assertEqual(len(self.executor.submitted), 1)
        operation = self.store.get_operation(self.executor.submitted[0])
        self.assertEqual((operation.kind, operation.origin), ("derive", "webhook"))
        due = datetime.fromisoformat(json.loads(operation.options)["not_before"])
        self.assertGreaterEqual(due, before + timedelta(seconds=300))
        self.assertLess(due, before + timedelta(seconds=310))
        # A run that has started is not joined: the next confirmation asks again.
        self.store.start_attempt(operation.id, "not_run")
        self.upload({"d.jpeg": jpeg()})
        self.assertEqual(len(self.executor.submitted), 2)

    def test_a_derive_is_not_a_load(self):
        self.upload({"a.jpeg": jpeg()})
        self.derive(self.executor.submitted[0])
        status = self.service.connection("screen")["status"]
        self.assertIsNone(status["last_attempt"])
        self.assertIsNone(status["last_ingestion_success"])
        self.upload({"b.jpeg": jpeg()})
        self.assertEqual(status["requested_not_started"], 0)
        self.assertEqual(
            self.service.connection("screen")["status"]["requested_not_started"], 0
        )
        self.assertEqual(
            self.store.recent_operations("test", "screen", requests_only=True), []
        )

    def test_a_lost_derive_is_closed_as_a_transformation(self):
        self.upload({"a.jpeg": jpeg()})
        died = self.executor.submitted[0]  # bound to its run, which ended
        self.store.close_unverified(died, None, "lost", "ended without a word")
        attempt = self.store.attempts(died)[-1]
        self.assertEqual(
            (attempt["ingestion"], attempt["transformation"]), ("not_run", "lost")
        )

    def test_a_waiting_run_is_joined_however_late_it_is(self):
        # A long load holds the deployment: the derive run is long overdue.
        self.upload({"a.jpeg": jpeg()})
        first = self.executor.submitted[0]
        with self.store.engine.begin() as db:
            stale = datetime.now(timezone.utc) - timedelta(hours=6)
            db.execute(
                operations.update()
                .where(operations.c.id == first)
                .values(options=json.dumps({"not_before": stale.isoformat()}))
            )
        self.upload({"b.jpeg": jpeg()})
        self.assertEqual(len(self.executor.submitted), 1)

    def test_a_waiting_run_without_its_own_backend_run_is_not_joined(self):
        self.executor.store = None  # e.g. a local executor: nothing bound
        self.upload({"a.jpeg": jpeg()})
        self.upload({"b.jpeg": jpeg()})
        self.assertEqual(len(self.executor.submitted), 2)

    def test_the_connection_page_shows_loads_and_one_media_line(self):
        self.upload({"a.jpeg": jpeg()})
        self.derive(self.executor.submitted[0])
        self.upload({"b.jpeg": jpeg()})
        view = self.service.connection("screen")
        self.assertEqual(view["operations"], [])
        self.assertEqual(view["media"]["waiting"], 1)
        self.assertEqual(view["media"]["last_attempt"]["transformation"], "succeeded")
        self.client.post("/login", data={"token": ADMIN})
        page = self.client.get("/connections/screen").text
        self.assertIn("Thumbnails: 1 file waiting", page)
        self.assertIn("Media: made 1; 0 waiting", page)
        self.assertNotIn("derive", page.split("<h2>History</h2>")[1])

    def test_files_found_present_are_queued_too(self):
        body = jpeg()
        self.objects.add("site/screen/screenshots/a.jpeg", body)
        response = self.client.post(
            UPLOADS,
            json={"kind": "screenshots", "objects": [{"key": "a.jpeg", "size": 9}]},
            headers=self.token,
        )
        self.assertEqual(response.json()["objects"][0]["status"], "upload")
        self.assertEqual(self.pending(), [])
        response = self.client.post(
            UPLOADS,
            json={
                "kind": "screenshots",
                "objects": [{"key": "a.jpeg", "size": len(body)}],
            },
            headers=self.token,
        )
        self.assertEqual(response.json()["objects"][0]["status"], "present")
        self.assertEqual(self.pending(), ["a.jpeg"])

    def test_a_kind_without_thumbnails_is_not_queued(self):
        token = self.service.create_delivery_token("messages")
        body = jpeg()
        self.objects.add("site/messages/attachments/g.jpeg", body)
        response = self.client.post(
            "/api/connections/messages/media/confirm",
            json={
                "kind": "attachments",
                "objects": [{"key": "g.jpeg", "size": len(body)}],
            },
            headers={"Authorization": f"Bearer {token}"},
        )
        self.assertEqual(response.json()["objects"][0]["status"], "stored")
        self.assertEqual((self.pending(), self.executor.submitted), ([], []))

    def test_a_refused_submission_keeps_the_files_and_the_answer(self):
        self.executor.refuse = True
        with self.assertLogs("src.core.service", "WARNING"):
            self.upload({"a.jpeg": jpeg()})
        self.assertEqual(self.pending(), ["a.jpeg"])
        refused = self.store.recent_operations("test", "screen")[0]
        self.assertEqual(refused["attempts"][0]["error_type"], "SubmitFailed")
        # Closed, so the next confirmation is not joined to it.
        self.executor.refuse = False
        self.upload({"b.jpeg": jpeg()})
        self.assertEqual(len(self.executor.submitted), 1)

    def test_a_deployment_that_cannot_be_prepared_closes_the_run(self):
        with (
            patch.object(
                self.service,
                "prepare_connection",
                side_effect=OperationError("Prefect is down"),
            ),
            self.assertLogs("src.core.service", "WARNING"),
        ):
            self.upload({"a.jpeg": jpeg()})
        self.assertEqual(self.store.unstarted_derives("test", "screen"), [])
        self.upload({"b.jpeg": jpeg()})
        self.assertEqual(len(self.executor.submitted), 1)

    def test_another_request_deciding_is_enough(self):
        with patch(
            "src.core.runner.request_derive", side_effect=ConnectionBusy("busy")
        ):
            self.upload({"a.jpeg": jpeg()})
        self.assertEqual((self.pending(), self.executor.submitted), (["a.jpeg"], []))

    def test_a_queue_that_cannot_be_written_fails_the_confirmation(self):
        self.objects.add("site/screen/screenshots/a.jpeg", b"xy")
        failure = sa.exc.OperationalError("INSERT", {}, Exception("down"))
        with (
            patch.object(self.store, "add_pending_media", side_effect=failure),
            self.assertLogs("src.core.service", "ERROR"),
        ):
            response = self.client.post(
                CONFIRM,
                json={"kind": "screenshots", "objects": [{"key": "a.jpeg", "size": 2}]},
                headers=self.token,
            )
        self.assertGreaterEqual(response.status_code, 400)
        self.assertIn("send them again", response.text)

    def test_removing_the_connection_forgets_its_queue(self):
        self.upload({"a.jpeg": jpeg()})
        self.store.set_backfill_after("test", "screen", "screenshots", "a.jpeg")
        self.service.remove_connection("screen")
        self.assertEqual(self.pending(), [])
        self.assertEqual(self.store.backfill_after("test", "screen", "screenshots"), "")


class DeriveRunTests(DeriveFixture):
    def test_every_confirmed_file_gets_a_row(self):
        big = jpeg(2880, 1800)
        self.upload(
            {
                "2026/11/20/a.jpeg": big,
                "2026/11/20/b.heic": b"heic bytes",
                "2026/11/20/c.jpeg": big[: len(big) // 3],
                "2026/11/20/d.png": b"not an image",
                "2026/11/20/gone.jpeg": jpeg(),
            }
        )
        del self.objects.bodies["site/screen/screenshots/2026/11/20/gone.jpeg"]
        operation_id = self.executor.submitted[0]
        outcome = self.derive(operation_id)
        self.assertEqual(
            (outcome.ingestion, outcome.transformation), ("not_run", "succeeded")
        )
        rows = self.rows()
        self.assertEqual(
            {key: row["thumbnail_status"] for key, row in rows.items()},
            {
                "2026/11/20/a.jpeg": "made",
                "2026/11/20/b.heic": "unsupported",
                "2026/11/20/c.jpeg": "unreadable",
                "2026/11/20/d.png": "unreadable",
            },
        )
        made = rows["2026/11/20/a.jpeg"]
        self.assertEqual(
            (made["connection_id"], made["kind"], made["size"], made["content_type"]),
            ("screen", "screenshots", len(big), "image/jpeg"),
        )
        self.assertEqual(
            (made["thumbnail_w360"], made["thumbnail_w1280"]), (True, True)
        )
        self.assertEqual(made["derive_version"], 1)
        heic = rows["2026/11/20/b.heic"]
        self.assertEqual(
            (heic["thumbnail_w360"], heic["thumbnail_w1280"], heic["content_type"]),
            (False, False, "image/heic"),
        )
        for width, size in ((360, (360, 225)), (1280, (1280, 800))):
            body, content_type = self.objects.bodies[
                f"site/screen/screenshots.w{width}/2026/11/20/a.jpeg.webp"
            ]
            self.assertEqual(content_type, "image/webp")
            self.assertEqual(Image.open(BytesIO(body)).size, size)
        derived = [n for n in self.objects.bodies if ".w" in n.split("/")[2]]
        self.assertEqual(len(derived), 2)
        # A type the kind makes no thumbnail of is not even read.
        self.assertNotIn("site/screen/screenshots/2026/11/20/b.heic", self.objects.gets)
        self.assertEqual(self.pending(), [])
        self.assertEqual(
            self.detail(operation_id),
            "Media: gone 1, made 1, unreadable 2, unsupported 1; 0 waiting",
        )

    def test_files_already_derived_are_not_read_again(self):
        body = jpeg()
        self.queue({"a.jpeg": body})
        self.derive(self.request())
        self.objects.gets.clear()
        self.queue({"a.jpeg": body})
        again = self.request()
        self.derive(again)
        self.assertEqual(self.objects.gets, [])
        self.assertIn("known 1", self.detail(again))
        self.assertEqual(len(self.rows()), 1)
        # Another size is another file: derived again.
        self.queue({"a.jpeg": jpeg(800, 500)})
        self.derive(self.request())
        self.assertEqual(self.objects.gets, ["site/screen/screenshots/a.jpeg"])
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()["a.jpeg"]["size"], len(jpeg(800, 500)))

    def test_a_file_replaced_during_the_run_stays_queued(self):
        self.queue({"a.jpeg": jpeg()})
        newer = jpeg(800, 500)

        def replaced_meanwhile(decoder, body, formats):
            self.queue({"a.jpeg": newer})
            return decode(decoder, body, formats)

        with patch.object(Decoder, "thumbnails", replaced_meanwhile):
            self.derive(self.request(), defer=self.schedule)
        self.assertEqual(self.pending(), ["a.jpeg"])
        [(following, _)] = self.scheduled
        self.derive(following, defer=self.schedule)
        self.assertEqual(self.rows()["a.jpeg"]["size"], len(newer))
        self.assertEqual(self.pending(), [])

    def test_a_file_larger_than_its_kind_allows_is_too_large(self):
        self.queue({"a.jpeg": jpeg()})
        with patch.object(self.objects, "get", side_effect=ObjectTooLarge("big")):
            self.derive(self.request())
        self.assertEqual(self.rows()["a.jpeg"]["thumbnail_status"], "too_large")
        self.assertEqual(self.pending(), [])

    def test_a_storage_failure_keeps_what_is_left(self):
        self.queue({f"{i}.jpeg": jpeg(400, 300) for i in range(4)})
        real = self.objects.get
        calls = []

        def flaky(name, max_bytes):
            calls.append(name)
            if len(calls) == 3:
                raise ConnectionError("bucket unreachable")
            return real(name, max_bytes)

        operation_id = self.request()
        with (
            patch.object(self.objects, "get", side_effect=flaky),
            self.assertRaises(IngestionFailed),
        ):
            self.derive(operation_id)
        attempt = self.store.attempts(operation_id)[-1]
        self.assertEqual(attempt["transformation"], "failed")
        self.assertIn("Media storage is unavailable", attempt["error_message"])
        # Newest first; the two before the failure were recorded.
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(len(self.pending()), 2)
        self.derive(self.request())
        self.assertEqual((len(self.rows()), self.pending()), (4, []))

    def test_a_file_that_ends_the_run_costs_only_itself(self):
        # The reviewer's case: good files before a poison file, the run's own
        # process dying on the poison file each time, then a clean run.
        files = {f"{i:02d}.jpeg": jpeg(400, 300) for i in range(5)}
        files["99.jpeg"] = poison = jpeg(401, 300)
        self.queue(files)

        def dies_on_poison(decoder, body, formats):
            if body == poison:
                raise Killed()
            return decode(decoder, body, formats)

        for _ in range(3):
            with (
                patch.object(Decoder, "thumbnails", dies_on_poison),
                self.assertRaises(Killed),
            ):
                self.derive(self.request())
        self.assertEqual(self.attempts(), {**dict.fromkeys(files, 0), "99.jpeg": 3})
        self.objects.gets.clear()
        self.derive(self.request())
        self.assertEqual(
            {key: row["thumbnail_status"] for key, row in self.rows().items()},
            {**dict.fromkeys(files, "made"), "99.jpeg": "failed"},
        )
        self.assertNotIn("site/screen/screenshots/99.jpeg", self.objects.gets)

    def test_a_lost_decoder_costs_its_file_one_try_and_the_run_goes_on(self):
        lost = jpeg(801, 500)
        self.queue({"a.jpeg": jpeg(), "b.jpeg": lost, "c.jpeg": jpeg()})

        def lost_on_b(decoder, body, formats):
            if body == lost:
                raise DecoderLost()
            return decode(decoder, body, formats)

        for tries in range(1, 4):
            operation_id = self.request()
            with patch.object(Decoder, "thumbnails", lost_on_b):
                self.derive(operation_id, defer=self.schedule)
            self.assertIn("interrupted 1", self.detail(operation_id))
            self.assertEqual(self.attempts(), {"b.jpeg": tries})
            # It made progress, so the next run is scheduled to try again.
            self.assertEqual(len(self.scheduled), tries)
        self.derive(self.request(), defer=self.schedule)
        self.assertEqual(
            {key: row["thumbnail_status"] for key, row in self.rows().items()},
            {"a.jpeg": "made", "b.jpeg": "failed", "c.jpeg": "made"},
        )
        self.assertEqual(self.pending(), [])

    def test_without_storage_the_run_is_blocked(self):
        self.queue({"a.jpeg": jpeg()})
        operation_id = self.request()
        with self.assertRaises(OperationError):
            self.derive(operation_id, object_store=None)
        attempt = self.store.attempts(operation_id)[-1]
        self.assertEqual(attempt["ingestion"], "blocked")
        self.assertIn("not configured", attempt["error_message"])
        self.assertEqual(self.pending(), ["a.jpeg"])

    def test_a_settings_edit_does_not_block_a_derive(self):
        self.queue({"a.jpeg": jpeg()})
        operation_id = self.request()
        # As if the connection's settings were saved after the request.
        with self.store.engine.begin() as db:
            db.execute(
                operations.update()
                .where(operations.c.id == operation_id)
                .values(config_revision="an older revision")
            )
        self.assertEqual(self.derive(operation_id).transformation, "succeeded")
        self.assertEqual(self.rows()["a.jpeg"]["thumbnail_status"], "made")


class DecoderTests(unittest.TestCase):
    """The child process that decodes, with real generated images."""

    @classmethod
    def setUpClass(cls):
        out = BytesIO()
        Image.new("LA", (6000, 6000)).save(out, "PNG")
        cls.large = out.getvalue()  # about 36 MP: some 450 MB to decode

    def test_a_decode_that_takes_too_long_is_lost_and_the_next_file_works(self):
        decoder = Decoder(timeout=0.05)
        self.addCleanup(decoder.close)
        with self.assertRaises(DecoderLost):
            decoder.thumbnails(self.large, ("PNG",))
        decoder.timeout = 30
        self.assertEqual(decoder.thumbnails(jpeg(), ("JPEG",))[0], "made")

    def test_a_child_killed_while_decoding_is_lost(self):
        decoder = Decoder()
        self.addCleanup(decoder.close)
        self.assertEqual(decoder.thumbnails(jpeg(), ("JPEG",))[0], "made")
        pid = decoder._child.pid
        timer = threading.Timer(0.05, os.kill, (pid, signal.SIGKILL))
        timer.start()
        self.addCleanup(timer.cancel)
        with self.assertRaises(DecoderLost):
            decoder.thumbnails(self.large, ("PNG",))
        self.assertEqual(
            decoder.thumbnails(b"not an image", ("JPEG",))[0], "unreadable"
        )

    def test_the_memory_limit_makes_too_large_not_a_crash(self):
        decoder = Decoder(memory=200 * 1024 * 1024)
        self.addCleanup(decoder.close)
        self.assertEqual(decoder.thumbnails(self.large, ("PNG",)), ("too_large", {}))
        self.assertEqual(decoder.thumbnails(jpeg(), ("JPEG",))[0], "made")

    def test_the_child_has_no_credentials(self):
        decoder = Decoder()
        self.addCleanup(decoder.close)
        secrets = {"MIROIR_MEDIA_SECRET_ACCESS_KEY": "secret", "PGPASSWORD": "x"}
        with (
            patch.dict("os.environ", secrets),
            patch("src.core.derive.subprocess.Popen", wraps=subprocess.Popen) as popen,
        ):
            decoder.thumbnails(jpeg(), ("JPEG",))
        self.assertEqual(set(popen.call_args.kwargs["env"]), {"PATH"})


class DeriveBatchTests(DeriveFixture):
    media: ClassVar[dict[str, Any]] = {"derive_batch": 2}

    def test_a_full_batch_schedules_the_next_now(self):
        self.queue({f"{i}.png": b"x" for i in range(5)})
        first = self.request()
        before = datetime.now(timezone.utc)
        self.derive(first, defer=self.schedule)
        self.assertEqual(len(self.rows()), 2)
        self.assertIn("3 waiting", self.detail(first))
        [(following, due)] = self.scheduled
        self.assertGreaterEqual(due, before)
        self.assertLess(due, before + timedelta(seconds=30))
        self.assertEqual(self.store.get_operation(following).kind, "derive")
        self.derive(following, defer=self.schedule)
        self.derive(self.scheduled[-1][0], defer=self.schedule)
        self.assertEqual((len(self.rows()), len(self.scheduled)), (5, 2))

    def test_a_refused_schedule_closes_the_next_run(self):
        self.queue({f"{i}.png": b"x" for i in range(3)})

        def refuse(operation_id, start_at):
            raise RuntimeError("Prefect is down")

        with self.assertLogs("src.core.runner", "WARNING"):
            self.derive(self.request(), defer=refuse)
        [waiting] = [
            o
            for o in self.store.recent_operations("test", "screen")
            if not o["attempts"] or o["attempts"][0]["error_type"] == "NotScheduled"
        ][:1]
        self.assertEqual(waiting["attempts"][0]["error_type"], "NotScheduled")
        self.assertEqual(self.store.unstarted_derives("test", "screen"), [])

    def test_a_run_that_changes_nothing_schedules_nothing(self):
        # The reviewer's case: an entry of a kind that is not derived (renamed,
        # or its thumbnails removed) must not keep scheduling runs.
        old = datetime.now(timezone.utc)
        self.store.add_pending_media(
            "test", "screen", "screenshots_old", [("x.jpeg", 5, old)]
        )
        operation_id = self.request()
        self.derive(operation_id, defer=self.schedule)
        self.assertEqual(self.detail(operation_id), "Media: nothing queued; 0 waiting")
        self.assertEqual(self.scheduled, [])

    def test_without_a_scheduler_one_run_drains_the_queue(self):
        self.queue({f"{i}.png": b"x" for i in range(5)})
        operation_id = self.request()
        self.derive(operation_id)
        self.assertEqual((len(self.rows()), self.pending()), (5, []))
        self.assertEqual(self.detail(operation_id), "Media: unreadable 5; 0 waiting")


class BackfillTests(DeriveFixture):
    def setUp(self):
        super().setUp()
        old = datetime(2025, 1, 1, tzinfo=timezone.utc)
        for i in range(5):
            self.objects.add(
                f"site/screen/screenshots/2025/{i}.png", b"x", modified=old
            )
        # Not files of the kind: a thumbnail, another kind, another connection.
        self.objects.add("site/screen/screenshots.w360/2025/0.png.webp", b"w")
        self.objects.add("site/screen/other/x.png", b"o")
        self.objects.add("site/screens/screenshots/y.png", b"o")
        self.objects.add("site/screen/screenshots/bad//key.png", b"o")

    def test_a_walk_continues_where_it_stopped_and_starts_over_when_done(self):
        first = self.request({"kind": "screenshots", "limit": 3})
        self.derive(first)
        self.assertEqual(
            sorted(self.rows()), ["2025/0.png", "2025/1.png", "2025/2.png"]
        )
        self.assertIn("backfill walked 3, walk continues", self.detail(first))
        self.assertEqual(
            self.store.backfill_after("test", "screen", "screenshots"), "2025/2.png"
        )
        self.assertEqual(
            self.rows()["2025/0.png"]["stored_at"],
            datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        second = self.request({"kind": "screenshots", "limit": 3})
        self.derive(second)
        # The store said nothing follows: finished without an empty page.
        self.assertIn("walked 3, walk finished", self.detail(second))
        self.assertEqual(len(self.rows()), 5)  # the invalid key is skipped
        self.assertEqual(self.store.backfill_after("test", "screen", "screenshots"), "")
        self.objects.gets.clear()
        again = self.request({"kind": "screenshots", "limit": 100})
        self.derive(again)
        self.assertEqual(self.objects.gets, [])
        self.assertIn("known 5", self.detail(again))
        self.assertIn("walked 6, walk finished", self.detail(again))

    def test_short_pages_and_a_long_invalid_name_do_not_end_the_walk(self):
        self.objects.page_size = 2  # fewer than asked, while more remain
        long_name = "2025/1z" + "x" * 600 + ".png"  # longer than a key may be
        self.objects.add(f"site/screen/screenshots/{long_name}", b"x")
        walk = self.request({"kind": "screenshots", "limit": 100})
        self.derive(walk)
        self.assertIn("walked 7, walk finished", self.detail(walk))
        self.assertEqual(len(self.rows()), 5)
        # It can end a page and become the cursor without harm.
        self.store.set_backfill_after("test", "screen", "screenshots", long_name)
        self.assertEqual(
            self.store.backfill_after("test", "screen", "screenshots"), long_name
        )
        resumed = self.request({"kind": "screenshots", "limit": 100})
        self.derive(resumed)
        self.assertIn("walked 4, walk finished", self.detail(resumed))

    def test_the_command_runs_a_backfill_here(self):
        out = io.StringIO()
        arguments = [
            "--config",
            str(self.config_path),
            "media-backfill",
            "screen",
            "screenshots",
            "--limit",
            "2",
            "--duckdb",
            str(self.warehouse),
            "--metadata",
            str(self.store.url),
        ]
        with (
            patch("src.cli.main._worker_object_store", return_value=self.objects),
            contextlib.redirect_stdout(out),
        ):
            self.assertEqual(main(arguments), 0)
        printed = json.loads(out.getvalue())
        self.assertEqual(printed["transformation"], "succeeded")
        self.assertIn("backfill walked 2", printed["detail"])
        self.assertEqual(len(self.rows()), 2)

    def test_the_command_refuses_what_has_no_thumbnails(self):
        err = io.StringIO()
        for kind, limit in (("attachments", "5"), ("screenshots", "0")):
            arguments = [
                "--config",
                str(self.config_path),
                "media-backfill",
                "messages" if kind == "attachments" else "screen",
                kind,
                "--limit",
                limit,
                "--duckdb",
                str(self.warehouse),
                "--metadata",
                str(self.store.url),
            ]
            with (
                patch("src.cli.main._worker_object_store", return_value=self.objects),
                contextlib.redirect_stderr(err),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(main(arguments), 1)
        self.assertIn("no thumbnails", err.getvalue())
        self.assertIn("limit must be between", err.getvalue())

    def test_a_local_run_never_uses_the_installation_store(self):
        for metadata in (None, "postgresql+psycopg2://miroir@db.test/miroir"):
            err = io.StringIO()
            arguments = [
                "--config",
                str(self.config_path),
                "media-backfill",
                "screen",
                "screenshots",
                "--duckdb",
                str(self.warehouse),
            ] + (["--metadata", metadata] if metadata else [])
            with (
                patch.dict("os.environ", {"MIROIR_METADATA_URL": str(self.store.url)}),
                contextlib.redirect_stderr(err),
            ):
                self.assertEqual(main(arguments), 2)
            self.assertIn("local SQLite", err.getvalue())
        self.assertEqual(self.pending(), [])

    def test_without_prefect_the_command_says_so(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            code = main(
                [
                    "--config",
                    str(self.config_path),
                    "media-backfill",
                    "screen",
                    "screenshots",
                    "--metadata",
                    str(self.store.url),
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("No Prefect deployment", err.getvalue())


decode = Decoder.thumbnails


class DerivePostgresTests(DeriveFixture):
    """The metadata store and the warehouse in a disposable Postgres."""

    @classmethod
    def setUpClass(cls):
        cls.cluster_root, cls.port = start_postgres(cls, "miroir-derive-")

    def metadata_url(self) -> str:
        return (
            f"postgresql+psycopg2://{getpass.getuser()}@127.0.0.1:{self.port}/postgres"
        )

    def destination(self) -> Any:
        return dlt.destinations.postgres(
            credentials={
                "host": "127.0.0.1",
                "port": self.port,
                "database": "postgres",
                "username": getpass.getuser(),
                "password": "",
            }
        )

    def rows(self) -> dict[str, dict]:
        dataset = self.miroir().connections[0].binding.dataset_name
        with (
            closing(
                psycopg2.connect(
                    host="127.0.0.1",
                    port=self.port,
                    user=getpass.getuser(),
                    dbname="postgres",
                )
            ) as db,
            db.cursor() as cursor,
        ):
            cursor.execute(f"SELECT * FROM {dataset}.media_objects")
            names = [c[0] for c in cursor.description]
            return {row[2]: dict(zip(names, row)) for row in cursor.fetchall()}

    def test_queue_run_and_walk(self):
        body = jpeg()
        self.upload({"a.jpeg": body, "b.heic": b"h"})
        self.assertEqual(len(self.executor.submitted), 1)
        self.store.begin_media_attempt("test", "screen", "screenshots", "a.jpeg")
        # The same file again keeps its attempts; another size starts over.
        now = datetime.now(timezone.utc)
        self.store.add_pending_media(
            "test", "screen", "screenshots", [("a.jpeg", len(body), now)]
        )
        self.assertEqual(self.attempts(), {"a.jpeg": 1, "b.heic": 0})
        self.store.add_pending_media(
            "test", "screen", "screenshots", [("b.heic", 2, now)]
        )
        self.assertEqual(self.attempts(), {"a.jpeg": 1, "b.heic": 0})
        self.derive(self.executor.submitted[0])
        rows = self.rows()
        self.assertEqual(rows["a.jpeg"]["thumbnail_status"], "made")
        self.assertEqual(rows["b.heic"]["thumbnail_status"], "unsupported")
        self.assertEqual((rows["b.heic"]["size"], self.pending()), (2, []))
        self.objects.gets.clear()
        walk = self.request({"kind": "screenshots", "limit": 10})
        self.derive(walk)
        self.assertEqual(self.objects.gets, [])
        self.assertIn("known 1", self.detail(walk))
        self.assertIn("walked 2, walk finished", self.detail(walk))
        self.assertEqual(self.pending(), [])


if __name__ == "__main__":
    unittest.main()
