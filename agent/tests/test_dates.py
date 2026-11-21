import json
import os
import time
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from miroir_agent.config import load_config
from miroir_agent.dates import parse_date
from miroir_agent.errors import ConfigError


@contextmanager
def local_zone(zone: str) -> Iterator[None]:
    try:
        with patch.dict(os.environ, TZ=zone):
            time.tzset()
            yield
    finally:
        time.tzset()


class DateTests(unittest.TestCase):
    def test_local_dates(self) -> None:
        for zone, expected in (("UTC0", 1700006400), ("EST5", 1700024400)):
            with self.subTest(zone=zone), local_zone(zone):
                self.assertEqual(parse_date("2023-11-15"), (expected, True))

    def test_end_of_day_uses_next_local_midnight(self) -> None:
        with local_zone("EST5EDT,M3.2.0,M11.1.0"):
            start, _ = parse_date("2024-03-10")
            end, bare = parse_date("2024-03-10", through_day=True)
            self.assertTrue(bare)
            self.assertEqual(end - start, 23 * 3600)

    def test_instants(self) -> None:
        with local_zone("EST5"):
            for value in (
                "2023-11-14T22:13:20Z",
                "2023-11-15T00:13:20+02:00",
                "2023-11-14T17:13:20-05:00",
            ):
                self.assertEqual(parse_date(value), (1700000000, False))

    def test_invalid_dates(self) -> None:
        for value in (
            "2023-11-14T22:13:20",
            "bad input",
            "2023-02-29",
            "20231114",
            "2023-11-14T22:13:20+01:60",
            "2023-11-14 22:13:20Z",
        ):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(
                    ValueError, "^Invalid date or offset-qualified instant$"
                ),
            ):
                parse_date(value)

    def options(self, name: str, options: dict):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "agent.json"
            path.write_text(
                json.dumps(
                    {
                        "state_dir": "state",
                        "collectors": [
                            {
                                "id": "example",
                                "collector": name,
                                "receive_url": "https://example.test/receive",
                                "token_env": "TOKEN",
                                "options": options,
                            }
                        ],
                    }
                )
            )
            return load_config(path).collectors[0].options

    def test_config_since(self) -> None:
        with local_zone("UTC0"):
            for name, options in (
                ("zen_history", {}),
                ("screenmemory", {"data_dir": "source"}),
            ):
                self.assertNotIn("since", self.options(name, options).model_fields_set)
                for value, expected in (
                    ("2023-11-15", 1700006400),
                    ("2023-11-14T22:13:20Z", 1700000000),
                ):
                    self.assertEqual(
                        self.options(name, options | {"since": value}).since, expected
                    )
                for value in ("bad input", 1700000000):
                    with self.assertRaises(ConfigError):
                        self.options(name, options | {"since": value})
                with self.assertRaisesRegex(
                    ConfigError, r"options\.cutoff_ts: unknown key"
                ):
                    self.options(name, options | {"cutoff" + "_ts": 0})
