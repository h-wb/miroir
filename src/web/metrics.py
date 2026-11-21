"""Connection and collector status in the Prometheus text format (decision 039).

Written by hand: a few gauges per connection do not need a client library.
"""

import logging
import math
from collections.abc import Iterable, Iterator
from datetime import datetime
from typing import Any

log = logging.getLogger(__name__)

# name, help; every metric here is a gauge.
_METRICS = (
    (
        "miroir_connection_last_received_timestamp_seconds",
        "When the connection last received a file, as a Unix time.",
    ),
    (
        "miroir_collector_last_report_timestamp_seconds",
        "When the connection's collector last reported a run, as a Unix time.",
    ),
    (
        "miroir_collector_last_success_timestamp_seconds",
        "When the collector last reported a run that did not fail, as a Unix time.",
    ),
    (
        "miroir_collector_consecutive_failures",
        "Failed runs reported in a row, up to the latest report.",
    ),
    (
        "miroir_collector_pending",
        "1 when a batch was waiting in the collector's spool after its last run.",
    ),
    (
        "miroir_collector_unreported_runs",
        "Earlier runs the collector could not report, as of its last report.",
    ),
    (
        "miroir_collector_last_count",
        "Each count of the last report, by part (rows or media) and name.",
    ),
)

Sample = tuple[str, dict[str, str], float]


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _timestamp(value: str | None) -> float | None:
    return None if value is None else datetime.fromisoformat(value).timestamp()


def _counts(connection: str, report: dict[str, Any]) -> Iterator[Sample]:
    for part in ("rows", "media"):
        result = report.get(part)
        if result is None:
            continue
        for name, value in result["counts"].items():
            values = value.items() if isinstance(value, dict) else [("", value)]
            for inner, number in values:
                label = f"{name}.{inner}" if inner else name
                yield (
                    "miroir_collector_last_count",
                    {"connection": connection, "part": part, "name": label},
                    number,
                )


def _samples(connection: str, delivery: dict[str, Any]) -> Iterator[Sample]:
    labels = {"connection": connection}
    received = _timestamp(delivery.get("last_received_at"))
    if received is not None:
        yield "miroir_connection_last_received_timestamp_seconds", labels, received
    status = delivery.get("collector")
    if status is None:
        return
    report = status["last_report"]
    yield (
        "miroir_collector_last_report_timestamp_seconds",
        labels,
        _timestamp(report["received_at"]) or 0.0,
    )
    success = _timestamp(status["last_success_at"])
    if success is not None:
        yield "miroir_collector_last_success_timestamp_seconds", labels, success
    yield (
        "miroir_collector_consecutive_failures",
        labels,
        status["consecutive_failures"],
    )
    yield "miroir_collector_pending", labels, 1 if report["pending"] else 0
    yield "miroir_collector_unreported_runs", labels, report["unreported"]
    yield from _counts(connection, report)


def _number(value: float) -> str:
    # Only values a float holds exactly; anything else marks a bad record.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("Not a number")
    if isinstance(value, int):
        if abs(value) > 2**53:
            raise ValueError("Out of range")
        return str(value)
    if not (math.isfinite(value) and abs(value) <= 2**53):
        raise ValueError("Out of range")
    return str(int(value)) if value.is_integer() else repr(value)


def _lines(connection: str, delivery: dict[str, Any]) -> Iterator[tuple[str, str]]:
    for name, labels, value in _samples(connection, delivery):
        text = ",".join(f'{key}="{_escape(item)}"' for key, item in labels.items())
        yield name, f"{name}{{{text}}} {_number(value)}"


def _connection_lines(
    connection: str, delivery: dict[str, Any]
) -> list[tuple[str, str]]:
    """Every line of one connection, or none of its collector's if any is
    unreadable: one odd stored report must not take the page down."""
    try:
        return list(_lines(connection, delivery))
    except Exception:  # noqa: BLE001 - a bad record never breaks a scrape
        log.warning("Collector status of %s could not be exported", connection)
        return list(_lines(connection, {**delivery, "collector": None}))


def render_metrics(deliveries: Iterable[tuple[str, dict[str, Any]]]) -> str:
    """One sample per connection and metric; connections without reports
    appear only in the received-time metric."""
    by_name: dict[str, list[str]] = {name: [] for name, _ in _METRICS}
    for connection, delivery in deliveries:
        for name, line in _connection_lines(connection, delivery):
            by_name[name].append(line)
    lines = []
    for name, help_text in _METRICS:
        lines += [f"# HELP {name} {help_text}", f"# TYPE {name} gauge", *by_name[name]]
    return "\n".join(lines) + "\n"
