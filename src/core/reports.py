"""What a collector may say about one of its runs (decision 039).

A report is fixed words and counts only, never source content. The shape is
strict: unknown keys, other types and anything longer than a short word are
refused, so a stored report is safe to show and to export as metrics.
"""

from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
)

MAX_REPORT_BYTES = 16 * 1024
INVALID_REPORT = "The report does not have the expected shape"

Outcome = Literal["delivered", "nothing_new", "collected", "failed", "already_running"]
# Not a closed list: a newer collector's new step or reason is still accepted.
Word = Annotated[str, StringConstraints(pattern=r"^[a-z_]{1,24}$")]
Label = Annotated[str, StringConstraints(pattern=r"^[ -~]{1,40}$")]
# No dot: a nested count is exported as `outer.inner`, which must stay unique.
CountName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_-]{1,40}$")]
# Exact as a float, so every count is exported as it was sent.
Count = Annotated[int, Field(ge=0, le=2**53)]
EARLIEST = datetime(2000, 1, 1, tzinfo=timezone.utc)
LATEST = datetime(2100, 1, 1, tzinfo=timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("A time must carry its offset")
    try:
        value = value.astimezone(timezone.utc)
    except OverflowError:
        raise ValueError("A time out of range") from None
    if not EARLIEST <= value < LATEST:
        raise ValueError("A time out of range")
    return value


Instant = Annotated[datetime, AfterValidator(_utc)]
Counts = Annotated[
    dict[
        CountName,
        Count | Annotated[dict[CountName, Count], Field(max_length=40)],
    ],
    Field(max_length=40),
]


class _Strict(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class PassReport(_Strict):
    outcome: Outcome
    counts: Counts
    step: Word | None
    reason: Word | None
    http_status: Annotated[int, Field(ge=100, le=599)] | None


class CollectorReport(_Strict):
    # Strict int, so `true` is not taken for 1 as a Literal would.
    version: Annotated[int, Field(ge=1, le=1)]
    collector: Label
    source: Label
    started_at: Instant
    finished_at: Instant
    rows: PassReport
    media: PassReport | None
    pending: bool
    unreported: Annotated[int, Field(ge=0, le=100_000)]


class InvalidReport(ValueError):
    """A report body that is not a report; the message never echoes it."""


def parse_report(body: bytes) -> dict[str, Any]:
    """Validate a report body and return it as plain JSON values."""
    try:
        report = CollectorReport.model_validate_json(body)
    except (ValidationError, ValueError, OverflowError, RecursionError):
        raise InvalidReport(INVALID_REPORT) from None
    return report.model_dump(mode="json")
