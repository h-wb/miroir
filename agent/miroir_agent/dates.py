"""Parse local calendar dates and offset-qualified instants."""

import re
from datetime import datetime, timedelta

_DATE = re.compile(
    r"\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d))?",
    re.ASCII,
)


def parse_date(value: str, *, through_day: bool = False) -> tuple[int, bool]:
    """Return Unix seconds and whether the input was a bare date."""
    try:
        if not _DATE.fullmatch(value):
            raise ValueError
        instant = datetime.fromisoformat(value)
        bare = len(value) == 10
        if bare and through_day:
            instant += timedelta(days=1)
        return int(instant.timestamp()), bare
    except (ValueError, OverflowError, OSError):
        raise ValueError("Invalid date or offset-qualified instant") from None
