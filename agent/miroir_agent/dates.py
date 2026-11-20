"""Parse local calendar dates and offset-qualified instants."""

from datetime import date, datetime, time, timedelta


def parse_date(value: str, *, through_day: bool = False) -> tuple[int, bool]:
    """Return Unix seconds and whether the input was a bare date."""
    try:
        day = date.fromisoformat(value[:10])
        if day.isoformat() != value[:10]:
            raise ValueError
        if len(value) == 10:
            if through_day:
                day += timedelta(days=1)
            return int(datetime.combine(day, time.min).timestamp()), True
        if value[10:11] != "T" or value[13:14] != ":" or value[16:17] != ":":
            raise ValueError
        clock = value[11:13] + value[14:16] + value[17:19]
        offset = value[19:]
        offset_digits = offset[1:3] + offset[4:]
        if offset != "Z" and (
            len(offset) != 6
            or offset[0] not in "+-"
            or offset[3] != ":"
            or not offset_digits.isascii()
            or not offset_digits.isdigit()
            or int(offset[1:3]) >= 24
            or int(offset[4:]) >= 60
        ):
            raise ValueError
        if len(clock) != 6 or not clock.isascii() or not clock.isdigit():
            raise ValueError
        normalized = value[:19] + ("+00:00" if offset == "Z" else offset)
        return int(datetime.fromisoformat(normalized).timestamp()), False
    except (ValueError, OverflowError, OSError):
        raise ValueError("Invalid date or offset-qualified instant") from None
