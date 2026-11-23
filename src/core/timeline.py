"""Read access to the warehouse for the timeline (decision 040).

The server reads the warehouse only here, and only to read. It connects as a role
that may select from the timeline's tables and nothing else; every session it
opens is also read-only and has its own statement time limit, so neither
safeguard depends on how the role was created.
"""

import base64
import json
import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, cast
from urllib.parse import quote
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, DBAPIError, SQLAlchemyError

from src.core.analysis import analysis_plan
from src.core.config import ConfigurationError, ResolvedAnalysis, ResolvedMiroir
from src.core.operations import OperationError

log = logging.getLogger(__name__)

URL_VARIABLE = "MIROIR_TIMELINE_DATABASE_URL"
PARTS_PREFIX = "MIROIR_TIMELINE_DATABASE__"
# Expected of the role too; a session sets it so a role without it is still bounded.
STATEMENT_TIMEOUT_MS = 5000
CONNECT_TIMEOUT_SECONDS = 5
NOT_CONFIGURED = (
    "The timeline is not configured: set MIROIR_TIMELINE_DATABASE_URL or "
    "MIROIR_TIMELINE_DATABASE__{HOST,USERNAME,PASSWORD} to a read-only role "
    "on the warehouse."
)
UNAVAILABLE = "The timeline database is unavailable."


def timeline_database_url() -> str | None:
    """The read-only warehouse URL from the environment, or None if unset.

    `MIROIR_TIMELINE_DATABASE_URL` wins when set. Otherwise the URL is composed
    from `MIROIR_TIMELINE_DATABASE__{HOST,USERNAME,PASSWORD}` (required) and
    `__{PORT,DATABASE}` (default 5432 and the username), as the metadata URL is.
    """
    if url := os.environ.get(URL_VARIABLE):
        return url
    host = os.environ.get(f"{PARTS_PREFIX}HOST")
    username = os.environ.get(f"{PARTS_PREFIX}USERNAME")
    password = os.environ.get(f"{PARTS_PREFIX}PASSWORD")
    if not (host and username and password):
        return None
    port = os.environ.get(f"{PARTS_PREFIX}PORT") or "5432"
    database = os.environ.get(f"{PARTS_PREFIX}DATABASE") or username
    return (
        f"postgresql+psycopg2://{quote(username, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{database}"
    )


# The installation's reader role allows 6 connections: 4 for the routes' queries
# and 1 for the media route's lookups, so an image never waits behind searches.
LOOKUP_TIMEOUT_MS = 1000
LOOKUP_POOL_TIMEOUT_SECONDS = 2


class TimelineDatabase:
    """Small pools of read-only, time-limited sessions on the warehouse: one for
    the timeline's queries, one connection of its own for the media route's
    lookups (decision 040)."""

    def __init__(
        self, url: str, statement_timeout_ms: int = STATEMENT_TIMEOUT_MS
    ) -> None:
        try:
            parsed = make_url(url)
        except (ArgumentError, ValueError):
            raise ConfigurationError(
                "The timeline database URL is not a valid database URL"
            ) from None
        if parsed.drivername not in ("postgresql", "postgresql+psycopg2"):
            raise ConfigurationError("The timeline database must be PostgreSQL")
        url_parsed = parsed.set(drivername="postgresql+psycopg2")
        self.engine = _engine(
            url_parsed, 2, 2, CONNECT_TIMEOUT_SECONDS, statement_timeout_ms
        )
        self.lookup_engine = _engine(
            url_parsed,
            1,
            0,
            LOOKUP_POOL_TIMEOUT_SECONDS,
            min(LOOKUP_TIMEOUT_MS, statement_timeout_ms),
        )

    def health(self) -> dict[str, Any]:
        """One trivial query: the session's own read-only flag and time limit."""
        try:
            with self.engine.connect() as db:
                row = db.execute(
                    sa.text(
                        "SELECT current_setting('transaction_read_only'),"
                        " current_setting('statement_timeout')"
                    )
                ).one()
        except SQLAlchemyError as error:
            # The driver's message can name the host and role; only its type is logged.
            log.warning("Timeline database check failed: %s", type(error).__name__)
            raise OperationError(UNAVAILABLE) from None
        return {"read_only": row[0] == "on", "statement_timeout": row[1]}

    def close(self) -> None:
        self.engine.dispose()
        self.lookup_engine.dispose()


def _engine(
    url: Any, size: int, overflow: int, wait: float, timeout_ms: int
) -> sa.Engine:
    return sa.create_engine(
        url,
        hide_parameters=True,
        pool_size=size,
        max_overflow=overflow,
        pool_timeout=wait,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={
            "connect_timeout": CONNECT_TIMEOUT_SECONDS,
            "application_name": "miroir-timeline",
            # Replaces any `options` the URL carries, so it cannot lift these.
            "options": (
                f"-c statement_timeout={int(timeout_ms)}"
                " -c default_transaction_read_only=on"
            ),
        },
    )


def status(
    database: TimelineDatabase | None, miroir: ResolvedMiroir | None = None
) -> dict[str, Any]:
    """What the timeline routes report: not configured, unreachable, or ready.

    With `miroir`, a configuration without a timeline analysis is not configured
    either, and the analysis shown is named.
    """
    if database is None:
        return {"configured": False, "ready": False, "message": NOT_CONFIGURED}
    source = timeline_source(miroir) if miroir is not None else None
    if miroir is not None and source is None:
        return {"configured": False, "ready": False, "message": NO_ANALYSIS}
    described = source.describe() if source else {}
    try:
        checked = database.health()
    except OperationError as error:
        return {
            "configured": True,
            "ready": False,
            "message": str(error),
            **described,
        }
    ready = checked["read_only"]
    return {
        **described,
        "configured": True,
        "ready": ready,
        "message": "The timeline database is reachable and read-only."
        if ready
        else "The timeline database session is not read-only.",
        **checked,
    }


# ---- Which tables -------------------------------------------------------------

PACKAGE = "timeline"
SCREEN_ROLE = "screen"
SCREENSHOTS = "screenshots"
NO_ANALYSIS = (
    "The timeline is not configured: no analysis of the timeline package is set up."
)
NOT_BUILT = "The timeline has not been built yet, or its reader role cannot read it."
TIMED_OUT = (
    f"The timeline query took longer than {STATEMENT_TIMEOUT_MS // 1000} seconds; "
    "narrow it (a shorter range, more words) and try again."
)
_SCHEMA = re.compile(r"[a-z][a-z0-9_]{0,62}")


class TimelineUnavailable(OperationError):
    """The timeline cannot answer now: not configured, not built, unreachable."""


class TimelineTimeout(OperationError):
    """A query reached the session's time limit."""


@dataclass(frozen=True)
class TimelineSource:
    """The analysis whose tables the server reads, and how to read them."""

    analysis_id: str
    schema: str
    timezone: str
    # Further timeline analyses, not shown: only the first one is.
    ignored: tuple[str, ...] = ()

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def table(self, name: str) -> str:
        return f'"{self.schema}".{name}'

    def describe(self) -> dict[str, Any]:
        described: dict[str, Any] = {
            "analysis": self.analysis_id,
            "timezone": self.timezone,
        }
        if self.ignored:
            described["warning"] = (
                f"{len(self.ignored) + 1} timeline analyses are configured; "
                f"only the first, {self.analysis_id}, is shown."
            )
        return described


def timeline_source(miroir: ResolvedMiroir) -> TimelineSource | None:
    """The first configured timeline analysis, in configuration order, or None.

    Its schema is derived as the build derives it (analysis_plan), so no setting
    names it.
    """
    found = [a for a in miroir.analyses if a.package.id == PACKAGE]
    if not found:
        return None
    return _source(found[0], tuple(a.id for a in found[1:]))


def _source(analysis: ResolvedAnalysis, ignored: tuple[str, ...]) -> TimelineSource:
    schema = analysis_plan(analysis).relations[0].split(".")[0]
    if not _SCHEMA.fullmatch(schema):  # derived from validated IDs; kept honest
        raise TimelineUnavailable("The timeline analysis has an unexpected schema")
    return TimelineSource(analysis.id, schema, analysis.config.timezone, ignored)


def media_withheld(
    database: "TimelineDatabase | None",
    miroir: ResolvedMiroir,
    connection_id: str,
    kind: str,
    key: str,
) -> bool | None:
    """Whether a file belongs to a withheld frame, for the media route.

    None when no timeline analysis reads this connection's screenshots: there is
    nothing to ask. Otherwise the analyses that can answer are asked: those with
    a reader role whose tables are built. One answer is enough; withheld in any
    that answers is withheld; a key none knows (no frame yet) is not. Fails
    closed: when none can answer, it raises as the timeline routes do, so the
    file is never served unchecked.
    """
    if kind != SCREENSHOTS:
        return None
    analyses = [
        a
        for a in miroir.analyses
        if a.package.id == PACKAGE
        and SCREEN_ROLE in a.inputs
        and a.inputs[SCREEN_ROLE].config.id == connection_id
    ]
    if not analyses:
        return None
    if database is None:
        raise TimelineUnavailable(NOT_CONFIGURED)
    readable = [
        _source(a, ())
        for a in analyses
        if analysis_plan(a).variables.get("timeline_reader_role")
    ]
    answered = False
    refusal: TimelineUnavailable | TimelineTimeout = TimelineUnavailable(NOT_BUILT)
    for source in readable:
        try:
            if TimelineReader(database, source).withheld(key):
                return True
        except TimelineTimeout as error:
            refusal = error
        except TimelineUnavailable as error:
            if not isinstance(refusal, TimelineTimeout):
                refusal = error
        else:
            answered = True
    if not answered:
        raise refusal
    return False


# ---- Reading --------------------------------------------------------------------

# Bounds on what one answer holds.
MAX_DAYS_RANGE = 400
MAX_DAY_FRAMES = 20_000
MAX_TEXT = 100_000
MAX_EVENTS = 500
MAX_EVENT_TEXT = 2_000
MAX_SEARCH = 50
MAX_QUERY = 200
MOMENT_SECONDS = 300
# Private-use characters mark a snippet's matches; they are removed from the
# text first, so none in it can be taken for a mark.
_START, _STOP = "", ""
_HEADLINE = (
    f"StartSel={_START}, StopSel={_STOP}, MaxWords=30, MinWords=12, "
    'MaxFragments=2, FragmentDelimiter=" … "'
)
# A frame's screenshot, as the screen index codes it.
MEDIA_UNKNOWN, MEDIA_THUMBNAILS, MEDIA_NO_THUMBNAILS, MEDIA_WITHHELD = 0, 1, 2, 3
_MEDIA_STATES = {
    MEDIA_UNKNOWN: "unknown",
    MEDIA_THUMBNAILS: "thumbnails",
    MEDIA_NO_THUMBNAILS: "stored",
    MEDIA_WITHHELD: "withheld",
}
SOURCES = ("screen", "browser")


def media_code(
    withheld: str | None, status: str | None, w360: bool | None, w1280: bool | None
) -> int:
    """What is known of a frame's screenshot. No record is unknown, not absent: the
    file may still be queued or not derived yet (decision 040, phase 1b)."""
    if withheld:
        return MEDIA_WITHHELD  # never uploaded, whatever a record says
    if status is None:
        return MEDIA_UNKNOWN
    return MEDIA_THUMBNAILS if w360 and w1280 else MEDIA_NO_THUMBNAILS


def snippet(marked: str | None) -> list[str]:
    """A headline as alternating plain and matched parts, plain first, so a page
    can show it as text without parsing markup."""
    parts = [""]
    for char in marked or "":
        # An odd count of parts means the last one is plain.
        if (char == _START and len(parts) % 2) or (
            char == _STOP and not len(parts) % 2
        ):
            parts.append("")
        elif char not in (_START, _STOP):
            parts[-1] += char
    while len(parts) > 1 and parts[-1] == "":
        parts.pop()
    return parts


def _iso(value: datetime, zone: ZoneInfo) -> str:
    return value.astimezone(zone).isoformat()


def _app(row: Any) -> dict[str, str]:
    return {"bundle_id": row[0], "name": row[1], "colour": row[2]}


class _Apps:
    """A per-answer list of apps, referred to by index."""

    def __init__(self) -> None:
        self.index: dict[str, int] = {}

    def __call__(self, bundle_id: str | None) -> int | None:
        if bundle_id is None:
            return None
        return self.index.setdefault(bundle_id, len(self.index))

    def described(self, reader: "TimelineReader", db: Any) -> list[dict[str, str]]:
        if not self.index:
            return []
        rows = db.execute(
            sa.text(
                f"SELECT bundle_id, name, colour FROM {reader.source.table('apps')}"
                " WHERE bundle_id = ANY(:ids)"
            ),
            {"ids": list(self.index)},
        ).all()
        known = {row[0]: _app(row) for row in rows}
        # An app the last build has not named yet keeps its identifier.
        return [
            known.get(
                bundle, {"bundle_id": bundle, "name": bundle, "colour": "#9e9e9e"}
            )
            for bundle in self.index
        ]


def _cursor(values: list[Any]) -> str:
    raw = json.dumps(values, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def read_cursor(value: str, shape: tuple[type, ...]) -> list[Any]:
    """An opaque paging cursor back into its values; OperationError if it is not one
    this server wrote."""
    try:
        if len(value) > 1024:
            raise ValueError
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        values = json.loads(raw)
    except (ValueError, UnicodeDecodeError, RecursionError):
        raise OperationError("Invalid paging cursor") from None
    if (
        not isinstance(values, list)
        or len(values) != len(shape)
        or not all(
            isinstance(v, kind) and not isinstance(v, bool)
            for v, kind in zip(values, shape, strict=True)
        )
    ):
        raise OperationError("Invalid paging cursor")
    if any(isinstance(v, str) and "\x00" in v for v in values):
        raise OperationError("Invalid paging cursor")
    return values


def _day_bounds_sql(day: str) -> tuple[str, str]:
    """A local day's start and end in the analysis's timezone. Where clocks go back
    at midnight, Postgres reads the repeated midnight as the later one, so the
    hour before it belongs to the previous day. Selecting by these bounds keeps
    the time index usable; `_local_day_sql` and the models (timeline_local_day)
    assign a time to a day by the same rule."""
    return (
        f"(CAST(:{day} AS date)::timestamp AT TIME ZONE :tz)",
        f"((CAST(:{day} AS date) + 1)::timestamp AT TIME ZONE :tz)",
    )


def _local_day_sql(column: str) -> str:
    """The local day whose bounds (`_day_bounds_sql`) hold a time."""
    date_at = f"CAST({column} AT TIME ZONE :tz AS date)"
    return (
        f"(CASE WHEN {column} < ({date_at}::timestamp AT TIME ZONE :tz)"
        f" THEN {date_at} - 1 ELSE {date_at} END)"
    )


class TimelineReader:
    """The timeline's queries, each through one read-only, time-limited session."""

    def __init__(self, database: TimelineDatabase, source: TimelineSource) -> None:
        self.database = database
        self.source = source

    @contextmanager
    def _session(self, lookup: bool = False) -> Iterator[Any]:
        engine = self.database.lookup_engine if lookup else self.database.engine
        try:
            with engine.connect() as db:
                yield db
        except DBAPIError as error:
            code = getattr(error.orig, "pgcode", None)
            if code == "57014":  # query_canceled: the statement time limit
                raise TimelineTimeout(TIMED_OUT) from None
            log.warning("Timeline query failed: %s %s", type(error).__name__, code)
            if code in ("42P01", "3F000", "42501", "42703"):
                # No table, schema or column yet, or no privilege on them.
                raise TimelineUnavailable(NOT_BUILT) from None
            raise TimelineUnavailable(UNAVAILABLE) from None
        except SQLAlchemyError as error:
            log.warning("Timeline query failed: %s", type(error).__name__)
            raise TimelineUnavailable(UNAVAILABLE) from None

    def days(self, start: date, end: date) -> dict[str, Any]:
        """Counts per local day and source, from `start` to `end` inclusive."""
        if end < start or (end - start).days >= MAX_DAYS_RANGE:
            raise OperationError(
                f"The range must run forward and span at most {MAX_DAYS_RANGE} days"
            )
        with self._session() as db:
            rows = db.execute(
                sa.text(
                    f"SELECT day, source, events, frames FROM {self.source.table('days')}"
                    " WHERE day >= :start AND day <= :end AND timezone = :tz"
                    " ORDER BY day, source"
                ),
                {"start": start, "end": end, "tz": self.source.timezone},
            ).all()
        days: dict[str, dict[str, Any]] = {}
        for day, source, events, frames in rows:
            counts: dict[str, int] = {"events": int(events)}
            if frames is not None:
                counts["frames"] = int(frames)
            days.setdefault(day.isoformat(), {})[source] = counts
        return {
            **self.source.describe(),
            "from": start.isoformat(),
            "to": end.isoformat(),
            "days": [{"day": day, **sources} for day, sources in days.items()],
        }

    def screen_day(self, day: date) -> dict[str, Any]:
        """One local day's frames in time order, as columns for a small answer."""
        lo, hi = _day_bounds_sql("day")
        frames = self.source.table("screen_frames")
        apps = _Apps()
        with self._session() as db:
            bounds = db.execute(
                sa.text(f"SELECT {lo}, {hi}"),
                {"day": day, "tz": self.source.timezone},
            ).one()
            rows = db.execute(
                sa.text(
                    "SELECT CAST(EXTRACT(EPOCH FROM captured_at - :lo) AS integer),"
                    " bundle_id, image_path, withheld, media_status, thumbnail_w360,"
                    f" thumbnail_w1280, connection FROM {frames}"
                    " WHERE captured_at >= :lo AND captured_at < :hi"
                    " ORDER BY captured_at, image_path LIMIT :limit"
                ),
                {"lo": bounds[0], "hi": bounds[1], "limit": MAX_DAY_FRAMES + 1},
            ).all()
            truncated = len(rows) > MAX_DAY_FRAMES
            rows = rows[:MAX_DAY_FRAMES]
            columns: dict[str, list[Any]] = {"t": [], "app": [], "key": [], "media": []}
            for t, bundle, key, withheld, status, w360, w1280, _ in rows:
                columns["t"].append(t)
                columns["app"].append(apps(bundle))
                columns["key"].append(key)
                columns["media"].append(media_code(withheld, status, w360, w1280))
            described = apps.described(self, db)
        zone = self.source.zone
        return {
            **self.source.describe(),
            "day": day.isoformat(),
            "start": _iso(bounds[0], zone),
            "end": _iso(bounds[1], zone),
            "connection": rows[0][7] if rows else None,
            "media_kind": SCREENSHOTS,
            "count": len(rows),
            "truncated": truncated,
            "apps": described,
            "frames": columns,
        }

    def withheld(self, key: str) -> bool:
        """Whether the frame with this media key is withheld; False if unknown.
        Through the lookup connection, so it never waits behind searches."""
        with self._session(lookup=True) as db:
            found = db.execute(
                sa.text(
                    "SELECT withheld IS NOT NULL"
                    f" FROM {self.source.table('screen_frames')}"
                    " WHERE image_path = :key"
                ),
                {"key": key},
            ).scalar_one_or_none()
        return bool(found)

    def frame(self, key: str) -> dict[str, Any] | None:
        """One frame: time, app, text, what was masked or withheld, its media."""
        tz = self.source.timezone
        with self._session() as db:
            row = db.execute(
                sa.text(
                    "SELECT f.image_path, f.captured_at, f.bundle_id, a.name, a.colour,"
                    " left(f.text, :max_text), length(f.text) > :max_text, f.withheld,"
                    " f.masked, f.connection, f.media_status, f.thumbnail_w360,"
                    " f.thumbnail_w1280,"
                    f" {_local_day_sql('f.captured_at')}"
                    f" FROM {self.source.table('screen_frames')} f"
                    f" LEFT JOIN {self.source.table('apps')} a"
                    " ON a.bundle_id = f.bundle_id"
                    " WHERE f.image_path = :key"
                ),
                {"key": key, "tz": tz, "max_text": MAX_TEXT},
            ).one_or_none()
            if row is None:
                return None
            lo, _ = _day_bounds_sql("day")
            start = db.execute(sa.text(f"SELECT {lo}"), {"day": row[13], "tz": tz})
            day_start = start.scalar_one()
        (key, captured, bundle, name, colour, text, cut, withheld, masked) = row[:9]
        connection, status, w360, w1280, day = row[9:]
        code = media_code(withheld, status, w360, w1280)
        app = None
        if bundle is not None:
            app = {"bundle_id": bundle, "name": name or bundle, "colour": colour}
        return {
            **self.source.describe(),
            "key": key,
            "captured_at": _iso(captured, self.source.zone),
            "day": day.isoformat(),
            "t": int((captured - day_start).total_seconds()),
            "app": app,
            "text": text,
            "text_truncated": bool(cut),
            "withheld": withheld,
            "masked": masked,
            "media": _media(connection, SCREENSHOTS, key, code, status),
        }

    def _instant(self, db: Any, value: datetime | date, end: bool) -> datetime:
        """A time as given, or a local day's start (its end, for an end)."""
        if isinstance(value, datetime):
            return value
        bound = _day_bounds_sql("day")[1 if end else 0]
        found = db.execute(
            sa.text(f"SELECT {bound}"), {"day": value, "tz": self.source.timezone}
        )
        return cast(datetime, found.scalar_one())

    def events(
        self,
        start: datetime | date,
        end: datetime | date,
        sources: tuple[str, ...],
        limit: int,
        after: tuple[datetime, str] | None,
    ) -> dict[str, Any]:
        """Events that started in [start, end), oldest first, paged by keyset on
        (started_at, event_id). One index range per source, merged. A day as
        `end` includes that day."""
        if not 1 <= limit <= MAX_EVENTS:
            raise OperationError(f"The limit must be between 1 and {MAX_EVENTS}")
        if not sources or set(sources) - set(SOURCES):
            raise OperationError(f"Sources are {', '.join(SOURCES)}")
        events = self.source.table("events")
        parameters: dict[str, Any] = {"limit": limit + 1, "max_text": MAX_EVENT_TEXT}
        keyset = ""
        if after is not None:
            parameters.update(after_at=after[0], after_id=after[1])
            keyset = (
                " AND started_at >= :after_at"
                " AND (started_at, event_id) > (:after_at, :after_id)"
            )
        parts = []
        for number, source in enumerate(sources):
            parameters[f"source{number}"] = source
            parts.append(
                "(SELECT source, event_id, started_at, ended_at, kind,"
                " left(title, :max_text), left(body, :max_text), group_key, payload,"
                " media_kind, media_key, media_status, thumbnail_w360,"
                f" thumbnail_w1280, media_withheld, connection FROM {events}"
                f" WHERE source = :source{number}"
                f" AND started_at >= :lo AND started_at < :hi{keyset}"
                " ORDER BY started_at, event_id LIMIT :limit)"
            )
        sql = (
            "SELECT * FROM (" + " UNION ALL ".join(parts) + ") merged"
            " ORDER BY started_at, event_id LIMIT :limit"
        )
        with self._session() as db:
            parameters["lo"] = self._instant(db, start, end=False)
            parameters["hi"] = self._instant(db, end, end=True)
            if parameters["hi"] <= parameters["lo"]:
                raise OperationError("The end must come after the start")
            rows = db.execute(sa.text(sql), parameters).all()
        more = len(rows) > limit
        rows = rows[:limit]
        zone = self.source.zone
        answer = []
        for row in rows:
            (source, event_id, started, ended, kind, title, body, group, payload) = row[
                :9
            ]
            media_kind, media_key, status, w360, w1280, withheld, connection = row[9:]
            event: dict[str, Any] = {
                "source": source,
                "event_id": event_id,
                "started_at": _iso(started, zone),
                "ended_at": _iso(ended, zone) if ended is not None else None,
                "kind": kind,
                "title": title,
                "body": body,
                "group": group,
                "payload": payload,
                "media": None,
            }
            if media_key is not None:
                event["media"] = _media(
                    connection,
                    media_kind,
                    media_key,
                    media_code("withheld" if withheld else None, status, w360, w1280),
                    status,
                )
            answer.append(event)
        last = rows[-1] if rows else None
        return {
            **self.source.describe(),
            "events": answer,
            "next": _cursor([last[2].isoformat(), last[1]]) if more and last else None,
        }

    def search_screen(
        self,
        query: str,
        app: str | None,
        start: date | None,
        end: date | None,
        limit: int,
        before: tuple[int, str] | None,
    ) -> dict[str, Any]:
        """Screen-text matches grouped into moments (one app, one five-minute
        window), newest first, with a snippet for the moments returned only."""
        words = _query(query, limit)
        frames = self.source.table("screen_frames")
        parameters: dict[str, Any] = {
            "q": words,
            "tz": self.source.timezone,
            "limit": limit + 1,
            "width": MOMENT_SECONDS,
            "headline": _HEADLINE,
            "marks": _START + _STOP,
        }
        conditions = ["to_tsvector('simple', f.text) @@ q.query"]
        conditions += _range_conditions("f.captured_at", start, end, parameters)
        if app is not None:
            conditions.append("f.bundle_id = :app")
            parameters["app"] = app
        if before is not None:
            # The window's end bounds the time index too.
            conditions.append(
                "f.captured_at < to_timestamp((:before_bucket + 1) * :width)"
                " AND (floor(EXTRACT(EPOCH FROM f.captured_at) / :width) < :before_bucket"
                " OR (floor(EXTRACT(EPOCH FROM f.captured_at) / :width) = :before_bucket"
                " AND coalesce(f.bundle_id, '') > :before_app))"
            )
            parameters.update(before_bucket=before[0], before_app=before[1])
        sql = (
            "WITH q AS (SELECT websearch_to_tsquery('simple', :q) AS query),"
            " moments AS ("
            " SELECT CAST(floor(EXTRACT(EPOCH FROM f.captured_at) / :width) AS bigint)"
            " AS bucket, f.bundle_id, count(*) AS matches,"
            " min(f.captured_at) AS first_at, max(f.captured_at) AS last_at,"
            " (array_agg(f.image_path ORDER BY f.captured_at, f.image_path))[1] AS key"
            f" FROM {frames} f, q WHERE {' AND '.join(conditions)}"
            " GROUP BY 1, 2 ORDER BY 1 DESC, coalesce(f.bundle_id, '') LIMIT :limit)"
            " SELECT m.bucket, m.bundle_id, m.matches, m.first_at, m.last_at, m.key,"
            " ts_headline('simple', translate(f.text, :marks, ''), q.query, :headline),"
            " f.withheld, f.media_status, f.thumbnail_w360, f.thumbnail_w1280,"
            f" f.connection, {_local_day_sql('m.first_at')}"
            f" FROM moments m JOIN {frames} f ON f.image_path = m.key, q"
            " ORDER BY m.bucket DESC, coalesce(m.bundle_id, '')"
        )
        apps = _Apps()
        zone = self.source.zone
        with self._session() as db:
            _searchable(db, words)
            rows = db.execute(sa.text(sql), parameters).all()
            more = len(rows) > limit
            rows = rows[:limit]
            moments = []
            for row in rows:
                _, bundle, matches, first_at, last_at, key, marked = row[:7]
                withheld, status, w360, w1280, connection, day = row[7:]
                moments.append(
                    {
                        "key": key,
                        "connection": connection,
                        "at": _iso(first_at, zone),
                        "until": _iso(last_at, zone),
                        "day": day.isoformat(),
                        "app": apps(bundle),
                        "matches": int(matches),
                        "snippet": snippet(marked),
                        "media": media_code(withheld, status, w360, w1280),
                    }
                )
            described = apps.described(self, db)
        last = rows[-1] if rows else None
        return {
            **self.source.describe(),
            "kind": "screen",
            "moment_seconds": MOMENT_SECONDS,
            "apps": described,
            "moments": moments,
            "next": _cursor([int(last[0]), last[1] or ""]) if more and last else None,
        }

    def search_browser(
        self,
        query: str,
        start: date | None,
        end: date | None,
        limit: int,
        before: tuple[datetime, str] | None,
    ) -> dict[str, Any]:
        """Visits whose title or address matches, newest first."""
        words = _query(query, limit)
        parameters: dict[str, Any] = {
            "q": words,
            "tz": self.source.timezone,
            "limit": limit + 1,
            "headline": _HEADLINE,
            "marks": _START + _STOP,
            "max_text": MAX_EVENT_TEXT,
        }
        conditions = ["e.source = 'browser'", "e.search @@ q.query"]
        conditions += _range_conditions("e.started_at", start, end, parameters)
        if before is not None:
            conditions.append(
                "e.started_at <= :before_at AND (e.started_at, e.event_id)"
                " < (:before_at, :before_id)"
            )
            parameters.update(before_at=before[0], before_id=before[1])
        sql = (
            "WITH q AS (SELECT websearch_to_tsquery('simple', :q) AS query),"
            " hits AS (SELECT e.event_id, e.started_at, e.title, e.body, e.group_key"
            f" FROM {self.source.table('events')} e, q"
            f" WHERE {' AND '.join(conditions)}"
            " ORDER BY e.started_at DESC, e.event_id DESC LIMIT :limit)"
            " SELECT h.event_id, h.started_at, left(h.title, :max_text),"
            " left(h.body, :max_text), h.group_key,"
            " ts_headline('simple', translate(coalesce(h.title, ''), :marks, ''),"
            " q.query, :headline)"
            " FROM hits h, q ORDER BY h.started_at DESC, h.event_id DESC"
        )
        with self._session() as db:
            _searchable(db, words)
            rows = db.execute(sa.text(sql), parameters).all()
        more = len(rows) > limit
        rows = rows[:limit]
        zone = self.source.zone
        visits = [
            {
                "event_id": event_id,
                "at": _iso(started, zone),
                "title": title,
                "url": body,
                "site": site,
                "snippet": snippet(marked),
            }
            for event_id, started, title, body, site, marked in rows
        ]
        last = rows[-1] if rows else None
        return {
            **self.source.describe(),
            "kind": "browser",
            "visits": visits,
            "next": _cursor([last[1].isoformat(), last[0]]) if more and last else None,
        }


def _media(
    connection: str | None, kind: str, key: str, code: int, status: str | None
) -> dict[str, Any]:
    return {
        "connection": connection,
        "kind": kind,
        "key": key,
        "state": _MEDIA_STATES[code],
        "status": status,
    }


NO_WORDS = "The search has no words to look for."
ONLY_EXCLUDED = (
    "The search must look for at least one word: words after a minus only exclude,"
    " and every part joined by 'or' needs a word of its own."
)


def _searchable(db: Any, words: str) -> None:
    """Refuse a search the text index cannot answer. `querytree` keeps the part of
    a query an index can use; it is 'T' when every part is negated (`-a`, `a or
    -b`), which would read every row, and empty when there is no word at all."""
    tree = db.execute(
        sa.text("SELECT querytree(websearch_to_tsquery('simple', :q))"), {"q": words}
    ).scalar_one()
    if tree == "":
        raise OperationError(NO_WORDS)
    if tree == "T":
        raise OperationError(ONLY_EXCLUDED)


def _query(query: str, limit: int) -> str:
    words = query.strip()
    if not words or len(words) > MAX_QUERY:
        raise OperationError(f"The search must have 1 to {MAX_QUERY} characters")
    if not 1 <= limit <= MAX_SEARCH:
        raise OperationError(f"The limit must be between 1 and {MAX_SEARCH}")
    return words


def _range_conditions(
    column: str, start: date | None, end: date | None, parameters: dict[str, Any]
) -> list[str]:
    """Local days from `start` to `end`, inclusive, as bounds on a time column."""
    if start is not None and end is not None and end < start:
        raise OperationError("The range must run forward")
    conditions = []
    if start is not None:
        parameters["start"] = start
        conditions.append(f"{column} >= {_day_bounds_sql('start')[0]}")
    if end is not None:
        parameters["end"] = end
        conditions.append(f"{column} < {_day_bounds_sql('end')[1]}")
    return conditions
