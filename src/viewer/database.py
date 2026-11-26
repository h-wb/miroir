"""The viewer's connection to the warehouse (decision 040).

The viewer connects as a role that may select from the timeline's tables and
nothing else; every session it opens is also read-only and has its own
statement time limit, so neither safeguard depends on how the role was created.
Its settings are environment variables (`MIROIR_TIMELINE_DATABASE*`), never the
configuration file.
"""

import logging
import os
from typing import Any
from urllib.parse import quote

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, SQLAlchemyError

from src.kernel.errors import OperationError


class TimelineConfigurationError(ValueError):
    """A database setting the viewer refuses; the message never echoes it."""


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
            raise TimelineConfigurationError(
                "The timeline database URL is not a valid database URL"
            ) from None
        if parsed.drivername not in ("postgresql", "postgresql+psycopg2"):
            raise TimelineConfigurationError("The timeline database must be PostgreSQL")
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
