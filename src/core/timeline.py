"""Read access to the warehouse for the timeline (decision 040).

The server reads the warehouse only here, and only to read. It connects as a role
that may select from the timeline's tables and nothing else; every session it
opens is also read-only and has its own statement time limit, so neither
safeguard depends on how the role was created.
"""

import logging
import os
from typing import Any
from urllib.parse import quote

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, SQLAlchemyError

from src.core.config import ConfigurationError
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


class TimelineDatabase:
    """A small pool of read-only, time-limited sessions on the warehouse."""

    def __init__(self, url: str) -> None:
        try:
            parsed = make_url(url)
        except (ArgumentError, ValueError):
            raise ConfigurationError(
                "The timeline database URL is not a valid database URL"
            ) from None
        if parsed.drivername not in ("postgresql", "postgresql+psycopg2"):
            raise ConfigurationError("The timeline database must be PostgreSQL")
        self.engine = sa.create_engine(
            parsed.set(drivername="postgresql+psycopg2"),
            hide_parameters=True,
            pool_size=2,
            max_overflow=2,
            pool_timeout=CONNECT_TIMEOUT_SECONDS,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={
                "connect_timeout": CONNECT_TIMEOUT_SECONDS,
                "application_name": "miroir-timeline",
                # Replaces any `options` the URL carries, so it cannot lift these.
                "options": (
                    f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"
                    " -c default_transaction_read_only=on"
                ),
            },
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


def status(database: TimelineDatabase | None) -> dict[str, Any]:
    """What the timeline routes report: not configured, unreachable, or ready."""
    if database is None:
        return {"configured": False, "ready": False, "message": NOT_CONFIGURED}
    try:
        checked = database.health()
    except OperationError as error:
        return {"configured": True, "ready": False, "message": str(error)}
    ready = checked["read_only"]
    return {
        "configured": True,
        "ready": ready,
        "message": "The timeline database is reachable and read-only."
        if ready
        else "The timeline database session is not read-only.",
        **checked,
    }
