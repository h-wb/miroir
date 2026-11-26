"""The derive queue: files confirmed in the bucket that the worker has not derived
yet, and where a backfill's walk of the bucket stopped (decision 040).

Media owns these tables and every read and write of them. The control plane's
metadata store holds them in its database and its schema version: its
`metadata` takes a copy of each table here, and its one migration sequence
creates them (version 8). Their names, columns and indexes are persistent
identities; `tests/test_media_queue.py` holds their DDL.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa

metadata = sa.MetaData()

# Version 8 (decision 040): files confirmed in the bucket that the worker has not
# derived yet. The derive run drains it; a newer confirmation of the same file
# with another size replaces the entry.
media_pending = sa.Table(
    "media_pending",
    metadata,
    sa.Column("miroir_id", sa.String(24), primary_key=True),
    sa.Column("connection_id", sa.String(24), primary_key=True),
    sa.Column("kind", sa.String(40), primary_key=True),
    sa.Column("key", sa.String(512), primary_key=True),
    sa.Column("size", sa.BigInteger, nullable=False),
    sa.Column("stored_at", sa.DateTime(timezone=True), nullable=False),
    # Decodes begun: a file whose decoding ended the process is not tried forever.
    sa.Column("attempts", sa.Integer, nullable=False),
    # Derive it again even if its row says it was derived (a backfill's --retry).
    sa.Column("retry", sa.Boolean, nullable=False),
    sa.Index(
        "ix_media_pending_stored",
        "miroir_id",
        "connection_id",
        "kind",
        "stored_at",
    ),
)

# Version 8: where a backfill's walk of the bucket stopped, per connection and kind.
media_backfills = sa.Table(
    "media_backfills",
    metadata,
    sa.Column("miroir_id", sa.String(24), primary_key=True),
    sa.Column("connection_id", sa.String(24), primary_key=True),
    sa.Column("kind", sa.String(40), primary_key=True),
    # The last name listed, valid key or not: an object copied in by other means
    # can be named longer than a key may be.
    sa.Column("after", sa.Text, nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
)


@dataclass(frozen=True)
class PendingMedia:
    key: str
    size: int
    stored_at: datetime
    attempts: int
    retry: bool = False


def _utc(value: datetime) -> datetime:
    # SQLite returns naive datetimes; everything written here is UTC.
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class MediaQueue:
    """The queue's reads and writes, on the metadata store's engine."""

    def __init__(self, engine: sa.Engine):
        self.engine = engine

    def add_pending_media(
        self,
        miroir_id: str,
        connection_id: str,
        kind: str,
        objects: list[tuple[str, int, datetime]],
    ) -> None:
        """Queue (key, size, stored at) for the derive run. A file already queued
        with the same size keeps its entry and its count of attempts."""
        if not objects:
            return
        from sqlalchemy.dialects import postgresql, sqlite

        insert = (
            postgresql.insert
            if self.engine.dialect.name == "postgresql"
            else sqlite.insert
        )
        rows = {
            key: {
                "miroir_id": miroir_id,
                "connection_id": connection_id,
                "kind": kind,
                "key": key,
                "size": size,
                "stored_at": stored_at,
                "attempts": 0,
                "retry": False,
            }
            for key, size, stored_at in objects
        }
        statement = insert(media_pending).values(list(rows.values()))
        statement = statement.on_conflict_do_update(
            index_elements=["miroir_id", "connection_id", "kind", "key"],
            set_={
                "size": statement.excluded.size,
                "stored_at": statement.excluded.stored_at,
                "attempts": 0,
            },
            where=media_pending.c.size != statement.excluded.size,
        )
        with self.engine.begin() as db:
            db.execute(statement)

    def queue_retry(
        self,
        miroir_id: str,
        connection_id: str,
        kind: str,
        objects: list[tuple[str, int, datetime]],
    ) -> None:
        """Queue (key, size, stored at) to be derived again whatever their rows
        say. One write, so a run that dies after it leaves them queued."""
        if not objects:
            return
        from sqlalchemy.dialects import postgresql, sqlite

        insert = (
            postgresql.insert
            if self.engine.dialect.name == "postgresql"
            else sqlite.insert
        )
        rows = {
            key: {
                "miroir_id": miroir_id,
                "connection_id": connection_id,
                "kind": kind,
                "key": key,
                "size": size,
                "stored_at": stored_at,
                "attempts": 0,
                "retry": True,
            }
            for key, size, stored_at in objects
        }
        statement = insert(media_pending).values(list(rows.values()))
        statement = statement.on_conflict_do_update(
            index_elements=["miroir_id", "connection_id", "kind", "key"],
            set_={
                "size": statement.excluded.size,
                "stored_at": statement.excluded.stored_at,
                "attempts": 0,
                "retry": True,
            },
        )
        with self.engine.begin() as db:
            db.execute(statement)

    def pending_media(
        self, miroir_id: str, connection_id: str, kind: str, limit: int
    ) -> list[PendingMedia]:
        """The most recently stored first, so new files do not wait for a backfill."""
        with self.engine.connect() as db:
            rows = db.execute(
                sa.select(
                    media_pending.c.key,
                    media_pending.c.size,
                    media_pending.c.stored_at,
                    media_pending.c.attempts,
                    media_pending.c.retry,
                )
                .where(
                    media_pending.c.miroir_id == miroir_id,
                    media_pending.c.connection_id == connection_id,
                    media_pending.c.kind == kind,
                )
                .order_by(media_pending.c.stored_at.desc(), media_pending.c.key)
                .limit(limit)
            ).all()
        return [
            PendingMedia(
                row.key, row.size, _utc(row.stored_at), row.attempts, bool(row.retry)
            )
            for row in rows
        ]

    def count_pending_media(
        self, miroir_id: str, connection_id: str, kinds: Sequence[str]
    ) -> int:
        """Files waiting, of the given kinds only: an entry of a kind that is no
        longer derived waits for nothing."""
        with self.engine.connect() as db:
            return int(
                db.execute(
                    sa.select(sa.func.count()).where(
                        media_pending.c.miroir_id == miroir_id,
                        media_pending.c.connection_id == connection_id,
                        media_pending.c.kind.in_(list(kinds)),
                    )
                ).scalar_one()
            )

    def begin_media_attempt(
        self, miroir_id: str, connection_id: str, kind: str, key: str
    ) -> None:
        """Count a decode about to begin: raised only while it is in flight."""
        self._media_attempts(
            miroir_id, connection_id, kind, key, media_pending.c.attempts + 1
        )

    def end_media_attempt(
        self, miroir_id: str, connection_id: str, kind: str, key: str
    ) -> None:
        """The decode reached an outcome, so nothing that happens later to the
        run counts against this file."""
        self._media_attempts(miroir_id, connection_id, kind, key, 0)

    def restore_media_attempt(
        self, miroir_id: str, connection_id: str, kind: str, key: str, attempts: int
    ) -> None:
        """Give back a try counted for a decode that failed for no fault of the file."""
        self._media_attempts(miroir_id, connection_id, kind, key, attempts)

    def _media_attempts(
        self, miroir_id: str, connection_id: str, kind: str, key: str, value: Any
    ) -> None:
        with self.engine.begin() as db:
            db.execute(
                media_pending.update()
                .where(
                    media_pending.c.miroir_id == miroir_id,
                    media_pending.c.connection_id == connection_id,
                    media_pending.c.kind == kind,
                    media_pending.c.key == key,
                )
                .values(attempts=value)
            )

    def remove_pending_media(
        self,
        miroir_id: str,
        connection_id: str,
        kind: str,
        done: list[PendingMedia],
    ) -> None:
        """Drop entries the run handled, unless a confirmation replaced them since."""
        with self.engine.begin() as db:
            for entry in done:
                db.execute(
                    media_pending.delete().where(
                        media_pending.c.miroir_id == miroir_id,
                        media_pending.c.connection_id == connection_id,
                        media_pending.c.kind == kind,
                        media_pending.c.key == entry.key,
                        media_pending.c.size == entry.size,
                    )
                )

    def backfill_after(self, miroir_id: str, connection_id: str, kind: str) -> str:
        """The last key a backfill walked past, or "" to start from the beginning."""
        with self.engine.connect() as db:
            after = db.execute(
                sa.select(media_backfills.c.after).where(
                    media_backfills.c.miroir_id == miroir_id,
                    media_backfills.c.connection_id == connection_id,
                    media_backfills.c.kind == kind,
                )
            ).scalar()
        return after or ""

    def set_backfill_after(
        self, miroir_id: str, connection_id: str, kind: str, after: str
    ) -> None:
        """Remember where the walk stopped; "" when it reached the end."""
        scope = (
            media_backfills.c.miroir_id == miroir_id,
            media_backfills.c.connection_id == connection_id,
            media_backfills.c.kind == kind,
        )
        with self.engine.begin() as db:
            db.execute(media_backfills.delete().where(*scope))
            if after:
                db.execute(
                    media_backfills.insert().values(
                        miroir_id=miroir_id,
                        connection_id=connection_id,
                        kind=kind,
                        after=after,
                        updated_at=datetime.now(timezone.utc),
                    )
                )

    def delete_media_records(self, miroir_id: str, connection_id: str) -> None:
        """Forget the queue and the walk of a removed connection; its files stay."""
        with self.engine.begin() as db:
            for table in (media_pending, media_backfills):
                db.execute(
                    table.delete().where(
                        table.c.miroir_id == miroir_id,
                        table.c.connection_id == connection_id,
                    )
                )
