"""The derive queue's tables, moved into media (package split, step 5).

`fixtures/media_queue_ddl.sql` was generated from `src.control.operations` before
the move. The tables the metadata store creates, and those media reads, must
compile to the same DDL in both dialects: moving them changed no schema, so
`SCHEMA_VERSION` stayed.
"""

import unittest
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateIndex, CreateTable

from src.control import operations
from src.media import queue

FIXTURE = Path(__file__).parent / "fixtures" / "media_queue_ddl.sql"
TABLES = ("media_pending", "media_backfills")


def ddl(metadata: sa.MetaData) -> str:
    out = []
    for name, dialect in (
        ("sqlite", sqlite.dialect()),
        ("postgresql", postgresql.dialect()),
    ):
        out.append(f"-- {name}")
        for table in metadata.sorted_tables:
            if table.name not in TABLES:
                continue
            out.append(str(CreateTable(table).compile(dialect=dialect)).strip() + ";")
            for index in sorted(table.indexes, key=lambda i: str(i.name)):
                out.append(
                    str(CreateIndex(index).compile(dialect=dialect)).strip() + ";"
                )
    return "\n".join(line.rstrip() for line in "\n".join(out).splitlines()) + "\n"


class MediaQueueSchemaTests(unittest.TestCase):
    def test_the_ddl_is_unchanged(self) -> None:
        expected = FIXTURE.read_text(encoding="utf-8")
        self.assertEqual(ddl(operations.metadata), expected)
        self.assertEqual(ddl(queue.metadata), expected)

    def test_the_store_holds_them_in_its_schema_version(self) -> None:
        self.assertEqual(operations.SCHEMA_VERSION, 8)
        self.assertEqual(set(queue.metadata.tables), set(TABLES))
        self.assertLessEqual(set(TABLES), set(operations.metadata.tables))


if __name__ == "__main__":
    unittest.main()
