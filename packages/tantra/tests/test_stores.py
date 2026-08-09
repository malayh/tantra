from pathlib import Path

from tantra.stores.fs import FileSystemStore
from tantra.stores.memory import MemoryStore
from tantra.stores.postgres import PostgresStore
from tantra.stores.sqlite import SQLiteStore
from tantra.testing import store_conformance


async def test_memory_store_conformance() -> None:
    store = MemoryStore()
    await store_conformance(lambda: store)


async def test_fs_store_conformance(tmp_path: Path) -> None:
    await store_conformance(lambda: FileSystemStore(tmp_path / "sessions"))


async def test_sqlite_store_conformance(tmp_path: Path) -> None:
    await store_conformance(lambda: SQLiteStore(tmp_path / "sessions" / "tantra.db"))


async def test_postgres_store_conformance(postgres_dsn: str, pg_schema: str) -> None:
    await store_conformance(lambda: PostgresStore(postgres_dsn, schema=pg_schema))
