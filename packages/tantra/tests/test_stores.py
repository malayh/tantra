from collections.abc import Iterator
from pathlib import Path

from tantra.events import SessionHeader, Stamped, TextPart
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


class SliceOnlyLog(list[Stamped]):
    def __iter__(self) -> Iterator[Stamped]:
        raise AssertionError("read_page iterated the journal")


async def test_memory_read_page_selects_only_the_requested_slice() -> None:
    store = MemoryStore()
    await store.setup()
    header = SessionHeader(id="bounded", agent="test")
    await store.create(header)
    await store.append(
        header.id,
        [TextPart(sample_id="sample", text=str(index)) for index in range(100)],
        expect_seq=0,
    )
    store._events[header.id] = SliceOnlyLog(store._events[header.id])

    page = await store.read_page(header.id, after=90, limit=3)

    assert [item.seq for item in page] == [91, 92, 93]
