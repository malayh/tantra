from pathlib import Path

from tantra.stores.fs import FileSystemStore
from tantra.stores.memory import MemoryStore
from tantra.testing import store_conformance


async def test_memory_store_conformance() -> None:
    store = MemoryStore()
    await store_conformance(lambda: store)


async def test_fs_store_conformance(tmp_path: Path) -> None:
    await store_conformance(lambda: FileSystemStore(tmp_path / "sessions"))
