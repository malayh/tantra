import sqlite3
import uuid
from pathlib import Path

import pytest

from tantra.errors import CorruptLog
from tantra.events import SessionHeader, TextPart
from tantra.memory import BuiltinMemory, MemoryWrite
from tantra.stores.sqlite import SQLiteStore

DEPLOYS = MemoryWrite(
    kind="preference",
    title="Deploys run on Fridays",
    body="The team ships to production every Friday afternoon.",
)
MIMIR = MemoryWrite(
    kind="fact",
    title="The p99 panel reads from Mimir",
    body="Latency percentiles for the checkout service come from Mimir, not Prometheus.",
    entities=["mimir", "checkout"],
)
POSTGRES = MemoryWrite(
    kind="decision",
    title="Sessions live in Postgres",
    body="Session rows are stored in Postgres under a dedicated tantra schema.",
)

QUERY = "where does the p99 panel get its latency data"


class FakeEmbedder:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[float(len(text)), 1.0] for text in texts]


async def _store(tmp_path: Path) -> SQLiteStore:
    store = SQLiteStore(tmp_path / "nested" / "tantra.db")
    await store.setup()
    return store


async def test_the_database_runs_in_wal_mode(tmp_path: Path) -> None:
    store = await _store(tmp_path)

    conn = sqlite3.connect(store.path)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()

    assert mode == "wal"


async def test_a_corrupt_stored_event_row_raises_corrupt_log_naming_the_session(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text=f"part {i}") for i in range(2)], expect_seq=0)

    conn = sqlite3.connect(store.path)
    conn.execute("UPDATE events SET stamped = ? WHERE seq = 2", ('{"seq": 2}',))
    conn.commit()
    conn.close()

    seen = []
    with pytest.raises(CorruptLog) as raised:
        async for stamped in store.read(header.id):
            seen.append(stamped)
    assert header.id in str(raised.value)
    assert [s.seq for s in seen] == [1]


async def test_a_corrupt_stored_memory_row_raises_corrupt_log_naming_the_row(tmp_path: Path) -> None:
    store = await _store(tmp_path)
    mid = await BuiltinMemory(store).write(MIMIR)

    conn = sqlite3.connect(store.path)
    conn.execute("UPDATE memories SET row = ? WHERE id = ?", ('{"id": 4}', mid))
    conn.commit()
    conn.close()

    with pytest.raises(CorruptLog) as raised:
        await store.memory_all()
    assert mid in str(raised.value)

    with pytest.raises(CorruptLog) as caught:
        await store.memory_get(mid)
    assert mid in str(caught.value)


async def test_memory_rows_round_trip_and_stay_keyword_only_even_with_an_embedder(tmp_path: Path) -> None:
    memory = BuiltinMemory(await _store(tmp_path), FakeEmbedder())
    for row in (DEPLOYS, MIMIR, POSTGRES):
        await memory.write(row)

    hits = await memory.recall(QUERY)

    assert [hit.memory.title for hit in hits][0] == MIMIR.title
    assert {hit.mode for hit in hits} == {"keyword"}
    assert (await memory.get(hits[0].memory.id)).embedding is not None


async def test_supersede_hides_the_old_row_from_recall_but_get_still_returns_it(tmp_path: Path) -> None:
    memory = BuiltinMemory(await _store(tmp_path))
    old_id = await memory.write(MIMIR)
    replacement = MemoryWrite(
        kind="fact",
        title="The p99 panel reads from Thanos",
        body="Latency percentiles for the checkout service moved off Mimir onto Thanos.",
    )

    new_id = await memory.supersede(old_id, replacement)

    assert [hit.memory.id for hit in await memory.recall(QUERY)] == [new_id]
    stale = await memory.get(old_id)
    assert stale is not None
    assert stale.superseded_by == new_id
