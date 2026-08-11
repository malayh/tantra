import asyncio
import threading
import uuid
from datetime import UTC, datetime

import pytest

from tantra.errors import CorruptLog, SeqConflict
from tantra.events import SessionHeader, TextPart
from tantra.memory import BuiltinMemory, MemoryRecord, MemoryWrite
from tantra.stores.base import select_headers
from tantra.stores.postgres import PostgresStore

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")

CONTENDERS = 4
ROUNDS = 8
SETUP_RACERS = 6
DEAD_ROWS = 25

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
EDGE = MemoryWrite(
    kind="fact",
    title="Edge routing lives in the mesh",
    body="Traffic reaches workloads through the service mesh gateway.",
)

QUERY = "where does the p99 panel get its latency data"
UNRELATED = "kubernetes ingress annotations"

NEAR = [1.0, 0.0, 0.0]
FAR = [0.0, 1.0, 0.0]


class ScriptedEmbedder:
    def __init__(self, table: dict[str, list[float]]) -> None:
        self.table = table

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._lookup(text) for text in texts]

    def _lookup(self, text: str) -> list[float]:
        for needle, vector in self.table.items():
            if needle in text:
                return vector
        return [0.0, 0.0, 1.0]


class NonFiniteEmbedder:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [[float("inf"), 0.0, 0.0] for _ in texts]


async def _store(dsn: str, schema: str) -> PostgresStore:
    store = PostgresStore(dsn, schema=schema)
    await store.setup()
    return store


def _query(dsn: str, schema: str, statement: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(dsn) as conn:
        cursor = conn.execute(sql.SQL(statement).format(schema=sql.Identifier(schema)), params)
        return cursor.fetchall()


def _append_in_thread(dsn: str, schema: str, sid: str, barrier: threading.Barrier) -> int | None:
    async def attempt() -> int:
        store = PostgresStore(dsn, schema=schema)
        try:
            barrier.wait(timeout=30)
            return await store.append(sid, [TextPart(sample_id="s1", text="x" * 4096)], expect_seq=0)
        finally:
            await store.close()

    try:
        return asyncio.run(attempt())
    except SeqConflict:
        return None


def _setup_in_thread(dsn: str, schema: str, barrier: threading.Barrier) -> None:
    async def attempt() -> None:
        store = PostgresStore(dsn, schema=schema)
        try:
            barrier.wait(timeout=30)
            await store.setup()
        finally:
            await store.close()

    asyncio.run(attempt())


def _dead(index: int, *, deleted: bool) -> MemoryRecord:
    return MemoryRecord(
        id=f"dead-{index}",
        kind="fact",
        title="Edge routing lives in the mesh",
        body="Traffic reaches workloads through the service mesh gateway.",
        embedding=NEAR,
        deleted=deleted,
        superseded_by=None if deleted else "somewhere-else",
    )


async def test_setup_is_versioned_and_running_it_twice_leaves_the_version_unchanged(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    versions = _query(postgres_dsn, pg_schema, "SELECT version FROM {schema}.schema_version ORDER BY version")
    assert versions == [(1,), (2,)]

    await store.setup()
    await PostgresStore(postgres_dsn, schema=pg_schema).setup()

    assert _query(postgres_dsn, pg_schema, "SELECT version FROM {schema}.schema_version ORDER BY version") == [
        (1,),
        (2,),
    ]


async def test_a_second_store_instance_sees_the_first_ones_writes(postgres_dsn: str, pg_schema: str) -> None:
    writer = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build", metadata={"company": 42})
    await writer.create(header)
    await writer.append(header.id, [TextPart(sample_id="s1", text="one")], expect_seq=0)
    assert await writer.acquire_lease(header.id, "worker-a", 30) is True

    reader = PostgresStore(postgres_dsn, schema=pg_schema)

    loaded = await reader.header(header.id)
    assert loaded is not None
    assert loaded.last_seq == 1
    assert loaded.lease is not None
    assert loaded.lease.holder == "worker-a"
    assert [s.event.text async for s in reader.read(header.id)] == ["one"]
    assert [h.id for h in await reader.list(metadata={"company": 42})] == [header.id]
    assert await reader.acquire_lease(header.id, "worker-b", 30) is False

    with pytest.raises(SeqConflict):
        await reader.append(header.id, [TextPart(sample_id="s1", text="two")], expect_seq=0)
    assert [s.seq async for s in writer.read(header.id)] == [1]


async def test_a_corrupt_stored_event_row_raises_corrupt_log_naming_the_session(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text=f"part {i}") for i in range(2)], expect_seq=0)

    damaged = _query(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.events SET stamped = '{{\"seq\": 2}}'::jsonb WHERE session_id = %s AND seq = 2 RETURNING seq",
        (header.id,),
    )
    assert damaged == [(2,)]

    seen = []
    with pytest.raises(CorruptLog) as raised:
        async for stamped in store.read(header.id):
            seen.append(stamped)
    assert header.id in str(raised.value)
    assert [s.seq for s in seen] == [1]


async def test_a_row_that_keyword_misses_but_vector_matches_comes_back_tagged_vector(
    postgres_dsn: str, pg_schema: str
) -> None:
    embedder = ScriptedEmbedder({UNRELATED: NEAR, "Edge routing": NEAR, "Deploys run": FAR})
    memory = BuiltinMemory(await _store(postgres_dsn, pg_schema), embedder)
    edge_id = await memory.write(EDGE)
    await memory.write(DEPLOYS)

    hits = await memory.recall(UNRELATED)

    assert [(hit.memory.id, hit.mode) for hit in hits] == [(edge_id, "vector")]
    assert hits[0].score == pytest.approx(1.0)


async def test_keyword_and_vector_hits_merge_with_the_best_match_first(postgres_dsn: str, pg_schema: str) -> None:
    embedder = ScriptedEmbedder({QUERY: NEAR, "p99 panel reads": NEAR, "Deploys run": FAR})
    memory = BuiltinMemory(await _store(postgres_dsn, pg_schema), embedder)
    mimir_id = await memory.write(MIMIR)
    deploys_id = await memory.write(DEPLOYS)

    hits = await memory.recall(QUERY)

    assert [hit.memory.id for hit in hits] == [mimir_id, deploys_id]
    assert [hit.mode for hit in hits] == ["vector", "keyword"]
    assert hits[0].score == pytest.approx(1.0)
    assert 0 < hits[1].score < 1.0


async def test_a_live_row_stays_reachable_behind_a_full_page_of_dead_vector_matches(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    for index in range(DEAD_ROWS):
        await store.memory_put(_dead(index, deleted=index % 5 != 0))
    live = MemoryRecord(
        id="live",
        kind="fact",
        title="Edge routing lives in the mesh",
        body="Traffic reaches workloads through the service mesh gateway.",
        embedding=[0.99, 0.01, 0.0],
    )
    await store.memory_put(live)

    hits = await BuiltinMemory(store, ScriptedEmbedder({UNRELATED: NEAR})).recall(UNRELATED)

    assert [(hit.memory.id, hit.mode) for hit in hits] == [("live", "vector")]


async def test_the_vector_pass_applies_the_same_filters_as_the_keyword_pass(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    for kind in ("fact", "secret"):
        await store.memory_put(
            MemoryRecord(
                id=kind,
                kind=kind,
                title="Edge routing lives in the mesh",
                body="Traffic reaches workloads through the service mesh gateway.",
                tags=[kind],
                embedding=NEAR,
            )
        )
    memory = BuiltinMemory(store, ScriptedEmbedder({UNRELATED: NEAR}))

    assert [hit.memory.id for hit in await memory.recall(UNRELATED, kind="fact")] == ["fact"]
    assert [hit.memory.id for hit in await memory.recall(UNRELATED, tags=["secret"])] == ["secret"]
    assert await memory.recall(UNRELATED, kind="rumour") == []
    assert {hit.memory.id for hit in await memory.recall(UNRELATED)} == {"fact", "secret"}


async def test_a_non_finite_embedding_is_the_embed_failed_case_and_never_reaches_the_column(
    postgres_dsn: str, pg_schema: str
) -> None:
    memory = BuiltinMemory(await _store(postgres_dsn, pg_schema), NonFiniteEmbedder())

    mid = await memory.write(MIMIR)

    row = await memory.get(mid)
    assert row is not None
    assert row.embedding is None
    assert [(hit.memory.id, hit.mode) for hit in await memory.recall(QUERY)] == [(mid, "keyword")]


async def test_racing_appends_from_separate_instances_lose_with_seq_conflict(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)

    for _ in range(ROUNDS):
        header = SessionHeader(id=uuid.uuid4().hex, agent="build")
        await store.create(header)
        barrier = threading.Barrier(CONTENDERS)

        results = await asyncio.gather(
            *(
                asyncio.to_thread(_append_in_thread, postgres_dsn, pg_schema, header.id, barrier)
                for _ in range(CONTENDERS)
            )
        )

        assert results.count(1) == 1, f"{results.count(1)} writers appended at seq 1"
        assert [s.seq async for s in store.read(header.id)] == [1]
        loaded = await store.header(header.id)
        assert loaded is not None
        assert loaded.last_seq == 1


async def test_racing_setups_on_a_fresh_schema_all_succeed(postgres_dsn: str, pg_schema: str) -> None:
    barrier = threading.Barrier(SETUP_RACERS)

    await asyncio.gather(
        *(asyncio.to_thread(_setup_in_thread, postgres_dsn, pg_schema, barrier) for _ in range(SETUP_RACERS))
    )

    assert _query(postgres_dsn, pg_schema, "SELECT version FROM {schema}.schema_version ORDER BY version") == [
        (1,),
        (2,),
    ]


async def test_listing_orders_ids_exactly_the_way_select_headers_does(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    tag = uuid.uuid4().hex
    created = datetime.now(UTC)
    headers = [
        SessionHeader(id=f"{prefix}-{tag}", agent="build", created_at=created, metadata={"tag": tag})
        for prefix in ("A", "a", "B", "b", "Z", "z")
    ]
    for header in headers:
        await store.create(header)

    listed = [h.id for h in await store.list(metadata={"tag": tag})]

    assert listed == [h.id for h in select_headers(headers, metadata={"tag": tag})]
    assert listed == [f"{prefix}-{tag}" for prefix in ("z", "b", "a", "Z", "B", "A")]


async def test_an_unknown_before_cursor_returns_the_whole_page(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    tag = uuid.uuid4().hex
    headers = [SessionHeader(id=uuid.uuid4().hex, agent="build", metadata={"tag": tag}) for _ in range(3)]
    for header in headers:
        await store.create(header)

    listed = await store.list(metadata={"tag": tag}, before=uuid.uuid4().hex)

    assert [h.id for h in listed] == [h.id for h in select_headers(headers, metadata={"tag": tag})]
    assert len(listed) == 3


async def test_a_corrupt_stored_memory_row_raises_corrupt_log_naming_the_row(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    mid = await BuiltinMemory(store).write(MIMIR)

    damaged = _query(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.memories SET row = '{{\"id\": 4}}'::jsonb WHERE id = %s RETURNING id",
        (mid,),
    )
    assert damaged == [(mid,)]

    with pytest.raises(CorruptLog) as raised:
        await store.memory_all()
    assert mid in str(raised.value)

    with pytest.raises(CorruptLog) as caught:
        await store.memory_get(mid)
    assert mid in str(caught.value)


async def test_supersede_hides_the_old_row_from_recall_but_get_still_returns_it(
    postgres_dsn: str, pg_schema: str
) -> None:
    memory = BuiltinMemory(await _store(postgres_dsn, pg_schema))
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
