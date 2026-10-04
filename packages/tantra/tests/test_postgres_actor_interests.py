from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from tantra import PostgresCoordinator, PostgresStore, SessionHeader
from tantra.coordinator import _Observation

psycopg = pytest.importorskip("psycopg")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb


async def next_matching(iterator: Any, predicate: Any) -> Any:
    async with asyncio.timeout(2):
        while True:
            snapshot = await anext(iterator)
            if predicate(snapshot):
                return snapshot


async def create_tree(store: PostgresStore, root: str, count: int = 100) -> list[str]:
    children = [f"{root}-child-{index:03d}" for index in range(count)]
    await store.create(SessionHeader(id=root, root_id=root, agent="bot", model="m"))
    for child in children:
        await store.create(SessionHeader(id=child, root_id=root, parent_id=root, agent="bot", model="m"))
    return children


async def test_actor_interest_registration_is_explicit_and_scheduled() -> None:
    store = PostgresStore("unused")
    coordinator = PostgresCoordinator(store)
    state = _Observation(users=1)
    coordinator._observations["root"] = state

    coordinator._set_actor_interests("root", {"first"})
    assert coordinator._actor_interests == {"root": frozenset({"first"})}
    assert state.interest_epoch == 1
    assert coordinator._dirty_roots == {"root"}

    coordinator._dirty_roots.clear()
    coordinator._set_actor_interests("root", {"first"})
    assert state.interest_epoch == 1
    assert not coordinator._dirty_roots

    coordinator._set_actor_interests("root", set())
    assert coordinator._actor_interests == {"root": frozenset()}
    assert state.interest_epoch == 2
    assert coordinator._dirty_roots == {"root"}

    coordinator._set_actor_interests("root", None)
    assert "root" not in coordinator._actor_interests
    assert state.interest_epoch == 3
    await coordinator.close()
    await store.close()


async def test_interest_epoch_discards_an_inflight_filtered_query(monkeypatch: pytest.MonkeyPatch) -> None:
    store = PostgresStore("unused")
    coordinator = PostgresCoordinator(store)
    root = uuid4().hex
    first = uuid4().hex
    second = uuid4().hex
    state = _Observation(users=1)
    coordinator._observations[root] = state
    coordinator._set_actor_interests(root, {first})
    coordinator._dirty_roots.clear()
    entered = asyncio.Event()
    release = asyncio.Event()

    class Cursor:
        async def fetchall(self) -> list[tuple[Any, ...]]:
            return [
                (
                    "root",
                    root,
                    None,
                    {
                        "change_id": 0,
                        "writer": None,
                        "owner": None,
                        "generation": 0,
                        "valid": False,
                        "expires": None,
                        "recovery": {},
                        "deleted": False,
                    },
                ),
                ("actor", root, first, [4, False, 0]),
            ]

    class Connection:
        async def execute(self, statement: Any, params: Any) -> Cursor:
            entered.set()
            await release.wait()
            return Cursor()

    @asynccontextmanager
    async def connection() -> Any:
        yield Connection()

    monkeypatch.setattr(coordinator, "_data_connection", connection)
    pending = asyncio.create_task(coordinator._catch_up({root}, periodic=False))
    await asyncio.wait_for(entered.wait(), 1)
    coordinator._set_actor_interests(root, {second})
    release.set()
    await pending

    assert state.snapshot is None
    assert coordinator._dirty_roots == {root}
    await coordinator.close()
    await store.close()


async def test_filtered_observation_fallback_addition_and_full_subscriber(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root = uuid4().hex
    children = await create_tree(store, root)
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    await coordinator.start()
    fallback = coordinator._observe_actors(root)
    filtered = None
    full = None
    second_full = None
    try:
        initial = await asyncio.wait_for(anext(fallback), 2)
        assert initial.actor_coverage is None
        assert set(initial.actors) == {root, *children}
        await fallback.aclose()

        missing = uuid4().hex
        interests = {root, children[49], missing}
        coordinator._set_actor_interests(root, interests)
        filtered = coordinator._observe_actors(root)
        selected = await asyncio.wait_for(anext(filtered), 2)
        assert selected.actor_coverage == frozenset(interests)
        assert set(selected.actors) == {root, children[49]}

        interests.add(children[99])
        coordinator._set_actor_interests(root, interests)
        added = await next_matching(filtered, lambda item: item.actor_coverage == frozenset(interests))
        assert set(added.actors) == {root, children[49], children[99]}

        full = coordinator.observe(root)
        root_wide = await asyncio.wait_for(anext(full), 2)
        assert root_wide.actor_coverage is None
        assert set(root_wide.actors) == {root, *children}
        second_full = coordinator.observe(root)
        assert (await asyncio.wait_for(anext(second_full), 2)).actor_coverage is None
        assert coordinator._observations[root].full_users == 2
        await full.aclose()
        full = None
        assert coordinator._observations[root].full_users == 1
        coordinator._schedule(root)
        still_full = await asyncio.wait_for(anext(second_full), 2)
        assert still_full.actor_coverage is None
        assert set(still_full.actors) == {root, *children}
        await second_full.aclose()
        second_full = None

        narrowed = await next_matching(filtered, lambda item: item.actor_coverage == frozenset(interests))
        assert set(narrowed.actors) == {root, children[49], children[99]}
    finally:
        if full is not None:
            await full.aclose()
        if second_full is not None:
            await second_full.aclose()
        if filtered is not None:
            await filtered.aclose()
        await fallback.aclose()
        coordinator._set_actor_interests(root, None)
        await coordinator.close()
        await store.close()


async def test_empty_actor_scope_keeps_root_control_current(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root = uuid4().hex
    await create_tree(store, root, count=1)
    coordinator = PostgresCoordinator(store, lease_ttl=0.2, catch_up_interval=0.03)
    await coordinator.start()
    ownership = await coordinator.acquire(root)
    assert ownership is not None
    coordinator._set_actor_interests(root, set())
    iterator = coordinator._observe_actors(root)
    try:
        initial = await asyncio.wait_for(anext(iterator), 2)
        assert initial.actor_coverage == frozenset()
        assert initial.actors == {}
        assert initial.owner_valid

        connection_id = uuid4()
        async with coordinator.transaction(ownership) as guarded:
            token = await guarded.claim_writer(connection_id)
        writer = await next_matching(iterator, lambda item: item.writer_connection == str(token.connection_id))
        assert writer.actors == {}

        listener = coordinator._listener_task
        assert listener is not None
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        coordinator._listener_task = None
        expired = await next_matching(iterator, lambda item: not item.owner_valid)
        assert expired.actor_coverage == frozenset()
        assert expired.writer_connection == str(connection_id)
    finally:
        await iterator.aclose()
        coordinator._set_actor_interests(root, None)
        await coordinator.close()
        await store.close()


async def test_actor_interest_queries_use_root_and_actor_indexes(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    created = datetime.now(UTC)
    root = "root-00000"
    rows = []
    for index in range(10_000):
        sid = f"root-{index:05d}"
        header = {"id": sid, "root_id": sid, "agent": "bot", "model": "m"}
        rows.append((sid, Jsonb(header), Jsonb({}), created, 0))
    children = []
    for index in range(100):
        sid = f"child-{index:03d}"
        header = {"id": sid, "root_id": root, "parent_id": root, "agent": "bot", "model": "m"}
        children.append((sid, Jsonb(header), Jsonb({}), root, created, 0))
    async with store._connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.executemany(
                store._sql(
                    "INSERT INTO {schema}.sessions (id, header, metadata, created_at, last_seq)"
                    " VALUES (%s, %s, %s, %s, %s)"
                ),
                rows,
            )
            await cursor.executemany(
                store._sql(
                    "INSERT INTO {schema}.sessions"
                    " (id, header, metadata, parent_id, created_at, last_seq)"
                    " VALUES (%s, %s, %s, %s, %s, %s)"
                ),
                children,
            )
        await conn.execute(store._sql("ANALYZE {schema}.sessions"))

        async def explain(statement: str, params: tuple[Any, ...]) -> dict[str, Any]:
            cursor = await conn.execute(
                psycopg.sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + store._sql(statement), params
            )
            return (await cursor.fetchone())[0][0]["Plan"]

        plans = {
            "full": await explain(
                "WITH full_roots AS (SELECT unnest(%s::text[]) AS root_id)"
                " SELECT actor.id FROM full_roots f JOIN {schema}.sessions actor"
                " ON actor.root_key = f.root_id",
                ([root],),
            ),
            "filtered": await explain(
                "WITH actor_interests AS (SELECT * FROM unnest(%s::text[], %s::text[])"
                " AS interests(root_id, actor_id))"
                " SELECT actor.id FROM actor_interests i JOIN LATERAL"
                " (SELECT actor.id, actor.root_key FROM {schema}.sessions actor"
                " WHERE actor.id = i.actor_id OFFSET 0) actor ON actor.root_key = i.root_id",
                ([root], [children[49][0]]),
            ),
        }

    def indexes(node: dict[str, Any]) -> set[str]:
        return ({node["Index Name"]} if "Index Name" in node else set()).union(
            *(indexes(child) for child in node.get("Plans", []))
        )

    assert "sessions_root_idx" in indexes(plans["full"])
    assert "sessions_pkey" in indexes(plans["filtered"])
    await store.close()
