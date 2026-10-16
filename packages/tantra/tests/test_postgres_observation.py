from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import Agent, PostgresCoordinator, PostgresStore, Runtime, SessionHeader
from tantra import runtime as runtime_module
from tantra.coordinator import TREE_KINDS, CommandReply, RootObservation, _Observation
from tantra.events import (
    InputQueued,
    SessionCreated,
    TextDelta,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
)
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd
from tantra.providers.fake import FAKE_LIMITS
from tantra.stores.postgres import MIGRATIONS

psycopg = pytest.importorskip("psycopg")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb


class Bot(Agent):
    pass


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="ok")


async def eventually(predicate: Any, timeout: float = 5) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


async def close_all(iterators: list[Any]) -> None:
    await asyncio.gather(*(iterator.aclose() for iterator in iterators))


async def test_journal_hint_deadline_does_not_slide_under_continuous_notifications(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PostgresStore("unused")
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    coordinator._observations["root"] = _Observation()
    refreshed = asyncio.Event()

    async def catch_up(roots: set[str], *, periodic: bool) -> None:
        assert roots == {"root"} and not periodic
        refreshed.set()

    monkeypatch.setattr(coordinator, "_catch_up", catch_up)
    coordinator._schedule("root", journal=True)
    deadline = coordinator._journal_deadlines["root"]
    assert 0 < deadline - asyncio.get_running_loop().time() <= 0.025
    task = asyncio.create_task(coordinator._observe())
    try:
        async with asyncio.timeout(1):
            while not refreshed.is_set():
                coordinator._schedule("root", journal=True)
                assert coordinator._journal_deadlines["root"] == deadline
                await asyncio.sleep(0.001)
        assert "root" not in coordinator._journal_deadlines
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await coordinator.close()
        await store.close()


async def test_control_preempts_only_its_root_and_queries_do_not_lose_new_hints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PostgresStore("unused")
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    coordinator._observations = {root: _Observation() for root in ("first", "second", "control")}
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[set[str]] = []

    async def catch_up(roots: set[str], *, periodic: bool) -> None:
        calls.append(roots)
        if len(calls) == 1:
            entered.set()
            await release.wait()

    monkeypatch.setattr(coordinator, "_catch_up", catch_up)
    coordinator._schedule("first", journal=True)
    coordinator._schedule("second", journal=True)
    second_deadline = coordinator._journal_deadlines["second"]
    coordinator._schedule("first")
    assert "first" not in coordinator._journal_deadlines
    assert coordinator._journal_deadlines["second"] == second_deadline
    task = asyncio.create_task(coordinator._observe())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert "first" in calls[0]
        coordinator._schedule("first", journal=True)
        coordinator._schedule("control", dispatch=True)
        assert coordinator._dispatch_event.is_set()
        release.set()
        await eventually(lambda: any("control" in roots for roots in calls))
        await eventually(lambda: sum("first" in roots for roots in calls) == 2)
        await eventually(lambda: any("second" in roots for roots in calls))
        assert not coordinator._journal_deadlines
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await coordinator.close()
        await store.close()


async def test_observer_departure_and_close_clear_pending_journal_deadlines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = PostgresStore("unused")
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    coordinator._started = True
    root = uuid4().hex
    iterator = coordinator.observe(root)
    first = asyncio.create_task(anext(iterator))
    await eventually(lambda: root in coordinator._observations)
    coordinator._dirty_roots.clear()
    coordinator._schedule(root, journal=True)
    assert root in coordinator._journal_deadlines
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    await iterator.aclose()
    assert root not in coordinator._journal_deadlines
    coordinator._observations[root] = _Observation()
    coordinator._schedule(root, journal=True)
    await coordinator.close()
    assert not coordinator._journal_deadlines and not coordinator._dirty_roots
    await store.close()


@pytest.mark.parametrize("terminal", [TurnCompleted, TurnFailed, TurnCancelled, TurnInterrupted])
async def test_observation_terminal_evidence_survives_lost_hints_and_transport_expiry(
    postgres_dsn: str, pg_schema: str, terminal: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root = uuid4().hex
    await store.create(SessionHeader(id=root, root_id=root, agent="bot", model="m"))
    await store.append(root, [SessionCreated(agent="bot", root_id=root, model="m")])
    coordinator = PostgresCoordinator(store, catch_up_interval=0.03)
    await coordinator.start()
    iterator = coordinator.observe(root)
    try:
        initial = await anext(iterator)
        assert initial.actors[root] == (1, False) and initial.terminal_sequences == {root: 0}
        assert coordinator._listener_task is not None
        coordinator._listener_task.cancel()
        await asyncio.gather(coordinator._listener_task, return_exceptions=True)
        coordinator._listener_task = None
        args = (
            {"stop_reason": "stop"}
            if terminal is TurnCompleted
            else {"error": "failed"}
            if terminal is TurnFailed
            else {"reason": "test"}
        )
        end = await store.append(root, [terminal(turn_id=uuid4().hex, **args), TextDelta(text="after terminal")])
        async with store._connection() as conn:
            await conn.execute(store._sql("DELETE FROM {schema}.coordinator_changes WHERE root_id = %s"), (root,))

        def no_body_decode(*args: Any) -> None:
            raise AssertionError("observation decoded an event body")

        monkeypatch.setattr("tantra.stores.postgres._parse", no_body_decode)
        async with asyncio.timeout(2):
            while coordinator.observation(root).terminal_sequences[root] != end - 1:
                await anext(iterator)
        current = coordinator.observation(root)
        assert current.actors[root] == (end, False) and current.terminal_sequences[root] == end - 1
    finally:
        await iterator.aclose()
        await coordinator.close()
        await store.close()


async def test_malformed_notification_kinds_do_not_stop_listener(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store, catch_up_interval=0.04)
    await coordinator.start()
    root = uuid4().hex
    iterator = coordinator.observe(root)
    try:
        await anext(iterator)
        await eventually(lambda: coordinator._listener_conn is not None)
        async with store._connection() as conn:

            async def notify(kind: Any) -> None:
                await conn.execute(
                    "SELECT pg_notify(%s, %s)",
                    (coordinator._channel, json.dumps({"root_id": root, "kind": kind})),
                )

            await notify("journal")
            await eventually(lambda: coordinator.routed_wakeups > 0)
            before = coordinator.routed_wakeups
            for kind in ([], {}, None, 42):
                await notify(kind)
            await notify("journal")
            await eventually(lambda: coordinator.routed_wakeups >= before + 5)
        assert coordinator._listener_task is not None
        assert not coordinator._listener_task.done()
        assert coordinator.observation(root) is not None
    finally:
        await iterator.aclose()
        await coordinator.close()
        await store.close()


@pytest.mark.parametrize("count", [1, 64, 1000])
async def test_idle_observers_share_one_periodic_check(postgres_dsn: str, pg_schema: str, count: int) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store, catch_up_interval=0.04)
    await coordinator.start()
    roots = [uuid4().hex for _ in range(count)]
    iterators = [coordinator.observe(root) for root in roots]
    try:
        snapshots = await asyncio.gather(*(anext(iterator) for iterator in iterators))
        assert [snapshot.root_id for snapshot in snapshots] == roots
        settled = coordinator.observation_ticks
        await eventually(lambda: coordinator.observation_ticks > settled and not coordinator._dirty_roots)
        before = coordinator.observation_checks, coordinator.observation_ticks
        await eventually(lambda: coordinator.observation_ticks >= before[1] + 3)
        after = coordinator.observation_checks, coordinator.observation_ticks
        assert after[0] - before[0] == after[1] - before[1] == 3
        checks = coordinator.observation_checks
        assert all(coordinator.observation(root) is not None for root in roots)
        assert coordinator.observation_checks == checks
    finally:
        await close_all(iterators)
        assert not coordinator._observations
        await coordinator.close()
        await store.close()


async def test_runtime_routes_actor_changes_without_idle_journal_reads(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store, catch_up_interval=0.03)
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    first = await runtime.create(Bot)
    second = await runtime.create(Bot)
    first_seq = (await runtime.status(first)).last_seq
    second_seq = (await runtime.status(second)).last_seq
    reads: Counter[str] = Counter()
    read_page = store.read_page

    async def counted(sid: str, *, after: int = 0, limit: int = 1000):
        reads[sid] += 1
        return await read_page(sid, after=after, limit=limit)

    monkeypatch.setattr(store, "read_page", counted)
    streams = [runtime.events(first, after=first_seq), runtime.events(second, after=second_seq)]
    pending = [asyncio.create_task(anext(stream)) for stream in streams]
    try:
        await eventually(lambda: len(coordinator._observations) == 2)
        await eventually(lambda: all(coordinator.observation(root.hex) is not None for root in (first, second)))
        await eventually(
            lambda: all(
                coordinator.observation(root.hex).actor_coverage == frozenset({root.hex})
                and runtime._pages.readers[root.hex].flight is None
                for root in (first, second)
            )
        )
        for _ in range(5):
            baseline = reads.copy()
            tick = coordinator.observation_ticks
            await eventually(lambda tick=tick: coordinator.observation_ticks >= tick + 2)
            if reads == baseline:
                break
        assert reads == baseline
        tick = coordinator.observation_ticks
        await eventually(lambda: coordinator.observation_ticks >= tick + 3)
        assert reads == baseline
        async with runtime.connect(first, writable=True) as connection:
            result = await connection.prompt("go", command_id=uuid4())
        assert result.outcome == "completed"
        item = await asyncio.wait_for(pending[0], 2)
        assert item.seq == first_seq + 1
        assert not pending[1].done()
        assert reads[first.hex] > baseline[first.hex]
        assert reads[second.hex] == baseline[second.hex]
        await streams[0].aclose()
        resumed = runtime.events(first, after=item.seq)
        try:
            following = await asyncio.wait_for(anext(resumed), 2)
            assert following.seq == item.seq + 1
        finally:
            await resumed.aclose()
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await close_all(streams)
        await runtime.aclose()
        await store.close()


async def test_periodic_observation_recovers_expired_transport_and_owner(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root = uuid4().hex
    await store.create(SessionHeader(id=root, root_id=root, agent="bot", model="m"))
    await store.append(root, [SessionCreated(agent="bot", root_id=root, model="m")])
    coordinator = PostgresCoordinator(store, lease_ttl=0.2, catch_up_interval=0.03)
    await coordinator.start()
    ownership = await coordinator.acquire(root)
    assert ownership is not None
    iterator = coordinator.observe(root)
    try:
        initial = await anext(iterator)
        assert initial.owner_valid and initial.actors[root] == (1, False)
        listener = coordinator._listener_task
        assert listener is not None
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        coordinator._listener_task = None
        async with coordinator.transaction(ownership) as guarded:
            await guarded.append(root, [InputQueued(command_id=uuid4().hex, input="retained")])
        async with store._connection() as conn:
            await conn.execute(store._sql("DELETE FROM {schema}.coordinator_changes WHERE root_id = %s"), (root,))
        recovered = initial
        async with asyncio.timeout(2):
            while recovered.actors[root][0] == 1 or recovered.owner_valid:
                recovered = await anext(iterator)
        assert recovered.actors[root] == (2, False)
        assert not recovered.owner_valid
    finally:
        await iterator.aclose()
        await coordinator.close()
        await store.close()


async def test_shared_observation_queries_use_the_scaling_indexes(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    created = datetime.now(UTC)
    old = created - timedelta(hours=25)
    future = created + timedelta(hours=1)
    rows = []
    for index in range(10_000):
        sid = f"root-{index:05d}"
        header = {"id": sid, "root_id": sid, "agent": "bot", "model": "m"}
        rows.append((sid, Jsonb(header), Jsonb({}), created, 0))
    async with store._connection() as conn:

        async def insert_many(statement: str, params: list[tuple[Any, ...]]) -> None:
            async with conn.cursor() as cursor:
                await cursor.executemany(store._sql(statement), params)

        await insert_many(
            "INSERT INTO {schema}.sessions (id, header, metadata, created_at, last_seq) VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
        children = []
        for index in range(100):
            sid = f"child-{index:05d}"
            header = {"id": sid, "root_id": "root-00000", "parent_id": "root-00000", "agent": "bot", "model": "m"}
            children.append((sid, Jsonb(header), Jsonb({}), "root-00000", created, 0))
        await insert_many(
            "INSERT INTO {schema}.sessions (id, header, metadata, parent_id, created_at, last_seq)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            children,
        )
        owners = [(f"root-{index:05d}", "instance", 1, future) for index in range(100)]
        await insert_many(
            "INSERT INTO {schema}.coordinator_roots (root_id, owner_instance, generation, expires_at)"
            " VALUES (%s, %s, %s, %s)",
            owners,
        )
        requests = []
        for index in range(10_000):
            expired = index < 100
            requests.append(
                (
                    uuid4(),
                    f"root-{index % 100:05d}",
                    "instance",
                    1,
                    Jsonb({}),
                    old if expired else future,
                    old if expired else None,
                    created + timedelta(microseconds=index),
                )
            )
        await insert_many(
            "INSERT INTO {schema}.coordinator_requests"
            " (request_id, root_id, destination_instance, destination_generation, envelope,"
            " deadline, completed_at, created_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            requests,
        )
        changes = [
            ("root-00000", "journal", old if index < 100 else created + timedelta(microseconds=index))
            for index in range(10_000)
        ]
        await insert_many(
            "INSERT INTO {schema}.coordinator_changes (root_id, kind, created_at) VALUES (%s, %s, %s)",
            changes,
        )
        await conn.execute(
            store._sql("ANALYZE {schema}.sessions, {schema}.coordinator_requests, {schema}.coordinator_changes")
        )

        async def explain(statement: str, params: tuple[Any, ...]) -> dict[str, Any]:
            cursor = await conn.execute(
                psycopg.sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + store._sql(statement), params
            )
            return (await cursor.fetchone())[0][0]["Plan"]

        plans = {
            "root": await explain(
                "SELECT id FROM {schema}.sessions WHERE root_key = %s",
                ("root-05000",),
            ),
            "sessions": await explain(
                'SELECT id FROM {schema}.sessions ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT 100',
                (),
            ),
            "children": await explain(
                "SELECT id FROM {schema}.sessions WHERE parent_id = %s"
                ' ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT 100',
                ("root-00000",),
            ),
            "catch_up": await explain(
                "WITH interested AS (SELECT unnest(%s::text[]) AS root_id)"
                " SELECT s.id FROM interested i JOIN {schema}.sessions s"
                " ON s.root_key = i.root_id",
                (["root-00001", "root-05000", "root-09999"],),
            ),
            "root_request": await explain(
                "SELECT request_id FROM {schema}.coordinator_requests"
                " WHERE root_id = %s AND reply IS NULL ORDER BY created_at, request_id LIMIT 1",
                ("root-00000",),
            ),
            "request": await explain(
                "SELECT q.request_id FROM {schema}.coordinator_requests q"
                " JOIN {schema}.coordinator_roots r ON r.root_id = q.root_id"
                " WHERE q.reply IS NULL AND q.deadline > clock_timestamp()"
                " AND NOT (q.root_id = ANY(%s::text[]))"
                " AND q.destination_instance = %s AND q.destination_generation = r.generation"
                " AND r.owner_instance = %s AND r.expires_at > clock_timestamp()"
                " ORDER BY q.created_at, q.request_id LIMIT 1",
                ([], "instance", "instance"),
            ),
        }
        async with conn.transaction(force_rollback=True):
            plans["request_cleanup"] = await explain(
                "WITH doomed AS (SELECT request_id FROM {schema}.coordinator_requests"
                " WHERE coalesce(completed_at, deadline) < clock_timestamp() - interval '24 hours'"
                " ORDER BY coalesce(completed_at, deadline), request_id LIMIT %s FOR UPDATE SKIP LOCKED)"
                " DELETE FROM {schema}.coordinator_requests r USING doomed d"
                " WHERE r.request_id = d.request_id",
                (100,),
            )
            plans["change_cleanup"] = await explain(
                "WITH doomed AS (SELECT id FROM {schema}.coordinator_changes"
                " WHERE created_at < clock_timestamp() - interval '24 hours'"
                " ORDER BY created_at, id LIMIT %s FOR UPDATE SKIP LOCKED)"
                " DELETE FROM {schema}.coordinator_changes c USING doomed d WHERE c.id = d.id",
                (100,),
            )

    def indexes(node: dict[str, Any]) -> set[str]:
        return ({node["Index Name"]} if "Index Name" in node else set()).union(
            *(indexes(child) for child in node.get("Plans", []))
        )

    assert "sessions_root_idx" in indexes(plans["root"])
    assert "sessions_order_idx" in indexes(plans["sessions"])
    assert "sessions_parent_order_idx" in indexes(plans["children"])
    assert "sessions_root_idx" in indexes(plans["catch_up"])
    assert "coordinator_requests_root_order_idx" in indexes(plans["root_request"])
    assert "coordinator_requests_root_order_idx" in indexes(plans["request"])
    assert "coordinator_requests_cleanup_idx" in indexes(plans["request_cleanup"])
    assert "coordinator_changes_cleanup_idx" in indexes(plans["change_cleanup"])
    await store.close()


LEGACY_CATCH_UP = (
    "WITH interested AS (SELECT unnest(%s::text[]) AS root_id)"
    ", full_roots AS (SELECT unnest(%s::text[]) AS root_id)"
    ", actor_interests AS (SELECT * FROM unnest(%s::text[], %s::text[])"
    " AS interests(root_id, actor_id))"
    " SELECT 'root', i.root_id, NULL::text, jsonb_build_object("
    " 'change_id', coalesce(c.id, 0), 'writer', r.writer_connection,"
    " 'owner', r.owner_instance, 'generation', coalesce(r.generation, 0),"
    " 'valid', coalesce(r.owner_instance IS NOT NULL AND r.expires_at > clock_timestamp(), false),"
    " 'expires', r.expires_at, 'recovery', coalesce(r.recovery, '{{}}'::jsonb),"
    " 'deleted', d.actor_id IS NOT NULL)"
    " FROM interested i LEFT JOIN {schema}.coordinator_roots r ON r.root_id = i.root_id"
    " LEFT JOIN {schema}.deleted_sessions d ON d.actor_id = i.root_id"
    " LEFT JOIN LATERAL (SELECT id FROM {schema}.coordinator_changes"
    " WHERE root_id = i.root_id ORDER BY id DESC LIMIT 1) c ON true"
    " UNION ALL SELECT 'actor', s.root_id, s.id, jsonb_build_array(s.last_seq,"
    " coalesce(a.active AND r.owner_instance IS NOT NULL"
    " AND r.expires_at > clock_timestamp(), false), coalesce(t.seq, 0))"
    " FROM (SELECT f.root_id, actor.id, actor.last_seq"
    " FROM full_roots f JOIN {schema}.sessions actor ON actor.root_key = f.root_id"
    " UNION ALL SELECT i.root_id, actor.id, actor.last_seq"
    " FROM actor_interests i JOIN LATERAL (SELECT actor.id, actor.last_seq, actor.root_key"
    " FROM {schema}.sessions actor WHERE actor.id = i.actor_id OFFSET 0) actor"
    " ON actor.root_key = i.root_id) s"
    " LEFT JOIN {schema}.coordinator_activity a ON a.root_id = s.root_id AND a.actor_id = s.id"
    " LEFT JOIN {schema}.coordinator_roots r ON r.root_id = s.root_id"
    " LEFT JOIN LATERAL (SELECT seq FROM {schema}.journal_index"
    " WHERE actor_id = s.id AND seq <= s.last_seq AND event_type IN"
    " ('turn_completed', 'turn_failed', 'turn_cancelled', 'turn_interrupted')"
    " ORDER BY seq DESC LIMIT 1) t ON true"
    " UNION ALL SELECT 'reply', request_id::text, NULL::text, reply"
    " FROM {schema}.coordinator_requests WHERE request_id = ANY(%s::uuid[])"
)


def legacy_observations(
    coordinator: PostgresCoordinator, rows: list[tuple[Any, ...]], scopes: dict[str, Any]
) -> tuple[dict[str, RootObservation], dict[UUID, CommandReply | None]]:
    headers: dict[str, dict[str, Any]] = {}
    actors: dict[str, dict[str, tuple[int, bool]]] = {root: {} for root in scopes}
    terminals: dict[str, dict[str, int]] = {root: {} for root in scopes}
    replies: dict[UUID, CommandReply | None] = {}
    for kind, key, actor, data in rows:
        if kind == "root":
            headers[key] = data
        elif kind == "actor":
            actors[key][actor] = (int(data[0]), bool(data[1]))
            terminals[key][actor] = int(data[2])
        elif UUID(key) in coordinator._pending_requests:
            replies[UUID(key)] = CommandReply.model_validate(data) if data else None
    snapshots = {
        root_id: RootObservation(
            root_id=root_id,
            sample=coordinator._observation_sample,
            change_id=int(data["change_id"]),
            writer_connection=data["writer"],
            owner_instance=data["owner"],
            owner_generation=int(data["generation"]),
            owner_valid=bool(data["valid"]),
            recovery=dict(data["recovery"]),
            actors=actors[root_id],
            expires_at=datetime.fromisoformat(data["expires"]) if data["expires"] else None,
            deleted=bool(data["deleted"]),
            terminal_sequences=terminals[root_id],
            actor_coverage=scopes[root_id],
        )
        for root_id, data in headers.items()
    }
    return snapshots, replies


@pytest.mark.parametrize("periodic", [False, True])
async def test_typed_catch_up_matches_the_legacy_jsonb_rows(
    postgres_dsn: str, pg_schema: str, periodic: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    separator = "&" if "?" in postgres_dsn else "?"
    store = PostgresStore(f"{postgres_dsn}{separator}options=-c%20TimeZone%3DAsia%2FKolkata", schema=pg_schema)
    await store.setup()
    missing, expired, interested, deleted = (uuid4().hex for _ in range(4))
    trees = {root: [f"{root}-a", f"{root}-b"] for root in (missing, expired, interested)}
    for root, children in trees.items():
        await store.create(SessionHeader(id=root, agent="bot", model="m"))
        for child in children:
            await store.create(SessionHeader(id=child, root_id=root, parent_id=root, agent="bot", model="m"))
        turn = uuid4().hex
        for actor, terminal in ((root, TurnCancelled(turn_id=turn, reason="x")), (children[0], None)):
            events: list[Any] = [InputQueued(command_id=turn, input="go"), TurnStarted(turn_id=turn, input="go")]
            await store.append(actor, events + ([terminal, TextDelta(text="after")] if terminal else []))
        await store.append(children[1], [TurnCompleted(turn_id=turn, stop_reason="stop"), TextDelta(text="x")])
    answered, pending, unrelated = uuid4(), uuid4(), uuid4()
    reply = CommandReply(request_id=answered, result={"nested": {"items": [1, {"deep": None}], "text": "雪"}})
    recovery = {"phase": "interrupting", "cursor": 4, "nested": {"items": [1, {"deep": None}], "text": "雪"}}
    old = datetime(2020, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
    future = datetime.now(UTC).replace(microsecond=123456) + timedelta(hours=1)
    async with store._connection() as conn:
        assert (await (await conn.execute("SHOW TimeZone")).fetchone())[0] == "Asia/Kolkata"
        async with conn.cursor() as cursor:
            await cursor.executemany(
                store._sql(
                    "INSERT INTO {schema}.coordinator_roots"
                    " (root_id, owner_instance, generation, expires_at, writer_connection, recovery)"
                    " VALUES (%s, %s, %s, %s, %s, %s)"
                ),
                [
                    (expired, "old-owner", 3, old, "writer-a", Jsonb(recovery)),
                    (interested, "live-owner", 5, future, None, Jsonb({})),
                    (deleted, None, 2, None, None, Jsonb({})),
                ],
            )
            await cursor.executemany(
                store._sql("INSERT INTO {schema}.coordinator_activity (root_id, actor_id, active) VALUES (%s, %s, %s)"),
                [
                    (expired, expired, True),
                    (interested, interested, True),
                    (interested, f"{interested}-a", False),
                ],
            )
            await cursor.executemany(
                store._sql("INSERT INTO {schema}.coordinator_changes (root_id, kind) VALUES (%s, %s)"),
                [(expired, "journal"), (interested, "header"), (interested, "writer"), (deleted, "deleted")],
            )
            await cursor.executemany(
                store._sql(
                    "INSERT INTO {schema}.coordinator_requests"
                    " (request_id, root_id, destination_instance, destination_generation, envelope, deadline, reply)"
                    " VALUES (%s, %s, 'live-owner', 5, '{{}}'::jsonb, %s, %s)"
                ),
                [
                    (answered, interested, future, Jsonb(reply.model_dump(mode="json"))),
                    (pending, interested, future, None),
                    (unrelated, interested, future, Jsonb(reply.model_dump(mode="json"))),
                ],
            )
        await conn.execute(
            store._sql("INSERT INTO {schema}.deleted_sessions (actor_id, root_id) VALUES (%s, %s)"), (deleted, deleted)
        )
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    coordinator._started = True
    for root in (missing, expired, deleted):
        coordinator._observations[root] = _Observation(users=1, full_users=1)
    coordinator._observations[interested] = _Observation(users=1)
    coordinator._actor_interests[interested] = frozenset({interested, f"{interested}-a", "absent"})
    coordinator._pending_requests = {answered: (interested, 1), pending: (interested, 1)}
    scopes = {root: coordinator._actor_interests.get(root) if root == interested else None for root in trees}
    scopes[deleted] = None
    legacy: list[list[tuple[Any, ...]]] = []
    execute = psycopg.AsyncConnection.execute

    async def capture(conn: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
        cursor = await execute(conn, query, params, **kwargs)
        if not isinstance(query, str) and query.as_string(conn).startswith("WITH interested"):
            legacy.append(await (await execute(conn, store._sql(LEGACY_CATCH_UP), params)).fetchall())
        return cursor

    try:
        with monkeypatch.context() as patch:
            patch.setattr(psycopg.AsyncConnection, "execute", capture)
            await coordinator._catch_up(set(scopes), periodic=periodic)
        assert len(legacy) == 1
        expected, replies = legacy_observations(coordinator, legacy[0], scopes)
        assert set(expected) == set(scopes)
        markers = dict.fromkeys(scopes, 0)
        async with store._connection() as conn:
            cursor = await conn.execute(store._sql("SELECT root_id, kind, id FROM {schema}.coordinator_changes"))
            for root, kind, change_id in await cursor.fetchall():
                if kind in TREE_KINDS:
                    markers[root] = max(markers[root], change_id)
        assert markers[interested] > 0 and markers[deleted] > 0 and markers[expired] == markers[missing] == 0
        assert {root: coordinator.observation(root) for root in scopes} == {
            root: replace(snapshot, tree_change_id=markers[root]) for root, snapshot in expected.items()
        }
        assert coordinator._observed_replies == replies == {answered: reply, pending: None}
        assert coordinator.observation(expired).expires_at == old
        assert coordinator.observation(interested).expires_at == future
        for root in (expired, interested):
            offset = coordinator.observation(root).expires_at.utcoffset()
            assert offset == expected[root].expires_at.utcoffset() == timedelta(hours=5, minutes=30)
        assert coordinator.observation(expired).recovery == recovery
        assert coordinator.observation(interested).actors == {
            interested: (4, True),
            f"{interested}-a": (2, False),
        }
        assert coordinator.observation(missing).terminal_sequences == {
            missing: 3,
            f"{missing}-a": 0,
            f"{missing}-b": 1,
        }
        assert coordinator.observation(deleted).deleted and not coordinator.observation(expired).owner_valid
        assert coordinator.observation(interested).owner_valid
    finally:
        await coordinator.close()
        await store.close()


def catch_up_capture(coordinator: PostgresCoordinator, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    captured: list[Any] = []
    original = coordinator._sql

    def capture(body: str) -> Any:
        result = original(body)
        if body.startswith("WITH interested"):
            captured.append(result)
        return result

    monkeypatch.setattr(coordinator, "_sql", capture)
    return captured


def kind_lists(body: str) -> list[list[str]]:
    return [re.findall(r"'([a-z_]+)'", listed) for listed in re.findall(r"\bkind IN \(([^)]*)\)", body)]


async def test_tree_kinds_are_one_set_for_runtime_catch_up_and_migration_13(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    coordinator._started = True
    coordinator._observations["root"] = _Observation()
    captured = catch_up_capture(coordinator, monkeypatch)
    try:
        await coordinator._catch_up({"root"}, periodic=False)
        async with store._connection() as conn:
            body = captured[0].as_string(conn)
            cursor = await conn.execute(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND indexname = 'coordinator_changes_tree_idx'",
                (pg_schema,),
            )
            definition = (await cursor.fetchone())[0]
        assert runtime_module.TREE_KINDS is TREE_KINDS
        assert TREE_KINDS == frozenset({"header", "activity", "ownership", "deleted"})
        assert len(MIGRATIONS[12]) == 1
        assert kind_lists(body) == kind_lists(MIGRATIONS[12][0]) == [["header", "activity", "ownership", "deleted"]]
        assert "ON {schema}.coordinator_changes (root_id, id)" in MIGRATIONS[12][0]
        assert "(root_id, id)" in definition
        assert set(re.findall(r"'([a-z_]+)'::text", definition)) == TREE_KINDS
        assert coordinator.observation("root").tree_change_id == 0
    finally:
        await coordinator.close()
        await store.close()


async def test_the_tree_marker_reads_one_index_row_under_a_journal_backlog(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, other = "root-a", "root-b"
    async with store._connection() as conn:
        cursor = await conn.execute(
            store._sql("INSERT INTO {schema}.coordinator_changes (root_id, kind) VALUES (%s, 'header') RETURNING id"),
            (root,),
        )
        header_id = (await cursor.fetchone())[0]
        cursor = await conn.execute(
            store._sql(
                "INSERT INTO {schema}.coordinator_changes (root_id, kind)"
                " SELECT %s, 'journal' FROM generate_series(1, 10000) RETURNING id"
            ),
            (root,),
        )
        last_id = max(row[0] for row in await cursor.fetchall())
        await conn.execute(
            store._sql(
                "INSERT INTO {schema}.coordinator_changes (root_id, kind)"
                " SELECT %s, kind FROM unnest(%s::text[]) AS kinds(kind), generate_series(1, 2000)"
            ),
            (other, ["header", "activity", "ownership", "deleted", "journal"]),
        )
        await conn.execute(store._sql("ANALYZE {schema}.coordinator_changes"))
    coordinator = PostgresCoordinator(store, catch_up_interval=10)
    coordinator._started = True
    coordinator._observations[root] = _Observation()
    captured = catch_up_capture(coordinator, monkeypatch)
    try:
        await coordinator._catch_up({root}, periodic=False)
        observed = coordinator.observation(root)
        assert (observed.change_id, observed.tree_change_id) == (last_id, header_id)
        params = ([root], [root], [], [], [])
        async with store._connection() as conn:
            body = captured[0].as_string(conn)
            cursor = await conn.execute(
                psycopg.sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + captured[0], params
            )
            analyzed = (await cursor.fetchone())[0][0]["Plan"]
            numbered = iter(range(1, 6))
            generic = re.sub(r"%s", lambda _: f"${next(numbered)}", body)
            cursor = await conn.execute(f"EXPLAIN (GENERIC_PLAN, FORMAT JSON) {generic}")
            planned = (await cursor.fetchone())[0][0]["Plan"]

        def nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
            return [node, *(found for child in node.get("Plans", []) for found in nodes(child))]

        used = [node for node in nodes(analyzed) if node.get("Index Name") == "coordinator_changes_tree_idx"]
        assert len(used) == 1
        assert used[0]["Actual Loops"] == 1 and used[0]["Actual Rows"] <= 1
        assert used[0].get("Rows Removed by Filter", 0) == 0
        assert any(node.get("Index Name") == "coordinator_changes_tree_idx" for node in nodes(planned))
    finally:
        await coordinator.close()
        await store.close()
