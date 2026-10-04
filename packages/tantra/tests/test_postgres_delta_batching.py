from __future__ import annotations

import asyncio
import multiprocessing
from collections.abc import AsyncIterator, Sequence
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import Agent, PostgresCoordinator, PostgresStore, Runtime
from tantra.coordinator import CoordinatedStore
from tantra.errors import CoordinatorUnavailable, LeaseLost
from tantra.events import SessionEvent, TextDelta, TurnCancelled, TurnInterrupted
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd
from tantra.providers.fake import FAKE_LIMITS
from tantra.stores.postgres import _parse

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")


class Bot(Agent):
    pass


class BurstProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.calls += 1
        try:
            for index in range(64):
                yield TextDelta(text=f"fragment-{index:03}")
            yield StreamEnd(text="done")
        finally:
            self.closed.set()


async def runtime_for(dsn: str, schema: str) -> tuple[Runtime, BurstProvider]:
    store = PostgresStore(dsn, schema=schema)
    provider = BurstProvider()
    runtime = Runtime(
        provider,
        store,
        [Bot],
        default_model="fake",
        coordinator=PostgresCoordinator(store, lease_ttl=1, catch_up_interval=0.02, request_timeout=3),
    )
    await runtime.start()
    return runtime, provider


async def evidence(dsn: str, schema: str, root: UUID) -> list[Any]:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT seq, stamped FROM {}.events WHERE session_id=%s ORDER BY seq").format(
                sql.Identifier(schema)
            ),
            (root.hex,),
        )
        rows = await cursor.fetchall()
    items = [_parse(root.hex, raw) for _, raw in rows]
    assert [item.seq for item in items] == list(range(1, len(items) + 1))
    assert all(seq == item.seq for (seq, _), item in zip(rows, items, strict=True))
    return items


def assert_prefix(items: list[Any], count: int) -> None:
    assert [item.event for item in items if isinstance(item.event, TextDelta)] == [
        TextDelta(text=f"fragment-{index:03}") for index in range(count)
    ]


@pytest.mark.parametrize("action", ["cancel", "shutdown", "delete", "takeover"])
async def test_control_at_second_flush_preserves_prefix_and_closes_provider(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    owner, provider = await runtime_for(postgres_dsn, pg_schema)
    remote, _ = await runtime_for(postgres_dsn, pg_schema)
    root = await owner.create(Bot)
    reached = asyncio.Event()
    release = asyncio.Event()
    append = owner._engine_append
    flushes = 0

    async def gated(actor: str, root_id: str, generation: int, events: Sequence[SessionEvent]) -> Any:
        nonlocal flushes
        if events and isinstance(events[0], TextDelta):
            flushes += 1
            if flushes == 2:
                reached.set()
                await release.wait()
        return await append(actor, root_id, generation, events)

    monkeypatch.setattr(owner, "_engine_append", gated)
    try:
        async with owner.connect(root, writable=True) as connection:
            command = uuid4()
            await connection.send("go", command_id=command)
            await asyncio.wait_for(reached.wait(), 3)
            assert_prefix(await evidence(postgres_dsn, pg_schema, root), 32)
            if action == "shutdown":
                await owner.aclose()
            elif action == "delete":
                assert await remote.delete(root, allow_active=True)
            elif action == "takeover":
                stale = owner._ownerships[root.hex]
                renewal = owner._renewals[root.hex]
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
                async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {}.coordinator_roots SET expires_at=clock_timestamp()-interval '1 second' "
                            "WHERE root_id=%s"
                        ).format(sql.Identifier(pg_schema)),
                        (root.hex,),
                    )
                async with remote.connect(root, writable=True) as replacement:
                    assert (await replacement.send("go", command_id=command)).duplicate
                    assert (await replacement.prompt("go", command_id=command)).outcome == "interrupted"
                with pytest.raises(LeaseLost):
                    async with owner.coordinator.transaction(stale):
                        pass
                release.set()
            else:
                async with remote.connect(root, writable=True) as replacement:
                    await replacement.cancel(command_id=uuid4())
            await asyncio.wait_for(provider.closed.wait(), 3)
            release.set()
            if action == "delete":
                assert await evidence(postgres_dsn, pg_schema, root) == []
                assert await owner.store.header(root.hex) is None
            else:
                items = await evidence(postgres_dsn, pg_schema, root)
                assert_prefix(items, 32)
                terminals = [item.event for item in items if isinstance(item.event, TurnCancelled | TurnInterrupted)]
                assert len(terminals) == 1 and terminals[0].turn_id == command.hex
            assert provider.calls == 1
    finally:
        release.set()
        await owner.aclose()
        await remote.aclose()
        await owner.store.close()
        await remote.store.close()


@pytest.mark.parametrize("committed", [False, True])
async def test_flush_rollback_or_lost_commit_ack_is_never_blindly_retried(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch, committed: bool
) -> None:
    owner, provider = await runtime_for(postgres_dsn, pg_schema)
    root = await owner.create(Bot)
    command = uuid4()
    armed = True
    if committed:
        original = owner._append

        async def lost_ack(actor: str, events: Sequence[SessionEvent], store: Any = None) -> Any:
            nonlocal armed
            result = await original(actor, events, store)
            if armed and events and isinstance(events[0], TextDelta):
                armed = False
                raise CoordinatorUnavailable("committed acknowledgment lost")
            return result

        monkeypatch.setattr(owner, "_append", lost_ack)
    else:
        original = CoordinatedStore.append

        async def rollback(store: Any, actor: str, events: Sequence[SessionEvent]) -> Any:
            nonlocal armed
            result = await original(store, actor, events)
            if armed and events and isinstance(events[0], TextDelta):
                armed = False
                assert_prefix(await evidence(postgres_dsn, pg_schema, root), 0)
                raise CoordinatorUnavailable("rollback before commit")
            return result

        monkeypatch.setattr(CoordinatedStore, "append", rollback)
    replacement = None
    try:
        async with owner.connect(root, writable=True) as connection:
            with pytest.raises(CoordinatorUnavailable):
                await asyncio.wait_for(connection.prompt("go", command_id=command), 5)
        assert not armed and provider.calls == 1
        assert provider.closed.is_set()
        items = await evidence(postgres_dsn, pg_schema, root)
        assert_prefix(items, 32 if committed else 0)
        await owner.aclose()
        replacement, replacement_provider = await runtime_for(postgres_dsn, pg_schema)
        async with replacement.connect(root, writable=True) as connection:
            assert (await connection.send("go", command_id=command)).duplicate
            assert (await connection.prompt("go", command_id=command)).outcome == "interrupted"
        assert replacement_provider.calls == 0
        assert_prefix(await evidence(postgres_dsn, pg_schema, root), 32 if committed else 0)
    finally:
        await owner.aclose()
        await owner.store.close()
        if replacement is not None:
            await replacement.aclose()
            await replacement.store.close()


async def crash_owner(dsn: str, schema: str, root: str, command: str, stage: str, pipe: Any) -> None:
    runtime, _ = await runtime_for(dsn, schema)
    flushes = 0
    if stage == "during":
        original = CoordinatedStore.append

        async def gate(store: Any, actor: str, events: Sequence[SessionEvent]) -> Any:
            nonlocal flushes
            result = await original(store, actor, events)
            if events and isinstance(events[0], TextDelta):
                flushes += 1
                if flushes == 2:
                    pipe.send("during")
                    await asyncio.Event().wait()
            return result

        CoordinatedStore.append = gate
    else:
        original = runtime._engine_append

        async def gate(actor: str, root_id: str, generation: int, events: Sequence[SessionEvent]) -> Any:
            nonlocal flushes
            delta = bool(events and isinstance(events[0], TextDelta))
            if delta:
                flushes += 1
            if delta and flushes == 2 and stage == "before":
                pipe.send("before")
                await asyncio.Event().wait()
            result = await original(actor, root_id, generation, events)
            if delta and flushes == 2 and stage == "after":
                pipe.send("after")
                await asyncio.Event().wait()
            return result

        runtime._engine_append = gate
    try:
        async with runtime.connect(UUID(hex=root), writable=True) as connection:
            await connection.send("go", command_id=UUID(hex=command))
            await asyncio.Event().wait()
    except BaseException as exc:
        pipe.send(repr(exc))
        raise


def crash_worker(*args: Any) -> None:
    asyncio.run(crash_owner(*args))


@pytest.mark.parametrize("stage", ["before", "during", "after"])
async def test_process_death_at_flush_boundary_recovers_exact_committed_prefix(
    postgres_dsn: str, pg_schema: str, stage: str
) -> None:
    runtime, _ = await runtime_for(postgres_dsn, pg_schema)
    root = await runtime.create(Bot)
    command = uuid4()
    await runtime.aclose()
    await runtime.store.close()
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=crash_worker, args=(postgres_dsn, pg_schema, root.hex, command.hex, stage, child))
    replacement = None
    process.start()
    try:
        assert await asyncio.to_thread(parent.poll, 10)
        assert parent.recv() == stage
        count = 64 if stage == "after" else 32
        assert_prefix(await evidence(postgres_dsn, pg_schema, root), count)
        process.kill()
        await asyncio.to_thread(process.join, 5)
        assert not process.is_alive()
        replacement, provider = await runtime_for(postgres_dsn, pg_schema)
        async with asyncio.timeout(3):
            while await replacement.coordinator.locate(root.hex) is not None:
                await asyncio.sleep(0.02)
        async with replacement.connect(root, writable=True) as connection:
            assert (await connection.send("go", command_id=command)).duplicate
            assert (await asyncio.wait_for(connection.prompt("go", command_id=command), 5)).outcome == "interrupted"
        assert provider.calls == 0
        items = await evidence(postgres_dsn, pg_schema, root)
        assert_prefix(items, count)
        assert sum(isinstance(item.event, TurnInterrupted) for item in items) == 1
    finally:
        if process.is_alive():
            process.kill()
        await asyncio.to_thread(process.join, 5)
        parent.close()
        child.close()
        if replacement is not None:
            await replacement.aclose()
            await replacement.store.close()


@pytest.mark.parametrize("kind", ["standalone", "custom_store", "custom_coordinator"])
async def test_runtime_batching_eligibility(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    class CustomStore(PostgresStore):
        pass

    class CustomCoordinator(PostgresCoordinator):
        pass

    store_type = CustomStore if kind == "custom_store" else PostgresStore
    store = store_type(postgres_dsn, schema=pg_schema)
    await store.setup()
    coordinator_type = CustomCoordinator if kind == "custom_coordinator" else PostgresCoordinator
    coordinator = None if kind == "standalone" else coordinator_type(store, catch_up_interval=0.02)
    runtime = Runtime(BurstProvider(), store, [Bot], default_model="fake", coordinator=coordinator)
    batches = []
    target = PostgresStore if kind == "standalone" else CoordinatedStore
    append = target.append

    async def record(source: Any, sid: str, events: Sequence[SessionEvent]) -> int:
        result = await append(source, sid, events)
        if events and isinstance(events[0], TextDelta):
            batches.append(len(events))
        return result

    monkeypatch.setattr(target, "append", record)
    await runtime.start()
    try:
        root = await runtime.create(Bot)
        async with runtime.connect(root, writable=True) as connection:
            assert (await connection.prompt("go", command_id=uuid4())).text == "done"
        assert batches == ([32, 32] if kind == "standalone" else [1] * 64)
    finally:
        await runtime.aclose()
        await store.close()
