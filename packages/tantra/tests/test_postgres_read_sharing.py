from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest

from tantra import Agent, PostgresCoordinator, PostgresStore, Runtime
from tantra.errors import CoordinatorUnavailable, SessionNotFound
from tantra.events import TextDelta
from tantra.providers.fake import FakeProvider


class Bot(Agent):
    pass


async def eventually(predicate: Any) -> None:
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.005)


def make_runtime(store: PostgresStore) -> Runtime:
    return Runtime(
        FakeProvider([]),
        store,
        [Bot],
        default_model="m",
        coordinator=PostgresCoordinator(store, catch_up_interval=0.03),
    )


async def test_eight_remote_streams_share_committed_rows_and_keep_independent_cursors(
    postgres_dsn: str, pg_schema: str
) -> None:
    first = PostgresStore(postgres_dsn, schema=pg_schema)
    second = PostgresStore(postgres_dsn, schema=pg_schema)
    await first.setup()
    await second.setup()
    owner, remote = make_runtime(first), make_runtime(second)
    await owner.start()
    await remote.start()
    root = await owner.create(Bot)
    streams = [remote.events(root, after=1) for _ in range(8)]
    pending = [asyncio.create_task(anext(stream)) for stream in streams]
    calls: list[tuple[int, int]] = []
    original = remote._pages.read

    async def counted(sid: str, *, after: int, limit: int = 1000) -> list[Any]:
        calls.append((after, limit))
        return await original(sid, after=after, limit=limit)

    remote._pages.read = counted
    try:
        await eventually(
            lambda: (
                remote._stream_interests.get(root.hex, {}).get(root.hex) == 8
                and root.hex in remote._observations
                and remote._observations[root.hex].actor_coverage == frozenset({root.hex})
                and remote._pages.readers[root.hex].flight is None
            )
        )
        calls.clear()
        ownership = await owner.coordinator.acquire(root.hex)
        assert ownership is not None
        async with owner.coordinator.transaction(ownership) as guarded:
            await guarded.append(
                root.hex, [TextDelta(text=str(index), payload={"nested": [index]}) for index in range(32)]
            )
        await owner.coordinator.release(ownership)
        received = await asyncio.wait_for(asyncio.gather(*pending), 5)
        assert calls == [(1, 256)]
        assert all(item.seq == 2 and item.event.payload == {"nested": [0]} for item in received)
        received[0].event.payload["nested"].append(99)
        assert all(item.event.payload == {"nested": [0]} for item in received[1:])
        expected = await first.read_page(root.hex, after=1)
        tails = []
        for stream in streams:
            tails.append([await anext(stream) for _ in range(31)])
        assert all([item.seq for item in tail] == list(range(3, 34)) for tail in tails)
        assert all([item.event for item in tail] == [item.event for item in expected[1:]] for tail in tails)
        assert calls == [(1, 256)]
        reconnected = remote.events(root, after=2)
        try:
            assert (await anext(reconnected)).event == expected[1].event
        finally:
            await reconnected.aclose()
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for stream in streams:
            await stream.aclose()
        assert not remote._pages.recent and not remote._pages.readers
        assert not remote._stream_interests and not remote._watchers
        assert not remote.coordinator._actor_interests and not remote.coordinator._observations
        await remote.aclose()
        await owner.aclose()
        await second.close()
        await first.close()


@pytest.mark.parametrize("fault", ["rollback", "lost_ack"])
async def test_append_fault_never_publishes_unconfirmed_rows(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    runtime = make_runtime(store)
    await runtime.start()
    root = await runtime.create(Bot)
    ownership = await runtime.coordinator.acquire(root.hex)
    assert ownership is not None
    runtime._ownerships[root.hex] = ownership
    runtime._pages.retain(root.hex)
    original = runtime.coordinator.transaction

    @asynccontextmanager
    async def failed(*args: Any):
        async with original(*args) as guarded:
            yield guarded
            if fault == "rollback":
                raise CoordinatorUnavailable("rollback")
        raise CoordinatorUnavailable("lost acknowledgment")

    try:
        monkeypatch.setattr(runtime.coordinator, "transaction", failed)
        with pytest.raises(CoordinatorUnavailable):
            await runtime._append(root.hex, [TextDelta(text=fault)])
        assert not runtime._pages.recent and runtime._pages.high_water(root.hex) == 0
        journal = await store.read_page(root.hex)
        assert len(journal) == (1 if fault == "rollback" else 2)
        if fault == "lost_ack":
            assert journal[-1].event.text == "lost_ack"
            recovered = await runtime._pages.page(root.hex, after=1)
            assert recovered == journal[1:]
            assert runtime._pages.high_water(root.hex) == 2
        monkeypatch.setattr(runtime.coordinator, "transaction", original)
    finally:
        monkeypatch.setattr(runtime.coordinator, "transaction", original)
        await runtime._pages.release(root.hex)
        await runtime.aclose()
        await store.close()


async def test_confirmed_append_cache_matches_the_durable_codec_and_deletion_purges_it(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    runtime = make_runtime(store)
    await runtime.start()
    root = await runtime.create(Bot)
    ownership = await runtime.coordinator.acquire(root.hex)
    assert ownership is not None
    runtime._ownerships[root.hex] = ownership
    runtime._pages.retain(root.hex)
    event = TextDelta(text="\x00雪", payload={"tuple": (1, 2), "id": uuid4()})
    try:
        await runtime._append(root.hex, [event])
        event.payload["tuple"] = (99,)
        stored = await store.read_page(root.hex, after=1)
        assert await runtime._pages.page(root.hex, after=1) == stored
        assert stored[0].event.payload["tuple"] == [1, 2]
        assert await runtime.delete(root) is True
        assert not runtime._pages.recent
        with pytest.raises(SessionNotFound, match=root.hex):
            runtime._pages.check(root.hex)
    finally:
        await runtime._pages.release(root.hex)
        await runtime.aclose()
        await store.close()


async def test_historical_stream_keeps_thousand_row_pages(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    runtime = make_runtime(store)
    await runtime.start()
    root = await runtime.create(Bot)
    ownership = await runtime.coordinator.acquire(root.hex)
    assert ownership is not None
    async with runtime.coordinator.transaction(ownership) as guarded:
        await guarded.append(root.hex, [TextDelta(text=str(index)) for index in range(4000)])
    await runtime.coordinator.release(ownership)
    calls: list[int] = []
    original = runtime._pages.read

    async def counted(sid: str, *, after: int, limit: int = 1000) -> list[Any]:
        calls.append(limit)
        return await original(sid, after=after, limit=limit)

    runtime._pages.read = counted
    stream = runtime.events(root)
    try:
        received = [await anext(stream) for _ in range(1001)]
        assert [item.seq for item in received] == list(range(1, 1002))
        assert calls == [1000, 1000]
        assert not runtime._pages.recent
    finally:
        await stream.aclose()
        await runtime.aclose()
        await store.close()


async def test_connection_only_deletion_releases_its_own_watcher_registration(
    postgres_dsn: str, pg_schema: str
) -> None:
    first = PostgresStore(postgres_dsn, schema=pg_schema)
    second = PostgresStore(postgres_dsn, schema=pg_schema)
    await first.setup()
    await second.setup()
    owner, remote = make_runtime(first), make_runtime(second)
    await owner.start()
    await remote.start()
    root = await owner.create(Bot)
    try:
        async with remote.connect(root):
            await eventually(lambda: root.hex in remote._observations)
            assert remote.coordinator._actor_interests[root.hex] == frozenset()
            assert await owner.delete(root)
            await eventually(lambda: not remote._connections and not remote._watchers)
            await eventually(lambda: not remote.coordinator._observations)
            assert not remote.coordinator._actor_interests
    finally:
        await remote.aclose()
        await owner.aclose()
        await second.close()
        await first.close()


async def test_closing_runtime_terminates_a_paused_cached_stream(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    runtime = make_runtime(store)
    await runtime.start()
    root = await runtime.create(Bot)
    stream = runtime.events(root)
    try:
        assert (await anext(stream)).seq == 1
        assert runtime._pages.recent
        await runtime.aclose()
        with pytest.raises(StopAsyncIteration):
            await anext(stream)
        assert not runtime._pages.readers and not runtime._pages.recent
    finally:
        await stream.aclose()
        await runtime.aclose()
        await store.close()
