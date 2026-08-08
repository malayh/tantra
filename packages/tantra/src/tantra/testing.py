from __future__ import annotations

import asyncio
import sys
import threading
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from tantra.errors import SeqConflict, SessionExists, SessionNotFound
from tantra.events import (
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
    TurnStarted,
    Usage,
)
from tantra.stores.base import Store

StoreFactory = Callable[[], Store]

CONTENDERS = 8
CONTENTION_ROUNDS = 40
SWITCH_INTERVAL = 1e-6
LEASE_TTL = 0.3
EXPIRY_DEADLINE = 10.0


async def store_conformance(store_factory: StoreFactory) -> None:
    """Run the shared Store conformance suite; raises AssertionError on the first violation.

    `store_factory` must return a Store over the same underlying storage on every call — separate
    instances stand in for separate processes. It is called from worker threads, so it must build
    a store without touching a running event loop.

    The lease-contention check races real threads and shortens the interpreter's thread switch
    interval while it runs, so that a backend whose critical section is unguarded actually loses.
    """
    await store_factory().setup()
    await _check_create_and_header(store_factory)
    await _check_create_rejects_a_duplicate(store_factory)
    await _check_append_and_read(store_factory)
    await _check_stale_expect_seq(store_factory)
    await _check_put_header(store_factory)
    await _check_unknown_session(store_factory)
    await _check_list(store_factory)
    await _check_lease(store_factory)
    await _check_lease_expiry(store_factory)
    await _check_lease_contention(store_factory)


def _header(**kwargs: Any) -> SessionHeader:
    kwargs.setdefault("id", uuid.uuid4().hex)
    kwargs.setdefault("agent", "build")
    return SessionHeader(**kwargs)


async def _expect(error: type[Exception], coro: Awaitable[Any], message: str) -> None:
    try:
        await coro
    except error:
        return
    raise AssertionError(message)


def _events() -> list[SessionEvent]:
    return [
        TurnStarted(turn_id="t1", input="fix the p99 panel"),
        SampleStarted(turn_id="t1", sample_id="s1", model="google/gemini-3-pro"),
        ToolCallRequested(sample_id="s1", call_id="c1", name="search_metrics", args={"query": "x" * 8192}),
        ToolCallCompleted(call_id="c1", result={"rows": [1, 2, 3]}, is_error=False),
        SampleCompleted(sample_id="s1", usage=Usage(input_tokens=10, output_tokens=5), finish_reason="stop"),
        TurnCompleted(turn_id="t1", stop_reason="done", output={"ok": True}),
    ]


async def _drain(store: Store, sid: str, *, from_seq: int = 0) -> list[Stamped]:
    return [stamped async for stamped in store.read(sid, from_seq=from_seq)]


async def _check_create_and_header(factory: StoreFactory) -> None:
    store = factory()
    assert await store.header(uuid.uuid4().hex) is None

    header = _header(metadata={"company": 42, "user": 7}, title="p99")
    await store.create(header)

    loaded = await factory().header(header.id)
    assert loaded is not None
    assert loaded.id == header.id
    assert loaded.agent == "build"
    assert loaded.title == "p99"
    assert loaded.metadata == {"company": 42, "user": 7}
    assert loaded.status == "idle"
    assert loaded.depth == 0
    assert loaded.parent_id is None
    assert loaded.last_seq == 0
    assert loaded.lease is None


async def _check_create_rejects_a_duplicate(factory: StoreFactory) -> None:
    store = factory()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text="one")], expect_seq=0)

    await _expect(
        SessionExists,
        factory().create(_header(id=header.id)),
        "create on an existing session id did not raise SessionExists",
    )

    loaded = await factory().header(header.id)
    assert loaded is not None
    assert loaded.last_seq == 1
    assert [s.seq for s in await _drain(factory(), header.id)] == [1]


async def _check_append_and_read(factory: StoreFactory) -> None:
    store = factory()
    header = _header()
    await store.create(header)

    events = _events()
    assert await store.append(header.id, events[:3], expect_seq=0) == 3
    assert await store.append(header.id, events[3:], expect_seq=3) == 6

    stamped = await _drain(factory(), header.id)
    assert [s.seq for s in stamped] == [1, 2, 3, 4, 5, 6]
    assert [s.event for s in stamped] == events

    suffix = await _drain(factory(), header.id, from_seq=4)
    assert [s.seq for s in suffix] == [5, 6]
    assert [s.event for s in suffix] == events[4:]

    assert await _drain(store, header.id, from_seq=6) == []

    loaded = await factory().header(header.id)
    assert loaded is not None
    assert loaded.last_seq == 6

    empty = _header()
    await store.create(empty)
    assert await _drain(store, empty.id) == []
    assert await store.append(empty.id, [], expect_seq=0) == 0


async def _check_stale_expect_seq(factory: StoreFactory) -> None:
    store = factory()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text="one")], expect_seq=0)

    for stale in (0, 5):
        await _expect(
            SeqConflict,
            store.append(header.id, [TextPart(sample_id="s1", text="two")], expect_seq=stale),
            f"append with expect_seq={stale} did not raise SeqConflict",
        )

    stamped = await _drain(factory(), header.id)
    assert [s.seq for s in stamped] == [1]
    loaded = await factory().header(header.id)
    assert loaded is not None
    assert loaded.last_seq == 1


async def _check_put_header(factory: StoreFactory) -> None:
    store = factory()
    header = _header()
    await store.create(header)
    await store.append(header.id, [TextPart(sample_id="s1", text="one")], expect_seq=0)
    assert await store.acquire_lease(header.id, "worker-a", 30) is True

    stale = await store.header(header.id)
    assert stale is not None
    stale.title = "renamed"
    stale.status = "running"
    stale.usage = Usage(input_tokens=11, output_tokens=3)
    stale.pending_ask = "ask-1"
    stale.last_seq = 0
    stale.lease = None
    await store.put_header(stale)

    loaded = await factory().header(header.id)
    assert loaded is not None
    assert loaded.title == "renamed"
    assert loaded.status == "running"
    assert loaded.usage.input_tokens == 11
    assert loaded.pending_ask == "ask-1"
    assert loaded.last_seq == 1
    assert loaded.lease is not None
    assert loaded.lease.holder == "worker-a"

    await store.release_lease(header.id, "worker-a")


async def _check_unknown_session(factory: StoreFactory) -> None:
    store = factory()
    sid = uuid.uuid4().hex

    assert await store.header(sid) is None
    assert await _drain(store, sid) == []
    assert await store.list(parent_id=sid) == []

    await _expect(
        SessionNotFound,
        store.append(sid, [TextPart(sample_id="s1", text="x")], expect_seq=0),
        "append to an unknown session did not raise SessionNotFound",
    )
    await _expect(
        SessionNotFound,
        store.put_header(_header(id=sid)),
        "put_header on an unknown session did not raise SessionNotFound",
    )
    await _expect(
        SessionNotFound,
        store.acquire_lease(sid, "worker-a", 30),
        "acquire_lease on an unknown session did not raise SessionNotFound",
    )


async def _check_list(factory: StoreFactory) -> None:
    store = factory()
    tag = uuid.uuid4().hex
    base = datetime.now(UTC)
    made = []
    for index in range(3):
        header = _header(metadata={"tag": tag, "index": index}, created_at=base - timedelta(seconds=3 - index))
        await store.create(header)
        made.append(header)

    reader = factory()
    newest_first = [made[2].id, made[1].id, made[0].id]
    assert [h.id for h in await reader.list(metadata={"tag": tag})] == newest_first
    assert [h.id for h in await reader.list(metadata={"tag": tag}, limit=2)] == newest_first[:2]
    assert [h.id for h in await reader.list(metadata={"tag": tag}, before=made[2].id)] == newest_first[1:]
    assert [h.id for h in await reader.list(metadata={"tag": tag, "index": 1})] == [made[1].id]
    assert await reader.list(metadata={"tag": tag, "index": 99}) == []

    child = _header(parent_id=made[0].id, depth=1, metadata={"tag": tag})
    await store.create(child)
    assert [h.id for h in await reader.list(parent_id=made[0].id)] == [child.id]
    assert [h.id for h in await reader.list(metadata={"tag": tag}, parent_id=made[0].id)] == [child.id]


async def _check_lease(factory: StoreFactory) -> None:
    worker_a = factory()
    worker_b = factory()
    header = _header()
    await worker_a.create(header)

    assert await worker_a.acquire_lease(header.id, "worker-a", 30) is True
    assert await worker_b.acquire_lease(header.id, "worker-b", 30) is False
    assert await worker_a.acquire_lease(header.id, "worker-a", 30) is True

    loaded = await worker_b.header(header.id)
    assert loaded is not None
    assert loaded.lease is not None
    assert loaded.lease.holder == "worker-a"

    await worker_b.release_lease(header.id, "worker-b")
    assert await worker_b.acquire_lease(header.id, "worker-b", 30) is False

    await worker_a.release_lease(header.id, "worker-a")
    loaded = await worker_b.header(header.id)
    assert loaded is not None
    assert loaded.lease is None
    assert await worker_b.acquire_lease(header.id, "worker-b", 30) is True
    await worker_b.release_lease(header.id, "worker-b")


async def _check_lease_expiry(factory: StoreFactory) -> None:
    worker_a = factory()
    worker_b = factory()
    header = _header()
    await worker_a.create(header)

    assert await worker_a.acquire_lease(header.id, "worker-a", LEASE_TTL) is True
    assert await worker_b.acquire_lease(header.id, "worker-b", 30) is False

    deadline = asyncio.get_running_loop().time() + EXPIRY_DEADLINE
    while not await worker_b.acquire_lease(header.id, "worker-b", 30):
        assert asyncio.get_running_loop().time() < deadline, "lease outlived its ttl"
        await asyncio.sleep(0.02)

    assert await worker_a.acquire_lease(header.id, "worker-a", 30) is False
    await worker_b.release_lease(header.id, "worker-b")


def _acquire_in_thread(factory: StoreFactory, sid: str, holder: str, barrier: threading.Barrier) -> bool:
    async def attempt() -> bool:
        store = factory()
        barrier.wait(timeout=30)
        return await store.acquire_lease(sid, holder, 30)

    return asyncio.run(attempt())


async def _check_lease_contention(factory: StoreFactory) -> None:
    store = factory()
    switch_interval = sys.getswitchinterval()
    sys.setswitchinterval(SWITCH_INTERVAL)
    try:
        for _ in range(CONTENTION_ROUNDS):
            header = _header()
            await store.create(header)
            barrier = threading.Barrier(CONTENDERS)

            results = await asyncio.gather(
                *(
                    asyncio.to_thread(_acquire_in_thread, factory, header.id, f"worker-{index}", barrier)
                    for index in range(CONTENDERS)
                )
            )
            assert results.count(True) == 1, f"{results.count(True)} holders acquired one lease at once"

            loaded = await factory().header(header.id)
            assert loaded is not None
            assert loaded.lease is not None
            assert loaded.lease.holder == f"worker-{results.index(True)}"
    finally:
        sys.setswitchinterval(switch_interval)
