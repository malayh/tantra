from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from tantra import Agent, Runtime, SessionNotFound, TantraError, tool
from tantra.errors import SessionExists
from tantra.events import AskRaised, SessionHeader, Usage
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall, UserMessage
from tantra.providers.fake import FAKE_LIMITS, FakeProvider, Sample
from tantra.stores.fs import FileSystemStore
from tantra.stores.memory import MemoryStore
from tantra.stores.sqlite import SQLiteStore


class Bot(Agent):
    pass


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        yield StreamEnd(text=value, usage=Usage(input_tokens=1, output_tokens=1))


class GateProvider(EchoProvider):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        yield StreamEnd(text="late")


class ObservedReadStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.waiting = asyncio.Event()

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Any]:
        page = await super().read_page(sid, after=after, limit=limit)
        if self.armed and not page:
            self.waiting.set()
        return page


@pytest.fixture(params=("memory", "sqlite"))
async def runtime_store(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> AsyncIterator[tuple[Runtime, MemoryStore | SQLiteStore]]:
    store: MemoryStore | SQLiteStore
    if request.param == "memory":
        store = MemoryStore()
    else:
        store = SQLiteStore(tmp_path / "tantra.db")
    await store.setup()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    try:
        yield runtime, store
    finally:
        await runtime.aclose()


async def test_idle_unknown_root_child_markers_and_unrelated_sessions(
    runtime_store: tuple[Runtime, MemoryStore | SQLiteStore],
) -> None:
    runtime, store = runtime_store
    root = await runtime.create(Bot)
    other = await runtime.create(Bot)
    child = uuid4()
    await store.create(SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="bot", model="m"))

    assert await runtime.delete(uuid4()) is False
    with pytest.raises(TantraError, match="not a root"):
        await runtime.delete(child)
    assert await runtime.delete(root) is True
    assert await runtime.delete(root) is False
    with pytest.raises(SessionExists):
        await runtime.create(Bot, session_id=root)

    async with runtime.connect(other, writable=True) as connection:
        assert (await connection.prompt("other", command_id=uuid4())).text == "other"
    fresh = await runtime.create(Bot)
    async with runtime.connect(fresh, writable=True) as connection:
        assert (await connection.prompt("fresh", command_id=uuid4())).text == "fresh"


async def test_callback_failure_rolls_back_and_tree_remains_usable(
    runtime_store: tuple[Runtime, MemoryStore | SQLiteStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, store = runtime_store
    root = await runtime.create(Bot)
    original = runtime._begin_deletion

    def fail(*_args: Any) -> list[asyncio.Task[None]]:
        raise RuntimeError("rollback")

    monkeypatch.setattr(runtime, "_begin_deletion", fail)
    with pytest.raises(RuntimeError, match="rollback"):
        await runtime.delete(root)
    assert await store.header(root.hex) is not None

    monkeypatch.setattr(runtime, "_begin_deletion", original)
    async with runtime.connect(root, writable=True) as connection:
        assert (await connection.prompt("still works", command_id=uuid4())).text == "still works"
    assert await runtime.delete(root) is True


async def test_forced_delete_cancels_generation_and_fails_pending_result() -> None:
    provider = GateProvider()
    runtime = Runtime(provider, MemoryStore(), [Bot], default_model="m")
    root = await runtime.create(Bot)
    try:
        async with runtime.connect(root, writable=True) as connection:
            prompt = asyncio.create_task(connection.prompt("blocked", command_id=uuid4()))
            await asyncio.wait_for(provider.started.wait(), 1)
            assert await runtime.delete(root, allow_active=True) is True
            await asyncio.wait_for(provider.cancelled.wait(), 1)
            with pytest.raises(SessionNotFound):
                await asyncio.wait_for(prompt, 1)
    finally:
        provider.release.set()
        await runtime.aclose()


async def test_blocked_event_stream_fails_when_session_is_deleted() -> None:
    store = ObservedReadStore()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    root = await runtime.create(Bot)
    header = await store.header(root.hex)
    assert header is not None
    stream = runtime.events(root, after=header.last_seq)
    store.armed = True
    blocked = asyncio.create_task(anext(stream))
    try:
        await asyncio.wait_for(store.waiting.wait(), 1)
        assert await runtime.delete(root) is True
        with pytest.raises(SessionNotFound):
            await asyncio.wait_for(blocked, 1)
    finally:
        await stream.aclose()
        await runtime.aclose()


async def test_pending_approval_is_deleted_without_executing_tool() -> None:
    invoked = []

    @tool(permission="ask")
    async def dangerous() -> str:
        invoked.append("called")
        return "done"

    class Guarded(Agent):
        tools = [dangerous]

    provider = FakeProvider(
        [Sample(tool_calls=[ToolCall(id="danger", name="dangerous", args="{}")]), Sample(text="done")]
    )
    runtime = Runtime(provider, MemoryStore(), [Guarded], default_model="m")
    root = await runtime.create(Guarded)
    try:
        async with runtime.connect(root, writable=True) as connection:
            await connection.send("go", command_id=uuid4())
            while not isinstance((await anext(connection)).event, AskRaised):
                pass
            assert await runtime.delete(root, allow_active=True) is True
        assert invoked == []
    finally:
        await runtime.aclose()


async def test_open_readonly_and_writable_connections_fail_after_delete() -> None:
    runtime = Runtime(EchoProvider(), MemoryStore(), [Bot], default_model="m")
    root = await runtime.create(Bot)
    reader = runtime.connect(root)
    writer = runtime.connect(root, writable=True)
    await reader.__aenter__()
    await writer.__aenter__()
    try:
        assert await runtime.delete(root) is True
        with pytest.raises(SessionNotFound):
            await anext(reader)
        with pytest.raises(SessionNotFound):
            await writer.send("late", command_id=uuid4())
    finally:
        await writer.__aexit__(None, None, None)
        await reader.__aexit__(None, None, None)
        await runtime.aclose()


async def test_filesystem_store_rejects_delete_before_mutation(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path / "sessions")
    await store.setup()
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m")
    root = await runtime.create(Bot)
    try:
        with pytest.raises(NotImplementedError, match="does not support"):
            await runtime.delete(root)
        assert await store.header(root.hex) is not None
        async with runtime.connect(root, writable=True) as connection:
            assert (await connection.prompt("kept", command_id=uuid4())).text == "kept"
    finally:
        await runtime.aclose()


async def test_readonly_enter_racing_delete_rejects_without_retaining_connection(
    runtime_store: tuple[Runtime, MemoryStore | SQLiteStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, _ = runtime_store
    root = await runtime.create(Bot)
    original = runtime._root_header
    ready, release = asyncio.Event(), asyncio.Event()
    first = True

    async def gated_header(sid: str) -> SessionHeader:
        nonlocal first
        header = await original(sid)
        if first:
            first = False
            ready.set()
            await release.wait()
        return header

    monkeypatch.setattr(runtime, "_root_header", gated_header)
    connection = runtime.connect(root)
    enter = asyncio.create_task(connection.__aenter__())
    try:
        await asyncio.wait_for(ready.wait(), 1)
        assert await runtime.delete(root)
        release.set()
        with pytest.raises(SessionNotFound):
            await asyncio.wait_for(enter, 1)
        assert not connection._entered
        assert not runtime._connections
        assert not runtime._connection_interests
    finally:
        release.set()
        enter.cancel()
        await asyncio.gather(enter, return_exceptions=True)
        await connection.__aexit__(None, None, None)
