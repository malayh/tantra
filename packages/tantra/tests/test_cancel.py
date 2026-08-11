from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import Approval, ApprovalResponse
from tantra.errors import SessionNotFound
from tantra.events import (
    AskRaised,
    CancelRequested,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextPart,
    ToolCallCompleted,
    TurnCompleted,
)
from tantra.harness import Harness
from tantra.hooks import Hook
from tantra.loop import Emitted
from tantra.providers.base import ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool

POLL_ROUNDS = 500
POLL_INTERVAL = 0.01


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


class RecordingStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.cancels: list[str] = []

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        if any(isinstance(event, CancelRequested) for event in events):
            self.cancels.append(sid)
        return await super().append(sid, events, expect_seq=expect_seq)


class RacingStore:
    """Grows the log after every read, so any read-then-append with an expect_seq loses the race."""

    def __init__(self, inner: MemoryStore) -> None:
        self.inner = inner
        self.injected = 0

    async def header(self, sid: str) -> SessionHeader | None:
        return await self.inner.header(sid)

    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        async for stamped in self.inner.read(sid, from_seq=from_seq):
            yield stamped
        self.injected += 1
        await self.inner.append(sid, [TextPart(sample_id="racer", text="tick")], expect_seq=None)

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        return await self.inner.append(sid, events, expect_seq=expect_seq)


async def history(store: MemoryStore, sid: str) -> list[SessionEvent]:
    return [stamped.event async for stamped in store.read(sid)]


async def until_sampling(store: MemoryStore, sid: str) -> None:
    for _ in range(POLL_ROUNDS):
        if any(isinstance(event, SampleStarted) for event in await history(store, sid)):
            return
        await asyncio.sleep(POLL_INTERVAL)
    raise AssertionError(f"session {sid} never started a sample")


async def until_spawned(store: MemoryStore, sid: str) -> str:
    for _ in range(POLL_ROUNDS):
        children = await store.list(parent_id=sid)
        if children:
            return children[0].id
        await asyncio.sleep(POLL_INTERVAL)
    raise AssertionError(f"session {sid} never spawned a child")


async def test_cancel_from_a_second_harness_stops_the_turn_at_the_next_boundary() -> None:
    executed: list[str] = []
    watcher: dict[str, Harness] = {}

    @tool
    async def touch(name: str, ctx: Context) -> str:
        """Records a call while another harness cancels the session."""
        executed.append(name)
        assert await watcher["other"].cancel(ctx.session_id)
        return f"did {name}"

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(
                    tool_calls=[
                        call("touch", '{"name": "a"}', cid="c1"),
                        call("touch", '{"name": "b"}', cid="c2"),
                    ]
                ),
                Sample(text="never reached"),
            ]
        ),
        store,
        [Worker],
        default_model="fake/model",
    )
    watcher["other"] = Harness(FakeProvider([]), store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id

    events = await collect(harness.run(sid, "go"))

    assert executed == ["a"]
    first, second = picks(events, ToolCallCompleted)
    assert first.call_id == "c1"
    assert first.result == "did a"
    assert second.call_id == "c2"
    assert second.is_error
    assert second.result == "not executed: turn cancelled"
    assert picks(events, TurnCompleted)[0].stop_reason == "cancelled"
    assert len(picks(events, SampleStarted)) == 1
    assert len(harness.provider.requests) == 1

    logged = [stamped.event async for stamped in store.read(sid)]
    assert len([event for event in logged if isinstance(event, CancelRequested)]) == 1
    assert (await store.header(sid)).status == "idle"


async def test_cancel_between_samples_ends_the_turn_before_the_next_call() -> None:
    watcher: dict[str, Harness] = {}

    @tool
    async def touch(ctx: Context) -> str:
        """Cancels the session from another harness."""
        await watcher["other"].cancel(ctx.session_id)
        return "done"

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("touch", "{}")]), Sample(text="never reached")]),
        store,
        [Worker],
        default_model="fake/model",
    )
    watcher["other"] = Harness(FakeProvider([]), store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id

    events = await collect(harness.run(sid, "go"))

    assert picks(events, ToolCallCompleted)[0].result == "done"
    assert len(picks(events, SampleStarted)) == 1
    assert picks(events, TurnCompleted)[0].stop_reason == "cancelled"


async def test_a_cancel_landing_after_the_batch_stops_the_turn_at_the_sample_boundary() -> None:
    watcher: dict[str, Harness] = {}

    @tool
    async def touch(ctx: Context) -> str:
        """Does its work uninterrupted."""
        return "done"

    class Worker(Agent):
        tools = [touch]

    class Canceller(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            if isinstance(emitted.event, ToolCallCompleted):
                assert await watcher["other"].cancel(emitted.session_id)

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("touch", "{}")]), Sample(text="never reached")]),
        store,
        [Worker],
        default_model="fake/model",
        hooks=[Canceller()],
    )
    watcher["other"] = Harness(FakeProvider([]), store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id

    events = await collect(harness.run(sid, "go"))

    completed = picks(events, ToolCallCompleted)[0]
    assert completed.result == "done"
    assert not completed.is_error
    assert len(picks(events, SampleStarted)) == 1
    assert len(harness.provider.requests) == 1
    assert picks(events, TurnCompleted)[0].stop_reason == "cancelled"


async def test_cancelling_a_suspended_turn_completes_it_on_resume_without_sampling() -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        """Asks for approval."""
        reply = await ctx.ask(Approval(title="proceed?"))
        return f"allowed={reply.allow}"

    class Asker(Agent):
        tools = [confirm]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("confirm", "{}")])]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))
    ask_id = picks(opening, AskRaised)[0].ask_id
    assert (await store.header(sid)).pending_ask == ask_id

    assert await harness.cancel(sid) is True

    fresh = Harness(FakeProvider([]), store, [Asker], default_model="fake/model")
    resumed = await collect(fresh.resume(sid))

    completed = picks(resumed, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result == "not executed: turn cancelled"
    assert picks(resumed, TurnCompleted)[0].stop_reason == "cancelled"
    header = await store.header(sid)
    assert header.status == "idle"
    assert header.pending_ask is None


async def test_answering_a_cancelled_ask_still_ends_the_turn_cancelled() -> None:
    executed: list[str] = []

    @tool
    async def confirm(ctx: Context) -> str:
        """Asks for approval."""
        reply = await ctx.ask(Approval(title="proceed?"))
        executed.append("ran")
        return f"allowed={reply.allow}"

    class Asker(Agent):
        tools = [confirm]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("confirm", "{}")])]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))
    ask_id = picks(opening, AskRaised)[0].ask_id
    await harness.cancel(sid)

    resumed = await collect(harness.resume(sid, ask_id, ApprovalResponse(allow=True)))

    assert executed == []
    assert picks(resumed, TurnCompleted)[0].stop_reason == "cancelled"


async def test_cancel_returns_false_when_no_turn_is_in_flight() -> None:
    class Bot(Agent): ...

    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model="fake/model")
    sid = (await harness.create_session(Bot)).id

    assert await harness.cancel(sid) is False

    await collect(harness.run(sid, "go"))

    assert await harness.cancel(sid) is False
    logged = [stamped.event async for stamped in store.read(sid)]
    assert not [event for event in logged if isinstance(event, CancelRequested)]


async def test_cancel_of_an_unknown_session_raises() -> None:
    class Bot(Agent): ...

    harness = Harness(FakeProvider([]), MemoryStore(), [Bot], default_model="fake/model")

    with pytest.raises(SessionNotFound):
        await harness.cancel("missing")


@tool
async def look(q: str) -> str:
    """Look something up."""
    return f"found {q}"


class Worker(Agent):
    tools = [look]


class Boss(Agent):
    subagents = [Worker]


async def test_cancel_lands_in_one_attempt_against_an_actively_appending_log() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(text="thinking", tool_calls=[call("look", '{"q": "a"}')])]),
        store,
        [Worker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Worker)).id

    stream = harness.run(sid, "go")
    async for event in stream:
        if isinstance(event.event, SampleStarted):
            break
    await stream.aclose()

    racing = RacingStore(store)
    watcher = Harness(FakeProvider([]), racing, [Worker], default_model="fake/model")

    assert await watcher.cancel(sid) is True

    assert racing.injected == 1
    logged = await history(store, sid)
    assert len([event for event in logged if isinstance(event, CancelRequested)]) == 1
    assert isinstance(logged[-1], CancelRequested)
    assert isinstance(logged[-2], TextPart)


async def test_cancel_mid_final_text_only_sample_ends_the_turn_cancelled(gated_provider: Any) -> None:
    store = MemoryStore()
    provider = gated_provider([Sample(text="the final answer")])
    harness = Harness(provider, store, [Worker], default_model="fake/model")
    watcher = Harness(FakeProvider([]), store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id

    provider.gate.clear()
    turn = asyncio.ensure_future(collect(harness.run(sid, "go")))
    await until_sampling(store, sid)

    assert await watcher.cancel(sid) is True

    provider.gate.set()
    events = await turn

    assert picks(events, TurnCompleted)[0].stop_reason == "cancelled"
    assert (await store.header(sid)).status == "idle"


async def test_recursive_cancel_flags_children_deepest_first_then_the_target(gated_provider: Any) -> None:
    store = RecordingStore()
    provider = gated_provider(
        [
            Sample(tool_calls=[call("worker", '{"task": "dig"}', cid="p1")]),
            Sample(text="child thinking"),
            Sample(text="never reached"),
        ]
    )
    harness = Harness(provider, store, [Boss], default_model="fake/model")
    watcher = Harness(FakeProvider([]), store, [Boss], default_model="fake/model")
    sid = (await harness.create_session(Boss)).id

    provider.gate.clear()
    turn = asyncio.ensure_future(collect(harness.run(sid, "go")))
    child_sid = await until_spawned(store, sid)
    await until_sampling(store, child_sid)

    assert await watcher.cancel(sid, recursive=True) is True

    provider.gate.set()
    events = await turn

    assert store.cancels == [child_sid, sid]
    child_log = await history(store, child_sid)
    assert [event for event in child_log if isinstance(event, CancelRequested)]
    assert [event.stop_reason for event in child_log if isinstance(event, TurnCompleted)] == ["cancelled"]
    assert [event.session_id for event in events if isinstance(event.event, TurnCompleted)][-1] == sid
    assert picks(events, TurnCompleted)[-1].stop_reason == "cancelled"


async def test_recursive_cancel_returns_false_when_nothing_in_the_tree_is_running() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("worker", '{"task": "dig"}', cid="p1")]),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Boss],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Boss)).id

    await collect(harness.run(sid, "go"))
    child_sid = (await store.list(parent_id=sid))[0].id

    assert await harness.cancel(sid, recursive=True) is False
    for target in (sid, child_sid):
        assert not [event for event in await history(store, target) if isinstance(event, CancelRequested)]


async def test_a_cancel_recorded_after_turn_completed_does_not_poison_the_next_turn() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(text="first answer"), Sample(text="second answer")]),
        store,
        [Worker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Worker)).id

    first = await collect(harness.run(sid, "one"))
    assert picks(first, TurnCompleted)[0].stop_reason == "completed"

    await store.append(sid, [CancelRequested(turn_id="stale")], expect_seq=None)

    second = await collect(harness.run(sid, "two"))

    assert picks(second, TurnCompleted)[0].stop_reason == "completed"
    assert [event.text for event in picks(second, TextPart)] == ["second answer"]
