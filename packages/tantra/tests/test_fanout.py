from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import FreeText, FreeTextResponse
from tantra.errors import ProviderError, SessionBusy, TantraError
from tantra.events import (
    AskRaised,
    ChildSessionSpawned,
    ToolCallCompleted,
    ToolProgress,
    TurnCompleted,
    TurnFailed,
)
from tantra.harness import Harness
from tantra.loop import Emitted
from tantra.providers.base import ProviderEvent, SampleRequest, ToolCall, UserMessage
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def picks(events: list[Emitted], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


class BrokenFor(FakeProvider):
    def __init__(self, samples: list[Sample], refuses: str) -> None:
        super().__init__(samples)
        self.refuses = refuses

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if any(isinstance(message, UserMessage) and message.content == self.refuses for message in req.messages):
            raise ProviderError("provider refused this turn", status_code=400)
        async for event in super().stream(req):
            yield event


class Worker(Agent):
    """Answers whatever it is asked."""


async def test_fan_out_of_three_returns_two_results_and_one_error() -> None:
    captured: list[list[Any]] = []

    @tool
    async def dispatch(ctx: Context) -> str:
        """Fans work out to three agents."""
        results = await ctx.fan_out([(Worker, "a"), ("ghost", "b"), ("worker", "c")])
        captured.append(results)
        return "dispatched"

    class Dispatcher(Agent):
        tools = [dispatch]
        subagents = [Worker]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("dispatch", "{}", cid="p1")]),
                Sample(text="child done"),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Dispatcher],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Dispatcher)).id

    events = await collect(harness.run(sid, "go"))

    results = captured[0]
    assert len(results) == 3
    assert results[0] == "child done"
    assert results[2] == "child done"
    assert isinstance(results[1], TantraError)
    assert "unknown agent 'ghost'" in str(results[1])

    assert len(picks(events, ChildSessionSpawned)) == 2
    assert len(await store.list(parent_id=sid)) == 2
    assert [event.result for event in picks(events, ToolCallCompleted) if event.call_id == "p1"] == ["dispatched"]
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_a_failing_child_turn_becomes_one_error_slot_and_the_parent_completes() -> None:
    captured: list[list[Any]] = []

    @tool
    async def dispatch(ctx: Context) -> str:
        """Fans work out to two agents, one at a time."""
        captured.append(await ctx.fan_out([(Worker, "a"), (Worker, "b")], max_concurrency=1))
        return "dispatched"

    class Dispatcher(Agent):
        tools = [dispatch]
        subagents = [Worker]

    store = MemoryStore()
    harness = Harness(
        BrokenFor(
            [
                Sample(tool_calls=[call("dispatch", "{}", cid="p1")]),
                Sample(text="a done"),
                Sample(text="parent done"),
            ],
            refuses="b",
        ),
        store,
        [Dispatcher],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Dispatcher)).id

    events = await collect(harness.run(sid, "go"))

    results = captured[0]
    assert results[0] == "a done"
    assert isinstance(results[1], TantraError)
    assert "provider refused this turn" in str(results[1])

    failed = picks(events, TurnFailed)
    assert len(failed) == 1
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"
    assert len(await store.list(parent_id=sid)) == 2


async def test_one_asking_child_suspends_the_fan_out_and_the_rest_keep_their_results() -> None:
    captured: list[list[Any]] = []

    @tool
    async def confirm(ctx: Context) -> str:
        """Asks the human first."""
        reply = await ctx.ask(FreeText(prompt="ok?"))
        return f"heard {reply.text}"

    class Asker(Agent):
        tools = [confirm]

    @tool
    async def dispatch(ctx: Context) -> str:
        """Fans work out to two agents, one at a time."""
        captured.append(await ctx.fan_out([(Worker, "a"), (Asker, "b")], max_concurrency=1))
        return "dispatched"

    class Dispatcher(Agent):
        tools = [dispatch]
        subagents = [Worker, Asker]

    store = MemoryStore()
    opening_harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("dispatch", "{}", cid="p1")]),
                Sample(text="a done"),
                Sample(tool_calls=[call("confirm", "{}", cid="k1")]),
            ]
        ),
        store,
        [Dispatcher],
        default_model="fake/model",
    )
    sid = (await opening_harness.create_session(Dispatcher)).id

    opening = await collect(opening_harness.run(sid, "go"))

    raised = picks(opening, AskRaised)[0]
    asking_sid = [event.session_id for event in opening if isinstance(event.event, AskRaised)][0]
    assert not captured
    assert not [event for event in opening if event.session_id == sid and isinstance(event.event, TurnCompleted)]
    assert (await store.header(sid)).pending_ask == raised.ask_id
    children = sorted(header.id for header in await store.list(parent_id=sid))
    assert len(children) == 2

    del opening_harness
    fresh = Harness(
        FakeProvider([Sample(text="b done"), Sample(text="parent done")]),
        store,
        [Dispatcher],
        default_model="fake/model",
    )

    await collect(fresh.resume(asking_sid, raised.ask_id, FreeTextResponse(text="yes")))
    finished = await collect(fresh.resume(sid))

    assert captured[0] == ["a done", "b done"]
    assert [event.result for event in picks(finished, ToolCallCompleted) if event.call_id == "p1"] == ["dispatched"]
    assert picks(finished, TurnCompleted)[0].stop_reason == "completed"
    assert sorted(header.id for header in await store.list(parent_id=sid)) == children


async def test_a_failing_slot_never_shifts_the_children_a_resumed_fan_out_attaches_to() -> None:
    captured: list[list[Any]] = []

    @tool
    async def confirm(ctx: Context) -> str:
        """Asks the human first."""
        reply = await ctx.ask(FreeText(prompt="ok?"))
        return f"heard {reply.text}"

    class Asker(Agent):
        tools = [confirm]

    @tool
    async def dispatch(ctx: Context) -> str:
        """Fans out to one missing agent and two real ones."""
        captured.append(await ctx.fan_out([("ghost", "a"), (Worker, "b"), (Asker, "c")], max_concurrency=1))
        return "dispatched"

    class Dispatcher(Agent):
        tools = [dispatch]
        subagents = [Worker, Asker]

    store = MemoryStore()
    opening_harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("dispatch", "{}", cid="p1")]),
                Sample(text="b done"),
                Sample(tool_calls=[call("confirm", "{}", cid="k1")]),
            ]
        ),
        store,
        [Dispatcher],
        default_model="fake/model",
    )
    sid = (await opening_harness.create_session(Dispatcher)).id

    opening = await collect(opening_harness.run(sid, "go"))

    spawned = [event.child_session_id for event in picks(opening, ChildSessionSpawned)]
    raised = picks(opening, AskRaised)[0]
    asking_sid = [event.session_id for event in opening if isinstance(event.event, AskRaised)][0]
    assert len(spawned) == 2
    assert asking_sid == spawned[1]

    del opening_harness
    fresh = Harness(
        FakeProvider([Sample(text="c done"), Sample(text="parent done")]),
        store,
        [Dispatcher],
        default_model="fake/model",
    )

    await collect(fresh.resume(asking_sid, raised.ask_id, FreeTextResponse(text="yes")))
    finished = await collect(fresh.resume(sid))

    results = captured[0]
    assert isinstance(results[0], TantraError)
    assert "unknown agent 'ghost'" in str(results[0])
    assert results[1] == "b done"
    assert results[2] == "c done"
    assert not picks(finished, ChildSessionSpawned)
    assert sorted(header.id for header in await store.list(parent_id=sid)) == sorted(spawned)
    assert picks(finished, TurnCompleted)[0].stop_reason == "completed"


async def test_a_busy_fan_out_child_aborts_the_parent_turn_instead_of_filling_its_slot() -> None:
    attempts: list[int] = []
    captured: list[list[Any]] = []

    @tool
    async def flaky(ctx: Context) -> str:
        """Reports progress, then stalls on the first attempt."""
        attempts.append(1)
        await ctx.emit("working")
        if len(attempts) == 1:
            await asyncio.sleep(5.0)
        return "child work"

    class Slow(Agent):
        tools = [flaky]

    @tool
    async def dispatch(ctx: Context) -> str:
        """Fans one slow task out."""
        captured.append(await ctx.fan_out([(Slow, "a")], max_concurrency=1))
        return "dispatched"

    class Dispatcher(Agent):
        tools = [dispatch]
        subagents = [Slow]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("dispatch", "{}", cid="p1")]),
                Sample(tool_calls=[call("flaky", "{}", cid="k1")]),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Dispatcher],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Dispatcher)).id

    stream = harness.run(sid, "go")
    child_sid = ""
    async for event in stream:
        if isinstance(event.event, ChildSessionSpawned):
            child_sid = event.event.child_session_id
        if isinstance(event.event, ToolProgress):
            break
    await stream.aclose()

    assert await store.acquire_lease(child_sid, "intruder", 60.0)

    with pytest.raises(SessionBusy, match="is busy: lease held by another writer"):
        await collect(harness.resume(sid))

    parent_log = [stamped.event async for stamped in store.read(sid)]
    assert not captured
    assert not [event for event in parent_log if isinstance(event, ToolCallCompleted)]
    assert not [event for event in parent_log if isinstance(event, TurnCompleted)]

    await store.release_lease(child_sid, "intruder")
    resumed = await collect(harness.resume(sid))

    assert captured[0] == ["child done"]
    assert [event.result for event in picks(resumed, ToolCallCompleted) if event.call_id == "p1"] == ["dispatched"]
    assert picks(resumed, TurnCompleted)[-1].stop_reason == "completed"
    assert [header.id for header in await store.list(parent_id=sid)] == [child_sid]


async def test_fan_out_forwards_every_child_event_and_keeps_the_parent_log_clean() -> None:
    @tool
    async def dispatch(ctx: Context) -> str:
        """Fans work out to two agents."""
        await ctx.fan_out([(Worker, "a"), (Worker, "b")], max_concurrency=1)
        return "dispatched"

    class Dispatcher(Agent):
        tools = [dispatch]
        subagents = [Worker]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("dispatch", "{}", cid="p1")]),
                Sample(text="child done"),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Dispatcher],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Dispatcher)).id

    events = await collect(harness.run(sid, "go"))

    children = {event.child_session_id for event in picks(events, ChildSessionSpawned)}
    assert len(children) == 2
    forwarded = [event for event in events if event.session_id in children]
    assert all(event.depth == 1 for event in forwarded)
    assert len([event for event in forwarded if isinstance(event.event, TurnCompleted)]) == 2

    parent_log = [stamped.event async for stamped in store.read(sid)]
    assert len([event for event in parent_log if isinstance(event, ChildSessionSpawned)]) == 2
    assert not [event for event in parent_log if isinstance(event, AskRaised)]
