from __future__ import annotations

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


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


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
