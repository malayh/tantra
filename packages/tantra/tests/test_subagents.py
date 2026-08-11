from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import FreeText, FreeTextResponse
from tantra.errors import SessionBusy, TantraError
from tantra.events import (
    AskRaised,
    CancelRequested,
    ChildSessionSpawned,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    ToolProgress,
    TurnCompleted,
)
from tantra.harness import Harness
from tantra.hooks import Hook
from tantra.loop import Emitted
from tantra.providers.base import ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def picks(events: list[Emitted], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


async def history(store: MemoryStore, sid: str) -> list[SessionEvent]:
    return [stamped.event async for stamped in store.read(sid)]


@tool
async def look(q: str) -> str:
    """Look something up."""
    return f"found {q}"


class Researcher(Agent):
    tools = [look]


class Boss(Agent):
    subagents = [Researcher]


async def test_child_tool_events_ride_the_parent_stream_but_never_the_parent_log() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("researcher", '{"task": "dig"}', cid="p1")]),
                Sample(tool_calls=[call("look", '{"q": "a"}', cid="k1"), call("look", '{"q": "b"}', cid="k2")]),
                Sample(text="child answer"),
                Sample(text="parent answer"),
            ]
        ),
        store,
        [Boss],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Boss, {"company": 42})).id

    events = await collect(harness.run(sid, "go"))

    spawned = picks(events, ChildSessionSpawned)
    assert len(spawned) == 1
    assert spawned[0].agent == "researcher"
    assert spawned[0].call_id == "p1"
    child_sid = spawned[0].child_session_id

    forwarded = [event for event in events if event.session_id == child_sid]
    assert forwarded
    assert all(event.depth == 1 for event in forwarded)
    assert [event.event.args["q"] for event in forwarded if isinstance(event.event, ToolCallRequested)] == ["a", "b"]
    assert [event.event.result for event in forwarded if isinstance(event.event, ToolCallCompleted)] == [
        "found a",
        "found b",
    ]

    parent_log = await history(store, sid)
    assert [event.name for event in parent_log if isinstance(event, ToolCallRequested)] == ["researcher"]
    assert len([event for event in parent_log if isinstance(event, ChildSessionSpawned)]) == 1
    result = [event for event in parent_log if isinstance(event, ToolCallCompleted)][0]
    assert result.result == "child answer"
    assert not result.is_error
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"

    fresh = Harness(FakeProvider([]), store, [Boss], default_model="fake/model")
    replayed = await collect(fresh.replay(sid))
    assert all(event.session_id == sid for event in replayed)
    assert [event.event for event in replayed] == parent_log

    child_header = await store.header(child_sid)
    assert child_header.parent_id == sid
    assert child_header.depth == 1
    assert child_header.metadata == {"company": 42}
    assert [header.id for header in await store.list(parent_id=sid)] == [child_sid]


async def test_replaying_a_child_session_keeps_its_depth() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("researcher", '{"task": "dig"}', cid="p1")]),
                Sample(tool_calls=[call("look", '{"q": "a"}', cid="k1")]),
                Sample(text="child answer"),
                Sample(text="parent answer"),
            ]
        ),
        store,
        [Boss],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Boss)).id

    events = await collect(harness.run(sid, "go"))
    child_sid = picks(events, ChildSessionSpawned)[0].child_session_id

    replayed = await collect(harness.replay(child_sid))

    assert replayed
    assert all(event.session_id == child_sid for event in replayed)
    assert all(event.depth == 1 for event in replayed), "replay flattened a child session to depth 0"


async def test_the_subagent_tool_is_named_after_the_agent_and_takes_a_task() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Boss], default_model="fake/model")

    delegate = harness.tools["boss"]["researcher"]
    assert delegate.schema.name == "researcher"
    assert delegate.schema.description == "Delegate a task to the researcher sub-agent."
    assert delegate.schema.parameters["properties"]["task"]["type"] == "string"
    assert "ctx" not in delegate.schema.parameters["properties"]


async def test_a_subagent_docstring_becomes_the_tool_description() -> None:
    class Scout(Agent):
        """Scouts ahead and reports back."""

    class Captain(Agent):
        subagents = [Scout]

    harness = Harness(FakeProvider([]), MemoryStore(), [Captain], default_model="fake/model")
    assert harness.tools["captain"]["scout"].schema.description == "Scouts ahead and reports back."


async def test_spawn_accepts_a_class_or_a_name_and_records_one_child_per_call() -> None:
    @tool
    async def delegate(ctx: Context) -> str:
        """Delegates twice, by class then by name."""
        first = await ctx.spawn(Researcher, "one")
        second = await ctx.spawn("researcher", "two")
        return f"{first}|{second}"

    class Delegator(Agent):
        tools = [delegate]
        subagents = [Researcher]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("delegate", "{}", cid="p1")]),
                Sample(text="one done"),
                Sample(text="two done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Delegator],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Delegator)).id

    events = await collect(harness.run(sid, "go"))

    assert [event.result for event in picks(events, ToolCallCompleted) if event.call_id == "p1"] == [
        "one done|two done"
    ]
    spawned = picks(events, ChildSessionSpawned)
    assert len(spawned) == 2
    assert len({event.child_session_id for event in spawned}) == 2
    assert len(await store.list(parent_id=sid)) == 2


async def test_spawn_returns_the_parsed_output_of_a_child_with_an_output_schema() -> None:
    class Report(BaseModel):
        title: str
        findings: int

    class Analyst(Agent):
        output_schema = Report

    @tool
    async def analyze(ctx: Context) -> Any:
        """Delegates to the analyst."""
        return await ctx.spawn(Analyst, "analyze this")

    class Chief(Agent):
        tools = [analyze]
        subagents = [Analyst]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("analyze", "{}", cid="p1")]),
                Sample(tool_calls=[call("submit_output", '{"title": "p99", "findings": 3}', cid="k1")]),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Chief],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Chief)).id

    events = await collect(harness.run(sid, "go"))

    parent_result = [event for event in picks(events, ToolCallCompleted) if event.call_id == "p1"][0]
    assert parent_result.result == {"title": "p99", "findings": 3}
    assert not parent_result.is_error


async def test_spawning_an_unknown_agent_raises_before_any_child_session_exists() -> None:
    @tool
    async def summon(ctx: Context) -> str:
        """Spawns an agent that was never declared."""
        return await ctx.spawn("ghost", "x")

    class Summoner(Agent):
        tools = [summon]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("summon", "{}", cid="p1")]),
                Sample(text="recovered"),
            ]
        ),
        store,
        [Summoner],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Summoner)).id

    events = await collect(harness.run(sid, "go"))

    failure = picks(events, ToolCallCompleted)[0]
    assert failure.is_error
    assert "unknown agent 'ghost'" in failure.result
    assert not picks(events, ChildSessionSpawned)
    assert await store.list(parent_id=sid) == []
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_max_depth_stops_the_spawn_before_the_child_session_is_created() -> None:
    @tool
    async def deeper(ctx: Context) -> str:
        """Spawns one level deeper."""
        return await ctx.spawn(Researcher, "deeper")

    class Middle(Agent):
        tools = [deeper]
        subagents = [Researcher]

    class Top(Agent):
        subagents = [Middle]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("middle", '{"task": "go"}', cid="p1")]),
                Sample(tool_calls=[call("deeper", "{}", cid="k1")]),
                Sample(text="child recovered"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Top],
        default_model="fake/model",
        max_depth=1,
    )
    sid = (await harness.create_session(Top)).id

    events = await collect(harness.run(sid, "go"))

    child_sid = picks(events, ChildSessionSpawned)[0].child_session_id
    refused = [event for event in picks(events, ToolCallCompleted) if event.call_id == "k1"][0]
    assert refused.is_error
    assert "max_depth 1 exceeded" in refused.result
    assert await store.list(parent_id=child_sid) == []
    assert len(picks(events, ChildSessionSpawned)) == 1
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_a_child_ask_suspends_the_ancestry_and_two_resumes_finish_both_turns() -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        """Asks the human before answering."""
        reply = await ctx.ask(FreeText(prompt="which one?"))
        return f"picked {reply.text}"

    class Asker(Agent):
        tools = [confirm]

    class Manager(Agent):
        subagents = [Asker]

    store = MemoryStore()
    opening_harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("asker", '{"task": "decide"}', cid="p1")]),
                Sample(tool_calls=[call("confirm", "{}", cid="k1")]),
            ]
        ),
        store,
        [Manager],
        default_model="fake/model",
    )
    sid = (await opening_harness.create_session(Manager)).id

    opening = await collect(opening_harness.run(sid, "go"))

    child_sid = picks(opening, ChildSessionSpawned)[0].child_session_id
    raised = picks(opening, AskRaised)[0]
    assert [event.session_id for event in opening if isinstance(event.event, AskRaised)] == [child_sid]
    assert not picks(opening, TurnCompleted)

    parent_log = await history(store, sid)
    assert not [event for event in parent_log if isinstance(event, AskRaised)]
    assert not [event for event in parent_log if isinstance(event, ToolCallCompleted)]
    assert [event.ask_id for event in await history(store, child_sid) if isinstance(event, AskRaised)] == [
        raised.ask_id
    ]

    parent_header = await store.header(sid)
    child_header = await store.header(child_sid)
    assert parent_header.status == "awaiting_input"
    assert parent_header.pending_ask == raised.ask_id
    assert child_header.status == "awaiting_input"
    assert child_header.pending_ask == raised.ask_id

    del opening_harness
    fresh = Harness(
        FakeProvider([Sample(text="child done"), Sample(text="parent done")]),
        store,
        [Manager],
        default_model="fake/model",
    )

    answered = await collect(fresh.resume(child_sid, raised.ask_id, FreeTextResponse(text="blue")))
    assert picks(answered, ToolCallCompleted)[0].result == "picked blue"
    assert picks(answered, TurnCompleted)[0].stop_reason == "completed"

    finished = await collect(fresh.resume(sid))
    assert [event.result for event in picks(finished, ToolCallCompleted) if event.call_id == "p1"] == ["child done"]
    assert picks(finished, TurnCompleted)[0].stop_reason == "completed"

    assert [header.id for header in await store.list(parent_id=sid)] == [child_sid]
    assert (await store.header(sid)).status == "idle"


async def test_a_grandchild_ask_suspends_three_levels_and_one_root_resume_re_drives_the_chain() -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        """Asks the human before answering."""
        reply = await ctx.ask(FreeText(prompt="ok?"))
        return f"heard {reply.text}"

    class Leaf(Agent):
        tools = [confirm]

    class Mid(Agent):
        subagents = [Leaf]

    class Root(Agent):
        subagents = [Mid]

    store = MemoryStore()
    opening_harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("mid", '{"task": "go"}', cid="p1")]),
                Sample(tool_calls=[call("leaf", '{"task": "go"}', cid="m1")]),
                Sample(tool_calls=[call("confirm", "{}", cid="l1")]),
            ]
        ),
        store,
        [Root],
        default_model="fake/model",
    )
    sid = (await opening_harness.create_session(Root)).id

    opening = await collect(opening_harness.run(sid, "go"))

    mid_sid, leaf_sid = [event.child_session_id for event in picks(opening, ChildSessionSpawned)]
    raised = picks(opening, AskRaised)[0]
    assert {(event.session_id, event.depth) for event in opening} == {(sid, 0), (mid_sid, 1), (leaf_sid, 2)}
    for target in (sid, mid_sid, leaf_sid):
        header = await store.header(target)
        assert header.status == "awaiting_input"
        assert header.pending_ask == raised.ask_id

    with pytest.raises(TantraError, match="unknown or already answered"):
        await collect(opening_harness.resume(sid, raised.ask_id, FreeTextResponse(text="no")))

    del opening_harness
    fresh = Harness(
        FakeProvider([Sample(text="leaf done"), Sample(text="mid done"), Sample(text="root done")]),
        store,
        [Root],
        default_model="fake/model",
    )

    await collect(fresh.resume(leaf_sid, raised.ask_id, FreeTextResponse(text="yes")))
    finished = await collect(fresh.resume(sid))

    assert [event.session_id for event in finished if isinstance(event.event, TurnCompleted)] == [mid_sid, sid]
    assert [event.result for event in picks(finished, ToolCallCompleted) if event.call_id == "p1"] == ["mid done"]
    assert len(await store.list(parent_id=sid)) == 1
    assert len(await store.list(parent_id=mid_sid)) == 1


async def test_abandoning_a_parent_mid_child_turn_reattaches_instead_of_spawning_a_twin() -> None:
    attempts: list[int] = []

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

    class Overseer(Agent):
        subagents = [Slow]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("slow", '{"task": "grind"}', cid="p1")]),
                Sample(tool_calls=[call("flaky", "{}", cid="k1")]),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Overseer],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Overseer)).id

    stream = harness.run(sid, "go")
    child_sid = ""
    async for event in stream:
        if isinstance(event.event, ChildSessionSpawned):
            child_sid = event.event.child_session_id
        if isinstance(event.event, ToolProgress):
            break
    await stream.aclose()

    assert child_sid
    assert len(attempts) == 1

    resumed = await collect(harness.resume(sid))

    assert len(attempts) == 2
    assert not picks(resumed, ChildSessionSpawned)
    assert [event.result for event in picks(resumed, ToolCallCompleted) if event.call_id == "p1"] == ["child done"]
    assert picks(resumed, TurnCompleted)[-1].stop_reason == "completed"
    assert [header.id for header in await store.list(parent_id=sid)] == [child_sid]
    assert len([event for event in await history(store, sid) if isinstance(event, ChildSessionSpawned)]) == 1


async def test_a_failed_spawn_does_not_shift_the_next_spawn_onto_its_child() -> None:
    failures: list[str] = []

    @tool
    async def confirm(ctx: Context) -> str:
        """Asks the human first."""
        reply = await ctx.ask(FreeText(prompt="ok?"))
        return f"heard {reply.text}"

    class Helper(Agent):
        tools = [confirm]

    @tool
    async def resilient(ctx: Context) -> str:
        """Tries a missing agent, shrugs it off, then delegates for real."""
        try:
            await ctx.spawn("ghost", "a")
        except TantraError as exc:
            failures.append(str(exc))
        return await ctx.spawn(Helper, "b")

    class Chief(Agent):
        tools = [resilient]
        subagents = [Helper]

    store = MemoryStore()
    opening_harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("resilient", "{}", cid="p1")]),
                Sample(tool_calls=[call("confirm", "{}", cid="k1")]),
            ]
        ),
        store,
        [Chief],
        default_model="fake/model",
    )
    sid = (await opening_harness.create_session(Chief)).id

    opening = await collect(opening_harness.run(sid, "go"))
    child_sid = picks(opening, ChildSessionSpawned)[0].child_session_id
    raised = picks(opening, AskRaised)[0]
    assert len(failures) == 1

    del opening_harness
    fresh = Harness(
        FakeProvider([Sample(text="child done"), Sample(text="parent done")]),
        store,
        [Chief],
        default_model="fake/model",
    )

    await collect(fresh.resume(child_sid, raised.ask_id, FreeTextResponse(text="yes")))
    finished = await collect(fresh.resume(sid))

    assert len(failures) == 2
    assert all("unknown agent 'ghost'" in failure for failure in failures)
    assert [event.result for event in picks(finished, ToolCallCompleted) if event.call_id == "p1"] == ["child done"]
    assert [header.id for header in await store.list(parent_id=sid)] == [child_sid]
    assert len([event for event in await history(store, sid) if isinstance(event, ChildSessionSpawned)]) == 1


async def test_a_busy_child_leaves_the_parent_turn_incomplete_instead_of_answering_the_spawn() -> None:
    attempts: list[int] = []

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

    class Overseer(Agent):
        subagents = [Slow]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("slow", '{"task": "grind"}', cid="p1")]),
                Sample(tool_calls=[call("flaky", "{}", cid="k1")]),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Overseer],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Overseer)).id

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

    parent_log = await history(store, sid)
    assert not [event for event in parent_log if isinstance(event, ToolCallCompleted)]
    assert not [event for event in parent_log if isinstance(event, TurnCompleted)]
    assert len(attempts) == 1

    await store.release_lease(child_sid, "intruder")
    resumed = await collect(harness.resume(sid))

    assert len(attempts) == 2
    assert [event.result for event in picks(resumed, ToolCallCompleted) if event.call_id == "p1"] == ["child done"]
    assert picks(resumed, TurnCompleted)[-1].stop_reason == "completed"
    assert [header.id for header in await store.list(parent_id=sid)] == [child_sid]


async def test_a_cancelled_child_becomes_an_error_result_not_a_silent_success() -> None:
    @tool
    async def bail(ctx: Context) -> str:
        """Flags its own session for cancellation mid-turn."""
        header = await ctx.store.header(ctx.session_id)
        await ctx.store.append(ctx.session_id, [CancelRequested(turn_id=ctx.turn_id)], expect_seq=header.last_seq)
        return "bailed"

    class Quitter(Agent):
        tools = [bail]

    class Employer(Agent):
        subagents = [Quitter]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("quitter", '{"task": "go"}', cid="p1")]),
                Sample(tool_calls=[call("bail", "{}", cid="k1")]),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Employer],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Employer)).id

    events = await collect(harness.run(sid, "go"))

    child_sid = picks(events, ChildSessionSpawned)[0].child_session_id
    child_terminal = [event for event in await history(store, child_sid) if isinstance(event, TurnCompleted)][0]
    assert child_terminal.stop_reason == "cancelled"

    delegated = [event for event in picks(events, ToolCallCompleted) if event.call_id == "p1"][0]
    assert delegated.is_error
    assert "sub-agent turn cancelled" in delegated.result
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_a_child_that_hits_max_steps_without_output_becomes_an_error_result() -> None:
    class Capped(Agent):
        tools = [look]
        max_steps = 1

    class Employer(Agent):
        subagents = [Capped]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("capped", '{"task": "go"}', cid="p1")]),
                Sample(tool_calls=[call("look", '{"q": "a"}', cid="k1")]),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Employer],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Employer)).id

    events = await collect(harness.run(sid, "go"))

    child_sid = picks(events, ChildSessionSpawned)[0].child_session_id
    child_terminal = [event for event in await history(store, child_sid) if isinstance(event, TurnCompleted)][0]
    assert child_terminal.stop_reason == "max_steps"

    delegated = [event for event in picks(events, ToolCallCompleted) if event.call_id == "p1"][0]
    assert delegated.is_error
    assert "hit max_steps without producing output" in delegated.result
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_a_missing_ancestor_header_fails_closed_instead_of_widening_permissions() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="never")]), store, [Boss], default_model="fake/model")
    await store.create(SessionHeader(id="orphan", agent="researcher", parent_id="vanished", depth=1))
    await store.append(
        "orphan",
        [SessionCreated(agent="researcher", parent_id="vanished", depth=1)],
        expect_seq=0,
    )

    with pytest.raises(TantraError, match="parent session 'vanished' is missing"):
        await collect(harness.run("orphan", "go"))


async def test_a_parent_deny_rule_beats_a_child_allow_rule() -> None:
    executed: list[str] = []

    @tool
    async def risky(ctx: Context) -> str:
        """Does something the parent forbids."""
        executed.append("ran")
        return "ran"

    class Worker(Agent):
        tools = [risky]
        permissions = {"risky": "allow"}

    class Governor(Agent):
        subagents = [Worker]
        permissions = {"risky": "deny"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("worker", '{"task": "go"}', cid="p1")]),
                Sample(tool_calls=[call("risky", "{}", cid="k1")]),
                Sample(text="child done"),
                Sample(text="parent done"),
            ]
        ),
        store,
        [Governor],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Governor)).id

    events = await collect(harness.run(sid, "go"))

    denied = [event for event in picks(events, ToolCallCompleted) if event.call_id == "k1"][0]
    assert denied.is_error
    assert denied.result == "denied by permissions: risky"
    assert executed == []
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_the_permission_chain_is_rebuilt_when_a_child_resumes_in_a_fresh_harness() -> None:
    executed: list[str] = []

    @tool
    async def risky() -> str:
        """Does something the parent forbids."""
        executed.append("ran")
        return "ran"

    @tool
    async def confirm(ctx: Context) -> str:
        """Asks the human first."""
        reply = await ctx.ask(FreeText(prompt="ok?"))
        return f"heard {reply.text}"

    class Worker(Agent):
        tools = [confirm, risky]
        permissions = {"risky": "allow"}

    class Governor(Agent):
        subagents = [Worker]
        permissions = {"risky": "deny"}

    store = MemoryStore()
    opening_harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("worker", '{"task": "go"}', cid="p1")]),
                Sample(tool_calls=[call("confirm", "{}", cid="k1"), call("risky", "{}", cid="k2")]),
            ]
        ),
        store,
        [Governor],
        default_model="fake/model",
    )
    sid = (await opening_harness.create_session(Governor)).id

    opening = await collect(opening_harness.run(sid, "go"))
    child_sid = picks(opening, ChildSessionSpawned)[0].child_session_id
    raised = picks(opening, AskRaised)[0]

    del opening_harness
    fresh = Harness(
        FakeProvider([Sample(text="child done"), Sample(text="parent done")]),
        store,
        [Governor],
        default_model="fake/model",
    )

    answered = await collect(fresh.resume(child_sid, raised.ask_id, FreeTextResponse(text="yes")))

    denied = [event for event in picks(answered, ToolCallCompleted) if event.call_id == "k2"][0]
    assert denied.is_error
    assert denied.result == "denied by permissions: risky"
    assert executed == []

    finished = await collect(fresh.resume(sid))
    assert picks(finished, TurnCompleted)[0].stop_reason == "completed"


async def test_a_forwarded_child_event_fires_on_event_exactly_once() -> None:
    seen: list[tuple[str, int]] = []

    class Recorder(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            if emitted.seq is not None:
                seen.append((emitted.session_id, emitted.seq))

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("researcher", '{"task": "dig"}', cid="p1")]),
                Sample(tool_calls=[call("look", '{"q": "a"}', cid="k1")]),
                Sample(text="child answer"),
                Sample(text="parent answer"),
            ]
        ),
        store,
        [Boss],
        default_model="fake/model",
        hooks=[Recorder()],
    )
    sid = (await harness.create_session(Boss)).id

    events = await collect(harness.run(sid, "go"))

    child_sid = picks(events, ChildSessionSpawned)[0].child_session_id
    assert len(seen) == len(set(seen))
    assert [key for key in seen if key[0] == child_sid]
    assert len([event for event in picks(events, TextPart) if event.text == "child answer"]) == 1
