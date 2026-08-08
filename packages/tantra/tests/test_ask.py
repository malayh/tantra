from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import Approval, ApprovalResponse, FreeText, FreeTextResponse
from tantra.context import TurnContext
from tantra.errors import SeqConflict, TantraError
from tantra.events import (
    AskAnswered,
    AskRaised,
    SessionEvent,
    TextPart,
    ToolCallCompleted,
    ToolCallStarted,
    ToolProgress,
    TurnCompleted,
    TurnStarted,
)
from tantra.harness import Harness
from tantra.loop import DEFAULT_RETRY, TurnLoop
from tantra.providers.base import ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.fs import FileSystemStore
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


def call(name: str, args: str, cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


async def history(store: Any, sid: str) -> list[SessionEvent]:
    return [stamped.event async for stamped in store.read(sid)]


def count(events: list[SessionEvent], kind: Any) -> int:
    return len([event for event in events if isinstance(event, kind)])


async def test_a_permission_ask_suspends_and_a_discarded_harness_resumes_it() -> None:
    executed: list[str] = []

    @tool
    async def write_dashboard(path: str) -> str:
        """Write a dashboard."""
        executed.append(path)
        return f"wrote {path}"

    class Editor(Agent):
        tools = [write_dashboard]
        permissions = {"write_*": "ask"}

    store = MemoryStore()
    first = Harness(
        FakeProvider([Sample(tool_calls=[call("write_dashboard", '{"path": "p99.json"}')])]),
        store,
        [Editor],
        default_model="fake/model",
    )
    sid = (await first.create_session(Editor)).id

    opening = await collect(first.run(sid, "write the p99 dashboard"))

    raised = picks(opening, AskRaised)[0]
    assert isinstance(raised.request, Approval)
    assert raised.request.extra == {"permission": "write_dashboard"}
    assert raised.call_id == "c1"
    assert not executed
    assert not picks(opening, ToolCallStarted)
    assert not picks(opening, TurnCompleted)
    header = await store.header(sid)
    assert header.status == "awaiting_input"
    assert header.pending_ask == raised.ask_id

    del first
    second = Harness(FakeProvider([Sample(text="dashboard written.")]), store, [Editor], default_model="fake/model")

    resumed = await collect(second.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))

    assert executed == ["p99.json"]
    assert picks(resumed, AskAnswered)[0].ask_id == raised.ask_id
    assert picks(resumed, ToolCallStarted)[0].call_id == "c1"
    assert picks(resumed, ToolCallCompleted)[0].result == "wrote p99.json"
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"

    replayed = await collect(second.replay(sid))
    assert [event.seq for event in replayed] == list(range(1, len(replayed) + 1))
    header = await store.header(sid)
    assert header.status == "idle"
    assert header.pending_ask is None


async def test_resuming_a_permission_ask_with_a_denial_skips_only_that_call() -> None:
    executed: list[str] = []

    @tool
    async def write_dashboard(path: str) -> str:
        """Write a dashboard."""
        executed.append(f"write:{path}")
        return "wrote"

    @tool
    async def read_metrics(query: str) -> str:
        """Read metrics."""
        executed.append(f"read:{query}")
        return "read"

    class Editor(Agent):
        tools = [write_dashboard, read_metrics]
        permissions = {"write_*": "ask"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(
                    tool_calls=[
                        call("write_dashboard", '{"path": "a.json"}', cid="c1"),
                        call("read_metrics", '{"query": "b"}', cid="c2"),
                    ]
                ),
                Sample(text="all done"),
            ]
        ),
        store,
        [Editor],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Editor)).id

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]
    assert not picks(opening, ToolCallCompleted)

    resumed = await collect(harness.resume(sid, raised.ask_id, ApprovalResponse(allow=False)))

    denied, read = picks(resumed, ToolCallCompleted)
    assert denied.call_id == "c1"
    assert denied.is_error
    assert denied.result == "denied by user"
    assert read.call_id == "c2"
    assert read.result == "read"
    assert executed == ["read:b"]
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"


async def test_ctx_ask_re_executes_the_tool_and_replays_the_recorded_answer() -> None:
    runs: list[int] = []
    answers: list[str] = []

    @tool
    async def confirm(topic: str, ctx: Context) -> str:
        """Ask the human about a topic."""
        runs.append(1)
        reply = await ctx.ask(FreeText(prompt=f"what about {topic}?"))
        answers.append(reply.text)
        return f"heard {reply.text}"

    class Asker(Agent):
        tools = [confirm]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("confirm", '{"topic": "p99"}')]), Sample(text="ok")]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))

    raised = picks(opening, AskRaised)[0]
    assert isinstance(raised.request, FreeText)
    assert raised.request.prompt == "what about p99?"
    assert len(runs) == 1
    assert not answers

    resumed = await collect(harness.resume(sid, raised.ask_id, FreeTextResponse(text="fine")))

    assert len(runs) == 2
    assert answers == ["fine"]
    assert picks(resumed, ToolCallCompleted)[0].result == "heard fine"
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"

    logged = await history(store, sid)
    assert count(logged, AskRaised) == 1
    assert count(logged, AskAnswered) == 1
    assert count(logged, ToolCallStarted) == 1


async def test_two_asks_in_one_tool_take_two_suspend_resume_cycles() -> None:
    runs: list[int] = []
    seen: list[tuple[str, str]] = []

    @tool
    async def interview(ctx: Context) -> str:
        """Asks twice."""
        runs.append(1)
        first = await ctx.ask(FreeText(prompt="first?"))
        second = await ctx.ask(FreeText(prompt="second?"))
        seen.append((first.text, second.text))
        return f"{first.text}/{second.text}"

    class Asker(Agent):
        tools = [interview]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("interview", "{}")]), Sample(text="ok")]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))
    first_ask = picks(opening, AskRaised)[0]
    assert first_ask.request.prompt == "first?"

    middle = await collect(harness.resume(sid, first_ask.ask_id, FreeTextResponse(text="one")))
    second_ask = picks(middle, AskRaised)[0]
    assert second_ask.request.prompt == "second?"
    assert second_ask.ask_id != first_ask.ask_id
    assert (await store.header(sid)).pending_ask == second_ask.ask_id
    assert len(runs) == 2
    assert not seen

    final = await collect(harness.resume(sid, second_ask.ask_id, FreeTextResponse(text="two")))

    assert len(runs) == 3
    assert seen == [("one", "two")]
    assert picks(final, ToolCallCompleted)[0].result == "one/two"
    assert picks(final, TurnCompleted)[0].stop_reason == "completed"
    logged = await history(store, sid)
    assert count(logged, AskRaised) == 2
    assert count(logged, ToolCallStarted) == 1


async def test_a_bare_resume_re_drives_an_abandoned_turn_without_a_second_started() -> None:
    attempts: list[int] = []

    @tool
    async def flaky(ctx: Context) -> str:
        """Reports progress, then stalls on the first attempt."""
        attempts.append(1)
        await ctx.emit("working")
        if len(attempts) == 1:
            await asyncio.sleep(5.0)
        return "done"

    class Slow(Agent):
        tools = [flaky]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("flaky", "{}")]), Sample(text="finished")]),
        store,
        [Slow],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Slow)).id

    stream = harness.run(sid, "go")
    async for event in stream:
        if isinstance(event.event, ToolProgress):
            break
    await stream.aclose()

    resumed = await collect(harness.resume(sid))

    assert len(attempts) == 2
    assert picks(resumed, ToolCallCompleted)[0].result == "done"
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"
    logged = await history(store, sid)
    assert count(logged, ToolCallStarted) == 1
    assert count(logged, ToolProgress) == 2


async def test_a_resume_finishing_an_answered_submit_output_never_samples_again() -> None:
    @tool
    async def read_metrics(query: str) -> str:
        """Read metrics."""
        return "read"

    class Dashboard(BaseModel):
        title: str
        panels: int

    class Builder(Agent):
        tools = [read_metrics]
        output_schema = Dashboard

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(
                    tool_calls=[
                        call("submit_output", '{"title": "p99", "panels": 2}', cid="c1"),
                        call("read_metrics", '{"query": "x"}', cid="c2"),
                    ]
                )
            ]
        ),
        store,
        [Builder],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Builder)).id

    stream = harness.run(sid, "build it")
    async for event in stream:
        if isinstance(event.event, ToolCallCompleted):
            break
    await stream.aclose()

    fresh = Harness(FakeProvider([]), store, [Builder], default_model="fake/model")
    resumed = await collect(fresh.resume(sid))

    orphan = picks(resumed, ToolCallCompleted)[0]
    assert orphan.call_id == "c2"
    assert orphan.is_error
    assert orphan.result == "not executed: turn completed"
    turn = picks(resumed, TurnCompleted)[0]
    assert turn.stop_reason == "output"
    assert turn.output == {"title": "p99", "panels": 2}


async def test_an_invalid_json_call_stays_rejected_across_a_suspend() -> None:
    detonated: list[str] = []

    @tool
    async def nuke(target: str = "EVERYTHING") -> str:
        """Destroys a target."""
        detonated.append(target)
        return f"destroyed {target}"

    @tool
    async def asker(ctx: Context) -> str:
        """Asks before proceeding."""
        reply = await ctx.ask(Approval(title="proceed?"))
        return f"allowed={reply.allow}"

    class Risky(Agent):
        tools = [asker, nuke]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(
                    tool_calls=[
                        call("asker", "{}", cid="c1"),
                        call("nuke", '{"target": "just-this"', cid="c2"),
                    ]
                ),
                Sample(text="done"),
            ]
        ),
        store,
        [Risky],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Risky)).id

    opening = await collect(harness.run(sid, "go"))
    rejected = picks(opening, ToolCallCompleted)
    assert [event.call_id for event in rejected] == ["c2"]
    assert rejected[0].is_error
    assert rejected[0].result.startswith("invalid JSON arguments")

    ask_id = picks(opening, AskRaised)[0].ask_id
    resumed = await collect(harness.resume(sid, ask_id, ApprovalResponse(allow=True)))

    assert detonated == []
    assert [event.call_id for event in picks(resumed, ToolCallCompleted)] == ["c1"]
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"
    logged = await history(store, sid)
    results = [event for event in logged if isinstance(event, ToolCallCompleted) and event.call_id == "c2"]
    assert len(results) == 1
    assert results[0].is_error
    assert not [event for event in logged if isinstance(event, ToolCallStarted) and event.call_id == "c2"]


async def test_a_bare_resume_replays_the_pending_ask_without_re_running_the_tool() -> None:
    runs: list[int] = []

    @tool
    async def confirm(ctx: Context) -> str:
        """Asks for approval."""
        runs.append(1)
        reply = await ctx.ask(Approval(title="proceed?"))
        return f"allowed={reply.allow}"

    class Asker(Agent):
        tools = [confirm]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("confirm", "{}")]), Sample(text="done")]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))
    raised = picks(opening, AskRaised)[0]
    before = await store.header(sid)

    for _ in range(3):
        replayed = await collect(harness.resume(sid))
        assert len(replayed) == 1
        assert isinstance(replayed[0].event, AskRaised)
        assert replayed[0].event.ask_id == raised.ask_id
        assert replayed[0].seq == before.last_seq

    assert len(runs) == 1
    after = await store.header(sid)
    assert after.last_seq == before.last_seq
    assert after.status == "awaiting_input"
    assert after.pending_ask == raised.ask_id

    resumed = await collect(harness.resume(sid, raised.ask_id, ApprovalResponse(allow=True)))
    assert len(runs) == 2
    assert picks(resumed, TurnCompleted)[0].stop_reason == "completed"


async def test_resume_rejects_an_ask_from_an_earlier_terminated_turn() -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        """Asks for approval."""
        reply = await ctx.ask(Approval(title="proceed?"))
        return f"allowed={reply.allow}"

    class Asker(Agent):
        tools = [confirm]

    store = MemoryStore()
    harness = Harness(
        FakeProvider(
            [
                Sample(tool_calls=[call("confirm", "{}", cid="c1")]),
                Sample(tool_calls=[call("confirm", "{}", cid="c2")]),
            ]
        ),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    first_turn = await collect(harness.run(sid, "first"))
    stale = picks(first_turn, AskRaised)[0].ask_id
    await harness.cancel(sid)
    await collect(harness.resume(sid))

    second_turn = await collect(harness.run(sid, "second"))
    live = picks(second_turn, AskRaised)[0].ask_id
    assert live != stale

    with pytest.raises(TantraError, match="unknown or already answered"):
        await collect(harness.resume(sid, stale, ApprovalResponse(allow=True)))

    header = await store.header(sid)
    assert header.pending_ask == live
    assert not [event for event in await history(store, sid) if isinstance(event, AskAnswered)]


async def test_a_permission_ask_refuses_a_non_approval_response() -> None:
    @tool
    async def write_dashboard(path: str) -> str:
        """Write a dashboard."""
        return "wrote"

    class Editor(Agent):
        tools = [write_dashboard]
        permissions = {"write_*": "ask"}

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("write_dashboard", '{"path": "x"}')]), Sample(text="ok")]),
        store,
        [Editor],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Editor)).id

    opening = await collect(harness.run(sid, "go"))
    ask_id = picks(opening, AskRaised)[0].ask_id
    before = await store.header(sid)

    with pytest.raises(TantraError, match="needs an ApprovalResponse"):
        await collect(harness.resume(sid, ask_id, FreeTextResponse(text="sure")))

    after = await store.header(sid)
    assert after.last_seq == before.last_seq
    assert after.pending_ask == ask_id


async def test_the_loop_refuses_to_advance_a_turn_whose_ask_is_unanswered() -> None:
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
    await collect(harness.run(sid, "go"))

    header = await store.header(sid)
    logged = await history(store, sid)
    started = [event for event in logged if isinstance(event, TurnStarted)][0]
    turn = TurnContext(
        session_id=sid, turn_id=started.turn_id, agent="asker", depth=0, input=started.input, metadata={}
    )
    loop = TurnLoop(
        store=store,
        provider=FakeProvider([]),
        header=header,
        agent=Asker,
        tools={"confirm": confirm},
        model="fake/model",
        turn=turn,
        history=logged,
        retry=DEFAULT_RETRY,
        holder="probe",
        lease_ttl=60.0,
    )

    with pytest.raises(TantraError, match="is unanswered"):
        await collect(loop.run())


async def test_a_foreign_non_cancel_event_stops_the_turn_instead_of_being_absorbed() -> None:
    @tool
    async def meddle(ctx: Context) -> str:
        """Appends an event straight to the store, behind the loop's back."""
        current = await ctx.store.header(ctx.session_id)
        await ctx.store.append(
            ctx.session_id,
            [TextPart(sample_id="foreign", text="not mine")],
            expect_seq=current.last_seq,
        )
        return "done"

    class Meddler(Agent):
        tools = [meddle]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("meddle", "{}")]), Sample(text="never reached")]),
        store,
        [Meddler],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Meddler)).id

    with pytest.raises(SeqConflict):
        await collect(harness.run(sid, "go"))

    logged = await history(store, sid)
    assert [event for event in logged if isinstance(event, TextPart) and event.sample_id == "foreign"]
    assert not [event for event in logged if isinstance(event, ToolCallCompleted)]
    assert not [event for event in logged if isinstance(event, TurnCompleted)]


async def test_resume_rejects_an_unknown_or_already_answered_ask() -> None:
    @tool
    async def interview(ctx: Context) -> str:
        """Asks twice."""
        first = await ctx.ask(FreeText(prompt="first?"))
        second = await ctx.ask(FreeText(prompt="second?"))
        return f"{first.text}/{second.text}"

    class Asker(Agent):
        tools = [interview]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("interview", "{}")]), Sample(text="ok")]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))
    first_ask = picks(opening, AskRaised)[0]

    with pytest.raises(TantraError, match="unknown or already answered"):
        await collect(harness.resume(sid, "no-such-ask", FreeTextResponse(text="x")))

    await collect(harness.resume(sid, first_ask.ask_id, FreeTextResponse(text="one")))

    with pytest.raises(TantraError, match="unknown or already answered"):
        await collect(harness.resume(sid, first_ask.ask_id, FreeTextResponse(text="again")))


async def test_resume_requires_an_ask_id_and_a_response_together() -> None:
    @tool
    async def noop(ctx: Context) -> str:
        """Does nothing."""
        return "ok"

    class Bot(Agent):
        tools = [noop]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("noop", "{}")]), Sample(text="ok")]),
        store,
        [Bot],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Bot)).id
    stream = harness.run(sid, "go")
    await anext(stream)
    await stream.aclose()

    with pytest.raises(TantraError, match="together, or neither"):
        await collect(harness.resume(sid, "some-ask"))


async def test_resume_without_an_incomplete_turn_raises() -> None:
    class Bot(Agent): ...

    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model="fake/model")
    sid = (await harness.create_session(Bot)).id
    await collect(harness.run(sid, "go"))

    with pytest.raises(TantraError, match="no incomplete turn"):
        await collect(harness.resume(sid))


async def test_a_stored_ask_request_reads_back_as_its_typed_class(tmp_path: Path) -> None:
    @tool
    async def pick(ctx: Context) -> str:
        """Asks for approval."""
        reply = await ctx.ask(Approval(title="proceed?", body="the diff", extra={"risk": "high"}))
        return f"allowed={reply.allow}"

    class Asker(Agent):
        tools = [pick]

    store = FileSystemStore(tmp_path / "sessions")
    await store.setup()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[call("pick", "{}")]), Sample(text="ok")]),
        store,
        [Asker],
        default_model="fake/model",
    )
    sid = (await harness.create_session(Asker)).id

    opening = await collect(harness.run(sid, "go"))
    ask_id = picks(opening, AskRaised)[0].ask_id

    stored = [event for event in await history(store, sid) if isinstance(event, AskRaised)][0]
    assert isinstance(stored.request, Approval)
    assert stored.request.title == "proceed?"
    assert stored.request.body == "the diff"
    assert stored.request.extra == {"risk": "high"}

    resumed = await collect(harness.resume(sid, ask_id, ApprovalResponse(allow=True)))

    answered = [event for event in await history(store, sid) if isinstance(event, AskAnswered)][0]
    assert isinstance(answered.response, ApprovalResponse)
    assert answered.response.allow is True
    assert picks(resumed, ToolCallCompleted)[0].result == "allowed=True"
