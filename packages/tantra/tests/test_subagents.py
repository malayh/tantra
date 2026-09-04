from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from pydantic import BaseModel

from tantra import Agent, Context, Harness, TantraError, TaskRef, tool
from tantra.adapters.collect import collect
from tantra.ask import FreeText, FreeTextResponse
from tantra.context import build_messages
from tantra.events import (
    AgentMessageQueued,
    AskAnswered,
    AskRaised,
    ChildSessionSpawned,
    KillRequested,
    ReasoningPart,
    SampleStarted,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    TaskNoticeQueued,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.extratools.shell import bash
from tantra.hooks import Hook
from tantra.loop import INBOX_RESULT, KILLED_RESULT, Emitted
from tantra.providers.base import ModelLimits, ProviderEvent, ReasoningBlock, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tasking import TASK_TOOL_NAMES, TaskSupervisor, derive_task_state, kill_id, message_id, task_id
from tantra.tracing import NullTracer


def call(name: str, args: str, cid: str) -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def picks(events: list[Emitted], kind: Any) -> list[Any]:
    return [item.event for item in events if isinstance(item.event, kind)]


async def history(store: MemoryStore, sid: str) -> list[SessionEvent]:
    return [item.event async for item in store.read(sid)]


class RoutedProvider:
    provider_name = "routed"

    def __init__(self) -> None:
        self.root_samples: list[Sample] = []
        self.requests: list[SampleRequest] = []
        self.counts: defaultdict[str, int] = defaultdict(int)
        self.release = asyncio.Event()
        self.two_started = asyncio.Event()
        self.waiting = asyncio.Event()
        self.active = 0
        self.max_active = 0
        self.attempts: defaultdict[str, int] = defaultdict(int)

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.requests.append(req)
        index = self.counts[req.model]
        self.counts[req.model] += 1
        if req.model == "root":
            if index == 1:
                await self.two_started.wait()
            if index >= len(self.root_samples):
                self.waiting.set()
                await asyncio.Event().wait()
            sample = self.root_samples[index]
        else:
            task_input = req.messages[0].content
            self.attempts[task_input] += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == 2:
                self.two_started.set()
            try:
                await self.release.wait()
                sample = Sample(text=f"done {task_input}")
            finally:
                self.active -= 1
        yield StreamEnd(
            text=sample.text,
            reasoning=[ReasoningBlock(text=sample.reasoning)] if sample.reasoning else [],
            tool_calls=sample.tool_calls,
            usage=sample.usage,
            finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
        )


class ModelProvider:
    provider_name = "model"

    def __init__(self, samples: dict[str, list[Sample]]) -> None:
        self.samples = samples

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        sample = self.samples[req.model].pop(0)
        yield StreamEnd(
            text=sample.text,
            reasoning=[ReasoningBlock(text=sample.reasoning)] if sample.reasoning else [],
            tool_calls=sample.tool_calls,
            usage=sample.usage,
            finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
        )


class KillProvider:
    provider_name = "kill"

    def __init__(self) -> None:
        self.root_samples: list[Sample] = []
        self.root_count = 0
        self.child_started = asyncio.Event()
        self.child_cancelled = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if req.model == "root":
            index = self.root_count
            self.root_count += 1
            if index == 1:
                await self.child_started.wait()
            sample = self.root_samples[index]
            yield StreamEnd(
                text=sample.text,
                tool_calls=sample.tool_calls,
                usage=sample.usage,
                finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
            )
            return
        self.child_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.child_cancelled.set()
            raise


class RemoteKillProvider:
    provider_name = "remote-kill"

    def __init__(self) -> None:
        self.root_samples: list[Sample] = []
        self.root_count = 0
        self.child_started = asyncio.Event()
        self.release_child = asyncio.Event()
        self.child_cancelled = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if req.model == "root":
            sample = self.root_samples[self.root_count]
            self.root_count += 1
        else:
            self.child_started.set()
            try:
                await self.release_child.wait()
            except asyncio.CancelledError:
                self.child_cancelled.set()
                raise
            sample = Sample(text="child boundary")
        yield StreamEnd(
            text=sample.text,
            tool_calls=sample.tool_calls,
            usage=sample.usage,
            finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
        )


class OutcomeTracer(NullTracer):
    def __init__(self) -> None:
        self.turns: list[dict[str, Any]] = []
        self.tools: list[tuple[Any, dict[str, Any]]] = []

    def end_turn(self, span: Any, **kwargs: Any) -> None:
        self.turns.append(kwargs)

    def start_tool(self, parent: Any, call: ToolCallRequested, **kwargs: Any) -> Any:
        return call.call_id

    def end_tool(self, span: Any, **kwargs: Any) -> None:
        self.tools.append((span, kwargs))


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    max_steps = 20
    subagents = [Worker]


def six_launches() -> list[ToolCall]:
    return [call("worker", f'{{"task":"job {index}"}}', f"launch-{index}") for index in range(6)]


def wait_calls(count: int) -> list[Sample]:
    return [Sample(tool_calls=[call("task_wait", "{}", f"wait-{index}")]) for index in range(count)]


async def test_six_tasks_are_async_bounded_inspectable_waited_and_return_results() -> None:
    store = MemoryStore()
    provider = RoutedProvider()
    harness = Harness(provider, store, [Root], max_concurrency=2)
    sid = (await harness.create_session(Root)).id
    ids = [task_id(sid, f"launch-{index}", 0) for index in range(6)]
    provider.root_samples = [
        Sample(tool_calls=six_launches()),
        Sample(
            tool_calls=[
                call("task_status", f'{{"task_id":"{ids[0]}","limit":100}}', "status-running"),
                call("task_status", f'{{"task_id":"{ids[-1]}","limit":1}}', "status-queued"),
                call("task_messages", f'{{"task_id":"{ids[0]}"}}', "messages-running"),
            ]
        ),
        *wait_calls(6),
        Sample(
            tool_calls=[
                call("task_result", f'{{"task_id":"{task}"}}', f"result-{index}") for index, task in enumerate(ids)
            ]
        ),
        Sample(text="root done"),
    ]

    running = asyncio.create_task(collect(harness.run(sid, "root work")))
    await asyncio.wait_for(provider.two_started.wait(), 1)
    while provider.counts["root"] < 2:
        await asyncio.sleep(0)
    states = [derive_task_state(await history(store, task)) for task in ids]
    assert states.count("running") == 2
    assert states.count("queued") == 4
    assert provider.counts["root"] == 2
    while not {
        "status-running",
        "status-queued",
        "messages-running",
    }.issubset({event.call_id for event in await history(store, sid) if isinstance(event, ToolCallCompleted)}):
        await asyncio.sleep(0)
    provider.release.set()
    events = await asyncio.wait_for(running, 2)

    assert provider.max_active == 2
    assert [event.child_session_id for event in picks(events, ChildSessionSpawned)] == ids
    parent_log = await history(store, sid)
    assert not any(isinstance(event, TurnStarted) and event.input.startswith("job ") for event in parent_log)
    status_running = next(event for event in picks(events, ToolCallCompleted) if event.call_id == "status-running")
    status_queued = next(event for event in picks(events, ToolCallCompleted) if event.call_id == "status-queued")
    assert status_running.result["state"] == "running"
    assert status_running.result["events"][0]["seq"] == 1
    assert not status_running.result["has_more"]
    assert status_queued.result["state"] == "queued"
    assert len(status_queued.result["events"]) == 1
    messages = next(event for event in picks(events, ToolCallCompleted) if event.call_id == "messages-running")
    assert messages.result == [{"role": "user", "content": "job 0"}]
    for index, task in enumerate(ids):
        result = next(event for event in picks(events, ToolCallCompleted) if event.call_id == f"result-{index}")
        assert result.result == {"task_id": task, "state": "completed", "text": f"done job {index}"}
    notices = [event for event in parent_log if isinstance(event, TaskNoticeQueued)]
    assert {event.task_session_id for event in notices} == set(ids)
    assert all(event.state == "completed" for event in notices)
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_abandonment_and_fresh_harness_recovery_keep_ids_and_cap() -> None:
    store = MemoryStore()
    opening_provider = RoutedProvider()
    opening = Harness(opening_provider, store, [Root], max_concurrency=2)
    sid = (await opening.create_session(Root)).id
    ids = [task_id(sid, f"launch-{index}", 0) for index in range(6)]
    opening_provider.root_samples = [Sample(tool_calls=six_launches())]

    stream = opening.run(sid, "root work")
    seen: list[Emitted] = []
    async for emitted in stream:
        seen.append(emitted)
        if opening_provider.two_started.is_set() and len(picks(seen, ChildSessionSpawned)) == 6:
            break
    await stream.aclose()
    assert [event.child_session_id for event in picks(seen, ChildSessionSpawned)] == ids
    assert len(await store.list(parent_id=sid)) == 6

    fresh_provider = RoutedProvider()
    fresh_provider.root_samples = [*wait_calls(6), Sample(text="recovered")]
    fresh = Harness(fresh_provider, store, [Root], max_concurrency=2)
    resumed = asyncio.create_task(collect(fresh.resume(sid)))
    await asyncio.wait_for(fresh_provider.two_started.wait(), 1)
    fresh_provider.release.set()
    events = await asyncio.wait_for(resumed, 2)

    assert fresh_provider.max_active == 2
    assert len(await store.list(parent_id=sid)) == 6
    assert not picks(events, ChildSessionSpawned)
    assert all(fresh_provider.attempts[f"job {index}"] == 1 for index in range(6))
    states = [derive_task_state(await history(store, task)) for task in ids]
    assert states == ["completed"] * 6
    assert picks(events, TurnCompleted)[-1].stop_reason == "completed"


async def test_child_ask_is_visible_and_answering_then_root_resume_reconciles_notice() -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        reply = await ctx.ask(FreeText(prompt="which?"))
        return f"picked {reply.text}"

    class Asker(Agent):
        model = "asker"
        tools = [confirm]

    class Manager(Agent):
        model = "manager"
        subagents = [Asker]

    store = MemoryStore()
    opening = Harness(
        ModelProvider(
            {
                "manager": [
                    Sample(tool_calls=[call("asker", '{"task":"choose"}', "launch")]),
                    Sample(tool_calls=[call("task_wait", "{}", "wait")]),
                ],
                "asker": [Sample(tool_calls=[call("confirm", "{}", "confirm")])],
            }
        ),
        store,
        [Manager],
    )
    sid = (await opening.create_session(Manager)).id
    stream = opening.run(sid, "go")
    seen: list[Emitted] = []
    async for emitted in stream:
        seen.append(emitted)
        if emitted.session_id != sid and isinstance(emitted.event, AskRaised):
            break
    await stream.aclose()
    child = next(item.session_id for item in seen if isinstance(item.event, AskRaised))
    raised = picks(seen, AskRaised)[0]
    assert derive_task_state(await history(store, child)) == "awaiting_input"

    child_harness = Harness(
        ModelProvider({"asker": [Sample(text="child done")]}),
        store,
        [Manager],
    )
    answered = await collect(child_harness.resume(child, raised.ask_id, FreeTextResponse(text="blue")))
    assert picks(answered, TurnCompleted)[0].stop_reason == "completed"

    fresh = Harness(
        ModelProvider(
            {
                "manager": [
                    Sample(tool_calls=[call("task_result", f'{{"task_id":"{child}"}}', "result")]),
                    Sample(text="manager done"),
                ]
            }
        ),
        store,
        [Manager],
    )
    finished = await collect(fresh.resume(sid))
    result = next(event for event in picks(finished, ToolCallCompleted) if event.call_id == "result")
    assert result.result == {"task_id": child, "state": "completed", "text": "child done"}
    notices = [event for event in await history(store, sid) if isinstance(event, TaskNoticeQueued)]
    assert len(notices) == 1


async def test_direct_child_resume_notifies_live_waiting_parent_once() -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        reply = await ctx.ask(FreeText(prompt="which?"))
        return f"picked {reply.text}"

    class Asker(Agent):
        model = "asker"
        tools = [confirm]

    class Manager(Agent):
        model = "manager"
        subagents = [Asker]

    store = MemoryStore()
    provider = ModelProvider(
        {
            "manager": [
                Sample(tool_calls=[call("asker", '{"task":"choose"}', "launch")]),
                Sample(tool_calls=[call("task_wait", "{}", "wait")]),
                Sample(text="manager done"),
            ],
            "asker": [
                Sample(tool_calls=[call("confirm", "{}", "confirm")]),
                Sample(text="child done"),
            ],
        }
    )
    harness = Harness(provider, store, [Manager], lease_ttl=0.3)
    sid = (await harness.create_session(Manager)).id
    parent = asyncio.create_task(collect(harness.run(sid, "go")))

    children: list[SessionHeader] = []
    while not children:
        children = await store.list(parent_id=sid)
        await asyncio.sleep(0)
    child = children[0].id
    child_log = await history(store, child)
    while not any(isinstance(event, AskRaised) for event in child_log):
        await asyncio.sleep(0)
        child_log = await history(store, child)
    parent_log = await history(store, sid)
    while not any(isinstance(event, ToolCallRequested) and event.call_id == "wait" for event in parent_log):
        await asyncio.sleep(0)
        parent_log = await history(store, sid)

    raised = next(event for event in child_log if isinstance(event, AskRaised))
    direct = await collect(harness.resume(child, raised.ask_id, FreeTextResponse(text="blue")))
    events = await asyncio.wait_for(parent, 2)
    notices = [event for event in await history(store, sid) if isinstance(event, TaskNoticeQueued)]

    assert len(notices) == 1
    assert notices[0].task_session_id == child
    assert len([item for item in events if isinstance(item.event, TaskNoticeQueued)]) == 1
    assert not [item for item in direct if isinstance(item.event, TaskNoticeQueued)]


@pytest.mark.parametrize("with_answer", [False, True])
async def test_killed_suspended_resume_never_replays_or_answers_the_ask(with_answer: bool) -> None:
    @tool
    async def confirm(ctx: Context) -> str:
        reply = await ctx.ask(FreeText(prompt="which?"))
        return reply.text

    class Suspended(Agent):
        model = "suspended"
        tools = [confirm]

    class Parent(Agent):
        model = "parent"
        subagents = [Suspended]

    store = MemoryStore()
    opening = Harness(
        FakeProvider([Sample(tool_calls=[call("confirm", "{}", "confirm")])]),
        store,
        [Parent],
    )
    root = await opening.create_session(Parent)
    child = SessionHeader(id="suspended-child", agent="suspended", parent_id=root.id, depth=1, task_input="choose")
    await store.create(child)
    await store.append(
        child.id,
        [SessionCreated(agent=child.agent, parent_id=root.id, depth=1)],
        expect_seq=0,
    )
    suspended = await collect(opening.run(child.id, "choose"))
    asked = picks(suspended, AskRaised)[0]
    killer = TaskSupervisor(opening, root.id)
    await killer.kill(root.id, "kill-suspended", child.id)

    seen: list[str] = []
    after: list[str] = []

    class Recorder(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            seen.append(emitted.event.type)

        async def after_turn(self, turn: Any, event: SessionEvent) -> None:
            after.append(event.type)

    fresh = Harness(FakeProvider([]), store, [Parent], hooks=[Recorder()])
    if with_answer:
        resumed = await collect(fresh.resume(child.id, asked.ask_id, FreeTextResponse(text="blue")))
    else:
        resumed = await collect(fresh.resume(child.id))

    stamped = [item async for item in store.read(child.id)]
    kill_seq = next(item.seq for item in stamped if isinstance(item.event, KillRequested))
    after_kill = [item.event for item in stamped if item.seq > kill_seq]
    assert not [event for event in after_kill if isinstance(event, AskRaised | AskAnswered)]
    terminals = [event for event in await history(store, child.id) if isinstance(event, TurnCompleted)]
    assert [event.stop_reason for event in terminals] == ["killed"]
    completed = next(event for event in after_kill if isinstance(event, ToolCallCompleted))
    assert (completed.call_id, completed.result, completed.is_error) == ("confirm", KILLED_RESULT, True)
    assert not picks(resumed, AskRaised)
    assert not picks(resumed, AskAnswered)
    assert [event.stop_reason for event in picks(resumed, TurnCompleted)] == ["killed"]
    notices = [event for event in await history(store, root.id) if isinstance(event, TaskNoticeQueued)]
    assert [(event.task_session_id, event.state) for event in notices] == [(child.id, "killed")]
    assert seen.count("turn_completed") == 1
    assert after == ["turn_completed"]
    header = await store.header(child.id)
    assert header is not None and header.status == "idle"
    await killer.close()


async def test_status_messages_result_bounds_lineage_and_redaction() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Root])
    root = (await harness.create_session(Root)).id
    child = SessionHeader(id="child", agent="worker", parent_id=root, depth=1, task_input="secret")
    grandchild = SessionHeader(id="grand", agent="worker", parent_id="child", depth=2, task_input="nested")
    outsider = (await harness.create_session(Root)).id
    for header in (child, grandchild):
        await store.create(header)
        await store.append(
            header.id,
            [
                SessionCreated(agent=header.agent, parent_id=header.parent_id, depth=header.depth),
                TurnStarted(turn_id=f"turn-{header.id}", input=header.task_input or ""),
                SampleStarted(turn_id=f"turn-{header.id}", sample_id="sample", model="worker"),
                ReasoningPart(sample_id="sample", text="hidden"),
                TextPart(sample_id="sample", text="visible"),
                TurnCompleted(turn_id=f"turn-{header.id}", stop_reason="completed"),
            ],
            expect_seq=0,
        )
    supervisor = TaskSupervisor(harness, root)

    status = await supervisor.status(root, "grand", 1, 2)
    assert status["parent"] == "child"
    assert status["depth"] == 2
    assert [event["seq"] for event in status["events"]] == [2, 3]
    assert status["next_seq"] == 3
    assert status["has_more"]
    messages = await supervisor.messages(root, "grand", 100)
    assert messages == [
        {"role": "user", "content": "nested"},
        {"role": "assistant", "text": "visible", "tool_calls": []},
    ]
    assert await supervisor.result(root, "grand") == {
        "task_id": "grand",
        "state": "completed",
        "text": "visible",
    }
    for bad in (0, 101):
        with pytest.raises(TantraError, match="between 1 and 100"):
            await supervisor.status(root, "child", None, bad)
        with pytest.raises(TantraError, match="between 1 and 100"):
            await supervisor.messages(root, "child", bad)
    with pytest.raises(TantraError, match="not a descendant"):
        await supervisor.status(outsider, "child", None, 20)


async def test_send_notify_bounds_authority_order_and_replay_deduplication() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Root])
    root = (await harness.create_session(Root)).id
    child = SessionHeader(id="child", agent="worker", parent_id=root, depth=1, task_input="child")
    leaf = SessionHeader(id="leaf", agent="worker", parent_id=child.id, depth=2, task_input="leaf")
    outsider = (await harness.create_session(Root)).id
    for header in (child, leaf):
        await store.create(header)
        await store.append(
            header.id,
            [SessionCreated(agent=header.agent, parent_id=header.parent_id, depth=header.depth)],
            expect_seq=0,
        )
    supervisor = TaskSupervisor(harness, root)

    first = await supervisor.send(root, "send-1", leaf.id, "  keep spacing  ")
    replay = await supervisor.send(root, "send-1", leaf.id, "  keep spacing  ")
    second = await supervisor.send(root, "send-2", leaf.id, "second")
    notified = await supervisor.notify_parent(leaf.id, "notify", "ready")

    assert first == replay == message_id(root, "send-1")
    assert second == message_id(root, "send-2")
    assert notified == message_id(leaf.id, "notify")
    leaf_messages = [event for event in await history(store, leaf.id) if isinstance(event, AgentMessageQueued)]
    assert [(event.message_id, event.source, event.text) for event in leaf_messages] == [
        (message_id(root, "send-1"), "parent", "  keep spacing  "),
        (message_id(root, "send-2"), "parent", "second"),
    ]
    delivered = [
        message.content for message in build_messages(await history(store, leaf.id)) if hasattr(message, "content")
    ]
    assert "\n".join(delivered).count("keep spacing") == 1
    child_messages = [event for event in await history(store, child.id) if isinstance(event, AgentMessageQueued)]
    assert [(event.message_id, event.source, event.text) for event in child_messages] == [
        (message_id(leaf.id, "notify"), "child", "ready")
    ]
    assert not [event for event in await history(store, root) if isinstance(event, AgentMessageQueued)]
    for invalid in ("", "   ", "x" * 32_769):
        with pytest.raises(TantraError, match="blank|exceeds"):
            await supervisor.send(root, "invalid", leaf.id, invalid)
    with pytest.raises(TantraError, match="not a descendant"):
        await supervisor.send(outsider, "outside", leaf.id, "no")
    with pytest.raises(TantraError, match="root session"):
        await supervisor.notify_parent(root, "root-notify", "no")


async def test_task_kill_cancels_an_active_provider_and_completes_durably() -> None:
    seen: list[tuple[str, str]] = []

    class Recorder(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            seen.append((emitted.session_id, emitted.event.type))

    store = MemoryStore()
    provider = KillProvider()
    tracer = OutcomeTracer()
    harness = Harness(provider, store, [Root], hooks=[Recorder()], telemetry=tracer)
    sid = (await harness.create_session(Root)).id
    child = task_id(sid, "launch", 0)
    provider.root_samples = [
        Sample(tool_calls=[call("worker", '{"task":"stay busy"}', "launch")]),
        Sample(tool_calls=[call("task_kill", f'{{"task_id":"{child}"}}', "kill")]),
        Sample(text="root done"),
    ]

    events = await asyncio.wait_for(collect(harness.run(sid, "go")), 2)
    child_events = await history(store, child)

    assert provider.child_cancelled.is_set()
    assert [event.request_id for event in child_events if isinstance(event, KillRequested)] == [kill_id(sid, "kill")]
    assert [event.stop_reason for event in child_events if isinstance(event, TurnCompleted)] == ["killed"]
    assert not [event for event in child_events if isinstance(event, TurnFailed)]
    killed = next(event for event in picks(events, ToolCallCompleted) if event.call_id == "kill")
    assert killed.result == {"task_id": child, "state": "killed"}
    notices = [event for event in await history(store, sid) if isinstance(event, TaskNoticeQueued)]
    assert [(event.task_session_id, event.state) for event in notices] == [(child, "killed")]
    assert seen.count((child, "kill_requested")) == 1
    assert seen.count((child, "turn_completed")) == 1
    assert seen.count((sid, "task_notice_queued")) == 1
    killed_turn = next(turn for turn in tracer.turns if turn["stop_reason"] == "killed")
    assert killed_turn["outcome"] == "cancelled"


async def test_killing_waiter_does_not_reacquire_a_permit_held_by_its_sibling() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Root], max_concurrency=1, lease_ttl=0.3)
    root = await harness.create_session(Root)
    waiter = SessionHeader(id="waiter", agent="worker", parent_id=root.id, depth=1, task_input="wait")
    target = SessionHeader(id="target", agent="worker", parent_id=waiter.id, depth=2, task_input="target")
    for header in (waiter, target):
        await store.create(header)
        await store.append(
            header.id,
            [
                SessionCreated(agent=header.agent, parent_id=header.parent_id, depth=header.depth),
                TurnStarted(turn_id=f"turn-{header.id}", input=header.task_input or ""),
            ],
            expect_seq=0,
        )
    supervisor = TaskSupervisor(harness, root.id)
    await supervisor.gate.acquire()
    supervisor.held.add(waiter.id)
    assert await store.acquire_lease(waiter.id, "holder", 1)
    waiting = asyncio.create_task(supervisor.wait(waiter.id, "holder", [target.id]))
    supervisor.runners[waiter.id] = waiting
    while waiter.id in supervisor.held:
        await asyncio.sleep(0)
    await supervisor.gate.acquire()

    await supervisor.kill(root.id, "kill-waiter", waiter.id)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(waiting, 0.2)

    supervisor.gate.release()
    await supervisor.close()


async def test_kill_reports_completion_that_wins_the_append_race() -> None:
    class CompletionRaceStore(MemoryStore):
        target = "child"
        raced = False

        async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
            if sid == self.target and not self.raced and any(isinstance(event, KillRequested) for event in events):
                self.raced = True
                await super().append(
                    sid,
                    [TurnCompleted(turn_id="turn-child", stop_reason="completed")],
                    expect_seq=expect_seq,
                )
            return await super().append(sid, events, expect_seq=expect_seq)

    store = CompletionRaceStore()
    harness = Harness(FakeProvider([]), store, [Root])
    root = await harness.create_session(Root)
    child = SessionHeader(id="child", agent="worker", parent_id=root.id, depth=1, task_input="child")
    await store.create(child)
    await store.append(
        child.id,
        [
            SessionCreated(agent=child.agent, parent_id=root.id, depth=1),
            TurnStarted(turn_id="turn-child", input="child"),
        ],
        expect_seq=0,
    )
    supervisor = TaskSupervisor(harness, root.id)

    with pytest.raises(TantraError, match="completed"):
        await supervisor.kill(root.id, "kill", child.id)

    assert not [event for event in await history(store, child.id) if isinstance(event, KillRequested)]


async def test_kill_finalizes_run_and_resume_setup_windows() -> None:
    entered = asyncio.Event()
    after: list[str] = []

    class BlockingHook(Hook):
        async def before_turn(self, turn: Any) -> None:
            entered.set()
            await asyncio.Event().wait()

        async def after_turn(self, turn: Any, event: SessionEvent) -> None:
            after.append(event.type)

    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Root], hooks=[BlockingHook()])
    root = await harness.create_session(Root)
    child = SessionHeader(id="run-child", agent="worker", parent_id=root.id, depth=1, task_input="child")
    await store.create(child)
    await store.append(
        child.id,
        [SessionCreated(agent=child.agent, parent_id=root.id, depth=1)],
        expect_seq=0,
    )
    supervisor = TaskSupervisor(harness, root.id)
    await supervisor.start()
    await asyncio.wait_for(entered.wait(), 1)
    await supervisor.kill(root.id, "kill-run", child.id)
    await asyncio.wait_for(supervisor.runners[child.id], 1)

    assert [event.stop_reason for event in await history(store, child.id) if isinstance(event, TurnCompleted)] == [
        "killed"
    ]
    assert after == ["turn_completed"]
    await supervisor.close()

    resumed = asyncio.Event()

    async def deps(header: SessionHeader) -> None:
        if header.id == "resume-child":
            resumed.set()
            await asyncio.Event().wait()

    resume_harness = Harness(FakeProvider([]), store, [Root], deps_factory=deps)
    resume_child = SessionHeader(
        id="resume-child",
        agent="worker",
        parent_id=root.id,
        depth=1,
        task_input="resume",
    )
    await store.create(resume_child)
    await store.append(
        resume_child.id,
        [
            SessionCreated(agent=resume_child.agent, parent_id=root.id, depth=1),
            TurnStarted(turn_id="resume-turn", input="resume"),
        ],
        expect_seq=0,
    )
    resume_supervisor = TaskSupervisor(resume_harness, root.id)
    await resume_supervisor.start()
    await asyncio.wait_for(resumed.wait(), 1)
    await resume_supervisor.kill(root.id, "kill-resume", resume_child.id)
    await asyncio.wait_for(resume_supervisor.runners[resume_child.id], 1)

    assert [
        event.stop_reason for event in await history(store, resume_child.id) if isinstance(event, TurnCompleted)
    ] == ["killed"]
    await resume_supervisor.close()


async def test_root_stream_drains_killed_tool_cleanup_completion_and_aborted_span() -> None:
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    @tool
    async def hold() -> str:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            await release_cleanup.wait()
            raise

    class ToolWorker(Agent):
        model = "tool-worker"
        tools = [hold]

    class ToolRoot(Agent):
        model = "tool-root"
        subagents = [ToolWorker]

    class Provider:
        provider_name = "cleanup"

        def __init__(self) -> None:
            self.counts: defaultdict[str, int] = defaultdict(int)
            self.child = ""

        def limits(self, model: str) -> ModelLimits:
            return ModelLimits(context_window=1_000_000, max_output=64_000)

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            index = self.counts[req.model]
            self.counts[req.model] += 1
            if req.model == "tool-root":
                if index == 0:
                    sample = Sample(tool_calls=[call("tool_worker", '{"task":"hold"}', "launch")])
                elif index == 1:
                    await started.wait()
                    sample = Sample(tool_calls=[call("task_kill", f'{{"task_id":"{self.child}"}}', "kill")])
                else:
                    sample = Sample(text="root done")
            else:
                sample = Sample(tool_calls=[call("hold", "{}", "hold")])
            yield StreamEnd(
                text=sample.text,
                tool_calls=sample.tool_calls,
                usage=sample.usage,
                finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
            )

    store = MemoryStore()
    provider = Provider()
    tracer = OutcomeTracer()
    harness = Harness(provider, store, [ToolRoot], telemetry=tracer)
    sid = (await harness.create_session(ToolRoot)).id
    provider.child = task_id(sid, "launch", 0)
    running = asyncio.create_task(collect(harness.run(sid, "go")))
    await asyncio.wait_for(cleanup_started.wait(), 1)
    while not any(isinstance(event, TurnCompleted) for event in await history(store, sid)):
        await asyncio.sleep(0)

    assert not running.done()
    release_cleanup.set()
    events = await asyncio.wait_for(running, 1)

    assert [
        event.stop_reason for event in await history(store, provider.child) if isinstance(event, TurnCompleted)
    ] == ["killed"]
    emitted_terminals = [
        item.event.stop_reason
        for item in events
        if item.session_id == provider.child and isinstance(item.event, TurnCompleted)
    ]
    assert emitted_terminals == ["killed"]
    hold_span = next(kwargs for span, kwargs in tracer.tools if span == "hold")
    assert hold_span["outcome"] == "aborted"
    killed_turn = next(turn for turn in tracer.turns if turn["stop_reason"] == "killed")
    assert killed_turn["outcome"] == "cancelled"


async def test_kill_scans_recursive_descendants_again_after_target_append() -> None:
    class RaceStore(MemoryStore):
        target_id = "target"
        injected = False
        kill_order: list[str] = []

        async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
            killing_target = sid == self.target_id and any(isinstance(event, KillRequested) for event in events)
            if killing_target and not self.injected:
                self.injected = True
                raced = SessionHeader(
                    id="raced",
                    agent="worker",
                    parent_id=self.target_id,
                    depth=2,
                    task_input="raced",
                )
                await self.create(raced)
                await super().append(
                    raced.id,
                    [SessionCreated(agent=raced.agent, parent_id=raced.parent_id, depth=raced.depth)],
                    expect_seq=0,
                )
            last = await super().append(sid, events, expect_seq=expect_seq)
            if any(isinstance(event, KillRequested) for event in events):
                self.kill_order.append(sid)
            return last

    store = RaceStore()
    harness = Harness(FakeProvider([]), store, [Root])
    root = await harness.create_session(Root)
    target = SessionHeader(id="target", agent="worker", parent_id=root.id, depth=1, task_input="target")
    deep = SessionHeader(id="deep", agent="worker", parent_id=target.id, depth=2, task_input="deep")
    for header in (target, deep):
        await store.create(header)
        await store.append(
            header.id,
            [
                SessionCreated(agent=header.agent, parent_id=header.parent_id, depth=header.depth),
                TurnStarted(turn_id=f"turn-{header.id}", input=header.task_input or ""),
            ],
            expect_seq=0,
        )
    supervisor = TaskSupervisor(harness, root.id)

    assert await supervisor.kill(root.id, "kill-tree", target.id) == {"task_id": target.id, "state": "killed"}

    assert store.kill_order == [deep.id, target.id, "raced"]
    for sid in (target.id, deep.id, "raced"):
        assert [event.request_id for event in await history(store, sid) if isinstance(event, KillRequested)] == [
            kill_id(root.id, "kill-tree")
        ]


async def test_nested_correction_skips_stale_call_and_notify_wakes_only_direct_parent() -> None:
    gate_started = asyncio.Event()
    stale_calls: list[str] = []

    @tool
    async def gate(ctx: Context) -> str:
        gate_started.set()
        while True:
            if any(isinstance(event, AgentMessageQueued) for event in await history(ctx.store, ctx.session_id)):
                return "corrected"
            await asyncio.sleep(0)

    @tool
    async def stale() -> str:
        stale_calls.append("called")
        return "stale"

    class Leaf(Agent):
        model = "leaf"
        tools = [gate, stale]

    class Mid(Agent):
        model = "mid"
        subagents = [Leaf]

    class Manager(Agent):
        model = "manager"
        subagents = [Mid]
        max_steps = 10

    class Provider:
        provider_name = "nested-messages"

        def __init__(self) -> None:
            self.counts: defaultdict[str, int] = defaultdict(int)
            self.leaf = ""
            self.requests: list[SampleRequest] = []

        def limits(self, model: str) -> ModelLimits:
            return ModelLimits(context_window=1_000_000, max_output=64_000)

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            self.requests.append(req)
            index = self.counts[req.model]
            self.counts[req.model] += 1
            if req.model == "manager":
                if index == 0:
                    sample = Sample(tool_calls=[call("mid", '{"task":"delegate"}', "launch-mid")])
                elif index == 1:
                    await gate_started.wait()
                    args = json.dumps({"task_id": self.leaf, "message": "use the corrected plan"})
                    sample = Sample(tool_calls=[call("task_send", args, "correct-leaf")])
                elif index == 2:
                    sample = Sample(tool_calls=[call("task_wait", "{}", "root-wait")])
                else:
                    sample = Sample(text="root done")
            elif req.model == "mid":
                if index == 0:
                    sample = Sample(tool_calls=[call("leaf", '{"task":"work"}', "launch-leaf")])
                elif index == 1:
                    sample = Sample(tool_calls=[call("task_wait", "{}", "mid-wait")])
                else:
                    sample = Sample(text="mid done")
            elif index == 0:
                sample = Sample(
                    tool_calls=[
                        call("gate", "{}", "gate"),
                        call("stale", "{}", "stale"),
                    ]
                )
            elif index == 1:
                sample = Sample(tool_calls=[call("notify_parent", '{"message":"correction applied"}', "notify")])
            else:
                sample = Sample(text="leaf done")
            yield StreamEnd(
                text=sample.text,
                tool_calls=sample.tool_calls,
                usage=sample.usage,
                finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
            )

    store = MemoryStore()
    provider = Provider()
    harness = Harness(provider, store, [Manager], max_depth=2, lease_ttl=0.3)
    sid = (await harness.create_session(Manager)).id
    mid = task_id(sid, "launch-mid", 0)
    provider.leaf = task_id(mid, "launch-leaf", 0)

    await asyncio.wait_for(collect(harness.run(sid, "go")), 2)

    leaf_log = await history(store, provider.leaf)
    stale_result = next(
        event for event in leaf_log if isinstance(event, ToolCallCompleted) and event.call_id == "stale"
    )
    assert stale_result.result == INBOX_RESULT
    assert stale_result.is_error
    assert not stale_calls
    mid_messages = [event for event in await history(store, mid) if isinstance(event, AgentMessageQueued)]
    assert [(event.sender_session_id, event.text) for event in mid_messages] == [(provider.leaf, "correction applied")]
    assert not [event for event in await history(store, sid) if isinstance(event, AgentMessageQueued)]
    root_log = await history(store, sid)
    mid_notice = next(index for index, event in enumerate(root_log) if isinstance(event, TaskNoticeQueued))
    root_wait = next(
        index
        for index, event in enumerate(root_log)
        if isinstance(event, ToolCallCompleted) and event.call_id == "root-wait"
    )
    assert mid_notice < root_wait
    correction_requests = [req for req in provider.requests if req.model == "leaf"][1:]
    delivered = (
        "\n".join(message.content for message in req.messages if hasattr(message, "content"))
        for req in correction_requests
    )
    assert all(text.count("use the corrected plan") == 1 for text in delivered)
    leaf_messages = [event for event in leaf_log if isinstance(event, AgentMessageQueued)]
    assert len(leaf_messages) == 1


async def test_save_and_exit_message_yields_retrievable_structured_output() -> None:
    ready = asyncio.Event()

    class Saved(BaseModel):
        status: str
        count: int

    @tool
    async def wait_for_message(ctx: Context) -> str:
        ready.set()
        while True:
            if any(isinstance(event, AgentMessageQueued) for event in await history(ctx.store, ctx.session_id)):
                return "message received"
            await asyncio.sleep(0)

    class Saver(Agent):
        model = "saver"
        tools = [wait_for_message]
        output_schema = Saved

    class SaveRoot(Agent):
        model = "save-root"
        subagents = [Saver]
        max_steps = 10

    class Provider:
        provider_name = "save"

        def __init__(self) -> None:
            self.counts: defaultdict[str, int] = defaultdict(int)
            self.child = ""
            self.requests: list[SampleRequest] = []

        def limits(self, model: str) -> ModelLimits:
            return ModelLimits(context_window=1_000_000, max_output=64_000)

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            self.requests.append(req)
            index = self.counts[req.model]
            self.counts[req.model] += 1
            if req.model == "save-root":
                if index == 0:
                    sample = Sample(tool_calls=[call("saver", '{"task":"draft"}', "launch")])
                elif index == 1:
                    await ready.wait()
                    args = json.dumps({"task_id": self.child, "message": "save-and-exit"})
                    sample = Sample(tool_calls=[call("task_send", args, "save")])
                elif index == 2:
                    sample = Sample(tool_calls=[call("task_wait", "{}", "wait")])
                elif index == 3:
                    sample = Sample(tool_calls=[call("task_result", json.dumps({"task_id": self.child}), "result")])
                else:
                    sample = Sample(text="done")
            elif index == 0:
                sample = Sample(tool_calls=[call("wait_for_message", "{}", "hold")])
            else:
                sample = Sample(tool_calls=[call("submit_output", '{"status":"saved","count":2}', "output")])
            yield StreamEnd(
                text=sample.text,
                tool_calls=sample.tool_calls,
                usage=sample.usage,
                finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
            )

    store = MemoryStore()
    provider = Provider()
    harness = Harness(provider, store, [SaveRoot])
    sid = (await harness.create_session(SaveRoot)).id
    provider.child = task_id(sid, "launch", 0)

    events = await asyncio.wait_for(collect(harness.run(sid, "go")), 2)

    result = next(event for event in picks(events, ToolCallCompleted) if event.call_id == "result")
    assert result.result == {
        "task_id": provider.child,
        "state": "completed",
        "output": {"status": "saved", "count": 2},
    }
    saver_request = [req for req in provider.requests if req.model == "saver"][1]
    delivered = [message.content for message in saver_request.messages if hasattr(message, "content")]
    assert "\n".join(delivered).count("save-and-exit") == 1


async def test_task_kill_reaps_a_bash_process_group(tmp_path: Any) -> None:
    pid_file = tmp_path / "integrated-child.pid"

    class ShellWorker(Agent):
        model = "shell-worker"
        tools = [bash()]
        permissions = {"bash": "allow"}

    class ShellRoot(Agent):
        model = "shell-root"
        subagents = [ShellWorker]

    class Provider:
        provider_name = "shell-kill"

        def __init__(self) -> None:
            self.counts: defaultdict[str, int] = defaultdict(int)
            self.child = ""

        def limits(self, model: str) -> ModelLimits:
            return ModelLimits(context_window=1_000_000, max_output=64_000)

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            index = self.counts[req.model]
            self.counts[req.model] += 1
            if req.model == "shell-root":
                if index == 0:
                    sample = Sample(tool_calls=[call("shell_worker", '{"task":"run"}', "launch")])
                elif index == 1:
                    while not pid_file.exists():
                        await asyncio.sleep(0.01)
                    sample = Sample(tool_calls=[call("task_kill", json.dumps({"task_id": self.child}), "kill")])
                else:
                    sample = Sample(text="done")
            else:
                command = f"sh -c 'sleep 30 & echo $! > {pid_file}; wait'"
                sample = Sample(tool_calls=[call("bash", json.dumps({"command": command}), "bash")])
            yield StreamEnd(
                text=sample.text,
                tool_calls=sample.tool_calls,
                usage=sample.usage,
                finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
            )

    store = MemoryStore()
    provider = Provider()
    harness = Harness(provider, store, [ShellRoot])
    sid = (await harness.create_session(ShellRoot)).id
    provider.child = task_id(sid, "launch", 0)

    events = await asyncio.wait_for(collect(harness.run(sid, "go")), 3)
    child_pid = int(pid_file.read_text())

    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)
    child_terminals = [
        item.event.stop_reason
        for item in events
        if item.session_id == provider.child and isinstance(item.event, TurnCompleted)
    ]
    assert child_terminals == ["killed"]


async def test_queued_kill_never_starts_and_running_sibling_continues() -> None:
    store = MemoryStore()
    provider = RoutedProvider()
    harness = Harness(provider, store, [Root], max_concurrency=1)
    root = await harness.create_session(Root)
    supervisor = TaskSupervisor(harness, root.id)
    first = await supervisor.create(root, "worker", "first", 0, "first")
    second = await supervisor.create(root, "worker", "second", 0, "second")
    supervisor.launch(first.task_id, None)
    supervisor.launch(second.task_id, None)
    while provider.attempts["first"] < 1:
        await asyncio.sleep(0)

    assert await supervisor.kill(root.id, "kill-second", second.task_id) == {
        "task_id": second.task_id,
        "state": "killed",
    }
    provider.release.set()
    await asyncio.wait_for(supervisor.runners[first.task_id], 2)
    with pytest.raises(asyncio.CancelledError):
        await supervisor.runners[second.task_id]

    assert derive_task_state(await history(store, first.task_id)) == "completed"
    assert derive_task_state(await history(store, second.task_id)) == "killed"
    assert not [event for event in await history(store, second.task_id) if isinstance(event, TurnStarted)]
    assert provider.attempts["second"] == 0
    assert await supervisor.kill(root.id, "kill-again", second.task_id) == {
        "task_id": second.task_id,
        "state": "killed",
    }
    with pytest.raises(TantraError, match="completed"):
        await supervisor.kill(root.id, "kill-first", first.task_id)
    await supervisor.close()


async def test_fresh_supervisor_remote_kill_takes_effect_at_next_boundary() -> None:
    store = MemoryStore()
    provider = RemoteKillProvider()
    harness = Harness(provider, store, [Root], lease_ttl=0.3)
    sid = (await harness.create_session(Root)).id
    child = task_id(sid, "launch", 0)
    provider.root_samples = [
        Sample(tool_calls=[call("worker", '{"task":"remote"}', "launch")]),
        Sample(tool_calls=[call("task_wait", "{}", "wait")]),
        Sample(text="root done"),
    ]
    running = asyncio.create_task(collect(harness.run(sid, "go")))
    await asyncio.wait_for(provider.child_started.wait(), 1)
    while not any(
        isinstance(event, ToolCallRequested) and event.call_id == "wait" for event in await history(store, sid)
    ):
        await asyncio.sleep(0)

    remote = TaskSupervisor(harness, sid)
    await remote.kill(sid, "remote", child)
    assert not provider.child_cancelled.is_set()
    provider.release_child.set()
    await asyncio.wait_for(running, 2)

    child_events = await history(store, child)
    assert [event.stop_reason for event in child_events if isinstance(event, TurnCompleted)] == ["killed"]
    assert not provider.child_cancelled.is_set()
    await remote.close()


async def test_generated_tools_refs_validation_reserved_names_and_hook_forwarding() -> None:
    @tool
    async def launch(ctx: Context) -> TaskRef:
        return await ctx.spawn(Worker, "one")

    class CustomRoot(Agent):
        model = "root"
        max_steps = 10
        tools = [launch]
        subagents = [Worker]

    seen: list[tuple[str, int]] = []

    class Recorder(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            if emitted.seq is not None:
                seen.append((emitted.session_id, emitted.seq))

    provider = RoutedProvider()
    provider.root_samples = [
        Sample(tool_calls=[call("launch", "{}", "custom")]),
        *wait_calls(9),
    ]
    provider.release.set()
    provider.two_started.set()
    store = MemoryStore()
    harness = Harness(provider, store, [CustomRoot], max_concurrency=1, hooks=[Recorder()])
    sid = (await harness.create_session(CustomRoot)).id
    events = await asyncio.wait_for(collect(harness.run(sid, "go")), 2)
    launched = next(event for event in picks(events, ToolCallCompleted) if event.call_id == "custom")
    assert isinstance(launched.result, TaskRef)
    generated = harness.tools["custom_root"]["worker"]
    assert generated.schema.parameters["properties"]["task"]["type"] == "string"
    builtins = {entry.name for entry in harness.tools["custom_root"].values()} - {"launch", "worker"}
    assert TASK_TOOL_NAMES.issuperset(builtins)
    assert len(seen) == len(set(seen))

    with pytest.raises(TantraError, match="max_concurrency"):
        Harness(FakeProvider([]), MemoryStore(), [Root], max_concurrency=0)

    for name in TASK_TOOL_NAMES | {"submit_output"}:
        reserved = tool(lambda: None, name=name)

        class Bad(Agent):
            tools = [reserved]

        with pytest.raises(TantraError, match="reserved tool name"):
            Harness(FakeProvider([]), MemoryStore(), [Bad])


def test_task_messages_use_provider_visible_shapes() -> None:
    events: list[SessionEvent] = [
        TurnStarted(turn_id="turn", input="go"),
        SampleStarted(turn_id="turn", sample_id="sample", model="worker"),
        ReasoningPart(sample_id="sample", text="hidden"),
        TextPart(sample_id="sample", text="shown"),
        ToolCallRequested(sample_id="sample", call_id="call", name="x", args={"q": 1}),
        ToolCallCompleted(call_id="call", result="ok"),
    ]
    visible = [
        message.model_dump(mode="json", exclude={"reasoning"})
        if message.role == "assistant"
        else message.model_dump(mode="json")
        for message in build_messages(events)
    ]
    assert visible == [
        {"role": "user", "content": "go"},
        {
            "role": "assistant",
            "text": "shown",
            "tool_calls": [{"type": "tool_call", "id": "call", "name": "x", "args": '{"q": 1}'}],
        },
        {"role": "tool", "call_id": "call", "content": "ok", "is_error": False},
    ]
