from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any

import pytest

from tantra import Agent, Context, Harness, TantraError, TaskRef, tool
from tantra.adapters.collect import collect
from tantra.ask import FreeText, FreeTextResponse
from tantra.context import build_messages
from tantra.events import (
    AskRaised,
    ChildSessionSpawned,
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
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.loop import Emitted
from tantra.providers.base import ModelLimits, ProviderEvent, ReasoningBlock, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tasking import TASK_TOOL_NAMES, TaskSupervisor, derive_task_state, task_id


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
