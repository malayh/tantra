from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator

from sarathi.agent import Sarathi, deps_factory
from sarathi.api.ws import Connection, ConnectionHub
from tantra import Emitted, Harness, MemoryStore
from tantra.adapters.collect import collect
from tantra.events import (
    AgentMessageQueued,
    ChildSessionSpawned,
    KillRequested,
    TaskNoticeQueued,
    ToolCallCompleted,
    TurnStarted,
)
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.tasking import derive_task_state, task_id


def call(name: str, args: dict[str, object], call_id: str) -> ToolCall:
    return ToolCall(id=call_id, name=name, args=json.dumps(args))


class LifecycleProvider:
    provider_name = "sarathi-lifecycle"

    def __init__(self, root_id: str) -> None:
        self.researchers = [task_id(root_id, f"launch-{index}", 0) for index in range(3)]
        self.investigator = task_id(self.researchers[0], "launch-investigator", 0)
        self.counts: defaultdict[tuple[str, str], int] = defaultdict(int)
        self.release = asyncio.Event()
        self.two_started = asyncio.Event()
        self.third_started = asyncio.Event()
        self.active = 0
        self.max_active = 0
        self.cancelled: set[str] = set()

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    def role(self, req: SampleRequest) -> str:
        prompt = req.system[0].text
        if "You are Sarathi" in prompt:
            return "root"
        if "You are a research subagent" in prompt:
            return "researcher"
        return "investigator"

    async def blocked_researcher(self, task: str) -> StreamEnd:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.active == 2:
            self.two_started.set()
        if task == "job 2":
            self.third_started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.add(task)
            raise
        finally:
            self.active -= 1
        return StreamEnd(text=f"draft {task}", finish_reason="stop")

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        role = self.role(req)
        task = req.messages[0].content
        key = role, task
        index = self.counts[key]
        self.counts[key] += 1
        if role == "root":
            sample = await self.root_sample(index)
        elif role == "researcher" and index == 0:
            sample = await self.blocked_researcher(task)
        elif role == "researcher" and task == "job 0":
            sample = self.primary_researcher_sample(index)
        elif role == "investigator" and index == 0:
            sample = StreamEnd(
                tool_calls=[call("notify_parent", {"message": "verified source"}, "notify-researcher")],
                finish_reason="tool_calls",
            )
        elif role == "investigator":
            sample = StreamEnd(text="investigator done", finish_reason="stop")
        else:
            sample = StreamEnd(text=f"done {task}", finish_reason="stop")
        yield sample

    async def root_sample(self, index: int) -> StreamEnd:
        if index == 0:
            return StreamEnd(
                tool_calls=[call("researcher", {"task": f"job {number}"}, f"launch-{number}") for number in range(3)],
                finish_reason="tool_calls",
            )
        if index == 1:
            await self.two_started.wait()
            return StreamEnd(
                tool_calls=[
                    call("task_status", {"task_id": self.researchers[0]}, "status-running"),
                    call("task_status", {"task_id": self.researchers[2]}, "status-queued"),
                    call("task_messages", {"task_id": self.researchers[0]}, "messages-running"),
                ],
                finish_reason="tool_calls",
            )
        if index == 2:
            return StreamEnd(
                tool_calls=[
                    call("task_send", {"task_id": self.researchers[0], "message": "focus on standards"}, "send")
                ],
                finish_reason="tool_calls",
            )
        if index == 3:
            return StreamEnd(
                tool_calls=[call("task_kill", {"task_id": self.researchers[1]}, "kill")],
                finish_reason="tool_calls",
            )
        if index in {4, 5}:
            return StreamEnd(
                tool_calls=[call("task_wait", {"task_ids": [self.researchers[0]]}, f"wait-{index}")],
                finish_reason="tool_calls",
            )
        if index == 6:
            return StreamEnd(
                tool_calls=[call("task_result", {"task_id": self.researchers[0]}, "result-0")],
                finish_reason="tool_calls",
            )
        if index == 7:
            return StreamEnd(
                tool_calls=[call("task_wait", {"task_ids": [self.researchers[2]]}, "wait-2")],
                finish_reason="tool_calls",
            )
        if index == 8:
            return StreamEnd(
                tool_calls=[
                    call("task_result", {"task_id": self.researchers[2]}, "result-2"),
                    call("task_result", {"task_id": self.researchers[1]}, "result-killed"),
                ],
                finish_reason="tool_calls",
            )
        return StreamEnd(text="root done", finish_reason="stop")

    def primary_researcher_sample(self, index: int) -> StreamEnd:
        if index == 1:
            return StreamEnd(
                tool_calls=[call("investigator", {"task": "verify source"}, "launch-investigator")],
                finish_reason="tool_calls",
            )
        if index == 2:
            return StreamEnd(
                tool_calls=[
                    call("task_status", {"task_id": self.investigator}, "leaf-status"),
                    call("task_messages", {"task_id": self.investigator}, "leaf-messages"),
                    call("task_wait", {"task_ids": [self.investigator]}, "leaf-wait"),
                ],
                finish_reason="tool_calls",
            )
        if index == 3:
            return StreamEnd(
                tool_calls=[
                    call("task_result", {"task_id": self.investigator}, "leaf-result"),
                    call("notify_parent", {"message": "research complete"}, "notify-root"),
                ],
                finish_reason="tool_calls",
            )
        return StreamEnd(text="researcher done", finish_reason="stop")


async def history(store: MemoryStore, session_id: str) -> list[object]:
    return [stamped.event async for stamped in store.read(session_id)]


async def test_sarathi_full_async_lifecycle_is_bounded_nested_and_replay_stable() -> None:
    store = MemoryStore()
    bootstrap = Harness(LifecycleProvider("seed"), store, [Sarathi], default_model="test-model")
    root = await bootstrap.create_session(Sarathi, {"user": "1"})
    provider = LifecycleProvider(root.id)
    harness = Harness(
        provider,
        store,
        [Sarathi],
        default_model="test-model",
        deps_factory=deps_factory,
        max_concurrency=2,
    )
    running = asyncio.create_task(collect(harness.run(root.id, "go")))
    await asyncio.wait_for(provider.third_started.wait(), 2)
    provider.release.set()
    emitted = await asyncio.wait_for(running, 3)

    root_events = await history(store, root.id)
    completed = {event.call_id: event for event in root_events if isinstance(event, ToolCallCompleted)}
    assert completed["status-running"].result["state"] == "running"
    assert completed["status-queued"].result["state"] == "queued"
    assert completed["messages-running"].result == [{"role": "user", "content": "job 0"}]
    assert isinstance(completed["send"].result, str)
    assert completed["kill"].result == {"task_id": provider.researchers[1], "state": "killed"}
    assert completed["result-0"].result == {
        "task_id": provider.researchers[0],
        "state": "completed",
        "text": "researcher done",
    }
    assert completed["result-2"].result == {
        "task_id": provider.researchers[2],
        "state": "completed",
        "text": "draft job 2",
    }
    assert completed["result-killed"].result == {"task_id": provider.researchers[1], "state": "killed"}
    assert provider.max_active == 2
    assert provider.cancelled == {"job 1"}
    assert [
        event.child_session_id for event in root_events if isinstance(event, ChildSessionSpawned)
    ] == provider.researchers
    assert len(await store.list(parent_id=root.id)) == 3

    first_events = await history(store, provider.researchers[0])
    leaf_events = await history(store, provider.investigator)
    assert [event.child_session_id for event in first_events if isinstance(event, ChildSessionSpawned)] == [
        provider.investigator
    ]
    assert [(event.source, event.text) for event in first_events if isinstance(event, AgentMessageQueued)] == [
        ("parent", "focus on standards"),
        ("child", "verified source"),
    ]
    assert not [
        event for event in root_events if isinstance(event, AgentMessageQueued) and event.text == "verified source"
    ]
    assert [(event.source, event.text) for event in root_events if isinstance(event, AgentMessageQueued)] == [
        ("child", "research complete")
    ]
    assert derive_task_state(leaf_events) == "completed"
    killed_events = await history(store, provider.researchers[1])
    assert derive_task_state(killed_events) == "killed"
    assert len([event for event in killed_events if isinstance(event, KillRequested)]) == 1
    assert len([event for event in root_events if isinstance(event, TaskNoticeQueued)]) == 3
    assert len({item.session_id for item in emitted if item.depth == 1}) == 3

    replayed = [item async for item in harness.replay(root.id)]
    assert [(item.session_id, item.seq) for item in replayed] == [
        (root.id, index) for index in range(1, len(root_events) + 1)
    ]
    assert len(await store.list(parent_id=root.id)) == 3
    assert len([event for event in killed_events if isinstance(event, TurnStarted)]) == 1


class RecoveryOpeningProvider:
    provider_name = "sarathi-recovery-opening"

    def __init__(self, root_id: str) -> None:
        self.researchers = [task_id(root_id, f"recover-launch-{index}", 0) for index in range(4)]
        self.investigator = task_id(self.researchers[0], "recover-investigator", 0)
        self.counts: defaultdict[tuple[str, str], int] = defaultdict(int)
        self.block = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    def role(self, req: SampleRequest) -> str:
        prompt = req.system[0].text
        if "You are Sarathi" in prompt:
            return "root"
        if "You are a research subagent" in prompt:
            return "researcher"
        return "investigator"

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        role = self.role(req)
        task = req.messages[0].content
        key = role, task
        index = self.counts[key]
        self.counts[key] += 1
        if role == "root" and index == 0:
            sample = StreamEnd(
                tool_calls=[
                    call(
                        "researcher",
                        {"task": "nested" if number == 0 else f"recovery {number}"},
                        f"recover-launch-{number}",
                    )
                    for number in range(4)
                ],
                finish_reason="tool_calls",
            )
        elif role == "root" and index == 1:
            sample = StreamEnd(
                tool_calls=[call("task_kill", {"task_id": self.researchers[3]}, "recover-kill")],
                finish_reason="tool_calls",
            )
        elif role == "root":
            sample = StreamEnd(
                tool_calls=[call("task_wait", {"task_ids": self.researchers[:3]}, "recover-root-wait")],
                finish_reason="tool_calls",
            )
        elif role == "researcher" and task == "nested" and index == 0:
            sample = StreamEnd(
                tool_calls=[call("investigator", {"task": "verify recovery"}, "recover-investigator")],
                finish_reason="tool_calls",
            )
        elif role == "researcher" and task == "nested":
            sample = StreamEnd(
                tool_calls=[call("task_wait", {"task_ids": [self.investigator]}, "recover-child-wait")],
                finish_reason="tool_calls",
            )
        else:
            await self.block.wait()
            sample = StreamEnd(text=f"opening {task}", finish_reason="stop")
        yield sample


class RecoveryProvider:
    provider_name = "sarathi-recovery"

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.two_started = asyncio.Event()
        self.started: list[tuple[str, str]] = []
        self.active = 0
        self.max_active = 0

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=64_000)

    def role(self, req: SampleRequest) -> str:
        prompt = req.system[0].text
        if "You are Sarathi" in prompt:
            return "root"
        if "You are a research subagent" in prompt:
            return "researcher"
        return "investigator"

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        role = self.role(req)
        task = req.messages[0].content
        if role == "root" or role == "researcher" and task == "nested":
            yield StreamEnd(text=f"recovered {role}", finish_reason="stop")
            return
        self.started.append((role, task))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.active == 2:
            self.two_started.set()
        try:
            await self.release.wait()
        finally:
            self.active -= 1
        yield StreamEnd(text=f"recovered {task}", finish_reason="stop")


class RecordingSocket:
    def __init__(self) -> None:
        self.frames: list[dict[str, object]] = []

    async def send_text(self, payload: str) -> None:
        self.frames.append(json.loads(payload))


async def wait_for_recovery_snapshot(
    store: MemoryStore,
    root_id: str,
    provider: RecoveryOpeningProvider,
) -> None:
    expected = {
        root_id: "waiting",
        provider.researchers[0]: "waiting",
        provider.researchers[1]: "running",
        provider.researchers[2]: "queued",
        provider.researchers[3]: "killed",
        provider.investigator: "running",
    }
    for _ in range(300):
        states = {sid: derive_task_state(await history(store, sid)) for sid in expected}
        if states == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"recovery snapshot never settled: {states}")


async def test_fresh_harness_recovers_nested_tasks_without_twins_or_killed_restart() -> None:
    store = MemoryStore()
    bootstrap = Harness(RecoveryOpeningProvider("seed"), store, [Sarathi], default_model="test-model")
    root = await bootstrap.create_session(Sarathi, {"user": "1"})
    opening_provider = RecoveryOpeningProvider(root.id)
    opening = Harness(
        opening_provider,
        store,
        [Sarathi],
        default_model="test-model",
        deps_factory=deps_factory,
        max_concurrency=2,
    )
    opening_turn = asyncio.create_task(collect(opening.run(root.id, "recover")))
    await asyncio.wait_for(wait_for_recovery_snapshot(store, root.id, opening_provider), 4)
    opening_turn.cancel()
    await asyncio.gather(opening_turn, return_exceptions=True)

    session_ids = [root.id, *opening_provider.researchers, opening_provider.investigator]
    before = {sid: [(item.seq, item.event.model_dump_json()) async for item in store.read(sid)] for sid in session_ids}
    assert {header.id for header in await store.list(parent_id=root.id)} == set(opening_provider.researchers)
    assert [header.id for header in await store.list(parent_id=opening_provider.researchers[0])] == [
        opening_provider.investigator
    ]

    recovery_provider = RecoveryProvider()
    recovered = Harness(
        recovery_provider,
        store,
        [Sarathi],
        default_model="test-model",
        deps_factory=deps_factory,
        max_concurrency=2,
    )
    socket = RecordingSocket()
    connection = Connection(socket, recovered, root.id, "1")
    await connection.replay(root.id)
    replayed = [
        (str(frame["session_id"]), int(frame["seq"])) for frame in socket.frames if frame.get("seq") is not None
    ]
    assert len(replayed) == len(set(replayed))

    resumed = asyncio.create_task(connection.pump(recovered.resume(root.id)))
    await asyncio.wait_for(recovery_provider.two_started.wait(), 3)
    assert recovery_provider.max_active == 2
    assert ("researcher", "recovery 3") not in recovery_provider.started
    recovery_provider.release.set()
    assert await asyncio.wait_for(resumed, 5) is None
    delivered = [
        (str(frame["session_id"]), int(frame["seq"])) for frame in socket.frames if frame.get("seq") is not None
    ]
    assert len(delivered) == len(set(delivered))

    assert {header.id for header in await store.list(parent_id=root.id)} == set(opening_provider.researchers)
    assert [header.id for header in await store.list(parent_id=opening_provider.researchers[0])] == [
        opening_provider.investigator
    ]
    for sid in session_ids:
        after = [(item.seq, item.event.model_dump_json()) async for item in store.read(sid)]
        assert after[: len(before[sid])] == before[sid]
        assert len([seq for seq, _ in after]) == len({seq for seq, _ in after})

    assert derive_task_state(await history(store, root.id)) == "completed"
    for sid in [*opening_provider.researchers[:3], opening_provider.investigator]:
        assert derive_task_state(await history(store, sid)) == "completed"
    killed_id = opening_provider.researchers[3]
    killed = await history(store, killed_id)
    assert derive_task_state(killed) == "killed"
    assert len([event for event in killed if isinstance(event, KillRequested)]) == 1
    assert not [event for event in killed if isinstance(event, TurnStarted)]
    assert [(item.seq, item.event.model_dump_json()) async for item in store.read(killed_id)] == before[killed_id]

    root_events = await history(store, root.id)
    launches = [event for event in root_events if isinstance(event, ChildSessionSpawned)]
    assert [event.child_session_id for event in launches] == opening_provider.researchers
    notices = [event.notice_id for event in root_events if isinstance(event, TaskNoticeQueued)]
    assert len(notices) == len(set(notices)) == 4
    child_events = await history(store, opening_provider.researchers[0])
    assert [event.child_session_id for event in child_events if isinstance(event, ChildSessionSpawned)] == [
        opening_provider.investigator
    ]
    child_notices = [event.notice_id for event in child_events if isinstance(event, TaskNoticeQueued)]
    assert len(child_notices) == len(set(child_notices)) == 1


async def test_broadcaster_buffers_replay_handoff_and_deduplicates_persisted_frames() -> None:
    store = MemoryStore()
    harness = Harness(RecoveryProvider(), store, [Sarathi], default_model="test-model")
    root = await harness.create_session(Sarathi, {"user": "1"})
    hub = ConnectionHub()
    socket = RecordingSocket()
    connection = Connection(socket, harness, root.id, "1", hub)
    await hub.register(connection)

    initial = [item async for item in store.read(root.id)][0]
    replay_race = Emitted(session_id=root.id, depth=0, seq=initial.seq, event=initial.event)
    await hub.publish(root.id, replay_race)
    await connection.replay(root.id)
    await hub.activate(connection)

    seq = await store.append(root.id, [TurnStarted(turn_id="turn", input="go")], expect_seq=initial.seq)
    started = [item async for item in store.read(root.id)][-1]
    live = Emitted(session_id=root.id, depth=0, seq=seq, event=started.event)
    await hub.publish(root.id, live)
    await hub.publish(root.id, live)

    delivered = [
        (str(frame["session_id"]), int(frame["seq"])) for frame in socket.frames if frame.get("seq") is not None
    ]
    assert delivered == [(root.id, initial.seq), (root.id, seq)]


class FailingSocket:
    async def send_text(self, payload: str) -> None:
        raise RuntimeError(payload)


async def test_broadcaster_removes_a_dead_secondary_without_failing_the_owner() -> None:
    store = MemoryStore()
    harness = Harness(RecoveryProvider(), store, [Sarathi], default_model="test-model")
    root = await harness.create_session(Sarathi, {"user": "1"})
    hub = ConnectionHub()
    owner_socket = RecordingSocket()
    owner = Connection(owner_socket, harness, root.id, "1", hub)
    dead = Connection(FailingSocket(), harness, root.id, "1", hub)
    await hub.register(owner)
    await hub.register(dead)
    await hub.activate(owner)
    await hub.activate(dead)

    stamped = [item async for item in store.read(root.id)][0]
    emitted = Emitted(session_id=root.id, depth=0, seq=stamped.seq, event=stamped.event)
    await hub.publish(root.id, emitted)

    assert len(owner_socket.frames) == 1
    assert hub.connections[root.id] == {owner}
