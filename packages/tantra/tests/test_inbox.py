from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.ask import ApprovalResponse
from tantra.context import assemble_messages, pending_inbox
from tantra.errors import SeqConflict, SessionNotFound, TantraError
from tantra.events import (
    AgentMessageQueued,
    AskAnswered,
    AskRaised,
    ChildSessionSpawned,
    KillRequested,
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    Stamped,
    TaskNoticeQueued,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    TurnCompleted,
    TurnStarted,
)
from tantra.harness import Harness
from tantra.hooks import Hook
from tantra.loop import Emitted
from tantra.providers.base import AssistantMessage, ToolCall, ToolResultMessage, UserMessage
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool

INBOX_RESULT = "skipped: newer agent message"


class Bot(Agent): ...


def call(name: str, args: str = "{}", cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=args)


def picks(events: list[Emitted], kind: Any) -> list[Any]:
    return [item.event for item in events if isinstance(item.event, kind)]


async def log(store: MemoryStore, sid: str) -> list[SessionEvent]:
    return [item.event async for item in store.read(sid)]


async def until_sample(store: MemoryStore, sid: str) -> None:
    for _ in range(500):
        if any(isinstance(event, SampleStarted) for event in await log(store, sid)):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"session {sid} did not start sampling")


async def incomplete_root(store: MemoryStore) -> tuple[Harness, str]:
    harness = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")
    header = await harness.create_session(Bot)
    await store.append(header.id, [TurnStarted(turn_id="t1", input="go")], expect_seq=header.last_seq)
    return harness, header.id


async def test_send_user_message_validates_root_live_turn_and_text() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")

    with pytest.raises(SessionNotFound):
        await harness.send_user_message("missing", "hello")

    root = await harness.create_session(Bot)
    with pytest.raises(TantraError, match="no incomplete turn"):
        await harness.send_user_message(root.id, "hello")

    await store.append(root.id, [TurnStarted(turn_id="t1", input="go")], expect_seq=root.last_seq)
    for blank in ("", "  \n\t"):
        with pytest.raises(TantraError, match="blank"):
            await harness.send_user_message(root.id, blank)
    with pytest.raises(TantraError, match="32768"):
        await harness.send_user_message(root.id, "x" * 32_769)

    child = SessionHeader(id="child", agent="bot", parent_id=root.id, depth=1)
    await store.create(child)
    with pytest.raises(TantraError, match="roots only"):
        await harness.send_user_message(child.id, "hello")


async def test_send_user_message_preserves_text_and_returns_a_uuid() -> None:
    store = MemoryStore()
    harness, sid = await incomplete_root(store)

    message_id = await harness.send_user_message(sid, "  keep spacing  ")

    assert UUID(message_id).hex == message_id
    queued = [event for event in await log(store, sid) if isinstance(event, AgentMessageQueued)]
    assert queued == [
        AgentMessageQueued(
            message_id=message_id,
            sender_session_id=None,
            source="user",
            text="  keep spacing  ",
        )
    ]


class TerminalRaceStore:
    def __init__(self, inner: MemoryStore) -> None:
        self.inner = inner
        self.injected = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        if not self.injected and any(isinstance(event, AgentMessageQueued) for event in events):
            self.injected = True
            await self.inner.append(
                sid,
                [TurnCompleted(turn_id="t1", stop_reason="completed")],
                expect_seq=expect_seq,
            )
        return await self.inner.append(sid, events, expect_seq=expect_seq)


async def test_send_user_message_cannot_land_after_terminal_completion() -> None:
    store = MemoryStore()
    _, sid = await incomplete_root(store)
    harness = Harness(FakeProvider([]), TerminalRaceStore(store), [Bot], default_model="fake/model")

    with pytest.raises(TantraError, match="no incomplete turn"):
        await harness.send_user_message(sid, "too late")

    assert not [event for event in await log(store, sid) if isinstance(event, AgentMessageQueued)]


async def test_messages_and_notices_render_once_in_safe_sequence_order() -> None:
    first = AgentMessageQueued(message_id="m1", sender_session_id=None, source="user", text="first")
    duplicate = AgentMessageQueued(message_id="m1", sender_session_id=None, source="user", text="duplicate")
    notice = TaskNoticeQueued(
        notice_id="child-1:12",
        task_session_id="child-1",
        state="completed",
        terminal_seq=12,
    )
    events: list[SessionEvent] = [
        TurnStarted(turn_id="t1", input="go"),
        ChildSessionSpawned(call_id="spawn", child_session_id="child-1", agent="researcher"),
        SampleStarted(turn_id="t1", sample_id="s1", model="m"),
        ToolCallRequested(sample_id="s1", call_id="c1", name="look", args={}),
        first,
        duplicate,
        notice,
        ToolCallStarted(call_id="c1"),
        ToolCallCompleted(call_id="c1", result="done"),
        SampleCompleted(sample_id="s1"),
    ]

    messages = assemble_messages("", events)

    assert [type(message) for message in messages] == [
        UserMessage,
        AssistantMessage,
        ToolResultMessage,
        UserMessage,
        UserMessage,
    ]
    assert messages[3].content == "[user message id=m1]\nfirst"
    assert messages[4].content == "[task notice id=child-1:12 task_id=child-1 agent=researcher state=completed]"
    assert pending_inbox(events) == [first, notice]


async def test_task_notice_uses_unknown_agent_without_a_matching_spawn() -> None:
    notice = TaskNoticeQueued(
        notice_id="orphan:4",
        task_session_id="orphan",
        state="failed",
        terminal_seq=4,
    )

    messages = assemble_messages("", [TurnStarted(turn_id="t1", input="go"), notice])

    assert messages[-1].content == "[task notice id=orphan:4 task_id=orphan agent=unknown state=failed]"


async def test_message_during_sample_skips_every_call_and_resamples(gated_provider: Any) -> None:
    executed: list[str] = []

    @tool
    async def touch(name: str) -> str:
        """Record one touch."""
        executed.append(name)
        return name

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    provider = gated_provider(
        [
            Sample(
                text="old plan",
                tool_calls=[
                    call("touch", '{"name":"a"}', "c1"),
                    call("touch", '{"name":"b"}', "c2"),
                ],
            ),
            Sample(text="updated answer"),
        ]
    )
    harness = Harness(provider, store, [Worker], default_model="fake/model")
    watcher = Harness(FakeProvider([]), store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id
    provider.gate.clear()
    turn = asyncio.create_task(collect(harness.run(sid, "go")))
    await until_sample(store, sid)

    message_id = await watcher.send_user_message(sid, "change course")
    provider.gate.set()
    events = await turn

    assert executed == []
    completed = picks(events, ToolCallCompleted)
    assert [event.call_id for event in completed] == ["c1", "c2"]
    assert all(event.is_error and event.result == INBOX_RESULT for event in completed)
    queued = [item for item in events if isinstance(item.event, AgentMessageQueued)]
    assert [(item.seq, item.event.message_id) for item in queued] == [
        (next(item.seq for item in await _stamped(store, sid) if item.event == queued[0].event), message_id)
    ]
    messages = provider.requests[1].messages
    assert [type(message) for message in messages] == [
        UserMessage,
        AssistantMessage,
        ToolResultMessage,
        ToolResultMessage,
        UserMessage,
    ]
    assert messages[-1].content == f"[user message id={message_id}]\nchange course"
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def _stamped(store: MemoryStore, sid: str) -> list[Stamped]:
    return [item async for item in store.read(sid)]


async def test_message_during_tool_lets_it_finish_and_skips_the_rest() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    executed: list[str] = []

    @tool
    async def touch(name: str, ctx: Context) -> str:
        """Gate the first touch."""
        executed.append(name)
        if name == "a":
            started.set()
            await release.wait()
        return f"done {name}"

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    provider = FakeProvider(
        [
            Sample(
                tool_calls=[
                    call("touch", '{"name":"a"}', "c1"),
                    call("touch", '{"name":"b"}', "c2"),
                ]
            ),
            Sample(text="updated answer"),
        ]
    )
    harness = Harness(provider, store, [Worker], default_model="fake/model")
    watcher = Harness(FakeProvider([]), store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id
    turn = asyncio.create_task(collect(harness.run(sid, "go")))
    await started.wait()

    await watcher.send_user_message(sid, "stop after this")
    release.set()
    events = await turn

    assert executed == ["a"]
    first, second = picks(events, ToolCallCompleted)
    assert (first.call_id, first.result, first.is_error) == ("c1", "done a", False)
    assert (second.call_id, second.result, second.is_error) == ("c2", INBOX_RESULT, True)
    assert len(provider.requests) == 2


async def test_message_during_text_only_sample_prevents_terminal_completion(gated_provider: Any) -> None:
    store = MemoryStore()
    provider = gated_provider([Sample(text="old answer"), Sample(text="new answer")])
    harness = Harness(provider, store, [Bot], default_model="fake/model")
    watcher = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")
    sid = (await harness.create_session(Bot)).id
    provider.gate.clear()
    turn = asyncio.create_task(collect(harness.run(sid, "go")))
    await until_sample(store, sid)

    await watcher.send_user_message(sid, "one more thing")
    provider.gate.set()
    events = await turn

    assert len(provider.requests) == 2
    assert len(picks(events, TurnCompleted)) == 1
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_task_notice_is_actionable_but_kill_request_is_only_absorbed(gated_provider: Any) -> None:
    @tool
    async def touch() -> str:
        """Return a value."""
        return "done"

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    provider = gated_provider([Sample(text="old", tool_calls=[call("touch")]), Sample(text="new")])
    harness = Harness(provider, store, [Worker], default_model="fake/model")
    sid = (await harness.create_session(Worker)).id
    provider.gate.clear()
    turn = asyncio.create_task(collect(harness.run(sid, "go")))
    await until_sample(store, sid)
    await store.append(
        sid,
        [
            KillRequested(request_id="k1", requested_by_session_id="parent"),
            TaskNoticeQueued(
                notice_id="task:9",
                task_session_id="task",
                state="completed",
                terminal_seq=9,
            ),
        ],
        expect_seq=None,
    )
    provider.gate.set()

    events = await turn

    assert len(picks(events, KillRequested)) == 1
    assert len(picks(events, TaskNoticeQueued)) == 1
    assert picks(events, ToolCallCompleted)[0].result == INBOX_RESULT
    assert len(provider.requests) == 2


async def test_absorbed_duplicate_envelopes_stream_once_but_render_first_id_once(gated_provider: Any) -> None:
    seen: list[tuple[int | None, str]] = []

    class Recorder(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            if isinstance(emitted.event, AgentMessageQueued):
                seen.append((emitted.seq, emitted.event.text))

    store = MemoryStore()
    provider = gated_provider([Sample(text="old"), Sample(text="new")])
    harness = Harness(provider, store, [Bot], default_model="fake/model", hooks=[Recorder()])
    sid = (await harness.create_session(Bot)).id
    provider.gate.clear()
    turn = asyncio.create_task(collect(harness.run(sid, "go")))
    await until_sample(store, sid)
    await store.append(
        sid,
        [
            AgentMessageQueued(message_id="same", sender_session_id=None, source="user", text="first"),
            AgentMessageQueued(message_id="same", sender_session_id=None, source="user", text="second"),
        ],
        expect_seq=None,
    )
    provider.gate.set()

    events = await turn

    streamed = [item for item in events if isinstance(item.event, AgentMessageQueued)]
    assert [(item.seq, item.event.text) for item in streamed] == seen
    assert len(streamed) == 2
    rendered = [
        message.content
        for message in provider.requests[1].messages
        if isinstance(message, UserMessage) and message.content.startswith("[user message id=same]")
    ]
    assert rendered == ["[user message id=same]\nfirst"]


class InjectOnAnswerStore:
    def __init__(self, inner: MemoryStore) -> None:
        self.inner = inner
        self.injected = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        if not self.injected and any(isinstance(event, AskAnswered) for event in events):
            self.injected = True
            await self.inner.append(
                sid,
                [AgentMessageQueued(message_id="raced", sender_session_id=None, source="user", text="new plan")],
                expect_seq=expect_seq,
            )
        return await self.inner.append(sid, events, expect_seq=expect_seq)


async def test_resume_answer_absorption_streams_and_hooks_the_message_once() -> None:
    ran: list[str] = []

    @tool(permission="ask")
    async def touch() -> str:
        """Record execution."""
        ran.append("yes")
        return "done"

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    opening = Harness(
        FakeProvider([Sample(tool_calls=[call("touch")])]),
        store,
        [Worker],
        default_model="fake/model",
    )
    sid = (await opening.create_session(Worker)).id
    opened = await collect(opening.run(sid, "go"))
    ask_id = picks(opened, AskRaised)[0].ask_id
    seen: list[int | None] = []

    class Recorder(Hook):
        async def on_event(self, emitted: Emitted) -> None:
            if isinstance(emitted.event, AgentMessageQueued):
                seen.append(emitted.seq)

    fresh = Harness(
        FakeProvider([Sample(text="updated")]),
        InjectOnAnswerStore(store),
        [Worker],
        default_model="fake/model",
        hooks=[Recorder()],
    )

    resumed = await collect(fresh.resume(sid, ask_id, ApprovalResponse(allow=True)))

    absorbed = [item for item in resumed if isinstance(item.event, AgentMessageQueued)]
    assert len(absorbed) == 1
    assert seen == [absorbed[0].seq]
    assert ran == []
    assert picks(resumed, ToolCallCompleted)[0].result == INBOX_RESULT


async def test_fresh_harness_resume_delivers_a_persisted_message_and_replay_keeps_one_envelope() -> None:
    store = MemoryStore()
    opening = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")
    sid = (await opening.create_session(Bot)).id
    stream = opening.run(sid, "go")
    async for item in stream:
        if isinstance(item.event, SampleStarted):
            break
    await stream.aclose()

    message_id = await opening.send_user_message(sid, "resume with this")
    provider = FakeProvider([Sample(text="done")])
    fresh = Harness(provider, store, [Bot], default_model="fake/model")

    await collect(fresh.resume(sid))
    replayed = await collect(fresh.replay(sid))

    assert [item.event.message_id for item in replayed if isinstance(item.event, AgentMessageQueued)] == [message_id]
    assert provider.requests[0].messages[-1].content == f"[user message id={message_id}]\nresume with this"


async def test_unknown_foreign_event_still_raises_seq_conflict(gated_provider: Any) -> None:
    store = MemoryStore()
    provider = gated_provider([Sample(text="old")])
    harness = Harness(provider, store, [Bot], default_model="fake/model")
    sid = (await harness.create_session(Bot)).id
    provider.gate.clear()
    turn = asyncio.create_task(collect(harness.run(sid, "go")))
    await until_sample(store, sid)
    await store.append(sid, [TurnStarted(turn_id="foreign", input="bad")], expect_seq=None)
    provider.gate.set()

    with pytest.raises(SeqConflict, match="cannot absorb foreign"):
        await turn


async def test_pending_message_replays_started_call_then_skips_only_unstarted_calls() -> None:
    executed: list[str] = []

    @tool
    async def touch(name: str) -> str:
        """Record one touch."""
        executed.append(name)
        return f"done {name}"

    class Worker(Agent):
        tools = [touch]

    store = MemoryStore()
    opening = Harness(
        FakeProvider(
            [
                Sample(
                    tool_calls=[
                        call("touch", '{"name":"a"}', "c1"),
                        call("touch", '{"name":"b"}', "c2"),
                    ]
                )
            ]
        ),
        store,
        [Worker],
        default_model="fake/model",
    )
    sid = (await opening.create_session(Worker)).id
    stream = opening.run(sid, "go")
    async for item in stream:
        if isinstance(item.event, ToolCallStarted) and item.event.call_id == "c1":
            break
    await stream.aclose()
    assert executed == []

    await opening.send_user_message(sid, "change course")
    provider = FakeProvider([Sample(text="updated")])
    fresh = Harness(provider, store, [Worker], default_model="fake/model")

    resumed = await collect(fresh.resume(sid))

    assert executed == ["a"]
    first, second = picks(resumed, ToolCallCompleted)
    assert (first.call_id, first.result, first.is_error) == ("c1", "done a", False)
    assert (second.call_id, second.result, second.is_error) == ("c2", INBOX_RESULT, True)
    assert provider.requests[0].messages[-1].content.endswith("change course")
