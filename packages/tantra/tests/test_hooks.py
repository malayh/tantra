from __future__ import annotations

from typing import Any

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.context import TurnContext
from tantra.events import (
    SampleStarted,
    SessionEvent,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    TurnCompleted,
    TurnFailed,
)
from tantra.harness import Harness
from tantra.hooks import Denial, Hook
from tantra.loop import Emitted
from tantra.providers.base import TextDelta, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import tool

SAMPLES = [
    Sample(tool_calls=[ToolCall(id="c1", name="search_metrics", args='{"query": "p99"}')]),
    Sample(text="found it"),
]


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


@tool
async def search_metrics(query: str) -> str:
    """Search for metrics."""
    return f"found {query}"


class Bot(Agent):
    tools = [search_metrics]


class Recorder(Hook):
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.events: list[Emitted] = []

    async def before_turn(self, turn: TurnContext) -> None:
        self.calls.append("before_turn")

    async def before_sample(self, turn: TurnContext) -> None:
        self.calls.append("before_sample")

    async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> None:
        self.calls.append("before_tool")

    async def after_tool(self, call: ToolCallRequested, result: Any, is_error: bool, turn: TurnContext) -> None:
        self.calls.append("after_tool")

    async def after_turn(self, turn: TurnContext, event: SessionEvent) -> None:
        self.calls.append("after_turn")

    async def on_event(self, emitted: Emitted) -> None:
        self.events.append(emitted)


async def build(samples: list[Sample], hooks: list[Hook], agent: type[Agent] = Bot) -> tuple[Harness, Any, str]:
    store = MemoryStore()
    harness = Harness(FakeProvider(samples), store, [agent], default_model="fake/model", hooks=hooks)
    sid = (await harness.create_session(agent)).id
    return harness, store, sid


async def test_every_hook_point_fires_in_loop_order() -> None:
    recorder = Recorder()
    harness, store, sid = await build(list(SAMPLES), [recorder])

    events = await collect(harness.run(sid, "go"))

    assert recorder.calls == [
        "before_turn",
        "before_sample",
        "before_tool",
        "after_tool",
        "before_sample",
        "after_turn",
    ]
    assert [type(emitted.event).__name__ for emitted in recorder.events] == [
        type(emitted.event).__name__ for emitted in events
    ]
    assert any(isinstance(emitted.event, TextDelta) for emitted in recorder.events)


async def test_after_turn_receives_the_terminal_event() -> None:
    seen: list[SessionEvent] = []

    class Watcher(Hook):
        async def after_turn(self, turn: TurnContext, event: SessionEvent) -> None:
            seen.append(event)

    harness, store, sid = await build(list(SAMPLES), [Watcher()])

    await collect(harness.run(sid, "go"))

    assert len(seen) == 1
    assert isinstance(seen[0], TurnCompleted)
    assert seen[0].stop_reason == "completed"
    assert not isinstance(seen[0], TurnFailed)


async def test_a_denial_blocks_the_invocation_and_the_loop_continues() -> None:
    invoked: list[str] = []

    @tool
    async def write_dashboard(path: str) -> str:
        """Write a dashboard."""
        invoked.append(path)
        return "wrote"

    class Editor(Agent):
        tools = [write_dashboard]

    class Guard(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial | None:
            if call.name == "write_dashboard":
                return Denial("writes are off in this run")
            return None

    harness, store, sid = await build(
        [
            Sample(tool_calls=[ToolCall(id="c1", name="write_dashboard", args='{"path": "x"}')]),
            Sample(text="understood"),
        ],
        [Guard()],
        agent=Editor,
    )

    events = await collect(harness.run(sid, "go"))

    assert not invoked
    completed = picks(events, ToolCallCompleted)[0]
    assert completed.is_error
    assert completed.result == "denied by hook: writes are off in this run"
    assert not picks(events, ToolCallStarted)
    assert len(picks(events, SampleStarted)) == 2
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_before_tool_transforms_the_executed_args_but_not_the_log() -> None:
    seen: list[str] = []

    @tool
    async def search(query: str) -> str:
        """Search."""
        seen.append(query)
        return f"found {query}"

    class Bot2(Agent):
        tools = [search]

    class Rewriter(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> ToolCallRequested:
            return call.model_copy(update={"args": {"query": "sanitised"}})

    harness, store, sid = await build(
        [
            Sample(tool_calls=[ToolCall(id="c1", name="search", args='{"query": "raw"}')]),
            Sample(text="done"),
        ],
        [Rewriter()],
        agent=Bot2,
    )

    events = await collect(harness.run(sid, "go"))

    assert seen == ["sanitised"]
    assert picks(events, ToolCallRequested)[0].args == {"query": "raw"}
    assert picks(events, ToolCallCompleted)[0].result == "found sanitised"


async def test_after_tool_transforms_the_persisted_result() -> None:
    class Shouter(Hook):
        async def after_tool(self, call: ToolCallRequested, result: Any, is_error: bool, turn: TurnContext) -> str:
            return str(result).upper()

    harness, store, sid = await build(list(SAMPLES), [Shouter()])

    events = await collect(harness.run(sid, "go"))

    assert picks(events, ToolCallCompleted)[0].result == "FOUND P99"
    stored = [stamped.event async for stamped in store.read(sid)]
    assert [event.result for event in stored if isinstance(event, ToolCallCompleted)] == ["FOUND P99"]
    assert harness.provider.requests[1].messages[2].content == "FOUND P99"


async def test_hooks_run_in_order_and_the_first_denial_wins() -> None:
    order: list[str] = []
    invoked: list[str] = []

    @tool
    async def search(query: str) -> str:
        """Search."""
        invoked.append(query)
        return "found"

    class Bot3(Agent):
        tools = [search]

    class First(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> Denial:
            order.append("first")
            return Denial("nope")

    class Second(Hook):
        async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> None:
            order.append("second")

    harness, store, sid = await build(
        [
            Sample(tool_calls=[ToolCall(id="c1", name="search", args='{"query": "x"}')]),
            Sample(text="ok"),
        ],
        [First(), Second()],
        agent=Bot3,
    )

    events = await collect(harness.run(sid, "go"))

    assert order == ["first"]
    assert not invoked
    assert picks(events, ToolCallCompleted)[0].result == "denied by hook: nope"
