from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from tantra.adapters.collect import collect
from tantra.agent import Agent
from tantra.errors import ProviderError, SessionBusy, SessionNotFound, TantraError, TurnIncomplete
from tantra.events import (
    SampleStarted,
    SessionCreated,
    TextPart,
    ToolProgress,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.harness import Harness
from tantra.loop import RetryConfig
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context, tool


@tool
async def echo(value: str, ctx: Context) -> str:
    """Echo a value back."""
    return value


class Bot(Agent):
    tools = [echo]


class Flaky:
    def __init__(self, inner: FakeProvider, failures: int, error: Any) -> None:
        self.inner = inner
        self.failures = failures
        self.error = error
        self.attempts = 0

    def limits(self, model: str) -> ModelLimits:
        return self.inner.limits(model)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.attempts += 1
        if self.attempts <= self.failures:
            raise self.error()
        async for event in self.inner.stream(req):
            yield event


def picks(events: list[Any], kind: Any) -> list[Any]:
    return [event.event for event in events if isinstance(event.event, kind)]


async def session(harness: Harness, agent: type[Agent] = Bot, **metadata: Any) -> str:
    header = await harness.create_session(agent, metadata=metadata or None)
    return header.id


async def test_create_session_appends_session_created() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")

    header = await harness.create_session("bot", metadata={"company": 42})

    assert header.agent == "bot"
    assert header.last_seq == 1
    stored = await store.header(header.id)
    assert stored.metadata == {"company": 42}
    created = [stamped.event async for stamped in store.read(header.id)][0]
    assert isinstance(created, SessionCreated)
    assert created.agent == "bot"
    assert created.depth == 0
    assert created.parent_id is None


async def test_create_session_accepts_a_class_or_a_name() -> None:
    harness = Harness(FakeProvider([]), MemoryStore(), [Bot], default_model="fake/model")

    assert (await harness.create_session(Bot)).agent == "bot"
    assert (await harness.create_session("bot")).agent == "bot"
    with pytest.raises(TantraError, match="unknown agent"):
        await harness.create_session("nope")


async def test_run_on_an_unknown_session_raises() -> None:
    harness = Harness(FakeProvider([]), MemoryStore(), [Bot], default_model="fake/model")

    with pytest.raises(SessionNotFound):
        await collect(harness.run("missing", "hi"))


async def test_run_raises_session_busy_when_the_lease_is_held() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model="fake/model")
    sid = await session(harness)
    assert await store.acquire_lease(sid, "another-worker", 60.0)

    with pytest.raises(SessionBusy):
        await collect(harness.run(sid, "hi"))


async def test_the_lease_is_released_after_a_turn() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model="fake/model")
    sid = await session(harness)

    await collect(harness.run(sid, "hi"))

    assert (await store.header(sid)).lease is None
    assert await store.acquire_lease(sid, "someone", 1.0)


async def test_run_raises_turn_incomplete_on_an_abandoned_turn_and_releases_the_lease() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="hi")]), store, [Bot], default_model="fake/model")
    sid = await session(harness)
    header = await store.header(sid)
    await store.append(sid, [TurnStarted(turn_id="t1", input="first")], expect_seq=header.last_seq)

    with pytest.raises(TurnIncomplete):
        await collect(harness.run(sid, "second"))

    assert (await store.header(sid)).lease is None


async def test_abandoning_the_stream_releases_the_lease_and_leaves_the_turn_incomplete() -> None:
    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[ToolCall(id="c1", name="echo", args='{"value": "x"}')])]),
        store,
        [Bot],
        default_model="fake/model",
    )
    sid = await session(harness)

    stream = harness.run(sid, "go")
    seen = [event async for event in _take(stream, 2)]
    await stream.aclose()

    assert [type(event.event).__name__ for event in seen] == ["TurnStarted", "SampleStarted"]
    assert (await store.header(sid)).lease is None
    with pytest.raises(TurnIncomplete):
        await collect(harness.run(sid, "again"))


async def test_abandoning_the_stream_cancels_the_running_tool() -> None:
    marks: list[str] = []

    @tool
    async def slow(ctx: Context) -> str:
        """Reports progress, then does slow work."""
        await ctx.emit("starting")
        try:
            await asyncio.sleep(5.0)
        except asyncio.CancelledError:
            marks.append("cancelled")
            raise
        marks.append("written")
        return "done"

    class Slow(Agent):
        tools = [slow]

    harness = Harness(
        FakeProvider([Sample(tool_calls=[ToolCall(id="c1", name="slow", args="{}")])]),
        MemoryStore(),
        [Slow],
        default_model="fake/model",
    )
    sid = await session(harness, Slow)

    stream = harness.run(sid, "go")
    async for event in stream:
        if isinstance(event.event, ToolProgress):
            break
    await stream.aclose()

    assert marks == ["cancelled"]


async def test_a_stolen_lease_stops_the_turn_without_appending_anything_else() -> None:
    @tool
    async def steal(ctx: Context) -> str:
        """Lets the lease lapse, then takes it as somebody else."""
        await asyncio.sleep(0.05)
        await ctx.store.acquire_lease(ctx.session_id, "thief", 60.0)
        return "stolen"

    class Thief(Agent):
        tools = [steal]

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[ToolCall(id="c1", name="steal", args="{}")]), Sample(text="never")]),
        store,
        [Thief],
        default_model="fake/model",
        lease_ttl=0.01,
    )
    sid = await session(harness, Thief)

    with pytest.raises(TantraError, match="lease lost"):
        await collect(harness.run(sid, "go"))

    logged = [type(stamped.event).__name__ async for stamped in store.read(sid)]
    assert logged[-1] == "ToolCallCompleted"
    assert "TurnCompleted" not in logged
    assert "TurnFailed" not in logged
    assert (await store.header(sid)).lease.holder == "thief"


async def _take(stream: Any, count: int) -> AsyncIterator[Any]:
    taken = 0
    async for event in stream:
        yield event
        taken += 1
        if taken == count:
            return


async def test_a_completed_turn_does_not_block_the_next_one() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="one"), Sample(text="two")]), store, [Bot], default_model="m")
    sid = await session(harness)

    await collect(harness.run(sid, "first"))
    events = await collect(harness.run(sid, "second"))

    assert picks(events, TurnCompleted)[0].stop_reason == "completed"
    assert len(picks(events, TurnStarted)) == 1


async def test_a_missing_model_raises_at_run_start() -> None:
    class NoModel(Agent): ...

    harness = Harness(FakeProvider([Sample(text="hi")]), MemoryStore(), [NoModel])
    sid = await session(harness, NoModel)

    with pytest.raises(TantraError, match="no model"):
        await collect(harness.run(sid, "hi"))


async def test_deps_factory_is_called_per_run_and_reaches_the_tool() -> None:
    seen: list[Any] = []

    @tool
    async def peek(ctx: Context) -> str:
        """Look at deps."""
        seen.append(ctx.deps)
        return "ok"

    class Peeker(Agent):
        tools = [peek]

    calls: list[str] = []

    def factory(header: Any) -> dict[str, Any]:
        calls.append(header.id)
        return {"company": header.metadata.get("company")}

    store = MemoryStore()
    harness = Harness(
        FakeProvider([Sample(tool_calls=[ToolCall(id="c1", name="peek", args="{}")]), Sample(text="done")]),
        store,
        [Peeker],
        default_model="fake/model",
        deps_factory=factory,
    )
    header = await harness.create_session(Peeker, metadata={"company": 7})

    await collect(harness.run(header.id, "go"))

    assert calls == [header.id]
    assert seen == [{"company": 7}]


async def test_an_async_deps_factory_is_awaited() -> None:
    async def factory(header: Any) -> str:
        return f"deps-for-{header.agent}"

    seen: list[Any] = []

    @tool
    async def peek(ctx: Context) -> str:
        """Look at deps."""
        seen.append(ctx.deps)
        return "ok"

    class Peeker(Agent):
        tools = [peek]

    harness = Harness(
        FakeProvider([Sample(tool_calls=[ToolCall(id="c1", name="peek", args="{}")]), Sample(text="done")]),
        MemoryStore(),
        [Peeker],
        default_model="fake/model",
        deps_factory=factory,
    )
    sid = await session(harness, Peeker)

    await collect(harness.run(sid, "go"))

    assert seen == ["deps-for-peeker"]


async def test_a_retryable_provider_error_is_retried_and_nothing_partial_is_persisted() -> None:
    store = MemoryStore()
    provider = Flaky(
        FakeProvider([Sample(text="finally")]),
        failures=2,
        error=lambda: ProviderError("rate limited", status_code=429),
    )
    harness = Harness(
        provider, store, [Bot], default_model="fake/model", retry=RetryConfig(max_attempts=3, base_delay=0.001)
    )
    sid = await session(harness)

    events = await collect(harness.run(sid, "go"))

    assert provider.attempts == 3
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"
    assert len(picks(events, SampleStarted)) == 1
    assert [t.text for t in picks(events, TextPart)] == ["finally"]
    assert (await store.header(sid)).status == "idle"


async def test_a_connection_style_retryable_flag_is_honoured() -> None:
    provider = Flaky(
        FakeProvider([Sample(text="finally")]),
        failures=1,
        error=lambda: ProviderError("connection reset", retryable=True),
    )
    harness = Harness(
        provider, MemoryStore(), [Bot], default_model="fake/model", retry=RetryConfig(max_attempts=2, base_delay=0.001)
    )
    sid = await session(harness)

    events = await collect(harness.run(sid, "go"))

    assert provider.attempts == 2
    assert picks(events, TurnCompleted)[0].stop_reason == "completed"


async def test_a_non_retryable_provider_error_fails_the_turn_once() -> None:
    store = MemoryStore()
    provider = Flaky(
        FakeProvider([Sample(text="never reached")]),
        failures=1,
        error=lambda: ProviderError("bad request", status_code=400),
    )
    harness = Harness(
        provider, store, [Bot], default_model="fake/model", retry=RetryConfig(max_attempts=3, base_delay=0.001)
    )
    sid = await session(harness)

    events = await collect(harness.run(sid, "go"))

    assert provider.attempts == 1
    failed = picks(events, TurnFailed)[0]
    assert "bad request" in failed.error
    assert not picks(events, TurnCompleted)
    assert (await store.header(sid)).status == "failed"

    fresh = Harness(FakeProvider([]), store, [Bot], default_model="fake/model")
    replayed = await collect(fresh.replay(sid))
    assert [type(event.event).__name__ for event in replayed] == [
        "SessionCreated",
        "TurnStarted",
        "SampleStarted",
        "TurnFailed",
    ]


async def test_an_exhausted_fake_script_is_not_retried() -> None:
    provider = Flaky(FakeProvider([]), failures=0, error=lambda: ProviderError("unused"))
    harness = Harness(
        provider, MemoryStore(), [Bot], default_model="fake/model", retry=RetryConfig(max_attempts=3, base_delay=0.001)
    )
    sid = await session(harness)

    events = await collect(harness.run(sid, "go"))

    assert provider.attempts == 1
    assert "no scripted sample" in picks(events, TurnFailed)[0].error


async def test_retries_are_exhausted_into_a_turn_failed() -> None:
    store = MemoryStore()
    provider = Flaky(
        FakeProvider([Sample(text="never reached")]),
        failures=5,
        error=lambda: ProviderError("overloaded", status_code=503),
    )
    harness = Harness(
        provider, store, [Bot], default_model="fake/model", retry=RetryConfig(max_attempts=3, base_delay=0.001)
    )
    sid = await session(harness)

    events = await collect(harness.run(sid, "go"))

    assert provider.attempts == 3
    assert "overloaded" in picks(events, TurnFailed)[0].error
    assert (await store.header(sid)).status == "failed"


async def test_replay_from_seq_returns_a_contiguous_suffix() -> None:
    store = MemoryStore()
    harness = Harness(FakeProvider([Sample(text="done")]), store, [Bot], default_model="fake/model")
    sid = await session(harness)
    await collect(harness.run(sid, "go"))

    tail = await collect(harness.replay(sid, from_seq=2))

    assert [event.seq for event in tail] == list(range(3, 3 + len(tail)))
    assert all(event.session_id == sid for event in tail)


async def test_replay_of_an_unknown_session_raises() -> None:
    harness = Harness(FakeProvider([]), MemoryStore(), [Bot], default_model="fake/model")

    with pytest.raises(SessionNotFound):
        await collect(harness.replay("missing"))
