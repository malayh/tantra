from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

from stress.conftest import track_runtime
from stress.driver import (
    Policy,
    PolicyState,
    SyntheticProvider,
    blob,
    is_summarize,
    last_user,
    turn_step,
    worker_policy,
)
from stress.invariants import check_log, check_pairs, check_window, event_type, log, picks
from tantra import (
    Agent,
    ApprovalResponse,
    CompactionConfig,
    ModelLimits,
    PruneThenSummarize,
    Runtime,
    Sample,
    SampleRequest,
    Store,
    tool,
)
from tantra.providers.base import ToolCall

MODEL = "stress/worker"

LIMITS = ModelLimits(context_window=32_000, max_output=2_000)

CONFIG = CompactionConfig(
    buffer=2_000,
    prune_pool_min=4_000,
    prune_gain_min=2_000,
    tail_turns=2,
    summarize_at=0.95,
)

MARKERS = ("ORBIT-7741", "ledger-key-9f3a", "codename-BLUEHERON")

TURNS = 60

RESTUB_TURNS = 12

BIG_TURN = "read the whole ledger"

MONSTER_TURN = "monster"


@tool
async def fetch_blob(size: int) -> str:
    """Return a deterministic blob of `size` characters."""
    return blob(size, tag=f"blob-{size}")


@tool
async def risky_note(text: str) -> str:
    """Record a note. Needs approval before it runs."""
    return f"noted {text}"


class Marathoner(Agent):
    """Fetches blobs and reports on them."""

    model = MODEL
    prompt = "You fetch blobs and summarise them."
    tools = [fetch_blob, risky_note]
    permissions = {"fetch_blob": "allow", "risky_note": "ask"}


def build(store: Store, policy: Policy, *, seed: int = 0) -> tuple[Runtime, SyntheticProvider]:
    provider = SyntheticProvider(policy, limits=LIMITS, seed=seed)
    provider.state.markers = list(MARKERS)
    runtime = track_runtime(
        Runtime(
            provider,
            store,
            [Marathoner],
            default_model=MODEL,
            compactor=PruneThenSummarize(CONFIG),
        )
    )
    return runtime, provider


async def execute(runtime: Runtime, sid: str, input: str) -> Any:
    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        return await connection.prompt(input, command_id=uuid4())


async def wait_for(store: Store, sid: str, kind: str) -> Any:
    for _ in range(10_000):
        events = picks(await log(store, sid), kind)
        if events:
            return events[-1]
        await asyncio.sleep(0)
    raise AssertionError(f"{kind} was not recorded")


async def public_log(runtime: Runtime, sid: str, last_seq: int) -> list[tuple[int, Any]]:
    stream = runtime.events(UUID(hex=sid))
    events: list[tuple[int, Any]] = []
    try:
        async for item in stream:
            events.append((item.seq, item.event))
            if item.seq == last_seq:
                return events
    finally:
        await stream.aclose()
    return events


def fetch(state: PolicyState, size: int) -> ToolCall:
    return ToolCall(id=state.next_call_id(), name="fetch_blob", args=json.dumps({"size": size}))


async def stamped_log(store: Store, sid: str) -> list[tuple[int, Any]]:
    return [(item.seq, item.event) async for item in store.read(sid)]


def stubs(events: list[Any]) -> list[Any]:
    return [event for event in picks(events, "tool_call_completed") if str(event.result).startswith("[pruned:")]


def jittered() -> Policy:
    def sized(state: PolicyState) -> int:
        return 5_000 + state.rng.randrange(0, 4_000)

    return worker_policy(size=sized, calls=2, answer_chars=10_000)


def blocks(request: SampleRequest) -> list[list[dict[str, Any]]]:
    grouped: list[list[dict[str, Any]]] = []
    for message in request.messages:
        dumped = message.model_dump()
        if dumped.get("role") == "user" or not grouped:
            grouped.append([])
        grouped[-1].append(dumped)
    return grouped


def first_compacted(provider: SyntheticProvider) -> int:
    for index, request in enumerate(provider.requests):
        if request.messages and str(getattr(request.messages[0], "content", "")).startswith("## Goal"):
            return index
    raise AssertionError("no request was ever assembled over a compaction summary")


async def test_marathon_many_turns(store: Store) -> None:
    runtime, provider = build(store, jittered())
    sid = (await runtime.create(Marathoner)).hex

    for index in range(TURNS):
        marker = MARKERS[index] if index < len(MARKERS) else ""
        await execute(runtime, sid, f"turn {index} {marker}".strip())

    events = await log(store, sid)
    applied = picks(events, "compaction_applied")

    assert len(picks(events, "turn_completed")) == TURNS
    assert len(applied) >= 3
    assert stubs(events)
    for event in applied:
        assert all(marker in event.summary for marker in MARKERS)

    check_pairs(provider.requests)
    check_window(provider.requests, LIMITS, CONFIG)
    await check_log(store, sid)

    stamped = await stamped_log(store, sid)
    replayed = await public_log(runtime, sid, stamped[-1][0])
    assert replayed == stamped

    for call_id in {stub.call_id for stub in stubs(events)}:
        history = [
            (seq, event)
            for seq, event in stamped
            if event_type(event) == "tool_call_completed" and event.call_id == call_id
        ]
        (original_seq, original), later = history[0], history[1:]
        assert not str(original.result).startswith("[pruned:")
        assert len(str(original.result)) >= 5_000
        assert later != []
        assert all(str(event.result).startswith("[pruned:") for _, event in later)
        assert original_seq < min(seq for seq, _ in later)


async def restubbed(store: Store) -> tuple[dict[str, int], SyntheticProvider]:
    runtime, provider = build(store, jittered())
    sid = (await runtime.create(Marathoner)).hex
    for index in range(RESTUB_TURNS):
        await execute(runtime, sid, f"turn {index}")
    counts: dict[str, int] = {}
    for event in stubs(await log(store, sid)):
        counts[event.call_id] = counts.get(event.call_id, 0) + 1
    return counts, provider


async def test_prune_never_restubs_the_same_call(store: Store) -> None:
    counts, _ = await restubbed(store)

    assert counts != {}
    assert sorted(call_id for call_id, count in counts.items() if count > 1) == []


async def test_compacted_window_stays_inside_the_estimator_slack(store: Store) -> None:
    _, provider = await restubbed(store)

    check_window(provider.requests, LIMITS, CONFIG)


async def test_tail_turns_intact(store: Store) -> None:
    base = worker_policy(size=3_000, calls=2, answer_chars=6_000)

    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if not is_summarize(req) and last_user(req) == BIG_TURN and turn_step(req) < 1:
            return Sample(tool_calls=[fetch(state, 90_000)])
        return base(req, state)

    runtime, provider = build(store, policy)
    sid = (await runtime.create(Marathoner)).hex

    for index in range(4):
        await execute(runtime, sid, f"warmup {index}")
    await execute(runtime, sid, BIG_TURN)

    events = await log(store, sid)
    assert len(picks(events, "compaction_applied")) == 1

    index = first_compacted(provider)
    assert is_summarize(provider.requests[index - 1])
    pre, post = blocks(provider.requests[index - 2]), blocks(provider.requests[index])

    assert post[0][0]["content"].startswith("## Goal")
    assert post[1][0]["content"] == "warmup 3"
    assert post[1] in pre
    assert post[-1][0]["content"] == BIG_TURN
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_live_ask_after_compaction(store: Store) -> None:
    base = worker_policy(size=3_000, calls=2, answer_chars=6_000)

    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if is_summarize(req) or last_user(req) != BIG_TURN:
            return base(req, state)
        step = turn_step(req)
        if step < 1:
            return Sample(tool_calls=[fetch(state, 90_000)])
        if step == 1:
            return Sample(
                tool_calls=[
                    ToolCall(id=state.next_call_id(), name="risky_note", args=json.dumps({"text": "after the brief"}))
                ]
            )
        return Sample(text="filed after approval")

    runtime, provider = build(store, policy)
    sid = (await runtime.create(Marathoner)).hex
    for index in range(4):
        await execute(runtime, sid, f"warmup {index}")

    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        pending = asyncio.create_task(connection.prompt(BIG_TURN, command_id=uuid4()))
        raised = await wait_for(store, sid, "ask_raised")
        events = await log(store, sid)
        kinds = [event_type(event) for event in events]
        assert kinds.index("compaction_applied") < kinds.index("ask_raised")
        await connection.answer(
            UUID(hex=raised.ask_id),
            ApprovalResponse(allow=True),
            command_id=uuid4(),
        )
        result = await pending

    events = await log(store, sid)
    assert result.outcome == "completed"
    assert "noted after the brief" in [str(event.result) for event in picks(events, "tool_call_completed")]
    check_pairs(provider.requests)
    assert any(request.messages[0].content.startswith("## Goal") for request in provider.requests if request.messages)
    await check_log(store, sid)


async def test_monster_turn_prune_only(store: Store) -> None:
    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if is_summarize(req):
            raise AssertionError("a session this short must never reach the summarize tier")
        if turn_step(req) >= 1:
            return Sample(text="ok")
        if last_user(req) == MONSTER_TURN:
            return Sample(tool_calls=[fetch(state, 100_000), fetch(state, 100_000)])
        return Sample(tool_calls=[fetch(state, 200)])

    runtime, provider = build(store, policy)
    sid = (await runtime.create(Marathoner)).hex

    await execute(runtime, sid, MONSTER_TURN)
    assert stubs(await log(store, sid)) == []

    await execute(runtime, sid, "small one")
    assert stubs(await log(store, sid)) == []

    await execute(runtime, sid, "small two")

    events = await log(store, sid)
    assert picks(events, "compaction_applied") == []
    assert [event.call_id for event in stubs(events)] != []
    assert len(picks(events, "turn_completed")) == 3
    assert all(event.stop_reason == "completed" for event in picks(events, "turn_completed"))

    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_compaction_survives_writer_replacement(store: Store) -> None:
    policy = worker_policy(size=6_000, calls=2, answer_chars=10_000)
    runtime, provider = build(store, policy, seed=1)
    sid = (await runtime.create(Marathoner)).hex

    for index in range(20):
        marker = MARKERS[index] if index < len(MARKERS) else ""
        await execute(runtime, sid, f"turn {index} {marker}".strip())

    stamped = await stamped_log(store, sid)
    applied = [event for _, event in stamped if event_type(event) == "compaction_applied"]
    assert applied
    for event in applied:
        assert all(marker in event.summary for marker in MARKERS)

    events = [event for _, event in stamped]
    assert len(picks(events, "turn_completed")) == 20
    check_pairs(provider.requests)
    check_window(provider.requests, LIMITS, CONFIG)
    await check_log(store, sid)
