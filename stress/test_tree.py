from __future__ import annotations

import json
from contextlib import aclosing
from typing import Any

from stress.driver import (
    Policy,
    PolicyState,
    SyntheticProvider,
    by_model,
    call_policy,
    last_user,
    turn_step,
)
from stress.invariants import check_log, check_pairs, event_type, log, picks
from tantra import (
    Agent,
    ApprovalResponse,
    Context,
    FreeText,
    FreeTextResponse,
    Harness,
    ProviderError,
    Sample,
    SampleRequest,
    Store,
    collect,
    tool,
)
from tantra.providers.base import ToolCall

MODEL_ROOT = "stress/root"
MODEL_MID = "stress/mid"
MODEL_LEAF = "stress/leaf"
MODEL_BOSS = "stress/boss"
MODEL_HAND = "stress/hand"

FAN_TASKS = 12

FAN_FAILURES = (3, 7)


@tool
async def confirm(step: str, ctx: Context) -> str:
    """Ask for two confirmations, in order, before reporting."""
    first = await ctx.ask(FreeText(prompt=f"first check for {step}"))
    second = await ctx.ask(FreeText(prompt=f"second check for {step}"))
    return f"{step}:{first.text}/{second.text}"


@tool
async def sign_off(step: str, ctx: Context) -> str:
    """Ask for one confirmation before reporting."""
    answer = await ctx.ask(FreeText(prompt=f"sign off {step}"))
    return f"{step}:{answer.text}"


class Leaf(Agent):
    """Confirms things with a human."""

    model = MODEL_LEAF
    tools = [confirm, sign_off]
    permissions = {"confirm": "allow", "sign_off": "allow"}


class Mid(Agent):
    """Passes work down to the leaf."""

    model = MODEL_MID
    subagents = [Leaf]
    permissions = {"leaf": "allow"}


class Root(Agent):
    """Passes work down to the mid."""

    model = MODEL_ROOT
    subagents = [Mid]
    permissions = {"mid": "allow"}


class Boss(Agent):
    """Passes work straight to the leaf."""

    model = MODEL_BOSS
    subagents = [Leaf]
    permissions = {"leaf": "allow"}


def build(store: Store, policy: Policy, agents: list[type[Agent]], **options: Any) -> tuple[Harness, SyntheticProvider]:
    provider = SyntheticProvider(policy)
    return Harness(provider, store, agents, max_depth=2, **options), provider


def chain_policy(leaf_tool: str) -> Policy:
    return by_model(
        {
            MODEL_ROOT: call_policy("mid", {"task": "dig"}, answer="root done"),
            MODEL_MID: call_policy("leaf", {"task": "check"}, answer="mid done"),
            MODEL_LEAF: call_policy(leaf_tool, {"step": "ship"}, answer="leaf done"),
            MODEL_BOSS: call_policy("leaf", {"task": "check"}, answer="boss done"),
        }
    )


def emitted(events: list[Any], kind: str, session_id: str | None = None) -> list[Any]:
    return [
        item.event
        for item in events
        if event_type(item.event) == kind and (session_id is None or item.session_id == session_id)
    ]


async def test_deep_bubble_resume_fresh(store: Store) -> None:
    policy = chain_policy("confirm")
    harness, provider = build(store, policy, [Root])
    root_sid = (await harness.create_session(Root)).id

    opening = await collect(harness.run(root_sid, "go"))
    spawned = emitted(opening, "child_session_spawned")
    assert [event.agent for event in spawned] == ["mid", "leaf"]
    mid_sid, leaf_sid = spawned[0].child_session_id, spawned[1].child_session_id

    depths = {item.session_id: item.depth for item in opening}
    assert depths == {root_sid: 0, mid_sid: 1, leaf_sid: 2}
    assert emitted(opening, "ask_raised", root_sid) == []
    assert len(emitted(opening, "ask_raised", leaf_sid)) == 1

    root_log = await log(store, root_sid)
    assert [event.agent for event in picks(root_log, "child_session_spawned")] == ["mid"]
    assert [event.name for event in picks(root_log, "tool_call_requested")] == ["mid"]

    first_ask = emitted(opening, "ask_raised", leaf_sid)[0]
    del harness

    second, waiting = build(store, policy, [Root])
    answered = await collect(second.resume(leaf_sid, first_ask.ask_id, FreeTextResponse(text="yes-one")))
    assert len(picks([item.event for item in answered], "ask_raised")) == 1
    bubbled = await collect(second.resume(root_sid))
    second_ask = emitted(bubbled, "ask_raised", leaf_sid)[0]
    assert second_ask.ask_id != first_ask.ask_id
    assert (await store.header(root_sid)).status == "awaiting_input"

    third, closer = build(store, policy, [Root])
    await collect(third.resume(leaf_sid, second_ask.ask_id, FreeTextResponse(text="yes-two")))
    closing = await collect(third.resume(root_sid))

    assert [event.stop_reason for event in emitted(closing, "turn_completed", root_sid)] == ["completed"]
    leaf_results = [str(event.result) for event in picks(await log(store, leaf_sid), "tool_call_completed")]
    assert "ship:yes-one/yes-two" in leaf_results
    assert len(await store.list(parent_id=root_sid)) == 1
    assert len(await store.list(parent_id=mid_sid)) == 1
    for sid in (root_sid, mid_sid, leaf_sid):
        await check_log(store, sid)

    assert check_pairs(provider.requests, min_pairs=0) == 0
    assert waiting.requests == []
    assert check_pairs(closer.requests) == 3


async def test_fan_out_mixed(store: Store) -> None:
    captured: list[list[Any]] = []

    @tool
    async def dispatch(ctx: Context) -> str:
        """Fan the work out to a dozen workers."""
        tasks = [(Hand, f"boom {index}" if index in FAN_FAILURES else f"task {index}") for index in range(FAN_TASKS)]
        results = await ctx.fan_out(tasks, max_concurrency=4)
        captured.append(results)
        return f"{sum(1 for result in results if isinstance(result, str))} of {len(results)} done"

    class Hand(Agent):
        """Does one small task."""

        model = MODEL_HAND

    class Dispatcher(Agent):
        """Fans work out."""

        model = MODEL_ROOT
        tools = [dispatch]
        subagents = [Hand]
        permissions = {"dispatch": "allow"}

    def hand(req: SampleRequest, state: PolicyState) -> Sample:
        if last_user(req).startswith("boom"):
            raise ProviderError("the worker refuses this task", status_code=400)
        return Sample(text=f"handled {last_user(req)}")

    policy = by_model({MODEL_ROOT: call_policy("dispatch", {}, answer="all filed"), MODEL_HAND: hand})
    harness, provider = build(store, policy, [Dispatcher])
    sid = (await harness.create_session(Dispatcher)).id

    events = await collect(harness.run(sid, "go"))
    results = captured[0]

    assert len(results) == FAN_TASKS
    assert [index for index, result in enumerate(results) if isinstance(result, Exception)] == list(FAN_FAILURES)
    assert results[0] == "handled task 0"
    assert [event.stop_reason for event in emitted(events, "turn_completed", sid)] == ["completed"]

    children = await store.list(parent_id=sid)
    assert len(children) == FAN_TASKS
    assert len({header.id for header in children}) == FAN_TASKS
    assert len(picks(await log(store, sid), "child_session_spawned")) == FAN_TASKS
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_abandon_mid_spawn_no_twin(store: Store) -> None:
    policy = chain_policy("sign_off")
    harness, abandoned = build(store, policy, [Boss])
    sid = (await harness.create_session(Boss)).id

    seen: list[Any] = []
    async with aclosing(harness.run(sid, "go")) as live:
        async for item in live:
            seen.append(item)
            if event_type(item.event) == "ask_raised":
                break

    assert event_type(seen[-1].event) == "ask_raised"
    leaf_sid, ask = seen[-1].session_id, seen[-1].event
    assert leaf_sid != sid
    assert len(await store.list(parent_id=sid)) == 1
    del harness

    fresh, provider = build(store, policy, [Boss])
    await collect(fresh.resume(leaf_sid, ask.ask_id, FreeTextResponse(text="signed")))
    closing = await collect(fresh.resume(sid))

    assert [event.stop_reason for event in emitted(closing, "turn_completed", sid)] == ["completed"]
    assert len(await store.list(parent_id=sid)) == 1
    assert len(picks(await log(store, sid), "child_session_spawned")) == 1
    assert "ship:signed" in [str(event.result) for event in picks(await log(store, leaf_sid), "tool_call_completed")]
    assert check_pairs(abandoned.requests, min_pairs=0) == 0
    assert check_pairs(provider.requests) == 2
    await check_log(store, sid)
    await check_log(store, leaf_sid)


async def test_derived_permissions_at_depth(store: Store) -> None:
    probed: list[str] = []
    written: list[str] = []

    @tool
    async def probe(q: str) -> str:
        """Probe something harmless."""
        probed.append(q)
        return f"probed {q}"

    @tool
    async def forbidden_write(path: str) -> str:
        """Write a file the parent never allows."""
        written.append(path)
        return f"wrote {path}"

    class Kid(Agent):
        """Probes and writes."""

        model = MODEL_LEAF
        tools = [probe, forbidden_write]
        permissions = {"probe": "allow", "forbidden_write": "allow"}

    class Parent(Agent):
        """Delegates to the kid."""

        model = MODEL_ROOT
        subagents = [Kid]
        permissions = {"kid": "allow", "forbidden_*": "deny"}

    def kid(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) >= 1:
            return Sample(text="kid done")
        return Sample(
            tool_calls=[
                ToolCall(id=state.next_call_id(), name="probe", args=json.dumps({"q": "metrics"})),
                ToolCall(id=state.next_call_id(), name="forbidden_write", args=json.dumps({"path": "/etc/hosts"})),
            ]
        )

    policy = by_model({MODEL_ROOT: call_policy("kid", {"task": "look"}, answer="parent done"), MODEL_LEAF: kid})
    harness, provider = build(store, policy, [Parent], default_permission="ask")
    sid = (await harness.create_session(Parent)).id

    opening = await collect(harness.run(sid, "go"))
    kid_sid = emitted(opening, "child_session_spawned")[0].child_session_id
    raised = emitted(opening, "ask_raised", kid_sid)

    assert len(raised) == 1
    assert raised[0].request.extra["permission"] == "probe"
    assert probed == []

    del harness
    fresh, resumed = build(store, policy, [Parent], default_permission="ask")
    await collect(fresh.resume(kid_sid, raised[0].ask_id, ApprovalResponse(allow=True)))
    closing = await collect(fresh.resume(sid))

    kid_log = await log(store, kid_sid)
    denied = [event for event in picks(kid_log, "tool_call_completed") if event.is_error]

    assert probed == ["metrics"]
    assert written == []
    assert len(denied) == 1
    assert "denied by permissions: forbidden_write" in str(denied[0].result)
    assert [event.stop_reason for event in emitted(closing, "turn_completed", sid)] == ["completed"]
    assert check_pairs(provider.requests, min_pairs=0) == 0
    assert check_pairs(resumed.requests) == 3
    await check_log(store, sid)
    await check_log(store, kid_sid)
