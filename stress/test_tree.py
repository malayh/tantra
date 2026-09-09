from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

from stress.driver import Policy, PolicyState, SyntheticProvider, by_model, last_user, turn_step
from stress.invariants import check_log, check_pairs, log, picks
from tantra import (
    Agent,
    ApprovalResponse,
    Context,
    FreeText,
    FreeTextResponse,
    Runtime,
    Sample,
    SampleRequest,
    Store,
    tool,
)
from tantra.providers.base import ToolCall

MODEL_ROOT = "stress/root"
MODEL_MID = "stress/mid"
MODEL_LEAF = "stress/leaf"
MODEL_HAND = "stress/hand"
FAN_TASKS = 12
FAN_FAILURES = (3, 7)


@tool
async def confirm(step: str, ctx: Context) -> str:
    """Ask for two confirmations, in order, before reporting."""
    first = await ctx.ask(FreeText(prompt=f"first check for {step}"))
    second = await ctx.ask(FreeText(prompt=f"second check for {step}"))
    return f"{step}:{first.text}/{second.text}"


class Leaf(Agent):
    model = MODEL_LEAF
    tools = [confirm]
    permissions = {"confirm": "allow", "finish": "allow"}


class Mid(Agent):
    model = MODEL_MID
    subagents = [Leaf]
    permissions = {"spawn": "allow", "finish": "allow"}


class Root(Agent):
    model = MODEL_ROOT
    subagents = [Mid]
    permissions = {"spawn": "allow"}


def build(store: Store, policy: Policy, agents: list[type[Agent]], **options: Any) -> tuple[Runtime, SyntheticProvider]:
    provider = SyntheticProvider(policy)
    return Runtime(provider, store, agents, max_depth=2, **options), provider


def calls(state: PolicyState, wanted: list[tuple[str, dict[str, Any]]]) -> Sample:
    return Sample(
        tool_calls=[ToolCall(id=state.next_call_id(), name=name, args=json.dumps(args)) for name, args in wanted]
    )


async def wait_for(store: Store, sid: str, kind: str, count: int = 1) -> Any:
    for _ in range(20_000):
        events = picks(await log(store, sid), kind)
        if len(events) >= count:
            return events[count - 1]
        await asyncio.sleep(0)
    raise AssertionError(f"{kind} was not recorded for {sid}")


async def test_deep_tree_uses_live_answers_and_explicit_finish(store: Store) -> None:
    def root_policy(req: SampleRequest, state: PolicyState) -> Sample:
        if last_user(req) == "go":
            step = turn_step(req)
            if step < 1:
                return calls(state, [("spawn", {"agent_name": "mid", "input": "dig"})])
            if step == 1:
                child_id = next(message.content for message in reversed(req.messages) if message.role == "tool")
                return calls(state, [("send", {"agent_id": child_id, "input": "root ping"})])
            return Sample(text="root waiting")
        return Sample(text="root done")

    def mid_policy(req: SampleRequest, state: PolicyState) -> Sample:
        incoming = last_user(req)
        if incoming == "dig":
            if turn_step(req) < 1:
                return calls(state, [("spawn", {"agent_name": "leaf", "input": "check"})])
            return Sample(text="mid waiting")
        if incoming.endswith("root ping"):
            if turn_step(req) < 1:
                parent_id = incoming.removeprefix("[agent ").split("]", 1)[0]
                return calls(state, [("send", {"agent_id": parent_id, "input": "mid pong"})])
            return Sample(text="mid acknowledged")
        return calls(state, [("finish", {"result": "mid done"})])

    def leaf_policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return calls(state, [("confirm", {"step": "ship"})])
        return calls(state, [("finish", {"result": "leaf done"})])

    policy = by_model({MODEL_ROOT: root_policy, MODEL_MID: mid_policy, MODEL_LEAF: leaf_policy})
    runtime, provider = build(store, policy, [Root])
    root_id = await runtime.create(Root)

    async with runtime.connect(root_id, writable=True) as connection:
        opening = await connection.prompt("go", command_id=uuid4())
        mid_id = picks(await log(store, root_id.hex), "child_created")[0].child_id
        leaf_created = await wait_for(store, mid_id, "child_created")
        leaf_id = leaf_created.child_id
        first = await wait_for(store, leaf_id, "ask_raised")
        await connection.answer(UUID(hex=first.ask_id), FreeTextResponse(text="yes-one"), command_id=uuid4())
        second = await wait_for(store, leaf_id, "ask_raised", 2)
        await connection.answer(UUID(hex=second.ask_id), FreeTextResponse(text="yes-two"), command_id=uuid4())
        await wait_for(store, leaf_id, "agent_finished")
        await wait_for(store, mid_id, "agent_finished")
        await wait_for(store, root_id.hex, "turn_completed", 2)

    root_log = await log(store, root_id.hex)
    mid_log = await log(store, mid_id)
    leaf_results = [str(event.result) for event in picks(await log(store, leaf_id), "tool_call_completed")]
    root_ping = f"[agent {root_id}] root ping"
    mid_pong = f"[agent {UUID(hex=mid_id)}] mid pong"
    assert opening.text == "root waiting"
    assert root_ping in [event.input for event in picks(mid_log, "input_queued")]
    assert root_ping in [event.input for event in picks(mid_log, "turn_started")]
    assert mid_pong in [event.input for event in picks(root_log, "input_queued")]
    assert mid_pong in [event.input for event in picks(root_log, "turn_started")]
    assert "ship:yes-one/yes-two" in leaf_results
    assert len(await store.list(parent_id=root_id.hex)) == 1
    assert len(await store.list(parent_id=mid_id)) == 1
    assert len(picks(await log(store, root_id.hex), "child_created")) == 1
    assert len(picks(await log(store, mid_id), "child_created")) == 1
    assert check_pairs(provider.requests) >= 4
    for sid in (root_id.hex, mid_id, leaf_id):
        await check_log(store, sid)


async def test_parallel_actors_finish_into_independent_journals(store: Store) -> None:
    active = 0
    peak = 0
    release = asyncio.Event()

    @tool
    async def work(index: int) -> str:
        """Perform one synchronized unit of work."""
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == FAN_TASKS:
            release.set()
        await asyncio.wait_for(release.wait(), timeout=2)
        active -= 1
        if index in FAN_FAILURES:
            raise RuntimeError(f"worker {index} failed")
        return f"handled task {index}"

    class Hand(Agent):
        model = MODEL_HAND
        tools = [work]
        permissions = {"work": "allow", "finish": "allow"}

    class Dispatcher(Agent):
        model = MODEL_ROOT
        subagents = [Hand]
        permissions = {"spawn": "allow"}

    def dispatcher(req: SampleRequest, state: PolicyState) -> Sample:
        if last_user(req) == "go":
            if turn_step(req) < 1:
                wanted = [("spawn", {"agent_name": "hand", "input": f"task {index}"}) for index in range(FAN_TASKS)]
                return calls(state, wanted)
            return Sample(text="dispatched")
        return Sample(text="received")

    def hand(req: SampleRequest, state: PolicyState) -> Sample:
        index = int(last_user(req).split()[-1])
        if turn_step(req) < 1:
            return calls(state, [("work", {"index": index})])
        return calls(state, [("finish", {"result": f"done {index}"})])

    runtime, provider = build(store, by_model({MODEL_ROOT: dispatcher, MODEL_HAND: hand}), [Dispatcher])
    root_id = await runtime.create(Dispatcher)
    async with runtime.connect(root_id, writable=True) as connection:
        result = await connection.prompt("go", command_id=uuid4())

    children = await store.list(parent_id=root_id.hex, limit=FAN_TASKS)
    for child in children:
        await wait_for(store, child.id, "agent_finished")

    assert result.text == "dispatched"
    assert peak == FAN_TASKS
    assert len(children) == FAN_TASKS
    assert len({child.id for child in children}) == FAN_TASKS
    assert len(picks(await log(store, root_id.hex), "child_created")) == FAN_TASKS
    errors = []
    for child in children:
        child_log = await log(store, child.id)
        errors.extend(event for event in picks(child_log, "tool_call_completed") if event.is_error)
        await check_log(store, child.id)
    assert sorted(int(str(event.result).split()[1]) for event in errors) == list(FAN_FAILURES)
    assert check_pairs(provider.requests) >= FAN_TASKS * 2
    await check_log(store, root_id.hex)


async def test_interruption_requires_later_root_activation(store: Store) -> None:
    started = asyncio.Event()

    @tool
    async def hold() -> str:
        """Wait until Runtime shutdown interrupts the turn."""
        started.set()
        await asyncio.Event().wait()
        return "impossible"

    class Interrupted(Agent):
        model = MODEL_ROOT
        tools = [hold]
        permissions = {"hold": "allow"}

    def blocking(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return calls(state, [("hold", {})])
        return Sample(text="unexpected")

    runtime, _ = build(store, blocking, [Interrupted])
    root_id = await runtime.create(Interrupted)
    async with runtime.connect(root_id, writable=True) as connection:
        pending = asyncio.create_task(connection.prompt("first", command_id=uuid4()))
        await asyncio.wait_for(started.wait(), timeout=2)
        await runtime.aclose()
        interrupted = await pending

    replacement, provider = build(store, lambda req, state: Sample(text="recovered"), [Interrupted])
    async with replacement.connect(root_id, writable=True) as current:
        recovered = await current.prompt("continue", command_id=uuid4())

    events = await log(store, root_id.hex)
    assert interrupted.outcome == "interrupted"
    assert recovered.text == "recovered"
    assert len(picks(events, "turn_interrupted")) == 1
    assert len(picks(events, "turn_completed")) == 1
    assert len(provider.requests) == 1
    await check_log(store, root_id.hex)


async def test_permissions_apply_at_depth(store: Store) -> None:
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
        model = MODEL_LEAF
        tools = [probe, forbidden_write]
        permissions = {"probe": "ask", "forbidden_write": "deny", "finish": "allow"}

    class Parent(Agent):
        model = MODEL_ROOT
        subagents = [Kid]
        permissions = {"spawn": "allow"}

    def parent(req: SampleRequest, state: PolicyState) -> Sample:
        if last_user(req) == "go":
            if turn_step(req) < 1:
                return calls(state, [("spawn", {"agent_name": "kid", "input": "look"})])
            return Sample(text="parent waiting")
        return Sample(text="parent done")

    def kid(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return calls(
                state,
                [
                    ("probe", {"q": "metrics"}),
                    ("forbidden_write", {"path": "/etc/hosts"}),
                ],
            )
        return calls(state, [("finish", {"result": "kid done"})])

    runtime, provider = build(
        store,
        by_model({MODEL_ROOT: parent, MODEL_LEAF: kid}),
        [Parent],
        default_permission="ask",
    )
    root_id = await runtime.create(Parent)

    async with runtime.connect(root_id, writable=True) as connection:
        opening = await connection.prompt("go", command_id=uuid4())
        kid_id = picks(await log(store, root_id.hex), "child_created")[0].child_id
        raised = await wait_for(store, kid_id, "ask_raised")
        assert raised.request.extra["permission"] == "probe"
        await connection.answer(UUID(hex=raised.ask_id), ApprovalResponse(allow=True), command_id=uuid4())
        await wait_for(store, kid_id, "agent_finished")

    kid_log = await log(store, kid_id)
    denied = [event for event in picks(kid_log, "tool_call_completed") if event.is_error]
    assert opening.text == "parent waiting"
    assert probed == ["metrics"]
    assert written == []
    assert len(denied) == 1
    assert "denied by permissions: forbidden_write" in str(denied[0].result)
    assert check_pairs(provider.requests) >= 3
    await check_log(store, root_id.hex)
    await check_log(store, kid_id)
