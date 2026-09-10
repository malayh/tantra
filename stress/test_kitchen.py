from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from stress.conftest import track_runtime
from stress.driver import Policy, PolicyState, SyntheticEmbedder, SyntheticProvider, by_model, flaky, turn_step
from stress.invariants import check_log, check_pairs, event_type, log, pairs_intact, picks
from tantra import (
    Agent,
    ApprovalResponse,
    BuiltinMemory,
    Choice,
    ChoiceResponse,
    Context,
    Denial,
    FileSystemSkills,
    FreeText,
    FreeTextResponse,
    Hook,
    MemoryStore,
    MemoryWrite,
    RetryConfig,
    Runtime,
    Sample,
    SampleRequest,
    SessionHeader,
    Store,
    memory_recall,
    memory_write,
    tool,
)
from tantra.providers.base import ToolCall

MODEL_LEAD = "kitchen/lead"

MODEL_CHECK = "kitchen/check"

DOCS = {
    "orbit": "ORBIT-7741 keys the seventh orbit ledger.",
    "ledger": "The ledger balances nightly at 02:00 UTC.",
    "heron": "BLUEHERON is the migration codename.",
}

FOLDED = "Nightly ledger rules, folded across two source lines."

VECTOR = "vector"

KEYWORD = "keyword"


@dataclass
class Ledger:
    processes: list[str] = field(default_factory=list)
    desks: list[Any] = field(default_factory=list)
    lookups: list[str] = field(default_factory=list)
    interviews: list[str] = field(default_factory=list)
    published: list[str] = field(default_factory=list)
    tallied: list[int] = field(default_factory=list)

    def clear(self) -> None:
        for value in vars(self).values():
            value.clear()


LEDGER = Ledger()


@dataclass
class Desk:
    process: str
    docs: dict[str, str]
    fetched: list[str] = field(default_factory=list)


@pytest.fixture(autouse=True)
def ledger() -> Ledger:
    LEDGER.clear()
    return LEDGER


def desk_factory() -> Callable[[SessionHeader], Desk]:
    process = uuid4().hex[:8]

    def build(header: SessionHeader) -> Desk:
        return Desk(process=process, docs=dict(DOCS))

    return build


@tool
async def lookup(topic: str, ctx: Context) -> str:
    """Return the desk's note on `topic`."""
    LEDGER.processes.append(ctx.deps.process)
    LEDGER.desks.append(ctx.deps)
    LEDGER.lookups.append(topic)
    ctx.deps.fetched.append(topic)
    await ctx.emit(f"reading {topic}")
    return ctx.deps.docs.get(topic, "nothing on file")


@tool
async def tally(items: list[str]) -> int:
    """Count the items handed in."""
    LEDGER.tallied.append(len(items))
    return len(items)


@tool(permission="allow")
async def publish(title: str) -> str:
    """Publish the finished brief under `title`."""
    LEDGER.published.append(title)
    return f"published {title}"


@tool
async def interview(subject: str, ctx: Context) -> str:
    """Interview `subject`: one free-text question, then one rating."""
    LEDGER.interviews.append(ctx.deps.process)
    LEDGER.desks.append(ctx.deps)
    await ctx.emit(f"interviewing {subject}")
    quote = await ctx.ask(FreeText(prompt=f"what did {subject} say?"))
    rating = await ctx.ask(Choice(title=f"how solid is {subject}?", options=["weak", "solid"]))
    return f"{subject}: {quote.text} ({rating.selected})"


class Checker(Agent):
    """Check one claim against the desk and report back."""

    model = MODEL_CHECK
    tools = [lookup]
    permissions = {"lookup": "allow"}


class Lead(Agent):
    """Research a topic with the desk's tools."""

    model = MODEL_LEAD
    prompt = "You run the research desk."
    tools = [lookup, tally, publish, interview, memory_write, memory_recall]
    subagents = [Checker]
    permissions = {"*": "allow"}


class Recorder(Hook):
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.events: list[str] = []

    async def before_turn(self, turn: Any) -> None:
        self.calls.append("before_turn")

    async def before_sample(self, turn: Any) -> None:
        self.calls.append("before_sample")

    async def before_tool(self, call: Any, turn: Any) -> Any:
        self.calls.append(f"before_tool:{call.name}")
        return None

    async def after_tool(self, call: Any, result: Any, is_error: bool, turn: Any) -> Any:
        self.calls.append(f"after_tool:{call.name}")
        return None

    async def after_turn(self, turn: Any, event: Any) -> None:
        self.calls.append(f"after_turn:{event_type(event)}")

    async def on_event(self, emitted: Any) -> None:
        self.events.append(event_type(emitted.event))


class Redactor(Hook):
    async def before_tool(self, call: Any, turn: Any) -> Any:
        if call.name != "lookup":
            return None
        return call.model_copy(update={"args": {"topic": "ledger"}})


class Guard(Hook):
    async def before_tool(self, call: Any, turn: Any) -> Any:
        if call.name == "publish":
            return Denial(reason="drafts are never published unattended")
        return None


class Boom:
    async def embed(self, texts: list[str]) -> list[list[float]]:
        raise RuntimeError("the embedding service is down")


def build(
    store: Store,
    policy: Policy,
    agents: Sequence[type[Agent]] = (Lead,),
    **options: Any,
) -> tuple[Runtime, SyntheticProvider]:
    provider = SyntheticProvider(policy)
    runtime = track_runtime(Runtime(provider, store, list(agents), deps_factory=desk_factory(), **options))
    return runtime, provider


def calls(state: PolicyState, wanted: Sequence[tuple[str, dict[str, Any]]]) -> Sample:
    return Sample(
        tool_calls=[ToolCall(id=state.next_call_id(), name=name, args=json.dumps(args)) for name, args in wanted]
    )


def batch_policy(*wanted: tuple[str, dict[str, Any]], answer: str = "filed") -> Policy:
    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return calls(state, wanted)
        return Sample(text=answer)

    return policy


def results_by_tool(events: Sequence[Any]) -> dict[str, Any]:
    names = {event.call_id: event.name for event in picks(events, "tool_call_requested")}
    return {names[event.call_id]: event for event in picks(events, "tool_call_completed") if event.call_id in names}


async def execute(runtime: Runtime, sid: str, input: str) -> Any:
    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        return await connection.prompt(input, command_id=uuid4())


async def wait_for(store: Store, sid: str, kind: str, count: int = 1) -> Any:
    for _ in range(10_000):
        events = picks(await log(store, sid), kind)
        if len(events) >= count:
            return events[count - 1]
        await asyncio.sleep(0)
    raise AssertionError(f"{kind} was not recorded")


async def vector_capable(store: Store) -> bool:
    search = getattr(store, "memory_search", None)
    if search is None:
        return False
    return await search([0.5] * 8, 1) is not None


async def test_every_hook_fires_over_a_multi_tool_turn() -> None:
    store = MemoryStore()
    recorder = Recorder()
    runtime, provider = build(
        store,
        batch_policy(("lookup", {"topic": "orbit"}), ("tally", {"items": ["a", "b"]})),
        hooks=[recorder],
    )
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "brief me")

    assert recorder.calls[:2] == ["before_turn", "before_sample"]
    assert sorted(recorder.calls[2:6]) == [
        "after_tool:lookup",
        "after_tool:tally",
        "before_tool:lookup",
        "before_tool:tally",
    ]
    assert recorder.calls[-2:] == ["before_sample", "after_turn:turn_completed"]
    assert recorder.events.count("tool_call_requested") == 2
    assert recorder.events.count("tool_call_completed") == 2
    assert recorder.events.count("tool_progress") == 1
    assert recorder.events.count("turn_started") == 1
    assert "text_delta" in recorder.events
    assert "tool_call_delta" in recorder.events
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_before_tool_transform_leaves_the_log_honest() -> None:
    store = MemoryStore()
    runtime, provider = build(store, batch_policy(("lookup", {"topic": "orbit"})), hooks=[Redactor()])
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "brief me")
    events = await log(store, sid)

    assert LEDGER.lookups == ["ledger"]
    assert picks(events, "tool_call_requested")[0].args == {"topic": "orbit"}
    assert picks(events, "tool_call_completed")[0].result == DOCS["ledger"]
    check_pairs(provider.requests)


async def test_before_tool_denial_is_a_guardrail() -> None:
    store = MemoryStore()
    runtime, provider = build(
        store,
        batch_policy(("publish", {"title": "draft"}), ("lookup", {"topic": "ledger"})),
        hooks=[Guard()],
    )
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "ship it")
    events = await log(store, sid)
    denied = results_by_tool(events)["publish"]

    assert LEDGER.published == []
    assert LEDGER.lookups == ["ledger"]
    assert denied.is_error
    assert "denied by hook: drafts are never published unattended" in str(denied.result)
    assert [event.call_id for event in picks(events, "tool_call_started")] == [
        event.call_id for event in picks(events, "tool_call_completed")
    ]
    assert [event.stop_reason for event in picks(events, "turn_completed")] == ["completed"]
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_permission_matrix() -> None:
    class Gatekept(Agent):
        """Runs the desk under a permission matrix."""

        model = MODEL_LEAD
        tools = [lookup, tally, publish, memory_write, memory_recall]
        permissions = {"memory_*": "allow", "memory_write": "deny", "publish": "ask"}

    store = MemoryStore()
    policy = batch_policy(
        ("memory_recall", {"query": "ledger"}),
        ("memory_write", {"kind": "fact", "title": "t", "body": "b"}),
        ("publish", {"title": "draft"}),
        ("tally", {"items": ["a"]}),
    )
    runtime, provider = build(
        store,
        policy,
        agents=(Gatekept,),
        memory=BuiltinMemory(store),
        default_permission="ask",
    )
    sid = (await runtime.create(Gatekept)).hex
    assert publish.permission == "allow"

    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        pending = asyncio.create_task(connection.prompt("go", command_id=uuid4()))
        answered: set[str] = set()
        while len(answered) < 2:
            asks = picks(await log(store, sid), "ask_raised")
            for event in asks:
                permission = str(event.request.extra["permission"])
                if permission in answered:
                    continue
                await connection.answer(
                    UUID(hex=event.ask_id),
                    ApprovalResponse(allow=permission == "publish"),
                    command_id=uuid4(),
                )
                answered.add(permission)
            await asyncio.sleep(0)
        result = await pending

    events = await log(store, sid)
    outcome = results_by_tool(events)
    started = {event.call_id for event in picks(events, "tool_call_started")}
    names = {event.call_id: event.name for event in picks(events, "tool_call_requested")}

    assert result.outcome == "completed"
    assert answered == {"publish", "tally"}
    assert not outcome["memory_recall"].is_error
    assert outcome["memory_write"].is_error
    assert "denied by permissions: memory_write" in str(outcome["memory_write"].result)
    assert not outcome["publish"].is_error
    assert LEDGER.published == ["draft"]
    assert outcome["tally"].is_error
    assert "denied by user" in str(outcome["tally"].result)
    assert LEDGER.tallied == []
    assert sorted(names[call_id] for call_id in started) == ["memory_recall", "memory_write", "publish", "tally"]
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_ask_flavors_are_answered_by_the_live_writer(store: Store) -> None:
    policy = batch_policy(("interview", {"subject": "ledger"}))
    runtime, provider = build(store, policy)
    sid = (await runtime.create(Lead)).hex

    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        pending = asyncio.create_task(connection.prompt("go", command_id=uuid4()))
        free_text = await wait_for(store, sid, "ask_raised")
        assert free_text.request.kind == "free_text"
        await connection.answer(
            UUID(hex=free_text.ask_id),
            FreeTextResponse(text="it balances"),
            command_id=uuid4(),
        )
        choice = await wait_for(store, sid, "ask_raised", 2)
        assert choice.request.kind == "choice"
        assert choice.request.options == ["weak", "solid"]
        await connection.answer(
            UUID(hex=choice.ask_id),
            ChoiceResponse(selected="solid"),
            command_id=uuid4(),
        )
        result = await pending

    events = await log(store, sid)
    completed = picks(events, "tool_call_completed")

    assert result.outcome == "completed"
    assert len(LEDGER.interviews) == 1
    assert len({id(desk) for desk in LEDGER.desks}) == 1
    assert len(picks(events, "tool_progress")) == 1
    assert [str(event.result) for event in completed] == ["ledger: it balances (solid)"]
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_memory_through_tools_and_repair(store: Store) -> None:
    memory = BuiltinMemory(store, embedder=SyntheticEmbedder())
    written = {
        "kind": "fact",
        "title": "orbit ledger key",
        "body": "ORBIT-7741 keys the seventh orbit ledger",
        "entities": ["orbit"],
    }
    policy = by_model(
        {
            MODEL_LEAD: batch_policy(
                ("memory_write", written),
                ("memory_recall", {"query": "orbit ledger key", "k": 3}),
            )
        }
    )
    runtime, provider = build(store, policy, memory=memory)
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "remember this")
    events = await log(store, sid)
    recalled = results_by_tool(events)["memory_recall"].result

    assert len(recalled) == 1
    assert recalled[0]["title"] == "orbit ledger key"
    assert recalled[0]["mode"] in (KEYWORD, VECTOR)
    check_pairs(provider.requests)
    await check_log(store, sid)

    stale = await memory.write(MemoryWrite(kind="fact", title="heron codename", body="codename is GREYHERON"))
    replacement = MemoryWrite(kind="fact", title="heron codename", body="codename is BLUEHERON")
    fresh = await memory.supersede(stale, replacement)
    hits = await memory.recall("heron codename")

    assert [hit.memory.id for hit in hits if hit.memory.id in (stale, fresh)] == [fresh]
    assert (await memory.get(stale)).superseded_by == fresh

    await memory.delete(fresh)
    assert [hit.memory.id for hit in await memory.recall("heron codename") if hit.memory.id == fresh] == []
    assert (await memory.get(fresh)).deleted is True

    broken = BuiltinMemory(store, embedder=Boom())
    limp = await broken.write(MemoryWrite(kind="fact", title="tide table", body="the tide table is unembeddable"))
    assert (await memory.get(limp)).embedding is None

    assert await memory.backfill() >= 1
    assert (await memory.get(limp)).embedding is not None

    probe = await memory.recall("xyzzy quuz frobnicate")
    if await vector_capable(store):
        assert probe != []
        assert {hit.mode for hit in probe} == {VECTOR}
    else:
        assert probe == []
        assert {hit.mode for hit in await memory.recall("tide table")} == {KEYWORD}


@pytest.fixture
def skills_root(tmp_path: Path) -> Path:
    root = tmp_path / "skills"
    notes = root / "desk-notes"
    notes.mkdir(parents=True)
    (notes / "SKILL.md").write_text(
        "---\nname: desk-notes\ndescription: How the desk files its notes.\n---\n# Desk notes\nFile everything.\n",
        encoding="utf-8",
    )
    rules = root / "ledger-rules"
    (rules / "references").mkdir(parents=True)
    (rules / "SKILL.md").write_text(
        "---\nname: ledger-rules\ndescription: >\n  Nightly ledger rules, folded\n  across two source lines.\n"
        "---\n# Ledger rules\nBalance at 02:00 UTC.\n",
        encoding="utf-8",
    )
    (rules / "references" / "rates.md").write_text("overnight rates\n", encoding="utf-8")
    broken = root / "half-written"
    broken.mkdir(parents=True)
    (broken / "SKILL.md").write_text("---\nname: half-written\ndescription: never closes its fence\n", encoding="utf-8")
    return root


async def test_skills_index_load_and_filter(skills_root: Path) -> None:
    catalogue = FileSystemSkills(skills_root)
    index = await catalogue.index()

    assert sorted(info.name for info in index) == ["desk-notes", "ledger-rules"]
    assert [info.description for info in index if info.name == "ledger-rules"] == [FOLDED]
    assert [path.parent.name for path, _ in catalogue.skipped] == ["half-written"]
    assert "fence is never closed" in catalogue.skipped[0][1]

    store = MemoryStore()
    runtime, provider = build(store, batch_policy(("skill", {"name": "ledger-rules"})), skills=catalogue)
    sid = (await runtime.create(Lead)).hex
    await execute(runtime, sid, "read the rules")

    loaded = results_by_tool(await log(store, sid))["skill"]
    assert "Balance at 02:00 UTC." in str(loaded.result)
    assert "references/rates.md" in str(loaded.result)
    assert skill_lines(provider.requests[0]) == [
        "- desk-notes: How the desk files its notes.",
        f"- ledger-rules: {FOLDED}",
    ]

    class Narrow(Agent):
        """Reads only the desk notes."""

        model = MODEL_LEAD
        tools = [lookup]
        skills = ["desk-notes"]
        permissions = {"*": "allow"}

    other = MemoryStore()
    limited, second = build(
        other,
        batch_policy(("skill", {"name": "ledger-rules"})),
        agents=(Narrow,),
        skills=FileSystemSkills(skills_root),
    )
    narrow_sid = (await limited.create(Narrow)).hex
    await execute(limited, narrow_sid, "read the rules")

    refused = results_by_tool(await log(other, narrow_sid))["skill"]
    assert skill_lines(second.requests[0]) == ["- desk-notes: How the desk files its notes."]
    assert refused.is_error
    assert "not available to this agent" in str(refused.result)


def skill_lines(request: SampleRequest) -> list[str]:
    lines: list[str] = []
    for block in request.system:
        lines.extend(line for line in block.text.splitlines() if line.startswith("- "))
    return lines


class Brief(BaseModel):
    topic: str
    findings: list[str]
    confidence: float


async def test_output_schema_carries_the_parsed_value() -> None:
    class Analyst(Agent):
        """Produce a structured brief."""

        model = MODEL_LEAD
        tools = [lookup]
        output_schema = Brief
        permissions = {"*": "allow"}

    payload = {"topic": "orbit", "findings": ["ORBIT-7741"], "confidence": 0.8}

    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return calls(state, [("lookup", {"topic": "orbit"})])
        return calls(state, [("submit_output", payload)])

    store = MemoryStore()
    runtime, provider = build(store, policy, agents=(Analyst,))
    sid = (await runtime.create(Analyst)).hex

    result = await execute(runtime, sid, "brief me")
    replayed = picks(await log(store, sid), "turn_completed")

    assert result.stop_reason == "output"
    assert result.output == payload
    assert replayed[0].output == payload
    assert "submit_output" in [schema.name for schema in provider.requests[0].tools]
    await check_log(store, sid)


async def test_subagent_journals_are_independent_and_fire_hooks() -> None:
    store = MemoryStore()
    recorder = Recorder()

    def checker_policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return calls(state, [("lookup", {"topic": "orbit"})])
        return calls(state, [("finish", {"result": "verified"})])

    policy = by_model(
        {
            MODEL_LEAD: batch_policy(("spawn", {"agent_name": "checker", "input": "verify the orbit key"})),
            MODEL_CHECK: checker_policy,
        }
    )
    runtime, provider = build(store, policy, hooks=[recorder])
    sid = (await runtime.create(Lead)).hex

    result = await execute(runtime, sid, "verify it")
    child_created = picks(await log(store, sid), "child_created")[0]
    child_id = child_created.child_id
    await wait_for(store, child_id, "agent_finished")
    parent_log = await log(store, sid)
    child_log = await log(store, child_id)

    assert result.outcome == "completed"
    assert [event.name for event in picks(parent_log, "tool_call_requested")] == ["spawn"]
    assert picks(parent_log, "tool_progress") == []
    assert len(picks(child_log, "tool_progress")) == 1
    assert recorder.calls.count("before_turn") >= 2
    assert "before_tool:lookup" in recorder.calls
    assert "before_tool:spawn" in recorder.calls
    assert LEDGER.lookups == ["orbit"]
    check_pairs(provider.requests)
    await check_log(store, sid)
    await check_log(store, child_id)


async def test_connection_cancel_stops_a_live_tool_batch() -> None:
    started = asyncio.Event()

    @tool
    async def hold() -> str:
        """Wait until cancelled."""
        started.set()
        await asyncio.Event().wait()
        return "impossible"

    class Blocked(Agent):
        model = MODEL_LEAD
        tools = [hold]
        permissions = {"*": "allow"}

    store = MemoryStore()
    runtime, provider = build(store, batch_policy(("hold", {}), ("hold", {})), agents=(Blocked,))
    sid = (await runtime.create(Blocked)).hex

    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        command_id = uuid4()
        pending = asyncio.create_task(connection.prompt("wait", command_id=command_id))
        await asyncio.wait_for(started.wait(), timeout=2)
        await connection.cancel(command_id=uuid4())
        result = await pending

    events = await log(store, sid)
    assert result.outcome == "cancelled"
    assert len(picks(events, "turn_cancelled")) == 1
    assert len(provider.requests) == 1
    await check_log(store, sid)


async def test_max_steps_answers_every_orphaned_call() -> None:
    class Capped(Agent):
        """Gets exactly one sample."""

        model = MODEL_LEAD
        tools = [lookup, tally]
        max_steps = 1
        permissions = {"*": "allow"}

    store = MemoryStore()
    runtime, _ = build(
        store,
        batch_policy(("lookup", {"topic": "orbit"}), ("tally", {"items": ["a"]})),
        agents=(Capped,),
    )
    sid = (await runtime.create(Capped)).hex

    await execute(runtime, sid, "go")
    events = await log(store, sid)

    assert [event.stop_reason for event in picks(events, "turn_completed")] == ["max_steps"]
    assert len(picks(events, "tool_call_requested")) == 2
    assert all(event.is_error for event in picks(events, "tool_call_completed"))
    assert all("not executed: max steps reached" in str(event.result) for event in picks(events, "tool_call_completed"))
    assert [event.call_id for event in picks(events, "tool_call_started")] == [
        event.call_id for event in picks(events, "tool_call_completed")
    ]
    assert LEDGER.lookups == []
    assert pairs_intact(events) == 2
    await check_log(store, sid)


async def test_invalid_tool_json_becomes_an_error_result() -> None:
    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name="lookup", args="{not json")])
        return Sample(text="recovered")

    store = MemoryStore()
    runtime, provider = build(store, policy)
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "go")
    events = await log(store, sid)
    rejected = picks(events, "tool_call_completed")[0]

    assert rejected.is_error
    assert "invalid JSON arguments" in str(rejected.result)
    assert LEDGER.lookups == []
    assert [event.call_id for event in picks(events, "tool_call_started")] == [rejected.call_id]
    assert [event.stop_reason for event in picks(events, "turn_completed")] == ["completed"]
    assert len(provider.requests) == 2
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_one_transient_failure_is_retried_away() -> None:
    store = MemoryStore()
    policy = flaky(batch_policy(("lookup", {"topic": "orbit"})), fail_on=[2])
    runtime, provider = build(store, policy)
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "go")
    events = await log(store, sid)

    assert len(provider.requests) == 3
    assert [event.stop_reason for event in picks(events, "turn_completed")] == ["completed"]
    assert picks(events, "turn_failed") == []
    assert len(picks(events, "sample_started")) == 2
    check_pairs(provider.requests)
    await check_log(store, sid)


async def test_exhausted_retries_fail_the_turn_and_a_later_prompt_recovers() -> None:
    store = MemoryStore()
    policy = flaky(batch_policy(("lookup", {"topic": "orbit"})), fail_on=[2, 3, 4])
    runtime, provider = build(store, policy, retry=RetryConfig(max_attempts=3, base_delay=0.01))
    sid = (await runtime.create(Lead)).hex

    await execute(runtime, sid, "go")
    events = await log(store, sid)

    assert len(picks(events, "turn_failed")) == 1
    assert "synthetic transient failure" in picks(events, "turn_failed")[0].error
    assert event_type(events[-1]) == "turn_failed"
    assert event_type(events[-2]) == "sample_started"
    assert len(provider.requests) == 4

    await execute(runtime, sid, "try again")
    events = await log(store, sid)

    assert [event.stop_reason for event in picks(events, "turn_completed")] == ["completed"]
    assert LEDGER.lookups == ["orbit", "orbit"]
    assert len(picks(events, "turn_started")) == 2
    await check_log(store, sid)
