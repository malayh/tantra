from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg import sql
from pydantic import BaseModel

from stress.driver import SyntheticProvider, blob, is_summarize, last_user, turn_step
from stress.invariants import check_log, check_pairs, pairs_intact
from tantra import Agent, ApprovalResponse, CompactionConfig, Context, ModelLimits, Sample, tool
from tantra.events import (
    AskAnswered,
    AskRaised,
    CompactionApplied,
    InputQueued,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    TurnCompleted,
)
from tantra.providers.base import SampleRequest, ToolCall

VERSION = "behavior-v1"
SCENARIOS = (
    "sql_read",
    "approved_write",
    "denied_write",
    "skill",
    "structured_result",
    "child_completion",
    "compacted_recall",
)
MARKER = "ARCHIVE-MARKER-731"
ARCHIVE_MARKERS = (MARKER, "FILLER-ONE", "FILLER-TWO", "FILLER-THREE")
SKILL_RESULT = "CERULEAN-17"
SKILL_NAME = "settlement-rule"
RECALL_INPUT = "Recall the original archival marker from the first archive turn. Reply with only that marker."
COMPACT_ARCHIVE_INPUTS = tuple(
    f"Archive turn {index} marker {marker}. Load the settlement skill, then call behavior_archive_blob "
    f"with marker {marker} and size 12000."
    for index, marker in enumerate(ARCHIVE_MARKERS)
)
COMPACT_LIMITS = ModelLimits(context_window=16_000, max_output=4_096)
CHILD_INPUT = "Read and finish the total."
CHILD_PARENT_RESULTS = ("60", '"60"')
CHILD_PROMPT = (
    "Call behavior_gated_fixture_stats exactly once. Then call finish exactly once with the numeric total. "
    "Do not call send."
)
CHILD_PARENT_PROMPT = (
    f'Spawn exactly one fixture_child with input "{CHILD_INPUT}" and omit name. After spawn returns, reply exactly '
    "WAITING without calling status or send. When the finished child result arrives as a new input, reply with only "
    "that result."
)
COMPACTION = CompactionConfig(
    buffer=1_024,
    prune_pool_min=100_000,
    prune_gain_min=50_000,
    tail_turns=1,
    summarize_at=0.75,
    trigger_at=0.80,
    recent_tokens=2_500,
    summary_max_output=4_096,
)
SKILL_BODY = f"""---
name: {SKILL_NAME}
description: Settlement code derivation rule.
---
# Settlement rule

The exact settlement code is {SKILL_RESULT}.
When asked for the settlement code, return that code exactly.
"""
PROMPTS = {
    "sql_read": "Call behavior_fixture_stats exactly once. Reply with only the numeric total.",
    "approved_write": (
        "Call behavior_bounded_write exactly once with operation_key approved-write-v1 and value 7. "
        "After approval and success, reply exactly WRITE-OK."
    ),
    "denied_write": (
        "Call behavior_bounded_write exactly once with operation_key denied-write-v1 and value 9. "
        "If denied, reply exactly WRITE-DENIED."
    ),
    "skill": (
        f"Load the {SKILL_NAME} skill. Apply its rule and reply with only the exact settlement code. "
        "The code is not present outside that skill."
    ),
    "structured_result": ("Call behavior_fixture_stats exactly once, then submit the typed total and row count."),
    "child_completion": (
        "Spawn exactly one fixture_child to calculate the fixture total. Wait for its explicit finish result, "
        "then reply with only that total."
    ),
    "compacted_recall": (
        "Preserve the original marker from the first archive turn and loaded settlement rules across compaction. "
        "Follow each input exactly."
    ),
}
INPUTS = {
    "sql_read": "Read the fixture total now.",
    "approved_write": "Perform the approved bounded operation.",
    "denied_write": "Attempt the bounded operation and handle denial safely.",
    "skill": "Use the named skill to derive the settlement code.",
    "structured_result": "Return the typed fixture statistics.",
    "child_completion": "Delegate this fixture total check to one child.",
    "compacted_recall": {
        "archive": [
            f"Archive turn {index} marker {marker}. Load the settlement skill and archive the fixed record."
            for index, marker in enumerate(ARCHIVE_MARKERS)
        ],
        "recall": RECALL_INPUT,
    },
}
FIXTURE_MANIFEST = {
    "version": VERSION,
    "fixture": [10, 20, 30],
    "write": {"approved-write-v1": 7, "denied-write-v1": 9},
    "skill": SKILL_BODY,
    "marker": MARKER,
    "prompts": PROMPTS,
    "inputs": INPUTS,
    "schema": {"total": "integer", "count": "integer"},
    "compaction": asdict(COMPACTION),
}


class BehaviorTotal(BaseModel):
    total: int
    count: int


class ModelBehaviorError(AssertionError):
    pass


class OracleError(AssertionError):
    pass


def behavior_fixture_identity(scenario: str, case: str = "main") -> str:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown behavior scenario: {scenario}")
    tools = (behavior_fixture_stats, behavior_bounded_write, behavior_archive_blob)
    payload = {
        "scenario": scenario,
        **FIXTURE_MANIFEST,
        "tool_schemas": [entry.schema.model_dump(mode="json") for entry in tools],
        "output_schema": BehaviorTotal.model_json_schema(),
    }
    if scenario == "compacted_recall":
        payload["scenario_inputs"] = {"archive": COMPACT_ARCHIVE_INPUTS, "recall": RECALL_INPUT}
    if scenario == "child_completion":
        payload["child_fixture"] = {
            "child_prompt": CHILD_PROMPT,
            "parent_prompt": CHILD_PARENT_PROMPT,
            "child_input": CHILD_INPUT,
            "child_tool": behavior_gated_fixture_stats.schema.model_dump(mode="json"),
            "permissions": {"parent": {"spawn": "allow", "status": "deny", "send": "deny"}},
        }
    if case != "main":
        payload["case"] = case
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def behavior_session_id(scenario: str, trial: int = 0, case: str = "main") -> UUID:
    return uuid5(NAMESPACE_URL, f"tantra-bench/{VERSION}/{scenario}/{trial}/{case}/session")


def behavior_command_id(scenario: str, trial: int = 0, label: str = "turn") -> UUID:
    return uuid5(behavior_session_id(scenario, trial), label)


def prepare_behavior_skills(schema: str) -> Path:
    root = Path("/tmp") / f"tantra-bench-{schema}-skills"
    skill = root / SKILL_NAME
    skill.mkdir(parents=True, exist_ok=True)
    (skill / "SKILL.md").write_text(SKILL_BODY, encoding="utf-8")
    return root


async def seed_behavior(dsn: str, schema: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.behavior_effects ("
                "operation_key text PRIMARY KEY, value integer NOT NULL CHECK (value BETWEEN 0 AND 100))"
            ).format(sql.Identifier(schema))
        )
        await conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {}.behavior_audit ("
                "operation_key text PRIMARY KEY, action text NOT NULL, value integer NOT NULL, "
                "FOREIGN KEY (operation_key) REFERENCES {}.behavior_effects(operation_key))"
            ).format(sql.Identifier(schema), sql.Identifier(schema))
        )


async def _read_fixture_stats(ctx: Context) -> dict[str, int]:
    dsn, schema, slots = ctx.deps
    async with slots, await psycopg.AsyncConnection.connect(dsn) as conn:
        query = sql.SQL("SELECT count(*), sum(amount) FROM {}.fixture").format(sql.Identifier(schema))
        row = await (await conn.execute(query)).fetchone()
    return {"count": int(row[0]), "total": int(row[1])}


@tool(description="Read the row count and sum from the fixed three-row benchmark fixture.")
async def behavior_fixture_stats(ctx: Context) -> dict[str, int]:
    return await _read_fixture_stats(ctx)


_CHILD_GATES: dict[str, asyncio.Event] = {}


@tool(description="Read the fixed fixture statistics after the parent has completed its initial turn.")
async def behavior_gated_fixture_stats(ctx: Context) -> dict[str, int]:
    _, schema, _ = ctx.deps
    await _CHILD_GATES.setdefault(schema, asyncio.Event()).wait()
    return await _read_fixture_stats(ctx)


@tool(description="Apply one fixed, audited benchmark operation with an idempotency key.")
async def behavior_bounded_write(operation_key: str, value: int, ctx: Context) -> dict[str, Any]:
    allowed = {"approved-write-v1": 7, "denied-write-v1": 9}
    if allowed.get(operation_key) != value:
        raise ValueError("unknown bounded benchmark operation")
    dsn, schema, slots = ctx.deps
    async with slots, await psycopg.AsyncConnection.connect(dsn) as conn:
        inserted = await conn.execute(
            sql.SQL(
                "INSERT INTO {}.behavior_effects (operation_key, value) VALUES (%s, %s) "
                "ON CONFLICT (operation_key) DO NOTHING RETURNING operation_key"
            ).format(sql.Identifier(schema)),
            (operation_key, value),
        )
        created = await inserted.fetchone() is not None
        if created:
            await conn.execute(
                sql.SQL("INSERT INTO {}.behavior_audit (operation_key, action, value) VALUES (%s, %s, %s)").format(
                    sql.Identifier(schema)
                ),
                (operation_key, "bounded_write", value),
            )
        await conn.commit()
    return {"operation_key": operation_key, "value": value, "created": created}


@tool(description="Return a fixed-size archival benchmark record containing the supplied marker.")
async def behavior_archive_blob(marker: str, size: int) -> str:
    allowed = set(ARCHIVE_MARKERS)
    if marker not in allowed or size != 12_000:
        raise ValueError("unknown archival benchmark record")
    prefix = f"retained archival marker: {marker}\n"
    return prefix + blob(size - len(prefix), tag=marker)


class SqlReadAgent(Agent):
    prompt = PROMPTS["sql_read"]
    tools = [behavior_fixture_stats]
    permissions = {"behavior_fixture_stats": "allow"}
    max_steps = 6


class ApprovedWriteAgent(Agent):
    prompt = PROMPTS["approved_write"]
    tools = [behavior_bounded_write]
    permissions = {"behavior_bounded_write": "ask"}
    max_steps = 6


class DeniedWriteAgent(Agent):
    prompt = PROMPTS["denied_write"]
    tools = [behavior_bounded_write]
    permissions = {"behavior_bounded_write": "ask"}
    max_steps = 6


class SkillAgent(Agent):
    prompt = PROMPTS["skill"]
    skills = [SKILL_NAME]
    permissions = {"skill": "allow"}
    max_steps = 6


class StructuredResultAgent(Agent):
    prompt = PROMPTS["structured_result"]
    tools = [behavior_fixture_stats]
    permissions = {"behavior_fixture_stats": "allow"}
    output_schema = BehaviorTotal
    max_steps = 6


class FixtureChild(Agent):
    prompt = CHILD_PROMPT
    tools = [behavior_gated_fixture_stats]
    skills = []
    permissions = {"behavior_gated_fixture_stats": "allow", "send": "deny", "finish": "allow"}
    max_steps = 6


class ChildCompletionAgent(Agent):
    prompt = CHILD_PARENT_PROMPT
    subagents = [FixtureChild]
    skills = []
    permissions = {"spawn": "allow", "status": "deny", "send": "deny"}
    max_steps = 6


class CompactedRecallAgent(Agent):
    prompt = PROMPTS["compacted_recall"]
    tools = [behavior_archive_blob]
    skills = [SKILL_NAME]
    permissions = {"behavior_archive_blob": "allow", "skill": "allow"}
    max_steps = 6


BEHAVIOR_AGENTS = [
    SqlReadAgent,
    ApprovedWriteAgent,
    DeniedWriteAgent,
    SkillAgent,
    StructuredResultAgent,
    ChildCompletionAgent,
    CompactedRecallAgent,
]
AGENTS = {
    "sql_read": SqlReadAgent,
    "approved_write": ApprovedWriteAgent,
    "denied_write": DeniedWriteAgent,
    "skill": SkillAgent,
    "structured_result": StructuredResultAgent,
    "child_completion": ChildCompletionAgent,
    "compacted_recall": CompactedRecallAgent,
}


def _call(state: Any, name: str, args: dict[str, Any]) -> Sample:
    return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name=name, args=json.dumps(args, sort_keys=True))])


def behavior_policy(request: SampleRequest, state: Any) -> Sample:
    user = last_user(request)
    step = turn_step(request)
    if is_summarize(request):
        text = "\n".join(str(getattr(message, "content", "")) for message in request.messages)
        markers = [value for value in (MARKER, SKILL_RESULT) if value in text]
        return Sample(
            text=(
                "## Goal\nRetain benchmark evidence.\n\n## Constraints\nDeterministic.\n\n"
                "## Progress\nArchive compacted.\n\n## Key Decisions\nPreserve exact markers.\n\n"
                "## Next Steps\nRecall on request.\n\n## Critical Context\n" + "\n".join(markers)
            )
        )
    if user == INPUTS["sql_read"]:
        return _call(state, "behavior_fixture_stats", {}) if step == 0 else Sample(text="60")
    if user == INPUTS["approved_write"]:
        args = {"operation_key": "approved-write-v1", "value": 7}
        return _call(state, "behavior_bounded_write", args) if step == 0 else Sample(text="WRITE-OK")
    if user == INPUTS["denied_write"]:
        args = {"operation_key": "denied-write-v1", "value": 9}
        return _call(state, "behavior_bounded_write", args) if step == 0 else Sample(text="WRITE-DENIED")
    if user == INPUTS["skill"]:
        return _call(state, "skill", {"name": SKILL_NAME}) if step == 0 else Sample(text=SKILL_RESULT)
    if user == INPUTS["structured_result"]:
        if step == 0:
            return _call(state, "behavior_fixture_stats", {})
        return _call(state, "submit_output", {"total": 60, "count": 3})
    if user == INPUTS["child_completion"]:
        if step == 0:
            return _call(state, "spawn", {"agent_name": "fixture_child", "input": CHILD_INPUT})
        return Sample(text="WAITING")
    if user == CHILD_INPUT:
        if step == 0:
            return _call(state, "behavior_gated_fixture_stats", {})
        return _call(state, "finish", {"result": "60"})
    if user.startswith("[agent ") and " finished] " in user:
        result = json.loads(user.split(" finished] ", 1)[1])
        if result in (60, "60"):
            return Sample(text="60")
    if user.startswith("Archive turn "):
        marker = user.split(" marker ", 1)[1].split(". ", 1)[0]
        if step == 0:
            return Sample(
                tool_calls=[
                    ToolCall(id=state.next_call_id(), name="skill", args=json.dumps({"name": SKILL_NAME})),
                    ToolCall(
                        id=state.next_call_id(),
                        name="behavior_archive_blob",
                        args=json.dumps({"marker": marker, "size": 12_000}, sort_keys=True),
                    ),
                ]
            )
        return Sample(text="archived")
    if user == RECALL_INPUT:
        return Sample(text=MARKER)
    raise ModelBehaviorError(f"synthetic policy received unknown input: {user!r}")


@dataclass
class BackgroundTurn:
    sid: str
    command: str
    connection: Any
    task: asyncio.Task[Any]


class BehaviorState:
    def __init__(self, runtime: Any, provider: Any) -> None:
        self.runtime = runtime
        self.provider = provider
        self.turns: dict[str, BackgroundTurn] = {}

    async def operation(self, request: dict[str, Any]) -> dict[str, Any] | None:
        op = request["op"]
        if op == "behavior_setup":
            scenario = request["scenario"]
            trial = request.get("trial", 0)
            case = request.get("case", "main")
            sid = behavior_session_id(scenario, trial, case)
            if await self.runtime.store.header(sid.hex) is None:
                await self.runtime.create(AGENTS[scenario], session_id=sid)
            if scenario == "child_completion":
                _CHILD_GATES.setdefault(self.runtime.store.schema, asyncio.Event()).clear()
            self.provider.limit_override = COMPACT_LIMITS if scenario == "compacted_recall" else None
            source = self.provider.source
            if isinstance(source, SyntheticProvider):
                source.policy = behavior_policy
            if hasattr(source, "fixture_identity"):
                source.fixture_identity = behavior_fixture_identity(scenario, case)
            return {
                "sid": sid.hex,
                "command": uuid5(sid, "turn").hex,
                "fixture_identity": behavior_fixture_identity(scenario, case),
            }
        if op == "behavior_start":
            name = request["name"]
            if name in self.turns:
                raise ValueError(f"background turn already exists: {name}")
            sid = request["sid"]
            connection = self.runtime.connect(UUID(hex=sid), writable=True)
            await connection.__aenter__()
            task = asyncio.create_task(connection.prompt(request["input"], command_id=UUID(hex=request["command"])))
            self.turns[name] = BackgroundTurn(sid, request["command"], connection, task)
            return {"started": name, "sid": sid, "command": request["command"]}
        if op == "behavior_wait":
            return await self._wait(request)
        if op == "behavior_answer":
            turn = self.turns[request["name"]]
            events = await self._events(turn.sid)
            asks = [event for event in events if isinstance(event, AskRaised)]
            if len(asks) != 1:
                raise OracleError(f"expected one durable ask, found {len(asks)}")
            receipt = await turn.connection.answer(
                UUID(hex=asks[0].ask_id),
                ApprovalResponse(allow=request["allow"]),
                command_id=UUID(hex=request["answer_command"]),
            )
            return asdict(receipt)
        if op == "behavior_cancel":
            turn = self.turns[request["name"]]
            receipt = await turn.connection.cancel(command_id=UUID(hex=request["cancel_command"]))
            return asdict(receipt)
        if op == "behavior_result":
            turn = self.turns.pop(request["name"])
            try:
                result = await turn.task
                return asdict(result)
            finally:
                await turn.connection.__aexit__()
        if op == "behavior_evidence":
            return await self._evidence(request)
        if op == "behavior_cleanup":
            await self.close()
            return {"background_turns": 0}
        return None

    async def _events(self, sid: str) -> list[Any]:
        return [item.event async for item in self.runtime.store.read(sid)]

    async def _effects(self, key: str) -> dict[str, Any]:
        async with await psycopg.AsyncConnection.connect(self.runtime.store.dsn) as conn:
            schema = self.runtime.store.schema
            effect_rows = await (
                await conn.execute(
                    sql.SQL("SELECT value FROM {}.behavior_effects WHERE operation_key = %s ORDER BY value").format(
                        sql.Identifier(schema)
                    ),
                    (key,),
                )
            ).fetchall()
            audit_rows = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT action, value FROM {}.behavior_audit WHERE operation_key = %s ORDER BY action, value"
                    ).format(sql.Identifier(schema)),
                    (key,),
                )
            ).fetchall()
        return {
            "effects": len(effect_rows),
            "audits": len(audit_rows),
            "effect_values": [int(row[0]) for row in effect_rows],
            "audit_rows": [{"action": str(row[0]), "value": int(row[1])} for row in audit_rows],
        }

    async def _wait(self, request: dict[str, Any]) -> dict[str, Any]:
        name = request["name"]
        turn = self.turns.get(name)
        sid = request.get("sid") or (turn.sid if turn is not None else "")
        gate = request["gate"]
        async with asyncio.timeout(120):
            while True:
                events = await self._events(sid)
                if gate == "ask" and any(isinstance(event, AskRaised) for event in events):
                    key = request["operation_key"]
                    effects = await self._effects(key)
                    if effects["effects"] or effects["audits"]:
                        raise OracleError("write occurred before approval evidence")
                    return {"gate": gate, **effects}
                if gate == "compaction" and any(isinstance(event, CompactionApplied) for event in events):
                    event = next(event for event in reversed(events) if isinstance(event, CompactionApplied))
                    return {"gate": gate, "summary": event.summary, "floor_turn_id": event.floor_turn_id}
                if gate == "child_finished":
                    children = await self.runtime.store.list(parent_id=sid, limit=2)
                    if len(children) == 1:
                        child_events = await self._events(children[0].id)
                        finished = [event for event in child_events if event.type == "agent_finished"]
                        if finished:
                            return {"gate": gate, "child_id": children[0].id, "result": finished[0].result}
                        terminal = [
                            event
                            for event in child_events
                            if event.type in ("turn_completed", "turn_failed", "turn_cancelled", "turn_interrupted")
                        ]
                        if terminal:
                            event = terminal[-1]
                            detail = getattr(event, "error", None) or getattr(event, "stop_reason", None)
                            raise OracleError(f"child ended without explicit finish: {event.type}: {detail}")
                if gate == "parent_waiting" and turn is not None:
                    completed = [
                        event for event in events if isinstance(event, TurnCompleted) and event.turn_id == turn.command
                    ]
                    texts = [event.text.strip() for event in events if isinstance(event, TextPart)]
                    if len(completed) == 1 and texts and texts[-1] == "WAITING":
                        _CHILD_GATES.setdefault(self.runtime.store.schema, asyncio.Event()).set()
                        return {"gate": gate, "text": texts[-1]}
                if gate == "parent_final":
                    lifecycle = [
                        (index, event)
                        for index, event in enumerate(events)
                        if isinstance(event, InputQueued)
                        and event.input.startswith("[agent ")
                        and " finished]" in event.input
                    ]
                    completed = [
                        (index, event)
                        for index, event in enumerate(events)
                        if isinstance(event, TurnCompleted)
                        and lifecycle
                        and event.turn_id == lifecycle[-1][1].command_id
                    ]
                    if lifecycle and completed:
                        start, terminal = lifecycle[-1][0], completed[-1]
                        texts = [event.text for event in events[start : terminal[0]] if isinstance(event, TextPart)]
                        text = texts[-1] if texts else ""
                        if text not in CHILD_PARENT_RESULTS:
                            raise ModelBehaviorError(f"child parent returned {text!r}, expected 60")
                        return {
                            "gate": gate,
                            "lifecycle": len(lifecycle),
                            "text": text,
                            "turn_id": terminal[1].turn_id,
                        }
                if gate == "terminal" and turn is not None and turn.task.done():
                    return {"gate": gate}
                await asyncio.sleep(0.01)

    async def _fixture_stats(self) -> dict[str, int]:
        async with await psycopg.AsyncConnection.connect(self.runtime.store.dsn) as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT count(*), sum(amount) FROM {}.fixture").format(
                        sql.Identifier(self.runtime.store.schema)
                    )
                )
            ).fetchone()
        return {"count": int(row[0]), "total": int(row[1])}

    async def _evidence(self, request: dict[str, Any]) -> dict[str, Any]:
        scenario = request["scenario"]
        sid = request["sid"]
        events = await self._events(sid)
        await check_log(self.runtime.store, sid)
        if not request.get("unanswered"):
            pairs_intact(events)
        stats = await self._fixture_stats()
        if stats != {"count": 3, "total": 60}:
            raise OracleError(f"fixture changed: {stats}")
        calls = [event.name for event in events if isinstance(event, ToolCallRequested)]
        results = [event for event in events if isinstance(event, ToolCallCompleted)]
        call_names = {event.call_id: event.name for event in events if isinstance(event, ToolCallRequested)}
        raw_texts = [event.text for event in events if isinstance(event, TextPart)]
        texts = [text.strip() for text in raw_texts]
        evidence: dict[str, Any] = {"fixture": stats, "tool_calls": calls, "last_text": texts[-1] if texts else ""}
        if scenario == "sql_read":
            reads = [
                event
                for event in results
                if call_names.get(event.call_id) == "behavior_fixture_stats" and not event.is_error
            ]
            if calls != ["behavior_fixture_stats"] or [event.result for event in reads] != [stats]:
                raise OracleError(f"SQL read tool result did not match fixture: {reads}")
            if evidence["last_text"] != "60":
                raise ModelBehaviorError(f"SQL read did not return one exact total: {evidence}")
        elif scenario == "approved_write":
            effects = await self._effects("approved-write-v1")
            inputs = [
                event
                for event in events
                if isinstance(event, InputQueued) and event.command_id == behavior_command_id(scenario).hex
            ]
            asks = [event for event in events if isinstance(event, AskRaised)]
            answers = [event for event in events if isinstance(event, AskAnswered)]
            expected = {
                "effects": 1,
                "audits": 1,
                "effect_values": [7],
                "audit_rows": [{"action": "bounded_write", "value": 7}],
            }
            if effects != expected or len(inputs) != 1 or len(asks) != 1 or len(answers) != 1:
                raise OracleError(f"approved write effect is not idempotent and audited: {effects}")
            if calls != ["behavior_bounded_write"] or evidence["last_text"] != "WRITE-OK":
                raise ModelBehaviorError(f"approved write response was {evidence['last_text']!r}")
            evidence.update({**effects, "command_inputs": len(inputs), "asks": len(asks), "answers": len(answers)})
        elif scenario == "denied_write":
            effects = await self._effects("denied-write-v1")
            if request.get("unanswered"):
                asks = [event for event in events if isinstance(event, AskRaised)]
                answers = [event for event in events if isinstance(event, AskAnswered)]
                if effects["effects"] or effects["audits"] or len(asks) != 1 or answers:
                    raise OracleError(
                        f"unanswered approval changed effects or acquired an answer: {effects}, "
                        f"asks={len(asks)}, answers={len(answers)}"
                    )
                evidence.update(effects)
                evidence["unanswered_ask"] = asks[0].ask_id
                return evidence
            denied = [event for event in results if event.is_error and "denied by user" in str(event.result)]
            if effects["effects"] or effects["audits"] or len(denied) != 1 or calls != ["behavior_bounded_write"]:
                raise OracleError(
                    f"denied write produced effects or wrong tool evidence: {effects}, denied={len(denied)}"
                )
            if evidence["last_text"] != "WRITE-DENIED":
                raise ModelBehaviorError(f"denied write response was {evidence['last_text']!r}")
            evidence.update(effects)
        elif scenario == "skill":
            skill_results = [
                event for event in results if calls and not event.is_error and SKILL_RESULT in str(event.result)
            ]
            if calls != ["skill"] or len(skill_results) != 1 or evidence["last_text"] != SKILL_RESULT:
                raise ModelBehaviorError(f"skill rule was not loaded and applied exactly: {evidence}")
            evidence["skill_result"] = SKILL_RESULT
        elif scenario == "structured_result":
            terminals = [event for event in events if event.type == "turn_completed"]
            output = terminals[-1].output if terminals else None
            reads = [
                event
                for event in results
                if call_names.get(event.call_id) == "behavior_fixture_stats" and not event.is_error
            ]
            if calls != ["behavior_fixture_stats", "submit_output"] or [event.result for event in reads] != [stats]:
                raise OracleError(f"structured SQL tool result did not match fixture: {reads}")
            if output != stats:
                raise ModelBehaviorError(f"typed output did not match SQL evidence: {output!r} != {stats!r}")
            evidence["output"] = output
        elif scenario == "child_completion":
            parent_text = raw_texts[-1] if raw_texts else ""
            children = await self.runtime.store.list(parent_id=sid, limit=2)
            lifecycle = [
                event
                for event in events
                if isinstance(event, InputQueued) and event.input.startswith("[agent ") and " finished]" in event.input
            ]
            initial = [
                index
                for index, event in enumerate(events)
                if isinstance(event, TurnCompleted) and event.turn_id == behavior_command_id(scenario).hex
            ]
            lifecycle_index = [index for index, event in enumerate(events) if event in lifecycle]
            if (
                calls != ["spawn"]
                or len(children) != 1
                or len(lifecycle) != 1
                or len(initial) != 1
                or initial[0] >= lifecycle_index[0]
            ):
                raise OracleError(
                    f"child lifecycle was not exactly once with final total: children={len(children)}, "
                    f"lifecycle={len(lifecycle)}, calls={calls}, text={parent_text!r}"
                )
            if parent_text not in CHILD_PARENT_RESULTS:
                raise ModelBehaviorError(f"child parent returned {parent_text!r}, expected 60")
            child_events = await self._events(children[0].id)
            finished = [event for event in child_events if event.type == "agent_finished"]
            child_calls = [event.name for event in child_events if isinstance(event, ToolCallRequested)]
            child_names = {event.call_id: event.name for event in child_events if isinstance(event, ToolCallRequested)}
            child_reads = [
                event
                for event in child_events
                if isinstance(event, ToolCallCompleted)
                and child_names.get(event.call_id) == "behavior_gated_fixture_stats"
                and not event.is_error
            ]
            if (
                len(finished) != 1
                or finished[0].result not in (60, "60")
                or child_calls != ["behavior_gated_fixture_stats", "finish"]
                or [event.result for event in child_reads] != [stats]
            ):
                raise OracleError(
                    f"child did not read the fixture and explicitly finish once with 60: "
                    f"reads={child_reads}, finished={finished}"
                )
            await check_log(self.runtime.store, children[0].id)
            pairs_intact(child_events)
            evidence.update(
                {
                    "child_id": children[0].id,
                    "lifecycle": 1,
                    "child_result": finished[0].result,
                    "child_stats": child_reads[0].result,
                    "parent_text": parent_text,
                }
            )
        elif scenario == "compacted_recall":
            applied = [event for event in events if isinstance(event, CompactionApplied)]
            skill_results = [event for event in results if SKILL_RESULT in str(event.result)]
            if not applied or MARKER not in applied[-1].summary or not skill_results:
                raise OracleError("compaction summary did not retain marker and skill evidence")
            snapshot = await self.runtime.store.read_compacted(sid)
            retained = [item.event for item in snapshot.items]
            retained_skill = [
                event
                for event in retained
                if isinstance(event, ToolCallCompleted) and SKILL_RESULT in str(event.result)
            ]
            if not retained_skill:
                raise OracleError("compacted retained window lost skill content")
            summary_requests = sum(is_summarize(item) for item in self.provider.requests)
            if not request.get("recalled") and summary_requests < 1:
                raise OracleError("compaction did not issue a metered summary request")
            if request.get("recalled") and evidence["last_text"] != MARKER:
                raise ModelBehaviorError(f"compacted recall returned {evidence['last_text']!r}")
            evidence.update(
                {
                    "compactions": len(applied),
                    "summary_has_marker": MARKER in applied[-1].summary,
                    "skill_results": len(skill_results),
                    "retained_events": len(retained),
                    "retained_skill_content": True,
                    "summary_requests": summary_requests,
                }
            )
        if self.provider.requests:
            check_pairs(self.provider.requests, min_pairs=0)
        return evidence

    async def close(self) -> None:
        turns = list(self.turns.values())
        self.turns.clear()
        for turn in turns:
            if not turn.task.done():
                try:
                    await turn.connection.cancel(command_id=uuid5(UUID(hex=turn.command), "cleanup"))
                except Exception:
                    turn.task.cancel()
        if turns:
            await asyncio.gather(*(turn.task for turn in turns), return_exceptions=True)
            await asyncio.gather(*(turn.connection.__aexit__() for turn in turns), return_exceptions=True)
        _CHILD_GATES.pop(self.runtime.store.schema, None)


def _classification(error: str) -> str:
    if "ModelBehaviorError" in error:
        return "model_behavior"
    if "OracleError" in error or "AssertionError" in error:
        return "oracle"
    if "ProviderError" in error or "RecordedProvider" in error or "recording" in error.lower():
        return "provider"
    return "runtime"


def run_behavior(
    report: dict[str, Any],
    workers: list[Any],
    settings: dict[str, Any],
    args: Any,
    measured: Any,
    Worker: Any,
) -> None:
    selected = list(args.scenario or SCENARIOS)
    report.setdefault("scenarios", [])
    for scenario in selected:
        try:
            evidence = _run_scenario(scenario, report, workers, settings, measured, Worker)
            report["scenarios"].append(
                {"name": scenario, "status": "passed", "classification": "model_behavior", "evidence": evidence}
            )
            print(f"{scenario}: PASSED", flush=True)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            classification = _classification(error)
            report["scenarios"].append(
                {"name": scenario, "status": "failed", "classification": classification, "error": error}
            )
            print(f"{scenario}: FAILED [{classification}] {error}", flush=True)
            try:
                measured(report, workers[0], f"{scenario}/cleanup", "behavior_cleanup")
            except Exception:
                pass


def _run_scenario(
    scenario: str,
    report: dict[str, Any],
    workers: list[Any],
    settings: dict[str, Any],
    measured: Any,
    Worker: Any,
) -> dict[str, Any]:
    worker = workers[0]

    def call(label: str, operation: str, **kwargs: Any) -> dict[str, Any]:
        return measured(report, worker, f"{scenario}/{label}", operation, **kwargs)

    setup = call("setup", "behavior_setup", scenario=scenario, trial=0)
    sid = setup["sid"]
    if scenario == "compacted_recall":
        return _run_compaction(report, workers, settings, measured, Worker, setup)
    command = setup["command"]
    call("start", "behavior_start", name="main", sid=sid, command=command, input=INPUTS[scenario])
    if scenario in ("approved_write", "denied_write"):
        key = f"{scenario.replace('_', '-')}-v1"
        call("ask", "behavior_wait", name="main", gate="ask", operation_key=key)
        call(
            "answer",
            "behavior_answer",
            name="main",
            allow=scenario == "approved_write",
            answer_command=uuid5(UUID(hex=command), "approval").hex,
        )
    if scenario == "child_completion":
        call("parent_waiting", "behavior_wait", name="main", gate="parent_waiting")
        call("child_finished", "behavior_wait", name="main", gate="child_finished")
        call("parent_final", "behavior_wait", name="main", gate="parent_final")
    result = call("result", "behavior_result", name="main")
    if result["outcome"] != "completed":
        raise RuntimeError(f"turn ended {result['outcome']}: {result.get('error')}")
    if scenario == "approved_write":
        call("retry_start", "behavior_start", name="retry", sid=sid, command=command, input=INPUTS[scenario])
        retry = call("retry_result", "behavior_result", name="retry")
        if retry != result:
            raise OracleError("same-command retry returned a different stored result")
    evidence = call("evidence", "behavior_evidence", scenario=scenario, sid=sid)
    if scenario == "denied_write":
        second = call("unanswered_setup", "behavior_setup", scenario=scenario, trial=0, case="unanswered")
        call(
            "unanswered_start",
            "behavior_start",
            name="unanswered",
            sid=second["sid"],
            command=second["command"],
            input=INPUTS[scenario],
        )
        call("unanswered_ask", "behavior_wait", name="unanswered", gate="ask", operation_key="denied-write-v1")
        call(
            "unanswered_cancel",
            "behavior_cancel",
            name="unanswered",
            cancel_command=uuid5(UUID(hex=second["command"]), "cancel").hex,
        )
        cancelled = call("unanswered_result", "behavior_result", name="unanswered")
        if cancelled["outcome"] != "cancelled":
            raise OracleError(f"unanswered approval did not cancel cleanly: {cancelled}")
        unanswered = call(
            "unanswered_evidence",
            "behavior_evidence",
            scenario=scenario,
            sid=second["sid"],
            unanswered=True,
        )
        evidence["unanswered"] = unanswered
    call("cleanup", "behavior_cleanup")
    return evidence


def _run_compaction(
    report: dict[str, Any],
    workers: list[Any],
    settings: dict[str, Any],
    measured: Any,
    Worker: Any,
    setup: dict[str, Any],
) -> dict[str, Any]:
    scenario = "compacted_recall"
    worker = workers[0]
    sid = setup["sid"]
    for index, archive_input in enumerate(COMPACT_ARCHIVE_INPUTS):
        command = behavior_command_id(scenario, 0, f"archive-{index}").hex
        measured(
            report,
            worker,
            f"{scenario}/archive-{index}-start",
            "behavior_start",
            name=f"archive-{index}",
            sid=sid,
            command=command,
            input=archive_input,
        )
        result = measured(
            report, worker, f"{scenario}/archive-{index}-result", "behavior_result", name=f"archive-{index}"
        )
        if result["outcome"] != "completed":
            raise RuntimeError(f"archive turn {index} ended {result['outcome']}: {result.get('error')}")
    measured(report, worker, f"{scenario}/compaction", "behavior_wait", sid=sid, gate="compaction", name="")
    before = measured(report, worker, f"{scenario}/playback-before", "playback", sid=sid)
    initial = measured(report, worker, f"{scenario}/evidence-before", "behavior_evidence", scenario=scenario, sid=sid)
    measured(report, worker, f"{scenario}/cleanup", "behavior_cleanup")
    worker.stop()
    restart_mode = settings.get("history_mode", "full") if settings["mode"] == "replay" else "compacted"
    restarted = {**settings, "history_mode": restart_mode}
    workers[0] = Worker(restarted)
    worker = workers[0]
    resumed = measured(report, worker, f"{scenario}/resume", "behavior_setup", scenario=scenario, trial=0)
    after = measured(report, worker, f"{scenario}/playback-after", "playback", sid=sid)
    if before != after:
        raise OracleError(f"public replay changed across compacted worker restart: {before} != {after}")
    command = behavior_command_id(scenario, 0, "recall").hex
    measured(
        report,
        worker,
        f"{scenario}/recall-start",
        "behavior_start",
        name="recall",
        sid=resumed["sid"],
        command=command,
        input=RECALL_INPUT,
    )
    result = measured(report, worker, f"{scenario}/recall-result", "behavior_result", name="recall")
    if result["outcome"] != "completed":
        raise RuntimeError(f"recall ended {result['outcome']}: {result.get('error')}")
    evidence = measured(
        report,
        worker,
        f"{scenario}/evidence-after",
        "behavior_evidence",
        scenario=scenario,
        sid=sid,
        recalled=True,
    )
    evidence.update(
        {
            "initial": initial,
            "replay": before,
            "history_mode": restart_mode,
            "events_before_restart": before["events"],
            "events_after_restart": after["events"],
        }
    )
    measured(report, worker, f"{scenario}/cleanup-after", "behavior_cleanup")
    return evidence
