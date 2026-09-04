from __future__ import annotations

import inspect
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from tantra.agent import Agent, agent_name
from tantra.errors import TantraError
from tantra.events import (
    AgentMessageQueued,
    ChildSessionSpawned,
    CompactionApplied,
    ReasoningPart,
    SampleStarted,
    SessionEvent,
    TaskNoticeQueued,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    TurnStarted,
)
from tantra.providers.base import (
    AssistantMessage,
    Message,
    ModelLimits,
    Provider,
    ReasoningBlock,
    SampleRequest,
    SystemBlock,
    ToolCall,
    ToolResultMessage,
    ToolSchema,
    UserMessage,
)
from tantra.skills import SkillInfo
from tantra.tracing import NULL_TRACER, Tracer

SKILLS_PREAMBLE = "Skills available via the skill(name) tool:"


@dataclass
class TurnContext:
    session_id: str
    turn_id: str
    agent: str
    depth: int
    input: str
    metadata: dict[str, Any] = field(default_factory=dict)
    deps: Any = None
    history: list[SessionEvent] | None = None
    model: str | None = None
    limits: ModelLimits | None = None
    provider: Provider | None = None
    tracer: Tracer = NULL_TRACER


def _as_content(result: Any) -> str:
    return result if isinstance(result, str) else json.dumps(result, default=str)


def _inbox_key(event: AgentMessageQueued | TaskNoticeQueued) -> tuple[str, str]:
    if isinstance(event, AgentMessageQueued):
        return event.type, event.message_id
    return event.type, event.notice_id


def _deduplicate_inbox(events: Sequence[SessionEvent]) -> list[SessionEvent]:
    seen: set[tuple[str, str]] = set()
    kept: list[SessionEvent] = []
    for event in events:
        if isinstance(event, AgentMessageQueued | TaskNoticeQueued):
            key = _inbox_key(event)
            if key in seen:
                continue
            seen.add(key)
        kept.append(event)
    return kept


def pending_inbox(events: Sequence[SessionEvent]) -> list[AgentMessageQueued | TaskNoticeQueued]:
    deduplicated = _deduplicate_inbox(events)
    last_sample = max(
        (index for index, event in enumerate(deduplicated) if isinstance(event, SampleStarted)),
        default=-1,
    )
    return [
        event
        for index, event in enumerate(deduplicated)
        if index > last_sample and isinstance(event, AgentMessageQueued | TaskNoticeQueued)
    ]


def compaction_window(events: Sequence[SessionEvent]) -> tuple[str, list[SessionEvent]]:
    deduplicated = _deduplicate_inbox(events)
    latest = -1
    for index, event in enumerate(deduplicated):
        if isinstance(event, CompactionApplied):
            latest = index
    if latest < 0:
        return "", deduplicated
    applied = deduplicated[latest]
    floor = latest + 1
    if applied.floor_turn_id is not None:
        for index, event in enumerate(deduplicated):
            if isinstance(event, TurnStarted) and event.turn_id == applied.floor_turn_id:
                floor = index
                break
    pending = pending_inbox(deduplicated)
    if pending:
        pending_keys = {_inbox_key(event) for event in pending}
        pending_floor = next(
            index
            for index, event in enumerate(deduplicated)
            if isinstance(event, AgentMessageQueued | TaskNoticeQueued) and _inbox_key(event) in pending_keys
        )
        floor = min(floor, pending_floor)
    return applied.summary, deduplicated[floor:]


def _task_agents(events: Sequence[SessionEvent]) -> dict[str, str]:
    return {event.child_session_id: event.agent for event in events if isinstance(event, ChildSessionSpawned)}


def inbox_content(
    event: AgentMessageQueued | TaskNoticeQueued,
    events: Sequence[SessionEvent],
    task_agents: Mapping[str, str] | None = None,
) -> str:
    if isinstance(event, AgentMessageQueued):
        return f"[{event.source} message id={event.message_id}]\n{event.text}"
    agents = _task_agents(events) if task_agents is None else task_agents
    agent = agents.get(event.task_session_id, "unknown")
    return f"[task notice id={event.notice_id} task_id={event.task_session_id} agent={agent} state={event.state}]"


def assemble_messages(
    summary: str,
    events: Sequence[SessionEvent],
    *,
    task_agents: Mapping[str, str] | None = None,
) -> list[Message]:
    deduplicated = _deduplicate_inbox(events)
    messages: list[Message] = [UserMessage(content=summary)] if summary else []
    samples: dict[str, AssistantMessage] = {}
    results: dict[str, ToolResultMessage] = {}
    requested: set[str] = set()
    queued: list[AgentMessageQueued | TaskNoticeQueued] = []

    def sample_message(sample_id: str) -> AssistantMessage:
        message = samples.get(sample_id)
        if message is None:
            message = AssistantMessage()
            samples[sample_id] = message
            messages.append(message)
        return message

    def flush_inbox() -> None:
        messages.extend(UserMessage(content=inbox_content(event, deduplicated, task_agents)) for event in queued)
        queued.clear()

    for event in deduplicated:
        if isinstance(event, AgentMessageQueued | TaskNoticeQueued):
            queued.append(event)
        elif isinstance(event, SampleStarted):
            flush_inbox()
        elif isinstance(event, TurnStarted):
            messages.append(UserMessage(content=event.input))
        elif isinstance(event, TextPart):
            message = sample_message(event.sample_id)
            message.text = (message.text or "") + event.text
        elif isinstance(event, ReasoningPart):
            sample_message(event.sample_id).reasoning.append(ReasoningBlock(text=event.text, signature=event.signature))
        elif isinstance(event, ToolCallRequested):
            requested.add(event.call_id)
            sample_message(event.sample_id).tool_calls.append(
                ToolCall(id=event.call_id, name=event.name, args=json.dumps(event.args))
            )
        elif isinstance(event, ToolCallCompleted):
            if event.call_id not in requested:
                continue
            existing = results.get(event.call_id)
            if existing is not None:
                existing.content = _as_content(event.result)
                existing.is_error = event.is_error
                continue
            result = ToolResultMessage(
                call_id=event.call_id,
                content=_as_content(event.result),
                is_error=event.is_error,
            )
            results[event.call_id] = result
            messages.append(result)
    flush_inbox()
    return messages


def build_messages(events: Sequence[SessionEvent]) -> list[Message]:
    summary, window = compaction_window(events)
    return assemble_messages(summary, window, task_agents=_task_agents(events))


def _skills_block(skills: Sequence[SkillInfo]) -> SystemBlock:
    lines = [SKILLS_PREAMBLE, *(f"- {skill.name}: {skill.description}" for skill in skills)]
    return SystemBlock(text="\n".join(lines))


def build_sample_request(
    *,
    model: str,
    prompt: str,
    events: Sequence[SessionEvent],
    tools: Sequence[ToolSchema],
    params: dict[str, Any] | None = None,
    skills: Sequence[SkillInfo] = (),
) -> SampleRequest:
    system = [SystemBlock(text=prompt)] if prompt else []
    if skills:
        system.append(_skills_block(skills))
    return SampleRequest(
        model=model,
        system=system,
        messages=build_messages(events),
        tools=list(tools),
        params=dict(params or {}),
    )


async def resolve_prompt(prompt: Any, turn: TurnContext) -> str:
    if callable(prompt):
        value = prompt(turn)
        if inspect.isawaitable(value):
            value = await value
        return str(value)
    return str(prompt)


def resolve_model(agent: type[Agent], default_model: str | None) -> str:
    model = agent.model or default_model
    if not model:
        raise TantraError(f"agent {agent_name(agent)!r} sets no model and the harness has no default_model")
    return model
