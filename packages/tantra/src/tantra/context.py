from __future__ import annotations

import inspect
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from tantra.agent import Agent, agent_name
from tantra.errors import TantraError
from tantra.events import (
    CancellationRequested,
    CompactionApplied,
    ReasoningPart,
    SessionEvent,
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

SKILLS_GUIDANCE = (
    "Available skills can be loaded on demand with the skill tool. Load a skill when its description matches the "
    "task or the user explicitly requests it:"
)
CHILD_LIFECYCLE_GUIDANCE = (
    "Ordinary turn completion leaves the child reusable and sends a status-only notification to the parent. When the "
    "assignment is complete and a final result is ready, use the finish tool. Finishing permanently closes the child "
    "and delivers its result to the parent."
)
CANCELLATION_CONTEXT = "[runtime] The user cancelled the live root and descendant work."


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
    sample_request: SampleRequest | None = None


def _as_content(result: Any) -> str:
    return result if isinstance(result, str) else json.dumps(result, default=str)


def compaction_window(events: Sequence[SessionEvent]) -> tuple[str, list[SessionEvent]]:
    latest = -1
    for index, event in enumerate(events):
        if isinstance(event, CompactionApplied):
            latest = index
    if latest < 0:
        return "", list(events)
    applied = events[latest]
    floor = latest + 1
    if applied.floor_turn_id is not None:
        for index, event in enumerate(events):
            if isinstance(event, TurnStarted) and event.turn_id == applied.floor_turn_id:
                floor = index
                break
    return applied.summary, list(events[floor:])


def assemble_messages(summary: str, events: Sequence[SessionEvent]) -> list[Message]:
    messages: list[Message] = [UserMessage(content=summary)] if summary else []
    samples: dict[str, AssistantMessage] = {}
    results: dict[str, ToolResultMessage] = {}
    requested: set[str] = set()
    completed: set[str] = set()

    def sample_message(sample_id: str) -> AssistantMessage:
        message = samples.get(sample_id)
        if message is None:
            message = AssistantMessage()
            samples[sample_id] = message
            messages.append(message)
        return message

    for event in events:
        if isinstance(event, TurnStarted):
            messages.append(UserMessage(content=event.input))
        elif isinstance(event, TextPart):
            message = sample_message(event.sample_id)
            message.text = (message.text or "") + event.text
        elif isinstance(event, CancellationRequested):
            messages.append(UserMessage(content=CANCELLATION_CONTEXT))
        elif isinstance(event, ReasoningPart):
            sample_message(event.sample_id).reasoning.append(ReasoningBlock(text=event.text, signature=event.signature))
        elif isinstance(event, ToolCallRequested):
            requested.add(event.call_id)
            sample_message(event.sample_id).tool_calls.append(
                ToolCall(id=event.call_id, name=event.name, args=json.dumps(event.args))
            )
            if event.call_id not in results:
                result = ToolResultMessage(call_id=event.call_id, content="")
                results[event.call_id] = result
                messages.append(result)
        elif isinstance(event, ToolCallCompleted):
            if event.call_id not in requested:
                continue
            existing = results[event.call_id]
            existing.content = _as_content(event.result)
            existing.is_error = event.is_error
            completed.add(event.call_id)
    for message in samples.values():
        message.tool_calls = [call for call in message.tool_calls if call.id in completed]
    return [
        message
        for message in messages
        if (not isinstance(message, ToolResultMessage) or message.call_id in completed)
        and (not isinstance(message, AssistantMessage) or message.text or message.reasoning or message.tool_calls)
    ]


def build_messages(events: Sequence[SessionEvent]) -> list[Message]:
    summary, window = compaction_window(events)
    return assemble_messages(summary, window)


def _execution_environment_block(
    skills: Sequence[SkillInfo],
    child_lifecycle: bool,
) -> SystemBlock | None:
    sections = []
    if skills:
        lines = [SKILLS_GUIDANCE, *(f"- {skill.name}: {skill.description}" for skill in skills)]
        sections.append("Skills\n\n" + "\n".join(lines))
    if child_lifecycle:
        sections.append("Child lifecycle\n\n" + CHILD_LIFECYCLE_GUIDANCE)
    if not sections:
        return None
    return SystemBlock(text="Execution environment\n\n" + "\n\n".join(sections))


def build_sample_request(
    *,
    model: str,
    prompt: str,
    events: Sequence[SessionEvent],
    tools: Sequence[ToolSchema],
    params: dict[str, Any] | None = None,
    skills: Sequence[SkillInfo] = (),
    child_lifecycle: bool = False,
) -> SampleRequest:
    system = [SystemBlock(text=prompt)] if prompt else []
    environment = _execution_environment_block(skills, child_lifecycle)
    if environment is not None:
        system.append(environment)
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
        raise TantraError(f"agent {agent_name(agent)!r} sets no model and the runtime has no default_model")
    return model
