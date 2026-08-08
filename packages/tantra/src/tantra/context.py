from __future__ import annotations

import inspect
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from tantra.agent import Agent, agent_name
from tantra.errors import TantraError
from tantra.events import (
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
    return messages


def build_messages(events: Sequence[SessionEvent]) -> list[Message]:
    summary, window = compaction_window(events)
    return assemble_messages(summary, window)


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
