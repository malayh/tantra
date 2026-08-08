from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from tantra.context import TurnContext, _as_content, assemble_messages, compaction_window
from tantra.errors import ProviderError
from tantra.events import (
    CompactionApplied,
    SampleCompleted,
    SessionEvent,
    ToolCallCompleted,
    ToolCallRequested,
    TurnStarted,
    Usage,
)
from tantra.providers.base import ModelLimits, SampleRequest, StreamEnd, UserMessage
from tantra.skills import SKILL_TOOL

STRATEGY = "prune_then_summarize"

MIN_RESULT_CHARS = 256

SUMMARIZE_INSTRUCTION = """Write a brief that will replace everything above as the only record of it.

Keep what the work still needs: identifiers, paths, values, decisions and the reasons behind them.
Drop what a tool can produce again. Use exactly these sections, in this order:

## Goal
## Constraints
## Progress
## Key Decisions
## Next Steps
## Critical Context

Reply with the brief and nothing else."""


@dataclass(frozen=True)
class CompactionConfig:
    buffer: int = 20_000
    prune_pool_min: int = 40_000
    prune_gain_min: int = 20_000
    tail_turns: int = 2
    summarize_at: float = 0.95

    def usable(self, limits: ModelLimits) -> int:
        return limits.context_window - limits.max_output - self.buffer


DEFAULT_COMPACTION = CompactionConfig()


class Compactor(Protocol):
    """Keeps an assembled turn inside the model's context window, consulted before every sample."""

    async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
        """Return events that shrink the assembled context, or `[]` to leave the turn alone.

        `ctx.history` aliases the live log, `ctx.limits` describes the model in use. Returned events
        are appended to the log and emitted like any other, so a compaction survives a resume in
        another process. Nothing is ever rewritten — assembly derives the compacted view from them.
        """


def _rough(events: Sequence[SessionEvent], summary: str = "") -> int:
    messages = assemble_messages(summary, events)
    return sum(len(json.dumps(message.model_dump(), default=str)) for message in messages) // 4


def _reported(usage: Usage) -> int:
    return usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens + usage.output_tokens


def estimate_tokens(events: Sequence[SessionEvent], summary: str = "") -> int:
    base = 0
    base_index = -1
    for index, event in enumerate(events):
        if isinstance(event, SampleCompleted):
            base, base_index = _reported(event.usage), index
        elif isinstance(event, CompactionApplied):
            base, base_index = 0, -1
    if base_index < 0:
        return _rough(events, summary)
    counted = {
        event.call_id: len(_as_content(event.result))
        for event in events[:base_index]
        if isinstance(event, ToolCallCompleted)
    }
    added = 0
    for event in events[base_index + 1 :]:
        if isinstance(event, TurnStarted):
            added += len(event.input)
        elif isinstance(event, ToolCallCompleted):
            content = _as_content(event.result)
            added += len(content) - counted.get(event.call_id, 0)
            counted[event.call_id] = len(content)
    return max(base + added // 4, _rough(events, summary))


def _stub(name: str, content: str) -> str:
    return f"[pruned: {name} output, {len(content)} chars omitted]"


def _tail_start(events: Sequence[SessionEvent], tail_turns: int) -> int:
    if tail_turns <= 0:
        return len(events)
    starts = [index for index, event in enumerate(events) if isinstance(event, TurnStarted)]
    return starts[-tail_turns] if len(starts) > tail_turns else 0


class PruneThenSummarize:
    def __init__(self, config: CompactionConfig = DEFAULT_COMPACTION, model: str | None = None) -> None:
        self.config = config
        self.model = model

    async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
        usable = self.config.usable(ctx.limits)
        summary, window = compaction_window(ctx.history)
        before = estimate_tokens(window, summary)
        if before <= usable:
            return []

        target = int(usable * self.config.summarize_at)
        names = {event.call_id: event.name for event in window if isinstance(event, ToolCallRequested)}
        boundary = _tail_start(window, self.config.tail_turns)
        prefix, tail = window[:boundary], window[boundary:]
        stubs, gained = self._prune(prefix, names, before, target)

        floor = next((event.turn_id for event in tail if isinstance(event, TurnStarted)), None)
        if not prefix or floor is None or before - gained <= target:
            return stubs

        text = await self._brief(ctx, summary, [*prefix, *stubs])
        return [
            CompactionApplied(
                strategy=STRATEGY,
                tokens_before=before,
                tokens_after=_rough(tail, text),
                summary=text,
                floor_turn_id=floor,
            )
        ]

    def _prune(
        self,
        prefix: Sequence[SessionEvent],
        names: dict[str, str],
        before: int,
        target: int,
    ) -> tuple[list[SessionEvent], int]:
        latest: dict[str, ToolCallCompleted] = {}
        for event in prefix:
            if isinstance(event, ToolCallCompleted):
                latest[event.call_id] = event

        candidates: list[tuple[ToolCallCompleted, str]] = []
        for event in prefix:
            if not isinstance(event, ToolCallCompleted) or latest[event.call_id] is not event:
                continue
            name = names.get(event.call_id)
            if name is None or name == SKILL_TOOL:
                continue
            content = _as_content(event.result)
            if len(content) >= MIN_RESULT_CHARS:
                candidates.append((event, content))

        pool = sum(len(content) for _, content in candidates) // 4
        stubbed = sum(len(_stub(names[event.call_id], content)) for event, content in candidates) // 4
        if pool < self.config.prune_pool_min or pool - stubbed < self.config.prune_gain_min:
            return [], 0

        stubs: list[SessionEvent] = []
        gained = 0
        for event, content in reversed(candidates):
            stub = _stub(names[event.call_id], content)
            stubs.append(ToolCallCompleted(call_id=event.call_id, result=stub, is_error=event.is_error))
            gained += (len(content) - len(stub)) // 4
            if before - gained <= target:
                break
        return stubs, gained

    async def _brief(self, ctx: TurnContext, summary: str, prefix: Sequence[SessionEvent]) -> str:
        req = SampleRequest(
            model=self.model or ctx.model,
            messages=[*assemble_messages(summary, prefix), UserMessage(content=SUMMARIZE_INSTRUCTION)],
        )
        text = ""
        async for event in ctx.provider.stream(req):
            if isinstance(event, StreamEnd):
                text = event.text
        if not text:
            raise ProviderError("compaction summary came back empty; the prefix was left intact")
        return text
