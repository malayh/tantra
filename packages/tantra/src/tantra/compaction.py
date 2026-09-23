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
from tantra.tracing import current_span

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
    buffer: int = 4_096
    prune_pool_min: int = 40_000
    prune_gain_min: int = 20_000
    tail_turns: int = 2
    summarize_at: float = 0.95
    trigger_at: float = 0.80
    recent_tokens: int = 20_000
    summary_max_output: int = 4_096

    def __post_init__(self) -> None:
        if isinstance(self.trigger_at, bool) or not isinstance(self.trigger_at, (int, float)):
            raise ValueError("trigger_at must be a number")
        if not 0 < self.trigger_at <= 1:
            raise ValueError("trigger_at must be greater than 0 and at most 1")
        if isinstance(self.summarize_at, bool) or not isinstance(self.summarize_at, (int, float)):
            raise ValueError("summarize_at must be a number")
        if not 0 < self.summarize_at < 1:
            raise ValueError("summarize_at must be greater than 0 and less than 1")
        for name in ("recent_tokens", "summary_max_output"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("buffer", "prune_pool_min", "prune_gain_min", "tail_turns"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

    def usable(self, limits: ModelLimits) -> int:
        ratio = int(limits.context_window * self.trigger_at)
        hard_ceiling = limits.context_window - limits.max_output - self.buffer
        return min(ratio, hard_ceiling)


DEFAULT_COMPACTION = CompactionConfig()


class Compactor(Protocol):
    """Keeps an assembled turn inside the model's context window, consulted before every sample."""

    async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
        """Return events that shrink the assembled context, or `[]` to leave the turn alone.

        `ctx.history` aliases the live log, `ctx.limits` describes the model in use. Returned events
        are appended to the log and emitted like any other, so a compaction survives process loss. Nothing is ever
        rewritten — assembly derives the compacted view from them.
        """


def _rough(events: Sequence[SessionEvent], summary: str = "") -> int:
    messages = assemble_messages(summary, events)
    return sum(len(json.dumps(message.model_dump(), default=str)) for message in messages) // 4


def _reported(usage: Usage) -> int:
    return usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens + usage.output_tokens


def estimate_request_tokens(request: SampleRequest) -> int:
    payload = json.dumps(request.model_dump(mode="json"), default=str, separators=(",", ":")).encode()
    return (len(payload) + 3) // 4


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


def _projected(ctx: TurnContext, summary: str, events: Sequence[SessionEvent]) -> int:
    if ctx.sample_request is None:
        return _rough(events, summary)
    request = ctx.sample_request.model_copy(update={"messages": assemble_messages(summary, events)})
    return estimate_request_tokens(request)


def _tail_start(events: Sequence[SessionEvent], budget: int, tail_turns: int) -> int:
    starts = [index for index, event in enumerate(events) if isinstance(event, TurnStarted)]
    if not starts:
        return len(events)
    preferred = min(max(tail_turns, 1), len(starts))
    boundary = starts[-1]
    for start in reversed(starts[-preferred:-1]):
        if _rough(events[start:]) > budget:
            return boundary
        boundary = start
    for start in reversed(starts[:-preferred]):
        if _rough(events[start:]) > budget:
            break
        boundary = start
    return boundary


class PruneThenSummarize:
    def __init__(
        self,
        config: CompactionConfig = DEFAULT_COMPACTION,
        model: str | None = None,
        *,
        instruction: str = SUMMARIZE_INSTRUCTION,
    ) -> None:
        self.config = config
        self.model = model
        self.instruction = instruction

    async def compact(self, ctx: TurnContext) -> list[SessionEvent]:
        return await self._compact(ctx, forced=False)

    async def _force_compact(self, ctx: TurnContext) -> list[SessionEvent]:
        return await self._compact(ctx, forced=True)

    async def _compact(self, ctx: TurnContext, *, forced: bool) -> list[SessionEvent]:
        trigger = self.config.usable(ctx.limits)
        summary, window = compaction_window(ctx.history)
        reported = estimate_tokens(window, summary)
        projected = _projected(ctx, summary, window)
        before = max(reported, projected)
        if not forced and before < trigger:
            return []

        target = int(trigger * self.config.summarize_at)
        names = {event.call_id: event.name for event in window if isinstance(event, ToolCallRequested)}
        stubs, _ = self._prune(window, names, before, target)
        effective = [*window, *stubs]
        fixed = _projected(ctx, "", [])
        current_start = max(
            (index for index, event in enumerate(window) if isinstance(event, TurnStarted)),
            default=len(window),
        )
        irreducible = _projected(ctx, "", [*window[current_start:], *stubs])
        if irreducible >= trigger:
            raise ProviderError(
                f"fixed payload or current input is {irreducible} tokens, at or above the {trigger}-token budget"
            )
        recent_budget = max(0, min(self.config.recent_tokens, target - fixed))
        boundary = _tail_start(effective, recent_budget, self.config.tail_turns)
        starts = [index for index, event in enumerate(window) if isinstance(event, TurnStarted)]
        if (forced or reported > projected) and starts and boundary == starts[0] and len(starts) > 1:
            boundary = starts[1]
        prefix = window[:boundary]
        tail = [*window[boundary:], *stubs]
        floor = next((event.turn_id for event in window[boundary:] if isinstance(event, TurnStarted)), None)
        after_prune = _projected(ctx, summary, effective)
        if stubs and after_prune <= target:
            return stubs
        if floor is None or not any(isinstance(event, TurnStarted) for event in prefix):
            raise ProviderError(
                f"request cannot fit the {trigger}-token compaction budget without changing the current input"
            )

        text = await self._brief(ctx, summary, [*prefix, *stubs])
        after = _projected(ctx, text, tail)
        if after >= trigger:
            raise ProviderError(f"compacted request is {after} tokens, at or above the {trigger}-token budget")
        tail_calls = {event.call_id for event in window[boundary:] if isinstance(event, ToolCallRequested)}
        durable_stubs = [event for event in stubs if event.call_id in tail_calls]
        return [
            *durable_stubs,
            CompactionApplied(
                strategy=STRATEGY,
                tokens_before=before,
                tokens_after=after,
                summary=text,
                floor_turn_id=floor,
            ),
        ]

    def _prune(
        self,
        window: Sequence[SessionEvent],
        names: dict[str, str],
        before: int,
        target: int,
    ) -> tuple[list[ToolCallCompleted], int]:
        latest: dict[str, ToolCallCompleted] = {}
        for event in window:
            if isinstance(event, ToolCallCompleted):
                latest[event.call_id] = event

        candidates: list[tuple[ToolCallCompleted, str]] = []
        for event in window:
            if not isinstance(event, ToolCallCompleted) or latest[event.call_id] is not event:
                continue
            name = names.get(event.call_id)
            if name is None or name == SKILL_TOOL:
                continue
            content = _as_content(event.result)
            if len(content) >= MIN_RESULT_CHARS and not content.startswith("[pruned:"):
                candidates.append((event, content))

        pool = sum(len(content) for _, content in candidates) // 4
        stubbed = sum(len(_stub(names[event.call_id], content)) for event, content in candidates) // 4
        if pool < self.config.prune_pool_min or pool - stubbed < self.config.prune_gain_min:
            return [], 0

        stubs: list[ToolCallCompleted] = []
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
            messages=[*assemble_messages(summary, prefix), UserMessage(content=self.instruction)],
            params={"max_tokens": self.config.summary_max_output},
        )
        span = ctx.tracer.start_sample(current_span.get(), req, sample_id=None, provider=ctx.provider, compacted=False)
        end: StreamEnd | None = None
        error: BaseException | None = None
        try:
            async for event in ctx.provider.stream(req):
                if isinstance(event, StreamEnd):
                    end = event
        except BaseException as exc:
            error = exc
            raise
        finally:
            ctx.tracer.end_sample(span, end=end, error=error, attempts=1)
        text = end.text if end is not None else ""
        if not text:
            raise ProviderError("compaction summary came back empty; the prefix was left intact")
        return text
