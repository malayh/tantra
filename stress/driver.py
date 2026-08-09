from __future__ import annotations

import hashlib
import json
import random
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from tantra import ModelLimits, ProviderError, Sample, SampleRequest, Usage
from tantra.providers.base import (
    ProviderEvent,
    ReasoningBlock,
    ReasoningDelta,
    StreamEnd,
    TextDelta,
    ToolCall,
    ToolCallDelta,
)

SUMMARIZE_MARKER = "Reply with the brief and nothing else"

DEFAULT_LIMITS = ModelLimits(context_window=32_000, max_output=2_000)

DELTA_CHUNK = 512


@dataclass
class PolicyState:
    markers: list[str] = field(default_factory=list)
    rng: random.Random = field(default_factory=random.Random)
    tag: str = ""
    samples: int = 0
    calls: int = 0

    def next_call_id(self) -> str:
        self.calls += 1
        return f"{self.tag}-{self.calls}"


Policy = Callable[[SampleRequest, PolicyState], Sample]


def blob(size: int, tag: str = "blob") -> str:
    """Deterministic filler of exactly `size` characters, stable across processes."""
    unit = hashlib.blake2b(tag.encode(), digest_size=16).hexdigest()
    return (unit * (size // len(unit) + 1))[:size]


def message_text(message: Any) -> str:
    value = getattr(message, "content", None)
    if value is None:
        value = getattr(message, "text", None)
    return value if isinstance(value, str) else ""


def is_summarize(req: SampleRequest) -> bool:
    """True when the compactor is asking for a brief rather than the loop asking for a sample."""
    return bool(req.messages) and SUMMARIZE_MARKER in message_text(req.messages[-1])


def turn_step(req: SampleRequest) -> int:
    """How many assistant messages the running turn already has: 0 on its first sample."""
    step = 0
    for message in req.messages:
        role = getattr(message, "role", "")
        if role == "user":
            step = 0
        elif role == "assistant":
            step += 1
    return step


def last_user(req: SampleRequest) -> str:
    for message in reversed(req.messages):
        if getattr(message, "role", "") == "user":
            return message_text(message)
    return ""


def found_markers(req: SampleRequest, markers: Sequence[str]) -> list[str]:
    """The markers that actually reached this request's message content, in `markers` order."""
    text = "\n".join(message_text(message) for message in req.messages)
    return [marker for marker in markers if marker in text]


def brief(markers: Sequence[str]) -> str:
    facts = "\n".join(f"- {marker}" for marker in markers) or "- nothing worth keeping"
    return (
        "## Goal\nFinish the long-running stress run.\n\n"
        "## Constraints\nDeterministic content only, no network.\n\n"
        "## Progress\nBlobs fetched and folded into the record.\n\n"
        "## Key Decisions\nEvery marker fact is carried forward verbatim.\n\n"
        "## Next Steps\nKeep working the queue.\n\n"
        f"## Critical Context\n{facts}"
    )


def call_policy(tool: str, args: dict[str, Any], *, answer: str = "done") -> Policy:
    """One tool call on a turn's first sample, then a plain text answer."""
    payload = json.dumps(args)

    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name=tool, args=payload)])
        return Sample(text=answer)

    return policy


def worker_policy(
    *,
    tool: str = "fetch_blob",
    calls: int = 2,
    size: int | Callable[[PolicyState], int] = 6_000,
    answer_chars: int = 12_000,
) -> Policy:
    """Fetch `calls` blobs on a turn's first sample, then answer with `answer_chars` of text.

    A summarize request is answered with a brief carrying only those `PolicyState.markers` the
    request itself still contains — a fact compaction dropped before the brief cannot come back.
    """

    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if is_summarize(req):
            return Sample(text=brief(found_markers(req, state.markers)))
        if turn_step(req) < 1:
            return Sample(
                tool_calls=[
                    ToolCall(
                        id=state.next_call_id(),
                        name=tool,
                        args=json.dumps({"size": size(state) if callable(size) else size}),
                    )
                    for _ in range(calls)
                ]
            )
        return Sample(text=blob(answer_chars, tag=f"answer-{state.samples}"))

    return policy


def by_model(table: Mapping[str, Policy]) -> Policy:
    """Route each request to the policy registered for its `model`."""

    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        chosen = table.get(req.model)
        if chosen is None:
            raise ProviderError(f"no policy for model {req.model!r}", status_code=400)
        return chosen(req, state)

    return policy


def flaky(policy: Policy, *, fail_on: Sequence[int], status_code: int = 503) -> Policy:
    """Raise a retryable `ProviderError` on the given 1-based sample indices, else defer."""
    scheduled = set(fail_on)

    def wrapped(req: SampleRequest, state: PolicyState) -> Sample:
        if state.samples in scheduled:
            raise ProviderError(f"synthetic transient failure on sample {state.samples}", status_code=status_code)
        return policy(req, state)

    return wrapped


def _split(text: str) -> list[str]:
    return [text[start : start + DELTA_CHUNK] for start in range(0, len(text), DELTA_CHUNK)]


def _usage(req: SampleRequest, sample: Sample) -> Usage:
    reported = sample.usage
    produced = len(sample.text) + len(sample.reasoning) + sum(len(call.args) for call in sample.tool_calls)
    return Usage(
        input_tokens=reported.input_tokens or len(req.model_dump_json()) // 4,
        output_tokens=reported.output_tokens or produced // 4,
        cache_read_tokens=reported.cache_read_tokens,
        cache_write_tokens=reported.cache_write_tokens,
    )


class SyntheticProvider:
    """A `Provider` whose every sample is decided by a policy callable rather than a fixed script."""

    def __init__(self, policy: Policy, limits: ModelLimits = DEFAULT_LIMITS, seed: int = 0) -> None:
        self.policy = policy
        self.model_limits = limits
        self.requests: list[SampleRequest] = []
        self.state = PolicyState(rng=random.Random(seed), tag=uuid4().hex[:8])

    def limits(self, model: str) -> ModelLimits:
        return self.model_limits

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.requests.append(req)
        self.state.samples += 1
        sample = self.policy(req, self.state)

        for fragment in _split(sample.reasoning):
            yield ReasoningDelta(text=fragment)
        for fragment in _split(sample.text):
            yield TextDelta(text=fragment)
        for index, call in enumerate(sample.tool_calls):
            half = len(call.args) // 2
            yield ToolCallDelta(index=index, id=call.id, name=call.name, args_fragment=call.args[:half])
            yield ToolCallDelta(index=index, args_fragment=call.args[half:])
        for call in sample.tool_calls:
            yield call

        yield StreamEnd(
            text=sample.text,
            reasoning=[ReasoningBlock(text=sample.reasoning)] if sample.reasoning else [],
            tool_calls=list(sample.tool_calls),
            usage=_usage(req, sample),
            finish_reason=sample.finish_reason or ("tool_calls" if sample.tool_calls else "stop"),
        )


class SyntheticEmbedder:
    """Deterministic vectors from a stable digest — no model, no network, no per-process salt."""

    def __init__(self, dims: int = 8) -> None:
        self.dims = dims

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(text) for text in texts]

    def _vector(self, text: str) -> list[float]:
        digest = hashlib.blake2b(text.encode(), digest_size=self.dims).digest()
        return [byte / 255.0 for byte in digest]
