"""Manual live smoke for raw tantra — run by a human, never collected by pytest.

    uv run python stress/live_raw.py          drive a real turn against a real model
    uv run python stress/live_raw.py check    build the harness and exit, no network

Needs OPENAI_API_KEY, OPENAI_ENDPOINT and AGNI_MODELS (comma separated, first wins).
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel

from tantra import (
    Agent,
    Approval,
    ApprovalResponse,
    Context,
    Emitted,
    Harness,
    MemoryStore,
    OpenAICompatible,
    SessionHeader,
    Store,
    tool,
)

REQUIRED = ("OPENAI_API_KEY", "OPENAI_ENDPOINT", "AGNI_MODELS")

TASK = (
    "Delegate to the scribe to count the words in this sentence: "
    "'the ledger balances nightly at two in the morning'. "
    "Then call note_finding with the count so a human can approve it, "
    "and finish by submitting your verdict."
)

FIELDS = {
    "turn_started": ("input",),
    "sample_started": ("model",),
    "text_part": ("text",),
    "tool_call_requested": ("name", "args"),
    "tool_call_started": ("call_id",),
    "tool_call_completed": ("is_error", "result"),
    "child_session_spawned": ("agent", "child_session_id"),
    "ask_raised": ("ask_id",),
    "ask_answered": ("ask_id", "response"),
    "sample_completed": ("finish_reason",),
    "turn_completed": ("stop_reason", "output"),
    "turn_failed": ("error",),
}

QUIET = ("text_delta", "reasoning_delta", "tool_call_delta")

WIDTH = 140


class Verdict(BaseModel):
    answer: str
    confidence: float


@dataclass
class Desk:
    recorded: list[str] = field(default_factory=list)


@tool
async def note_finding(claim: str, ctx: Context) -> str:
    """Record one finding, once a human has approved it."""
    response = await ctx.ask(Approval(title="Record this finding?", body=claim))
    if not isinstance(response, ApprovalResponse) or not response.allow:
        return "the human refused: do not record it, and say so in your verdict"
    ctx.deps.recorded.append(claim)
    return f"recorded: {claim}"


@tool
async def word_count(text: str) -> int:
    """Count the whitespace-separated words in `text`."""
    return len(text.split())


class Scribe(Agent):
    """Counts the words in a passage and replies with the number alone."""

    tools = [word_count]
    permissions = {"word_count": "allow"}


class Lead(Agent):
    """Answers the desk's question with help from the scribe."""

    prompt = "You are a terse research desk lead. Use your tools; never guess a number you can count."
    tools = [note_finding]
    subagents = [Scribe]
    permissions = {"*": "allow"}
    output_schema = Verdict


def short(value: Any) -> str:
    text = " ".join(str(value).split())
    return text if len(text) <= WIDTH else f"{text[:WIDTH]}…"


def render(item: Emitted) -> None:
    kind = str(getattr(item.event, "type", "?"))
    if kind in QUIET:
        return
    detail = " ".join(f"{name}={short(getattr(item.event, name, None))}" for name in FIELDS.get(kind, ()))
    print(f"{'  ' * item.depth}[{kind}] {detail}".rstrip())


def answer(request: Any) -> ApprovalResponse:
    print(f"\n[ask] {getattr(request, 'title', request)}\n      {getattr(request, 'body', '')}")
    return ApprovalResponse(allow=input("      allow? [y/N] ").strip().lower() in ("y", "yes"))


async def drive(stream: Any) -> Emitted | None:
    pending: Emitted | None = None
    async with aclosing(stream) as live:
        async for item in live:
            render(item)
            if str(getattr(item.event, "type", "")) == "ask_raised":
                pending = item
    return pending


async def settle(harness: Harness, store: Store, sid: str, pending: Emitted | None) -> None:
    while pending is not None:
        response = answer(pending.event.request)
        pending = await drive(harness.resume(pending.session_id, pending.event.ask_id, response))
        header = await store.header(sid)
        if pending is None and header is not None and header.status == "awaiting_input":
            pending = await drive(harness.resume(sid))


def config() -> dict[str, str] | None:
    missing = [name for name in REQUIRED if not os.environ.get(name)]
    if missing:
        print(f"live_raw needs {', '.join(missing)} exported; refusing to run.")
        return None
    models = [name.strip() for name in os.environ["AGNI_MODELS"].split(",") if name.strip()]
    if not models:
        print("AGNI_MODELS is set but empty; refusing to run.")
        return None
    return {"key": os.environ["OPENAI_API_KEY"], "endpoint": os.environ["OPENAI_ENDPOINT"], "model": models[0]}


def assemble(settings: dict[str, str]) -> tuple[Harness, Store]:
    store = MemoryStore()
    provider = OpenAICompatible(base_url=settings["endpoint"], api_key=settings["key"])
    harness = Harness(
        provider,
        store,
        [Lead],
        default_model=settings["model"],
        deps_factory=lambda header: Desk(),
    )
    return harness, store


async def main() -> int:
    settings = config()
    if settings is None:
        return 2
    harness, store = assemble(settings)
    if "check" in sys.argv[1:]:
        print(f"harness ready: model={settings['model']} agents={sorted(harness.agents)} store={type(store).__name__}")
        return 0

    session: SessionHeader = await harness.create_session(Lead)
    print(f"session {session.id} on {settings['model']}\n")
    await settle(harness, store, session.id, await drive(harness.run(session.id, TASK)))

    terminal = [
        item.event
        async for item in harness.replay(session.id)
        if str(getattr(item.event, "type", "")) == "turn_completed"
    ]
    print(f"\nstop_reason: {terminal[-1].stop_reason if terminal else 'none'}")
    print(f"output: {terminal[-1].output if terminal else 'none'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
