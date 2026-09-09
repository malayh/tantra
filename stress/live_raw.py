"""Manual live Runtime smoke for a real OpenAI-compatible model.

    uv run python stress/live_raw.py
    uv run python stress/live_raw.py check

Needs OPENAI_API_KEY, OPENAI_ENDPOINT, and TANTRA_MODEL.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel

from tantra import Agent, Approval, ApprovalResponse, Context, LoggedEvent, MemoryStore, OpenAICompatible, Runtime, tool

REQUIRED = ("OPENAI_API_KEY", "OPENAI_ENDPOINT", "TANTRA_MODEL")
TASK = (
    "Spawn the scribe to count the words in 'the ledger balances nightly at two in the morning'. "
    "Wait for its finished message, call note_finding with the count, and submit a verdict."
)
FIELDS = {
    "turn_started": ("input",),
    "sample_started": ("model",),
    "text_part": ("text",),
    "tool_call_requested": ("name", "args"),
    "tool_call_started": ("call_id",),
    "tool_call_completed": ("is_error", "result"),
    "child_created": ("agent", "child_id"),
    "ask_raised": ("ask_id",),
    "ask_answered": ("ask_id", "response"),
    "agent_finished": ("result",),
    "sample_completed": ("finish_reason",),
    "turn_completed": ("stop_reason", "output"),
    "turn_failed": ("error",),
    "turn_interrupted": ("reason",),
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
    """Record one finding after human approval."""
    response = await ctx.ask(Approval(title="Record this finding?", body=claim))
    if not isinstance(response, ApprovalResponse) or not response.allow:
        return "the human refused"
    ctx.deps.recorded.append(claim)
    return f"recorded: {claim}"


@tool
async def word_count(text: str) -> int:
    """Count whitespace-separated words."""
    return len(text.split())


class Scribe(Agent):
    prompt = "Count words, then call finish with the number."
    tools = [word_count]
    permissions = {"word_count": "allow", "finish": "allow"}


class Lead(Agent):
    prompt = "Use actor tools explicitly. Wait for finished child messages before deciding."
    tools = [note_finding]
    subagents = [Scribe]
    permissions = {"*": "allow"}
    output_schema = Verdict


def short(value: Any) -> str:
    text = " ".join(str(value).split())
    return text if len(text) <= WIDTH else f"{text[:WIDTH]}…"


def render(item: LoggedEvent, depth: int) -> None:
    kind = str(getattr(item.event, "type", "?"))
    if kind in QUIET:
        return
    detail = " ".join(f"{name}={short(getattr(item.event, name, None))}" for name in FIELDS.get(kind, ()))
    print(f"{'  ' * depth}[{kind}] seq={item.seq} {detail}".rstrip())


def ask(request: Any) -> ApprovalResponse:
    print()
    print(f"[ask] {getattr(request, 'title', request)}")
    print(f"      {getattr(request, 'body', '')}")
    return ApprovalResponse(allow=input("      allow? [y/N] ").strip().lower() in ("y", "yes"))


async def watch_child(runtime: Runtime, connection: Any, child_id: UUID, depth: int) -> None:
    async for item in runtime.events(child_id):
        render(item, depth)
        if item.event.type == "ask_raised":
            await connection.answer(UUID(hex=item.event.ask_id), ask(item.event.request), command_id=uuid4())
        if item.event.type in ("agent_finished", "turn_failed", "turn_cancelled", "turn_interrupted"):
            return


def config() -> dict[str, str] | None:
    missing = [name for name in REQUIRED if not os.environ.get(name)]
    if missing:
        print(f"live_raw needs {', '.join(missing)} exported; refusing to start.")
        return None
    return {
        "key": os.environ["OPENAI_API_KEY"],
        "endpoint": os.environ["OPENAI_ENDPOINT"],
        "model": os.environ["TANTRA_MODEL"],
    }


def assemble(settings: dict[str, str]) -> tuple[Runtime, MemoryStore]:
    store = MemoryStore()
    provider = OpenAICompatible(base_url=settings["endpoint"], api_key=settings["key"])
    runtime = Runtime(provider, store, [Lead], default_model=settings["model"], deps_factory=lambda header: Desk())
    return runtime, store


async def main() -> int:
    settings = config()
    if settings is None:
        return 2
    runtime, store = assemble(settings)
    if "check" in sys.argv[1:]:
        print(f"runtime ready: model={settings['model']} agents={sorted(runtime.agents)} store={type(store).__name__}")
        await runtime.aclose()
        return 0

    root_id = await runtime.create(Lead)
    print(f"root {root_id} on {settings['model']}")
    print()
    async with runtime.connect(root_id, writable=True) as connection:
        async with asyncio.TaskGroup() as watchers:
            child_watchers: set[asyncio.Task[None]] = set()
            try:
                await connection.send(TASK, command_id=uuid4())
                async for item in connection:
                    render(item, 0)
                    if item.event.type == "child_created":
                        task = watchers.create_task(watch_child(runtime, connection, UUID(hex=item.event.child_id), 1))
                        child_watchers.add(task)
                    elif item.event.type == "ask_raised":
                        await connection.answer(
                            UUID(hex=item.event.ask_id), ask(item.event.request), command_id=uuid4()
                        )
                    elif item.event.type == "turn_completed" and item.event.output is not None:
                        print()
                        print(f"output: {item.event.output}")
                        break
                    elif item.event.type in ("turn_failed", "turn_cancelled", "turn_interrupted"):
                        break
            finally:
                for task in child_watchers:
                    if not task.done():
                        task.cancel()

    await runtime.aclose()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
