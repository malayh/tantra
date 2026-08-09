from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agni.main import build_harness
from agni.repl import Io, Repl
from tantra import Memory, SessionEvent, Skills, SQLiteStore, Store
from tantra.events import SampleCompleted, SampleStarted, TextPart, TurnCompleted, TurnStarted
from tantra.providers.base import ModelLimits, Provider, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.tools import Context

MODELS = ("fast/model", "slow/model")

BRIEF = """## Goal
Ship the parser.

## Constraints
No new dependencies.

## Progress
Three files read.

## Key Decisions
Keep the hand-rolled lexer.

## Next Steps
Write the tests.

## Critical Context
The entry point is src/parse.py."""


def call(name: str, args: dict[str, Any], cid: str = "c1") -> ToolCall:
    return ToolCall(id=cid, name=name, args=json.dumps(args))


async def history(store: Store, sid: str) -> list[SessionEvent]:
    return [stamped.event async for stamped in store.read(sid)]


def only(events: Sequence[Any], kind: Any) -> list[Any]:
    return [event for event in events if isinstance(event, kind)]


class TinyProvider(FakeProvider):
    def __init__(self, samples: list[Sample]) -> None:
        super().__init__(samples)
        self._window = ModelLimits(context_window=150_000, max_output=30_000)

    def limits(self, model: str) -> ModelLimits:
        return self._window


class ScriptedIo:
    def __init__(self, replies: Sequence[str] = ()) -> None:
        self.chunks: list[str] = []
        self.prompts: list[str] = []
        self.replies = list(replies)

    def write(self, text: str) -> None:
        self.chunks.append(text)

    async def ask(self, prompt: str) -> str:
        self.prompts.append(prompt)
        if not self.replies:
            raise AssertionError(f"the repl asked {prompt!r} with no scripted reply left")
        return self.replies.pop(0)

    @property
    def text(self) -> str:
        return "".join(self.chunks)

    @property
    def io(self) -> Io:
        return Io(write=self.write, ask=self.ask)


async def open_store(root: Path) -> SQLiteStore:
    store = SQLiteStore(root / "data" / "agni.db")
    await store.setup()
    return store


async def make_repl(
    root: Path,
    samples: Sequence[Sample] = (),
    *,
    replies: Sequence[str] = (),
    auto: bool = False,
    memory: Memory | None = None,
    skills: Skills | None = None,
    provider: Provider | None = None,
) -> tuple[Repl, ScriptedIo]:
    store = await open_store(root)
    harness = build_harness(
        provider if provider is not None else FakeProvider(list(samples)),
        store,
        model=MODELS[0],
        memory=memory,
        skills=skills,
        auto=auto,
    )
    session_id = (await harness.create_session("build")).id
    io = ScriptedIo(replies)
    return Repl(harness, session_id, MODELS, io.io), io


def tool_context(store: Store, progress: list[str]) -> Context:
    async def emit(message: str) -> None:
        progress.append(message)

    return Context(
        session_id="s1",
        turn_id="t1",
        call_id="c1",
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )


def chat_turn(turn: str, *, input: str, text: str) -> list[SessionEvent]:
    sample = f"{turn}-s1"
    return [
        TurnStarted(turn_id=turn, input=input),
        SampleStarted(turn_id=turn, sample_id=sample, model=MODELS[0]),
        TextPart(sample_id=sample, text=text),
        SampleCompleted(sample_id=sample),
        TurnCompleted(turn_id=turn, stop_reason="completed"),
    ]
