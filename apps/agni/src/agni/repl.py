from __future__ import annotations

import asyncio
import json
import signal
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import aclosing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agni.commands import COMMAND_NAMES, dispatch
from tantra import (
    Approval,
    ApprovalResponse,
    AskRequest,
    AskResponse,
    Choice,
    ChoiceResponse,
    Emitted,
    FreeTextResponse,
    Harness,
    TantraError,
)
from tantra.events import (
    AskRaised,
    ChildSessionSpawned,
    CompactionApplied,
    ToolCallCompleted,
    ToolCallRequested,
    ToolProgress,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.providers.base import TextDelta

RESET = "\x1b[0m"
DIM = "\x1b[2m"
CYAN = "\x1b[36m"
RED = "\x1b[31m"
YELLOW = "\x1b[33m"

PROMPT = "› "
BANNER = "agni — type to talk, /help for commands, ctrl-d to leave"
ARGS_WIDTH = 120
RESULT_WIDTH = 160


@dataclass
class Io:
    write: Callable[[str], None]
    ask: Callable[[str], Awaitable[str]]


def _clip(text: str, width: int) -> str:
    single = " ".join(text.split())
    return single if len(single) <= width else single[:width] + "…"


def _args(args: dict[str, Any]) -> str:
    return _clip(json.dumps(args, default=str), ARGS_WIDTH)


def _result(result: Any) -> str:
    return _clip(result if isinstance(result, str) else json.dumps(result, default=str), RESULT_WIDTH)


class Repl:
    def __init__(self, harness: Harness, session_id: str, models: Sequence[str], io: Io) -> None:
        self.harness = harness
        self.session_id = session_id
        self.models = list(models)
        self.io = io
        self.running = True
        self._streaming = False
        self._interrupted = False

    async def run(self) -> None:
        self.note(BANNER)
        while self.running:
            try:
                line = await self.io.ask(PROMPT)
            except KeyboardInterrupt:
                continue
            except EOFError:
                return
            await self.handle(line)

    async def handle(self, line: str) -> None:
        text = line.strip()
        if not text:
            return
        self._interrupted = False
        try:
            with self._catch_interrupt():
                if text.startswith("/"):
                    await dispatch(self, text)
                else:
                    await self.turn(text)
        except KeyboardInterrupt:
            self.interrupt()
        except TantraError as exc:
            self.line(f"{RED}{exc}{RESET}")

    def interrupt(self) -> None:
        if not self._interrupted:
            self._interrupted = True
            self.note("interrupted — the turn picks up where it stopped on your next message")

    async def turn(self, text: str) -> None:
        await self.settle()
        if self._interrupted:
            return
        await self.follow(self.harness.run(self.session_id, text))

    @contextmanager
    def _catch_interrupt(self) -> Iterator[None]:
        try:
            loop = asyncio.get_running_loop()
            previous = signal.getsignal(signal.SIGINT)
            loop.add_signal_handler(signal.SIGINT, self.interrupt)
        except (NotImplementedError, RuntimeError, ValueError):
            yield
            return
        try:
            yield
        finally:
            loop.remove_signal_handler(signal.SIGINT)
            if previous is not None:
                signal.signal(signal.SIGINT, previous)

    async def settle(self) -> None:
        if not await self.incomplete(self.session_id):
            return
        self.note("resuming the interrupted turn")
        await self.follow(self.harness.resume(self.session_id))

    async def incomplete(self, sid: str) -> bool:
        last: Any = None
        async for stamped in self.harness.store.read(sid):
            if isinstance(stamped.event, TurnStarted | TurnCompleted | TurnFailed):
                last = stamped.event
        return isinstance(last, TurnStarted)

    async def follow(self, stream: AsyncIterator[Emitted]) -> None:
        pending = await self._pump(stream)
        while pending is not None:
            target, raised = pending
            response = await self._answer(raised.request)
            pending = await self._pump(self.harness.resume(target, raised.ask_id, response))
            if pending is None and target != self.session_id and await self._bubbled():
                pending = await self._pump(self.harness.resume(self.session_id))

    async def _bubbled(self) -> bool:
        return not self._interrupted and await self.incomplete(self.session_id)

    async def _pump(self, stream: AsyncIterator[Emitted]) -> tuple[str, AskRaised] | None:
        pending: tuple[str, AskRaised] | None = None
        async with aclosing(stream) as events:
            async for emitted in events:
                if self._interrupted:
                    break
                if isinstance(emitted.event, AskRaised):
                    pending = (emitted.session_id, emitted.event)
                    continue
                self._render(emitted)
        self.flush()
        return None if self._interrupted else pending

    def _render(self, emitted: Emitted) -> None:
        event = emitted.event
        indent = "  " * emitted.depth
        if isinstance(event, TextDelta):
            self.io.write(event.text)
            self._streaming = True
        elif isinstance(event, ToolCallRequested):
            self.line(f"{indent}{CYAN}⏺ {event.name}{RESET} {DIM}{_args(event.args)}{RESET}")
        elif isinstance(event, ToolProgress):
            self.line(f"{indent}{DIM}  {event.message}{RESET}")
        elif isinstance(event, ToolCallCompleted):
            colour = RED if event.is_error else DIM
            self.line(f"{indent}{colour}  {_result(event.result)}{RESET}")
        elif isinstance(event, ChildSessionSpawned):
            self.line(f"{indent}{DIM}  → {event.agent} {event.child_session_id[:8]}{RESET}")
        elif isinstance(event, CompactionApplied):
            self.note(f"compacted context: {event.tokens_before} → {event.tokens_after} tokens")
        elif isinstance(event, TurnFailed):
            self.line(f"{RED}turn failed: {event.error}{RESET}")
        elif isinstance(event, TurnCompleted) and event.stop_reason not in ("completed", "output"):
            self.note(f"turn stopped: {event.stop_reason}")

    async def _answer(self, request: AskRequest) -> AskResponse:
        if isinstance(request, Approval):
            self.line(f"{YELLOW}? {request.title}{RESET}")
            if request.body:
                self.line(f"{DIM}{_clip(request.body, ARGS_WIDTH)}{RESET}")
            reply = await self.io.ask("  allow? [y/N] ")
            return ApprovalResponse(allow=reply.strip().lower() in ("y", "yes"))
        if isinstance(request, Choice):
            self.line(f"{YELLOW}? {request.title}{RESET}")
            for index, option in enumerate(request.options, 1):
                self.line(f"  {index}. {option}")
            while request.options:
                reply = (await self.io.ask("  choose a number: ")).strip()
                if reply.isdigit() and 1 <= int(reply) <= len(request.options):
                    return ChoiceResponse(selected=request.options[int(reply) - 1])
                self.note("pick one of the numbers listed")
            return ChoiceResponse(selected=(await self.io.ask("  ")).strip())
        self.line(f"{YELLOW}? {request.prompt}{RESET}")
        return FreeTextResponse(text=(await self.io.ask("  ")).strip())

    def flush(self) -> None:
        if self._streaming:
            self.io.write("\n")
            self._streaming = False

    def line(self, text: str = "") -> None:
        self.flush()
        self.io.write(text + "\n")

    def note(self, text: str) -> None:
        self.line(f"{DIM}{text}{RESET}")


def terminal_io(history: Path | None = None) -> Io:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.completion import WordCompleter
    from prompt_toolkit.history import FileHistory

    session: PromptSession[str] = PromptSession(
        completer=WordCompleter(list(COMMAND_NAMES), WORD=True),
        complete_while_typing=True,
        history=FileHistory(str(history)) if history is not None else None,
    )

    def write(text: str) -> None:
        sys.stdout.write(text)
        sys.stdout.flush()

    async def ask(prompt: str) -> str:
        return await session.prompt_async(prompt)

    return Io(write=write, ask=ask)
