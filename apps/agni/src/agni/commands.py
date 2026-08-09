from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from tantra import CompactionApplied, SessionBusy, SessionEvent, SessionHeader, TurnContext
from tantra.context import resolve_model
from tantra.events import TurnStarted

if TYPE_CHECKING:
    from agni.repl import Repl

RECENT = 15
TITLE_WIDTH = 50

HELP = (
    ("/new", "start a fresh session"),
    ("/resume", "list recent sessions and re-enter one"),
    ("/model", "switch the model for the next turn"),
    ("/compact", "summarise this session's history now"),
    ("/skills", "list the skills the agent can load"),
    ("/help", "show this list"),
    ("/exit", "leave agni"),
)


def _pick[T](reply: str, items: Sequence[T]) -> T | None:
    choice = reply.strip()
    if not choice.isdigit() or not 1 <= int(choice) <= len(items):
        return None
    return items[int(choice) - 1]


async def new(repl: Repl, arg: str) -> None:
    header = await repl.harness.create_session("build")
    repl.session_id = header.id
    repl.note(f"new session {header.id[:8]}")


async def resume(repl: Repl, arg: str) -> None:
    sessions = [header for header in await repl.harness.store.list(limit=RECENT) if header.parent_id is None]
    if not sessions:
        repl.note("no sessions stored yet")
        return
    for index, header in enumerate(sessions, 1):
        repl.line(f"  {index}. {_describe(header)}")
    chosen = _pick(arg or await repl.io.ask("  session number: "), sessions)
    if chosen is None:
        repl.note("nothing picked")
        return
    repl.session_id = chosen.id
    repl.note(f"session {chosen.id[:8]}")
    await repl.settle()


def _describe(header: SessionHeader) -> str:
    row = f"{header.id[:8]}  {header.agent:<8} {header.updated_at:%Y-%m-%d %H:%M}  {header.status}"
    if not header.title:
        return row
    title = header.title if len(header.title) <= TITLE_WIDTH else header.title[: TITLE_WIDTH - 1] + "…"
    return f"{row}  {title}"


async def model(repl: Repl, arg: str) -> None:
    for index, name in enumerate(repl.models, 1):
        marker = "*" if name == repl.harness.default_model else " "
        repl.line(f"  {marker} {index}. {name}")
    chosen = _pick(arg or await repl.io.ask("  model number: "), repl.models)
    if chosen is None:
        repl.note("nothing picked")
        return
    repl.harness.default_model = chosen
    repl.note(f"model is now {chosen}")


async def compact(repl: Repl, arg: str) -> None:
    harness = repl.harness
    if harness.compactor is None:
        repl.note("this harness has no compactor")
        return
    holder = uuid4().hex
    if not await harness.store.acquire_lease(repl.session_id, holder, harness.lease_ttl):
        raise SessionBusy(repl.session_id)
    try:
        produced = await _summarise(repl)
    finally:
        await harness.store.release_lease(repl.session_id, holder)
    if not produced:
        repl.note("history still fits the window; nothing to compact")
        return
    applied = next((event for event in produced if isinstance(event, CompactionApplied)), None)
    if applied is None:
        repl.note(f"pruned {len(produced)} tool results")
        return
    repl.note(f"compacted context: {applied.tokens_before} → {applied.tokens_after} tokens")


async def _summarise(repl: Repl) -> list[SessionEvent]:
    harness = repl.harness
    header = await harness.store.header(repl.session_id)
    history = [stamped.event async for stamped in harness.store.read(repl.session_id)]
    turn_id = next((event.turn_id for event in reversed(history) if isinstance(event, TurnStarted)), "")
    chosen = resolve_model(harness.agent_for(header.agent), harness.default_model)
    produced = await harness.compactor.compact(
        TurnContext(
            session_id=header.id,
            turn_id=turn_id,
            agent=header.agent,
            depth=header.depth,
            input="",
            metadata=header.metadata,
            history=history,
            model=chosen,
            limits=harness.provider.limits(chosen),
            provider=harness.provider,
        )
    )
    if not produced:
        return []
    current = await harness.store.header(header.id)
    await harness.store.append(header.id, produced, expect_seq=current.last_seq)
    return produced


async def skills(repl: Repl, arg: str) -> None:
    if repl.harness.skills is None:
        repl.note("no skills directory is configured")
        return
    index = await repl.harness.skills.index()
    if not index:
        repl.note("no skills found")
    for info in index:
        repl.line(f"  {info.name}: {info.description}")
    for path, reason in getattr(repl.harness.skills, "skipped", ()):
        repl.note(f"  skipped {path}: {reason}")


async def help_(repl: Repl, arg: str) -> None:
    for name, blurb in HELP:
        repl.line(f"  {name:<9} {blurb}")


async def exit_(repl: Repl, arg: str) -> None:
    repl.running = False


COMMANDS: dict[str, Callable[[Repl, str], Awaitable[Any]]] = {
    "/new": new,
    "/resume": resume,
    "/model": model,
    "/compact": compact,
    "/skills": skills,
    "/help": help_,
    "/exit": exit_,
}

COMMAND_NAMES = tuple(COMMANDS)


async def dispatch(repl: Repl, line: str) -> None:
    name, _, arg = line.partition(" ")
    handler = COMMANDS.get(name)
    if handler is None:
        repl.note(f"unknown command {name} — /help lists them")
        return
    await handler(repl, arg.strip())
