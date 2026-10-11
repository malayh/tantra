import asyncio
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends
from starlette.requests import HTTPConnection

from sarathi.config import get_settings
from sarathi.e2e import E2EPostgresCoordinator, e2e_gate
from sarathi.telemetry import get_telemetry
from sarathi.titles import generate_title
from tantra import (
    Agent,
    BuiltinMemory,
    FileSystemSkills,
    Hook,
    ModelLimits,
    OpenAICompatible,
    OpenAICompatibleEmbedder,
    PostgresCoordinator,
    PostgresStore,
    PruneThenSummarize,
    Runtime,
    SessionHeader,
    memory_tools,
)
from tantra.context import TurnContext
from tantra.events import TurnCancelled, TurnCompleted, TurnFailed, TurnInterrupted
from tantra.extratools.doc import read_doc
from tantra.extratools.web import web_fetch, web_search

MAX_OUTPUT = 8192
SKILLS_DIR = Path(__file__).parent / "skills"

memory_write, memory_recall = memory_tools(lambda ctx: {"user": ctx.deps["user_id"]})
ATTACHMENT_MARKER = "[attachment: "


class Subagent(Agent):
    """Execute an independent task with non-interactive tools and on-demand skills."""

    prompt = (
        "You are a general-purpose subagent. Complete the assigned task with ordinary tools. Never ask a human for "
        "input or approval. Deliver one final result to your parent, including a clear blocked or incomplete result "
        "when necessary."
    )
    tools = []


class Sarathi(Agent):
    prompt = (
        "You are Sarathi, a helpful AI assistant in a chat app. "
        "Answer clearly and concisely, and use markdown when it helps. "
        "For an attached PDF or Word file, use the path from its [attachment: name path=...] marker. "
        "Work inline by default. Delegate only when the user explicitly requests a subagent or independent or "
        "parallel work would materially help. When delegating, optionally use a short, descriptive display name."
    )
    tools = []
    subagents = [Subagent]
    permissions = {"memory_write": "ask"}


class TitleHook(Hook):
    def __init__(self, provider: OpenAICompatible, store: PostgresStore) -> None:
        self.provider = provider
        self.store = store
        self.runtime: Runtime | None = None
        self.started: set[str] = set()
        self.tasks: dict[str, asyncio.Task[None]] = {}

    async def before_turn(self, turn: TurnContext) -> None:
        if turn.depth != 0 or turn.submitted_by is None or turn.session_id in self.started:
            return
        text = "\n".join(line for line in turn.input.splitlines() if not line.startswith(ATTACHMENT_MARKER)).strip()
        if not text:
            return
        try:
            header = await self.store.header(turn.session_id)
        except Exception:
            return
        if header is None or header.title is not None or self.runtime is None:
            return
        self.started.add(turn.session_id)
        task = asyncio.create_task(self._after_terminal(turn.session_id, turn.turn_id, header.last_seq, text))
        self.tasks[turn.session_id] = task
        task.add_done_callback(lambda done, session_id=turn.session_id: self._finished(session_id, done))

    def _finished(self, session_id: str, task: asyncio.Task[None]) -> None:
        self.tasks.pop(session_id, None)
        if not task.cancelled():
            task.exception()

    async def _after_terminal(self, session_id: str, turn_id: str, after: int, text: str) -> None:
        assert self.runtime is not None
        terminals = (TurnCompleted, TurnFailed, TurnCancelled, TurnInterrupted)
        async for item in self.runtime.events(UUID(hex=session_id), after=after):
            if isinstance(item.event, terminals) and item.event.turn_id == turn_id:
                break
        header = await self.store.header(session_id)
        if header is None or header.title is not None:
            return
        title = await generate_title(self.provider, header.model or "", text)
        if not title:
            return
        header = await self.store.header(session_id)
        if header is None or header.title is not None:
            return
        await self.store.patch_header(session_id, title=title)

    async def aclose(self) -> None:
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@cache
def _wire_tools() -> None:
    settings = get_settings()
    search = [web_search(settings.BRAVE_API_KEY)] if settings.BRAVE_API_KEY else []
    gate = [e2e_gate(settings.E2E_GATE_URL)] if settings.E2E_GATE_URL else []
    non_interactive = [*search, *gate, web_fetch(proxy=settings.WEB_PROXY), read_doc(), memory_recall]
    Sarathi.tools = [*non_interactive, memory_write]
    Subagent.tools = non_interactive


def deps_factory(header: SessionHeader) -> dict[str, Any]:
    return {"user_id": header.metadata.get("user")}


def make_store() -> PostgresStore:
    return PostgresStore(get_settings().DATABASE_URL.replace("+psycopg", ""), schema="tantra")


@dataclass
class RuntimeResources:
    runtime: Runtime
    provider: OpenAICompatible
    store: PostgresStore
    memory: BuiltinMemory
    embedder: OpenAICompatibleEmbedder | None
    title_hook: TitleHook


async def make_resources() -> RuntimeResources:
    _wire_tools()
    settings = get_settings()
    limits = None
    if settings.SARATHI_CONTEXT_WINDOW is not None:
        limits = {
            name: ModelLimits(context_window=settings.SARATHI_CONTEXT_WINDOW, max_output=MAX_OUTPUT)
            for name in settings.models
        }
    provider = OpenAICompatible(settings.OPENAI_BASE_URL, settings.OPENAI_API_KEY, limits=limits)
    embedder = None
    if settings.EMBEDDING_MODEL:
        embedder = OpenAICompatibleEmbedder(settings.OPENAI_BASE_URL, settings.OPENAI_API_KEY, settings.EMBEDDING_MODEL)
    store = make_store()
    await store.setup()
    title_hook = TitleHook(provider, store)
    memory = BuiltinMemory(store, embedder)
    lease_ttl = settings.E2E_COORDINATOR_LEASE_TTL
    if settings.E2E_GATE_URL:
        coordinator = E2EPostgresCoordinator(
            store,
            settings.E2E_GATE_URL,
            lease_ttl=lease_ttl if lease_ttl is not None else 60.0,
        )
    else:
        coordinator = (
            PostgresCoordinator(store) if lease_ttl is None else PostgresCoordinator(store, lease_ttl=lease_ttl)
        )
    runtime = Runtime(
        provider,
        store,
        [Sarathi],
        default_model=settings.default_model,
        deps_factory=deps_factory,
        skills=FileSystemSkills(SKILLS_DIR),
        memory=memory,
        compactor=PruneThenSummarize(),
        telemetry=get_telemetry(),
        coordinator=coordinator,
        hooks=[title_hook],
    )
    title_hook.runtime = runtime
    try:
        await runtime.start()
    except BaseException:
        closers = [runtime.aclose(), provider.aclose(), store.close()]
        if embedder is not None:
            closers.append(embedder.aclose())
        await asyncio.gather(*closers, return_exceptions=True)
        raise
    return RuntimeResources(
        runtime=runtime,
        provider=provider,
        store=store,
        memory=memory,
        embedder=embedder,
        title_hook=title_hook,
    )


async def close_resources(resources: RuntimeResources) -> None:
    await resources.title_hook.aclose()
    await resources.runtime.aclose()
    await resources.provider.aclose()
    if resources.embedder is not None:
        await resources.embedder.aclose()
    await resources.store.close()


def get_resources(connection: HTTPConnection) -> RuntimeResources:
    return connection.app.state.resources


ResourcesDep = Annotated[RuntimeResources, Depends(get_resources)]
