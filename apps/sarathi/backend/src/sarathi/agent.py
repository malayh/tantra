import asyncio
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends
from starlette.requests import HTTPConnection

from sarathi.config import get_settings
from sarathi.telemetry import get_telemetry
from tantra import (
    Agent,
    BuiltinMemory,
    FileSystemSkills,
    ModelLimits,
    OpenAICompatible,
    OpenAICompatibleEmbedder,
    PostgresStore,
    PruneThenSummarize,
    Runtime,
    SessionHeader,
    memory_tools,
)
from tantra.extratools.doc import read_doc
from tantra.extratools.web import web_fetch, web_search

MAX_OUTPUT = 8192
SKILLS_DIR = Path(__file__).parent / "skills"

memory_write, memory_recall = memory_tools(lambda ctx: {"user": ctx.deps["user_id"]})


class Subagent(Agent):
    """Execute an independent task with non-interactive tools and on-demand skills."""

    prompt = (
        "You are a general-purpose subagent. Execute the assigned task with ordinary tools and load an on-demand "
        "skill when it applies. Never ask a human for input or approval. For current, sourced, or comparative "
        "investigation, load the research skill and preserve the research level named in the task; an omitted level "
        "means normal. Call finish(result) exactly once with your findings or a clear blocked or incomplete result. "
        "A turn-ended message is only status and does not deliver your result."
    )
    tools = []
    skills = ["research"]


class Sarathi(Agent):
    prompt = (
        "You are Sarathi, a helpful AI assistant in a chat app. "
        "Answer clearly and concisely, and use markdown when it helps. "
        "You can search the web with web_search, read a page with web_fetch, and read an attached PDF or Word "
        "file with read_doc(path) using the path from an [attachment: name path=...] marker in the user's message. "
        "Only fetch a URL that came from a web_search result or that the user gave you. "
        "Save durable facts the user tells you about themselves with memory_write. "
        "Use memory_recall when those facts would change your answer. "
        "If the user explicitly asks you to remember or save a fact, always call memory_write, even if an "
        "earlier attempt was interrupted. "
        "If the user denies permission for memory_write, do not call memory_write again for that request. "
        "Work inline by default. Spawn a subagent only when the user explicitly requests one or independent or "
        "parallel work would materially help. Research can be done inline: load the research skill when the task "
        "requires current, sourced, or comparative investigation. Preserve any requested shallow, normal, or deep "
        "research level whether working inline or delegating. Include the level in a delegated task, omitting it only "
        "when normal is intended. Delegate with spawn('subagent', task, name=...) and optionally use a short, "
        "descriptive display name. When you receive an [agent ... finished] message, synthesize its result and "
        "explicitly answer the user."
    )
    tools = []
    skills = ["research"]
    subagents = [Subagent]
    permissions = {"memory_write": "ask"}


@cache
def _wire_tools() -> None:
    settings = get_settings()
    search = [web_search(settings.BRAVE_API_KEY)] if settings.BRAVE_API_KEY else []
    non_interactive = [*search, web_fetch(proxy=settings.WEB_PROXY), read_doc(), memory_recall]
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
    title_started: set[str] = field(default_factory=set)
    title_tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict)


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
    memory = BuiltinMemory(store, embedder)
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
    )
    return RuntimeResources(runtime=runtime, provider=provider, store=store, memory=memory, embedder=embedder)


async def close_resources(resources: RuntimeResources) -> None:
    tasks = list(resources.title_tasks.values())
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await resources.runtime.aclose()
    await resources.provider.aclose()
    if resources.embedder is not None:
        await resources.embedder.aclose()
    await resources.store.close()


def get_resources(connection: HTTPConnection) -> RuntimeResources:
    return connection.app.state.resources


ResourcesDep = Annotated[RuntimeResources, Depends(get_resources)]
