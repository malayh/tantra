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
