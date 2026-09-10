import asyncio
from dataclasses import dataclass, field
from functools import cache
from typing import Annotated, Any

from fastapi import Depends
from starlette.requests import HTTPConnection

from sarathi.config import get_settings
from sarathi.telemetry import get_telemetry
from tantra import (
    Agent,
    BuiltinMemory,
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

memory_write, memory_recall = memory_tools(lambda ctx: {"user": ctx.deps["user_id"]})


class Researcher(Agent):
    """Research the web and return sourced findings."""

    prompt = (
        "You are a research subagent. Work the task with web_search and web_fetch: search, judge the hits, "
        "read the most promising pages, and follow up when a source is thin. "
        "Only fetch a URL that came from a web_search result or from the task itself. "
        "When the research is complete, call finish(result) with concrete findings, the URLs you read, "
        "and anything you could not confirm."
    )
    tools = []


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
        "For deep or wide research, call spawn('researcher', task). The child works independently. "
        "When you receive an [agent ... finished] message, synthesize its result into an explicit answer for the user."
    )
    tools = []
    subagents = [Researcher]
    permissions = {"memory_write": "ask"}


@cache
def _wire_tools() -> None:
    settings = get_settings()
    search = [web_search(settings.BRAVE_API_KEY)] if settings.BRAVE_API_KEY else []
    Sarathi.tools = [*search, web_fetch(proxy=settings.WEB_PROXY), read_doc(), memory_write, memory_recall]
    Researcher.tools = [*search, web_fetch(proxy=settings.WEB_PROXY)]


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
