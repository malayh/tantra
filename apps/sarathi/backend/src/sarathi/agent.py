from collections.abc import Callable
from functools import cache
from typing import Annotated, Any

from fastapi import Depends

from sarathi.config import get_settings
from sarathi.telemetry import get_telemetry
from tantra import (
    Agent,
    BuiltinMemory,
    Harness,
    ModelLimits,
    OpenAICompatible,
    OpenAICompatibleEmbedder,
    PostgresStore,
    PruneThenSummarize,
    SessionHeader,
    memory_tools,
)
from tantra.extratools.doc import read_doc
from tantra.extratools.web import web_fetch, web_search

MAX_OUTPUT = 8192

HarnessFactory = Callable[[str | None], Harness]

memory_write, memory_recall = memory_tools(lambda ctx: {"user": ctx.deps["user_id"]})


class Researcher(Agent):
    """Delegate a focused research task to a researcher that searches the web, fetches pages, and reports findings."""

    prompt = (
        "You are a research subagent. Work the task with web_search and web_fetch: search, judge the hits, "
        "read the most promising pages, and follow up when a source is thin. "
        "Only fetch a URL that came from a web_search result or from the task itself — never guess or build one. "
        "Report concrete findings with the URLs you actually read, and say plainly what you could not confirm."
    )
    tools = []


class Sarathi(Agent):
    prompt = (
        "You are Sarathi, a helpful AI assistant in a chat app. "
        "Answer clearly and concisely, and use markdown when it helps. "
        "You can search the web with web_search, read a page with web_fetch, and read an attached PDF or Word "
        "file with read_doc(path) using the path from an [attachment: name path=...] marker in the user's message. "
        "Only fetch a URL that came from a web_search result or that the user gave you — never guess or build one. "
        "Save durable facts the user tells you about themselves with memory_write, and look them up "
        "again with memory_recall when they would change your answer. "
        "Hand deep or wide research to the researcher subagent and synthesise what it reports."
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


def make_harness(model: str | None = None) -> Harness:
    _wire_tools()
    settings = get_settings()
    limits = None
    if settings.SARATHI_CONTEXT_WINDOW is not None:
        limits = {
            name: ModelLimits(context_window=settings.SARATHI_CONTEXT_WINDOW, max_output=MAX_OUTPUT)
            for name in settings.models
        }
    embedder = None
    if settings.EMBEDDING_MODEL:
        embedder = OpenAICompatibleEmbedder(settings.OPENAI_BASE_URL, settings.OPENAI_API_KEY, settings.EMBEDDING_MODEL)
    store = make_store()
    return Harness(
        OpenAICompatible(settings.OPENAI_BASE_URL, settings.OPENAI_API_KEY, limits=limits),
        store,
        [Sarathi],
        default_model=model or settings.default_model,
        deps_factory=deps_factory,
        memory=BuiltinMemory(store, embedder),
        compactor=PruneThenSummarize(),
        telemetry=get_telemetry(),
    )


async def close_harness(harness: Harness) -> None:
    await harness.store.close()
    await harness.provider.aclose()
    embedder = harness.memory.embedder if isinstance(harness.memory, BuiltinMemory) else None
    if embedder is not None:
        await embedder.aclose()


def harness_factory() -> HarnessFactory:
    return make_harness


FactoryDep = Annotated[HarnessFactory, Depends(harness_factory)]
