from collections.abc import Callable
from functools import cache
from typing import Annotated, Any

from fastapi import Depends

from sarathi.config import get_settings
from sarathi.provider import ReasoningCompat
from tantra import (
    Agent,
    BuiltinMemory,
    Context,
    Harness,
    MemoryWrite,
    ModelLimits,
    OpenAICompatibleEmbedder,
    PostgresStore,
    PruneThenSummarize,
    SessionHeader,
    tool,
)
from tantra.extratools.doc import read_doc
from tantra.extratools.web import web_fetch, web_search

MAX_OUTPUT = 8192
TITLE_CHARS = 80

HarnessFactory = Callable[[str | None], Harness]


@tool
async def memory_write(content: str, kind: str, ctx: Context) -> str:
    """Save one durable fact about this user and return its id.

    Write a single self-contained fact, decision or preference per call: `content` is the whole
    memory in one sentence, written so it still reads clearly months later. `kind` groups rows for
    filtered recall — pick a short lowercase label and reuse it ("preference", "decision", "fact").
    Do not save anything you can look up again. The user is asked to approve every write.
    """
    return await ctx.memory.write(
        MemoryWrite(
            kind=kind,
            title=content[:TITLE_CHARS],
            body=content,
            metadata={"user": ctx.deps["user_id"]},
        )
    )


@tool
async def memory_recall(query: str, ctx: Context) -> list[dict[str, Any]]:
    """Search what you have saved about this user and return the best matches, highest score first.

    Matching is keyword overlap against each memory's title and body, so query with the words you
    expect to find written there — a short phrase beats a single word. Deleted memories are never
    returned, and an empty list means nothing was saved about this yet.
    """
    hits = await ctx.memory.recall(query, metadata={"user": ctx.deps["user_id"]})
    return [
        {
            "id": hit.memory.id,
            "kind": hit.memory.kind,
            "title": hit.memory.title,
            "body": hit.memory.body,
            "score": hit.score,
            "mode": hit.mode,
        }
        for hit in hits
    ]


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
    Sarathi.tools = [*search, web_fetch(), read_doc(), memory_write, memory_recall]
    Researcher.tools = [*search, web_fetch()]


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
        ReasoningCompat(settings.OPENAI_BASE_URL, settings.OPENAI_API_KEY, limits=limits),
        store,
        [Sarathi],
        default_model=model or settings.default_model,
        deps_factory=deps_factory,
        memory=BuiltinMemory(store, embedder),
        compactor=PruneThenSummarize(),
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
