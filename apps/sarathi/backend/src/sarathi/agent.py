from collections.abc import Callable
from functools import cache
from typing import Annotated, Any

from fastapi import Depends

from sarathi.config import get_settings
from sarathi.provider import ReasoningCompat
from tantra import Agent, Harness, ModelLimits, PostgresStore, PruneThenSummarize, SessionHeader
from tantra.extratools.doc import read_doc
from tantra.extratools.web import web_fetch, web_search

MAX_OUTPUT = 8192

HarnessFactory = Callable[[str | None], Harness]


class Researcher(Agent):
    """Delegate a focused research task to a researcher that searches the web, fetches pages, and reports findings."""

    prompt = (
        "You are a research subagent. Work the task with web_search and web_fetch: search, judge the hits, "
        "read the most promising pages, and follow up when a source is thin. "
        "Report concrete findings with the URLs you actually read, and say plainly what you could not confirm."
    )
    tools = []


class Sarathi(Agent):
    prompt = (
        "You are Sarathi, a helpful AI assistant in a chat app. "
        "Answer clearly and concisely, and use markdown when it helps. "
        "You can search the web with web_search, read a page with web_fetch, and read an attached PDF or Word "
        "file with read_doc(path) using the path from an [attachment: name path=...] marker in the user's message. "
        "Hand deep or wide research to the researcher subagent and synthesise what it reports."
    )
    tools = []
    subagents = [Researcher]


@cache
def _wire_tools() -> None:
    settings = get_settings()
    search = [web_search(settings.BRAVE_API_KEY)] if settings.BRAVE_API_KEY else []
    Sarathi.tools = [*search, web_fetch(), read_doc()]
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
    return Harness(
        ReasoningCompat(settings.OPENAI_BASE_URL, settings.OPENAI_API_KEY, limits=limits),
        make_store(),
        [Sarathi],
        default_model=model or settings.default_model,
        deps_factory=deps_factory,
        compactor=PruneThenSummarize(),
    )


async def close_harness(harness: Harness) -> None:
    await harness.store.close()
    await harness.provider.aclose()


def harness_factory() -> HarnessFactory:
    return make_harness


FactoryDep = Annotated[HarnessFactory, Depends(harness_factory)]
