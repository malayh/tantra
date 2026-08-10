from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Depends

from sarathi.config import get_settings
from sarathi.provider import ReasoningCompat
from tantra import Agent, Harness, ModelLimits, PostgresStore, PruneThenSummarize, SessionHeader

MAX_OUTPUT = 8192

HarnessFactory = Callable[[str | None], Harness]


class Sarathi(Agent):
    prompt = (
        "You are Sarathi, a helpful AI assistant in a chat app. "
        "Answer clearly and concisely, and use markdown when it helps."
    )
    tools = []


def deps_factory(header: SessionHeader) -> dict[str, Any]:
    return {"user_id": header.metadata.get("user")}


def make_store() -> PostgresStore:
    return PostgresStore(get_settings().DATABASE_URL.replace("+psycopg", ""), schema="tantra")


def make_harness(model: str | None = None) -> Harness:
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
