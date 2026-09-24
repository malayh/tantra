from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from sarathi.agent import MAX_OUTPUT, Sarathi, Subagent, _wire_tools, make_resources
from sarathi.config import get_settings
from tantra import CompactionApplied, MemoryStore, ModelLimits, PruneThenSummarize, Runtime
from tantra.context import TurnContext
from tantra.providers.base import ProviderEvent, SampleRequest, StreamEnd
from tantra.providers.openai_compat import FALLBACK_LIMITS
from tantra.tools import Context


@pytest.fixture
def resources_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SARATHI_MODELS", "test-model,other-model")
    monkeypatch.setenv("EMBEDDING_MODEL", "")
    monkeypatch.setattr("sarathi.agent.make_store", MemoryStore)
    monkeypatch.setattr("sarathi.agent.get_telemetry", lambda: None)
    get_settings.cache_clear()
    _wire_tools.cache_clear()
    yield
    get_settings.cache_clear()
    _wire_tools.cache_clear()


async def close_test_resources(resources: object) -> None:
    await resources.runtime.aclose()
    await resources.provider.aclose()


async def test_explicit_context_window_maps_every_model_and_bypasses_discovery(
    monkeypatch: pytest.MonkeyPatch,
    resources_config: None,
) -> None:
    monkeypatch.setenv("SARATHI_CONTEXT_WINDOW", "200000")
    get_settings.cache_clear()
    resources = await make_resources()
    catalogue = AsyncMock(side_effect=AssertionError("catalogue request"))
    resources.provider._client.models.list = catalogue
    try:
        expected = ModelLimits(context_window=200_000, max_output=MAX_OUTPUT)
        assert await resources.provider.limits("test-model") == expected
        assert await resources.provider.limits("other-model") == expected
        assert catalogue.await_count == 0
        assert isinstance(resources.runtime.compactor, PruneThenSummarize)
        assert set(resources.runtime.agents) == {"sarathi", "subagent"}
    finally:
        await close_test_resources(resources)


async def test_unset_context_window_allows_catalogue_discovery(
    monkeypatch: pytest.MonkeyPatch,
    resources_config: None,
) -> None:
    monkeypatch.delenv("SARATHI_CONTEXT_WINDOW", raising=False)
    get_settings.cache_clear()
    resources = await make_resources()
    catalogue = AsyncMock(
        return_value=SimpleNamespace(
            data=[
                SimpleNamespace(
                    model_dump=lambda: {
                        "id": "test-model",
                        "context_length": 300_000,
                        "max_completion_tokens": 12_000,
                    }
                )
            ]
        )
    )
    resources.provider._client.models.list = catalogue
    try:
        assert await resources.provider.limits("test-model") == ModelLimits(
            context_window=300_000,
            max_output=12_000,
        )
        assert catalogue.await_count == 1
    finally:
        await close_test_resources(resources)


async def test_catalogue_failure_uses_cached_conservative_fallback(
    monkeypatch: pytest.MonkeyPatch,
    resources_config: None,
) -> None:
    monkeypatch.delenv("SARATHI_CONTEXT_WINDOW", raising=False)
    get_settings.cache_clear()
    resources = await make_resources()
    catalogue = AsyncMock(side_effect=RuntimeError("catalogue unavailable"))
    resources.provider._client.models.list = catalogue
    try:
        assert await resources.provider.limits("test-model") == FALLBACK_LIMITS
        assert await resources.provider.limits("other-model") == FALLBACK_LIMITS
        assert catalogue.await_count == 1
        assert FALLBACK_LIMITS == ModelLimits(context_window=128_000, max_output=4_096)
    finally:
        await close_test_resources(resources)


class RecordingCompactor:
    def __init__(self) -> None:
        self.seen: set[str] = set()

    async def compact(self, ctx: TurnContext) -> list[CompactionApplied]:
        if ctx.session_id in self.seen:
            return []
        self.seen.add(ctx.session_id)
        return [
            CompactionApplied(
                strategy="recording",
                tokens_before=2,
                tokens_after=1,
                summary=f"summary:{ctx.session_id}",
                floor_turn_id=ctx.turn_id,
            )
        ]


class CompletingProvider:
    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=128_000, max_output=4_096)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="done")


async def test_root_and_subagent_compaction_journals_are_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Sarathi, "tools", [])
    monkeypatch.setattr(Subagent, "tools", [])
    store = MemoryStore()
    compactor = RecordingCompactor()
    runtime = Runtime(CompletingProvider(), store, [Sarathi], default_model="test-model", compactor=compactor)
    root_id = await runtime.create(Sarathi)

    async def emit(_: str) -> None:
        return None

    try:
        async with runtime.connect(root_id, writable=True) as connection:
            await connection.prompt("root work", command_id=uuid4())
        root = await store.header(root_id.hex)
        assert root is not None
        child_public = await runtime._actor_spawn(
            root,
            Sarathi,
            Context(
                session_id=root.id,
                turn_id=uuid4().hex,
                call_id=uuid4().hex,
                depth=0,
                deps=None,
                store=store,
                emit=emit,
            ),
            "subagent",
            "child work",
        )
        child_id = UUID(child_public).hex

        async def idle() -> None:
            while runtime.active:
                await asyncio.sleep(0)

        await asyncio.wait_for(idle(), timeout=2)
        root_events = [item.event for item in await store.read_page(root.id)]
        child_events = [item.event for item in await store.read_page(child_id)]
        root_summaries = [event.summary for event in root_events if isinstance(event, CompactionApplied)]
        child_summaries = [event.summary for event in child_events if isinstance(event, CompactionApplied)]
        assert root_summaries == [f"summary:{root.id}"]
        assert child_summaries == [f"summary:{child_id}"]
        assert compactor.seen == {root.id, child_id}
    finally:
        await runtime.aclose()
