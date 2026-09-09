import asyncio
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI

from sarathi.agent import RuntimeResources, close_resources
from sarathi.main import lifespan


class AsyncCloser:
    def __init__(self, name: str, calls: list[str]) -> None:
        self.name = name
        self.calls = calls

    async def aclose(self) -> None:
        self.calls.append(self.name)

    async def close(self) -> None:
        self.calls.append(self.name)


async def test_close_resources_cancels_titles_and_closes_owned_resources() -> None:
    calls: list[str] = []
    started = asyncio.Event()

    async def title() -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(title())
    await started.wait()
    resources = RuntimeResources(
        runtime=AsyncCloser("runtime", calls),
        provider=AsyncCloser("provider", calls),
        store=AsyncCloser("store", calls),
        memory=SimpleNamespace(),
        embedder=AsyncCloser("embedder", calls),
        title_tasks={"root": task},
    )

    await close_resources(resources)

    assert task.cancelled()
    assert calls == ["runtime", "provider", "embedder", "store"]


async def test_lifespan_installs_resources_and_cleans_up(monkeypatch: Any) -> None:
    calls: list[str] = []
    resources = SimpleNamespace()

    async def make() -> Any:
        calls.append("make")
        return resources

    async def close(value: Any) -> None:
        assert value is resources
        calls.append("close")

    monkeypatch.setattr("sarathi.main.make_resources", make)
    monkeypatch.setattr("sarathi.main.close_resources", close)
    monkeypatch.setattr("sarathi.main.shutdown_telemetry", lambda: calls.append("telemetry"))
    monkeypatch.setattr("sarathi.main.get_settings", lambda: SimpleNamespace(UPLOAD_DIR="/tmp/sarathi-test-uploads"))
    application = FastAPI()

    async with lifespan(application):
        assert application.state.resources is resources
        assert calls == ["make"]

    assert calls == ["make", "close", "telemetry"]
