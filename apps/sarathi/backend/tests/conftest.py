import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from httpx_ws import AsyncWebSocketSession, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from sarathi.agent import HarnessFactory, Sarathi, _wire_tools, deps_factory, harness_factory
from sarathi.config import get_settings
from sarathi.db import get_db
from sarathi.models import Base
from tantra import BuiltinMemory, FakeProvider, Harness, MemoryStore, Sample
from tantra.providers.base import ProviderEvent, SampleRequest, TextDelta

PASSWORD = "hunter2hunter2"


def pytest_configure() -> None:
    os.environ.setdefault("SECRET_KEY", "test-secret-that-is-long-enough-for-hs256")
    os.environ.setdefault("OPENAI_BASE_URL", "http://provider.invalid/v1")
    os.environ.setdefault("OPENAI_API_KEY", "test-key")
    os.environ.setdefault("SARATHI_MODELS", "test-model,other-model")


class SharedStore(MemoryStore):
    async def close(self) -> None:
        return None


class SharedProvider(FakeProvider):
    def __init__(self, samples: list[Sample]) -> None:
        super().__init__(samples)
        self.gate = asyncio.Event()
        self.gate.set()

    async def aclose(self) -> None:
        return None

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        async for event in super().stream(req):
            yield event
            if isinstance(event, TextDelta):
                await self.gate.wait()


@pytest.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
def upload_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    root = tmp_path / "uploads"
    monkeypatch.setenv("UPLOAD_DIR", str(root))
    get_settings.cache_clear()
    yield root
    get_settings.cache_clear()


@pytest.fixture
def store() -> SharedStore:
    return SharedStore()


@pytest.fixture
def provider() -> SharedProvider:
    return SharedProvider([])


@pytest.fixture
def factory(store: SharedStore, provider: SharedProvider) -> HarnessFactory:
    def build(model: str | None = None) -> Harness:
        _wire_tools()
        return Harness(
            provider,
            store,
            [Sarathi],
            default_model=model or "test-model",
            deps_factory=deps_factory,
            memory=BuiltinMemory(store),
        )

    return build


@pytest.fixture
async def app(session_factory: async_sessionmaker[AsyncSession], factory: HarnessFactory) -> AsyncIterator[FastAPI]:
    from sarathi.main import app as application

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    application.dependency_overrides[get_db] = override_get_db
    application.dependency_overrides[harness_factory] = lambda: factory
    yield application
    application.dependency_overrides.clear()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


Socket = Callable[[str, str], AbstractAsyncContextManager[AsyncWebSocketSession]]


@pytest.fixture
def socket(app: FastAPI) -> Socket:
    @asynccontextmanager
    async def open_socket(sid: str, token: str) -> AsyncIterator[AsyncWebSocketSession]:
        transport = ASGIWebSocketTransport(app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
            url = f"/api/ws/sessions/{sid}?token={token}"
            async with aconnect_ws(url, http_client, keepalive_ping_interval_seconds=None) as ws:
                yield ws

    return open_socket


@pytest.fixture
def signup(client: httpx.AsyncClient) -> Callable[..., Awaitable[str]]:
    async def register(email: str = "a@example.com") -> str:
        response = await client.post("/api/auth/signup", json={"email": email, "password": PASSWORD})
        assert response.status_code == 200
        return str(response.json()["access_token"])

    return register


@pytest.fixture
def new_session(client: httpx.AsyncClient) -> Callable[..., Awaitable[str]]:
    async def create(token: str, model: str | None = None) -> str:
        response = await client.post(
            "/api/sessions", json={"model": model}, headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 201
        return str(response.json()["id"])

    return create
