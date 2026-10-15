import asyncio
import itertools
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from httpx_ws import AsyncWebSocketSession, WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from starlette.types import ASGIApp

from sarathi.agent import SKILLS_DIR, RuntimeResources, Sarathi, TitleHook, _wire_tools, deps_factory
from sarathi.config import get_settings
from sarathi.db import get_db
from sarathi.models import Base
from tantra import BuiltinMemory, FakeProvider, FileSystemSkills, MemoryStore, Runtime, Sample
from tantra.providers.base import ProviderEvent, SampleRequest, TextDelta

PASSWORD = "hunter2hunter2"
ORIGIN = "http://localhost:3000"
TIMEOUT = 5.0


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
        self.route: Callable[[SampleRequest], Sample] | None = None

    async def aclose(self) -> None:
        return None

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        stream = FakeProvider([self.route(req)]).stream(req) if self.route is not None else super().stream(req)
        if self.route is not None:
            self.requests.append(req)
        async for event in stream:
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
async def resources(store: SharedStore, provider: SharedProvider) -> AsyncIterator[RuntimeResources]:
    _wire_tools()
    await store.setup()
    memory = BuiltinMemory(store)
    title_hook = TitleHook(provider, store)
    runtime = Runtime(
        provider,
        store,
        [Sarathi],
        default_model="test-model",
        deps_factory=deps_factory,
        skills=FileSystemSkills(SKILLS_DIR),
        memory=memory,
        hooks=[title_hook],
    )
    title_hook.runtime = runtime
    resources = RuntimeResources(
        runtime=runtime,
        provider=provider,
        store=store,
        memory=memory,
        embedder=None,
        title_hook=title_hook,
    )
    yield resources
    await runtime.aclose()


@pytest.fixture
async def app(
    session_factory: async_sessionmaker[AsyncSession],
    resources: RuntimeResources,
) -> AsyncIterator[FastAPI]:
    from sarathi.main import create_app, mount_acp

    application = create_app()

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    application.state.resources = resources
    application.dependency_overrides[get_db] = override_get_db
    mount_acp(application, resources)
    yield application
    application.dependency_overrides.clear()


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client


@asynccontextmanager
async def dial(app: ASGIApp, url: str, origin: str | None = ORIGIN) -> AsyncIterator[AsyncWebSocketSession]:
    transport = ASGIWebSocketTransport(app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http_client:
        headers = {"Origin": origin} if origin is not None else {}
        async with aconnect_ws(url, http_client, keepalive_ping_interval_seconds=None, headers=headers) as ws:
            yield ws


class Rpc:
    def __init__(self, ws: AsyncWebSocketSession) -> None:
        self.ws = ws
        self.ids = itertools.count(1)
        self.responses: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self.updates: list[dict[str, Any]] = []
        self.requests: list[dict[str, Any]] = []
        self.changed = asyncio.Event()
        self.initialized: dict[str, Any] = {}

    async def run(self) -> None:
        try:
            while True:
                message = json.loads(await self.ws.receive_text())
                if "method" not in message:
                    self.responses.pop(message["id"]).set_result(message)
                elif message["method"] == "session/update":
                    self.updates.append(message["params"])
                elif "id" in message:
                    self.requests.append(message)
                self.changed.set()
        except WebSocketDisconnect:
            for future in self.responses.values():
                future.cancel()

    async def _send(self, message: dict[str, Any]) -> None:
        await self.ws.send_text(json.dumps({"jsonrpc": "2.0", **message}))

    async def request(self, method: str, params: Any = None) -> dict[str, Any]:
        request_id = next(self.ids)
        self.responses[request_id] = future = asyncio.get_running_loop().create_future()
        await self._send({"id": request_id, "method": method, **({} if params is None else {"params": params})})
        return await asyncio.wait_for(future, TIMEOUT)

    async def call(self, method: str, params: Any = None) -> Any:
        response = await self.request(method, params)
        assert "error" not in response, response
        return response["result"]

    async def notify(self, method: str, params: Any) -> None:
        await self._send({"method": method, "params": params})

    async def reply(self, request: dict[str, Any], result: Any) -> None:
        await self._send({"id": request["id"], "result": result})

    async def new(self, cwd: str = "/work") -> str:
        return str((await self.call("session/new", {"cwd": cwd, "mcpServers": []}))["sessionId"])

    async def prompt(self, session_id: str, *blocks: dict[str, Any]) -> dict[str, Any]:
        return await self.request("session/prompt", {"sessionId": session_id, "prompt": list(blocks)})

    def of(self, session_id: str, kind: str) -> list[dict[str, Any]]:
        return [
            params["update"]
            for params in self.updates
            if params["sessionId"] == session_id and params["update"]["sessionUpdate"] == kind
        ]

    async def until(self, condition: Callable[[], Any], timeout: float = TIMEOUT) -> Any:
        async def poll() -> Any:
            while True:
                self.changed.clear()
                if found := condition():
                    return found
                await self.changed.wait()

        return await asyncio.wait_for(poll(), timeout)


Acp = Callable[..., AbstractAsyncContextManager[Rpc]]


@pytest.fixture
def acp(app: FastAPI, client: httpx.AsyncClient) -> Acp:
    @asynccontextmanager
    async def connect(token: str, *, mode: str = "write", path: str = "/api/acp") -> AsyncIterator[Rpc]:
        issued = await client.post("/api/acp/ticket", json={"mode": mode}, headers={"Authorization": f"Bearer {token}"})
        assert issued.status_code == 200
        async with dial(app, f"{path}?ticket={issued.json()['ticket']}") as ws:
            rpc = Rpc(ws)
            reader = asyncio.create_task(rpc.run())
            try:
                rpc.initialized = await rpc.call(
                    "initialize",
                    {
                        "protocolVersion": 2,
                        "info": {"name": "test", "version": "0"},
                        "capabilities": {"_meta": {"tantra": {"version": 1}}},
                    },
                )
                yield rpc
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    return connect


@pytest.fixture
def signup(client: httpx.AsyncClient) -> Callable[..., Awaitable[str]]:
    async def register(email: str = "a@example.com") -> str:
        response = await client.post("/api/auth/signup", json={"email": email, "password": PASSWORD})
        assert response.status_code == 200
        return str(response.json()["access_token"])

    return register


@pytest.fixture
def new_session(acp: Acp) -> Callable[..., Awaitable[str]]:
    async def create(token: str) -> str:
        async with acp(token) as rpc:
            return await rpc.new()

    return create
