from __future__ import annotations

import asyncio
import json
import socket
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import parse_qs
from uuid import UUID

import uvicorn

from tantra import Agent, PostgresCoordinator, PostgresStore, Runtime, tool
from tantra.acp import AcpAccess, AcpAuth, AcpServer, AcpWebSocket
from tantra.events import TextDelta
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS

Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
CALLS = {
    "spawn": ToolCall(id="spawn-call", name="spawn", args=json.dumps({"agent_name": "worker", "input": "dig"})),
    "deploy": ToolCall(id="deploy-call", name="deploy", args=json.dumps({"target": "prod"})),
}


class Provider:
    def __init__(self, release: asyncio.Event) -> None:
        self.release = release

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, request: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if request.model == "worker":
            await self.release.wait()
            yield StreamEnd(text="dug")
            return
        index = max((i for i, message in enumerate(request.messages) if message.role == "user"), default=-1)
        latest = request.messages[index].content if index >= 0 else ""
        answered = any(message.role == "tool" for message in request.messages[index + 1 :])
        call = CALLS.get(latest)
        if call is not None and not answered:
            yield StreamEnd(tool_calls=[call])
            return
        reply = f"echo:{latest}"
        yield TextDelta(text=reply[:4])
        yield TextDelta(text=reply[4:])
        yield StreamEnd(text=reply)


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    subagents = [Worker]


async def scope(auth: AcpAuth, meta: Mapping[str, Any]) -> dict[str, str]:
    return {"user": "integration"}


async def allow(access: AcpAccess) -> bool:
    return True


async def bind(scope: Mapping[str, Any]) -> AcpAuth:
    token = parse_qs(scope.get("query_string", b"").decode()).get("token", [""])[0]
    if token not in ("a", "b"):
        raise PermissionError("invalid integration credential")
    return AcpAuth(principal=token, submitted_by=f"user-{token}")


class Fixture:
    def __init__(self, dsn: str, schema: str) -> None:
        self.dsn = dsn
        self.schema = schema
        self.release = asyncio.Event()
        self.invoked: list[str] = []
        self.stores: list[PostgresStore] = []
        self.runtimes: list[Runtime] = []
        self.endpoints: dict[str, AcpWebSocket] = {}
        self.connections: list[list[dict[str, Any]]] = []
        self.drop_next = False
        self.server: uvicorn.Server | None = None

    async def start(self) -> None:
        @tool(permission="ask")
        async def deploy(target: str) -> str:
            self.invoked.append(target)
            return target

        Root.tools = [deploy]
        for path in ("/one", "/two"):
            store = PostgresStore(self.dsn, schema=self.schema)
            coordinator = PostgresCoordinator(store, lease_ttl=5, request_timeout=5, catch_up_interval=0.02)
            runtime = Runtime(Provider(self.release), store, [Root], default_model="root", coordinator=coordinator)
            await runtime.start()
            self.stores.append(store)
            self.runtimes.append(runtime)
            server = AcpServer(runtime, agent=Root, scope=scope, policy=allow, name="integration")
            self.endpoints[path] = server.websocket(bind=bind, allowed_origins=set(), allow_missing_origin=True)

    async def __call__(self, scope: Mapping[str, Any], receive: Receive, send: Send) -> None:
        if scope["type"] == "websocket":
            endpoint = self.endpoints.get(scope.get("path", ""))
            if endpoint is None:
                await send({"type": "websocket.close", "code": 1008, "reason": "not found"})
                return
            frames: list[dict[str, Any]] = []
            self.connections.append(frames)
            await endpoint(scope, self._receive(frames, receive), self._send(frames, send))
        elif scope["type"] == "http":
            await self._http(scope, receive, send)

    def _receive(self, frames: list[dict[str, Any]], receive: Receive) -> Receive:
        async def recorded() -> dict[str, Any]:
            message = await receive()
            if message["type"] == "websocket.receive" and message.get("text") is not None:
                frames.append({"from": "client", "frame": json.loads(message["text"])})
            return message

        return recorded

    def _send(self, frames: list[dict[str, Any]], send: Send) -> Send:
        async def recorded(message: dict[str, Any]) -> None:
            if message["type"] == "websocket.send":
                frame = json.loads(message["text"])
                result = frame.get("result")
                if self.drop_next and isinstance(result, dict) and "messageId" in result:
                    self.drop_next = False
                    await send({"type": "websocket.close", "code": 1011, "reason": "dropped"})
                    raise OSError("integration fixture dropped a prompt response")
                frames.append({"from": "server", "frame": frame})
            await send(message)

        return recorded

    async def _http(self, scope: Mapping[str, Any], receive: Receive, send: Send) -> None:
        more = True
        while more:
            more = (await receive()).get("more_body", False)
        path = scope.get("path")
        post = scope.get("method") == "POST"
        status = 200
        result: Any
        if path == "/control/state":
            result = {"invoked": self.invoked, "connections": len(self.connections)}
        elif path == "/control/journal":
            actor = parse_qs(scope.get("query_string", b"").decode())["actor"][0]
            result = [item.event.model_dump(mode="json") async for item in self.stores[0].read(UUID(actor).hex)]
        elif path == "/control/frames":
            result = self.connections
        elif path == "/control/release" and post:
            self.release.set()
            result = {}
        elif path == "/control/drop" and post:
            self.drop_next = True
            result = {}
        elif path == "/control/shutdown" and post and self.server is not None:
            self.server.should_exit = True
            result = {}
        else:
            status = 404
            result = {"error": "not found"}
        encoded = json.dumps(result, separators=(",", ":")).encode()
        headers = [(b"content-type", b"application/json"), (b"content-length", str(len(encoded)).encode())]
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": encoded})

    async def close(self) -> None:
        for endpoint in self.endpoints.values():
            await endpoint.aclose()
        for runtime in reversed(self.runtimes):
            await runtime.aclose()
        for store in reversed(self.stores):
            await store.close()


async def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: server.py DSN SCHEMA PORT")
    fixture = Fixture(sys.argv[1], sys.argv[2])
    listener = socket.socket()
    try:
        await fixture.start()
        listener.bind(("127.0.0.1", int(sys.argv[3])))
        listener.listen()
        listener.setblocking(False)
        config = uvicorn.Config(fixture, log_config=None, log_level="warning", access_log=False, lifespan="off")
        fixture.server = uvicorn.Server(config)
        task = asyncio.create_task(fixture.server.serve(sockets=[listener]))
        while not fixture.server.started and not task.done():
            await asyncio.sleep(0.01)
        if task.done():
            await task
        print(json.dumps({"baseUrl": f"http://127.0.0.1:{sys.argv[3]}"}), flush=True)
        await task
    finally:
        listener.close()
        await fixture.close()


if __name__ == "__main__":
    asyncio.run(main())
