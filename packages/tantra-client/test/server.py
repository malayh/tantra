from __future__ import annotations

import asyncio
import json
import socket
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any
from uuid import uuid4

import uvicorn

from tantra import (
    Agent,
    CommandEnvelope,
    CommandReply,
    CommandTimeout,
    PostgresCoordinator,
    PostgresStore,
    Runtime,
    SessionHeader,
    tool,
)
from tantra.events import LoggedEvent, SessionCreated, TextDelta
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS
from tantra.websocket import InputCodec, SessionSocket, SocketAccess, SocketAuth, SocketBinding


class LossyCoordinator(PostgresCoordinator):
    def __init__(self, store: PostgresStore) -> None:
        super().__init__(store, lease_ttl=5, request_timeout=5, catch_up_interval=0.02)
        self.lose_next: str | None = None

    async def request(self, envelope: CommandEnvelope) -> CommandReply:
        reply = await super().request(envelope)
        if self.lose_next == envelope.operation:
            self.lose_next = None
            raise CommandTimeout("integration fixture lost a committed receipt")
        return reply


class Provider:
    def __init__(self, name: str) -> None:
        self.name = name
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, request: SampleRequest) -> AsyncIterator[ProviderEvent]:
        latest = next((message.content for message in reversed(request.messages) if message.role == "user"), "")
        if latest == "blocked":
            self.blocked.set()
            await self.release.wait()
            yield StreamEnd(text=f"{self.name}:{latest}")
            return
        if any(message.role == "tool" and message.call_id == "deploy-call" for message in request.messages):
            yield StreamEnd(text=f"{self.name}:approved")
            return
        if any(message.role == "user" and message.content == "approval" for message in request.messages):
            yield ToolCall(id="deploy-call", name="deploy", args='{"target":"prod"}')
            yield StreamEnd(tool_calls=[ToolCall(id="deploy-call", name="deploy", args='{"target":"prod"}')])
            return
        yield StreamEnd(text=f"{self.name}:{latest}")


class Approver(Agent):
    pass


class Fixture:
    def __init__(self, dsn: str, schema: str) -> None:
        self.dsn = dsn
        self.schema = schema
        self.root_id = uuid4()
        self.child_id = uuid4()
        self.invoked: list[str] = []
        self.approvals = 0
        self.stores: list[PostgresStore] = []
        self.runtimes: list[Runtime] = []
        self.coordinators: list[LossyCoordinator] = []
        self.providers = [Provider("one"), Provider("two")]
        self.endpoints: dict[str, SessionSocket] = {}
        self.anchor: Any = None

    async def start(self) -> None:
        @tool(permission="ask")
        async def deploy(target: str) -> str:
            self.invoked.append(target)
            return target

        Approver.tools = [deploy]
        for provider in self.providers:
            store = PostgresStore(self.dsn, schema=self.schema)
            coordinator = LossyCoordinator(store)
            runtime = Runtime(provider, store, [Approver], default_model="m", coordinator=coordinator)
            await runtime.start()
            self.stores.append(store)
            self.coordinators.append(coordinator)
            self.runtimes.append(runtime)
        await self.runtimes[0].create(Approver, session_id=self.root_id)
        ownership = await self.coordinators[0].acquire(self.root_id.hex)
        assert ownership is not None
        try:
            async with self.coordinators[0].transaction(ownership) as store:
                await store.append(self.root_id.hex, [TextDelta(text="root-seed")])
                await store.create(
                    SessionHeader(
                        id=self.child_id.hex,
                        root_id=self.root_id.hex,
                        parent_id=self.root_id.hex,
                        depth=1,
                        agent="approver",
                        model="m",
                    )
                )
                await store.append(
                    self.child_id.hex,
                    [
                        SessionCreated(
                            agent="approver",
                            root_id=self.root_id.hex,
                            parent_id=self.root_id.hex,
                            depth=1,
                            model="m",
                        ),
                        TextDelta(text="child-seed"),
                    ],
                )
        finally:
            await self.coordinators[0].release(ownership)
        self.anchor = self.runtimes[0].connect(self.root_id, writable=True)
        await self.anchor.__aenter__()
        self.endpoints = {
            "/one": self._endpoint(self.runtimes[0]),
            "/two": self._endpoint(self.runtimes[1]),
        }

    def _endpoint(self, runtime: Runtime) -> SessionSocket:
        async def revalidate() -> bool:
            return True

        async def bind(scope: Mapping[str, Any]) -> SocketBinding:
            if scope.get("query_string") != b"token=test":
                raise PermissionError("invalid integration credential")
            return SocketBinding(
                self.root_id,
                SocketAuth(principal="integration", revalidate=revalidate, submitted_by="integration-user"),
            )

        async def policy(access: SocketAccess) -> bool:
            if access.operation != "answer":
                return True
            self.approvals += 1
            return access.ask is not None and access.ask.event.request.extra == {
                "permission": "deploy",
                "arguments": {"target": "prod"},
            }

        async def views(context: Any) -> Callable[[LoggedEvent], Awaitable[Any]]:
            async def project(event: LoggedEvent) -> Any:
                return event.event.model_dump(mode="json")

            return project

        return SessionSocket(
            runtime,
            bind=bind,
            policy=policy,
            input_codec=InputCodec(str, lambda value: value),
            views=views,
            allowed_origins=set(),
            allow_missing_origin=True,
        )

    async def __call__(
        self,
        scope: Mapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] == "websocket":
            endpoint = self.endpoints.get(scope.get("path", ""))
            if endpoint is None:
                await send({"type": "websocket.close", "code": 4404, "reason": "not found"})
                return
            await endpoint(scope, receive, send)
            return
        if scope["type"] == "http":
            await self._http(scope, receive, send)

    async def _http(
        self,
        scope: Mapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        body = b""
        more = True
        while more:
            message = await receive()
            body += message.get("body", b"")
            more = message.get("more_body", False)
        path = scope.get("path")
        status = 200
        result: Any
        if path == "/control/state":
            result = self.state()
        elif path == "/control/journal":
            result = {
                str(actor_id): [item.event.model_dump(mode="json") async for item in self.stores[0].read(actor_id.hex)]
                for actor_id in (self.root_id, self.child_id)
            }
        elif path == "/control/release" and scope.get("method") == "POST":
            for provider in self.providers:
                provider.release.set()
            result = self.state()
        elif path == "/control/lose" and scope.get("method") == "POST":
            request = json.loads(body)
            self.coordinators[int(request["worker"]) - 1].lose_next = request["operation"]
            result = self.state()
        elif path == "/control/delete" and scope.get("method") == "POST":
            result = {"deleted": await self.runtimes[0].delete(self.root_id, allow_active=True)}
        else:
            status = 404
            result = {"error": "not found"}
        encoded = json.dumps(result, separators=(",", ":")).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(encoded)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": encoded})

    def state(self) -> dict[str, Any]:
        workers = []
        for runtime, coordinator in zip(self.runtimes, self.coordinators, strict=True):
            workers.append(
                {
                    "connections": len(runtime._connections),
                    "connection_interests": len(runtime._connection_interests),
                    "stream_interests": len(runtime._stream_interests),
                    "watchers": len(runtime._watchers),
                    "page_readers": len(runtime._pages.readers),
                    "actor_interests": len(coordinator._actor_interests),
                    "observations": len(coordinator._observations),
                }
            )
        return {
            "rootId": str(self.root_id),
            "childId": str(self.child_id),
            "blocked": [provider.blocked.is_set() for provider in self.providers],
            "invoked": self.invoked,
            "approvals": self.approvals,
            "workers": workers,
        }

    async def close(self) -> None:
        if self.anchor is not None:
            await self.anchor.__aexit__(None, None, None)
        for runtime in reversed(self.runtimes):
            await runtime.aclose()
        for store in reversed(self.stores):
            await store.close()


async def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: server.py DSN SCHEMA PORT")
    fixture = Fixture(sys.argv[1], sys.argv[2])
    await fixture.start()
    listener = socket.socket()
    listener.bind(("127.0.0.1", int(sys.argv[3])))
    listener.listen()
    listener.setblocking(False)
    config = uvicorn.Config(fixture, log_config=None, log_level="warning", access_log=False, lifespan="off")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        while not server.started and not task.done():
            await asyncio.sleep(0.01)
        if task.done():
            await task
        print(
            json.dumps(
                {
                    "baseUrl": f"http://127.0.0.1:{sys.argv[3]}",
                    "rootId": str(fixture.root_id),
                    "childId": str(fixture.child_id),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )
        await task
    finally:
        server.should_exit = True
        if not task.done():
            await task
        listener.close()
        await fixture.close()


if __name__ == "__main__":
    asyncio.run(main())
