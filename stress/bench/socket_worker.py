from __future__ import annotations

import asyncio
import json
import os
import resource
import socket
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from uuid import UUID

import psycopg
import uvicorn

import tantra.websocket as websocket_module
from stress.bench.worker import (
    COORDINATOR_SETTINGS,
    METRICS,
    BenchAgent,
    GatedProvider,
    MeasuredConnection,
    coordinator_stats,
    loop_probe,
    pool_stats,
    rss_mb,
)
from tantra import PostgresCoordinator, PostgresStore, Runtime
from tantra.events import LoggedEvent
from tantra.websocket import InputCodec, SessionSocket, SocketAccess, SocketAuth, SocketBinding


class Slots:
    def __init__(self, size: int) -> None:
        self.semaphore = asyncio.Semaphore(size)
        self.current = 0
        self.peak = 0

    async def __aenter__(self) -> None:
        await self.semaphore.acquire()
        self.current += 1
        self.peak = max(self.peak, self.current)

    async def __aexit__(self, *_args: Any) -> None:
        self.current -= 1
        self.semaphore.release()


class QueueMetrics:
    def __init__(self) -> None:
        self.frames = 0
        self.bytes = 0

    def clear(self) -> None:
        self.frames = 0
        self.bytes = 0


class App:
    def __init__(self, dsn: str, schema: str, worker: str) -> None:
        self.dsn = dsn
        self.schema = schema
        self.worker = worker
        self.store = PostgresStore(dsn, schema=schema)
        self.provider = GatedProvider({"mode": "socket", "model": "bench/synthetic"})
        self.slots = Slots(4)
        self.runtime = Runtime(
            self.provider,
            self.store,
            [BenchAgent],
            default_model="bench/synthetic",
            deps_factory=lambda _: (dsn, schema, self.slots),
            coordinator=PostgresCoordinator(self.store, **COORDINATOR_SETTINGS),
        )
        self.auth: Counter[str] = Counter()
        self.queue = QueueMetrics()
        self.started_cpu = time.process_time()
        self.endpoint = self._endpoint()

    def _endpoint(self) -> SessionSocket:
        async def bind(scope: Mapping[str, Any]) -> SocketBinding:
            self.auth["bind"] += 1
            if scope.get("query_string") != b"token=socket-bench":
                raise PermissionError("invalid benchmark credential")
            path = str(scope.get("path", "")).strip("/")
            try:
                root_id = UUID(path)
            except ValueError as exc:
                raise PermissionError("invalid benchmark root") from exc

            async def revalidate() -> bool:
                self.auth["revalidate"] += 1
                return True

            return SocketBinding(
                root_id,
                SocketAuth(principal="socket-bench", revalidate=revalidate, submitted_by="socket-bench"),
            )

        async def policy(access: SocketAccess) -> bool:
            self.auth[f"policy_{access.operation}"] += 1
            return access.actor.root_id == access.binding.root_id.hex

        async def views(_context: Any) -> Callable[[LoggedEvent], Awaitable[Any]]:
            async def project(event: LoggedEvent) -> Any:
                return event.event.model_dump(mode="json")

            return project

        return SessionSocket(
            self.runtime,
            bind=bind,
            policy=policy,
            input_codec=InputCodec(str, lambda value: value),
            views=views,
            allowed_origins=set(),
            allow_missing_origin=True,
        )

    async def start(self) -> None:
        await self.runtime.start()

    async def close(self) -> None:
        await self.runtime.aclose()
        await self.provider.aclose()
        await self.store.close()

    def reset(self) -> dict[str, Any]:
        METRICS.clear()
        self.auth.clear()
        self.queue.clear()
        self.slots.peak = self.slots.current
        self.started_cpu = time.process_time()
        return self.metrics()

    def metrics(self) -> dict[str, Any]:
        coordinator = self.runtime.coordinator
        resources = {
            "connections": len(self.runtime._connections),
            "connection_interests": len(self.runtime._connection_interests),
            "stream_interests": len(self.runtime._stream_interests),
            "watchers": len(self.runtime._watchers),
            "page_readers": len(self.runtime._pages.readers),
            "actor_interests": len(coordinator._actor_interests),
            "observations": len(coordinator._observations),
        }
        return {
            "worker": self.worker,
            "pid": os.getpid(),
            "sql_calls": METRICS.sql_calls,
            "fetched_rows": METRICS.fetched_rows,
            "sql_ms": METRICS.sql_ms,
            "journal_queries": METRICS.journal_queries,
            "journal_rows": METRICS.journal_rows,
            "event_body_queries": METRICS.event_body_queries,
            "event_body_rows": METRICS.event_body_rows,
            "result_queries": METRICS.result_queries,
            "header_queries": METRICS.header_queries,
            "auth": dict(self.auth),
            "auth_sql_connections": 0,
            "queue": {"max_frames": self.queue.frames, "max_bytes": self.queue.bytes},
            "pool": pool_stats(self.runtime),
            "coordinator": coordinator_stats(self.runtime),
            "control_connection": coordinator._control_conn is not None and not coordinator._control_conn.closed,
            "listen_connection": coordinator._listener_conn is not None and not coordinator._listener_conn.closed,
            "tool_connections": {"current": self.slots.current, "peak": self.slots.peak, "limit": 4},
            "resources": resources,
            "rss_mb": rss_mb(),
            "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1_024,
            "cpu_ms": (time.process_time() - self.started_cpu) * 1_000,
            "loop_lag_ms": list(METRICS.loop_lag_ms),
        }

    async def __call__(
        self,
        scope: Mapping[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] == "websocket":
            await self.endpoint(scope, receive, send)
            return
        if scope["type"] != "http":
            return
        more = True
        while more:
            message = await receive()
            more = bool(message.get("more_body"))
        path = scope.get("path")
        if path == "/control/metrics" and scope.get("method") == "GET":
            status, result = 200, self.metrics()
        elif path == "/control/reset" and scope.get("method") == "POST":
            status, result = 200, self.reset()
        else:
            status, result = 404, {"error": "not found"}
        encoded = json.dumps(result, separators=(",", ":")).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(encoded)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": encoded})


async def serve(dsn: str, schema: str, port: int, worker: str) -> None:
    psycopg.AsyncConnection = MeasuredConnection
    app = App(dsn, schema, worker)
    original_put = websocket_module._Queue.put

    async def measured_put(queue: Any, item: Any) -> None:
        await original_put(queue, item)
        app.queue.frames = max(app.queue.frames, queue.frames)
        app.queue.bytes = max(app.queue.bytes, queue.bytes)

    websocket_module._Queue.put = measured_put
    probe = asyncio.create_task(loop_probe())
    listener = socket.socket()
    listener.bind(("127.0.0.1", port))
    listener.listen()
    listener.setblocking(False)
    server = uvicorn.Server(uvicorn.Config(app, log_config=None, log_level="warning", access_log=False, lifespan="off"))
    task: asyncio.Task[Any] | None = None
    try:
        await app.start()
        task = asyncio.create_task(server.serve(sockets=[listener]))
        while not server.started and not task.done():
            await asyncio.sleep(0.01)
        if task.done():
            await task
        print(json.dumps({"ready": True, "pid": os.getpid(), "port": port}), flush=True)
        await task
    finally:
        server.should_exit = True
        if task is not None and not task.done():
            await task
        listener.close()
        probe.cancel()
        await asyncio.gather(probe, return_exceptions=True)
        await app.close()


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit("usage: socket_worker.py DSN SCHEMA PORT WORKER")
    asyncio.run(serve(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]))


if __name__ == "__main__":
    main()
