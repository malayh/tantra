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

import psycopg
import uvicorn

import tantra.acp.websocket as websocket_module
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
from tantra.acp import AcpAccess, AcpAuth, AcpServer, AcpWebSocket


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


class TransportMetrics:
    def __init__(self) -> None:
        self.frames = 0
        self.bytes = 0
        self.max_frame_bytes = 0
        self.max_send_ms = 0.0
        self.closes: Counter[str] = Counter()

    def clear(self) -> None:
        self.frames = 0
        self.bytes = 0
        self.max_frame_bytes = 0
        self.max_send_ms = 0.0
        self.closes.clear()

    def sent(self, frame: str, elapsed_ms: float) -> None:
        size = len(frame.encode())
        self.frames += 1
        self.bytes += size
        self.max_frame_bytes = max(self.max_frame_bytes, size)
        self.max_send_ms = max(self.max_send_ms, elapsed_ms)


class App:
    def __init__(self, dsn: str, schema: str, worker: str) -> None:
        self.dsn = dsn
        self.schema = schema
        self.worker = worker
        self.store = PostgresStore(dsn, schema=schema)
        self.provider = GatedProvider({"mode": "acp", "model": "bench/synthetic"})
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
        self.transport = TransportMetrics()
        self.started_cpu = time.process_time()
        self.endpoint = self._endpoint()

    def _endpoint(self) -> AcpWebSocket:
        async def bind(scope: Mapping[str, Any]) -> AcpAuth:
            self.auth["bind"] += 1
            if scope.get("query_string") != b"token=acp-bench":
                raise PermissionError("invalid benchmark credential")
            return AcpAuth(principal="acp-bench", submitted_by="acp-bench")

        async def metadata(_auth: AcpAuth, _meta: Mapping[str, Any]) -> Mapping[str, str]:
            return {}

        async def policy(access: AcpAccess) -> bool:
            self.auth[f"policy_{access.operation}"] += 1
            return True

        server = AcpServer(self.runtime, agent=BenchAgent, scope=metadata, policy=policy, name="acp-bench")
        return server.websocket(bind=bind, allowed_origins=set(), allow_missing_origin=True)

    async def start(self) -> None:
        await self.runtime.start()

    async def close(self) -> None:
        await self.runtime.aclose()
        await self.provider.aclose()
        await self.store.close()

    def reset(self) -> dict[str, Any]:
        METRICS.clear()
        self.auth.clear()
        self.transport.clear()
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
            "live_sockets": len(self.endpoint._live),
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
            "tree_queries": METRICS.tree_queries,
            "auth": dict(self.auth),
            "transport": {
                "frames": self.transport.frames,
                "bytes": self.transport.bytes,
                "max_frame_bytes": self.transport.max_frame_bytes,
                "max_send_ms": self.transport.max_send_ms,
                "closes": dict(self.transport.closes),
            },
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
        if scope["type"] == "websocket" and scope.get("path") == "/acp":
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
    original_send = websocket_module.WebSocketTransport.send
    original_close = websocket_module.WebSocketTransport.close

    async def measured_send(transport: Any, frame: str) -> None:
        started = time.perf_counter()
        await original_send(transport, frame)
        app.transport.sent(frame, (time.perf_counter() - started) * 1_000)

    async def measured_close(transport: Any, *args: Any, **kwargs: Any) -> None:
        closing = transport.code is None
        await original_close(transport, *args, **kwargs)
        if closing:
            app.transport.closes[str(transport.code)] += 1

    websocket_module.WebSocketTransport.send = measured_send
    websocket_module.WebSocketTransport.close = measured_close
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
        raise SystemExit("usage: acp_worker.py DSN SCHEMA PORT WORKER")
    asyncio.run(serve(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]))


if __name__ == "__main__":
    main()
