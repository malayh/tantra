from __future__ import annotations

import asyncio
import hashlib
import json
import os
import resource
import time
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg import sql

from stress.bench.providers import RecordedProvider, live_provider
from stress.driver import SyntheticProvider, call_policy
from stress.invariants import check_log, check_pairs, pairs_intact
from tantra import (
    Agent,
    Context,
    ModelLimits,
    PostgresCoordinator,
    PostgresStore,
    Runtime,
    WriterReplaced,
    WriterRequired,
    tool,
)
from tantra.agent import agent_name
from tantra.context import build_messages
from tantra.events import (
    CompactionApplied,
    InputQueued,
    ReasoningDelta,
    ReasoningPart,
    SampleCompleted,
    SampleStarted,
    SessionCreated,
    SessionHeader,
    TextDelta,
    TextPart,
    ToolCallCompleted,
    ToolCallDelta,
    ToolCallRequested,
    ToolCallStarted,
    TurnCompleted,
    TurnStarted,
)
from tantra.providers.base import ProviderEvent, SampleRequest

INPUT = "Call fixture_total exactly once, then reply with only the numeric total."
COORDINATOR_SETTINGS = {"lease_ttl": 10.0, "request_timeout": 30.0, "catch_up_interval": 0.2}


@tool(description="Read the sum of amounts in the benchmark PostgreSQL fixture.")
async def fixture_total(ctx: Context) -> int:
    dsn, schema, slots = ctx.deps
    async with slots, await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        query = sql.SQL("SELECT sum(amount) FROM {}.fixture").format(sql.Identifier(schema))
        row = await (await conn.execute(query)).fetchone()
    return int(row[0])


class BenchAgent(Agent):
    prompt = "Use fixture_total to answer the user's question. Return only the numeric total."
    tools = [fixture_total]
    max_steps = 6


@dataclass
class Metrics:
    sql_calls: int = 0
    fetched_rows: int = 0
    sql_ms: float = 0
    notifications: Counter[str] = field(default_factory=Counter)
    loop_lag_ms: list[float] = field(default_factory=list)

    def clear(self) -> None:
        self.sql_calls = self.fetched_rows = 0
        self.sql_ms = 0
        self.notifications.clear()
        self.loop_lag_ms.clear()


METRICS = Metrics()


class MeasuredCursor(psycopg.AsyncCursor):
    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        METRICS.sql_calls += 1
        try:
            return await super().execute(*args, **kwargs)
        finally:
            METRICS.sql_ms += (time.perf_counter() - started) * 1_000

    async def executemany(self, query: Any, params_seq: Any, **kwargs: Any) -> Any:
        params = list(params_seq)
        started = time.perf_counter()
        METRICS.sql_calls += len(params)
        try:
            return await super().executemany(query, params, **kwargs)
        finally:
            METRICS.sql_ms += (time.perf_counter() - started) * 1_000

    async def fetchone(self) -> Any:
        result = await super().fetchone()
        METRICS.fetched_rows += int(result is not None)
        return result

    async def fetchall(self) -> Any:
        result = await super().fetchall()
        METRICS.fetched_rows += len(result)
        return result


class MeasuredConnection(psycopg.AsyncConnection):
    @classmethod
    async def connect(cls, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("cursor_factory", MeasuredCursor)
        return await super().connect(*args, **kwargs)

    async def notifies(self, **kwargs: Any) -> AsyncIterator[Any]:
        async for notice in super().notifies(**kwargs):
            try:
                kind = json.loads(notice.payload).get("kind", "unknown")
            except ValueError:
                kind = "unknown"
            METRICS.notifications[kind] += 1
            yield notice


def session_id(index: int) -> UUID:
    return uuid5(NAMESPACE_URL, f"tantra-bench-v1/session/{index}")


async def seed(
    dsn: str, schema: str, count: int, histories: list[int], model: str, *, compacted: bool = False
) -> list[str]:
    store = PostgresStore(dsn, schema=schema)
    await store.setup()
    ids = [session_id(index).hex for index in range(count)]
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await conn.execute(
                sql.SQL("CREATE TABLE {}.fixture (amount integer NOT NULL)").format(sql.Identifier(schema))
            )
            await conn.execute(sql.SQL("INSERT INTO {}.fixture VALUES (10), (20), (30)").format(sql.Identifier(schema)))
        for index, sid in enumerate(ids):
            await store.create(SessionHeader(id=sid, root_id=sid, agent=agent_name(BenchAgent), model=model))
            await store.append(sid, [SessionCreated(agent=agent_name(BenchAgent), root_id=sid, model=model)])
            if index < len(histories):
                await seed_history(store, sid, histories[index], model)
                if compacted:
                    await store.append(
                        sid,
                        [
                            CompactionApplied(
                                strategy="bench_fixture",
                                tokens_before=1,
                                tokens_after=1,
                                summary="Historical fixture total: 60. Use fixture_total for fresh evidence.",
                            )
                        ],
                    )
        return ids
    finally:
        await store.close()


async def seed_history(store: PostgresStore, sid: str, size: int, model: str) -> None:
    if size < 15:
        raise ValueError("history size must be at least 15")
    cid, sample_id, call_id = (uuid5(UUID(hex=sid), key).hex for key in ("seed-turn", "seed-sample", "seed-call"))
    await store.append(
        sid,
        [
            InputQueued(command_id=cid, input="seed"),
            TurnStarted(turn_id=cid, input="seed"),
            SampleStarted(turn_id=cid, sample_id=sample_id, model=model),
        ],
    )
    delta_count = size - 11
    tool_fragments = delta_count // 10 * 2 + sum(7 <= slot < 9 for slot in range(delta_count % 10))
    counts: Counter[str] = Counter()
    batch = []
    for index in range(delta_count):
        slot = index % 10
        if slot < 7:
            event = ReasoningDelta(text="r")
        elif slot < 9:
            position = counts["tool_call_delta"]
            fragment = "{" if position == 0 else "}" if position == tool_fragments - 1 else " "
            event = ToolCallDelta(
                index=0,
                id=call_id if position == 0 else None,
                name="fixture_total" if position == 0 else None,
                args_fragment=fragment,
            )
        else:
            event = TextDelta(text="x")
        counts[event.type] += 1
        batch.append(event)
        if len(batch) == 1_000:
            await store.append(sid, batch)
            batch.clear()
    if batch:
        await store.append(sid, batch)
    await store.append(
        sid,
        [
            ReasoningPart(sample_id=sample_id, text="r" * counts["reasoning_delta"]),
            TextPart(sample_id=sample_id, text="x" * counts["text_delta"]),
            ToolCallRequested(sample_id=sample_id, call_id=call_id, name="fixture_total", args={}),
            ToolCallStarted(call_id=call_id),
            ToolCallCompleted(call_id=call_id, result=60),
            SampleCompleted(sample_id=sample_id),
            TurnCompleted(turn_id=cid, stop_reason="stop"),
        ],
    )
    header = await store.header(sid)
    assert header is not None and header.last_seq == size
    await check_log(store, sid)


class GatedProvider:
    def __init__(self, settings: dict[str, Any]) -> None:
        self.gate = asyncio.Event()
        self.gate.set()
        if settings["mode"] == "live":
            self.source = live_provider(settings)
        elif settings["mode"] == "replay":
            self.source = RecordedProvider(
                None,
                Path(settings["recordings"]),
                settings["endpoint"],
                ModelLimits(context_window=settings["context_window"], max_output=4_096),
            )
        else:
            self.source = SyntheticProvider(call_policy("fixture_total", {}, answer="60"))
        self.requests: list[SampleRequest] = []
        self.request_times: list[float] = []

    def limits(self, model: str) -> ModelLimits:
        return self.source.limits(model)

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.requests.append(req)
        self.request_times.append(time.perf_counter())
        await self.gate.wait()
        async for event in self.source.stream(req):
            yield event

    async def aclose(self) -> None:
        closer = getattr(self.source, "aclose", None)
        if closer is not None:
            await closer()


def rss_mb() -> float:
    resident = int(Path("/proc/self/statm").read_text().split()[1])
    return resident * os.sysconf("SC_PAGE_SIZE") / 1_048_576


def pool_stats(runtime: Runtime) -> dict[str, int | float]:
    return dict(runtime.store._pool.get_stats())


def coordinator_stats(runtime: Runtime) -> dict[str, int]:
    coordinator = runtime.coordinator
    return {
        "observation_checks": coordinator.observation_checks,
        "observation_ticks": coordinator.observation_ticks,
        "routed_wakeups": coordinator.routed_wakeups,
        "dispatch_peak": coordinator.dispatch_peak,
        "busy_roots": len(coordinator._dispatchers),
        "subscriptions": len(coordinator._observations),
    }


def metric_delta(after: dict[str, int | float], before: dict[str, int | float]) -> dict[str, int | float]:
    return {key: value - before.get(key, 0) for key, value in after.items()}


async def loop_probe() -> None:
    while True:
        started = time.perf_counter()
        await asyncio.sleep(0.01)
        METRICS.loop_lag_ms.append(max((time.perf_counter() - started - 0.01) * 1_000, 0))


class WorkerState:
    def __init__(self, runtime: Runtime, provider: GatedProvider) -> None:
        self.runtime = runtime
        self.provider = provider
        self.connections: dict[str, Any] = {}
        self.observers: list[asyncio.Task[None]] = []
        self.observed: Counter[str] = Counter()
        self.observer_errors: list[str] = []

    async def operation(self, request: dict[str, Any]) -> dict[str, Any]:
        op = request["op"]
        sid = request.get("sid", "")
        if op == "claim":
            connection = self.runtime.connect(UUID(hex=sid), writable=True)
            await connection.__aenter__()
            self.connections[sid] = connection
            return {"writable": True}
        if op == "release":
            connection = self.connections.pop(sid)
            await connection.__aexit__()
            return {"released": True}
        if op == "gate":
            self.provider.gate.set() if request["open"] else self.provider.gate.clear()
            return {"open": request["open"], "provider_requests": len(self.provider.requests)}
        if op in ("send", "duplicate", "replaced"):
            try:
                receipt = await self.connections[sid].send(INPUT, command_id=UUID(hex=request["command"]))
            except (WriterReplaced, WriterRequired):
                if op != "replaced":
                    raise
                return {"writer_replaced": True}
            assert op != "replaced", "old writer was allowed to send"
            assert receipt.duplicate == (op == "duplicate"), "incorrect command deduplication"
            return asdict(receipt)
        if op == "wait_started":
            async with asyncio.timeout(60):
                while True:
                    status = await self.runtime.status(UUID(hex=sid))
                    if status.current_turn_id == UUID(hex=request["command"]) and len(
                        self.provider.requests
                    ) >= request.get("min_requests", 1):
                        return {"started": True, "provider_requests": len(self.provider.requests)}
                    await asyncio.sleep(0.01)
        if op == "complete":
            result = await self.connections[sid].prompt(INPUT, command_id=UUID(hex=request["command"]))
            assert result.outcome == "completed", f"turn failed: {result.outcome}: {result.error}"
            assert result.text.strip() == "60", f"incorrect result: {result.text!r}"
            return asdict(result)
        if op == "context" and self.runtime.history_mode == "compacted":
            snapshot = await self.runtime.store.read_compacted(sid)
            events = [item.event for item in snapshot.items]
            return {"events": len(events), "messages": len(build_messages(events)), "last_seq": snapshot.last_seq}
        if op == "context":
            cursor, events = 0, []
            while True:
                page = await self.runtime.store.read_page(sid, after=cursor)
                if not page:
                    break
                events.extend(item.event for item in page)
                cursor = page[-1].seq
            messages = build_messages(events)
            return {"events": len(events), "messages": len(messages), "last_seq": cursor}
        if op == "playback":
            header = await self.runtime.store.header(sid)
            assert header is not None
            stream = self.runtime.events(UUID(hex=sid))
            count, digest = 0, hashlib.sha256()
            try:
                async for item in stream:
                    count += 1
                    assert item.seq == count, f"playback gap at {item.seq}, expected {count}"
                    digest.update(item.event.model_dump_json().encode())
                    if item.seq == header.last_seq:
                        break
            finally:
                await stream.aclose()
            return {"events": count, "last_seq": count, "sha256": digest.hexdigest()}
        if op == "verify":
            await check_log(self.runtime.store, sid)
            events = [item.event async for item in self.runtime.store.read(sid)]
            terminal = [
                event
                for event in events
                if getattr(event, "turn_id", None) == request["command"]
                and event.type in ("turn_completed", "turn_interrupted", "turn_failed", "turn_cancelled")
            ]
            assert len(terminal) == 1 and terminal[0].type == request["terminal"], "incorrect terminal history"
            assert (
                sum(isinstance(event, InputQueued) and event.command_id == request["command"] for event in events) == 1
            )
            if request["terminal"] == "turn_completed":
                pairs_intact(events)
            if self.provider.requests:
                check_pairs(self.provider.requests, min_pairs=0)
            return {"terminal": terminal[0].type, "events": len(events)}
        if op == "observe":
            for root in request["ids"]:
                self.observers.append(asyncio.create_task(self.observe(root)))
            async with asyncio.timeout(120):
                while not set(request["ids"]).issubset(self.runtime._watchers):
                    await asyncio.sleep(0.01)
            return {"observers": len(self.observers)}
        if op == "idle":
            await asyncio.sleep(request["seconds"])
            return {"observed": dict(self.observed), "observer_errors": self.observer_errors}
        if op == "drive":

            async def turn(root: str) -> dict[str, Any]:
                async with self.runtime.connect(UUID(hex=root), writable=True) as connection:
                    result = await connection.prompt(INPUT, command_id=uuid5(UUID(hex=root), "scale-turn"))
                    assert result.outcome == "completed" and result.text.strip() == "60"
                status = await self.runtime.status(UUID(hex=root))
                return {"sid": root, "outcome": result.outcome, "last_seq": status.last_seq}

            return {"turns": await asyncio.gather(*(turn(root) for root in request["ids"]))}
        if op == "stop_observers":
            for task in self.observers:
                task.cancel()
            await asyncio.gather(*self.observers, return_exceptions=True)
            self.observers.clear()
            async with asyncio.timeout(30):
                while self.runtime._watchers or self.runtime.coordinator._observations:
                    await asyncio.sleep(0.01)
            return {
                "observer_errors": self.observer_errors,
                "observed": dict(self.observed),
                "watchers": len(self.runtime._watchers),
                "subscriptions": len(self.runtime.coordinator._observations),
            }
        if op == "backlog":
            conn = await psycopg.AsyncConnection.connect(self.runtime.store.dsn, autocommit=True)
            try:
                query = sql.SQL(
                    "SELECT (SELECT count(*) FROM {}.coordinator_changes "
                    "WHERE created_at < now() - interval '24 hours'), "
                    "(SELECT count(*) FROM {}.coordinator_requests "
                    "WHERE coalesce(completed_at, deadline) < now() - interval '24 hours')"
                ).format(sql.Identifier(self.runtime.store.schema), sql.Identifier(self.runtime.store.schema))
                row = await (await conn.execute(query)).fetchone()
                return {"expired_changes": row[0], "expired_requests": row[1]}
            finally:
                await conn.close()
        raise ValueError(f"unknown worker operation: {op}")

    async def observe(self, sid: str) -> None:
        try:
            status = await self.runtime.status(UUID(hex=sid))
            expected = status.last_seq + 1
            async with self.runtime.connect(UUID(hex=sid), after=status.last_seq) as connection:
                async for item in connection:
                    assert item.seq == expected, f"observer gap: {item.seq}, expected {expected}"
                    expected += 1
                    self.observed[sid] = item.seq
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.observer_errors.append(f"{type(exc).__name__}: {exc}")


async def serve(pipe: Any, settings: dict[str, Any]) -> None:
    psycopg.AsyncConnection = MeasuredConnection
    store = PostgresStore(settings["dsn"], schema=settings["schema"])
    provider = GatedProvider(settings)
    slots = asyncio.Semaphore(4)
    runtime = Runtime(
        provider,
        store,
        [BenchAgent],
        default_model=settings["model"],
        deps_factory=lambda _: (settings["dsn"], settings["schema"], slots),
        coordinator=PostgresCoordinator(store, **COORDINATOR_SETTINGS),
        history_mode=settings.get("history_mode", "full"),
    )
    state = WorkerState(runtime, provider)
    probe = asyncio.create_task(loop_probe())
    try:
        await runtime.start()
        pipe.send({"ready": True, "pid": os.getpid()})
        while True:
            request = await asyncio.to_thread(pipe.recv)
            if request["op"] == "close":
                break
            METRICS.clear()
            pool_before = pool_stats(runtime)
            coordinator_before = coordinator_stats(runtime)
            started, cpu = time.perf_counter(), time.process_time()
            try:
                async with asyncio.timeout(180):
                    result = await state.operation(request)
                error = None
            except Exception as exc:
                result, error = {}, f"{type(exc).__name__}: {exc}"
            pool_after = pool_stats(runtime)
            coordinator_after = coordinator_stats(runtime)
            reply = {
                "operation": request["op"],
                "result": result,
                "error": error,
                "elapsed_ms": (time.perf_counter() - started) * 1_000,
                "cpu_ms": (time.process_time() - cpu) * 1_000,
                "rss_mb": rss_mb(),
                "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1_024,
                "sql_calls": METRICS.sql_calls,
                "fetched_rows": METRICS.fetched_rows,
                "sql_ms": METRICS.sql_ms,
                "notifications": dict(METRICS.notifications),
                "loop_lag_ms": list(METRICS.loop_lag_ms),
                "watchers": len(runtime._watchers),
                "known_roots": len(runtime._known_roots),
                "pool": pool_after,
                "pool_delta": metric_delta(pool_after, pool_before),
                "coordinator": coordinator_after,
                "coordinator_delta": metric_delta(coordinator_after, coordinator_before),
            }
            pipe.send(reply)
    finally:
        await state.operation({"op": "stop_observers"})
        await runtime.aclose()
        await provider.aclose()
        await store.close()
        probe.cancel()
        await asyncio.gather(probe, return_exceptions=True)
        pipe.close()


def worker_main(pipe: Any, settings: dict[str, Any]) -> None:
    try:
        asyncio.run(serve(pipe, settings))
    except BaseException as exc:
        try:
            pipe.send({"error": f"{type(exc).__name__}: {exc}"})
        except (BrokenPipeError, OSError):
            pass
        finally:
            pipe.close()
        raise SystemExit(1) from exc
