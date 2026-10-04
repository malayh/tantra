from __future__ import annotations

import asyncio
import hashlib
import json
import resource
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

import psycopg

from tantra import Agent, Context, ModelLimits, Runtime, Usage, tool
from tantra.agent import agent_name
from tantra.events import (
    CompactionApplied,
    ReasoningDelta,
    SessionCreated,
    SessionHeader,
    TextDelta,
    ToolCallDelta,
    ToolProgress,
    TurnCompleted,
)
from tantra.providers.base import ProviderEvent, ReasoningBlock, SampleRequest, StreamEnd, ToolCall

CPU_MODEL = "cpu/synthetic"
CPU_INPUT = "CPU fixture"
DELTA_TYPES = {"text_delta", "reasoning_delta", "tool_call_delta", "tool_progress"}
SAMPLE_ID_TYPES = {"sample_started", "text_part", "reasoning_part", "tool_call_requested", "sample_completed"}
_CPU_PROVIDERS: dict[str, CPUProvider] = {}


def fixture_payload(fragments: int) -> str:
    records = 8_192 if fragments == 8_192 else 1_024
    if fragments not in (64, 256, 1_024, 8_192):
        raise ValueError("fragments must be one of 64, 256, 1024, or 8192")
    return "".join(f"{index:016x}" for index in range(records))


def split_payload(payload: str, fragments: int) -> list[str]:
    if fragments <= 0 or fragments > len(payload):
        raise ValueError("fragments must be between 1 and the payload length")
    size, extra = divmod(len(payload), fragments)
    parts: list[str] = []
    offset = 0
    for index in range(fragments):
        width = size + (index < extra)
        parts.append(payload[offset : offset + width])
        offset += width
    if len(set(parts)) != len(parts) or "".join(parts) != payload:
        raise AssertionError("fixture fragments must be unique, ordered, and lossless")
    return parts


def _plain(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return _plain(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_plain(item) for item in value]
    if isinstance(value, UUID):
        return value.hex
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def normalized_digest(items: Sequence[Any]) -> str:
    plain = _plain(items)
    sample_ids: dict[str, str] = {}
    for item in plain:
        event = item.get("event", item) if isinstance(item, dict) else None
        if not isinstance(event, dict) or event.get("type") not in SAMPLE_ID_TYPES or "sample_id" not in event:
            continue
        sample_id = event["sample_id"]
        sample_ids.setdefault(sample_id, f"sample-{len(sample_ids)}")
        event["sample_id"] = sample_ids[sample_id]
    encoded = json.dumps(plain, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def assert_ordered_prefix(actual: Sequence[Any], expected: Sequence[Any]) -> None:
    left, right = _plain(actual), _plain(expected)
    if len(left) < len(right):
        raise AssertionError(f"ordered prefix ended at {len(left)}, expected {len(right)} items")
    for index, wanted in enumerate(right):
        if left[index] != wanted:
            raise AssertionError(f"ordered prefix differs at {index}: {left[index]!r} != {wanted!r}")


def assert_committed(observed_seq: int, committed_seq: int) -> None:
    if observed_seq > committed_seq:
        raise AssertionError("event delivered before independent database commit evidence")


@tool(description="Return the deterministic CPU fixture payload.")
async def cpu_echo(payload: str) -> str:
    return payload


@tool(description="Emit deterministic CPU fixture progress.")
async def cpu_progress(ctx: Context) -> str:
    provider = _CPU_PROVIDERS[ctx.session_id]
    for event in provider.progress_events:
        if provider.pause_s:
            await asyncio.sleep(provider.pause_s)
        provider.record(event)
        await ctx.emit(event.message)
    return "ok"


class CPUAgent(Agent):
    prompt = "Run the CPU fixture exactly as requested."
    tools = [cpu_echo, cpu_progress]
    max_steps = 3


class CPUProvider:
    def __init__(self) -> None:
        self.config: dict[str, Any] = {}
        self.payload = ""
        self.parts: list[str] = []
        self.pause_s = 0.0
        self.expected: list[Any] = []
        self.progress_events: list[ToolProgress] = []
        self.emissions: list[dict[str, Any]] = []
        self.requests: list[SampleRequest] = []
        self.configured_at = 0.0
        self._samples = 0

    def configure(self, config: Mapping[str, Any]) -> None:
        fragments = int(config["fragments"])
        kind = str(config["kind"])
        if kind not in ("text", "reasoning", "tool", "progress"):
            raise ValueError(f"unknown CPU fixture kind: {kind}")
        self.config = dict(config)
        self.payload = fixture_payload(fragments)
        self.parts = split_payload(self.payload, fragments)
        self.pause_s = float(config.get("pause_s", 0))
        self.expected = []
        self.progress_events = []
        if kind == "text":
            self.expected.extend(TextDelta(text=part) for part in self.parts)
        elif kind == "reasoning":
            self.expected.extend(ReasoningDelta(text=part) for part in self.parts)
        elif kind == "tool":
            args = json.dumps({"payload": self.payload}, separators=(",", ":"))
            for index, part in enumerate(split_payload(args, fragments)):
                self.expected.append(
                    ToolCallDelta(
                        index=0,
                        id="cpu-call" if index == 0 else None,
                        name="cpu_echo" if index == 0 else None,
                        args_fragment=part,
                    )
                )
            self.expected.append(TextDelta(text="ok"))
        else:
            self.expected.append(ToolCallDelta(index=0, id="cpu-call", name="cpu_progress", args_fragment="{}"))
            self.progress_events = [ToolProgress(call_id="cpu-call", message=part) for part in self.parts]
            self.expected.extend(self.progress_events)
            self.expected.append(TextDelta(text="ok"))
        self.emissions.clear()
        self.requests.clear()
        self.configured_at = time.perf_counter()
        self._samples = 0

    def limits(self, model: str) -> ModelLimits:
        return ModelLimits(context_window=1_000_000, max_output=200_000)

    def record(self, event: Any) -> None:
        emitted = time.perf_counter()
        self.emissions.append(
            {
                "index": len(self.emissions),
                "at_ms": (emitted - self.configured_at) * 1_000,
                "emitted_at_ms": emitted * 1_000,
                "event": _plain(event),
            }
        )

    async def _emit(self, event: ProviderEvent) -> AsyncIterator[ProviderEvent]:
        if self.pause_s:
            await asyncio.sleep(self.pause_s)
        self.record(event)
        yield event

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if not self.config:
            raise AssertionError("CPU provider was not configured")
        self.requests.append(req)
        sample = self._samples
        self._samples += 1
        kind = self.config["kind"]
        if kind == "text":
            if sample:
                raise AssertionError("text fixture sampled more than once")
            for event in self.expected:
                async for emitted in self._emit(event):
                    yield emitted
            yield StreamEnd(
                text=self.payload,
                usage=Usage(output_tokens=len(self.payload) // 4),
                finish_reason="stop",
            )
            return
        if kind == "reasoning":
            if sample:
                raise AssertionError("reasoning fixture sampled more than once")
            for event in self.expected:
                async for emitted in self._emit(event):
                    yield emitted
            yield StreamEnd(
                reasoning=[ReasoningBlock(text=self.payload)],
                usage=Usage(output_tokens=len(self.payload) // 4),
                finish_reason="stop",
            )
            return
        if sample == 0:
            deltas = self.expected[:-1]
            if kind == "progress":
                deltas = deltas[:1]
                args = "{}"
                name = "cpu_progress"
            else:
                args = json.dumps({"payload": self.payload}, separators=(",", ":"))
                name = "cpu_echo"
            for event in deltas:
                async for emitted in self._emit(event):
                    yield emitted
            call = ToolCall(id="cpu-call", name=name, args=args)
            yield call
            yield StreamEnd(tool_calls=[call], finish_reason="tool_calls")
            return
        if sample != 1:
            raise AssertionError(f"{kind} fixture sampled more than twice")
        final = self.expected[-1]
        async for emitted in self._emit(final):
            yield emitted
        yield StreamEnd(text="ok", usage=Usage(output_tokens=1), finish_reason="stop")

    async def aclose(self) -> None:
        return None


class _SeedStore:
    def __init__(self, runtime: Runtime, sid: str, ownership: Any) -> None:
        self.runtime = runtime
        self.sid = sid
        self.ownership = ownership

    def __getattr__(self, name: str) -> Any:
        return getattr(self.runtime.store, name)

    async def append(self, sid: str, events: Sequence[Any]) -> int:
        if sid != self.sid or self.runtime.coordinator is None:
            raise AssertionError("CPU history append lost its coordinated root")
        self.ownership = await self.runtime.coordinator.renew(self.ownership)
        async with self.runtime.coordinator.transaction(self.ownership) as store:
            return await store.append(sid, events)


@dataclass
class _Reader:
    task: asyncio.Task[dict[str, Any]]
    after: int


class CPUState:
    def __init__(self, runtime: Runtime, gated_provider: Any) -> None:
        self.runtime = runtime
        self.gated_provider = gated_provider
        self.source = getattr(gated_provider, "source", None)
        self.connections: dict[str, Any] = {}
        self.prompts: dict[str, asyncio.Task[Any]] = {}
        self.readers: dict[str, list[_Reader]] = {}
        self.reader_results: dict[str, list[dict[str, Any]]] = {}
        self.configs: dict[str, dict[str, Any]] = {}
        self.afters: dict[str, int] = {}
        self.measuring = False
        self.baseline: dict[str, Any] | None = None

    async def operation(self, request: dict[str, Any]) -> dict[str, Any] | None:
        op = request["op"]
        if not op.startswith("cpu_"):
            return None
        if not isinstance(self.source, CPUProvider):
            raise AssertionError("CPU operations require worker mode=cpu")
        sid = request.get("sid", "")
        if op == "cpu_prepare":
            return await self._prepare(sid, request["config"])
        if op == "cpu_observers":
            return await self._observers(
                sid,
                int(request["after"]),
                request["config"],
                int(request.get("count", request["config"].get("readers", 0))),
            )
        if op == "cpu_begin":
            return self._begin()
        if op == "cpu_start":
            command = UUID(hex=request.get("command", uuid5(UUID(hex=sid), "cpu-turn").hex))
            task = asyncio.create_task(self.connections[sid].prompt(CPU_INPUT, command_id=command))
            self.prompts[sid] = task
            await asyncio.sleep(0)
            return {"started": True, "command": command.hex}
        if op == "cpu_gate":
            self.gated_provider.gate.set() if request["open"] else self.gated_provider.gate.clear()
            return {"open": bool(request["open"])}
        if op == "cpu_wait":
            return await self._wait(sid)
        if op == "cpu_reader_wait":
            return await self._reader_wait(sid)
        if op == "cpu_end":
            return self._end()
        if op == "cpu_release":
            return await self._release(sid)
        raise ValueError(f"unknown CPU worker operation: {op}")

    async def _prepare(self, sid: str, raw_config: Mapping[str, Any]) -> dict[str, Any]:
        if sid in self.connections:
            raise AssertionError(f"CPU session {sid} is already prepared")
        config = {
            "fragments": int(raw_config["fragments"]),
            "kind": str(raw_config["kind"]),
            "pause_s": float(raw_config.get("pause_s", 0)),
            "readers": int(raw_config.get("readers", 0)),
            "slow_s": float(raw_config.get("slow_s", 0)),
            "history": int(raw_config.get("history", 0)),
            "history_mode": str(raw_config.get("history_mode", "full")),
            "children": int(raw_config.get("children", 0)),
            "audit_all": bool(raw_config.get("audit_all", False)),
        }
        if config["children"] not in (0, 100):
            raise ValueError("children must be 0 or 100")
        if config["history"] and config["history"] < 15:
            raise ValueError("history must be zero or at least 15")
        if config["history_mode"] not in ("full", "compacted"):
            raise ValueError("history_mode must be full or compacted")
        self.runtime.history_mode = config["history_mode"]
        public_id = UUID(hex=sid)
        await self.runtime.create(CPUAgent, session_id=public_id, model=CPU_MODEL)
        ownership = await self.runtime.coordinator.acquire(sid)
        if ownership is None:
            raise AssertionError("CPU fixture root has another owner")
        seed_store = _SeedStore(self.runtime, sid, ownership)
        if config["history"]:
            from stress.bench.worker import seed_history

            await seed_history(seed_store, sid, config["history"], CPU_MODEL)
            if config["history_mode"] == "compacted":
                await seed_store.append(
                    sid,
                    [
                        CompactionApplied(
                            strategy="cpu_fixture",
                            tokens_before=1,
                            tokens_after=1,
                            summary="CPU fixture history.",
                        )
                    ],
                )
        for index in range(config["children"]):
            child = uuid5(public_id, f"cpu-child/{index}").hex
            header = SessionHeader(
                id=child,
                root_id=sid,
                parent_id=sid,
                agent=agent_name(CPUAgent),
                model=CPU_MODEL,
                depth=1,
            )
            seed_store.ownership = await self.runtime.coordinator.renew(seed_store.ownership)
            async with self.runtime.coordinator.transaction(seed_store.ownership) as store:
                await store.create(header)
                await store.append(
                    child,
                    [
                        SessionCreated(
                            agent=agent_name(CPUAgent),
                            root_id=sid,
                            parent_id=sid,
                            depth=1,
                            model=CPU_MODEL,
                        )
                    ],
                )
        header = await self.runtime.store.header(sid)
        if header is None:
            raise AssertionError("CPU session was not created")
        await self.runtime.coordinator.release(seed_store.ownership)
        self.source.configure(config)
        _CPU_PROVIDERS[sid] = self.source
        self.configs[sid] = config
        self.afters[sid] = header.last_seq
        connection = self.runtime.connect(public_id, writable=True)
        await connection.__aenter__()
        self.connections[sid] = connection
        self.gated_provider.gate.clear()
        return {
            "sid": sid,
            "after": header.last_seq,
            "history": config["history"],
            "children": config["children"],
            "expected_delta_count": len(self.source.expected),
            "expected_delta_digest": normalized_digest(self.source.expected),
        }

    async def _observers(self, sid: str, after: int, config: Mapping[str, Any], count: int) -> dict[str, Any]:
        if sid in self.readers:
            raise AssertionError(f"CPU readers for {sid} already exist")
        if sid not in self.configs:
            self.source.configure(config)
            self.configs[sid] = dict(config)
        tasks = [_Reader(asyncio.create_task(self._read(sid, after, config)), after) for _ in range(count)]
        self.readers[sid] = tasks
        async with asyncio.timeout(30):
            while self.runtime._stream_interests.get(sid, {}).get(sid, 0) < count:
                if any(reader.task.done() for reader in tasks):
                    await asyncio.gather(*(reader.task for reader in tasks))
                await asyncio.sleep(0)
        return {"ready": True, "count": count, "after": after}

    async def _read(self, sid: str, after: int, config: Mapping[str, Any]) -> dict[str, Any]:
        stream = self.runtime.events(UUID(hex=sid), after=after)
        replay: list[dict[str, Any]] = []
        deltas: list[dict[str, Any]] = []
        observed_times: list[float] = []
        latencies: list[float] = []
        probes: list[dict[str, Any]] = []
        expected = [_plain(event) for event in self.source.expected]
        targets = set(range(len(expected))) if config.get("audit_all") else {0, len(expected) // 2, len(expected) - 1}
        started = time.perf_counter()
        try:
            async for item in stream:
                now = time.perf_counter()
                event = _plain(item.event)
                replay.append({"seq": item.seq, "event": event})
                if item.seq != after + len(replay):
                    raise AssertionError(f"CPU reader gap at {item.seq}, expected {after + len(replay)}")
                if event["type"] in DELTA_TYPES:
                    index = len(deltas)
                    deltas.append(event)
                    observed_times.append(now)
                    if index < len(self.source.emissions):
                        emitted = self.source.configured_at + self.source.emissions[index]["at_ms"] / 1_000
                        latencies.append((now - emitted) * 1_000)
                    if index in targets:
                        probes.append(await self._probe(sid, index, item.seq))
                if isinstance(item.event, TurnCompleted):
                    break
                if config.get("slow_s"):
                    await asyncio.sleep(float(config["slow_s"]))
        finally:
            await stream.aclose()
        assert_ordered_prefix(deltas, expected)
        if len(deltas) != len(expected):
            raise AssertionError(f"CPU reader saw {len(deltas)} deltas, expected {len(expected)}")
        inter = [(right - left) * 1_000 for left, right in zip(observed_times, observed_times[1:], strict=False)]
        return {
            "count": len(replay),
            "first_seq": replay[0]["seq"],
            "last_seq": replay[-1]["seq"],
            "first_ms": (observed_times[0] - started) * 1_000,
            "inter_ms": _timing(inter),
            "delivery_ms": _timing(latencies),
            "received_at_ms": [value * 1_000 for value in observed_times],
            "replay_digest": normalized_digest(replay),
            "delta_count": len(deltas),
            "delta_digest": normalized_digest(deltas),
            "probes": probes,
        }

    async def _probe(self, sid: str, index: int, seq: int) -> dict[str, Any]:
        started = time.perf_counter()
        async with await psycopg.AsyncConnection.connect(self.runtime.store.dsn, autocommit=True) as conn:
            row = await (
                await conn.execute(
                    self.runtime.store._sql("SELECT last_seq FROM {schema}.sessions WHERE id = %s"),
                    (sid,),
                )
            ).fetchone()
        if row is None:
            raise AssertionError("CPU reader session disappeared during commit verification")
        assert_committed(seq, row[0])
        return {
            "delta_index": index,
            "seq": seq,
            "stored_last_seq": int(row[0]),
            "committed": True,
            "snapshot_ms": (time.perf_counter() - started) * 1_000,
        }

    def _begin(self) -> dict[str, Any]:
        if self.measuring:
            raise AssertionError("CPU measurement is already active")
        from stress.bench.worker import METRICS, coordinator_stats, pool_stats, rss_mb

        self.measuring = True
        self.baseline = {
            "wall": time.perf_counter(),
            "cpu": time.process_time(),
            "rss": rss_mb(),
            "metrics": _metrics(METRICS),
            "pool": pool_stats(self.runtime),
            "coordinator": coordinator_stats(self.runtime),
        }
        return {"measuring": True}

    def _end(self) -> dict[str, Any]:
        if not self.measuring or self.baseline is None:
            raise AssertionError("CPU measurement is not active")
        from stress.bench.worker import METRICS, coordinator_stats, metric_delta, pool_stats, rss_mb

        baseline = self.baseline
        current = _metrics(METRICS)
        pool = pool_stats(self.runtime)
        coordinator = coordinator_stats(self.runtime)
        self.measuring = False
        self.baseline = None
        metrics = _metric_window(current, baseline["metrics"])
        return {
            "measuring": False,
            "elapsed_ms": (time.perf_counter() - baseline["wall"]) * 1_000,
            "cpu_ms": (time.process_time() - baseline["cpu"]) * 1_000,
            "rss_mb": rss_mb(),
            "rss_delta_mb": rss_mb() - baseline["rss"],
            "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1_024,
            **metrics,
            "pool": pool,
            "pool_delta": metric_delta(pool, baseline["pool"]),
            "coordinator": coordinator,
            "coordinator_delta": metric_delta(coordinator, baseline["coordinator"]),
            "watchers": len(self.runtime._watchers),
            "subscriptions": len(self.runtime.coordinator._observations),
        }

    async def _wait(self, sid: str) -> dict[str, Any]:
        result = await self.prompts[sid]
        async with asyncio.timeout(30):
            while sid in self.runtime.active:
                await asyncio.sleep(0.005)
        expected_text = self.source.payload if self.configs[sid]["kind"] == "text" else ""
        if self.configs[sid]["kind"] in ("tool", "progress"):
            expected_text = "ok"
        if result.outcome != "completed" or result.text != expected_text:
            raise AssertionError(f"CPU turn result was {result.outcome} {result.text!r}, expected {expected_text!r}")
        actual = [item["event"] for item in self.source.emissions]
        expected = [_plain(event) for event in self.source.expected]
        assert_ordered_prefix(actual, expected)
        if len(actual) != len(expected):
            raise AssertionError(f"CPU provider emitted {len(actual)} deltas, expected {len(expected)}")
        from stress.invariants import check_pairs, pairs_intact

        tool_fixture = self.configs[sid]["kind"] in ("tool", "progress")
        check_pairs(self.source.requests, min_pairs=int(tool_fixture))
        if tool_fixture:
            events = [item.event async for item in self.runtime.store.read(sid, from_seq=self.afters[sid])]
            pairs_intact(events)
        return {
            "outcome": result.outcome,
            "text": result.text,
            "emissions": list(self.source.emissions),
            "expected_deltas": expected,
            "expected_delta_count": len(expected),
            "expected_delta_digest": normalized_digest(expected),
            "provider_requests": len(self.source.requests),
        }

    async def _reader_wait(self, sid: str) -> dict[str, Any]:
        if sid not in self.reader_results:
            self.reader_results[sid] = await asyncio.gather(*(reader.task for reader in self.readers.get(sid, [])))
        return {
            "readers": self.reader_results[sid],
            "expected_delta_count": len(self.source.expected),
            "expected_delta_digest": normalized_digest(self.source.expected),
        }

    async def _release(self, sid: str) -> dict[str, Any]:
        prompt = self.prompts.pop(sid, None)
        readers = self.readers.pop(sid, [])
        tasks = [*(reader.task for reader in readers), *([prompt] if prompt is not None else [])]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        connection = self.connections.pop(sid, None)
        if connection is not None:
            await connection.__aexit__(None, None, None)
        self.reader_results.pop(sid, None)
        self.configs.pop(sid, None)
        self.afters.pop(sid, None)
        _CPU_PROVIDERS.pop(sid, None)
        async with asyncio.timeout(30):
            while sid in self.runtime._watchers or sid in self.runtime.coordinator._observations:
                await asyncio.sleep(0.01)
        if sid in self.runtime._watchers or sid in self.runtime.coordinator._observations:
            raise AssertionError("CPU observer resources were retained")
        return {
            "released": True,
            "watchers": len(self.runtime._watchers),
            "subscriptions": len(self.runtime.coordinator._observations),
        }

    async def close(self) -> None:
        sids = set(self.connections) | set(self.prompts) | set(self.readers) | set(self.configs)
        for sid in sids:
            with suppress(Exception):
                await self._release(sid)
        self.measuring = False
        self.baseline = None


def _timing(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "p50": ordered[(len(ordered) - 1) // 2],
        "p95": ordered[int((len(ordered) - 1) * 0.95)],
        "max": ordered[-1],
    }


def _metrics(metrics: Any) -> dict[str, Any]:
    value = {field.name: getattr(metrics, field.name) for field in fields(metrics)}
    value["notifications"] = dict(value["notifications"])
    value["loop_lag_ms"] = list(value["loop_lag_ms"])
    return value


def _metric_window(after: Mapping[str, Any], before: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in after.items():
        if key == "notifications":
            names = set(value) | set(before[key])
            result[key] = {name: value.get(name, 0) - before[key].get(name, 0) for name in names}
        elif key == "loop_lag_ms":
            result[key] = value[len(before[key]) :]
        else:
            result[key] = value - before[key]
    return result
