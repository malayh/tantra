from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from collections.abc import AsyncIterator, Callable
from contextvars import ContextVar
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from pydantic import TypeAdapter

from tantra import ModelLimits, OpenAICompatible, ProviderError, SampleRequest
from tantra.providers.base import Provider, ProviderEvent

TOKEN_LIMIT = 5_000_000
OUTPUT_LIMIT = 4_096
EVENT_ADAPTER = TypeAdapter(ProviderEvent)
FIXTURE_VERSION = 1
RECORDING_KEY: ContextVar[str | None] = ContextVar("bench_recording_key", default=None)
RECORDING_ATTEMPTS: ContextVar[list[str] | None] = ContextVar("bench_recording_attempts", default=None)


class BudgetExceeded(ProviderError):
    pass


class Budget:
    def __init__(self, path: Path, campaign: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.campaign = campaign
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS requests ("
                "id TEXT PRIMARY KEY, campaign TEXT NOT NULL, reserved INTEGER NOT NULL, "
                "tokens INTEGER, generation TEXT, usage TEXT, created REAL NOT NULL, recording_key TEXT)"
            )
            if "recording_key" not in {row[1] for row in conn.execute("PRAGMA table_info(requests)")}:
                conn.execute("ALTER TABLE requests ADD COLUMN recording_key TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS campaign_idx ON requests(campaign)")

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30)

    def reserve(self, capacity: int, recording_key: str | None = None) -> str:
        if capacity < 1:
            raise ValueError("reservation must be positive")
        request_id = uuid4().hex
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            used, invalid = conn.execute(
                "SELECT coalesce(sum(coalesce(tokens, reserved)), 0), "
                "coalesce(sum(tokens > reserved), 0) FROM requests WHERE campaign = ?",
                (self.campaign,),
            ).fetchone()
            if invalid or used + capacity > TOKEN_LIMIT:
                raise BudgetExceeded(f"campaign cannot reserve {capacity} tokens; available: {TOKEN_LIMIT - used}")
            conn.execute(
                "INSERT INTO requests(id, campaign, reserved, created, recording_key) VALUES (?, ?, ?, ?, ?)",
                (request_id, self.campaign, capacity, time.time(), recording_key),
            )
        return request_id

    def generation(self, request_id: str, generation: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE requests SET generation = ? WHERE id = ? AND campaign = ?",
                (generation, request_id, self.campaign),
            )

    def settle(self, request_id: str, usage: dict[str, Any]) -> None:
        prompt, completion = usage.get("prompt_tokens"), usage.get("completion_tokens")
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in (prompt, completion)):
            raise ValueError("usage must contain nonnegative prompt and completion counts")
        tokens = prompt + completion
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT reserved, tokens FROM requests WHERE id = ? AND campaign = ?", (request_id, self.campaign)
            ).fetchone()
            if row is None:
                raise ValueError("unknown reservation")
            if row[1] is not None and row[1] != tokens:
                raise ValueError("provider changed settled usage")
            conn.execute(
                "UPDATE requests SET tokens = ?, usage = ? WHERE id = ?", (tokens, json.dumps(usage), request_id)
            )
        if tokens > row[0]:
            raise ValueError("usage exceeds verified reservation; campaign blocked")

    def summary(self) -> dict[str, Any]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT reserved, tokens, usage FROM requests WHERE campaign = ?", (self.campaign,)
            ).fetchall()
        usages = [json.loads(row[2]) for row in rows if row[2] is not None]
        charged = sum(tokens if tokens is not None else reserved for reserved, tokens, _ in rows)
        return {
            "campaign": self.campaign,
            "limit": TOKEN_LIMIT,
            "requests": len(rows),
            "reported_tokens": sum(tokens or 0 for _, tokens, _ in rows),
            "unknown_reserved_tokens": sum(reserved for reserved, tokens, _ in rows if tokens is None),
            "remaining_tokens": TOKEN_LIMIT - charged,
            "cached_input_tokens": sum((u.get("prompt_tokens_details") or {}).get("cached_tokens", 0) for u in usages),
            "cache_write_tokens": sum(
                (u.get("prompt_tokens_details") or {}).get("cache_write_tokens", 0) for u in usages
            ),
            "reasoning_tokens": sum(
                (u.get("completion_tokens_details") or {}).get("reasoning_tokens", 0) for u in usages
            ),
            "reported_cost": sum(u["cost"] for u in usages if type(u.get("cost")) in (int, float)),
            "cost_available": bool(rows)
            and len(usages) == len(rows)
            and all(type(u.get("cost")) in (int, float) for u in usages),
        }

    def reconcile(self, client: httpx.Client, endpoint: str, limit: int = 20) -> list[dict[str, str]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, generation FROM requests WHERE campaign = ? AND tokens IS NULL "
                "AND generation IS NOT NULL ORDER BY created, id LIMIT ?",
                (self.campaign, limit),
            ).fetchall()
        results = []
        for request_id, generation in rows:
            try:
                response = client.get(f"{endpoint}/generation", params={"id": generation})
                response.raise_for_status()
                data = response.json()["data"]
                prompt, completion = data.get("native_tokens_prompt"), data.get("native_tokens_completion")
                if data.get("id") != generation or not data.get("finish_reason"):
                    raise ValueError("generation is not authoritatively complete")
                if any(type(value) is not int or value < 0 for value in (prompt, completion)):
                    raise ValueError("generation has no complete native-token usage")
                usage = {
                    "prompt_tokens": prompt,
                    "completion_tokens": completion,
                    "prompt_tokens_details": {"cached_tokens": data.get("native_tokens_cached") or 0},
                    "completion_tokens_details": {"reasoning_tokens": data.get("native_tokens_reasoning") or 0},
                }
                if data.get("total_cost") is not None:
                    usage["cost"] = data["total_cost"]
                self.settle(request_id, usage)
                results.append({"request_id": request_id, "status": "settled"})
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                results.append({"request_id": request_id, "status": "retained", "error": str(exc)})
        return results

    def recording_requests(self, key: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, generation, reserved, tokens, usage FROM requests WHERE campaign = ? "
                "AND recording_key = ? ORDER BY created, id",
                (self.campaign, key),
            ).fetchall()
        return [
            {
                "id": identity,
                "generation": generation,
                "reserved": reserved,
                "tokens": tokens,
                "usage": json.loads(usage) if usage is not None else None,
            }
            for identity, generation, reserved, tokens, usage in rows
        ]


class MeteredStream(httpx.AsyncByteStream):
    def __init__(
        self, stream: httpx.AsyncByteStream, budget: Budget, request_id: str, release: Callable[[], None] | None = None
    ) -> None:
        self.stream = stream
        self.budget = budget
        self.request_id = request_id
        self.pending = b""
        self.generation_id: str | None = None
        self.usage: dict[str, Any] | None = None
        self.release = release

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.stream:
            self.pending += chunk
            while b"\n" in self.pending:
                line, self.pending = self.pending.split(b"\n", 1)
                self.inspect(line)
            if len(self.pending) > 2_000_000:
                raise ProviderError("provider SSE line exceeds bench limit")
            yield chunk
        if self.pending:
            self.inspect(self.pending)

    def inspect(self, line: bytes) -> None:
        if not line.startswith(b"data:"):
            return
        payload = line[5:].strip()
        if payload == b"[DONE]":
            if self.usage is not None:
                self.budget.settle(self.request_id, self.usage)
            return
        try:
            raw = json.loads(payload)
        except ValueError:
            return
        if not isinstance(raw, dict):
            return
        generation = raw.get("id")
        if isinstance(generation, str) and self.generation_id is None:
            self.generation_id = generation
            self.budget.generation(self.request_id, generation)
        usage = raw.get("usage")
        if isinstance(usage, dict) and usage.get("prompt_tokens") is not None:
            self.usage = usage

    async def aclose(self) -> None:
        try:
            await self.stream.aclose()
        finally:
            if self.release is not None:
                release, self.release = self.release, None
                release()


class MeteredTransport(httpx.AsyncBaseTransport):
    def __init__(self, budget: Budget, capacity: int, slots: Any = None) -> None:
        self.inner = httpx.AsyncHTTPTransport(retries=0)
        self.budget = budget
        self.capacity = capacity
        self.slots = slots

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        paid = request.url.path.endswith("/chat/completions")
        release = None
        if paid and self.slots is not None:
            while not self.slots.acquire(False):
                await asyncio.sleep(0.01)
            release = self.slots.release
        try:
            reservation = self.budget.reserve(self.capacity, RECORDING_KEY.get()) if paid else None
            attempts = RECORDING_ATTEMPTS.get()
            if reservation is not None and attempts is not None:
                attempts.append(reservation)
            response = await self.inner.handle_async_request(request)
            if reservation is not None:
                if response.headers.get("X-OpenRouter-Cache-Status", "").upper() == "HIT":
                    await response.aclose()
                    raise ProviderError("response-cache hit cannot count as fresh inference")
                response.stream = MeteredStream(response.stream, self.budget, reservation, release)
                release = None
            return response
        finally:
            if release is not None:
                release()

    async def aclose(self) -> None:
        await self.inner.aclose()


def prepared(req: SampleRequest) -> SampleRequest:
    params = {key: value for key, value in req.params.items() if key not in ("max_tokens", "max_completion_tokens")}
    return req.model_copy(update={"params": {**params, "max_tokens": OUTPUT_LIMIT, "temperature": 0}})


def request_key(req: SampleRequest, endpoint: str, fixture_identity: str = "") -> str:
    payload = {"fixture_version": FIXTURE_VERSION, "endpoint": endpoint, "request": prepared(req).model_dump()}
    if fixture_identity:
        payload["fixture_identity"] = fixture_identity
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


class RecordedProvider:
    def __init__(
        self,
        source: Provider | None,
        directory: Path,
        endpoint: str,
        limits: ModelLimits,
        fixture_identity: str = "",
        budget: Budget | None = None,
        require_usage: bool = False,
    ) -> None:
        self.source = source
        self.directory = directory
        self.endpoint = endpoint
        self.model_limits = limits
        self.requests: list[SampleRequest] = []
        self.fixture_identity = fixture_identity
        self.timings: list[dict[str, Any]] = []
        self.budget = budget
        self.require_usage = require_usage or budget is not None

    def validate_usage(self, rows: list[dict[str, Any]]) -> None:
        if not self.require_usage:
            return
        if not rows:
            raise ProviderError("recording has no authoritative usage linkage")
        for row in rows:
            usage = row.get("usage") or {}
            prompt, completion = usage.get("prompt_tokens"), usage.get("completion_tokens")
            if (
                not row.get("generation")
                or type(row.get("tokens")) is not int
                or any(type(value) is not int or value < 0 for value in (prompt, completion))
                or row["tokens"] != prompt + completion
            ):
                raise ProviderError("recording has incomplete authoritative usage")

    def limits(self, model: str) -> ModelLimits:
        return self.model_limits

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        req = prepared(req)
        self.requests.append(req)
        key = request_key(req, self.endpoint, self.fixture_identity)
        path = self.directory / f"{key}.json"
        if self.source is None:
            if not path.is_file():
                diagnostic = self.directory / "misses" / path.name
                diagnostic.parent.mkdir(parents=True, exist_ok=True)
                diagnostic.write_text(
                    json.dumps(
                        {
                            "request": req.model_dump(),
                            "endpoint": self.endpoint,
                            "fixture_identity": self.fixture_identity,
                        }
                    )
                )
                raise ProviderError(f"recording miss: {path.name}; replay cannot make paid requests")
            recording = json.loads(path.read_text())
            if recording["request"] != req.model_dump() or recording["endpoint"] != self.endpoint:
                raise ProviderError("recording identity mismatch")
            if recording.get("fixture_identity", "") != self.fixture_identity:
                raise ProviderError("recording fixture mismatch")
            if not recording["events"] or recording["events"][-1]["type"] != "stream_end":
                raise ProviderError("recording has no completed stream")
            self.validate_usage(recording.get("usage_requests", []))
            for item, delay in zip(recording["events"], recording["delays"], strict=True):
                await asyncio.sleep(delay)
                yield EVENT_ADAPTER.validate_python(item)
            self.timings.append({"request_key": key, "provider_ms": sum(recording["delays"]) * 1000, "fresh": False})
            return
        events, delays = [], []
        attempts: list[str] = []
        token = RECORDING_KEY.set(key)
        attempts_token = RECORDING_ATTEMPTS.set(attempts)
        iterator = self.source.stream(req)
        try:
            while True:
                started = time.perf_counter()
                try:
                    event = await anext(iterator)
                except StopAsyncIteration:
                    break
                events.append(event.model_dump())
                delays.append(time.perf_counter() - started)
                yield event
        finally:
            try:
                await iterator.aclose()
            finally:
                RECORDING_KEY.reset(token)
                RECORDING_ATTEMPTS.reset(attempts_token)
        if not events or events[-1]["type"] != "stream_end":
            raise ProviderError("incomplete streams are never cached")
        self.directory.mkdir(parents=True, exist_ok=True)
        usage_requests = (
            [row for row in self.budget.recording_requests(key) if row["id"] in attempts]
            if self.budget is not None
            else []
        )
        self.validate_usage(usage_requests)
        recording = {
            "request": req.model_dump(),
            "endpoint": self.endpoint,
            "limits": self.model_limits.model_dump(),
            "fixture_identity": self.fixture_identity,
            "events": events,
            "delays": delays,
            "usage_requests": usage_requests,
        }
        encoded = json.dumps(recording)
        archive = self.directory / "archive"
        archive.mkdir(exist_ok=True)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        (archive / f"{key}.{digest}.json").write_text(encoded)
        if path.exists():
            previous = path.read_bytes()
            old_digest = hashlib.sha256(previous).hexdigest()
            (archive / f"{key}.{old_digest}.json").write_bytes(previous)
        temporary = path.with_suffix(f".{uuid4().hex}.tmp")
        temporary.write_text(encoded)
        temporary.replace(path)
        self.timings.append(
            {"request_key": key, "recording_sha256": digest, "provider_ms": sum(delays) * 1000, "fresh": True}
        )

    async def aclose(self) -> None:
        if self.source is not None:
            closer = getattr(self.source, "aclose", None)
            if closer is not None:
                await closer()


def live_provider(settings: dict[str, Any]) -> RecordedProvider:
    limits = ModelLimits(context_window=settings["context_window"], max_output=OUTPUT_LIMIT)
    budget = Budget(Path(settings["ledger"]), settings["campaign"])
    client = httpx.AsyncClient(
        transport=MeteredTransport(budget, limits.context_window + OUTPUT_LIMIT, settings.get("inference_slots")),
        headers={"X-OpenRouter-Cache": "false"},
    )
    source = OpenAICompatible(
        base_url=settings["endpoint"],
        api_key=settings["api_key"],
        limits={settings["model"]: limits},
        http_client=client,
        timeout=60,
    )
    return RecordedProvider(
        source, Path(settings["recordings"]), settings["endpoint"], limits, settings.get("fixture_identity", ""), budget
    )
