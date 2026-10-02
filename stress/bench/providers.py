from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from collections.abc import AsyncIterator
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


class BudgetExceeded(ProviderError):
    pass


class Budget:
    def __init__(self, path: Path, campaign: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.campaign = campaign
        with self.connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS requests ("
                "id TEXT PRIMARY KEY, campaign TEXT NOT NULL, reserved INTEGER NOT NULL, "
                "tokens INTEGER, generation TEXT, usage TEXT, created REAL NOT NULL)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS campaign_idx ON requests(campaign)")

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30)

    def reserve(self, capacity: int) -> str:
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
                "INSERT INTO requests(id, campaign, reserved, created) VALUES (?, ?, ?, ?)",
                (request_id, self.campaign, capacity, time.time()),
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
            "reported_cost": sum(u.get("cost") or 0 for u in usages),
            "cost_available": bool(usages) and all("cost" in u for u in usages),
        }


class MeteredStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpx.AsyncByteStream, budget: Budget, request_id: str) -> None:
        self.stream = stream
        self.budget = budget
        self.request_id = request_id
        self.pending = b""
        self.generation_id: str | None = None
        self.usage: dict[str, Any] | None = None

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
        await self.stream.aclose()


class MeteredTransport(httpx.AsyncBaseTransport):
    def __init__(self, budget: Budget, capacity: int) -> None:
        self.inner = httpx.AsyncHTTPTransport(retries=0)
        self.budget = budget
        self.capacity = capacity

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        reservation = self.budget.reserve(self.capacity) if request.url.path.endswith("/chat/completions") else None
        response = await self.inner.handle_async_request(request)
        if reservation is not None:
            response.stream = MeteredStream(response.stream, self.budget, reservation)
        return response

    async def aclose(self) -> None:
        await self.inner.aclose()


def prepared(req: SampleRequest) -> SampleRequest:
    return req.model_copy(update={"params": {**req.params, "max_completion_tokens": OUTPUT_LIMIT, "temperature": 0}})


def request_key(req: SampleRequest, endpoint: str) -> str:
    payload = {"fixture_version": FIXTURE_VERSION, "endpoint": endpoint, "request": prepared(req).model_dump()}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


class RecordedProvider:
    def __init__(self, source: Provider | None, directory: Path, endpoint: str, limits: ModelLimits) -> None:
        self.source = source
        self.directory = directory
        self.endpoint = endpoint
        self.model_limits = limits
        self.requests: list[SampleRequest] = []

    def limits(self, model: str) -> ModelLimits:
        return self.model_limits

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        req = prepared(req)
        self.requests.append(req)
        path = self.directory / f"{request_key(req, self.endpoint)}.json"
        if self.source is None:
            if not path.is_file():
                raise ProviderError(f"recording miss: {path.name}; replay cannot make paid requests")
            recording = json.loads(path.read_text())
            if recording["request"] != req.model_dump() or recording["endpoint"] != self.endpoint:
                raise ProviderError("recording identity mismatch")
            if not recording["events"] or recording["events"][-1]["type"] != "stream_end":
                raise ProviderError("recording has no completed stream")
            for item, delay in zip(recording["events"], recording["delays"], strict=True):
                await asyncio.sleep(delay)
                yield EVENT_ADAPTER.validate_python(item)
            return
        events, delays = [], []
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
            await iterator.aclose()
        if not events or events[-1]["type"] != "stream_end":
            raise ProviderError("incomplete streams are never cached")
        self.directory.mkdir(parents=True, exist_ok=True)
        recording = {
            "request": req.model_dump(),
            "endpoint": self.endpoint,
            "limits": self.model_limits.model_dump(),
            "events": events,
            "delays": delays,
        }
        temporary = path.with_suffix(f".{uuid4().hex}.tmp")
        temporary.write_text(json.dumps(recording))
        temporary.replace(path)

    async def aclose(self) -> None:
        if self.source is not None:
            closer = getattr(self.source, "aclose", None)
            if closer is not None:
                await closer()


def live_provider(settings: dict[str, Any]) -> RecordedProvider:
    limits = ModelLimits(context_window=settings["context_window"], max_output=OUTPUT_LIMIT)
    budget = Budget(Path(settings["ledger"]), settings["campaign"])
    client = httpx.AsyncClient(
        transport=MeteredTransport(budget, limits.context_window + OUTPUT_LIMIT),
        headers={"X-OpenRouter-Cache": "false"},
    )
    source = OpenAICompatible(
        base_url=settings["endpoint"],
        api_key=settings["api_key"],
        limits={settings["model"]: limits},
        http_client=client,
        timeout=60,
    )
    return RecordedProvider(source, Path(settings["recordings"]), settings["endpoint"], limits)
