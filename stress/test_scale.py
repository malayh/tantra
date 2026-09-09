from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from stress.conftest import BACKENDS, close_store, drop_schema
from stress.driver import Policy, PolicyState, SyntheticEmbedder, SyntheticProvider, blob, turn_step
from stress.invariants import check_log, event_type, log
from tantra import (
    Agent,
    BuiltinMemory,
    Context,
    FileSystemStore,
    FreeText,
    FreeTextResponse,
    MemoryStore,
    MemoryWrite,
    PostgresStore,
    Runtime,
    Sample,
    SampleRequest,
    SessionHeader,
    SQLiteStore,
    Store,
    WriterReplaced,
    tool,
)
from tantra.providers.base import ToolCall

MODEL = "scale/worker"

SLOW = frozenset({"sqlite"})

EVENTS = 10_000

BATCH = 500

SESSIONS = 200

PAGE = 25

BUDGET = 60.0

PHRASE = "quarterly heron audit"

AUDIT = "audit"

RETIRED = "retired"

KINDS = ("fact", "preference", "decision")

FILLER = blob(120, tag="scale")


@dataclass
class Substrate:
    """A backend plus a factory for further instances over the same storage."""

    backend: str
    make: Callable[[], Awaitable[Store]]


@pytest.fixture(params=BACKENDS)
async def substrate(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Substrate]:
    backend = request.param
    dsn = request.getfixturevalue("postgres_dsn") if backend == "postgres" else ""
    schema = f"s_{uuid4().hex[:8]}"
    opened: list[Store] = []

    async def make() -> Store:
        if backend == "memory":
            store: Store = MemoryStore()
        elif backend == "fs":
            store = FileSystemStore(tmp_path / "sessions")
        elif backend == "sqlite":
            store = SQLiteStore(tmp_path / "sessions.db")
        else:
            store = PostgresStore(dsn, schema=schema)
        await store.setup()
        opened.append(store)
        return store

    try:
        yield Substrate(backend=backend, make=make)
    finally:
        for store in opened:
            await close_store(store)
        if dsn:
            drop_schema(dsn, schema)


@tool
async def confirm(step: str, ctx: Context) -> str:
    """Ask a human to confirm `step`, then report it done."""
    answer = await ctx.ask(FreeText(prompt=f"confirm {step}"))
    return f"{step}:{answer.text}"


class Runner(Agent):
    """Runs one small step, confirming it first."""

    model = MODEL
    tools = [confirm]
    permissions = {"confirm": "allow"}


def build(store: Store, policy: Policy) -> tuple[Runtime, SyntheticProvider]:
    provider = SyntheticProvider(policy)
    return Runtime(provider, store, [Runner]), provider


def plain(text: str) -> Policy:
    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        return Sample(text=text)

    return policy


def confirming(step: str) -> Policy:
    def policy(req: SampleRequest, state: PolicyState) -> Sample:
        if turn_step(req) < 1:
            return Sample(
                tool_calls=[ToolCall(id=state.next_call_id(), name="confirm", args=json.dumps({"step": step}))]
            )
        return Sample(text="stepped")

    return policy


async def execute(runtime: Runtime, sid: str, input: str) -> Any:
    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        return await connection.prompt(input, command_id=uuid4())


async def wait_for(store: Store, sid: str, kind: str) -> Any:
    for _ in range(10_000):
        events = [event for event in await log(store, sid) if event_type(event) == kind]
        if events:
            return events[-1]
        await asyncio.sleep(0)
    raise AssertionError(f"{kind} was not recorded")


async def public_log(runtime: Runtime, sid: str, last_seq: int) -> list[Any]:
    stream = runtime.events(UUID(hex=sid))
    events: list[Any] = []
    try:
        async for item in stream:
            events.append(item)
            if item.seq == last_seq:
                return events
    finally:
        await stream.aclose()
    return events


async def seeded(store: Store) -> tuple[Runtime, str, Any]:
    """Create one actor turn and return its Runtime, id, and a `TextPart` to clone in bulk."""
    runtime, _ = build(store, plain("seeded"))
    sid = (await runtime.create(Runner)).hex
    await execute(runtime, sid, "seed the log")
    template = next(event for event in await log(store, sid) if event_type(event) == "text_part")
    return runtime, sid, template


def report(backend: str, label: str, elapsed: float, detail: str = "") -> None:
    print(f"[scale/{backend}] {label} {elapsed:.2f}s {detail}".rstrip())


async def test_ten_thousand_events(substrate: Substrate) -> None:
    store = await substrate.make()
    runtime, sid, template = await seeded(store)
    seed_count = len(await log(store, sid))

    start = time.perf_counter()
    last = 0
    for offset in range(0, EVENTS, BATCH):
        batch = [template.model_copy(update={"text": f"{offset + index}:{FILLER}"}) for index in range(BATCH)]
        last = await store.append(sid, batch)
    appended = time.perf_counter() - start

    start = time.perf_counter()
    stamped = [item async for item in store.read(sid)]
    read = time.perf_counter() - start

    start = time.perf_counter()
    replayed = await public_log(runtime, sid, stamped[-1].seq)
    replay = time.perf_counter() - start

    report(substrate.backend, "append", appended, f"{EVENTS} events in {EVENTS // BATCH} batches")
    report(substrate.backend, "read", read, f"{len(stamped)} events")
    report(substrate.backend, "replay", replay, f"{len(replayed)} events")

    assert last == seed_count + EVENTS
    assert [item.seq for item in stamped] == list(range(1, seed_count + EVENTS + 1))
    assert [item.seq for item in replayed] == [item.seq for item in stamped]
    assert replayed[-1].event.text == stamped[-1].event.text
    assert stamped[-1].event.text.startswith(f"{EVENTS - 1}:")
    assert appended < BUDGET
    assert read < BUDGET
    assert replay < BUDGET
    await check_log(store, sid)


async def test_list_paging(substrate: Substrate) -> None:
    store = await substrate.make()
    base = datetime.now(UTC) - timedelta(seconds=SESSIONS)
    tagged: set[str] = set()

    start = time.perf_counter()
    for index in range(SESSIONS):
        east = index % 3 == 0
        header = SessionHeader(
            id=uuid4().hex,
            agent="runner",
            created_at=base + timedelta(seconds=index),
            metadata={"desk": "east" if east else "west", "shard": index % 5},
        )
        await store.create(header)
        if east:
            tagged.add(header.id)
    created = time.perf_counter() - start

    start = time.perf_counter()
    seen: list[SessionHeader] = []
    cursor: str | None = None
    pages = 0
    while True:
        page = await store.list(limit=PAGE, before=cursor)
        if not page:
            break
        pages += 1
        seen.extend(page)
        cursor = page[-1].id
    paged = time.perf_counter() - start

    filtered = await store.list(metadata={"desk": "east"}, limit=SESSIONS)
    report(substrate.backend, "create", created, f"{SESSIONS} sessions")
    report(substrate.backend, "page", paged, f"{pages} pages of {PAGE}")

    keys = [(header.created_at, header.id) for header in seen]
    assert len(seen) == SESSIONS
    assert len({header.id for header in seen}) == SESSIONS
    assert pages == SESSIONS // PAGE
    assert keys == sorted(keys, reverse=True)
    assert {header.id for header in filtered} == tagged
    assert len(tagged) == len([index for index in range(SESSIONS) if index % 3 == 0])
    assert created < BUDGET
    assert paged < BUDGET


async def test_memory_at_scale(substrate: Substrate) -> None:
    store = await substrate.make()
    memory = BuiltinMemory(store, embedder=SyntheticEmbedder())
    count = 200 if substrate.backend in SLOW else 1000
    audits = count // 10
    retired = count // 20

    start = time.perf_counter()
    tagged: list[str] = []
    for index in range(count):
        audit = index % 10 == 0
        row = MemoryWrite(
            kind=AUDIT if audit else KINDS[index % 3],
            title=f"row {index}",
            body=f"{PHRASE} number {index}" if audit else f"routine note {index} about the west desk",
            tags=["nightly"] if audit else ["routine"],
            entities=["heron"] if audit else ["west"],
        )
        written = await memory.write(row)
        if audit:
            tagged.append(written)
    writes = time.perf_counter() - start

    start = time.perf_counter()
    top = await memory.recall(PHRASE, k=10, kind=AUDIT)
    recalled = time.perf_counter() - start

    report(substrate.backend, "memory write", writes, f"{count} rows")
    report(substrate.backend, "memory recall", recalled, f"{len(top)} hits of {audits} audits")

    assert len(tagged) == audits
    assert len(top) == 10
    assert {hit.memory.id for hit in top} <= set(tagged)
    assert top[0].score == 1.0
    assert len(await memory.recall(PHRASE, k=3, kind=AUDIT)) == 3
    assert writes < BUDGET

    superseded = []
    for old in tagged[:retired]:
        superseded.append(await memory.supersede(old, MemoryWrite(kind=RETIRED, title="retired", body="withdrawn")))

    live = await memory.recall(PHRASE, k=count, kind=AUDIT)
    assert {hit.memory.id for hit in live} == set(tagged[retired:])
    assert len(superseded) == retired
    for old in tagged[:retired]:
        assert (await memory.get(old)).superseded_by is not None

    probe = await memory.recall("xyzzy quuz frobnicate", k=5)
    search = getattr(store, "memory_search", None)
    hybrid = search is not None and await search([0.5] * 8, 1) is not None
    report(substrate.backend, "memory modes", 0.0, f"hybrid={hybrid} probe={sorted({hit.mode for hit in probe})}")
    if hybrid:
        assert probe != []
        assert {hit.mode for hit in probe} == {"vector"}
    else:
        assert probe == []
        assert {hit.mode for hit in live} == {"keyword"}


async def test_same_runtime_writer_answers_live_ask(substrate: Substrate) -> None:
    store = await substrate.make()
    runtime, _ = build(store, confirming("ship"))
    sid = (await runtime.create(Runner)).hex

    async with runtime.connect(UUID(hex=sid), writable=True) as connection:
        pending = asyncio.create_task(connection.prompt("go", command_id=uuid4()))
        ask = await wait_for(store, sid, "ask_raised")
        await connection.answer(
            UUID(hex=ask.ask_id),
            FreeTextResponse(text="approved"),
            command_id=uuid4(),
        )
        result = await pending

    events = await log(store, sid)
    completed = [event for event in events if event_type(event) == "tool_call_completed"]
    assert result.outcome == "completed"
    assert [str(event.result) for event in completed] == ["ship:approved"]
    await check_log(store, sid)


async def test_writer_takeover_and_independent_roots(substrate: Substrate) -> None:
    store = await substrate.make()
    runtime, _ = build(store, plain("done"))
    first = await runtime.create(Runner)
    second = await runtime.create(Runner)

    async with runtime.connect(first, writable=True) as stale:
        await stale.send("queued", command_id=uuid4())
        async with runtime.connect(first, writable=True) as current:
            with pytest.raises(WriterReplaced):
                await stale.send("rejected", command_id=uuid4())
            first_result = await current.prompt("accepted", command_id=uuid4())

    async with runtime.connect(second, writable=True) as independent:
        second_result = await independent.prompt("separate", command_id=uuid4())

    first_log = await log(store, first.hex)
    second_log = await log(store, second.hex)
    assert first_result.outcome == second_result.outcome == "completed"
    assert len([event for event in first_log if event_type(event) == "turn_completed"]) == 2
    assert len([event for event in second_log if event_type(event) == "turn_completed"]) == 1
    assert first_log[0].root_id == first.hex
    assert second_log[0].root_id == second.hex
    await check_log(store, first.hex)
    await check_log(store, second.hex)
