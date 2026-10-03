from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tantra import Agent, PostgresCoordinator, Runtime
from tantra.ask import FreeText
from tantra.coordinator import CancelPayload, CommandEnvelope
from tantra.events import (
    AskRaised,
    CancellationRequested,
    CompactionApplied,
    InputQueued,
    SampleCompleted,
    SessionHeader,
    TextDelta,
    TextPart,
    TurnCancelled,
    TurnCompleted,
    TurnInterrupted,
    TurnStarted,
)
from tantra.providers.fake import FakeProvider
from tantra.stores import postgres
from tantra.stores.postgres import PostgresStore


class Bot(Agent):
    model = "m"


@pytest.mark.parametrize("history_size", [4_000, 100_000])
async def test_recovery_cancel_results_and_shutdown_avoid_historical_reads(
    postgres_dsn, pg_schema, history_size, monkeypatch
):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, child, completed, abandoned, cancelled, later = (uuid4() for _ in range(6))
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="bot", model="m"))
    await store.create(
        SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="bot", model="m", depth=1)
    )
    await store.append(
        root.hex,
        [
            InputQueued(command_id=completed.hex, input="done"),
            TurnStarted(turn_id=completed.hex, input="done"),
            TextPart(sample_id="sample", text="old answer"),
            SampleCompleted(sample_id="sample"),
            TurnCompleted(turn_id=completed.hex, stop_reason="completed"),
        ],
    )
    for start in range(0, history_size, 1000):
        await store.append(root.hex, [TextDelta(text=str(index)) for index in range(start, start + 1000)])
    await store.append(
        root.hex,
        [
            TurnStarted(turn_id=abandoned.hex, input="abandoned"),
            AskRaised(ask_id=uuid4().hex, request=FreeText(prompt="old ask")),
            CompactionApplied(
                strategy="fixture",
                tokens_before=100,
                tokens_after=10,
                summary="old work completed",
                floor_turn_id=abandoned.hex,
            ),
        ],
    )
    await store.append(
        child.hex,
        [
            InputQueued(command_id=cancelled.hex, input="cancel me"),
            TurnStarted(turn_id=cancelled.hex, input="cancel me"),
            InputQueued(command_id=later.hex, input="keep queued"),
        ],
    )
    coordinator = PostgresCoordinator(store, lease_ttl=30, catch_up_interval=0.02)
    runtime = Runtime(FakeProvider([]), store, [Bot], coordinator=coordinator, history_mode="compacted")
    await runtime.start()
    ownership = await coordinator.acquire(root.hex)
    runtime._ownerships[root.hex] = ownership
    runtime._known_roots[root.hex] = root.hex

    async with coordinator.transaction(ownership) as view:
        await view.append(
            root.hex, [CancellationRequested(command_id=uuid4().hex, targets={child.hex: [cancelled.hex]})]
        )
    parsed = []
    original_parse = postgres._parse
    execute = postgres.psycopg.AsyncConnection.execute
    queries = []

    def counted(sid, raw):
        item = original_parse(sid, raw)
        parsed.append(item)
        return item

    async def capture(connection, query, params=None, **kwargs):
        rendered = query.as_string(connection) if hasattr(query, "as_string") else query
        if rendered.startswith("SELECT") and ".events" in rendered:
            queries.append((query, params))
        return await execute(connection, query, params, **kwargs)

    async def forbidden(*args, **kwargs):
        pytest.fail("operational Runtime path read full history")

    activated = []
    monkeypatch.setattr(postgres, "_parse", counted)
    monkeypatch.setattr(postgres.psycopg.AsyncConnection, "execute", capture)
    monkeypatch.setattr(runtime, "_journal", forbidden)
    monkeypatch.setattr(runtime, "_activate", lambda actor, root_id: activated.append(actor))
    try:
        async with runtime._lock(root.hex):
            await runtime._recover_locked(root.hex, ownership)
        assert later.hex in [item.command_id for item in (await store.read_operational(child.hex)).pending]
        assert child.hex in activated
        assert (await store.header(root.hex)).pending_ask is None
        interrupted = await runtime._result(root.hex, abandoned)
        assert interrupted.outcome == "interrupted"
        cancelled_result = await runtime._result(child.hex, cancelled)
        assert cancelled_result.outcome == "cancelled"
        assert (await runtime._result(root.hex, completed)).text == "old answer"
        assert await runtime._result(child.hex, later) is None
        snapshot = await runtime._history(root.hex)
        assert snapshot.items[0].event == TurnStarted(turn_id=abandoned.hex, input="abandoned")
        assert len(snapshot.items) < 10
        async with runtime._lock(root.hex):
            await runtime._recover_locked(root.hex, ownership)
        assert (
            sum(isinstance(item.event, TurnInterrupted) for item in await store.read_turn(root.hex, abandoned.hex)) == 1
        )
        assert (
            sum(isinstance(item.event, TurnCancelled) for item in await store.read_turn(child.hex, cancelled.hex)) == 1
        )
        async with coordinator.transaction(ownership) as view:
            token = await view.claim_writer(uuid4())
        envelope = CommandEnvelope(
            request_id=uuid4(),
            root_id=root.hex,
            operation="cancel",
            payload=CancelPayload(command_id=uuid4()),
            writer_token=token,
            deadline=datetime.now(UTC) + timedelta(seconds=30),
        )

        async def transact(apply):
            async with coordinator.transaction(ownership) as view:
                reply = await apply(envelope, view)
                assert reply.status == "ok", reply
                return reply

        await runtime._handle_request(envelope, transact)
        assert (await runtime._result(child.hex, later)).outcome == "cancelled"
        await runtime.aclose()
        assert len(parsed) < 100
        reads = []
        async with store._connection() as connection:
            for query, params in queries:
                cursor = await execute(connection, postgres.sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) ") + query, params)
                plans = [(await cursor.fetchone())[0][0]["Plan"]]
                while plans:
                    plan = plans.pop()
                    plans.extend(plan.get("Plans", []))
                    if plan.get("Relation Name") == "events" and plan["Actual Loops"]:
                        reads.append(plan)
        assert reads
        assert all(
            (
                "Index" in plan["Node Type"]
                or plan["Node Type"] == "Bitmap Heap Scan"
                and all("Index" in child["Node Type"] for child in plan.get("Plans", []))
            )
            and plan["Actual Rows"] + plan.get("Rows Removed by Filter", 0) <= 10
            for plan in reads
        ), reads
    finally:
        monkeypatch.undo()
        await runtime.aclose()
        await store.close()
