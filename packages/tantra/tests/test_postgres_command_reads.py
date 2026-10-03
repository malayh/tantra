from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tantra import Agent, FakeProvider, PostgresCoordinator, PostgresStore, Runtime
from tantra.ask import FreeTextResponse
from tantra.coordinator import (
    AnswerPayload,
    CancelPayload,
    ClaimWriterPayload,
    CommandEnvelope,
    ReleaseWriterPayload,
    SendPayload,
    WriterToken,
)
from tantra.events import (
    AgentFinished,
    AskAnswered,
    InputQueued,
    SessionHeader,
    TextDelta,
    TurnCompleted,
    TurnStarted,
)
from tantra.stores import postgres


class Bot(Agent):
    pass


@pytest.mark.parametrize("size", [4_000, 100_000])
async def test_indexed_reads_and_warm_dispatch_are_bounded(postgres_dsn, pg_schema, monkeypatch, size):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, completed, answer = uuid4(), uuid4(), uuid4()
    child, child_command = uuid4(), uuid4()
    response = FreeTextResponse(text="yes\x00")
    ask = uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="bot"))
    await store.create(SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="bot", depth=1))
    await store.append(child.hex, [InputQueued(command_id=child_command.hex, input="child")])
    await store.append(root.hex, [InputQueued(command_id=completed.hex, input="done")])
    await store.append(root.hex, [TurnStarted(turn_id=completed.hex, input="done")])
    for offset in range(0, size, 1000):
        await store.append(root.hex, [TextDelta(text="x") for _ in range(min(1000, size - offset))])
    await store.append(
        root.hex,
        [
            TurnCompleted(turn_id=completed.hex, stop_reason="stop"),
            AskAnswered(ask_id=ask.hex, command_id=answer.hex, response=response, answered_by=root.hex),
        ],
    )
    await store.patch_header(root.hex, finished=True)
    parsed = []
    original = postgres._parse

    def counted(sid, raw):
        stamped = original(sid, raw)
        parsed.append(stamped)
        return stamped

    monkeypatch.setattr(postgres, "_parse", counted)

    async def forbidden(*args, **kwargs):
        pytest.fail("indexed operation read the journal")

    monkeypatch.setattr(store, "read", forbidden)
    monkeypatch.setattr(store, "read_page", forbidden)
    connection = await store._connection()
    execute = connection.execute
    queries = []

    async def capture(query, params=None, **kwargs):
        if "WITH RECURSIVE actors AS" in query.as_string(connection):
            queries.append((query, params))
        return await execute(query, params, **kwargs)

    monkeypatch.setattr(connection, "execute", capture)
    found = await store.lookup_command(root.hex, completed.hex)
    assert found[0] == root.hex and found[1].seq == 1
    assert await store.lookup_command(root.hex, uuid4().hex) is None
    assert await store.lookup_finished(root.hex) is None
    assert (await store.enqueue(root.hex, InputQueued(command_id=completed.hex, input="done"))).duplicate
    assert len(parsed) == 2
    cursor = await execute(postgres.sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) ") + queries[0][0], queries[0][1])
    plans = [(await cursor.fetchone())[0][0]["Plan"]]
    event_reads = []
    while plans:
        plan = plans.pop()
        plans.extend(plan.get("Plans", []))
        if plan.get("Relation Name") == "events":
            event_reads.append(plan)
    assert event_reads
    assert all(plan["Actual Rows"] <= 1 and "Index" in plan["Node Type"] for plan in event_reads)
    monkeypatch.setattr(connection, "execute", execute)

    coordinator = PostgresCoordinator(store, lease_ttl=30, catch_up_interval=0.02)
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    ownership = await coordinator.acquire(root.hex)
    runtime._ownerships[root.hex] = ownership

    async def keep_owner(root_id):
        return None

    monkeypatch.setattr(runtime, "_release_if_idle_locked", keep_owner)
    monkeypatch.setattr(runtime, "_journal", forbidden)
    monkeypatch.setattr(runtime, "_accepted_input", lambda sid: None)
    token = None

    async def dispatch(operation, payload, *, error=None):
        envelope = CommandEnvelope(
            request_id=uuid4(),
            root_id=root.hex,
            operation=operation,
            payload=payload,
            writer_token=None if operation == "claim_writer" else token,
            deadline=datetime.now(UTC) + timedelta(seconds=30),
        )
        replies = []

        async def transact(apply):
            async with coordinator.transaction(ownership) as view:
                reply = await apply(envelope, view)
                replies.append(reply)
                return reply

        await runtime._handle_request(envelope, transact)
        assert replies[0].status == ("error" if error else "ok"), replies[0]
        if error:
            assert replies[0].error_code == error
        return replies[0].result

    try:
        parsed.clear()
        claimed = await dispatch("claim_writer", ClaimWriterPayload(connection_id=uuid4()))
        token = WriterToken.model_validate(claimed["writer_token"])
        assert (await dispatch("release_writer", ReleaseWriterPayload()))["released"]
        assert parsed == []
        claimed = await dispatch("claim_writer", ClaimWriterPayload(connection_id=uuid4()))
        token = WriterToken.model_validate(claimed["writer_token"])
        pending = uuid4()
        assert not (await dispatch("send", SendPayload(command_id=pending, input="new\x00")))["duplicate"]
        assert (await dispatch("send", SendPayload(command_id=pending, input="new\x00")))["duplicate"]
        assert (await dispatch("answer", AnswerPayload(command_id=answer, ask_id=ask, response=response)))["duplicate"]
        assert len(parsed) == 2
        for command, input in ((pending, "changed"), (child_command, "child"), (answer, "yes\x00")):
            await dispatch("send", SendPayload(command_id=command, input=input), error="invalid_command_reuse")
        await dispatch(
            "answer",
            AnswerPayload(command_id=answer, ask_id=ask, response=FreeTextResponse(text="changed")),
            error="invalid_command_reuse",
        )
        await dispatch(
            "answer",
            AnswerPayload(command_id=uuid4(), ask_id=ask, response=response),
            error="ask_expired",
        )
        assert len(parsed) == 6
        async with coordinator.transaction(ownership) as view:
            parsed.clear()
            await view.set_active(root.hex, False)
            assert (await view.enqueue(root.hex, InputQueued(command_id=pending.hex, input="new\x00"))).duplicate
        assert await coordinator.active(root.hex, root.hex)
        async with coordinator.transaction(ownership) as view:
            await view.set_active(root.hex, False)
            assert (await view.enqueue(root.hex, InputQueued(command_id=completed.hex, input="done"))).duplicate
            assert len(parsed) == 2
        assert not await coordinator.active(root.hex, root.hex)
        async with coordinator.transaction(ownership) as view:
            await view.append(root.hex, [AgentFinished(result="done")])
            parsed.clear()
            assert (await view.lookup_finished(root.hex)).event.result == "done"
            assert len(parsed) == 1
        await dispatch("send", SendPayload(command_id=uuid4(), input="after finish"), error="tantra_error")
        assert len(parsed) == 2
    finally:
        monkeypatch.undo()
        await runtime.aclose()
        await store.close()


async def test_cancellation_reads_the_tree_through_the_fenced_transaction(postgres_dsn, pg_schema, monkeypatch):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, child = uuid4(), uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="bot"))
    await store.create(SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="bot", depth=1))
    for actor in (root, child):
        await store.enqueue(actor.hex, InputQueued(command_id=uuid4().hex, input="pending"))
    coordinator = PostgresCoordinator(store, lease_ttl=30, catch_up_interval=0.02)
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    ownership = await coordinator.acquire(root.hex)
    runtime._ownerships[root.hex] = ownership
    async with coordinator.transaction(ownership) as view:
        token = await view.claim_writer(uuid4())
    read = runtime._operational
    actors = []

    async def fenced(actor_id, *, store=None):
        assert store is not None and store._active
        assert store.conn.info.transaction_status == postgres.psycopg.pq.TransactionStatus.INTRANS
        actors.append(actor_id)
        return await read(actor_id, store=store)

    monkeypatch.setattr(runtime, "_operational", fenced)
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

    try:
        await runtime._handle_request(envelope, transact)
        assert actors == [root.hex, child.hex]
    finally:
        monkeypatch.undo()
        await runtime.aclose()
        await store.close()
