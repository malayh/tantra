from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from tantra import ClaimWriterPayload, CommandEnvelope, CommandReply, PostgresCoordinator, PostgresStore, SessionHeader
from tantra.events import InputQueued


async def test_dispatch_is_bounded_ordered_and_progresses_past_busy_roots(postgres_dsn, pg_schema):
    store = PostgresStore(postgres_dsn, pg_schema)
    roots = [uuid4().hex for _ in range(6)]
    gates = {root: asyncio.Event() for root in roots}
    started = asyncio.Queue()
    applied = {root: [] for root in roots}
    running = set()

    async def handler(envelope, apply):
        assert envelope.root_id not in running
        running.add(envelope.root_id)
        started.put_nowait((envelope.root_id, envelope.request_id))
        try:
            await gates[envelope.root_id].wait()

            async def operation(locked, view):
                await view.append(locked.root_id, [InputQueued(command_id=str(locked.request_id), input="accepted")])
                applied[locked.root_id].append(locked.request_id)
                return CommandReply(request_id=locked.request_id, result={"accepted": True})

            await apply(operation)
        finally:
            running.remove(envelope.root_id)

    coordinator = PostgresCoordinator(store, request_timeout=5, catch_up_interval=0.02)
    await coordinator.start(handler)
    envelopes = []
    try:
        for root in roots:
            await store.create(SessionHeader(id=root, agent="build"))
            assert await coordinator.acquire(root) is not None
        for root in [roots[0], roots[0], *roots[1:]]:
            envelope = CommandEnvelope(
                request_id=uuid4(),
                root_id=root,
                operation="claim_writer",
                payload=ClaimWriterPayload(connection_id=uuid4()),
                deadline=datetime.now(UTC) + timedelta(seconds=10),
            )
            envelopes.append(envelope)
            owner = await coordinator.locate(root)
            assert owner is not None
            await coordinator._put_request(envelope, owner)
        first = [await asyncio.wait_for(started.get(), 2) for _ in range(4)]
        assert len({root for root, _ in first}) == 4
        assert coordinator.dispatch_peak == 4
        assert len(coordinator._dispatchers) == 4
        released = next(root for root, _ in first if root != roots[0])
        gates[released].set()
        next_root, _ = await asyncio.wait_for(started.get(), 2)
        assert next_root not in {root for root, _ in first}
        assert applied[roots[0]] == []
        for gate in gates.values():
            gate.set()
        async with asyncio.timeout(3):
            while any(
                reply is None for reply in [await coordinator._request_reply(item.request_id) for item in envelopes]
            ):
                await asyncio.sleep(0.01)
        assert applied[roots[0]] == [item.request_id for item in envelopes if item.root_id == roots[0]]
        assert coordinator.dispatch_peak == 4
    finally:
        for gate in gates.values():
            gate.set()
        await coordinator.close()
        await store.close()


async def test_close_cancels_dispatch_and_rolls_back_uncommitted_journal(postgres_dsn, pg_schema):
    store = PostgresStore(postgres_dsn, pg_schema)
    entered = asyncio.Event()
    root = uuid4().hex

    async def handler(envelope, apply):
        async def operation(locked, view):
            await view.append(root, [InputQueued(command_id="rolled-back", input="pending")])
            entered.set()
            await asyncio.Event().wait()
            return CommandReply(request_id=locked.request_id)

        await apply(operation)

    coordinator = PostgresCoordinator(store, catch_up_interval=0.02)
    await coordinator.start(handler)
    try:
        await store.create(SessionHeader(id=root, agent="build"))
        owner = await coordinator.acquire(root)
        assert owner is not None
        envelope = CommandEnvelope(
            request_id=uuid4(),
            root_id=root,
            operation="claim_writer",
            payload=ClaimWriterPayload(connection_id=uuid4()),
            deadline=datetime.now(UTC) + timedelta(seconds=10),
        )
        await coordinator._put_request(envelope, owner)
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.wait_for(coordinator.close(), 2)
        assert coordinator._dispatchers == {}
        assert await store.read_page(root) == []
        assert await store.lookup_command(root, "rolled-back") is None
        async with store._connection() as conn:
            row = await (
                await conn.execute(
                    store._sql("SELECT reply FROM {schema}.coordinator_requests WHERE request_id = %s"),
                    (envelope.request_id,),
                )
            ).fetchone()
            assert row == (None,)
        assert store._pool.get_stats()["pool_available"] == store._pool.get_stats()["pool_size"]
    finally:
        await coordinator.close()
        await store.close()


async def test_cleanup_drains_both_backlogs_with_bounds_and_keeps_command_identity(postgres_dsn, pg_schema):
    store = PostgresStore(postgres_dsn, pg_schema)
    coordinator = PostgresCoordinator(store)
    await coordinator.start()
    root = uuid4().hex
    try:
        await store.create(SessionHeader(id=root, agent="build"))
        await store.enqueue(root, InputQueued(command_id="lifetime", input="accepted"))
        owner = await coordinator.acquire(root)
        assert owner is not None
        async with store._connection() as conn:
            request = CommandEnvelope(
                request_id=uuid4(),
                root_id=root,
                operation="claim_writer",
                payload=ClaimWriterPayload(connection_id=uuid4()),
                deadline=datetime.now(UTC) + timedelta(days=1),
            )
            from psycopg.types.json import Jsonb

            async with conn.cursor() as cursor:
                await cursor.executemany(
                    store._sql(
                        "INSERT INTO {schema}.coordinator_requests"
                        " (request_id,root_id,destination_instance,destination_generation,"
                        " envelope,deadline,completed_at)"
                        " VALUES (%s,%s,%s,%s,%s,clock_timestamp()-interval '2 days',"
                        " clock_timestamp()-interval '2 days')"
                    ),
                    [
                        (
                            uuid4(),
                            root,
                            str(owner.instance_id),
                            owner.generation,
                            Jsonb(request.model_dump(mode="json")),
                        )
                        for _ in range(250)
                    ],
                )
                await cursor.executemany(
                    store._sql(
                        "INSERT INTO {schema}.coordinator_changes (root_id,kind,created_at)"
                        " VALUES (%s,'request',clock_timestamp()-interval '2 days')"
                    ),
                    [(root,) for _ in range(250)],
                )
        assert await coordinator.cleanup(0) == 0
        assert await coordinator.cleanup(100) == 100
        assert await coordinator.cleanup(100) == 100
        async with store._connection() as conn:
            row = await (
                await conn.execute(
                    store._sql(
                        "SELECT (SELECT count(*) FROM {schema}.coordinator_requests),"
                        " (SELECT count(*) FROM {schema}.coordinator_changes"
                        " WHERE created_at < clock_timestamp()-interval '1 day')"
                    )
                )
            ).fetchone()
            assert row == (150, 150)
        removed = 200
        while True:
            count = await coordinator.cleanup(100)
            assert 0 <= count <= 100
            removed += count
            if count < 100:
                break
        assert removed == 500
        evidence = await store.lookup_command(root, "lifetime")
        assert evidence is not None and evidence[1].seq == 1
        async with coordinator.transaction(owner) as view:
            duplicate = await view.enqueue(root, InputQueued(command_id="lifetime", input="accepted"))
            assert duplicate.duplicate and duplicate.seq == 1
    finally:
        await coordinator.close()
        await store.close()
