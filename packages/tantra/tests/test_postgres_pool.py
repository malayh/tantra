import asyncio
import uuid
from contextlib import AsyncExitStack

import pytest

from tantra import PostgresCoordinator, PostgresStore, SessionHeader
from tantra.events import InputQueued

psycopg = pytest.importorskip("psycopg")


async def _coordinator(dsn: str, schema: str) -> tuple[PostgresStore, PostgresCoordinator]:
    store = PostgresStore(dsn, schema=schema)
    coordinator = PostgresCoordinator(store, lease_ttl=5, request_timeout=1, catch_up_interval=0.02)
    await coordinator.start()
    return store, coordinator


async def test_renewal_uses_control_connection_when_all_pool_slots_are_occupied(
    postgres_dsn: str, pg_schema: str
) -> None:
    store, coordinator = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await store.create(SessionHeader(id=root, agent="build"))
    ownership = await coordinator.acquire(root)
    assert ownership is not None
    try:
        async with AsyncExitStack() as stack:
            connections = [await stack.enter_async_context(store._connection()) for _ in range(4)]
            assert len({conn.info.backend_pid for conn in connections}) == 4
            stats = store._pool.get_stats()
            assert stats["pool_size"] == 4
            assert stats["pool_available"] == 0

            renewed = await asyncio.wait_for(coordinator.renew(ownership), 1)

            assert renewed.generation == ownership.generation
            assert renewed.expires_at > ownership.expires_at
    finally:
        await coordinator.close()
        await store.close()


async def test_reused_pool_connection_does_not_retain_transaction_fence(postgres_dsn: str, pg_schema: str) -> None:
    store, coordinator = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await store.create(SessionHeader(id=root, agent="build"))
    ownership = await coordinator.acquire(root)
    assert ownership is not None
    try:
        background = [coordinator._observation_task, coordinator._dispatch_task]
        assert all(task is not None for task in background)
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        coordinator._observation_task = None
        coordinator._dispatch_task = None
        await store.close()
        async with store._setup_lock:
            await store._open_pool()
            await store._pool.wait()

        async with coordinator.transaction(ownership) as view:
            transaction_pid = view.conn.info.backend_pid
            await view.append(root, [InputQueued(command_id="accepted", input="accepted")])

        async with store._connection() as conn:
            assert conn.info.backend_pid == transaction_pid
            for setting in ("tantra.root_id", "tantra.instance_id", "tantra.generation"):
                cursor = await conn.execute("SELECT current_setting(%s, true)", (setting,))
                assert (await cursor.fetchone())[0] in (None, "")

        with pytest.raises(psycopg.Error):
            await store.append(root, [InputQueued(command_id="rejected", input="rejected")])
    finally:
        await coordinator.close()
        await store.close()
