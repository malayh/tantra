import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tantra import (
    ClaimWriterPayload,
    CommandEnvelope,
    CommandReply,
    CommandTimeout,
    CoordinatorUnavailable,
    LeaseLost,
    ModelChangeBusy,
    PostgresCoordinator,
    PostgresStore,
    SessionHeader,
    WriterReplaced,
)
from tantra.events import InputQueued, TurnCompleted, TurnStarted, Usage

psycopg = pytest.importorskip("psycopg")


async def _coordinator(
    dsn: str,
    schema: str,
    *,
    lease_ttl: float = 60.0,
    handler=None,
) -> tuple[PostgresStore, PostgresCoordinator]:
    store = PostgresStore(dsn, schema=schema)
    coordinator = PostgresCoordinator(
        store,
        lease_ttl=lease_ttl,
        request_timeout=1.0,
        catch_up_interval=0.02,
    )
    await coordinator.start(handler)
    return store, coordinator


async def _close(*pairs: tuple[PostgresStore, PostgresCoordinator]) -> None:
    for store, coordinator in pairs:
        await coordinator.close()
        await store.close()


async def _journal_notice(iterator):
    async for notice in iterator:
        if notice.kind == "journal":
            return notice
    raise AssertionError("watch ended")


async def _header_notice(iterator):
    async for notice in iterator:
        if notice.kind == "header":
            return notice
    raise AssertionError("watch ended")


async def test_concurrent_claim_has_one_winner_and_takeover_fences_the_old_owner(
    postgres_dsn: str, pg_schema: str
) -> None:
    first = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.12)
    second = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.12)
    root = uuid.uuid4().hex
    await first[0].create(SessionHeader(id=root, agent="build"))
    try:
        claims = await asyncio.gather(first[1].acquire(root), second[1].acquire(root))
        ownership = next(item for item in claims if item is not None)
        assert sum(item is not None for item in claims) == 1
        owner = first if ownership.instance_id == first[1].instance_id else second
        contender = second if owner is first else first

        await asyncio.sleep(0.15)
        replacement = await contender[1].acquire(root)

        assert replacement is not None
        assert replacement.generation == ownership.generation + 1
        with pytest.raises(LeaseLost):
            await owner[1].renew(ownership)
        with pytest.raises(LeaseLost):
            async with owner[1].transaction(ownership):
                pass
        async with contender[1].transaction(replacement) as store:
            assert await store.append(root, [InputQueued(command_id="new", input="accepted")]) == 1
    finally:
        await _close(first, second)


async def test_root_lock_closes_the_check_write_takeover_race(postgres_dsn: str, pg_schema: str) -> None:
    first = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.1)
    second = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.1)
    root = uuid.uuid4().hex
    await first[0].create(SessionHeader(id=root, agent="build"))
    ownership = await first[1].acquire(root)
    assert ownership is not None
    try:
        async with first[1].transaction(ownership) as store:
            await store.append(root, [InputQueued(command_id="old", input="committed")])
            await asyncio.sleep(0.12)
            takeover = asyncio.create_task(second[1].acquire(root))
            await asyncio.sleep(0.04)
            assert not takeover.done()

        replacement = await asyncio.wait_for(takeover, 1)
        assert replacement is not None
        assert replacement.generation == ownership.generation + 1
        assert [item.event.command_id async for item in first[0].read(root)] == ["old"]
    finally:
        await _close(first, second)


async def test_transaction_view_cannot_be_reused_after_exit(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        async with pair[1].transaction(ownership) as retained:
            assert await retained.header(root) is not None

        with pytest.raises(CoordinatorUnavailable, match="view is closed"):
            await retained.set_active(root, True)
        with pytest.raises(CoordinatorUnavailable, match="view is closed"):
            await retained.append(root, [InputQueued(command_id="stale", input="no")])
    finally:
        await _close(pair)


async def test_coordinator_table_mutation_checks_expiry_at_the_operation(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.1)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        async with pair[1].transaction(ownership) as store:
            await asyncio.sleep(0.12)
            with pytest.raises(LeaseLost):
                await store.set_active(root, True)
    finally:
        await _close(pair)


async def test_enrolled_roots_reject_bare_execution_writes_but_keep_application_patches(
    postgres_dsn: str, pg_schema: str
) -> None:
    managed = await _coordinator(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    root = uuid.uuid4().hex
    plain = uuid.uuid4().hex
    await managed[0].create(SessionHeader(id=root, agent="build", model="one"))
    await managed[0].create(SessionHeader(id=plain, agent="build"))
    ownership = await managed[1].acquire(root)
    assert ownership is not None
    try:
        with pytest.raises(psycopg.Error):
            await bare.append(root, [InputQueued(command_id="bare", input="rejected")])

        updated = await bare.patch_header(
            root,
            title="allowed",
            model="two",
            metadata={"tenant": "x"},
        )
        assert updated.title == "allowed"
        assert updated.model == "two"
        assert updated.metadata == {"tenant": "x"}
        notice = await asyncio.wait_for(_header_notice(managed[1].watch(root)), 1)
        assert notice.actor_id == root
        assert await bare.append(plain, [InputQueued(command_id="plain", input="accepted")]) == 1

        async with managed[1].transaction(ownership) as store:
            changed = await store.patch_header(root, model="three")
        assert changed.model == "three"
    finally:
        await bare.close()
        await _close(managed)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "running"),
        ("pending_ask", "ask"),
        ("usage", Usage(input_tokens=1)),
        ("finished", True),
    ],
)
async def test_model_patch_cannot_bypass_execution_header_fencing(
    postgres_dsn: str, pg_schema: str, field: str, value
) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build", model="one"))
    assert await pair[1].acquire(root) is not None
    try:
        with pytest.raises(psycopg.Error):
            await bare.patch_header(root, model="two", **{field: value})
    finally:
        await bare.close()
        await _close(pair)


async def test_whole_header_put_cannot_bypass_idle_model_patch_path(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build", model="one"))
    assert await pair[1].acquire(root) is not None
    header = await bare.header(root)
    assert header is not None
    try:
        with pytest.raises(psycopg.Error):
            await bare.put_header(header.model_copy(update={"model": "two"}))
    finally:
        await bare.close()
        await _close(pair)


@pytest.mark.parametrize("busy", ["queued", "running", "awaiting_input", "activity", "request"])
async def test_application_model_patch_rejects_work_anywhere_in_the_tree(
    postgres_dsn: str, pg_schema: str, busy: str
) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    root = uuid.uuid4().hex
    child = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build", model="one"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        if busy in {"queued", "running", "awaiting_input"}:
            async with pair[1].transaction(ownership) as store:
                await store.create(
                    SessionHeader(
                        id=child,
                        root_id=root,
                        parent_id=root,
                        agent="build",
                        status=busy,
                    )
                )
        elif busy == "activity":
            async with pair[1].transaction(ownership) as store:
                await store.set_active(child, True)
        else:
            envelope = CommandEnvelope(
                request_id=uuid.uuid4(),
                root_id=root,
                operation="claim_writer",
                payload=ClaimWriterPayload(connection_id=uuid.uuid4()),
                deadline=datetime.now(UTC) + timedelta(seconds=2),
            )
            await pair[1]._put_request(envelope, ownership)

        with pytest.raises(ModelChangeBusy):
            await bare.patch_header(root, model="two")
    finally:
        await bare.close()
        await _close(pair)


async def test_application_model_patch_serializes_with_coordinated_command(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    blocker = await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build", model="one"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    patch = None
    command = None

    async def enqueue():
        async with pair[1].transaction(ownership) as store:
            return await store.enqueue(root, InputQueued(command_id="command", input="accepted"))

    try:
        async with blocker.transaction():
            await blocker.execute(
                pair[1]._sql("SELECT 1 FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
                (root,),
            )
            patch = asyncio.create_task(bare.patch_header(root, model="two"))
            await asyncio.sleep(0.03)
            command = asyncio.create_task(enqueue())
            await asyncio.sleep(0.03)
            assert not patch.done()
            assert not command.done()

        assert (await asyncio.wait_for(patch, 1)).model == "two"
        assert (await asyncio.wait_for(command, 1)).seq == 1
    finally:
        for task in (patch, command):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (patch, command) if task is not None), return_exceptions=True)
        await blocker.close()
        await bare.close()
        await _close(pair)


async def test_empty_root_id_and_cross_root_request_claims_cannot_bypass_fencing(
    postgres_dsn: str, pg_schema: str
) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    first = uuid.uuid4().hex
    second = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=first, root_id="", agent="build"))
    await pair[0].create(SessionHeader(id=second, agent="build"))
    first_ownership = await pair[1].acquire(first)
    second_ownership = await pair[1].acquire(second)
    assert first_ownership is not None and second_ownership is not None
    envelope = CommandEnvelope(
        request_id=uuid.uuid4(),
        root_id=first,
        operation="claim_writer",
        payload=ClaimWriterPayload(connection_id=uuid.uuid4()),
        deadline=datetime.now(UTC) + timedelta(seconds=2),
    )
    try:
        with pytest.raises(psycopg.Error):
            await bare.append(first, [InputQueued(command_id="bare", input="no")])
        await pair[1]._put_request(envelope, first_ownership)
        async with pair[1].transaction(second_ownership) as store:
            assert await store.lock_request(envelope.request_id) is None
    finally:
        await bare.close()
        await _close(pair)


async def test_latest_writer_claim_wins_without_releasing_execution(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        async with pair[1].transaction(ownership) as store:
            first = await store.claim_writer(uuid.uuid4())
        async with pair[1].transaction(ownership) as store:
            second = await store.claim_writer(uuid.uuid4())
            with pytest.raises(WriterReplaced, match="replaced"):
                await store.validate_writer(first)
            await store.validate_writer(second)

        assert await pair[1].release(ownership)
        located = await pair[1].locate(root)
        assert located is None
        reacquired = await pair[1].acquire(root)
        assert reacquired is not None
        async with pair[1].transaction(reacquired) as store:
            await store.validate_writer(second)
    finally:
        await _close(pair)


async def test_queued_fifo_work_keeps_ownership_after_an_earlier_turn_finishes(
    postgres_dsn: str, pg_schema: str
) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    first = uuid.uuid4().hex
    second = uuid.uuid4().hex
    try:
        async with pair[1].transaction(ownership) as store:
            await store.enqueue(root, InputQueued(command_id=first, input="one"))
            await store.append(root, [TurnStarted(turn_id=first, input="one")])
            await store.enqueue(root, InputQueued(command_id=second, input="two"))
            await store.append(root, [TurnCompleted(turn_id=first, stop_reason="completed")])

        header = await pair[0].header(root)
        assert header is not None and header.status == "idle"
        assert await pair[1].active(root, root)
        assert not await pair[1].release(ownership)
        async with pair[1].transaction(ownership) as store:
            with pytest.raises(ModelChangeBusy):
                await store.patch_header(root, model="new-model")
    finally:
        await _close(pair)


async def test_duplicate_completed_input_does_not_reschedule_the_actor(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    command = uuid.uuid4().hex
    event = InputQueued(command_id=command, input="done")
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        async with pair[1].transaction(ownership) as store:
            await store.enqueue(root, event)
            await store.append(
                root,
                [TurnStarted(turn_id=command, input="done"), TurnCompleted(turn_id=command, stop_reason="completed")],
            )
            await store.set_active(root, False)
            duplicate = await store.enqueue(root, event)

        assert duplicate.duplicate
        assert not await pair[1].active(root, root)
        assert await pair[1].release(ownership)
    finally:
        await _close(pair)


async def test_activity_uses_live_ownership_and_recovery_state_is_transactional(
    postgres_dsn: str, pg_schema: str
) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.12)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        async with pair[1].transaction(ownership) as store:
            await store.set_active(root, True)
            await store.set_recovery({"phase": "interrupting", "cursor": 4})
            assert await store.recovery() == {"phase": "interrupting", "cursor": 4}

        assert await pair[1].active(root, root)
        ownership = await pair[1].renew(ownership)
        assert not await pair[1].release(ownership)
        await asyncio.sleep(0.15)
        assert not await pair[1].active(root, root)
    finally:
        await _close(pair)


async def test_request_acceptance_reply_and_duplicate_are_one_transaction(postgres_dsn: str, pg_schema: str) -> None:
    calls = 0

    async def handler(envelope, apply):
        nonlocal calls

        async def accept(locked, store):
            nonlocal calls
            calls += 1
            payload = locked.payload
            token = await store.claim_writer(payload.connection_id)
            await store.enqueue(
                locked.root_id,
                InputQueued(command_id=locked.request_id.hex, input="transported"),
            )
            return CommandReply(
                request_id=locked.request_id,
                result={"connection_id": str(token.connection_id)},
            )

        await apply(accept)

    owner = await _coordinator(postgres_dsn, pg_schema, handler=handler)
    sender = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await owner[0].create(SessionHeader(id=root, agent="build"))
    assert await owner[1].acquire(root) is not None
    envelope = CommandEnvelope(
        request_id=uuid.uuid4(),
        root_id=root,
        operation="claim_writer",
        payload=ClaimWriterPayload(connection_id=uuid.uuid4()),
        deadline=datetime.now(UTC) + timedelta(seconds=2),
    )
    try:
        first = await sender[1].request(envelope)
        duplicate = await sender[1].request(envelope)

        assert first == duplicate
        assert first.status == "ok"
        assert calls == 1
        events = [item async for item in owner[0].read(root)]
        assert len(events) == 1
        assert events[0].event.command_id == envelope.request_id.hex
    finally:
        await _close(owner, sender)


async def test_expired_request_never_runs(postgres_dsn: str, pg_schema: str) -> None:
    called = False

    async def handler(envelope, apply):
        nonlocal called

        async def accept(locked, store):
            nonlocal called
            called = True
            return CommandReply(request_id=locked.request_id)

        await apply(accept)

    owner = await _coordinator(postgres_dsn, pg_schema, handler=handler)
    sender = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await owner[0].create(SessionHeader(id=root, agent="build"))
    assert await owner[1].acquire(root) is not None
    envelope = CommandEnvelope(
        request_id=uuid.uuid4(),
        root_id=root,
        operation="claim_writer",
        payload=ClaimWriterPayload(connection_id=uuid.uuid4()),
        deadline=datetime.now(UTC) - timedelta(milliseconds=1),
    )
    try:
        with pytest.raises(CommandTimeout):
            await sender[1].request(envelope)
        await asyncio.sleep(0.05)
        assert not called
    finally:
        await _close(owner, sender)


async def test_retry_after_lost_reply_reads_the_committed_result_without_reapplying(
    postgres_dsn: str, pg_schema: str
) -> None:
    calls = 0

    async def handler(envelope, apply):
        nonlocal calls

        async def accept(locked, store):
            nonlocal calls
            calls += 1
            return CommandReply(request_id=locked.request_id, result={"accepted": True})

        await apply(accept)

    owner = await _coordinator(postgres_dsn, pg_schema, handler=handler)
    sender = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await owner[0].create(SessionHeader(id=root, agent="build"))
    ownership = await owner[1].acquire(root)
    assert ownership is not None
    envelope = CommandEnvelope(
        request_id=uuid.uuid4(),
        root_id=root,
        operation="claim_writer",
        payload=ClaimWriterPayload(connection_id=uuid.uuid4()),
        deadline=datetime.now(UTC) + timedelta(seconds=2),
    )
    try:
        await sender[1]._put_request(envelope, ownership)
        deadline = asyncio.get_running_loop().time() + 1
        while await sender[1]._request_reply(envelope.request_id) is None:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)

        reply = await sender[1].request(envelope)

        assert reply.result == {"accepted": True}
        assert calls == 1
    finally:
        await _close(owner, sender)


async def test_pending_request_is_rerouted_after_ownership_changes(postgres_dsn: str, pg_schema: str) -> None:
    async def handler(envelope, apply):
        async def accept(locked, store):
            return CommandReply(request_id=locked.request_id, result={"owner": "new"})

        await apply(accept)

    old_owner = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.12)
    new_owner = await _coordinator(postgres_dsn, pg_schema, lease_ttl=0.12, handler=handler)
    sender = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await old_owner[0].create(SessionHeader(id=root, agent="build"))
    assert await old_owner[1].acquire(root) is not None
    envelope = CommandEnvelope(
        request_id=uuid.uuid4(),
        root_id=root,
        operation="claim_writer",
        payload=ClaimWriterPayload(connection_id=uuid.uuid4()),
        deadline=datetime.now(UTC) + timedelta(seconds=2),
    )
    try:
        waiting = asyncio.create_task(sender[1].request(envelope))
        await asyncio.sleep(0.15)
        replacement = await new_owner[1].acquire(root)
        assert replacement is not None

        reply = await asyncio.wait_for(waiting, 1)

        assert reply.result == {"owner": "new"}
    finally:
        await _close(old_owner, new_owner, sender)


async def test_watch_catches_up_after_listener_disconnect(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None
    try:
        async with pair[1].transaction(ownership) as store:
            await store.append(root, [InputQueued(command_id="first", input="one")])
        iterator = pair[1].watch(root)
        first = await asyncio.wait_for(_journal_notice(iterator), 1)

        if pair[1]._listener_conn is not None:
            await pair[1]._listener_conn.close()
        async with pair[1].transaction(ownership) as store:
            await store.append(root, [InputQueued(command_id="second", input="two")])
        second = await asyncio.wait_for(_journal_notice(iterator), 1)

        assert second.change_id > first.change_id
        assert second.seq == 2
        await iterator.aclose()
    finally:
        await _close(pair)


async def test_blocked_store_connection_does_not_starve_renewal(postgres_dsn: str, pg_schema: str) -> None:
    pair = await _coordinator(postgres_dsn, pg_schema, lease_ttl=1.0)
    root = uuid.uuid4().hex
    await pair[0].create(SessionHeader(id=root, agent="build"))
    ownership = await pair[1].acquire(root)
    assert ownership is not None

    async def block_data_connection():
        async with pair[0]._lock:
            conn = await pair[0]._connection()
            await conn.execute("SELECT pg_sleep(0.25)")

    try:
        blocked = asyncio.create_task(block_data_connection())
        await asyncio.sleep(0.03)
        started = time.monotonic()
        renewed = await pair[1].renew(ownership)
        elapsed = time.monotonic() - started

        assert renewed.expires_at > ownership.expires_at
        assert elapsed < 0.15
        await blocked
    finally:
        await _close(pair)
