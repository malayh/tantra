from __future__ import annotations

import asyncio
import json
import multiprocessing
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import (
    Agent,
    CommandTimeout,
    CoordinatorUnavailable,
    FreeText,
    PostgresCoordinator,
    PostgresStore,
    Runtime,
    SessionBusy,
    SessionNotFound,
    TantraError,
    WriterReplaced,
)
from tantra.errors import SessionExists
from tantra.events import AskRaised, CancellationRequested, InputQueued, SessionHeader, TextPart, TurnStarted
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd
from tantra.providers.fake import FAKE_LIMITS

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")


class Bot(Agent):
    pass


class CountingProvider:
    def __init__(self) -> None:
        self.calls = 0

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.calls += 1
        yield StreamEnd(text="ok")


class GatedProvider(CountingProvider):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.calls += 1
        self.started.set()
        await self.release.wait()
        yield StreamEnd(text="ok")


def _coordinator(store: PostgresStore, *, lease_ttl: float = 0.3) -> PostgresCoordinator:
    return PostgresCoordinator(store, lease_ttl=lease_ttl, request_timeout=2.0, catch_up_interval=0.02)


async def _runtime(
    dsn: str,
    schema: str,
    *,
    provider: CountingProvider | None = None,
    lease_ttl: float = 0.3,
) -> tuple[PostgresStore, Runtime, CountingProvider]:
    store = PostgresStore(dsn, schema=schema)
    selected = provider or CountingProvider()
    runtime = Runtime(selected, store, [Bot], default_model="m", coordinator=_coordinator(store, lease_ttl=lease_ttl))
    await runtime.start()
    return store, runtime, selected


def _query(dsn: str, schema: str, statement: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        cursor = conn.execute(sql.SQL(statement).format(schema=sql.Identifier(schema)), params)
        return cursor.fetchall()


async def _tree(store: PostgresStore) -> tuple[UUID, UUID, UUID, UUID]:
    root, child, grandchild, other = uuid4(), uuid4(), uuid4(), uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", model="m"))
    await store.create(
        SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="Bot", model="m", depth=1)
    )
    await store.create(SessionHeader(id=grandchild.hex, parent_id=child.hex, agent="Bot", model="m", depth=2))
    await store.create(SessionHeader(id=other.hex, root_id=other.hex, agent="Bot", model="m"))
    return root, child, grandchild, other


async def _crash_delete_main(
    dsn: str,
    schema: str,
    root_id: str,
    stage: str,
    evidence: Any,
    hold: Any,
) -> None:
    store = PostgresStore(dsn, schema=schema)
    runtime = Runtime(
        CountingProvider(),
        store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(store, lease_ttl=0.2),
    )
    if stage == "before":
        original_delete = store._delete_tree

        async def gated_delete(*args: Any, **kwargs: Any) -> list[str]:
            before_delete = kwargs.get("before_delete")

            def gate(ids: list[str]) -> None:
                if before_delete is not None:
                    before_delete(ids)
                evidence.send((stage, tuple(ids)))
                hold.recv()

            kwargs["before_delete"] = gate
            return await original_delete(*args, **kwargs)

        store._delete_tree = gated_delete
    else:
        original_finish = runtime._finish_deletion

        async def gated_finish(root: str, ids: list[str], tasks: Any = ()) -> None:
            evidence.send((stage, tuple(ids)))
            hold.recv()
            await original_finish(root, ids, tasks)

        runtime._finish_deletion = gated_finish
    await runtime.start()
    try:
        await runtime.delete(UUID(hex=root_id), allow_active=True)
        await asyncio.Event().wait()
    except BaseException as exc:
        try:
            evidence.send(("error", repr(exc)))
        except BaseException:
            pass
        raise
    finally:
        await runtime.aclose()
        await store.close()


def _crash_delete_worker(*args: Any) -> None:
    asyncio.run(_crash_delete_main(*args))


def _receive_evidence(connection: Any, timeout: float = 5.0) -> tuple[str, tuple[str, ...]]:
    if not connection.poll(timeout):
        raise TimeoutError("deletion worker did not reach its crash gate")
    stage, evidence = connection.recv()
    if stage == "error":
        raise AssertionError(evidence)
    return stage, evidence


def _stop_worker(process: Any, connections: tuple[Any, ...]) -> None:
    if process.is_alive():
        process.kill()
    process.join(5)
    if process.is_alive():
        process.terminate()
        process.join(5)
    for connection in connections:
        connection.close()
    process.close()


def _start_crash_worker(dsn: str, schema: str, root_id: str, stage: str) -> tuple[Any, Any, Any]:
    context = multiprocessing.get_context("spawn")
    evidence_recv, evidence_send = context.Pipe(duplex=False)
    hold_recv, hold_send = context.Pipe(duplex=False)
    process = context.Process(
        target=_crash_delete_worker,
        args=(dsn, schema, root_id, stage, evidence_send, hold_recv),
    )
    process.start()
    evidence_send.close()
    hold_recv.close()
    return process, evidence_recv, hold_send


async def _crash_tree(store: PostgresStore) -> tuple[UUID, UUID]:
    root, child = uuid4(), uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", model="m"))
    await store.create(
        SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="Bot", model="m", depth=1)
    )
    await store.append(root.hex, [InputQueued(command_id=uuid4().hex, input="queued")])
    return root, child


async def test_store_delete_follows_legacy_parents_and_preserves_other_roots(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, child, grandchild, other = await _tree(store)
    try:
        with pytest.raises(TantraError, match="not a root"):
            await store.delete_tree(child.hex)
        assert set(await store.delete_tree(root.hex)) == {root.hex, child.hex, grandchild.hex}
        assert await store.delete_tree(root.hex) == []
        assert await store.header(root.hex) is None
        assert await store.header(child.hex) is None
        assert await store.header(grandchild.hex) is None
        assert await store.header(other.hex) is not None
    finally:
        await store.close()


@pytest.mark.parametrize(
    ("header", "event"),
    (
        ({"status": "queued"}, None),
        ({"status": "running"}, None),
        ({"status": "awaiting_input"}, None),
        ({"current_turn_id": "turn"}, None),
        ({"pending_ask": "ask"}, None),
        ({}, InputQueued(command_id="pending", input="go")),
        ({}, TurnStarted(turn_id="started", input="go")),
        ({}, AskRaised(ask_id="ask", request=FreeText(prompt="continue?"))),
    ),
)
async def test_header_and_indexed_activity_block_delete_until_forced(
    postgres_dsn: str,
    pg_schema: str,
    header: dict[str, Any],
    event: Any,
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    sid = uuid4().hex
    await store.create(SessionHeader(id=sid, root_id=sid, agent="Bot", model="m", **header))
    if event is not None:
        await store.append(sid, [event])
    try:
        with pytest.raises(SessionBusy):
            await store.delete_tree(sid)
        assert await store.delete_tree(sid, allow_active=True) == [sid]
    finally:
        await store.close()


async def test_forced_delete_does_not_decode_journal_bodies(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    sid = uuid4().hex
    await store.create(SessionHeader(id=sid, root_id=sid, agent="Bot", model="m"))
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(
            sql.SQL("INSERT INTO {}.events (session_id, seq, stamped) VALUES (%s, 1, '{{}}'::jsonb)").format(
                sql.Identifier(pg_schema)
            ),
            (sid,),
        )
    try:
        assert await store.delete_tree(sid, allow_active=True) == [sid]
        assert _query(postgres_dsn, pg_schema, "SELECT count(*) FROM {schema}.events") == [(0,)]
    finally:
        await store.close()


async def test_coordinated_delete_purges_tree_transport_history_and_retains_only_fences(
    postgres_dsn: str, pg_schema: str
) -> None:
    store, runtime, provider = await _runtime(postgres_dsn, pg_schema)
    root, child, grandchild, other = await _tree(store)
    await store.append(root.hex, [InputQueued(command_id="pending", input="go")])
    await store.append(child.hex, [TurnStarted(turn_id="child-turn", input="go")])
    await store.append(
        grandchild.hex,
        [CancellationRequested(command_id="cancel", targets={child.hex: ["child-turn"]})],
    )
    assert runtime.coordinator is not None
    ownership = await runtime.coordinator.acquire(root.hex)
    assert ownership is not None
    historical = uuid4()
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(
            sql.SQL("INSERT INTO {}.coordinator_activity (root_id, actor_id, active) VALUES (%s, %s, false)").format(
                sql.Identifier(pg_schema)
            ),
            (root.hex, child.hex),
        )
        conn.execute(
            sql.SQL(
                "INSERT INTO {}.coordinator_requests "
                "(request_id, root_id, destination_instance, destination_generation, "
                "envelope, reply, deadline, completed_at) "
                "VALUES (%s, %s, %s, %s, '{{}}'::jsonb, '{{}}'::jsonb, "
                "clock_timestamp(), clock_timestamp())"
            ).format(sql.Identifier(pg_schema)),
            (historical, root.hex, str(ownership.instance_id), ownership.generation),
        )
    ids = (root.hex, child.hex, grandchild.hex)
    try:
        assert await runtime.delete(root, allow_active=True)
        assert provider.calls == 0
        for table, column in (
            ("sessions", "id"),
            ("events", "session_id"),
            ("journal_index", "actor_id"),
        ):
            assert _query(
                postgres_dsn,
                pg_schema,
                f"SELECT count(*) FROM {{schema}}.{table} WHERE {column} = ANY(%s::text[])",
                (list(ids),),
            ) == [(0,)]
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.cancellation_targets "
            "WHERE source_actor_id = ANY(%s::text[]) OR actor_id = ANY(%s::text[])",
            (list(ids), list(ids)),
        ) == [(0,)]
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.coordinator_activity WHERE root_id = %s",
            (root.hex,),
        ) == [(0,)]
        requests = _query(
            postgres_dsn,
            pg_schema,
            "SELECT request_id, envelope->>'operation', reply->'result'->>'deleted' "
            "FROM {schema}.coordinator_requests WHERE root_id = %s",
            (root.hex,),
        )
        assert len(requests) == 1
        assert requests[0][0] != historical
        assert requests[0][1:] == ("delete", "true")
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT kind, actor_id, seq FROM {schema}.coordinator_changes WHERE root_id = %s",
            (root.hex,),
        ) == [("request", None, None), ("deleted", None, None)]
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT owner_instance, expires_at, writer_connection, recovery FROM {schema}.coordinator_roots "
            "WHERE root_id = %s",
            (root.hex,),
        ) == [(None, None, None, {})]
        assert set(
            _query(
                postgres_dsn,
                pg_schema,
                "SELECT actor_id, root_id FROM {schema}.deleted_sessions WHERE root_id = %s",
                (root.hex,),
            )
        ) == {(sid, root.hex) for sid in ids}
        assert await store.header(other.hex) is not None
    finally:
        await runtime.aclose()
        await store.close()


async def test_remote_delete_revokes_writer_and_ends_stream_and_result_wait(postgres_dsn: str, pg_schema: str) -> None:
    owner_store, owner, _ = await _runtime(postgres_dsn, pg_schema)
    root = await owner.create(Bot)
    reader_store, reader, _ = await _runtime(postgres_dsn, pg_schema)
    connection = owner.connect(root, writable=True)
    await connection.__aenter__()
    last_seq = (await reader.status(root)).last_seq
    stream = asyncio.create_task(anext(reader.events(root, after=last_seq)))
    result = asyncio.create_task(reader._wait_result(root.hex, uuid4()))
    try:
        await asyncio.sleep(0)
        assert reader.coordinator is not None
        listener = reader.coordinator._listener_conn
        if listener is not None:
            await listener.close()
            reader.coordinator._listener_conn = None
        assert await reader.delete(root)
        for _ in range(100):
            try:
                connection._check_iteration()
            except SessionNotFound:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("remote writer was not revoked")
        with pytest.raises(SessionNotFound):
            await asyncio.wait_for(stream, 1)
        with pytest.raises(SessionNotFound):
            await asyncio.wait_for(result, 1)
    finally:
        for task in (stream, result):
            if not task.done():
                task.cancel()
        await asyncio.gather(stream, result, return_exceptions=True)
        await connection.__aexit__(None, None, None)
        await reader.aclose()
        await reader_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_deletion_marker_survives_transport_cleanup_and_denies_uuid_reuse(
    postgres_dsn: str, pg_schema: str
) -> None:
    store, runtime, _ = await _runtime(postgres_dsn, pg_schema)
    root = await runtime.create(Bot)
    try:
        assert await runtime.delete(root)
        with psycopg.connect(postgres_dsn) as conn:
            for table in ("coordinator_requests", "coordinator_changes", "coordinator_roots"):
                conn.execute(
                    sql.SQL("DELETE FROM {}.{} WHERE root_id = %s").format(
                        sql.Identifier(pg_schema), sql.Identifier(table)
                    ),
                    (root.hex,),
                )
        with pytest.raises(SessionExists):
            await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", model="m"))
        with pytest.raises(SessionNotFound):
            await store.create(
                SessionHeader(id=uuid4().hex, root_id=root.hex, parent_id=root.hex, agent="Bot", model="m")
            )
        assert runtime.coordinator is not None
        with pytest.raises(SessionNotFound):
            await runtime.coordinator.acquire(root.hex)
    finally:
        await runtime.aclose()
        await store.close()


async def test_purge_failure_rolls_back_tree_journal_and_markers(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, child, _, _ = await _tree(store)
    await store.append(child.hex, [TextPart(sample_id="sample", text="kept")])
    function_name = f"fail_delete_{uuid4().hex}"
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(
            sql.SQL(
                "CREATE FUNCTION {}.{}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'stop'; END $$"
            ).format(sql.Identifier(pg_schema), sql.Identifier(function_name))
        )
        conn.execute(
            sql.SQL(
                "CREATE TRIGGER fail_delete BEFORE DELETE ON {}.sessions FOR EACH ROW "
                "WHEN (OLD.id = {}) EXECUTE FUNCTION {}.{}()"
            ).format(
                sql.Identifier(pg_schema),
                sql.Literal(child.hex),
                sql.Identifier(pg_schema),
                sql.Identifier(function_name),
            )
        )
    try:
        with pytest.raises(psycopg.Error, match="stop"):
            await store.delete_tree(root.hex)
        assert await store.header(root.hex) is not None
        assert await store.header(child.hex) is not None
        assert [item.event async for item in store.read(child.hex)] == [TextPart(sample_id="sample", text="kept")]
        assert _query(postgres_dsn, pg_schema, "SELECT count(*) FROM {schema}.deleted_sessions") == [(0,)]
    finally:
        await store.close()


async def test_lost_committed_delete_reply_is_safe_to_retry(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, runtime, _ = await _runtime(postgres_dsn, pg_schema)
    root = await runtime.create(Bot)
    assert runtime.coordinator is not None
    original = runtime.coordinator.request
    lost = False

    async def lose_reply(envelope: Any) -> Any:
        nonlocal lost
        reply = await original(envelope)
        if envelope.operation == "delete" and not lost:
            lost = True
            raise CommandTimeout("lost committed reply")
        return reply

    monkeypatch.setattr(runtime.coordinator, "request", lose_reply)
    try:
        with pytest.raises(CommandTimeout):
            await runtime.delete(root)
        assert lost
        monkeypatch.setattr(runtime.coordinator, "request", original)
        assert not await runtime.delete(root)
        assert await store.is_deleted(root.hex)
    finally:
        await runtime.aclose()
        await store.close()


async def test_cancelled_delete_caller_does_not_cancel_committed_cleanup(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, runtime, _ = await _runtime(postgres_dsn, pg_schema)
    root = await runtime.create(Bot)
    started = asyncio.Event()
    release = asyncio.Event()
    original = store._delete_tree

    async def gated(*args: Any, **kwargs: Any) -> list[str]:
        started.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(store, "_delete_tree", gated)
    task = asyncio.create_task(runtime.delete(root))
    try:
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        for _ in range(100):
            if await store.is_deleted(root.hex):
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("shielded deletion did not finish")
        assert await store.header(root.hex) is None
    finally:
        release.set()
        await runtime.aclose()
        await store.close()


async def test_delete_takes_over_expired_owner_without_recovery_or_model_calls(
    postgres_dsn: str, pg_schema: str
) -> None:
    owner_store, owner, owner_provider = await _runtime(postgres_dsn, pg_schema, lease_ttl=0.1)
    root = await owner.create(Bot)
    assert owner.coordinator is not None
    ownership = await owner.coordinator.acquire(root.hex)
    assert ownership is not None
    await owner.coordinator.close()
    await asyncio.sleep(0.12)
    taker_store, taker, taker_provider = await _runtime(postgres_dsn, pg_schema, lease_ttl=0.1)
    try:
        assert await taker.delete(root)
        assert owner_provider.calls == 0
        assert taker_provider.calls == 0
    finally:
        await taker.aclose()
        await taker_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_concurrent_send_and_forced_delete_leave_no_late_journal(postgres_dsn: str, pg_schema: str) -> None:
    owner_store, owner, _ = await _runtime(postgres_dsn, pg_schema)
    root = await owner.create(Bot)
    sender_store, sender, _ = await _runtime(postgres_dsn, pg_schema)
    connection = sender.connect(root, writable=True)
    await connection.__aenter__()
    send = asyncio.create_task(connection.send("race", command_id=uuid4()))
    delete = asyncio.create_task(sender.delete(root, allow_active=True))
    try:
        send_result, delete_result = await asyncio.gather(send, delete, return_exceptions=True)
        assert delete_result is True
        if isinstance(send_result, BaseException):
            assert isinstance(send_result, (SessionNotFound, WriterReplaced, TantraError))
        assert await sender_store.header(root.hex) is None
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.events WHERE session_id = %s",
            (root.hex,),
        ) == [(0,)]
    finally:
        await connection.__aexit__(None, None, None)
        await sender.aclose()
        await sender_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_delete_retries_stale_owner_observation(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_store, owner, _ = await _runtime(postgres_dsn, pg_schema)
    sender_store, sender, _ = await _runtime(postgres_dsn, pg_schema)
    root = await owner.create(Bot)
    assert owner.coordinator is not None
    assert sender.coordinator is not None
    assert await owner.coordinator.acquire(root.hex) is not None
    request = sender.coordinator.request
    calls = 0

    async def stale_once(envelope: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise CoordinatorUnavailable(f"root {root.hex} has no owner")
        return await request(envelope)

    monkeypatch.setattr(sender.coordinator, "request", stale_once)
    try:
        assert await sender.delete(root)
        assert calls == 2
        assert await sender_store.header(root.hex) is None
    finally:
        await sender.aclose()
        await sender_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_concurrent_delete_returns_false_after_other_runtime_commits(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_store, owner, _ = await _runtime(postgres_dsn, pg_schema)
    caller_store, caller, _ = await _runtime(postgres_dsn, pg_schema)
    root = await owner.create(Bot)
    original = caller_store.header
    ready, release = asyncio.Event(), asyncio.Event()
    first = True

    async def gated_header(sid: str) -> SessionHeader | None:
        nonlocal first
        header = await original(sid)
        if first and sid == root.hex:
            first = False
            ready.set()
            await release.wait()
        return header

    monkeypatch.setattr(caller_store, "header", gated_header)
    deletion = asyncio.create_task(caller.delete(root))
    try:
        await asyncio.wait_for(ready.wait(), 1)
        assert await owner.delete(root)
        release.set()
        assert await asyncio.wait_for(deletion, 2) is False
    finally:
        release.set()
        deletion.cancel()
        await asyncio.gather(deletion, return_exceptions=True)
        await caller.aclose()
        await caller_store.close()
        await owner.aclose()
        await owner_store.close()


@pytest.mark.parametrize("state", ("queued", "running", "awaiting_input"))
async def test_busy_delete_relinquishes_delete_only_ownership_without_recovery(
    postgres_dsn: str, pg_schema: str, state: str
) -> None:
    store, runtime, provider = await _runtime(postgres_dsn, pg_schema)
    root = uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", model="m", status=state))
    await store.append(root.hex, [InputQueued(command_id=uuid4().hex, input="retained")])
    assert runtime.coordinator is not None
    try:
        with pytest.raises(SessionBusy):
            await runtime.delete(root)
        assert await runtime.coordinator.locate(root.hex) is None
        assert root.hex not in runtime._ownerships
        assert root.hex not in runtime._unrecovered
        assert root.hex not in runtime._renewals
        assert await store.header(root.hex) is not None
        assert provider.calls == 0
        assert await runtime.delete(root, allow_active=True)
        assert provider.calls == 0
    finally:
        await runtime.aclose()
        await store.close()


async def test_failed_delete_relinquishes_delete_only_ownership_and_keeps_queued_work(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, runtime, provider = await _runtime(postgres_dsn, pg_schema)
    root = uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", model="m"))
    event = InputQueued(command_id=uuid4().hex, input="retained")
    await store.append(root.hex, [event])
    begin = runtime._begin_deletion

    def rollback(*args: Any) -> list[asyncio.Task[None]]:
        begin(*args)
        raise RuntimeError("rollback")

    monkeypatch.setattr(runtime, "_begin_deletion", rollback)
    assert runtime.coordinator is not None
    try:
        with pytest.raises(CoordinatorUnavailable):
            await runtime.delete(root, allow_active=True)
        assert await runtime.coordinator.locate(root.hex) is None
        assert root.hex not in runtime._ownerships
        assert root.hex not in runtime._unrecovered
        assert root.hex not in runtime._renewals
        assert root.hex not in runtime._deleting
        assert await store.header(root.hex) is not None
        assert [item.event async for item in store.read(root.hex)] == [event]
        assert provider.calls == 0
    finally:
        await runtime.aclose()
        await store.close()


async def test_forced_delete_rollback_interrupts_running_work_and_preserves_writer(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = GatedProvider()
    store, runtime, _ = await _runtime(postgres_dsn, pg_schema, provider=provider)
    root = await runtime.create(Bot)
    connection = runtime.connect(root, writable=True)
    await connection.__aenter__()
    prompt = asyncio.create_task(connection.prompt("running", command_id=uuid4()))
    begin = runtime._begin_deletion

    def rollback(*args: Any) -> list[asyncio.Task[None]]:
        begin(*args)
        raise RuntimeError("rollback")

    try:
        await asyncio.wait_for(provider.started.wait(), 2)
        with monkeypatch.context() as patch:
            patch.setattr(runtime, "_begin_deletion", rollback)
            with pytest.raises(CoordinatorUnavailable):
                await runtime.delete(root, allow_active=True)
        result = await asyncio.wait_for(prompt, 2)
        assert result.outcome == "interrupted"
        assert await store.header(root.hex) is not None
        assert not await store.is_deleted(root.hex)
        assert root.hex not in runtime._deleting
        assert provider.calls == 1
        provider.release.set()
        assert (await connection.prompt("still editable", command_id=uuid4())).text == "ok"
        assert provider.calls == 2
    finally:
        provider.release.set()
        prompt.cancel()
        await asyncio.gather(prompt, return_exceptions=True)
        await connection.__aexit__(None, None, None)
        await runtime.aclose()
        await store.close()


async def test_owner_sigkill_before_delete_commit_rolls_back_and_replacement_force_deletes(
    postgres_dsn: str, pg_schema: str
) -> None:
    seed = PostgresStore(postgres_dsn, schema=pg_schema)
    await seed.setup()
    root, child = await _crash_tree(seed)
    await seed.close()
    process, evidence, hold = _start_crash_worker(postgres_dsn, pg_schema, root.hex, "before")
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        stage, ids = await asyncio.to_thread(_receive_evidence, evidence)
        assert stage == "before"
        assert set(ids) == {root.hex, child.hex}
        process.kill()
        process.join(5)
        assert not process.is_alive()
        assert process.exitcode is not None and process.exitcode < 0
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT id FROM {schema}.sessions WHERE id = ANY(%s::text[]) ORDER BY id",
            ([root.hex, child.hex],),
        ) == [(sid,) for sid in sorted((root.hex, child.hex))]
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.events WHERE session_id = %s",
            (root.hex,),
        ) == [(1,)]
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.deleted_sessions WHERE root_id = %s",
            (root.hex,),
        ) == [(0,)]
        await asyncio.sleep(0.25)
        replacement_store, replacement, provider = await _runtime(postgres_dsn, pg_schema, lease_ttl=0.2)
        with pytest.raises(SessionBusy):
            await replacement.delete(root)
        assert await replacement.delete(root, allow_active=True)
        assert provider.calls == 0
        assert set(
            _query(
                postgres_dsn,
                pg_schema,
                "SELECT actor_id FROM {schema}.deleted_sessions WHERE root_id = %s",
                (root.hex,),
            )
        ) == {(root.hex,), (child.hex,)}
    finally:
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        _stop_worker(process, (evidence, hold))


async def test_owner_sigkill_after_delete_commit_keeps_absence_and_permanent_marker(
    postgres_dsn: str, pg_schema: str
) -> None:
    seed = PostgresStore(postgres_dsn, schema=pg_schema)
    await seed.setup()
    root, child = await _crash_tree(seed)
    await seed.close()
    process, evidence, hold = _start_crash_worker(postgres_dsn, pg_schema, root.hex, "after")
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        stage, ids = await asyncio.to_thread(_receive_evidence, evidence)
        assert stage == "after"
        assert set(ids) == {root.hex, child.hex}
        process.kill()
        process.join(5)
        assert not process.is_alive()
        assert process.exitcode is not None and process.exitcode < 0
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.sessions WHERE id = ANY(%s::text[])",
            ([root.hex, child.hex],),
        ) == [(0,)]
        assert set(
            _query(
                postgres_dsn,
                pg_schema,
                "SELECT actor_id FROM {schema}.deleted_sessions WHERE root_id = %s",
                (root.hex,),
            )
        ) == {(root.hex,), (child.hex,)}
        replacement_store, replacement, provider = await _runtime(postgres_dsn, pg_schema, lease_ttl=0.2)
        assert not await replacement.delete(root)
        assert provider.calls == 0
        with pytest.raises(SessionExists):
            await replacement_store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", model="m"))
        with pytest.raises(SessionExists):
            await replacement_store.create(SessionHeader(id=child.hex, root_id=child.hex, agent="Bot", model="m"))
    finally:
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        _stop_worker(process, (evidence, hold))


async def test_pending_mutating_transport_request_blocks_default_delete(postgres_dsn: str, pg_schema: str) -> None:
    store, runtime, provider = await _runtime(postgres_dsn, pg_schema)
    root = await runtime.create(Bot)
    assert runtime.coordinator is not None
    ownership = await runtime.coordinator.acquire(root.hex)
    assert ownership is not None
    request_id = uuid4()
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(
            sql.SQL(
                "INSERT INTO {}.coordinator_requests "
                "(request_id, root_id, destination_instance, destination_generation, envelope, deadline) "
                "VALUES (%s, %s, %s, %s, %s::jsonb, clock_timestamp() + interval '1 minute')"
            ).format(sql.Identifier(pg_schema)),
            (
                request_id,
                root.hex,
                str(uuid4()),
                ownership.generation,
                json.dumps({"operation": "send"}),
            ),
        )
    try:
        with pytest.raises(SessionBusy):
            await runtime.delete(root)
        assert await runtime.delete(root, allow_active=True)
        assert provider.calls == 0
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT count(*) FROM {schema}.coordinator_requests WHERE request_id = %s",
            (request_id,),
        ) == [(0,)]
    finally:
        await runtime.aclose()
        await store.close()


async def test_stale_operational_watermark_blocks_default_delete(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    sid = uuid4().hex
    await store.create(SessionHeader(id=sid, root_id=sid, agent="Bot", model="m"))
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(
            sql.SQL("UPDATE {}.sessions SET operational_seq = -1 WHERE id = %s").format(sql.Identifier(pg_schema)),
            (sid,),
        )
    try:
        with pytest.raises(SessionBusy):
            await store.delete_tree(sid)
        assert await store.delete_tree(sid, allow_active=True) == [sid]
    finally:
        await store.close()
