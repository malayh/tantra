from __future__ import annotations

import asyncio
import base64
import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tantra.ask import FreeTextResponse
from tantra.errors import CorruptLog, InvalidCommandReuse
from tantra.events import (
    AgentFinished,
    AskAnswered,
    CancellationRequested,
    InputQueued,
    SessionHeader,
    Stamped,
    TextDelta,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
)
from tantra.stores.postgres import MIGRATIONS, PostgresStore

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb

CONTENDERS = 4


async def _store(dsn: str, schema: str) -> PostgresStore:
    store = PostgresStore(dsn, schema=schema)
    await store.setup()
    return store


def _query(dsn: str, schema: str, statement: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        cursor = conn.execute(sql.SQL(statement).format(schema=sql.Identifier(schema)), params)
        return cursor.fetchall()


def _execute(dsn: str, schema: str, statement: str, params: tuple[Any, ...] = ()) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL(statement).format(schema=sql.Identifier(schema)), params)


def _current(stamped: Stamped) -> Jsonb:
    payload = base64.b64encode(stamped.model_dump_json().encode()).decode()
    return Jsonb({"tantra_stamped": {"version": 1, "payload": payload}})


def _legacy(stamped: Stamped) -> Jsonb:
    return Jsonb(stamped.model_dump(mode="json"))


def _install_v4(dsn: str, schema: str) -> None:
    ident = sql.Identifier(schema)
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(ident))
        conn.execute(sql.SQL("CREATE TABLE {}.schema_version (version int NOT NULL)").format(ident))
        for version, statements in enumerate(MIGRATIONS[:4], start=1):
            for statement in statements:
                conn.execute(sql.SQL(statement).format(schema=ident))
            conn.execute(sql.SQL("INSERT INTO {}.schema_version (version) VALUES (%s)").format(ident), (version,))


def _insert_session(dsn: str, schema: str, header: SessionHeader, rows: list[tuple[int, Jsonb]]) -> None:
    ident = sql.Identifier(schema)
    stored = header.model_copy(update={"last_seq": len(rows)})
    with psycopg.connect(dsn) as conn:
        conn.execute(
            sql.SQL(
                "INSERT INTO {}.sessions (id, header, metadata, parent_id, created_at, last_seq)"
                " VALUES (%s, %s, %s, %s, %s, %s)"
            ).format(ident),
            (
                stored.id,
                Jsonb(stored.model_dump(mode="json")),
                Jsonb(stored.metadata),
                stored.parent_id,
                stored.created_at,
                stored.last_seq,
            ),
        )
        with conn.cursor() as cursor:
            cursor.executemany(
                sql.SQL("INSERT INTO {}.events (session_id, seq, stamped) VALUES (%s, %s, %s)").format(ident),
                [(stored.id, seq, raw) for seq, raw in rows],
            )


def _enqueue_in_thread(
    dsn: str,
    schema: str,
    sid: str,
    event: InputQueued,
    barrier: threading.Barrier,
) -> tuple[int, bool]:
    async def attempt() -> tuple[int, bool]:
        store = PostgresStore(dsn, schema=schema)
        try:
            barrier.wait(timeout=30)
            result = await store.enqueue(sid, event)
            return result.seq, result.duplicate
        finally:
            await store.close()

    return asyncio.run(attempt())


async def test_projection_selects_lifecycle_events_and_returns_original_typed_payloads(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(root)
    command = uuid.uuid4().hex
    answer = uuid.uuid4().hex
    cancellation = uuid.uuid4().hex
    failed = uuid.uuid4().hex
    cancelled = uuid.uuid4().hex
    interrupted = uuid.uuid4().hex
    events = [
        InputQueued(command_id=command, input="left\x00right", vendor={"trace": 7}),
        AskAnswered(
            ask_id=uuid.uuid4().hex,
            response=FreeTextResponse(text="yes\x00please", vendor="answer"),
            command_id=answer,
            answered_by=root.id,
            vendor="event",
        ),
        AskAnswered(
            ask_id=uuid.uuid4().hex,
            response=FreeTextResponse(text="untracked"),
            command_id=None,
        ),
        CancellationRequested(command_id=cancellation, targets={root.id: [command]}),
        TurnStarted(turn_id=command, input="left\x00right"),
        TurnCompleted(turn_id=command, stop_reason="completed"),
        TurnFailed(turn_id=failed, error="boom"),
        TurnCancelled(turn_id=cancelled),
        TurnInterrupted(turn_id=interrupted, reason="restart"),
        AgentFinished(result={"value": 1}),
        TextDelta(text="ignored"),
        AgentFinished(result={"value": 2}),
    ]

    await store.append(root.id, events)

    rows = _query(
        postgres_dsn,
        pg_schema,
        "SELECT seq, event_type, command_id, turn_id FROM {schema}.journal_index WHERE actor_id = %s ORDER BY seq",
        (root.id,),
    )
    assert rows == [
        (1, "input_queued", command.encode(), None),
        (2, "ask_answered", answer.encode(), None),
        (4, "cancellation_requested", cancellation.encode(), None),
        (5, "turn_started", None, command.encode()),
        (6, "turn_completed", None, command.encode()),
        (7, "turn_failed", None, failed.encode()),
        (8, "turn_cancelled", None, cancelled.encode()),
        (9, "turn_interrupted", None, interrupted.encode()),
        (10, "agent_finished", None, None),
        (12, "agent_finished", None, None),
    ]
    assert await store.lookup_command(root.id, command) == (root.id, Stamped(seq=1, event=events[0]))
    assert await store.lookup_command(root.id, answer) == (root.id, Stamped(seq=2, event=events[1]))
    assert await store.lookup_finished(root.id) == Stamped(seq=12, event=events[11])


async def test_command_lookup_preserves_runtime_tree_order_and_independent_root_reuse(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    created = datetime.now(UTC)
    root = SessionHeader(id=uuid.uuid4().hex, agent="root", created_at=created)
    early = SessionHeader(
        id="1" * 32,
        root_id=root.id,
        parent_id=root.id,
        depth=1,
        agent="early",
        created_at=created + timedelta(seconds=1),
    )
    late = SessionHeader(
        id="2" * 32,
        root_id=root.id,
        parent_id=root.id,
        depth=1,
        agent="late",
        created_at=created + timedelta(seconds=1),
    )
    older = SessionHeader(
        id="3" * 32,
        root_id=root.id,
        parent_id=root.id,
        depth=1,
        agent="older",
        created_at=created + timedelta(milliseconds=500),
    )
    grandchild = SessionHeader(
        id="0" * 32,
        root_id=root.id,
        parent_id=early.id,
        depth=2,
        agent="grandchild",
        created_at=created,
    )
    other = SessionHeader(id=uuid.uuid4().hex, agent="other")
    for header in (root, late, grandchild, early, older, other):
        await store.create(header)
    command = uuid.uuid4().hex
    tied = uuid.uuid4().hex
    early_first = InputQueued(command_id=command, input="early-first")
    early_second = InputQueued(command_id=command, input="early-second")
    tied_early = InputQueued(command_id=tied, input="tied-early")
    await store.append(
        late.id,
        [InputQueued(command_id=command, input="late"), InputQueued(command_id=tied, input="tied-late")],
    )
    await store.append(grandchild.id, [InputQueued(command_id=command, input="grandchild")])
    await store.append(early.id, [early_first, early_second, tied_early])
    older_event = InputQueued(command_id=command, input="older")
    await store.append(older.id, [older_event])
    other_event = InputQueued(command_id=command, input="other-root")
    await store.append(other.id, [other_event])

    assert await store.lookup_command(root.id, command) == (older.id, Stamped(seq=1, event=older_event))
    assert await store.lookup_command(root.id, tied) == (early.id, Stamped(seq=3, event=tied_early))
    assert await store.lookup_command(other.id, command) == (other.id, Stamped(seq=1, event=other_event))

    root_event = InputQueued(command_id=command, input="root")
    await store.append(root.id, [TextDelta(text="before"), root_event])

    assert await store.lookup_command(root.id, command) == (root.id, Stamped(seq=2, event=root_event))


async def test_enqueue_deduplicates_only_inputs_on_the_same_actor(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    first = SessionHeader(id=uuid.uuid4().hex, agent="first")
    second = SessionHeader(id=uuid.uuid4().hex, agent="second")
    await store.create(first)
    await store.create(second)
    command = uuid.uuid4().hex
    await store.append(first.id, [CancellationRequested(command_id=command)])
    queued = InputQueued(command_id=command, input="accepted")

    accepted = await store.enqueue(first.id, queued)
    other_actor = await store.enqueue(second.id, queued)
    duplicate = await store.enqueue(first.id, queued.model_copy(deep=True))

    assert (accepted.seq, accepted.duplicate) == (2, False)
    assert (other_actor.seq, other_actor.duplicate) == (1, False)
    assert (duplicate.seq, duplicate.duplicate) == (2, True)
    with pytest.raises(InvalidCommandReuse):
        await store.enqueue(first.id, InputQueued(command_id=command, input="changed"))


async def test_generic_append_keeps_duplicate_commands_and_lookup_returns_the_first(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(root)
    command = uuid.uuid4().hex
    first = InputQueued(command_id=command, input="first")
    second = InputQueued(command_id=command, input="second")

    await store.append(root.id, [first, second])

    assert [item.event async for item in store.read(root.id)] == [first, second]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT seq FROM {schema}.journal_index WHERE actor_id = %s AND command_id = %s ORDER BY seq",
        (root.id, command.encode()),
    ) == [(1,), (2,)]
    assert await store.lookup_command(root.id, command) == (root.id, Stamped(seq=1, event=first))


async def test_projection_failure_rolls_back_append_enqueue_and_header_updates(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    appended = SessionHeader(id=uuid.uuid4().hex, agent="append")
    enqueued = SessionHeader(id=uuid.uuid4().hex, agent="enqueue")
    await store.create(appended)
    await store.create(enqueued)
    _execute(
        postgres_dsn,
        pg_schema,
        "CREATE FUNCTION {schema}.reject_journal_index() RETURNS trigger AS $$"
        " BEGIN RAISE EXCEPTION 'injected projection failure'; END;"
        " $$ LANGUAGE plpgsql",
    )
    _execute(
        postgres_dsn,
        pg_schema,
        "CREATE TRIGGER reject_journal_index BEFORE INSERT ON {schema}.journal_index"
        " FOR EACH ROW EXECUTE FUNCTION {schema}.reject_journal_index()",
    )
    append_event = InputQueued(command_id=uuid.uuid4().hex, input="append")
    enqueue_event = InputQueued(command_id=uuid.uuid4().hex, input="enqueue")

    with pytest.raises(psycopg.Error, match="injected projection failure"):
        await store.append(appended.id, [append_event])
    with pytest.raises(psycopg.Error, match="injected projection failure"):
        await store.enqueue(enqueued.id, enqueue_event)

    for header in (appended, enqueued):
        loaded = await store.header(header.id)
        assert loaded is not None
        assert (loaded.last_seq, loaded.status) == (0, "idle")
        assert await store.read_page(header.id) == []
    assert _query(postgres_dsn, pg_schema, "SELECT actor_id FROM {schema}.journal_index") == []

    _execute(postgres_dsn, pg_schema, "DROP TRIGGER reject_journal_index ON {schema}.journal_index")
    _execute(postgres_dsn, pg_schema, "DROP FUNCTION {schema}.reject_journal_index()")

    assert await store.append(appended.id, [append_event]) == 1
    accepted = await store.enqueue(enqueued.id, enqueue_event)
    assert (accepted.seq, accepted.duplicate) == (1, False)


async def test_concurrent_enqueue_commits_one_input_and_one_projection_row(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(root)
    event = InputQueued(command_id=uuid.uuid4().hex, input="once")
    barrier = threading.Barrier(CONTENDERS)

    results = await asyncio.gather(
        *(
            asyncio.to_thread(_enqueue_in_thread, postgres_dsn, pg_schema, root.id, event, barrier)
            for _ in range(CONTENDERS)
        )
    )

    assert sum(not duplicate for _, duplicate in results) == 1
    assert {seq for seq, _ in results} == {1}
    assert await store.read_page(root.id) == [Stamped(seq=1, event=event)]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT actor_id, seq FROM {schema}.journal_index WHERE actor_id = %s",
        (root.id,),
    ) == [(root.id, 1)]


async def test_migration_backfills_legacy_and_current_pages_and_setup_is_idempotent(
    postgres_dsn: str, pg_schema: str
) -> None:
    _install_v4(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    command = uuid.uuid4().hex + "\x00id"
    queued = InputQueued(command_id=command, input="left\x00right", vendor={"source": "current"})
    answer_command = uuid.uuid4().hex
    answered = AskAnswered(
        ask_id=uuid.uuid4().hex,
        response=FreeTextResponse(text="legacy"),
        command_id=answer_command,
        answered_by=root.id,
    )
    rows = [(1, _current(Stamped(seq=1, event=queued)))]
    rows.extend((seq, _legacy(Stamped(seq=seq, event=TextDelta(text=f"ignored-{seq}")))) for seq in range(2, 1001))
    rows.extend(
        [
            (1001, _legacy(Stamped(seq=1001, event=answered))),
            (1002, _current(Stamped(seq=1002, event=TurnStarted(turn_id=command, input="left\x00right")))),
            (1003, _current(Stamped(seq=1003, event=AgentFinished(result="done")))),
        ]
    )
    _insert_session(postgres_dsn, pg_schema, root, rows)
    original_rows = _query(
        postgres_dsn,
        pg_schema,
        "SELECT seq, stamped FROM {schema}.events WHERE session_id = %s ORDER BY seq",
        (root.id,),
    )
    store = PostgresStore(postgres_dsn, schema=pg_schema)

    await store.setup()
    await store.setup()
    await PostgresStore(postgres_dsn, schema=pg_schema).setup()

    assert original_rows == _query(
        postgres_dsn,
        pg_schema,
        "SELECT seq, stamped FROM {schema}.events WHERE session_id = %s ORDER BY seq",
        (root.id,),
    )

    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT version FROM {schema}.schema_version ORDER BY version",
    ) == [(1,), (2,), (3,), (4,), (5,), (6,), (7,)]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT seq, event_type FROM {schema}.journal_index WHERE actor_id = %s ORDER BY seq",
        (root.id,),
    ) == [(1, "input_queued"), (1001, "ask_answered"), (1002, "turn_started"), (1003, "agent_finished")]
    assert await store.lookup_command(root.id, command) == (root.id, Stamped(seq=1, event=queued))
    assert await store.lookup_command(root.id, answer_command) == (root.id, Stamped(seq=1001, event=answered))
    assert await store.lookup_finished(root.id) == Stamped(seq=1003, event=AgentFinished(result="done"))
    async with store._connection() as conn:
        assert not await store._input_pending(conn, root.id, command)


async def test_nul_command_identifiers_preserve_append_and_deduplication(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(root)
    event = InputQueued(command_id="command\x00雪", input="hello\x00")
    await store.append(root.id, [event])
    assert await store.lookup_command(root.id, event.command_id) == (root.id, Stamped(seq=1, event=event))
    assert (await store.enqueue(root.id, event)).duplicate
    async with store._connection() as conn:
        assert await store._input_pending(conn, root.id, event.command_id)


async def test_corrupt_backfill_rolls_back_the_migration_and_restart_succeeds(
    postgres_dsn: str, pg_schema: str
) -> None:
    _install_v4(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    _insert_session(postgres_dsn, pg_schema, root, [(1, Jsonb({"seq": 1}))])
    store = PostgresStore(postgres_dsn, schema=pg_schema)

    with pytest.raises(CorruptLog):
        await store.setup()

    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT version FROM {schema}.schema_version ORDER BY version",
    ) == [(1,), (2,), (3,), (4,)]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT to_regclass(%s)",
        (f"{pg_schema}.journal_index",),
    ) == [(None,)]

    event = InputQueued(command_id=uuid.uuid4().hex, input="repaired")
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.events SET stamped = %s WHERE session_id = %s AND seq = 1",
        (_current(Stamped(seq=1, event=event)), root.id),
    )
    await store.setup()

    assert await store.lookup_command(root.id, event.command_id) == (root.id, Stamped(seq=1, event=event))
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT version FROM {schema}.schema_version ORDER BY version",
    )[-1] == (7,)


async def test_interrupted_backfill_rolls_back_the_migration_and_restart_succeeds(
    postgres_dsn: str, pg_schema: str
) -> None:
    _install_v4(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="build")
    event = InputQueued(command_id=uuid.uuid4().hex, input="retry")
    _insert_session(postgres_dsn, pg_schema, root, [(1, _legacy(Stamped(seq=1, event=event)))])

    class InterruptedStore(PostgresStore):
        async def _backfill_journal_index(self, conn: Any) -> None:
            await super()._backfill_journal_index(conn)
            raise RuntimeError("migration interrupted")

    interrupted = InterruptedStore(postgres_dsn, schema=pg_schema)
    with pytest.raises(RuntimeError, match="migration interrupted"):
        await interrupted.setup()

    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT version FROM {schema}.schema_version ORDER BY version",
    ) == [(1,), (2,), (3,), (4,)]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT to_regclass(%s)",
        (f"{pg_schema}.journal_index",),
    ) == [(None,)]

    restarted = PostgresStore(postgres_dsn, schema=pg_schema)
    await restarted.setup()

    assert await restarted.lookup_command(root.id, event.command_id) == (root.id, Stamped(seq=1, event=event))
