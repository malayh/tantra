from __future__ import annotations

import base64
import random
import uuid
from typing import Any

import pytest

from tantra import PostgresCoordinator
from tantra.errors import CorruptLog
from tantra.events import (
    AgentFinished,
    CancellationRequested,
    CompactionApplied,
    InputQueued,
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextDelta,
    TextPart,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    Usage,
)
from tantra.runtime import _turn_result
from tantra.stores import postgres
from tantra.stores.base import reduce_journal
from tantra.stores.postgres import MIGRATIONS, PostgresStore

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb


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


def _id(label: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_URL, label).hex


def _install_v5(dsn: str, schema: str) -> None:
    ident = sql.Identifier(schema)
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(ident))
        conn.execute(sql.SQL("CREATE TABLE {}.schema_version (version int NOT NULL)").format(ident))
        for version, statements in enumerate(MIGRATIONS[:5], start=1):
            for statement in statements:
                conn.execute(sql.SQL(statement).format(schema=ident))
            conn.execute(sql.SQL("INSERT INTO {}.schema_version (version) VALUES (%s)").format(ident), (version,))


def _insert_session(dsn: str, schema: str, header: SessionHeader, rows: list[tuple[int, Jsonb]]) -> None:
    ident = sql.Identifier(schema)
    stored = header.model_copy(update={"last_seq": rows[-1][0] if rows else 0})
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


def _insert_v5_index(dsn: str, schema: str, actor_id: str, items: list[tuple[int, SessionEvent]]) -> None:
    rows = []
    for seq, event in items:
        command_id = event.command_id if isinstance(event, InputQueued | CancellationRequested) else None
        turn_id = (
            event.turn_id
            if isinstance(event, TurnStarted | TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted)
            else None
        )
        if command_id is None and turn_id is None and not isinstance(event, AgentFinished):
            continue
        rows.append(
            (
                actor_id,
                seq,
                event.type,
                command_id.encode() if command_id is not None else None,
                turn_id.encode() if turn_id is not None else None,
            )
        )
    ident = sql.Identifier(schema)
    with psycopg.connect(dsn) as conn, conn.cursor() as cursor:
        cursor.executemany(
            sql.SQL(
                "INSERT INTO {}.journal_index (actor_id, seq, event_type, command_id, turn_id)"
                " VALUES (%s, %s, %s, %s, %s)"
            ).format(ident),
            rows,
        )


def _random_events(seed: int) -> list[SessionEvent]:
    rng = random.Random(seed)
    turns = [_id(f"generated-{seed}-{index}") for index in range(5)]
    events: list[SessionEvent] = []
    for index in range(40):
        turn_id = rng.choice(turns)
        kind = rng.randrange(8)
        if kind in (0, 1):
            events.append(InputQueued(command_id=turn_id, input=f"input-{index}"))
        elif kind == 2:
            events.append(TurnStarted(turn_id=turn_id, input=f"start-{index}"))
        elif kind == 3:
            events.append(TurnCompleted(turn_id=turn_id, stop_reason="completed"))
        elif kind == 4:
            events.append(TurnFailed(turn_id=turn_id, error="failed"))
        elif kind == 5:
            events.append(TurnInterrupted(turn_id=turn_id, reason="lost"))
        elif kind == 6:
            events.append(TextDelta(text=f"delta-{index}"))
        else:
            events.append(AgentFinished(result={"index": index}))
    return events


@pytest.mark.parametrize("seed", range(12))
async def test_operational_projection_matches_full_reduction_for_generated_journals(
    postgres_dsn: str, pg_schema: str, seed: int
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    events = _random_events(seed)
    await store.create(header)
    for end in range(8, len(events) + 1, 8):
        await store.append(header.id, events[end - 8 : end])
        state = await store.read_operational(header.id)
        reduced = reduce_journal(events[:end])
        assert state.pending == reduced.pending
        assert state.incomplete == reduced.incomplete
        assert state.finished == next(
            (event for event in reversed(events[:end]) if isinstance(event, AgentFinished)), None
        )
        assert state.cancellations == {}
        assert state.last_seq == end
        assert state.version == 1


async def test_operational_projection_preserves_duplicate_inputs_unusual_order_and_latest_open_start(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(header)
    before = _id("unusual-before")
    open_old = _id("unusual-open-old")
    pending = _id("unusual-pending")
    open_new = _id("unusual-open-new")
    events: list[SessionEvent] = [
        InputQueued(command_id="duplicate\x00", input="first"),
        InputQueued(command_id="duplicate\x00", input="second"),
        TurnCompleted(turn_id=before, stop_reason="completed"),
        InputQueued(command_id=before, input="queued after terminal"),
        TurnStarted(turn_id=before, input="started after terminal"),
        TurnStarted(turn_id=open_old, input="old"),
        InputQueued(command_id=pending, input="pending"),
        TurnStarted(turn_id=open_new, input="new"),
        AgentFinished(result="first"),
        AgentFinished(result="latest"),
    ]

    await store.append(header.id, events)
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.sessions SET header = jsonb_set("
        " jsonb_set(header, '{{finished}}', 'false'::jsonb), '{{status}}', '\"idle\"'::jsonb) WHERE id = %s",
        (header.id,),
    )
    state = await store.read_operational(header.id)

    assert state.pending == events[:2] + [events[6]]
    assert state.incomplete == events[7]
    assert state.finished == events[9]
    assert state.last_seq == 10

    await store.append(header.id, [TurnCancelled(turn_id=open_new)])
    state = await store.read_operational(header.id)

    assert state.incomplete == events[5]
    assert state.last_seq == 11


async def test_cross_actor_cancellation_resolution_keeps_original_request_immutable(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="root")
    child = SessionHeader(id=uuid.uuid4().hex, root_id=root.id, parent_id=root.id, depth=1, agent="child")
    await store.create(root)
    await store.create(child)
    root_turn = _id("cancellation-root-turn")
    child_turn = _id("cancellation-child-turn")
    request = CancellationRequested(
        command_id=uuid.uuid4().hex,
        targets={root.id: [root_turn], child.id: [child_turn]},
    )
    await store.append(root.id, [request])

    assert (await store.read_operational(root.id)).cancellations == {
        root.id: [root_turn],
        child.id: [child_turn],
    }
    assert (await store.read_operational(child.id)).cancellations == {}

    await store.append(child.id, [TurnCancelled(turn_id=child_turn)])

    assert (await store.read_operational(root.id)).cancellations == {root.id: [root_turn]}
    assert (await store.read_page(root.id))[0].event == request

    await store.append(root.id, [TurnInterrupted(turn_id=root_turn, reason="owner_lost")])

    assert (await store.read_operational(root.id)).cancellations == {}
    assert (await store.read_page(root.id))[0].event == request


async def test_operational_maintenance_rolls_back_append_enqueue_and_lost_reply_retry(
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
        "CREATE FUNCTION {schema}.reject_operational_advance() RETURNS trigger AS $$"
        " BEGIN RAISE EXCEPTION 'injected operational failure'; END;"
        " $$ LANGUAGE plpgsql",
    )
    _execute(
        postgres_dsn,
        pg_schema,
        "CREATE TRIGGER reject_operational_advance BEFORE UPDATE OF operational_seq ON {schema}.sessions"
        " FOR EACH ROW WHEN (NEW.operational_seq > OLD.operational_seq)"
        " EXECUTE FUNCTION {schema}.reject_operational_advance()",
    )
    queued = InputQueued(command_id=uuid.uuid4().hex, input="once")

    with pytest.raises(psycopg.Error, match="injected operational failure"):
        await store.append(
            appended.id,
            [CancellationRequested(command_id=uuid.uuid4().hex, targets={appended.id: [_id("rollback-target")]})],
        )
    with pytest.raises(psycopg.Error, match="injected operational failure"):
        await store.enqueue(enqueued.id, queued)

    assert await store.read_page(appended.id) == []
    assert await store.read_page(enqueued.id) == []
    assert _query(postgres_dsn, pg_schema, "SELECT actor_id FROM {schema}.journal_index") == []
    assert _query(postgres_dsn, pg_schema, "SELECT source_actor_id FROM {schema}.cancellation_targets") == []
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT last_seq, operational_seq FROM {schema}.sessions ORDER BY id",
    ) == [(0, 0), (0, 0)]

    _execute(postgres_dsn, pg_schema, "DROP TRIGGER reject_operational_advance ON {schema}.sessions")
    _execute(postgres_dsn, pg_schema, "DROP FUNCTION {schema}.reject_operational_advance()")

    accepted = await store.enqueue(enqueued.id, queued)
    retried = await store.enqueue(enqueued.id, queued.model_copy(deep=True))
    state = await store.read_operational(enqueued.id)

    assert (accepted.seq, accepted.duplicate) == (1, False)
    assert (retried.seq, retried.duplicate) == (1, True)
    assert state.pending == [queued]
    assert state.last_seq == 1


async def test_migration_six_backfills_pages_envelopes_nul_and_projection_indexes(
    postgres_dsn: str, pg_schema: str
) -> None:
    _install_v5(postgres_dsn, pg_schema)
    child = SessionHeader(id="a-" + uuid.uuid4().hex, agent="child")
    root = SessionHeader(id="z-" + uuid.uuid4().hex, agent="root")
    turn_id = "turn\x00雪"
    pending_id = _id("migration-pending")
    child_terminal = TurnCancelled(turn_id=turn_id)
    resolved_input = InputQueued(command_id=turn_id, input="resolved\x00")
    pending_input = InputQueued(command_id=pending_id, input="pending")
    child_rows = [(1, _current(Stamped(seq=1, event=child_terminal)))]
    root_rows = [(1, _current(Stamped(seq=1, event=resolved_input)))]
    root_rows.extend((seq, _legacy(Stamped(seq=seq, event=TextDelta(text=f"ignored-{seq}")))) for seq in range(2, 1002))
    cancellation = CancellationRequested(
        command_id="cancel\x00雪",
        targets={child.id: [turn_id], root.id: [pending_id]},
    )
    marker = CompactionApplied(
        strategy="summary",
        tokens_before=100,
        tokens_after=10,
        summary="brief\x00",
        floor_turn_id=turn_id,
    )
    root_rows.extend(
        [
            (1002, _current(Stamped(seq=1002, event=cancellation))),
            (1003, _legacy(Stamped(seq=1003, event=pending_input))),
            (1004, _current(Stamped(seq=1004, event=marker))),
            (1005, _legacy(Stamped(seq=1005, event=AgentFinished(result={"ok": True})))),
        ]
    )
    _insert_session(postgres_dsn, pg_schema, child, child_rows)
    _insert_session(postgres_dsn, pg_schema, root, root_rows)
    _insert_v5_index(postgres_dsn, pg_schema, child.id, [(1, child_terminal)])
    _insert_v5_index(
        postgres_dsn,
        pg_schema,
        root.id,
        [(1, resolved_input), (1002, cancellation), (1003, pending_input), (1005, AgentFinished(result={"ok": True}))],
    )
    original = _query(
        postgres_dsn,
        pg_schema,
        "SELECT session_id, seq, stamped FROM {schema}.events ORDER BY session_id, seq",
    )
    store = PostgresStore(postgres_dsn, schema=pg_schema)

    await store.setup()
    await store.setup()

    assert original == _query(
        postgres_dsn,
        pg_schema,
        "SELECT session_id, seq, stamped FROM {schema}.events ORDER BY session_id, seq",
    )
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT version FROM {schema}.schema_version ORDER BY version",
    ) == [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT id, operational_version, operational_seq FROM {schema}.sessions ORDER BY id",
    ) == [(child.id, 1, 1), (root.id, 1, 1005)]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT actor_id, seq, event_type, live FROM {schema}.journal_index"
        " WHERE actor_id = %s AND event_type IN ('input_queued', 'compaction_applied') ORDER BY seq",
        (root.id,),
    ) == [
        (root.id, 1, "input_queued", True),
        (root.id, 1003, "input_queued", True),
        (root.id, 1004, "compaction_applied", False),
    ]
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT source_actor_id, source_seq, actor_id, turn_id FROM {schema}.cancellation_targets ORDER BY actor_id",
    ) == [(root.id, 1002, root.id, pending_id.encode())]
    definitions = [
        row[0]
        for row in _query(
            postgres_dsn,
            pg_schema,
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s",
            (pg_schema,),
        )
    ]
    assert any(
        "journal_index" in definition and "live" in definition and "WHERE" in definition for definition in definitions
    )
    assert any("journal_index" in definition and "compaction_applied" in definition for definition in definitions)
    state = await store.read_operational(root.id)
    assert state.pending == [resolved_input, pending_input]
    assert state.finished == AgentFinished(result={"ok": True})
    assert state.cancellations == {root.id: [pending_id]}


async def test_corrupt_migration_rolls_back_and_retries_without_rewriting_events(
    postgres_dsn: str, pg_schema: str
) -> None:
    _install_v5(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    _insert_session(postgres_dsn, pg_schema, header, [(1, Jsonb({"seq": 1}))])
    store = PostgresStore(postgres_dsn, schema=pg_schema)

    with pytest.raises(CorruptLog):
        await store.setup()

    assert _query(postgres_dsn, pg_schema, "SELECT max(version) FROM {schema}.schema_version") == [(5,)]
    assert (
        _query(
            postgres_dsn,
            pg_schema,
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = %s AND table_name = 'sessions' AND column_name = 'operational_seq'",
            (pg_schema,),
        )
        == []
    )
    assert _query(postgres_dsn, pg_schema, "SELECT to_regclass(%s)", (f"{pg_schema}.cancellation_targets",)) == [
        (None,)
    ]

    event = InputQueued(command_id="repaired\x00", input="ok")
    raw = _current(Stamped(seq=1, event=event))
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.events SET stamped = %s WHERE session_id = %s AND seq = 1",
        (raw, header.id),
    )
    _insert_v5_index(postgres_dsn, pg_schema, header.id, [(1, event)])
    repaired = _query(
        postgres_dsn,
        pg_schema,
        "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq = 1",
        (header.id,),
    )
    await store.setup()

    assert (await store.read_operational(header.id)).pending == [event]
    assert repaired == _query(
        postgres_dsn,
        pg_schema,
        "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq = 1",
        (header.id,),
    )


async def test_interrupted_migration_rolls_back_and_restart_succeeds(postgres_dsn: str, pg_schema: str) -> None:
    _install_v5(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    event = InputQueued(command_id=uuid.uuid4().hex, input="retry")
    _insert_session(postgres_dsn, pg_schema, header, [(1, _legacy(Stamped(seq=1, event=event)))])
    _insert_v5_index(postgres_dsn, pg_schema, header.id, [(1, event)])

    class InterruptedStore(PostgresStore):
        async def _backfill_operational(self, conn: Any) -> None:
            await super()._backfill_operational(conn)
            raise RuntimeError("migration interrupted")

    with pytest.raises(RuntimeError, match="migration interrupted"):
        await InterruptedStore(postgres_dsn, schema=pg_schema).setup()

    assert _query(postgres_dsn, pg_schema, "SELECT max(version) FROM {schema}.schema_version") == [(5,)]
    assert _query(postgres_dsn, pg_schema, "SELECT to_regclass(%s)", (f"{pg_schema}.cancellation_targets",)) == [
        (None,)
    ]

    restarted = PostgresStore(postgres_dsn, schema=pg_schema)
    await restarted.setup()

    assert (await restarted.read_operational(header.id)).pending == [event]


async def test_stale_and_invalid_operational_checkpoints_repair_from_authoritative_events(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    stale = SessionHeader(id=uuid.uuid4().hex, agent="stale")
    invalid = SessionHeader(id=uuid.uuid4().hex, agent="invalid")
    await store.create(stale)
    await store.create(invalid)
    done = _id("checkpoint-done")
    pending = _id("checkpoint-pending")
    closed = _id("checkpoint-closed")
    opened = _id("checkpoint-open")
    stale_events: list[SessionEvent] = [
        InputQueued(command_id=done, input="done"),
        TurnCompleted(turn_id=done, stop_reason="completed"),
        InputQueued(command_id=pending, input="pending"),
    ]
    invalid_events: list[SessionEvent] = [
        InputQueued(command_id=closed, input="closed"),
        TurnStarted(turn_id=closed, input="closed"),
        TurnInterrupted(turn_id=closed, reason="lost"),
        TurnStarted(turn_id=opened, input="open"),
    ]
    await store.append(stale.id, stale_events)
    await store.append(invalid.id, invalid_events)
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.sessions SET operational_seq = 1 WHERE id = %s",
        (stale.id,),
    )
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.sessions SET operational_version = 99, operational_seq = last_seq + 10 WHERE id = %s",
        (invalid.id,),
    )
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.journal_index SET live = true WHERE actor_id = %s",
        (invalid.id,),
    )

    stale_state = await store.read_operational(stale.id)
    invalid_state = await store.read_operational(invalid.id)

    assert stale_state.pending == [stale_events[2]]
    assert stale_state.incomplete is None
    assert stale_state.last_seq == 3
    assert invalid_state.pending == []
    assert invalid_state.incomplete == invalid_events[3]
    assert invalid_state.last_seq == 4
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT id, operational_version, operational_seq FROM {schema}.sessions WHERE id IN (%s, %s) ORDER BY id",
        (stale.id, invalid.id),
    ) == sorted([(stale.id, 1, 3), (invalid.id, 1, 4)])
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT seq, live FROM {schema}.journal_index WHERE actor_id = %s ORDER BY seq",
        (invalid.id,),
    ) == [(1, False), (2, False), (3, False), (4, True)]


@pytest.mark.parametrize(
    "terminal",
    [
        TurnCompleted(turn_id="placeholder", stop_reason="completed", output={"value": 1}),
        TurnFailed(turn_id="placeholder", error="boom"),
        TurnCancelled(turn_id="placeholder", reason="cancelled"),
        TurnInterrupted(turn_id="placeholder", reason="owner_lost"),
    ],
)
async def test_targeted_turn_reads_match_full_result_reconstruction(
    postgres_dsn: str, pg_schema: str, terminal: SessionEvent
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    turn_id = uuid.uuid4().hex
    terminal = terminal.model_copy(update={"turn_id": turn_id})
    events: list[SessionEvent] = [
        TextDelta(text="before"),
        TurnStarted(turn_id=turn_id, input="run"),
        SampleStarted(turn_id=turn_id, sample_id="first", model="test"),
        TextPart(sample_id="first", text="ignored"),
        SampleCompleted(sample_id="first", usage=Usage(input_tokens=1, output_tokens=2)),
        TextPart(sample_id="second", text="kept"),
        SampleCompleted(sample_id="second", usage=Usage(input_tokens=3, output_tokens=4)),
        terminal,
        TextDelta(text="after"),
    ]
    await store.create(header)
    await store.append(header.id, events)

    targeted = await store.read_turn(header.id, turn_id)
    full = await store.read_page(header.id)

    assert targeted == [Stamped(seq=index, event=event) for index, event in enumerate(events[1:8], start=2)]
    assert _turn_result(uuid.UUID(hex=header.id), uuid.UUID(hex=turn_id), targeted) == _turn_result(
        uuid.UUID(hex=header.id), uuid.UUID(hex=turn_id), full
    )


async def test_targeted_turn_reads_avoid_bodies_until_terminal_and_bound_completed_interval(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    active = SessionHeader(id=uuid.uuid4().hex, agent="active")
    completed = SessionHeader(id=uuid.uuid4().hex, agent="completed")
    prestart = SessionHeader(id=uuid.uuid4().hex, agent="prestart")
    for header in (active, completed, prestart):
        await store.create(header)
    active_turn = uuid.uuid4().hex
    await store.append(active.id, [TurnStarted(turn_id=active_turn, input="active"), TextDelta(text="streaming")])
    completed_turn = uuid.uuid4().hex
    await store.append(completed.id, [TextDelta(text=str(index)) for index in range(1200)])
    await store.append(
        completed.id,
        [
            TurnStarted(turn_id=completed_turn, input="done"),
            TextPart(sample_id="sample", text="answer"),
            SampleCompleted(sample_id="sample"),
            TurnCompleted(turn_id=completed_turn, stop_reason="completed"),
            TextDelta(text="after"),
        ],
    )
    prestart_turn = uuid.uuid4().hex
    prestart_terminal = TurnFailed(turn_id=prestart_turn, error="before start")
    await store.append(prestart.id, [prestart_terminal])
    parsed = 0
    original = postgres._parse

    def counted(sid: str, raw: Any) -> Stamped:
        nonlocal parsed
        parsed += 1
        return original(sid, raw)

    monkeypatch.setattr(postgres, "_parse", counted)

    assert await store.read_turn(active.id, active_turn) is None
    assert parsed == 0
    assert await store.read_turn(prestart.id, prestart_turn) == [Stamped(seq=1, event=prestart_terminal)]
    assert parsed == 1
    parsed = 0
    targeted = await store.read_turn(completed.id, completed_turn)

    assert targeted is not None
    assert [item.seq for item in targeted] == [1201, 1202, 1203, 1204]
    assert parsed == 4


async def test_compacted_reads_follow_latest_marker_floor_and_missing_floor_rules(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(header)
    keep = _id("compaction-keep")
    events: list[SessionEvent] = [
        TextDelta(text="discard"),
        TurnStarted(turn_id=keep, input="keep"),
        TextDelta(text="middle"),
        CompactionApplied(
            strategy="summary",
            tokens_before=100,
            tokens_after=50,
            summary="first",
            floor_turn_id=keep,
        ),
        TextDelta(text="later"),
        CompactionApplied(
            strategy="summary",
            tokens_before=50,
            tokens_after=20,
            summary="latest",
            floor_turn_id=keep,
        ),
        TextDelta(text="tail"),
    ]
    await store.append(header.id, events)

    snapshot = await store.read_compacted(header.id)

    assert snapshot.items == [Stamped(seq=index, event=event) for index, event in enumerate(events[1:], start=2)]
    assert snapshot.last_seq == 7

    missing = SessionHeader(id=uuid.uuid4().hex, agent="missing")
    await store.create(missing)
    missing_events: list[SessionEvent] = [
        TextDelta(text="discard"),
        CompactionApplied(
            strategy="summary",
            tokens_before=100,
            tokens_after=10,
            summary="brief",
            floor_turn_id=_id("compaction-absent"),
        ),
        TextDelta(text="tail"),
    ]
    await store.append(missing.id, missing_events)
    missing_snapshot = await store.read_compacted(missing.id)

    assert missing_snapshot.items == [Stamped(seq=2, event=missing_events[1]), Stamped(seq=3, event=missing_events[2])]
    assert missing_snapshot.last_seq == 3

    plain = SessionHeader(id=uuid.uuid4().hex, agent="plain")
    plain_events: list[SessionEvent] = [TextDelta(text="one"), TextDelta(text="two")]
    await store.create(plain)
    await store.append(plain.id, plain_events)
    plain_snapshot = await store.read_compacted(plain.id)

    assert plain_snapshot.items == [
        Stamped(seq=index, event=event) for index, event in enumerate(plain_events, start=1)
    ]
    assert plain_snapshot.last_seq == 2


async def test_compacted_read_decodes_only_the_retained_long_journal_window(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="build")
    await store.create(header)
    await store.append(header.id, [TextDelta(text=str(index)) for index in range(1200)])
    floor = _id("compaction-long-floor")
    tail: list[SessionEvent] = [
        TurnStarted(turn_id=floor, input="keep"),
        TextDelta(text="middle"),
        CompactionApplied(
            strategy="summary",
            tokens_before=10_000,
            tokens_after=10,
            summary="brief",
            floor_turn_id=floor,
        ),
        TextDelta(text="tail"),
    ]
    await store.append(header.id, tail)
    parsed = 0
    original = postgres._parse

    def counted(sid: str, raw: Any) -> Stamped:
        nonlocal parsed
        parsed += 1
        return original(sid, raw)

    monkeypatch.setattr(postgres, "_parse", counted)

    state = await store.read_operational(header.id)

    assert state.incomplete == tail[0]
    assert state.last_seq == 1204
    assert parsed == 1
    parsed = 0

    snapshot = await store.read_compacted(header.id)

    assert snapshot.items == [Stamped(seq=index, event=event) for index, event in enumerate(tail, start=1201)]
    assert snapshot.last_seq == 1204
    assert parsed == 5


@pytest.mark.parametrize("corruption", ["live_input", "live_start", "command_pointer"])
async def test_current_but_inconsistent_live_checkpoint_is_rebuilt(postgres_dsn, pg_schema, corruption):
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="repair")
    await store.create(header)
    turn = _id("inconsistent-turn")
    queued = InputQueued(command_id=turn, input="already completed")
    await store.append(
        header.id,
        [queued, TurnStarted(turn_id=turn, input=queued.input), TurnCompleted(turn_id=turn, stop_reason="done")],
    )
    if corruption == "command_pointer":
        await store.enqueue(header.id, InputQueued(command_id=_id("still-pending"), input="pending"))
        _execute(
            postgres_dsn,
            pg_schema,
            "UPDATE {schema}.journal_index SET command_id = %s WHERE actor_id = %s AND live",
            (b"wrong", header.id),
        )
    else:
        _execute(
            postgres_dsn,
            pg_schema,
            "UPDATE {schema}.journal_index SET live = true WHERE actor_id = %s AND event_type = %s",
            (header.id, "input_queued" if corruption == "live_input" else "turn_started"),
        )
    state = await store.read_operational(header.id)
    assert state.incomplete is None
    assert state.pending == (
        [InputQueued(command_id=_id("still-pending"), input="pending")] if corruption == "command_pointer" else []
    )
    assert not any(
        live
        for _, live in _query(
            postgres_dsn,
            pg_schema,
            "SELECT seq, live FROM {schema}.journal_index WHERE actor_id = %s AND seq <= 3",
            (header.id,),
        )
    )


@pytest.mark.parametrize("coordinated", [False, True])
@pytest.mark.parametrize("operation", ["stream", "empty", "enqueue"])
@pytest.mark.parametrize("version", [1, 99])
async def test_append_cannot_hide_uncovered_operational_evidence(
    postgres_dsn, pg_schema, coordinated, operation, version
):
    store = await _store(postgres_dsn, pg_schema)
    header = SessionHeader(id=uuid.uuid4().hex, agent="stale")
    await store.create(header)
    queued = InputQueued(command_id=_id("uncovered"), input="uncovered input")
    await store.enqueue(header.id, queued)
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.sessions SET operational_version = %s, operational_seq = 0 WHERE id = %s",
        (version, header.id),
    )
    _execute(
        postgres_dsn, pg_schema, "UPDATE {schema}.journal_index SET live = false WHERE actor_id = %s", (header.id,)
    )
    additional = InputQueued(command_id=_id("new-input"), input="new input")

    async def apply(source):
        if operation == "enqueue":
            await source.enqueue(header.id, additional)
        else:
            await source.append(header.id, [TextDelta(text="delta")] if operation == "stream" else [])
        assert _query(
            postgres_dsn,
            pg_schema,
            "SELECT operational_version, operational_seq FROM {schema}.sessions WHERE id = %s",
            (header.id,),
        ) == [(version, 0)]
        return await source.read_operational(header.id)

    if coordinated:
        coordinator = PostgresCoordinator(store)
        await coordinator.start()
        try:
            ownership = await coordinator.acquire(header.id)
            async with coordinator.transaction(ownership) as view:
                state = await apply(view)
        finally:
            await coordinator.close()
    else:
        state = await apply(store)
    assert state.pending == ([queued, additional] if operation == "enqueue" else [queued])
    assert _query(
        postgres_dsn,
        pg_schema,
        "SELECT operational_version, operational_seq, last_seq FROM {schema}.sessions WHERE id = %s",
        (header.id,),
    ) == [(1, state.last_seq, state.last_seq)]
