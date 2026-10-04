from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import pytest

from tantra import PostgresCoordinator, PostgresStore, SessionHeader
from tantra.coordinator import _Observation
from tantra.events import InputQueued, Stamped, TextDelta, TurnCancelled, TurnCompleted, TurnStarted
from tantra.stores.postgres import MIGRATIONS, _event_json

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb


async def test_terminal_index_migration_rolls_back_retries_and_preserves_journals(
    postgres_dsn: str,
    pg_schema: str,
) -> None:
    header = SessionHeader(id=uuid4().hex, agent="bot", model="m")
    command = uuid4().hex
    events = [InputQueued(command_id=command, input="nul\x00雪"), TurnCancelled(turn_id=command)]
    with psycopg.connect(postgres_dsn) as conn:

        def statement(body: str) -> Any:
            return sql.SQL(body).format(schema=sql.Identifier(pg_schema))

        conn.execute(statement("CREATE SCHEMA {schema}"))
        conn.execute(statement("CREATE TABLE {schema}.schema_version (version int NOT NULL)"))
        for version, statements in enumerate(MIGRATIONS[:9], start=1):
            for body in statements:
                conn.execute(statement(body))
            conn.execute(statement("INSERT INTO {schema}.schema_version VALUES (%s)"), (version,))
        conn.execute(
            statement(
                "INSERT INTO {schema}.sessions (id, header, created_at, last_seq, operational_seq)"
                " VALUES (%s, %s, %s, 2, 2)"
            ),
            (header.id, Jsonb(header.model_dump(mode="json")), header.created_at),
        )
        for seq, event in enumerate(events, start=1):
            conn.execute(
                statement("INSERT INTO {schema}.events VALUES (%s, %s, %s)"),
                (header.id, seq, _event_json(Stamped(seq=seq, event=event))),
            )
            conn.execute(
                statement(
                    "INSERT INTO {schema}.journal_index (actor_id, seq, event_type, command_id, turn_id)"
                    " VALUES (%s, %s, %s, %s, %s)"
                ),
                (
                    header.id,
                    seq,
                    event.type,
                    command.encode() if seq == 1 else None,
                    command.encode() if seq == 2 else None,
                ),
            )
        original = conn.execute(
            statement("SELECT session_id, seq, stamped FROM {schema}.events ORDER BY seq")
        ).fetchall()
        conn.execute(
            statement(
                "CREATE FUNCTION {schema}.reject_v10() RETURNS trigger LANGUAGE plpgsql AS $$"
                " BEGIN IF NEW.version = 10 THEN RAISE EXCEPTION 'stop migration';"
                " END IF; RETURN NEW; END $$"
            )
        )
        conn.execute(
            statement(
                "CREATE TRIGGER reject_v10 BEFORE INSERT ON {schema}.schema_version"
                " FOR EACH ROW EXECUTE FUNCTION {schema}.reject_v10()"
            )
        )

    store = PostgresStore(postgres_dsn, schema=pg_schema)
    try:
        with pytest.raises(psycopg.Error, match="stop migration"):
            await store.setup()
        with psycopg.connect(postgres_dsn) as conn:
            assert conn.execute(statement("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (9,)
            assert conn.execute("SELECT to_regclass(%s)", (f"{pg_schema}.journal_terminal_idx",)).fetchone() == (None,)
            assert (
                conn.execute(statement("SELECT session_id, seq, stamped FROM {schema}.events ORDER BY seq")).fetchall()
                == original
            )
            conn.execute(statement("DROP TRIGGER reject_v10 ON {schema}.schema_version"))
        await store.setup()
        await store.setup()
        with psycopg.connect(postgres_dsn) as conn:
            assert conn.execute(statement("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (10,)
            assert (
                conn.execute("SELECT to_regclass(%s)", (f"{pg_schema}.journal_terminal_idx",)).fetchone()[0] is not None
            )
            assert (
                conn.execute(statement("SELECT session_id, seq, stamped FROM {schema}.events ORDER BY seq")).fetchall()
                == original
            )
        assert [item.event for item in await store.read_turn(header.id, command)] == [events[-1]]
    finally:
        await store.close()


@pytest.mark.parametrize("history", [4_000, 100_000])
async def test_shared_observation_uses_terminal_index_without_event_body_reads(
    postgres_dsn: str,
    pg_schema: str,
    history: int,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    header = SessionHeader(id=uuid4().hex, root_id=None, agent="bot", model="m")
    await store.create(header)
    try:
        async with store._connection() as conn:
            async with conn.transaction():
                for after in range(0, history, 1_000):
                    bodies = []
                    index = []
                    for offset in range(1_000):
                        seq = after + offset + 1
                        key = str((seq - 1) // 4)
                        event = (
                            InputQueued(command_id=key, input="history")
                            if seq % 4 == 1
                            else TurnStarted(turn_id=key, input="history")
                            if seq % 4 == 2
                            else TextDelta(text=f"fragment-{seq}")
                            if seq % 4 == 3
                            else TurnCompleted(turn_id=key, stop_reason="stop")
                        )
                        bodies.append((header.id, seq, _event_json(Stamped(seq=seq, event=event))))
                        if not isinstance(event, TextDelta):
                            index.append(
                                (
                                    header.id,
                                    seq,
                                    event.type,
                                    key.encode() if isinstance(event, InputQueued) else None,
                                    key.encode() if not isinstance(event, InputQueued) else None,
                                )
                            )
                    async with conn.cursor() as cursor:
                        await cursor.executemany(store._sql("INSERT INTO {schema}.events VALUES (%s, %s, %s)"), bodies)
                        await cursor.executemany(
                            store._sql(
                                "INSERT INTO {schema}.journal_index"
                                " (actor_id, seq, event_type, command_id, turn_id) VALUES (%s, %s, %s, %s, %s)"
                            ),
                            index,
                        )
                await conn.execute(
                    store._sql("UPDATE {schema}.sessions SET last_seq = %s, operational_seq = %s WHERE id = %s"),
                    (history, history, header.id),
                )
            await conn.execute(store._sql("ANALYZE {schema}.journal_index, {schema}.sessions"))

        coordinator = PostgresCoordinator(store, catch_up_interval=10)
        coordinator._started = True
        coordinator._observations[header.id] = _Observation()
        observed_sql: list[Any] = []
        original = coordinator._sql

        def capture(body: str) -> Any:
            result = original(body)
            if body.startswith("WITH interested"):
                observed_sql.append(result)
            return result

        monkeypatch.setattr(coordinator, "_sql", capture)
        try:
            await coordinator._catch_up({header.id}, periodic=False)
            observed = coordinator.observation(header.id)
            assert observed.actors[header.id] == (history, False)
            assert observed.terminal_sequences == {header.id: history}
            async with store._connection() as conn:
                body = observed_sql[0].as_string(conn)
                assert ".events" not in body and "stamped" not in body
                cursor = await conn.execute(
                    sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + observed_sql[0], ([header.id], [])
                )
                plan = (await cursor.fetchone())[0]
            serialized = json.dumps(plan)
            assert "journal_terminal_idx" in serialized
            (tmp_path / f"terminal-plan-{history}.json").write_text(json.dumps(plan, indent=2))
        finally:
            await coordinator.close()
    finally:
        await store.close()
