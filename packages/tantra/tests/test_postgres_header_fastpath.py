from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tantra import LeaseLost, PostgresCoordinator, PostgresStore, SessionHeader
from tantra.cleanup import CleanupChanged, CleanupSelector
from tantra.events import InputQueued, ReasoningDelta, Stamped, TextDelta, ToolCallDelta, TurnStarted
from tantra.stores import postgres
from tantra.stores.base import reduce_header

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb


@pytest.mark.parametrize("coordinated", [False, True])
@pytest.mark.parametrize("checkpoint", ["current", "stale", "version"])
async def test_delta_append_skips_header_python_work_and_preserves_storage(
    postgres_dsn, pg_schema, coordinated, checkpoint, monkeypatch
):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store)
    await coordinator.start() if coordinated else await store.setup()
    sid = uuid4().hex
    header = SessionHeader(id=sid, root_id=sid, agent="bot", metadata={"tenant": "a"}, extra_field={"keep": [1, 2]})
    await store.create(header)
    owner = await coordinator.acquire(sid) if coordinated else None
    seed = [InputQueued(command_id="work", input="query"), TurnStarted(turn_id="work", input="query")]
    if coordinated:
        async with coordinator.transaction(owner) as view:
            await view.append(sid, seed)
    else:
        await store.append(sid, seed)
    if checkpoint != "current":
        async with store._connection() as conn:
            async with conn.transaction():
                if coordinated:
                    async with coordinator.transaction(owner) as view:
                        await view.conn.execute(
                            store._sql("UPDATE {schema}.sessions SET operational_seq=0 WHERE id=%s"), (sid,)
                        )
                else:
                    await conn.execute(store._sql("UPDATE {schema}.sessions SET operational_seq=0 WHERE id=%s"), (sid,))
    if checkpoint == "version":
        with psycopg.connect(postgres_dsn) as conn:
            conn.execute(store._sql("UPDATE {schema}.sessions SET operational_version=0 WHERE id=%s"), (sid,))
    previous = await store.header(sid)
    events = [
        TextDelta(text="nul\x00雪", extra="keep"),
        ReasoningDelta(text="think"),
        ToolCallDelta(index=0, arguments="{}"),
    ]

    def forbidden(*args, **kwargs):
        raise AssertionError("delta append loaded or serialized a header")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(SessionHeader, "model_validate", forbidden)
            patch.setattr(postgres, "_json", forbidden)
            patch.setattr(postgres, "reduce_header", forbidden)
            import tantra.coordinator as module

            patch.setattr(module, "_hydrate", forbidden)
            patch.setattr(module, "_json", forbidden)
            patch.setattr(module, "reduce_header", forbidden)
            if coordinated:
                async with coordinator.transaction(owner) as view:
                    assert await view.append(sid, events) == 5
            else:
                assert await store.append(sid, events) == 5
        current = await store.header(sid)
        expected = reduce_header(previous, events).model_dump(mode="json")
        expected.update(last_seq=5, updated_at=current.model_dump(mode="json")["updated_at"])
        assert current.model_dump(mode="json") == expected
        assert current.updated_at >= previous.updated_at
        with psycopg.connect(postgres_dsn) as conn:
            row = conn.execute(
                store._sql(
                    "SELECT header,metadata,last_seq,updated_at,operational_seq FROM {schema}.sessions WHERE id=%s"
                ),
                (sid,),
            ).fetchone()
        assert row[0] == expected
        assert row[1] == header.metadata and row[2] == 5 and row[3] == current.updated_at
        assert row[4] == (5 if checkpoint == "current" else 0)
        assert [item.event for item in await store.read_page(sid, after=2)] == events
    finally:
        if coordinated:
            await coordinator.close()
        await store.close()


@pytest.mark.parametrize("coordinated", [False, True])
@pytest.mark.parametrize("events", [[], [TextDelta(text="delta"), InputQueued(command_id="work", input="queued")]])
async def test_empty_and_mixed_appends_keep_header_reduction(postgres_dsn, pg_schema, coordinated, events, monkeypatch):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store)
    await coordinator.start() if coordinated else await store.setup()
    sid = uuid4().hex
    await store.create(SessionHeader(id=sid, agent="bot"))
    owner = await coordinator.acquire(sid) if coordinated else None
    calls = []

    def reduce(header, batch):
        calls.append(batch)
        return reduce_header(header, batch)

    import tantra.coordinator as module

    monkeypatch.setattr(module if coordinated else postgres, "reduce_header", reduce)
    try:
        if coordinated:
            async with coordinator.transaction(owner) as view:
                await view.append(sid, events)
        else:
            await store.append(sid, events)
        assert calls == [events]
    finally:
        if coordinated:
            await coordinator.close()
        await store.close()


async def test_combined_settings_are_transaction_local_and_cross_root_append_is_fenced(
    postgres_dsn, pg_schema, monkeypatch
):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store, request_timeout=2)
    await coordinator.start()
    roots = [uuid4().hex for _ in range(2)]
    for sid in roots:
        await store.create(SessionHeader(id=sid, agent="bot"))
    owners = [await coordinator.acquire(sid) for sid in roots]
    statements = []
    execute = psycopg.AsyncConnection.execute

    async def capture(conn, query, *args, **kwargs):
        statements.append(query if isinstance(query, str) else query.as_string(conn))
        return await execute(conn, query, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(psycopg.AsyncConnection, "execute", capture)
            async with coordinator.transaction(owners[0]) as view:
                await view.append(roots[0], [TextDelta(text="a")])
        settings = [body for body in statements if "set_config(" in body]
        assert len(settings) == 2
        assert sum(body.count("set_config(") for body in settings) == 6
        assert any("MATERIALIZED" in body and "FOR UPDATE" in body for body in statements)
        async with store._connection() as conn:
            cursor = await conn.execute(
                "SELECT current_setting('tantra.root_id', true),"
                "current_setting('tantra.instance_id', true),current_setting('tantra.generation', true)"
            )
            assert all(value in (None, "") for value in await cursor.fetchone())
        with pytest.raises(LeaseLost):
            async with coordinator.transaction(owners[0]) as view:
                await view.append(roots[1], [TextDelta(text="wrong root")])
        async with coordinator.transaction(owners[1]) as view:
            await view.append(roots[1], [TextDelta(text="b")])
            await view.conn.execute(
                store._sql(
                    "UPDATE {schema}.coordinator_roots"
                    " SET expires_at=clock_timestamp() - interval '1 second' WHERE root_id=%s"
                ),
                (roots[1],),
            )
            with pytest.raises(LeaseLost):
                await view._assert_fence()
            raise RuntimeError("rollback fence")
    except RuntimeError as exc:
        assert str(exc) == "rollback fence"
        assert await store.read_page(roots[1]) == []
    finally:
        await coordinator.close()
        await store.close()


async def test_initial_expiry_is_checked_after_root_row_lock(postgres_dsn, pg_schema):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    coordinator = PostgresCoordinator(store, lease_ttl=0.2, request_timeout=2)
    await coordinator.start()
    sid = uuid4().hex
    await store.create(SessionHeader(id=sid, agent="bot"))
    owner = await coordinator.acquire(sid)
    blocker = await psycopg.AsyncConnection.connect(postgres_dsn)

    async def enter():
        async with coordinator.transaction(owner):
            raise AssertionError("expired ownership was authorized")

    try:
        async with blocker.transaction():
            await blocker.execute(
                store._sql("SELECT 1 FROM {schema}.coordinator_roots WHERE root_id=%s FOR UPDATE"), (sid,)
            )
            task = asyncio.create_task(enter())
            await asyncio.sleep(0.3)
            assert not task.done()
        with pytest.raises(LeaseLost):
            await task
    finally:
        await blocker.close()
        await coordinator.close()
        await store.close()


async def test_delta_changes_cleanup_age_and_revision_for_legacy_descendant(postgres_dsn, pg_schema):
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root, child = uuid4(), uuid4()
    old = datetime.now(UTC) - timedelta(days=2)
    await store.create(SessionHeader(id=root.hex, agent="bot", updated_at=old))
    await store.create(SessionHeader(id=child.hex, parent_id=root.hex, agent="bot", updated_at=old))
    selector = CleanupSelector(root_ids=[root])
    try:
        before = (await store.select_cleanup(selector, limit=1))[0]
        await store.append(child.hex, [TextDelta(text="recent")])
        after = (await store.select_cleanup(selector, limit=1))[0]
        assert after.updated_at > before.updated_at and after.revision != before.revision
        assert (
            await store.select_cleanup(
                CleanupSelector(root_ids=[root], inactive_before=old + timedelta(days=1)), limit=1
            )
            == []
        )
        with pytest.raises(CleanupChanged):
            await store.delete_tree(root.hex, expected_revision=before.revision)
        assert await store.header(child.hex) is not None
    finally:
        await store.close()


async def test_generated_root_migration_rollback_retry_and_original_rows(postgres_dsn, pg_schema):
    headers = [SessionHeader(id=uuid4().hex, root_id=root, agent="bot") for root in (None, "", "explicit")]
    events = [TextDelta(text="nul\x00雪")]
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(store._sql("CREATE SCHEMA {schema}"))
        conn.execute(store._sql("CREATE TABLE {schema}.schema_version(version int NOT NULL)"))
        for version, statements in enumerate(postgres.MIGRATIONS[:10], start=1):
            for statement in statements:
                conn.execute(store._sql(statement))
            conn.execute(store._sql("INSERT INTO {schema}.schema_version VALUES(%s)"), (version,))
        for header in headers:
            conn.execute(
                store._sql("INSERT INTO {schema}.sessions(id,header,created_at,last_seq) VALUES(%s,%s,%s,1)"),
                (header.id, Jsonb(header.model_dump(mode="json")), header.created_at),
            )
            conn.execute(
                store._sql("INSERT INTO {schema}.events VALUES(%s,1,%s)"),
                (header.id, postgres._event_json(Stamped(seq=1, event=events[0]))),
            )
        before = conn.execute(store._sql("SELECT id,header,last_seq FROM {schema}.sessions ORDER BY id")).fetchall()
        bodies = conn.execute(
            store._sql("SELECT session_id,seq,stamped FROM {schema}.events ORDER BY session_id")
        ).fetchall()
        conn.execute(
            store._sql(
                "CREATE FUNCTION {schema}.reject_v11() RETURNS trigger LANGUAGE plpgsql AS $$"
                " BEGIN IF NEW.version=11 THEN RAISE EXCEPTION 'stop migration';"
                " END IF; RETURN NEW; END $$"
            )
        )
        conn.execute(
            store._sql(
                "CREATE TRIGGER reject_v11 BEFORE INSERT ON {schema}.schema_version"
                " FOR EACH ROW EXECUTE FUNCTION {schema}.reject_v11()"
            )
        )
    try:
        with pytest.raises(psycopg.Error, match="stop migration"):
            await store.setup()
        with psycopg.connect(postgres_dsn) as conn:
            assert conn.execute(store._sql("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (10,)
            assert (
                conn.execute(
                    "SELECT 1 FROM information_schema.columns WHERE table_schema=%s"
                    " AND table_name='sessions' AND column_name='root_key'",
                    (pg_schema,),
                ).fetchone()
                is None
            )
            conn.execute(store._sql("DROP TRIGGER reject_v11 ON {schema}.schema_version"))
        await store.setup()
        await store.setup()
        with psycopg.connect(postgres_dsn) as conn:
            assert conn.execute(store._sql("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (
                len(postgres.MIGRATIONS),
            )
            assert (
                conn.execute(store._sql("SELECT id,header,last_seq FROM {schema}.sessions ORDER BY id")).fetchall()
                == before
            )
            assert (
                conn.execute(
                    store._sql("SELECT session_id,seq,stamped FROM {schema}.events ORDER BY session_id")
                ).fetchall()
                == bodies
            )
            assert dict(conn.execute(store._sql("SELECT id,root_key FROM {schema}.sessions")).fetchall()) == {
                header.id: header.root_id or header.id for header in headers
            }
            definition = conn.execute(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=%s AND indexname='sessions_root_idx'", (pg_schema,)
            ).fetchone()[0]
            assert "(root_key)" in definition and "header" not in definition
    finally:
        await store.close()
