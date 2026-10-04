from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from tantra.cleanup import CleanupChanged, CleanupSelector
from tantra.errors import TantraError
from tantra.events import SessionHeader, TextPart
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
        return conn.execute(sql.SQL(statement).format(schema=sql.Identifier(schema)), params).fetchall()


def _execute(dsn: str, schema: str, statement: str, params: tuple[Any, ...] = ()) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL(statement).format(schema=sql.Identifier(schema)), params)


def _install_v8(dsn: str, schema: str) -> None:
    ident = sql.Identifier(schema)
    with psycopg.connect(dsn) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(ident))
        conn.execute(sql.SQL("CREATE TABLE {}.schema_version (version int NOT NULL)").format(ident))
        for version, statements in enumerate(MIGRATIONS[:8], start=1):
            for statement in statements:
                conn.execute(sql.SQL(statement).format(schema=ident))
            conn.execute(sql.SQL("INSERT INTO {}.schema_version VALUES (%s)").format(ident), (version,))


async def test_selection_uses_typed_root_metadata_and_rejects_explicit_children_before_filters(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    first, second, child = uuid4(), uuid4(), uuid4()
    await store.create(
        SessionHeader(
            id=first.hex,
            root_id=first.hex,
            agent="Bot",
            metadata={"nullable": None, "flag": True, "number": 1, "env": "prod", "team": "a"},
        )
    )
    await store.create(
        SessionHeader(
            id=second.hex,
            root_id=second.hex,
            agent="Bot",
            metadata={"flag": 1, "number": True, "env": "prod", "team": "b"},
        )
    )
    await store.create(SessionHeader(id=child.hex, root_id=first.hex, parent_id=first.hex, agent="Bot", depth=1))
    try:

        async def selected(metadata: dict[str, Any]) -> set[str]:
            rows = await store.select_cleanup(CleanupSelector(metadata=metadata), limit=10)
            return {row.root_id for row in rows}

        assert await selected({"nullable": None}) == {first.hex}
        assert await selected({"missing": None}) == set()
        assert await selected({"flag": True}) == {first.hex}
        assert await selected({"flag": 1}) == {second.hex}
        assert await selected({"number": 1.0}) == {first.hex}
        assert await selected({"number": True}) == {second.hex}
        assert await selected({"env": "prod", "team": "a"}) == {first.hex}
        with pytest.raises(TantraError, match="live child"):
            await store.select_cleanup(
                CleanupSelector(
                    root_ids=[child],
                    metadata={"env": "missing"},
                    inactive_before=datetime(2000, 1, 1, tzinfo=UTC),
                ),
                limit=0,
            )
    finally:
        await store.close()


async def test_selection_orders_roots_pages_strictly_and_uses_the_newest_descendant_timestamp(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    created = datetime(2024, 1, 1, tzinfo=UTC)
    cutoff = datetime(2025, 1, 1, tzinfo=UTC)
    ordered = sorted((uuid4(), uuid4()), key=lambda value: value.hex)
    recent_root, recent_child = uuid4(), uuid4()
    for root in ordered:
        await store.create(
            SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", created_at=created, updated_at=created)
        )
    await store.create(
        SessionHeader(
            id=recent_root.hex,
            root_id=recent_root.hex,
            agent="Bot",
            created_at=created - timedelta(days=1),
            updated_at=created,
        )
    )
    await store.create(
        SessionHeader(
            id=recent_child.hex,
            parent_id=recent_root.hex,
            agent="Bot",
            depth=1,
            created_at=created,
            updated_at=cutoff,
        )
    )
    try:
        selector = CleanupSelector(inactive_before=cutoff)
        first = await store.select_cleanup(selector, limit=1)
        second = await store.select_cleanup(selector, limit=1, after=first[0].cursor)
        assert [first[0].root_id, second[0].root_id] == [root.hex for root in ordered]
        assert await store.select_cleanup(selector, limit=1, after=second[0].cursor) == []
        assert first[0].created_at == created
        assert first[0].updated_at == created
        assert await store.delete_tree(ordered[0].hex, expected_revision=first[0].revision) == [ordered[0].hex]
    finally:
        await store.close()


async def test_selection_and_revision_do_not_read_historical_event_bodies(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", metadata={"keep": True}))
    await store.append(root.hex, [TextPart(sample_id="sample", text="body")])
    _execute(
        postgres_dsn,
        pg_schema,
        "UPDATE {schema}.events SET stamped = %s WHERE session_id = %s",
        (Jsonb({"broken": True}), root.hex),
    )
    try:
        rows = await store.select_cleanup(CleanupSelector(metadata={"keep": True}), limit=1)
        assert len(rows) == 1
        assert rows[0].root_id == root.hex
        assert len(rows[0].revision) == 64
    finally:
        await store.close()


async def test_guard_rejects_metadata_history_membership_and_activity_changes(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    roots = [uuid4() for _ in range(4)]
    for root in roots:
        await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", metadata={"group": "x"}))
    selected = {
        row.root_id: row.revision
        for row in await store.select_cleanup(CleanupSelector(metadata={"group": "x"}), limit=10)
    }
    await store.patch_header(roots[0].hex, metadata={"changed": True})
    await store.append(roots[1].hex, [TextPart(sample_id="sample", text="history")])
    child = uuid4()
    await store.create(SessionHeader(id=child.hex, root_id=roots[2].hex, parent_id=roots[2].hex, agent="Bot", depth=1))
    _execute(
        postgres_dsn,
        pg_schema,
        "INSERT INTO {schema}.coordinator_roots (root_id) VALUES (%s)",
        (roots[3].hex,),
    )
    _execute(
        postgres_dsn,
        pg_schema,
        "INSERT INTO {schema}.coordinator_activity (root_id, actor_id, active) VALUES (%s, %s, true)",
        (roots[3].hex, roots[3].hex),
    )
    try:
        for root in roots[:3]:
            with pytest.raises(CleanupChanged):
                await store.delete_tree(root.hex, allow_active=True, expected_revision=selected[root.hex])
        async with store._connection() as conn, conn.transaction():
            with pytest.raises(CleanupChanged):
                await store._delete_tree(
                    conn,
                    roots[3].hex,
                    allow_active=False,
                    expected_revision=selected[roots[3].hex],
                )
        assert all(header is not None for header in [await store.header(root.hex) for root in roots])
    finally:
        await store.close()


async def test_revision_ignores_ownership_and_delete_control_requests_but_tracks_mutating_requests(
    postgres_dsn: str, pg_schema: str
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot", metadata={"group": "x"}))
    before = (await store.select_cleanup(CleanupSelector(root_ids=[root]), limit=1))[0]
    _execute(
        postgres_dsn,
        pg_schema,
        "INSERT INTO {schema}.coordinator_roots (root_id, owner_instance, generation, expires_at)"
        " VALUES (%s, 'owner', 2, clock_timestamp() + interval '1 hour')",
        (root.hex,),
    )
    _execute(
        postgres_dsn,
        pg_schema,
        "INSERT INTO {schema}.coordinator_requests"
        " (request_id, root_id, destination_instance, destination_generation, envelope, deadline)"
        " VALUES (%s, %s, 'owner', 2, %s, clock_timestamp() + interval '1 hour')",
        (uuid4(), root.hex, Jsonb({"operation": "delete"})),
    )
    unchanged = (await store.select_cleanup(CleanupSelector(root_ids=[root]), limit=1))[0]
    _execute(
        postgres_dsn,
        pg_schema,
        "INSERT INTO {schema}.coordinator_requests"
        " (request_id, root_id, destination_instance, destination_generation, envelope, deadline)"
        " VALUES (%s, %s, 'owner', 2, %s, clock_timestamp() + interval '1 hour')",
        (uuid4(), root.hex, Jsonb({"operation": "send", "input": "go"})),
    )
    changed = (await store.select_cleanup(CleanupSelector(root_ids=[root]), limit=1))[0]
    try:
        assert unchanged.revision == before.revision
        assert not before.active
        assert changed.revision != before.revision
        assert changed.active
    finally:
        await store.close()


async def test_guard_reports_changed_when_a_selected_root_becomes_a_child(postgres_dsn: str, pg_schema: str) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root, parent = uuid4(), uuid4()
    await store.create(SessionHeader(id=root.hex, root_id=root.hex, agent="Bot"))
    await store.create(SessionHeader(id=parent.hex, root_id=parent.hex, agent="Bot"))
    selected = (await store.select_cleanup(CleanupSelector(root_ids=[root]), limit=1))[0]
    header = await store.header(root.hex)
    assert header is not None
    await store.put_header(header.model_copy(update={"root_id": parent.hex, "parent_id": parent.hex, "depth": 1}))
    try:
        with pytest.raises(CleanupChanged):
            await store.delete_tree(root.hex, allow_active=True, expected_revision=selected.revision)
        assert await store.header(root.hex) is not None
    finally:
        await store.close()


async def test_migration_backfills_syncs_rolls_back_and_repeats(postgres_dsn: str, pg_schema: str) -> None:
    stamp = datetime(2024, 3, 2, 1, tzinfo=UTC)
    root = SessionHeader(id=uuid4().hex, root_id=None, agent="Bot", created_at=stamp, updated_at=stamp)
    _install_v8(postgres_dsn, pg_schema)
    _execute(
        postgres_dsn,
        pg_schema,
        "INSERT INTO {schema}.sessions (id, header, metadata, parent_id, created_at, last_seq)"
        " VALUES (%s, %s, %s, NULL, %s, 0)",
        (root.id, Jsonb(root.model_dump(mode="json")), Jsonb({}), root.created_at),
    )
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    assert _query(postgres_dsn, pg_schema, "SELECT updated_at FROM {schema}.sessions") == [(stamp,)]
    await store.put_header(root)
    stored = await store.header(root.id)
    assert stored is not None
    native = _query(postgres_dsn, pg_schema, "SELECT updated_at FROM {schema}.sessions")[0][0]
    assert native == stored.updated_at
    assert native > stamp
    await store.setup()
    assert _query(postgres_dsn, pg_schema, "SELECT max(version) FROM {schema}.schema_version") == [(len(MIGRATIONS),)]
    await store.close()

    rollback_schema = f"{pg_schema}_rollback"
    _install_v8(postgres_dsn, rollback_schema)
    _execute(
        postgres_dsn,
        rollback_schema,
        "CREATE FUNCTION {schema}.reject_v9() RETURNS trigger LANGUAGE plpgsql AS $$"
        " BEGIN IF NEW.version = 9 THEN RAISE EXCEPTION 'stop'; END IF; RETURN NEW; END $$",
    )
    _execute(
        postgres_dsn,
        rollback_schema,
        "CREATE TRIGGER reject_v9 BEFORE INSERT ON {schema}.schema_version"
        " FOR EACH ROW EXECUTE FUNCTION {schema}.reject_v9()",
    )
    interrupted = PostgresStore(postgres_dsn, schema=rollback_schema)
    with pytest.raises(psycopg.Error, match="stop"):
        await interrupted.setup()
    assert (
        _query(
            postgres_dsn,
            rollback_schema,
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = %s AND table_name = 'sessions' AND column_name = 'updated_at'",
            (rollback_schema,),
        )
        == []
    )
    assert _query(postgres_dsn, rollback_schema, "SELECT max(version) FROM {schema}.schema_version") == [(8,)]
    await interrupted.close()
    _execute(postgres_dsn, rollback_schema, "DROP TRIGGER reject_v9 ON {schema}.schema_version")
    _execute(postgres_dsn, rollback_schema, "DROP FUNCTION {schema}.reject_v9()")
    recovered = PostgresStore(postgres_dsn, schema=rollback_schema)
    await recovered.setup()
    await recovered.setup()
    assert _query(postgres_dsn, rollback_schema, "SELECT max(version) FROM {schema}.schema_version") == [
        (len(MIGRATIONS),)
    ]
    await recovered.close()


async def test_replacement_timestamp_is_stamped_after_waiting_for_write_lock(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = await _store(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid4().hex, agent="Bot", updated_at=datetime(2000, 1, 1, tzinfo=UTC))
    await store.create(root)
    original = store._guard_session
    entered, release = asyncio.Event(), asyncio.Event()

    async def gated(*args: Any) -> None:
        await original(*args)
        entered.set()
        await release.wait()

    monkeypatch.setattr(store, "_guard_session", gated)
    task = asyncio.create_task(store.put_header(root))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        cutoff = datetime.now(UTC)
        release.set()
        await task
        assert await store.select_cleanup(CleanupSelector(inactive_before=cutoff), limit=1) == []
        stored = await store.header(root.id)
        assert stored is not None and stored.updated_at >= cutoff
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await store.close()
