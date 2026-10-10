from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from tantra.ask import Approval, FreeTextResponse
from tantra.errors import CorruptLog
from tantra.events import AgentFinished, AskAnswered, AskRaised, SessionHeader, Stamped, TextDelta
from tantra.stores import postgres
from tantra.stores.postgres import MIGRATIONS, PostgresStore

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")
Jsonb = pytest.importorskip("psycopg.types.json").Jsonb


def _current(stamped: Stamped) -> Jsonb:
    payload = base64.b64encode(stamped.model_dump_json().encode()).decode()
    return Jsonb({"tantra_stamped": {"version": 1, "payload": payload}})


def _legacy(stamped: Stamped) -> Jsonb:
    return Jsonb(stamped.model_dump(mode="json"))


def _install_v11(dsn: str, schema: str) -> PostgresStore:
    store = PostgresStore(dsn, schema=schema)
    with psycopg.connect(dsn) as conn:
        conn.execute(store._sql("CREATE SCHEMA {schema}"))
        conn.execute(store._sql("CREATE TABLE {schema}.schema_version(version int NOT NULL)"))
        for version, statements in enumerate(MIGRATIONS[:11], start=1):
            for statement in statements:
                conn.execute(store._sql(statement))
            conn.execute(store._sql("INSERT INTO {schema}.schema_version VALUES(%s)"), (version,))
    return store


def _insert_session(
    dsn: str,
    store: PostgresStore,
    header: SessionHeader,
    rows: list[tuple[int, Jsonb]],
) -> None:
    stored = header.model_copy(update={"last_seq": len(rows)})
    with psycopg.connect(dsn) as conn:
        conn.execute(
            store._sql(
                "INSERT INTO {schema}.sessions(id,header,metadata,parent_id,created_at,last_seq)"
                " VALUES(%s,%s,%s,%s,%s,%s)"
            ),
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
                store._sql("INSERT INTO {schema}.events(session_id,seq,stamped) VALUES(%s,%s,%s)"),
                [(stored.id, seq, body) for seq, body in rows],
            )


async def _seed_history(
    dsn: str,
    store: PostgresStore,
    header: SessionHeader,
    total: int,
    event: AskRaised,
) -> None:
    await store.create(header)
    stored = header.model_copy(update={"last_seq": total})
    filler = _legacy(Stamped(seq=1, event=AgentFinished()))
    ask = _current(Stamped(seq=total, event=event))
    with psycopg.connect(dsn) as conn:
        conn.execute(
            store._sql("UPDATE {schema}.sessions SET header=%s,last_seq=%s WHERE id=%s"),
            (Jsonb(stored.model_dump(mode="json")), total, header.id),
        )
        conn.execute(
            store._sql(
                "INSERT INTO {schema}.events(session_id,seq,stamped)"
                " SELECT %s,n,jsonb_set(%s::jsonb,'{{seq}}',to_jsonb(n),false)"
                " FROM generate_series(1,%s) n"
            ),
            (header.id, filler, total - 1),
        )
        conn.execute(
            store._sql(
                "INSERT INTO {schema}.journal_index(actor_id,seq,event_type)"
                " SELECT %s,n,'agent_finished' FROM generate_series(1,%s) n"
            ),
            (header.id, total - 1),
        )
        conn.execute(
            store._sql("INSERT INTO {schema}.events(session_id,seq,stamped) VALUES(%s,%s,%s)"),
            (header.id, total, ask),
        )
        conn.execute(
            store._sql(
                "INSERT INTO {schema}.journal_index(actor_id,seq,event_type,ask_id) VALUES(%s,%s,'ask_raised',%s)"
            ),
            (header.id, total, event.ask_id.encode()),
        )


def _lookup_plan(dsn: str, store: PostgresStore, root_id: str, ask_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            store._sql(
                "EXPLAIN (ANALYZE, FORMAT JSON) WITH RECURSIVE actors AS ("
                " SELECT id, created_at, 0 AS depth FROM {schema}.sessions WHERE id = %s"
                " UNION ALL SELECT s.id, s.created_at, a.depth + 1"
                " FROM {schema}.sessions s JOIN actors a ON s.parent_id = a.id"
                ") SELECT c.actor_id, e.stamped FROM (SELECT i.actor_id, i.seq FROM actors a"
                " JOIN {schema}.journal_index i ON i.actor_id = a.id"
                " WHERE i.ask_id = %s AND i.event_type = 'ask_raised'"
                ' ORDER BY a.depth, a.created_at, a.id COLLATE "C", i.seq LIMIT 2) c'
                " JOIN LATERAL (SELECT stamped FROM {schema}.events"
                " WHERE session_id = c.actor_id AND seq = c.seq LIMIT 1) e ON true"
            ),
            (root_id, ask_id.encode()),
        ).fetchone()
    return row[0][0]["Plan"]


def _plan_nodes(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value, *(node for item in value.values() for node in _plan_nodes(item))]
    if isinstance(value, list):
        return [node for item in value for node in _plan_nodes(item)]
    return []


async def test_lookup_ask_is_bounded_root_scoped_and_fails_closed_on_duplicates(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    created = datetime.now(UTC)
    root = SessionHeader(id=uuid.uuid4().hex, agent="root", created_at=created)
    child = SessionHeader(id=uuid.uuid4().hex, parent_id=root.id, agent="legacy", created_at=created)
    other = SessionHeader(id=uuid.uuid4().hex, agent="other", created_at=created)
    for header in (root, child, other):
        await store.create(header)
    ask_id = uuid.uuid4().hex
    event = AskRaised(ask_id=ask_id, request=Approval(title="original", body="body\x00雪"))
    other_event = AskRaised(ask_id=ask_id, request=Approval(title="other"))
    await store.append(child.id, [TextDelta(text=str(index)) for index in range(20)])
    await store.append(child.id, [event, AskAnswered(ask_id=ask_id, response=FreeTextResponse(text="done"))])
    await store.append(other.id, [other_event])

    parse = postgres._parse
    parsed: list[str] = []

    def counted(actor_id: str, raw: Any) -> Stamped:
        parsed.append(actor_id)
        return parse(actor_id, raw)

    monkeypatch.setattr(postgres, "_parse", counted)
    assert await store.lookup_ask(root.id, ask_id) == (child.id, Stamped(seq=21, event=event))
    assert parsed == [child.id]
    assert await store.lookup_ask(other.id, ask_id) == (other.id, Stamped(seq=1, event=other_event))
    assert await store.lookup_ask(root.id, "missing") is None

    await store.append(root.id, [AskRaised(ask_id=ask_id, request=Approval(title="duplicate"))])
    with pytest.raises(ValueError, match="ambiguous ask_id"):
        await store.lookup_ask(root.id, ask_id)
    await store.close()


async def test_warm_lookup_uses_ask_index_and_reads_one_body_from_large_journals(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root = SessionHeader(id=uuid.uuid4().hex, agent="root")
    child = SessionHeader(id=uuid.uuid4().hex, root_id=root.id, parent_id=root.id, depth=1, agent="child")
    root_ask = AskRaised(ask_id=uuid.uuid4().hex, request=Approval(title="root"))
    child_ask = AskRaised(ask_id=uuid.uuid4().hex, request=Approval(title="child"))
    await _seed_history(postgres_dsn, store, root, 100_000, root_ask)
    await _seed_history(postgres_dsn, store, child, 4_000, child_ask)
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(store._sql("ANALYZE {schema}.sessions"))
        conn.execute(store._sql("ANALYZE {schema}.journal_index"))
        conn.execute(store._sql("ANALYZE {schema}.events"))

    parse = postgres._parse
    parsed: list[str] = []

    def counted(actor_id: str, raw: Any) -> Stamped:
        parsed.append(actor_id)
        return parse(actor_id, raw)

    monkeypatch.setattr(postgres, "_parse", counted)
    assert await store.lookup_ask(root.id, root_ask.ask_id) == (root.id, Stamped(seq=100_000, event=root_ask))
    assert await store.lookup_ask(root.id, child_ask.ask_id) == (child.id, Stamped(seq=4_000, event=child_ask))
    assert parsed == [root.id, child.id]

    for ask_id in (root_ask.ask_id, child_ask.ask_id):
        nodes = _plan_nodes(_lookup_plan(postgres_dsn, store, root.id, ask_id))
        assert any(node.get("Index Name") == "journal_ask_idx" for node in nodes)
        body_reads = [
            node.get("Actual Rows", 0) * node.get("Actual Loops", 0)
            for node in nodes
            if node.get("Relation Name") == "events"
        ]
        assert body_reads and sum(body_reads) == 1
    await store.close()


async def test_ambiguous_ask_lookup_reads_only_two_original_bodies(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    await store.setup()
    root = SessionHeader(id=uuid.uuid4().hex, agent="root")
    await store.create(root)
    ask = AskRaised(ask_id=uuid.uuid4().hex, request=Approval(title="duplicate", body="x" * 1024))
    await store.append(root.id, [ask.model_copy(deep=True) for _ in range(1000)])
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute(store._sql("ANALYZE {schema}.sessions"))
        conn.execute(store._sql("ANALYZE {schema}.journal_index"))
        conn.execute(store._sql("ANALYZE {schema}.events"))

    with pytest.raises(ValueError, match="ambiguous"):
        await store.lookup_ask(root.id, ask.ask_id)
    nodes = _plan_nodes(_lookup_plan(postgres_dsn, store, root.id, ask.ask_id))
    body_reads = [
        node.get("Actual Rows", 0) * node.get("Actual Loops", 0)
        for node in nodes
        if node.get("Relation Name") == "events"
    ]
    assert body_reads and sum(body_reads) == 2
    await store.close()


async def test_ask_migration_backfills_both_codecs_in_bounded_pages_and_rolls_back_cleanly(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed = _install_v11(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="root")
    current = AskRaised(ask_id="current\x00雪", request=Approval(title="current", body="one\x00"))
    legacy = AskRaised(ask_id="legacy-雪", request=Approval(title="legacy", body="two"))
    rows = [(1, _current(Stamped(seq=1, event=current)))]
    rows.extend((seq, _legacy(Stamped(seq=seq, event=TextDelta(text=str(seq))))) for seq in range(2, 1002))
    rows.append((1002, _legacy(Stamped(seq=1002, event=legacy))))
    _insert_session(postgres_dsn, seed, root, rows)

    class PagingStore(PostgresStore):
        page_parse_counts: list[int] = []
        page_parse_count = 0

        async def _index_events(
            self,
            conn: Any,
            items: Any,
            *,
            operational: bool = True,
            backfill: bool = False,
            include_asks: bool = True,
        ) -> None:
            if backfill and not operational and include_asks:
                self.page_parse_counts.append(self.page_parse_count)
                self.page_parse_count = 0
            await super()._index_events(
                conn,
                items,
                operational=operational,
                backfill=backfill,
                include_asks=include_asks,
            )

    store = PagingStore(postgres_dsn, schema=pg_schema)
    parse = postgres._parse

    def counted(actor_id: str, raw: Any) -> Stamped:
        store.page_parse_count += 1
        return parse(actor_id, raw)

    monkeypatch.setattr(postgres, "_parse", counted)
    with psycopg.connect(postgres_dsn) as conn:
        before = conn.execute(
            store._sql("SELECT seq,stamped FROM {schema}.events WHERE session_id=%s ORDER BY seq"), (root.id,)
        ).fetchall()
        conn.execute(
            store._sql(
                "CREATE FUNCTION {schema}.reject_v12() RETURNS trigger LANGUAGE plpgsql AS $$"
                " BEGIN IF NEW.version=12 THEN RAISE EXCEPTION 'stop migration';"
                " END IF; RETURN NEW; END $$"
            )
        )
        conn.execute(
            store._sql(
                "CREATE TRIGGER reject_v12 BEFORE INSERT ON {schema}.schema_version"
                " FOR EACH ROW EXECUTE FUNCTION {schema}.reject_v12()"
            )
        )

    with pytest.raises(psycopg.Error, match="stop migration"):
        await store.setup()
    assert store.page_parse_counts == [1000, 2]
    with psycopg.connect(postgres_dsn) as conn:
        assert conn.execute(store._sql("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (11,)
        assert (
            conn.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema=%s"
                " AND table_name='journal_index' AND column_name='ask_id'",
                (pg_schema,),
            ).fetchone()
            is None
        )
        assert (
            conn.execute(
                store._sql("SELECT seq,stamped FROM {schema}.events WHERE session_id=%s ORDER BY seq"), (root.id,)
            ).fetchall()
            == before
        )
        conn.execute(store._sql("DROP TRIGGER reject_v12 ON {schema}.schema_version"))

    await store.setup()
    await store.setup()
    assert store.page_parse_counts == [1000, 2, 1000, 2]
    assert max(store.page_parse_counts) <= 1000
    assert await store.lookup_ask(root.id, current.ask_id) == (root.id, Stamped(seq=1, event=current))
    assert await store.lookup_ask(root.id, legacy.ask_id) == (root.id, Stamped(seq=1002, event=legacy))
    with psycopg.connect(postgres_dsn) as conn:
        assert conn.execute(store._sql("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (12,)
        assert (
            conn.execute(
                store._sql("SELECT seq,stamped FROM {schema}.events WHERE session_id=%s ORDER BY seq"), (root.id,)
            ).fetchall()
            == before
        )
        assert conn.execute(
            store._sql("SELECT seq,ask_id FROM {schema}.journal_index WHERE actor_id=%s ORDER BY seq"), (root.id,)
        ).fetchall() == [(1, current.ask_id.encode()), (1002, legacy.ask_id.encode())]
        definition = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname=%s AND indexname='journal_ask_idx'", (pg_schema,)
        ).fetchone()[0]
        assert "(actor_id, ask_id, seq)" in definition and "WHERE (ask_id IS NOT NULL)" in definition
    await store.close()


async def test_corrupt_ask_backfill_rolls_back_then_retries_after_repair(postgres_dsn: str, pg_schema: str) -> None:
    store = _install_v11(postgres_dsn, pg_schema)
    root = SessionHeader(id=uuid.uuid4().hex, agent="root")
    event = AskRaised(ask_id=uuid.uuid4().hex, request=Approval(title="recover"))
    _insert_session(
        postgres_dsn,
        store,
        root,
        [(1, _current(Stamped(seq=1, event=event))), (2, Jsonb({"seq": 2}))],
    )
    with psycopg.connect(postgres_dsn) as conn:
        before = conn.execute(
            store._sql("SELECT seq,stamped FROM {schema}.events WHERE session_id=%s ORDER BY seq"), (root.id,)
        ).fetchall()

    with pytest.raises(CorruptLog):
        await store.setup()
    with psycopg.connect(postgres_dsn) as conn:
        assert conn.execute(store._sql("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (11,)
        assert (
            conn.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema=%s"
                " AND table_name='journal_index' AND column_name='ask_id'",
                (pg_schema,),
            ).fetchone()
            is None
        )
        assert (
            conn.execute(
                store._sql("SELECT seq,stamped FROM {schema}.events WHERE session_id=%s ORDER BY seq"), (root.id,)
            ).fetchall()
            == before
        )
        fixed = _legacy(Stamped(seq=2, event=TextDelta(text="repaired")))
        conn.execute(
            store._sql("UPDATE {schema}.events SET stamped=%s WHERE session_id=%s AND seq=2"),
            (fixed, root.id),
        )

    await store.setup()
    await store.setup()
    assert await store.lookup_ask(root.id, event.ask_id) == (root.id, Stamped(seq=1, event=event))
    with psycopg.connect(postgres_dsn) as conn:
        after = conn.execute(
            store._sql("SELECT seq,stamped FROM {schema}.events WHERE session_id=%s ORDER BY seq"), (root.id,)
        ).fetchall()
        assert after == [before[0], (2, fixed.obj)]
        assert conn.execute(store._sql("SELECT max(version) FROM {schema}.schema_version")).fetchone() == (12,)
    await store.close()
