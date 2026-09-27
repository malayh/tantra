from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import AsyncIterator, Sequence
from typing import Any

from pydantic import ValidationError

from tantra.errors import (
    CoordinatorUnavailable,
    CorruptLog,
    InvalidCommandReuse,
    ModelChangeBusy,
    SessionExists,
    SessionNotFound,
    TantraError,
)
from tantra.events import (
    InputQueued,
    SessionEvent,
    SessionHeader,
    SessionStatus,
    Stamped,
    Usage,
)
from tantra.memory import MemoryRecord
from tantra.stores.base import UNSET, EnqueueResult, apply_patch, reduce_header

try:
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False

MISSING_PSYCOPG = "PostgresStore needs psycopg: install tantra-harness[postgres]"
EVENT_ENVELOPE = "tantra_stamped"
EVENT_VERSION = 1

MIGRATIONS: tuple[tuple[str, ...], ...] = (
    (
        """
        CREATE TABLE IF NOT EXISTS {schema}.sessions (
            id text PRIMARY KEY,
            header jsonb NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            parent_id text,
            created_at timestamptz NOT NULL,
            last_seq bigint NOT NULL DEFAULT 0
        )
        """,
        "CREATE INDEX IF NOT EXISTS sessions_metadata_idx ON {schema}.sessions USING gin (metadata)",
        "CREATE INDEX IF NOT EXISTS sessions_parent_idx ON {schema}.sessions (parent_id)",
        """
        CREATE TABLE IF NOT EXISTS {schema}.events (
            session_id text NOT NULL,
            seq bigint NOT NULL,
            stamped jsonb NOT NULL,
            PRIMARY KEY (session_id, seq)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS {schema}.memories (
            id text PRIMARY KEY,
            row jsonb NOT NULL
        )
        """,
    ),
    (
        "CREATE INDEX IF NOT EXISTS memories_metadata_idx"
        " ON {schema}.memories USING gin ((row->'metadata') jsonb_path_ops)",
    ),
    (
        """
        CREATE TABLE IF NOT EXISTS {schema}.coordinator_roots (
            root_id text PRIMARY KEY,
            owner_instance text,
            generation bigint NOT NULL DEFAULT 0,
            expires_at timestamptz,
            writer_connection text,
            recovery jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS {schema}.coordinator_activity (
            root_id text NOT NULL REFERENCES {schema}.coordinator_roots(root_id) ON DELETE CASCADE,
            actor_id text NOT NULL,
            active boolean NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (root_id, actor_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS {schema}.coordinator_requests (
            request_id uuid PRIMARY KEY,
            root_id text NOT NULL REFERENCES {schema}.coordinator_roots(root_id) ON DELETE CASCADE,
            destination_instance text NOT NULL,
            destination_generation bigint NOT NULL,
            envelope jsonb NOT NULL,
            reply jsonb,
            deadline timestamptz NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            completed_at timestamptz
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS coordinator_requests_destination_idx
        ON {schema}.coordinator_requests
        (destination_instance, destination_generation, created_at, request_id)
        WHERE reply IS NULL
        """,
        """
        CREATE INDEX IF NOT EXISTS coordinator_requests_cleanup_idx
        ON {schema}.coordinator_requests (completed_at, deadline)
        """,
        """
        CREATE TABLE IF NOT EXISTS {schema}.coordinator_changes (
            id bigserial PRIMARY KEY,
            root_id text NOT NULL,
            kind text NOT NULL,
            actor_id text,
            seq bigint,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS coordinator_changes_root_idx
        ON {schema}.coordinator_changes (root_id, id)
        """,
        """
        CREATE OR REPLACE FUNCTION {schema}.assert_coordinated_fence(candidate text) RETURNS void AS $$
        DECLARE
            enrolled boolean;
            owner text;
            fence_generation bigint;
            fence_expires_at timestamptz;
        BEGIN
            IF candidate IS NULL THEN
                RETURN;
            END IF;
            PERFORM pg_advisory_xact_lock(hashtextextended(candidate, 0));
            SELECT true, owner_instance, generation, expires_at
            INTO enrolled, owner, fence_generation, fence_expires_at
            FROM {schema}.coordinator_roots WHERE root_id = candidate FOR UPDATE;
            IF enrolled AND (
                current_setting('tantra.root_id', true) = candidate
                AND owner = current_setting('tantra.instance_id', true)
                AND fence_generation::text = current_setting('tantra.generation', true)
                AND fence_expires_at > clock_timestamp()
            ) IS DISTINCT FROM TRUE THEN
                RAISE EXCEPTION 'coordinated root % requires a valid ownership fence', candidate
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$ LANGUAGE plpgsql
        """,
        """
        CREATE OR REPLACE FUNCTION {schema}.guard_coordinated_write() RETURNS trigger AS $$
        DECLARE
            root text;
            old_root text;
            new_root text;
        BEGIN
            IF TG_TABLE_NAME = 'events' THEN
                IF TG_OP <> 'INSERT' THEN
                    SELECT COALESCE(NULLIF(header->>'root_id', ''), id) INTO old_root
                    FROM {schema}.sessions WHERE id = OLD.session_id;
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    SELECT COALESCE(NULLIF(header->>'root_id', ''), id) INTO new_root
                    FROM {schema}.sessions WHERE id = NEW.session_id;
                END IF;
            ELSE
                IF TG_OP <> 'INSERT' THEN
                    old_root := COALESCE(NULLIF(OLD.header->>'root_id', ''), OLD.id);
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    new_root := COALESCE(NULLIF(NEW.header->>'root_id', ''), NEW.id);
                END IF;
                IF TG_OP = 'UPDATE'
                    AND OLD.id = NEW.id
                    AND old_root IS NOT DISTINCT FROM new_root
                    AND OLD.last_seq = NEW.last_seq
                    AND OLD.parent_id IS NOT DISTINCT FROM NEW.parent_id
                    AND OLD.created_at = NEW.created_at
                    AND (OLD.header - ARRAY['title', 'metadata', 'updated_at'])
                        = (NEW.header - ARRAY['title', 'metadata', 'updated_at']) THEN
                    RETURN NEW;
                END IF;
            END IF;
            IF old_root IS NOT NULL AND new_root IS NOT NULL
                AND old_root IS DISTINCT FROM new_root THEN
                PERFORM {schema}.assert_coordinated_fence(LEAST(old_root, new_root));
                PERFORM {schema}.assert_coordinated_fence(GREATEST(old_root, new_root));
            ELSE
                PERFORM {schema}.assert_coordinated_fence(COALESCE(old_root, new_root));
            END IF;
            IF TG_OP = 'UPDATE' AND old_root IS DISTINCT FROM new_root
                AND EXISTS (
                    SELECT 1 FROM {schema}.coordinator_roots
                    WHERE root_id = old_root OR root_id = new_root
                ) THEN
                RAISE EXCEPTION 'cannot move a row across coordinated roots'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'sessions' THEN
                IF OLD.id IS DISTINCT FROM NEW.id AND EXISTS (
                    SELECT 1 FROM {schema}.coordinator_roots
                    WHERE root_id = old_root OR root_id = new_root
                ) THEN
                    RAISE EXCEPTION 'cannot move a row across coordinated roots'
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            root := CASE WHEN TG_OP = 'INSERT' THEN new_root ELSE old_root END;
            RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$ LANGUAGE plpgsql
        """,
        "DROP TRIGGER IF EXISTS sessions_coordinator_guard ON {schema}.sessions",
        """
        CREATE TRIGGER sessions_coordinator_guard
        BEFORE INSERT OR UPDATE OR DELETE ON {schema}.sessions
        FOR EACH ROW EXECUTE FUNCTION {schema}.guard_coordinated_write()
        """,
        "DROP TRIGGER IF EXISTS events_coordinator_guard ON {schema}.events",
        """
        CREATE TRIGGER events_coordinator_guard
        BEFORE INSERT OR UPDATE OR DELETE ON {schema}.events
        FOR EACH ROW EXECUTE FUNCTION {schema}.guard_coordinated_write()
        """,
    ),
    (
        """
        CREATE OR REPLACE FUNCTION {schema}.guard_coordinated_write() RETURNS trigger AS $$
        DECLARE
            root text;
            old_root text;
            new_root text;
        BEGIN
            IF TG_TABLE_NAME = 'events' THEN
                IF TG_OP <> 'INSERT' THEN
                    SELECT COALESCE(NULLIF(header->>'root_id', ''), id) INTO old_root
                    FROM {schema}.sessions WHERE id = OLD.session_id;
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    SELECT COALESCE(NULLIF(header->>'root_id', ''), id) INTO new_root
                    FROM {schema}.sessions WHERE id = NEW.session_id;
                END IF;
            ELSE
                IF TG_OP <> 'INSERT' THEN
                    old_root := COALESCE(NULLIF(OLD.header->>'root_id', ''), OLD.id);
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    new_root := COALESCE(NULLIF(NEW.header->>'root_id', ''), NEW.id);
                END IF;
                IF TG_OP = 'UPDATE'
                    AND OLD.id = NEW.id
                    AND old_root IS NOT DISTINCT FROM new_root
                    AND OLD.last_seq = NEW.last_seq
                    AND OLD.parent_id IS NOT DISTINCT FROM NEW.parent_id
                    AND OLD.created_at = NEW.created_at
                    AND (OLD.header - ARRAY['title', 'metadata', 'updated_at'])
                        = (NEW.header - ARRAY['title', 'metadata', 'updated_at']) THEN
                    RETURN NEW;
                END IF;
                IF TG_OP = 'UPDATE'
                    AND current_setting('tantra.model_patch_root', true) = old_root
                    AND OLD.id = NEW.id
                    AND old_root IS NOT DISTINCT FROM new_root
                    AND OLD.last_seq = NEW.last_seq
                    AND OLD.parent_id IS NOT DISTINCT FROM NEW.parent_id
                    AND OLD.created_at = NEW.created_at
                    AND (OLD.header - ARRAY['title', 'metadata', 'updated_at', 'model'])
                        = (NEW.header - ARRAY['title', 'metadata', 'updated_at', 'model']) THEN
                    RETURN NEW;
                END IF;
            END IF;
            IF old_root IS NOT NULL AND new_root IS NOT NULL
                AND old_root IS DISTINCT FROM new_root THEN
                PERFORM {schema}.assert_coordinated_fence(LEAST(old_root, new_root));
                PERFORM {schema}.assert_coordinated_fence(GREATEST(old_root, new_root));
            ELSE
                PERFORM {schema}.assert_coordinated_fence(COALESCE(old_root, new_root));
            END IF;
            IF TG_OP = 'UPDATE' AND old_root IS DISTINCT FROM new_root
                AND EXISTS (
                    SELECT 1 FROM {schema}.coordinator_roots
                    WHERE root_id = old_root OR root_id = new_root
                ) THEN
                RAISE EXCEPTION 'cannot move a row across coordinated roots'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'sessions' THEN
                IF OLD.id IS DISTINCT FROM NEW.id AND EXISTS (
                    SELECT 1 FROM {schema}.coordinator_roots
                    WHERE root_id = old_root OR root_id = new_root
                ) THEN
                    RAISE EXCEPTION 'cannot move a row across coordinated roots'
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            root := CASE WHEN TG_OP = 'INSERT' THEN new_root ELSE old_root END;
            RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$ LANGUAGE plpgsql
        """,
    ),
)


class PostgresStore:
    def __init__(self, dsn: str, schema: str = "tantra") -> None:
        if not HAS_PSYCOPG:
            raise TantraError(MISSING_PSYCOPG)
        self.dsn = dsn
        self.schema = schema
        self._ident = sql.Identifier(schema)
        self._key = _advisory_key(schema)
        self._conn: psycopg.AsyncConnection | None = None
        self._lock = asyncio.Lock()
        self._vector: bool | None = None

    async def setup(self) -> None:
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(%s)", (self._key,))
                await conn.execute(self._sql("CREATE SCHEMA IF NOT EXISTS {schema}"))
                await conn.execute(
                    self._sql("CREATE TABLE IF NOT EXISTS {schema}.schema_version (version int NOT NULL)")
                )
                cursor = await conn.execute(self._sql("SELECT coalesce(max(version), 0) FROM {schema}.schema_version"))
                current = (await cursor.fetchone())[0]
                for version, statements in enumerate(MIGRATIONS, start=1):
                    if version <= current:
                        continue
                    for statement in statements:
                        await conn.execute(self._sql(statement))
                    await conn.execute(
                        self._sql("INSERT INTO {schema}.schema_version (version) VALUES (%s)"), (version,)
                    )
            self._vector = await self._install_vector(conn)

    async def create(self, header: SessionHeader) -> None:
        stored = header.model_copy(deep=True)
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(
                self._sql(
                    "INSERT INTO {schema}.sessions (id, header, metadata, parent_id, created_at, last_seq)"
                    " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"
                ),
                (
                    stored.id,
                    _json(stored),
                    Jsonb(stored.metadata),
                    stored.parent_id,
                    stored.created_at,
                    stored.last_seq,
                ),
            )
            if cursor.rowcount == 0:
                raise SessionExists(header.id)

    async def header(self, sid: str) -> SessionHeader | None:
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(
                self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s"), (sid,)
            )
            row = await cursor.fetchone()
        return _hydrate(row) if row is not None else None

    async def put_header(self, h: SessionHeader) -> None:
        stored = h.model_copy(deep=True)
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                await self._guard_session(conn, h.id)
                cursor = await conn.execute(
                    self._sql("SELECT last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (h.id,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(h.id)
                stored.last_seq = row[0]
                await conn.execute(
                    self._sql(
                        "UPDATE {schema}.sessions SET header = %s, metadata = %s, parent_id = %s, created_at = %s"
                        " WHERE id = %s"
                    ),
                    (_json(stored), Jsonb(stored.metadata), stored.parent_id, stored.created_at, stored.id),
                )

    async def patch_header(
        self,
        sid: str,
        *,
        title: str | None = UNSET,
        model: str | None = UNSET,
        status: SessionStatus = UNSET,
        pending_ask: str | None = UNSET,
        usage: Usage = UNSET,
        metadata: dict[str, Any] = UNSET,
        finished: bool = UNSET,
    ) -> SessionHeader:
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                if any(value is not UNSET for value in (status, pending_ask, usage, finished)):
                    await self._guard_session(conn, sid)
                model_root = await self._prepare_model_patch(conn, sid) if model is not UNSET else None
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (sid,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                current = SessionHeader.model_validate(row[0])
                current.last_seq = row[1]
                root_id = current.root_id or current.id
                if model_root is not None and root_id != model_root:
                    raise CoordinatorUnavailable("session root changed while applying model patch")
                stored = apply_patch(
                    current,
                    title=title,
                    model=model,
                    status=status,
                    pending_ask=pending_ask,
                    usage=usage,
                    metadata=metadata,
                    finished=finished,
                )
                await conn.execute(
                    self._sql("UPDATE {schema}.sessions SET header = %s, metadata = %s WHERE id = %s"),
                    (_json(stored), Jsonb(stored.metadata), sid),
                )
                await self._publish_header_change(conn, root_id, sid)
                return stored

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int:
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                await self._guard_session(conn, sid)
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (sid,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                last_seq = row[1]
                header = SessionHeader.model_validate(row[0])
                seq = last_seq
                rows = []
                for event in events:
                    seq += 1
                    rows.append((sid, seq, _event_json(Stamped(seq=seq, event=event))))
                if rows:
                    await cursor.executemany(
                        self._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"), rows
                    )
                header = reduce_header(header, events)
                header.last_seq = seq
                await conn.execute(
                    self._sql("UPDATE {schema}.sessions SET header = %s, last_seq = %s WHERE id = %s"),
                    (_json(header), seq, sid),
                )
                return seq

    async def enqueue(self, sid: str, event: InputQueued) -> EnqueueResult:
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                await self._guard_session(conn, sid)
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
                    (sid,),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                cursor = await conn.execute(
                    self._sql("SELECT seq, stamped FROM {schema}.events WHERE session_id = %s ORDER BY seq"),
                    (sid,),
                )
                for seq, raw in await cursor.fetchall():
                    existing = _parse(sid, raw).event
                    if not isinstance(existing, InputQueued) or existing.command_id != event.command_id:
                        continue
                    if existing == event:
                        return EnqueueResult(seq=seq, duplicate=True)
                    raise InvalidCommandReuse(event.command_id)
                header = SessionHeader.model_validate(row[0])
                seq = row[1] + 1
                stamped = Stamped(seq=seq, event=event)
                await conn.execute(
                    self._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"),
                    (sid, seq, _event_json(stamped)),
                )
                header = reduce_header(header, [event])
                header.last_seq = seq
                await conn.execute(
                    self._sql("UPDATE {schema}.sessions SET header = %s, last_seq = %s WHERE id = %s"),
                    (_json(header), seq, sid),
                )
                return EnqueueResult(seq=seq, duplicate=False)

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Stamped]:
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(
                self._sql(
                    "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq > %s ORDER BY seq LIMIT %s"
                ),
                (sid, after, max(limit, 0)),
            )
            rows = await cursor.fetchall()
        return [_parse(sid, row[0]) for row in rows]

    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(
                self._sql("SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq > %s ORDER BY seq"),
                (sid, from_seq),
            )
            rows = await cursor.fetchall()
        for row in rows:
            yield _parse(sid, row[0])

    async def list(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        limit: int = 50,
        before: str | None = None,
    ) -> list[SessionHeader]:
        conditions = [sql.SQL("TRUE")]
        params: dict[str, Any] = {"limit": max(limit, 0)}
        if before is not None:
            conditions.append(
                self._sql(
                    "(NOT EXISTS (SELECT 1 FROM {schema}.sessions c WHERE c.id = %(before)s)"
                    ' OR (created_at, id COLLATE "C") <'
                    ' (SELECT c.created_at, c.id COLLATE "C" FROM {schema}.sessions c WHERE c.id = %(before)s))'
                )
            )
            params["before"] = before
        if parent_id is not None:
            conditions.append(sql.SQL("parent_id = %(parent_id)s"))
            params["parent_id"] = parent_id
        if metadata:
            conditions.append(sql.SQL("metadata @> %(metadata)s"))
            params["metadata"] = Jsonb(metadata)
        query = sql.SQL(
            "SELECT header, last_seq FROM {schema}.sessions WHERE {conditions}"
            ' ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT %(limit)s'
        ).format(schema=self._ident, conditions=sql.SQL(" AND ").join(conditions))
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
        return [_hydrate(row) for row in rows]

    async def memory_put(self, row: MemoryRecord) -> None:
        async with self._lock:
            conn = await self._connection()
            if await self._has_vector(conn):
                await conn.execute(
                    self._sql(
                        "INSERT INTO {schema}.memories (id, row, embedding) VALUES (%s, %s, %s::vector)"
                        " ON CONFLICT (id) DO UPDATE SET row = excluded.row, embedding = excluded.embedding"
                    ),
                    (row.id, _json(row), _literal(row.embedding) if row.embedding is not None else None),
                )
                return
            await conn.execute(
                self._sql(
                    "INSERT INTO {schema}.memories (id, row) VALUES (%s, %s)"
                    " ON CONFLICT (id) DO UPDATE SET row = excluded.row"
                ),
                (row.id, _json(row)),
            )

    async def memory_get(self, mid: str) -> MemoryRecord | None:
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(self._sql("SELECT row FROM {schema}.memories WHERE id = %s"), (mid,))
            row = await cursor.fetchone()
        return _parse_row(mid, row[0]) if row is not None else None

    async def memory_all(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        include_dead: bool = False,
    ) -> list[MemoryRecord]:
        conditions = [sql.SQL("TRUE")]
        params: dict[str, Any] = {}
        if not include_dead:
            conditions.append(
                sql.SQL("NOT COALESCE((row->>'deleted')::boolean, false) AND row->>'superseded_by' IS NULL")
            )
        if metadata:
            conditions.append(sql.SQL("row->'metadata' @> %(metadata)s"))
            params["metadata"] = Jsonb(metadata)
        query = sql.SQL("SELECT id, row FROM {schema}.memories WHERE {conditions} ORDER BY id").format(
            schema=self._ident, conditions=sql.SQL(" AND ").join(conditions)
        )
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
        return [_parse_row(mid, raw) for mid, raw in rows]

    async def memory_search(self, vector: list[float], k: int) -> list[tuple[MemoryRecord, float]] | None:
        async with self._lock:
            conn = await self._connection()
            if not await self._has_vector(conn):
                return None
            cursor = await conn.execute(
                self._sql(
                    "SELECT id, row, embedding <=> %s::vector AS distance FROM {schema}.memories"
                    " WHERE embedding IS NOT NULL"
                    " AND NOT COALESCE((row->>'deleted')::boolean, false)"
                    " AND row->>'superseded_by' IS NULL"
                    " ORDER BY distance LIMIT %s"
                ),
                (_literal(vector), max(k, 0)),
            )
            rows = await cursor.fetchall()
        return [(_parse_row(mid, raw), float(distance)) for mid, raw, distance in rows]

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None

    def _sql(self, statement: str) -> Any:
        return sql.SQL(statement).format(schema=self._ident)

    async def _connection(self) -> Any:
        if self._conn is None or self._conn.closed:
            self._conn = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True)
        return self._conn

    async def _install_vector(self, conn: Any) -> bool:
        try:
            async with conn.transaction():
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                await conn.execute(self._sql("ALTER TABLE {schema}.memories ADD COLUMN IF NOT EXISTS embedding vector"))
        except psycopg.Error:
            return False
        return True

    async def _has_vector(self, conn: Any) -> bool:
        if self._vector is None:
            cursor = await conn.execute(
                "SELECT 1 FROM information_schema.columns"
                " WHERE table_schema = %s AND table_name = 'memories' AND column_name = 'embedding'",
                (self.schema,),
            )
            self._vector = await cursor.fetchone() is not None
        return self._vector

    async def _guard_session(self, conn: Any, sid: str) -> None:
        cursor = await conn.execute(
            self._sql("SELECT COALESCE(NULLIF(header->>'root_id', ''), id) FROM {schema}.sessions WHERE id = %s"),
            (sid,),
        )
        row = await cursor.fetchone()
        root_id = row[0] if row is not None else sid
        await conn.execute(self._sql("SELECT {schema}.assert_coordinated_fence(%s)"), (root_id,))

    async def _prepare_model_patch(self, conn: Any, sid: str) -> str:
        cursor = await conn.execute(
            self._sql("SELECT COALESCE(NULLIF(header->>'root_id', ''), id) FROM {schema}.sessions WHERE id = %s"),
            (sid,),
        )
        row = await cursor.fetchone()
        root_id = row[0] if row is not None else sid
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (root_id,))
        cursor = await conn.execute(
            self._sql("SELECT 1 FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"),
            (root_id,),
        )
        if await cursor.fetchone() is None:
            return root_id
        cursor = await conn.execute(
            self._sql(
                "SELECT EXISTS ("
                " SELECT 1 FROM {schema}.coordinator_activity WHERE root_id = %s AND active"
                " UNION ALL SELECT 1 FROM {schema}.coordinator_requests"
                " WHERE root_id = %s AND reply IS NULL AND deadline > clock_timestamp()"
                " UNION ALL SELECT 1 FROM {schema}.sessions"
                " WHERE COALESCE(NULLIF(header->>'root_id', ''), id) = %s"
                " AND header->>'status' IN ('queued', 'running', 'awaiting_input')"
                ")"
            ),
            (root_id, root_id, root_id),
        )
        if (await cursor.fetchone())[0]:
            raise ModelChangeBusy("model cannot change while coordinated work is active")
        await conn.execute("SELECT set_config('tantra.model_patch_root', %s, true)", (root_id,))
        return root_id

    async def _publish_header_change(self, conn: Any, root_id: str, sid: str) -> None:
        await conn.execute(
            self._sql(
                "WITH change AS ("
                " INSERT INTO {schema}.coordinator_changes (root_id, kind, actor_id)"
                " SELECT %s, 'header', %s WHERE EXISTS ("
                " SELECT 1 FROM {schema}.coordinator_roots WHERE root_id = %s"
                ") RETURNING id"
                ") SELECT pg_notify(%s, json_build_object("
                " 'id', id, 'root_id', %s::text, 'kind', 'header', 'actor_id', %s::text, 'seq', NULL"
                ")::text) FROM change"
            ),
            (root_id, sid, root_id, f"tantra_{abs(self._key):x}"[:63], root_id, sid),
        )


def _advisory_key(schema: str) -> int:
    return int.from_bytes(hashlib.blake2b(schema.encode(), digest_size=8).digest(), "big", signed=True)


def _json(model: Any) -> Any:
    return Jsonb(model.model_dump(mode="json"))


def _event_json(stamped: Stamped) -> Any:
    payload = base64.b64encode(stamped.model_dump_json().encode()).decode()
    return Jsonb({EVENT_ENVELOPE: {"version": EVENT_VERSION, "payload": payload}})


def _literal(vector: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


def _hydrate(row: tuple[Any, ...]) -> SessionHeader:
    header = SessionHeader.model_validate(row[0])
    header.last_seq = row[1]
    return header


def _parse(sid: str, raw: Any) -> Stamped:
    try:
        envelope = raw.get(EVENT_ENVELOPE) if isinstance(raw, dict) else None
        if envelope is not None:
            if not isinstance(envelope, dict) or envelope.get("version") != EVENT_VERSION:
                raise ValueError("unsupported event envelope")
            payload = envelope.get("payload")
            if not isinstance(payload, str):
                raise ValueError("invalid event envelope")
            return Stamped.model_validate_json(base64.b64decode(payload, validate=True))
        return Stamped.model_validate(raw)
    except (ValidationError, ValueError) as exc:
        raise CorruptLog(f"{sid}: unreadable event log row") from exc


def _parse_row(mid: str, raw: Any) -> MemoryRecord:
    try:
        return MemoryRecord.model_validate(raw)
    except ValidationError as exc:
        raise CorruptLog(f"{mid}: unreadable memory row") from exc
