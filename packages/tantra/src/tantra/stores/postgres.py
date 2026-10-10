from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from tantra.cleanup import CleanupCandidate, CleanupSelector, check_revision, cleanup_cursor
from tantra.errors import (
    CoordinatorUnavailable,
    CorruptLog,
    InvalidCommandReuse,
    LeaseLost,
    ModelChangeBusy,
    SessionBusy,
    SessionExists,
    SessionNotFound,
    TantraError,
)
from tantra.events import (
    AgentFinished,
    AskAnswered,
    AskRaised,
    CancellationRequested,
    CompactionApplied,
    InputQueued,
    ReasoningDelta,
    SessionEvent,
    SessionHeader,
    SessionStatus,
    Stamped,
    TextDelta,
    ToolCallDelta,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    Usage,
)
from tantra.memory import MemoryRecord
from tantra.stores.base import UNSET, EnqueueResult, HistorySnapshot, OperationalState, apply_patch, reduce_header

try:
    import psycopg
    import psycopg_pool
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
    (
        """
        CREATE TABLE {schema}.journal_index (
            actor_id text NOT NULL,
            seq bigint NOT NULL,
            event_type text NOT NULL,
            command_id bytea,
            turn_id bytea,
            PRIMARY KEY (actor_id, seq),
            FOREIGN KEY (actor_id, seq) REFERENCES {schema}.events (session_id, seq) ON DELETE CASCADE
        )
        """,
        "CREATE INDEX journal_command_idx ON {schema}.journal_index (actor_id, command_id, seq)"
        " WHERE command_id IS NOT NULL",
        "CREATE INDEX journal_turn_idx ON {schema}.journal_index (actor_id, turn_id) WHERE turn_id IS NOT NULL",
        "CREATE INDEX journal_finish_idx ON {schema}.journal_index (actor_id, seq DESC)"
        " WHERE event_type = 'agent_finished'",
    ),
    (
        "ALTER TABLE {schema}.sessions ADD COLUMN operational_version int NOT NULL DEFAULT 1",
        "ALTER TABLE {schema}.sessions ADD COLUMN operational_seq bigint NOT NULL DEFAULT 0",
        "ALTER TABLE {schema}.journal_index ADD COLUMN live boolean NOT NULL DEFAULT false",
        "CREATE INDEX journal_work_idx ON {schema}.journal_index (actor_id, event_type, seq) WHERE live",
        "CREATE INDEX journal_compaction_idx ON {schema}.journal_index (actor_id, seq DESC)"
        " WHERE event_type = 'compaction_applied'",
        """
        CREATE TABLE {schema}.cancellation_targets (
            source_actor_id text NOT NULL,
            source_seq bigint NOT NULL,
            actor_id text NOT NULL,
            turn_id bytea NOT NULL,
            PRIMARY KEY (source_actor_id, source_seq, actor_id, turn_id),
            FOREIGN KEY (source_actor_id, source_seq)
                REFERENCES {schema}.events (session_id, seq) ON DELETE CASCADE
        )
        """,
        "CREATE INDEX cancellation_turn_idx ON {schema}.cancellation_targets (actor_id, turn_id)",
    ),
    (
        "CREATE INDEX sessions_root_idx ON {schema}.sessions ((COALESCE(NULLIF(header->>'root_id', ''), id)))",
        'CREATE INDEX sessions_order_idx ON {schema}.sessions (created_at DESC, id COLLATE "C" DESC)',
        "DROP INDEX {schema}.sessions_parent_idx",
        'CREATE INDEX sessions_parent_order_idx ON {schema}.sessions (parent_id, created_at DESC, id COLLATE "C" DESC)',
        "CREATE INDEX coordinator_requests_root_order_idx"
        " ON {schema}.coordinator_requests (root_id, created_at, request_id) WHERE reply IS NULL",
        "DROP INDEX {schema}.coordinator_requests_cleanup_idx",
        "CREATE INDEX coordinator_requests_cleanup_idx"
        " ON {schema}.coordinator_requests ((COALESCE(completed_at, deadline)), request_id)",
        "CREATE INDEX coordinator_changes_cleanup_idx ON {schema}.coordinator_changes (created_at, id)",
    ),
    (
        "CREATE TABLE {schema}.deleted_sessions (actor_id text PRIMARY KEY, root_id text NOT NULL)",
        """
        CREATE FUNCTION {schema}.guard_deleted_session() RETURNS trigger AS $$
        DECLARE
            actor text;
            root text;
            parent text;
        BEGIN
            IF TG_TABLE_NAME = 'sessions' THEN
                actor := NEW.id;
                root := COALESCE(NULLIF(NEW.header->>'root_id', ''), actor);
                parent := NEW.parent_id;
                IF parent IS NOT NULL AND NULLIF(NEW.header->>'root_id', '') IS NULL THEN
                    WITH RECURSIVE ancestors AS (
                        SELECT id, parent_id, header FROM {schema}.sessions WHERE id = parent
                        UNION SELECT s.id, s.parent_id, s.header FROM {schema}.sessions s
                              JOIN ancestors a ON s.id = a.parent_id
                    ) SELECT COALESCE(NULLIF(header->>'root_id', ''), id) INTO root
                      FROM ancestors WHERE parent_id IS NULL LIMIT 1;
                    root := COALESCE(root, actor);
                END IF;
            ELSE
                actor := NEW.session_id;
                SELECT COALESCE(NULLIF(header->>'root_id', ''), id)
                    INTO root FROM {schema}.sessions WHERE id = actor;
                root := COALESCE(root, actor);
            END IF;
            PERFORM pg_advisory_xact_lock(hashtextextended(root, 0));
            IF EXISTS (SELECT 1 FROM {schema}.deleted_sessions
                       WHERE actor_id = actor OR actor_id = root OR actor_id = parent) THEN
                RAISE EXCEPTION 'session % was deleted', actor USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        "CREATE TRIGGER deleted_session_guard BEFORE INSERT OR UPDATE ON {schema}.sessions"
        " FOR EACH ROW EXECUTE FUNCTION {schema}.guard_deleted_session()",
        "CREATE TRIGGER deleted_event_guard BEFORE INSERT OR UPDATE ON {schema}.events"
        " FOR EACH ROW EXECUTE FUNCTION {schema}.guard_deleted_session()",
    ),
    (
        "ALTER TABLE {schema}.sessions ADD COLUMN updated_at timestamptz",
        "UPDATE {schema}.sessions SET updated_at = COALESCE((header->>'updated_at')::timestamptz, created_at)",
        "ALTER TABLE {schema}.sessions ALTER COLUMN updated_at SET NOT NULL",
        "ALTER TABLE {schema}.sessions ALTER COLUMN updated_at SET DEFAULT clock_timestamp()",
        """
        CREATE FUNCTION {schema}.sync_session_updated_at() RETURNS trigger AS $$
        BEGIN
            NEW.updated_at := COALESCE((NEW.header->>'updated_at')::timestamptz, NEW.updated_at, clock_timestamp());
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """,
        """
        CREATE TRIGGER sessions_updated_at_sync
        BEFORE INSERT OR UPDATE OF header ON {schema}.sessions
        FOR EACH ROW EXECUTE FUNCTION {schema}.sync_session_updated_at()
        """,
    ),
    (
        "CREATE INDEX journal_terminal_idx ON {schema}.journal_index (actor_id, seq DESC)"
        " WHERE event_type IN ('turn_completed', 'turn_failed', 'turn_cancelled', 'turn_interrupted')",
    ),
    (
        "ALTER TABLE {schema}.sessions ADD COLUMN root_key text"
        " GENERATED ALWAYS AS (COALESCE(NULLIF(header->>'root_id', ''), id)) STORED",
        "DROP INDEX {schema}.sessions_root_idx",
        "CREATE INDEX sessions_root_idx ON {schema}.sessions (root_key)",
    ),
    (
        "ALTER TABLE {schema}.journal_index ADD COLUMN ask_id bytea",
        "CREATE INDEX journal_ask_idx ON {schema}.journal_index (actor_id, ask_id, seq) WHERE ask_id IS NOT NULL",
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
        self._pool = self._new_pool()
        self._pool_open = False
        self._setup_lock = asyncio.Lock()
        self._vector: bool | None = None

    async def setup(self) -> None:
        async with self._setup_lock:
            await self._open_pool()
            async with self._pool.connection() as conn:
                async with conn.transaction():
                    await conn.execute("SELECT pg_advisory_xact_lock(%s)", (self._key,))
                    await conn.execute(self._sql("CREATE SCHEMA IF NOT EXISTS {schema}"))
                    await conn.execute(
                        self._sql("CREATE TABLE IF NOT EXISTS {schema}.schema_version (version int NOT NULL)")
                    )
                    cursor = await conn.execute(
                        self._sql("SELECT coalesce(max(version), 0) FROM {schema}.schema_version")
                    )
                    current = (await cursor.fetchone())[0]
                    for version, statements in enumerate(MIGRATIONS, start=1):
                        if version <= current:
                            continue
                        for statement in statements:
                            await conn.execute(self._sql(statement))
                        if version == 5:
                            await self._backfill_journal_index(conn)
                        elif version == 6:
                            await self._backfill_operational(conn)
                        elif version == 12:
                            await self._backfill_asks(conn)
                        await conn.execute(
                            self._sql("INSERT INTO {schema}.schema_version (version) VALUES (%s)"), (version,)
                        )
                self._vector = await self._install_vector(conn)

    async def create(self, header: SessionHeader) -> None:
        stored = header.model_copy(deep=True)
        async with self._connection() as conn:
            async with conn.transaction():
                await self._check_create(conn, stored)
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

    async def _check_create(self, conn: Any, header: SessionHeader) -> None:
        root_id = header.root_id or header.id
        if header.root_id is None and header.parent_id is not None:
            cursor = await conn.execute(
                self._sql(
                    "WITH RECURSIVE ancestors AS (SELECT id, parent_id, root_key FROM {schema}.sessions WHERE id = %s"
                    " UNION SELECT s.id, s.parent_id, s.root_key FROM {schema}.sessions s"
                    " JOIN ancestors a ON s.id = a.parent_id)"
                    " SELECT root_key"
                    " FROM ancestors WHERE parent_id IS NULL LIMIT 1"
                ),
                (header.parent_id,),
            )
            ancestor = await cursor.fetchone()
            if ancestor is not None:
                root_id = ancestor[0]
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (root_id,))
        cursor = await conn.execute(
            self._sql("SELECT actor_id FROM {schema}.deleted_sessions WHERE actor_id = ANY(%s::text[])"),
            ([header.id, root_id, header.parent_id],),
        )
        deleted = {row[0] for row in await cursor.fetchall()}
        if header.id in deleted:
            raise SessionExists(header.id)
        if deleted:
            raise SessionNotFound(root_id)

    async def is_deleted(self, sid: str) -> bool:
        async with self._connection() as conn:
            cursor = await conn.execute(
                self._sql("SELECT EXISTS (SELECT 1 FROM {schema}.deleted_sessions WHERE actor_id = %s)"), (sid,)
            )
            return bool((await cursor.fetchone())[0])

    async def select_cleanup(
        self, selector: CleanupSelector, *, limit: int, after: str | None = None
    ) -> list[CleanupCandidate]:
        root_ids = None if selector.root_ids is None else [root.hex for root in selector.root_ids]
        cursor = cleanup_cursor(after)
        try:
            async with self._connection() as conn:
                async with conn.transaction():
                    await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    if root_ids:
                        found = await conn.execute(
                            self._sql(
                                "SELECT id FROM {schema}.sessions WHERE id = ANY(%s::text[])"
                                " AND (parent_id IS NOT NULL"
                                " OR root_key <> id) LIMIT 1"
                            ),
                            (root_ids,),
                        )
                        row = await found.fetchone()
                        if row is not None:
                            raise TantraError("cleanup root_ids contains a live child")
                    return await self._cleanup_rows(
                        conn,
                        root_ids=root_ids,
                        metadata=dict(selector.metadata or {}),
                        inactive_before=selector.inactive_before,
                        after=cursor,
                        limit=limit,
                    )
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            raise CoordinatorUnavailable("cannot select cleanup candidates") from exc

    async def _cleanup_rows(
        self,
        conn: Any,
        *,
        root_ids: list[str] | None,
        limit: int,
        metadata: dict[str, Any] | None = None,
        inactive_before: datetime | None = None,
        after: tuple[datetime, str] | None = None,
        require_root: bool = True,
    ) -> list[CleanupCandidate]:
        cursor = await conn.execute(
            self._sql(
                "WITH RECURSIVE roots AS ("
                " SELECT s.id, s.created_at FROM {schema}.sessions s"
                " WHERE (%s::boolean OR (s.parent_id IS NULL"
                " AND s.root_key = s.id))"
                " AND (%s::boolean OR s.id = ANY(%s::text[]))"
                " AND (%s::boolean OR s.metadata @> %s::jsonb)"
                ' AND (%s::boolean OR (s.created_at, s.id COLLATE "C")'
                ' > (%s::timestamptz, %s::text COLLATE "C"))'
                "), tree(root_id, id) AS ("
                " SELECT id, id FROM roots UNION"
                " SELECT t.root_id, s.id FROM {schema}.sessions s JOIN tree t ON s.parent_id = t.id"
                "), session_state AS ("
                " SELECT t.root_id, max(s.updated_at) AS updated_at,"
                " bool_or(s.operational_version <> 1 OR s.operational_seq <> s.last_seq"
                " OR s.header->>'status' IN ('queued', 'running', 'awaiting_input')"
                " OR s.header->>'current_turn_id' IS NOT NULL"
                " OR s.header->>'pending_ask' IS NOT NULL) AS active,"
                " jsonb_agg(jsonb_build_array(s.id, s.parent_id, s.created_at, s.updated_at,"
                " s.header, s.last_seq, s.operational_version, s.operational_seq)"
                ' ORDER BY s.id COLLATE "C") AS payload'
                " FROM tree t JOIN {schema}.sessions s ON s.id = t.id GROUP BY t.root_id"
                "), live_index AS ("
                " SELECT t.root_id, jsonb_agg(jsonb_build_array(i.actor_id, i.seq, i.event_type,"
                " encode(i.command_id, 'hex'), encode(i.turn_id, 'hex'))"
                ' ORDER BY i.actor_id COLLATE "C", i.seq) AS payload'
                " FROM tree t JOIN {schema}.journal_index i ON i.actor_id = t.id"
                " WHERE i.live GROUP BY t.root_id"
                "), activity AS ("
                " SELECT a.root_id, jsonb_agg(jsonb_build_array(a.actor_id, a.active)"
                ' ORDER BY a.actor_id COLLATE "C") AS payload'
                " FROM {schema}.coordinator_activity a JOIN roots r ON r.id = a.root_id"
                " WHERE a.active GROUP BY a.root_id"
                "), requests AS ("
                " SELECT q.root_id, jsonb_agg(jsonb_build_array(q.request_id::text, q.deadline,"
                " q.envelope->>'operation')"
                " ORDER BY q.request_id) AS payload"
                " FROM {schema}.coordinator_requests q JOIN roots r ON r.id = q.root_id"
                " WHERE q.reply IS NULL AND q.deadline > clock_timestamp()"
                " AND q.envelope->>'operation' IN ('send', 'answer', 'cancel') GROUP BY q.root_id"
                ") SELECT r.id, r.created_at, s.updated_at,"
                " encode(sha256(convert_to(jsonb_build_array(s.payload,"
                " COALESCE(i.payload, '[]'::jsonb), COALESCE(a.payload, '[]'::jsonb),"
                " COALESCE(q.payload, '[]'::jsonb))::text, 'UTF8')), 'hex'),"
                " COALESCE(s.active, false) OR i.payload IS NOT NULL"
                " OR a.payload IS NOT NULL OR q.payload IS NOT NULL"
                " FROM roots r JOIN session_state s ON s.root_id = r.id"
                " LEFT JOIN live_index i ON i.root_id = r.id"
                " LEFT JOIN activity a ON a.root_id = r.id"
                " LEFT JOIN requests q ON q.root_id = r.id"
                " WHERE (%s::boolean OR s.updated_at < %s::timestamptz)"
                ' ORDER BY r.created_at, r.id COLLATE "C" LIMIT %s'
            ),
            (
                not require_root,
                root_ids is None,
                root_ids or [],
                not metadata,
                Jsonb(metadata or {}),
                after is None,
                None if after is None else after[0],
                None if after is None else after[1],
                inactive_before is None,
                inactive_before,
                max(limit, 0),
            ),
        )
        return [
            CleanupCandidate(
                root_id=root_id,
                created_at=created_at,
                updated_at=updated_at,
                revision=revision,
                active=active,
            )
            for root_id, created_at, updated_at, revision, active in await cursor.fetchall()
        ]

    async def delete_tree(
        self,
        sid: str,
        *,
        allow_active: bool = False,
        before_delete: Callable[[list[str]], None] | None = None,
        expected_revision: str | None = None,
    ) -> list[str]:
        async with self._connection() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (sid,))
            cursor = await conn.execute(self._sql("SELECT 1 FROM {schema}.sessions WHERE id = %s"), (sid,))
            if await cursor.fetchone() is None:
                return []
            await self._guard_session(conn, sid)
            return await self._delete_tree(
                conn,
                sid,
                allow_active=allow_active,
                before_delete=before_delete,
                expected_revision=expected_revision,
            )

    async def _deletion_headers(self, conn: Any, sid: str) -> list[SessionHeader]:
        cursor = await conn.execute(
            self._sql(
                "WITH RECURSIVE tree(id) AS (SELECT id FROM {schema}.sessions WHERE id = %s"
                " UNION SELECT s.id FROM {schema}.sessions s JOIN tree t ON s.parent_id = t.id)"
                " SELECT s.header, s.last_seq FROM {schema}.sessions s JOIN tree t ON s.id = t.id"
                ' ORDER BY s.id COLLATE "C" FOR UPDATE OF s'
            ),
            (sid,),
        )
        headers = [_hydrate(row) for row in await cursor.fetchall()]
        return headers

    async def _delete_tree(
        self,
        conn: Any,
        sid: str,
        *,
        allow_active: bool,
        before_delete: Callable[[list[str]], None] | None = None,
        request_id: Any = None,
        expected_revision: str | None = None,
    ) -> list[str]:
        headers = await self._deletion_headers(conn, sid)
        if not headers:
            return []
        ids = [header.id for header in headers]
        state = (await self._cleanup_rows(conn, root_ids=[sid], limit=1, require_root=False))[0]
        check_revision(expected_revision, state.revision)
        root = next((header for header in headers if header.id == sid), None)
        if root is not None and (root.parent_id is not None or root.root_id not in (None, sid)):
            raise TantraError(f"session {sid} is not a root")
        if not allow_active and state.active:
            raise SessionBusy(sid)
        if before_delete is not None:
            before_delete(ids)
        await conn.execute(
            self._sql(
                "DELETE FROM {schema}.cancellation_targets"
                " WHERE source_actor_id = ANY(%s::text[]) OR actor_id = ANY(%s::text[])"
            ),
            (ids, ids),
        )
        await conn.execute(self._sql("DELETE FROM {schema}.events WHERE session_id = ANY(%s::text[])"), (ids,))
        await conn.execute(self._sql("DELETE FROM {schema}.sessions WHERE id = ANY(%s::text[])"), (ids,))
        await conn.execute(self._sql("INSERT INTO {schema}.deleted_sessions SELECT unnest(%s::text[]), %s"), (ids, sid))
        await conn.execute(self._sql("DELETE FROM {schema}.coordinator_activity WHERE root_id = %s"), (sid,))
        await conn.execute(
            self._sql(
                "DELETE FROM {schema}.coordinator_requests WHERE root_id = %s AND request_id IS DISTINCT FROM %s::uuid"
            ),
            (sid, request_id),
        )
        await conn.execute(self._sql("DELETE FROM {schema}.coordinator_changes WHERE root_id = %s"), (sid,))
        return ids

    async def header(self, sid: str) -> SessionHeader | None:
        async with self._connection() as conn:
            cursor = await conn.execute(
                self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s"), (sid,)
            )
            row = await cursor.fetchone()
        return _hydrate(row) if row is not None else None

    async def put_header(self, h: SessionHeader) -> None:
        stored = h.model_copy(deep=True)
        async with self._connection() as conn:
            async with conn.transaction():
                await self._guard_session(conn, h.id)
                cursor = await conn.execute(
                    self._sql("SELECT last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (h.id,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(h.id)
                stored.last_seq = row[0]
                stored.updated_at = datetime.now(UTC)
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
        async with self._connection() as conn:
            async with conn.transaction():
                root_id = None
                guarded = any(value is not UNSET for value in (status, pending_ask, usage, finished))
                if guarded:
                    await self._guard_session(conn, sid)
                model_root = await self._prepare_model_patch(conn, sid) if model is not UNSET else None
                if not guarded and model_root is None:
                    root_id = await self._prepare_header_patch(conn, sid)
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (sid,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                current = SessionHeader.model_validate(row[0])
                current.last_seq = row[1]
                current_root = current.root_id or current.id
                if root_id is not None and root_id != current_root:
                    raise CoordinatorUnavailable("session root changed while applying header patch")
                if model_root is not None and current_root != model_root:
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
                await self._publish_header_change(conn, current_root, sid)
                return stored

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int:
        async with self._connection() as conn:
            async with conn.transaction():
                await self._guard_session(conn, sid)
                seq = await self._append_deltas(conn, sid, events)
                if seq is not None:
                    return seq
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
                    await self._index_events(
                        conn, [(sid, last_seq + offset, event) for offset, event in enumerate(events, start=1)]
                    )
                header = reduce_header(header, events)
                header.last_seq = seq
                await conn.execute(
                    self._sql(
                        "UPDATE {schema}.sessions SET header = %s, last_seq = %s,"
                        " operational_seq = CASE WHEN operational_version = 1 AND operational_seq = last_seq"
                        " THEN %s ELSE operational_seq END WHERE id = %s"
                    ),
                    (_json(header), seq, seq, sid),
                )
                return seq

    async def _append_deltas(
        self,
        conn: Any,
        sid: str,
        events: Sequence[SessionEvent],
        *,
        root_id: str | None = None,
    ) -> int | None:
        if not events or not all(isinstance(event, TextDelta | ReasoningDelta | ToolCallDelta) for event in events):
            return None
        cursor = await conn.execute(
            self._sql("SELECT root_key, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (sid,)
        )
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(sid)
        if root_id is not None and root_id != row[0]:
            raise LeaseLost(row[0])
        seq = row[1] + len(events)
        await cursor.executemany(
            self._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"),
            [
                (sid, row[1] + offset, _event_json(Stamped(seq=row[1] + offset, event=event)))
                for offset, event in enumerate(events, start=1)
            ],
        )
        now = datetime.now(UTC)
        await conn.execute(
            self._sql(
                "UPDATE {schema}.sessions SET header = jsonb_set(jsonb_set(header, '{{last_seq}}',"
                " to_jsonb(%s::bigint)), '{{updated_at}}', to_jsonb(%s::text)), last_seq = %s,"
                " operational_seq = CASE WHEN operational_version = 1 AND operational_seq = last_seq"
                " THEN %s ELSE operational_seq END, updated_at = %s WHERE id = %s"
            ),
            (seq, now.isoformat().replace("+00:00", "Z"), seq, seq, now, sid),
        )
        return seq

    async def enqueue(self, sid: str, event: InputQueued) -> EnqueueResult:
        async with self._connection() as conn:
            async with conn.transaction():
                await self._guard_session(conn, sid)
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
                    (sid,),
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                existing = await self._lookup_input(conn, sid, event.command_id)
                if existing is not None:
                    if existing.event == event:
                        return EnqueueResult(seq=existing.seq, duplicate=True)
                    raise InvalidCommandReuse(event.command_id)
                header = SessionHeader.model_validate(row[0])
                seq = row[1] + 1
                stamped = Stamped(seq=seq, event=event)
                await conn.execute(
                    self._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"),
                    (sid, seq, _event_json(stamped)),
                )
                await self._index_events(conn, [(sid, seq, event)])
                header = reduce_header(header, [event])
                header.last_seq = seq
                await conn.execute(
                    self._sql(
                        "UPDATE {schema}.sessions SET header = %s, last_seq = %s,"
                        " operational_seq = CASE WHEN operational_version = 1 AND operational_seq = last_seq"
                        " THEN %s ELSE operational_seq END WHERE id = %s"
                    ),
                    (_json(header), seq, seq, sid),
                )
                return EnqueueResult(seq=seq, duplicate=False)

    async def lookup_command(self, root_id: str, command_id: str) -> tuple[str, Stamped] | None:
        async with self._connection() as conn:
            return await self._lookup_command(conn, root_id, command_id)

    async def lookup_ask(self, root_id: str, ask_id: str) -> tuple[str, Stamped] | None:
        async with self._connection() as conn:
            cursor = await conn.execute(
                self._sql(
                    "WITH RECURSIVE actors AS ("
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
            )
            rows = await cursor.fetchall()
        if len(rows) > 1:
            raise ValueError(f"ambiguous ask_id: {ask_id}")
        return (rows[0][0], _parse(rows[0][0], rows[0][1])) if rows else None

    async def lookup_finished(self, actor_id: str) -> Stamped | None:
        async with self._connection() as conn:
            return await self._lookup_finished(conn, actor_id)

    async def _lookup_command(self, conn: Any, root_id: str, command_id: str) -> tuple[str, Stamped] | None:
        cursor = await conn.execute(
            self._sql(
                "WITH RECURSIVE actors AS ("
                " SELECT id, created_at, 0 AS depth FROM {schema}.sessions WHERE id = %s"
                " UNION ALL SELECT s.id, s.created_at, a.depth + 1"
                " FROM {schema}.sessions s JOIN actors a ON s.parent_id = a.id"
                ") SELECT c.actor_id, e.stamped FROM (SELECT i.actor_id, i.seq FROM actors a"
                " JOIN LATERAL ("
                " SELECT actor_id, seq FROM {schema}.journal_index"
                " WHERE actor_id = a.id AND command_id = %s ORDER BY seq LIMIT 1"
                ') i ON true ORDER BY a.depth, a.created_at, a.id COLLATE "C" LIMIT 1) c'
                " JOIN LATERAL (SELECT stamped FROM {schema}.events"
                " WHERE session_id = c.actor_id AND seq = c.seq LIMIT 1) e ON true"
            ),
            (root_id, command_id.encode()),
        )
        row = await cursor.fetchone()
        return (row[0], _parse(row[0], row[1])) if row is not None else None

    async def _lookup_input(self, conn: Any, sid: str, command_id: str) -> Stamped | None:
        cursor = await conn.execute(
            self._sql(
                "SELECT e.stamped FROM (SELECT actor_id, seq FROM {schema}.journal_index"
                " WHERE actor_id = %s AND command_id = %s AND event_type = 'input_queued'"
                " ORDER BY seq LIMIT 1) i JOIN LATERAL (SELECT stamped FROM {schema}.events"
                " WHERE session_id = i.actor_id AND seq = i.seq LIMIT 1) e ON true"
            ),
            (sid, command_id.encode()),
        )
        row = await cursor.fetchone()
        return _parse(sid, row[0]) if row is not None else None

    async def _input_pending(self, conn: Any, sid: str, command_id: str) -> bool:
        cursor = await conn.execute(
            self._sql("SELECT NOT EXISTS (SELECT 1 FROM {schema}.journal_index WHERE actor_id = %s AND turn_id = %s)"),
            (sid, command_id.encode()),
        )
        return (await cursor.fetchone())[0]

    async def _lookup_finished(self, conn: Any, actor_id: str) -> Stamped | None:
        cursor = await conn.execute(
            self._sql(
                "SELECT e.stamped FROM (SELECT actor_id, seq FROM {schema}.journal_index"
                " WHERE actor_id = %s AND event_type = 'agent_finished' ORDER BY seq DESC LIMIT 1) i"
                " JOIN LATERAL (SELECT stamped FROM {schema}.events"
                " WHERE session_id = i.actor_id AND seq = i.seq LIMIT 1) e ON true"
            ),
            (actor_id,),
        )
        row = await cursor.fetchone()
        return _parse(actor_id, row[0]) if row is not None else None

    async def _index_events(
        self,
        conn: Any,
        items: Sequence[tuple[str, int, SessionEvent]],
        *,
        operational: bool = True,
        backfill: bool = False,
        include_asks: bool = True,
    ) -> None:
        rows = []
        for actor_id, seq, event in items:
            command_id = None
            turn_id = None
            ask_id = None
            if isinstance(event, InputQueued | AskAnswered | CancellationRequested):
                command_id = event.command_id
                if command_id is None:
                    continue
            elif isinstance(event, TurnStarted | TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted):
                turn_id = event.turn_id
            elif isinstance(event, AskRaised):
                if not include_asks:
                    continue
                ask_id = event.ask_id
            elif not isinstance(event, AgentFinished) and not (operational and isinstance(event, CompactionApplied)):
                continue
            rows.append(
                (
                    actor_id,
                    seq,
                    event.type,
                    command_id.encode() if command_id is not None else None,
                    turn_id.encode() if turn_id is not None else None,
                    ask_id.encode() if ask_id is not None else None,
                )
            )
        if rows:
            async with conn.cursor() as cursor:
                columns = "actor_id, seq, event_type, command_id, turn_id"
                values = "%s, %s, %s, %s, %s"
                if include_asks:
                    columns += ", ask_id"
                    values += ", %s"
                await cursor.executemany(
                    self._sql(f"INSERT INTO {{schema}}.journal_index ({columns}) VALUES ({values})")
                    + (sql.SQL(" ON CONFLICT (actor_id, seq) DO NOTHING") if backfill else sql.SQL("")),
                    rows if include_asks else [row[:-1] for row in rows],
                )
        if operational:
            await self._project_work(conn, items)

    async def _backfill_journal_index(self, conn: Any) -> None:
        after: tuple[str, int] | None = None
        while True:
            query = "SELECT session_id, seq, stamped FROM {schema}.events"
            if after is not None:
                query += " WHERE (session_id, seq) > (%s, %s)"
            query += " ORDER BY session_id, seq LIMIT 1000"
            cursor = await conn.execute(self._sql(query), after or ())
            rows = await cursor.fetchall()
            if not rows:
                return
            items = [(sid, seq, _parse(sid, raw).event) for sid, seq, raw in rows]
            await self._index_events(conn, items, operational=False, include_asks=False)
            after = rows[-1][:2]

    async def _backfill_asks(self, conn: Any) -> None:
        after: tuple[str, int] | None = None
        while True:
            query = "SELECT session_id, seq, stamped FROM {schema}.events"
            if after is not None:
                query += " WHERE (session_id, seq) > (%s, %s)"
            cursor = await conn.execute(self._sql(query + " ORDER BY session_id, seq LIMIT 1000"), after or ())
            rows = await cursor.fetchall()
            if not rows:
                return
            items = [
                (sid, seq, event) for sid, seq, raw in rows if isinstance(event := _parse(sid, raw).event, AskRaised)
            ]
            await self._index_events(
                conn,
                items,
                operational=False,
                backfill=True,
            )
            after = rows[-1][:2]

    async def _project_work(self, conn: Any, items: Sequence[tuple[str, int, SessionEvent]]) -> None:
        keys = {
            (sid, (event.command_id if isinstance(event, InputQueued) else event.turn_id).encode())
            for sid, _, event in items
            if isinstance(
                event, InputQueued | TurnStarted | TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted
            )
        }
        if keys:
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    self._sql(
                        "UPDATE {schema}.journal_index i SET live = NOT EXISTS ("
                        " SELECT 1 FROM {schema}.journal_index t WHERE t.actor_id = i.actor_id"
                        " AND t.turn_id = i.command_id)"
                        " WHERE i.actor_id = %s AND i.command_id = %s AND i.event_type = 'input_queued'"
                    ),
                    sorted(keys),
                )
                await cursor.executemany(
                    self._sql(
                        "UPDATE {schema}.journal_index i SET live = NOT EXISTS ("
                        " SELECT 1 FROM {schema}.journal_index t WHERE t.actor_id = i.actor_id"
                        " AND t.turn_id = i.turn_id AND t.event_type <> 'turn_started')"
                        " WHERE i.actor_id = %s AND i.turn_id = %s AND i.event_type = 'turn_started'"
                    ),
                    sorted(keys),
                )
        targets = [
            (sid, seq, actor_id, turn_id.encode())
            for sid, seq, event in items
            if isinstance(event, CancellationRequested)
            for actor_id, turn_ids in event.targets.items()
            for turn_id in turn_ids
        ]
        if targets:
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    self._sql(
                        "INSERT INTO {schema}.cancellation_targets (source_actor_id, source_seq, actor_id, turn_id)"
                        " SELECT %s, %s, %s, %s WHERE NOT EXISTS ("
                        " SELECT 1 FROM {schema}.journal_index WHERE actor_id = %s AND turn_id = %s"
                        " AND event_type <> 'turn_started') ON CONFLICT DO NOTHING"
                    ),
                    [(sid, seq, actor_id, key, actor_id, key) for sid, seq, actor_id, key in targets],
                )
        terminals = {
            (sid, event.turn_id.encode())
            for sid, _, event in items
            if isinstance(event, TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted)
        }
        if terminals:
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    self._sql("DELETE FROM {schema}.cancellation_targets WHERE actor_id = %s AND turn_id = %s"),
                    sorted(terminals),
                )

    async def _backfill_operational(self, conn: Any) -> None:
        await conn.execute(self._sql("UPDATE {schema}.journal_index SET live = false"))
        await conn.execute(self._sql("DELETE FROM {schema}.cancellation_targets"))
        after: tuple[str, int] | None = None
        while True:
            query = "SELECT session_id, seq, stamped FROM {schema}.events"
            if after is not None:
                query += " WHERE (session_id, seq) > (%s, %s)"
            cursor = await conn.execute(self._sql(query + " ORDER BY session_id, seq LIMIT 1000"), after or ())
            rows = await cursor.fetchall()
            if not rows:
                break
            await self._index_events(
                conn,
                [(sid, seq, _parse(sid, raw).event) for sid, seq, raw in rows],
                backfill=True,
                include_asks=False,
            )
            after = rows[-1][:2]
        await conn.execute(
            self._sql("UPDATE {schema}.sessions SET operational_version = 1, operational_seq = last_seq")
        )

    async def _repair_operational(self, conn: Any, sid: str, last_seq: int) -> None:
        await self._guard_session(conn, sid)
        await conn.execute(self._sql("DELETE FROM {schema}.journal_index WHERE actor_id = %s"), (sid,))
        await conn.execute(self._sql("DELETE FROM {schema}.cancellation_targets WHERE source_actor_id = %s"), (sid,))
        after = 0
        while after < last_seq:
            cursor = await conn.execute(
                self._sql(
                    "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq > %s"
                    " AND seq <= %s ORDER BY seq LIMIT 1000"
                ),
                (sid, after, last_seq),
            )
            page = [_parse(sid, row[0]) for row in await cursor.fetchall()]
            if not page or page[0].seq != after + 1:
                raise CorruptLog(f"missing journal evidence for {sid} after {after}")
            if any(item.seq != after + offset for offset, item in enumerate(page, start=1)):
                raise CorruptLog(f"non-contiguous journal for {sid}")
            await self._index_events(conn, [(sid, item.seq, item.event) for item in page])
            after = page[-1].seq
        await conn.execute(
            self._sql("UPDATE {schema}.sessions SET operational_version = 1, operational_seq = %s WHERE id = %s"),
            (last_seq, sid),
        )

    async def read_operational(self, actor_id: str) -> OperationalState:
        async with self._connection() as conn:
            async with conn.transaction():
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                return await self._read_operational(conn, actor_id)

    async def _read_operational(self, conn: Any, sid: str, *, repaired: bool = False) -> OperationalState:
        cursor = await conn.execute(
            self._sql("SELECT last_seq, operational_version, operational_seq FROM {schema}.sessions WHERE id = %s"),
            (sid,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(sid)
        last_seq, version, covered = row
        if version != 1 or not 0 <= covered <= last_seq:
            await self._repair_operational(conn, sid, last_seq)
        elif covered < last_seq:
            await self._guard_session(conn, sid)
            cursor = await conn.execute(
                self._sql(
                    "SELECT i.seq, e.stamped FROM {schema}.journal_index i"
                    " JOIN LATERAL (SELECT stamped FROM {schema}.events"
                    " WHERE session_id = i.actor_id AND seq = i.seq LIMIT 1) e ON true"
                    " WHERE i.actor_id = %s AND i.seq > %s AND i.seq <= %s ORDER BY i.seq"
                ),
                (sid, covered, last_seq),
            )
            items = [(sid, seq, _parse(sid, raw).event) for seq, raw in await cursor.fetchall()]
            await self._index_events(conn, items, backfill=True)
            await conn.execute(
                self._sql("UPDATE {schema}.sessions SET operational_seq = %s WHERE id = %s"), (last_seq, sid)
            )
        cursor = await conn.execute(
            self._sql(
                "SELECT i.seq, i.event_type, i.command_id, i.turn_id, e.stamped, NOT EXISTS ("
                " SELECT 1 FROM {schema}.journal_index t WHERE t.actor_id = i.actor_id"
                " AND t.turn_id = coalesce(i.command_id, i.turn_id) AND t.seq <= %s"
                " AND (i.event_type = 'input_queued' OR t.event_type <> 'turn_started')) FROM ("
                " SELECT actor_id, seq, event_type, command_id, turn_id FROM {schema}.journal_index"
                " WHERE actor_id = %s AND live AND seq <= %s) i"
                " JOIN LATERAL (SELECT stamped FROM {schema}.events"
                " WHERE session_id = i.actor_id AND seq = i.seq LIMIT 1) e ON true ORDER BY i.seq"
            ),
            (last_seq, sid, last_seq),
        )
        rows = await cursor.fetchall()
        items = [_parse(sid, raw) for _, _, _, _, raw, _ in rows]
        finished = await self._lookup_finished(conn, sid)
        invalid = (
            any(
                seq != item.seq
                or kind != item.event.type
                or not isinstance(item.event, InputQueued | TurnStarted)
                or not consistent
                or command != (item.event.command_id.encode() if isinstance(item.event, InputQueued) else None)
                or turn != (item.event.turn_id.encode() if isinstance(item.event, TurnStarted) else None)
                for (seq, kind, command, turn, _, consistent), item in zip(rows, items, strict=True)
            )
            or finished is not None
            and not isinstance(finished.event, AgentFinished)
        )
        if invalid:
            if repaired:
                raise CorruptLog(f"invalid operational evidence for {sid}")
            await self._repair_operational(conn, sid, last_seq)
            return await self._read_operational(conn, sid, repaired=True)
        cursor = await conn.execute(
            self._sql(
                "SELECT c.actor_id, c.turn_id FROM {schema}.cancellation_targets c"
                " WHERE c.source_actor_id = %s AND c.source_seq <= %s AND NOT EXISTS ("
                " SELECT 1 FROM {schema}.journal_index t WHERE t.actor_id = c.actor_id"
                " AND t.turn_id = c.turn_id AND t.event_type <> 'turn_started')"
                " ORDER BY c.source_seq, c.actor_id, c.turn_id"
            ),
            (sid, last_seq),
        )
        cancellations: dict[str, list[str]] = {}
        for actor_id, key in await cursor.fetchall():
            turn_id = bytes(key).decode()
            if turn_id not in cancellations.setdefault(actor_id, []):
                cancellations[actor_id].append(turn_id)
        return OperationalState(
            pending=[item.event for item in items if isinstance(item.event, InputQueued)],
            incomplete=next((item.event for item in reversed(items) if isinstance(item.event, TurnStarted)), None),
            finished=finished.event if finished is not None else None,
            cancellations=cancellations,
            last_seq=last_seq,
        )

    async def _read_interval(self, conn: Any, sid: str, begin: int, end: int) -> list[Stamped]:
        items: list[Stamped] = []
        after = begin - 1
        while after < end:
            cursor = await conn.execute(
                self._sql(
                    "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq > %s"
                    " AND seq <= %s ORDER BY seq LIMIT 1000"
                ),
                (sid, after, end),
            )
            page = [_parse(sid, row[0]) for row in await cursor.fetchall()]
            if not page:
                break
            items.extend(page)
            after = page[-1].seq
        return items

    async def read_turn(self, actor_id: str, turn_id: str) -> list[Stamped] | None:
        async with self._connection() as conn:
            return await self._read_turn(conn, actor_id, turn_id)

    async def _read_turn(self, conn: Any, sid: str, turn_id: str) -> list[Stamped] | None:
        cursor = await conn.execute(
            self._sql(
                "SELECT min(seq) FILTER (WHERE event_type = 'turn_started'),"
                " min(seq) FILTER (WHERE event_type <> 'turn_started') FROM {schema}.journal_index"
                " WHERE actor_id = %s AND turn_id = %s"
            ),
            (sid, turn_id.encode()),
        )
        start, end = await cursor.fetchone()
        if end is None:
            return None
        return await self._read_interval(conn, sid, start if start is not None and start <= end else end, end)

    async def read_compacted(self, actor_id: str) -> HistorySnapshot:
        async with self._connection() as conn:
            async with conn.transaction():
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                return await self._read_compacted(conn, actor_id)

    async def _read_compacted(self, conn: Any, sid: str) -> HistorySnapshot:
        cursor = await conn.execute(self._sql("SELECT last_seq FROM {schema}.sessions WHERE id = %s"), (sid,))
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(sid)
        last_seq = row[0]
        cursor = await conn.execute(
            self._sql(
                "SELECT e.stamped FROM (SELECT actor_id, seq FROM {schema}.journal_index"
                " WHERE actor_id = %s AND event_type = 'compaction_applied' AND seq <= %s"
                " ORDER BY seq DESC LIMIT 1) i JOIN LATERAL (SELECT stamped FROM {schema}.events"
                " WHERE session_id = i.actor_id AND seq = i.seq LIMIT 1) e ON true"
            ),
            (sid, last_seq),
        )
        row = await cursor.fetchone()
        begin = 1
        if row is not None:
            marker = _parse(sid, row[0])
            if not isinstance(marker.event, CompactionApplied):
                raise CorruptLog(f"invalid compaction evidence for {sid}")
            begin = marker.seq
            if marker.event.floor_turn_id is not None:
                cursor = await conn.execute(
                    self._sql(
                        "SELECT seq FROM {schema}.journal_index WHERE actor_id = %s AND turn_id = %s"
                        " AND event_type = 'turn_started' AND seq <= %s ORDER BY seq LIMIT 1"
                    ),
                    (sid, marker.event.floor_turn_id.encode(), last_seq),
                )
                floor = await cursor.fetchone()
                if floor is not None:
                    begin = min(begin, floor[0])
        return HistorySnapshot(await self._read_interval(conn, sid, begin, last_seq), last_seq)

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Stamped]:
        async with self._connection() as conn:
            cursor = await conn.execute(
                self._sql(
                    "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq > %s ORDER BY seq LIMIT %s"
                ),
                (sid, after, max(limit, 0)),
            )
            rows = await cursor.fetchall()
        return [_parse(sid, row[0]) for row in rows]

    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        async with self._connection() as conn:
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
        async with self._connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
        return [_hydrate(row) for row in rows]

    async def memory_put(self, row: MemoryRecord) -> None:
        async with self._connection() as conn:
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
        async with self._connection() as conn:
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
        async with self._connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
        return [_parse_row(mid, raw) for mid, raw in rows]

    async def memory_search(self, vector: list[float], k: int) -> list[tuple[MemoryRecord, float]] | None:
        async with self._connection() as conn:
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
        async with self._setup_lock:
            if self._pool_open:
                await self._pool.close()
            self._pool = self._new_pool()
            self._pool_open = False

    def _sql(self, statement: str) -> Any:
        return sql.SQL(statement).format(schema=self._ident)

    def _new_pool(self) -> Any:
        return psycopg_pool.AsyncConnectionPool(
            self.dsn,
            open=False,
            min_size=1,
            max_size=4,
            kwargs={"autocommit": True},
            connection_class=psycopg.AsyncConnection,
        )

    async def _open_pool(self) -> None:
        if not self._pool_open:
            await self._pool.open()
            self._pool_open = True

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[Any]:
        if not self._pool_open:
            async with self._setup_lock:
                await self._open_pool()
        pool = self._pool
        async with pool.connection() as conn:
            yield conn

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
            self._sql("SELECT root_key FROM {schema}.sessions WHERE id = %s"),
            (sid,),
        )
        row = await cursor.fetchone()
        root_id = row[0] if row is not None else sid
        await conn.execute(self._sql("SELECT {schema}.assert_coordinated_fence(%s)"), (root_id,))

    async def _prepare_header_patch(self, conn: Any, sid: str) -> str:
        cursor = await conn.execute(
            self._sql("SELECT root_key FROM {schema}.sessions WHERE id = %s"),
            (sid,),
        )
        row = await cursor.fetchone()
        root_id = row[0] if row is not None else sid
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (root_id,))
        await conn.execute(
            self._sql("SELECT 1 FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"),
            (root_id,),
        )
        return root_id

    async def _prepare_model_patch(self, conn: Any, sid: str) -> str:
        cursor = await conn.execute(
            self._sql("SELECT root_key FROM {schema}.sessions WHERE id = %s"),
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
                " WHERE root_key = %s"
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
