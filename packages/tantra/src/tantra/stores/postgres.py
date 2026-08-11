from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError

from tantra.errors import CorruptLog, SeqConflict, SessionExists, SessionNotFound, TantraError
from tantra.events import Lease, SessionEvent, SessionHeader, SessionStatus, Stamped, Usage
from tantra.memory import MemoryRecord
from tantra.stores.base import UNSET, apply_patch

try:
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb

    HAS_PSYCOPG = True
except ImportError:
    HAS_PSYCOPG = False

MISSING_PSYCOPG = "PostgresStore needs psycopg: install tantra-harness[postgres]"

MIGRATIONS: tuple[tuple[str, ...], ...] = (
    (
        """
        CREATE TABLE IF NOT EXISTS {schema}.sessions (
            id text PRIMARY KEY,
            header jsonb NOT NULL,
            metadata jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            parent_id text,
            created_at timestamptz NOT NULL,
            last_seq bigint NOT NULL DEFAULT 0,
            lease jsonb
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
        stored.lease = None
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(
                self._sql(
                    "INSERT INTO {schema}.sessions (id, header, metadata, parent_id, created_at, last_seq, lease)"
                    " VALUES (%s, %s, %s, %s, %s, %s, NULL) ON CONFLICT (id) DO NOTHING"
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
                self._sql("SELECT header, last_seq, lease FROM {schema}.sessions WHERE id = %s"), (sid,)
            )
            row = await cursor.fetchone()
        return _hydrate(row) if row is not None else None

    async def put_header(self, h: SessionHeader) -> None:
        stored = h.model_copy(deep=True)
        stored.lease = None
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
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
        status: SessionStatus = UNSET,
        pending_ask: str | None = UNSET,
        usage: Usage = UNSET,
        metadata: dict[str, Any] = UNSET,
    ) -> SessionHeader:
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq, lease FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (sid,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                current = SessionHeader.model_validate(row[0])
                current.last_seq = row[1]
                stored = apply_patch(
                    current,
                    title=title,
                    status=status,
                    pending_ask=pending_ask,
                    usage=usage,
                    metadata=metadata,
                )
                await conn.execute(
                    self._sql("UPDATE {schema}.sessions SET header = %s, metadata = %s WHERE id = %s"),
                    (_json(stored), Jsonb(stored.metadata), sid),
                )
                stored.lease = Lease.model_validate(row[2]) if row[2] is not None else None
                return stored

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        async with self._lock:
            conn = await self._connection()
            async with conn.transaction():
                cursor = await conn.execute(
                    self._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"), (sid,)
                )
                row = await cursor.fetchone()
                if row is None:
                    raise SessionNotFound(sid)
                last_seq = row[1]
                if expect_seq is not None and expect_seq != last_seq:
                    raise SeqConflict(f"{sid}: expected seq {last_seq}, got {expect_seq}")
                header = SessionHeader.model_validate(row[0])
                seq = last_seq
                rows = []
                for event in events:
                    seq += 1
                    rows.append((sid, seq, _json(Stamped(seq=seq, event=event))))
                if rows:
                    await cursor.executemany(
                        self._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"), rows
                    )
                header.last_seq = seq
                header.updated_at = datetime.now(UTC)
                await conn.execute(
                    self._sql("UPDATE {schema}.sessions SET header = %s, last_seq = %s WHERE id = %s"),
                    (_json(header), seq, sid),
                )
                return seq

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
            "SELECT header, last_seq, lease FROM {schema}.sessions WHERE {conditions}"
            ' ORDER BY created_at DESC, id COLLATE "C" DESC LIMIT %(limit)s'
        ).format(schema=self._ident, conditions=sql.SQL(" AND ").join(conditions))
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
        return [_hydrate(row) for row in rows]

    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool:
        now = datetime.now(UTC)
        lease = Lease(holder=holder, expires_at=now + timedelta(seconds=ttl))
        async with self._lock:
            conn = await self._connection()
            cursor = await conn.execute(self._sql("SELECT 1 FROM {schema}.sessions WHERE id = %s"), (sid,))
            if await cursor.fetchone() is None:
                raise SessionNotFound(sid)
            cursor = await conn.execute(
                self._sql(
                    "UPDATE {schema}.sessions SET lease = %(lease)s WHERE id = %(sid)s AND ("
                    " lease IS NULL OR lease->>'holder' = %(holder)s"
                    " OR (lease->>'expires_at')::timestamptz <= %(now)s) RETURNING id"
                ),
                {"lease": _json(lease), "sid": sid, "holder": holder, "now": now},
            )
            return await cursor.fetchone() is not None

    async def release_lease(self, sid: str, holder: str) -> None:
        async with self._lock:
            conn = await self._connection()
            await conn.execute(
                self._sql("UPDATE {schema}.sessions SET lease = NULL WHERE id = %s AND lease->>'holder' = %s"),
                (sid, holder),
            )

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


def _advisory_key(schema: str) -> int:
    return int.from_bytes(hashlib.blake2b(schema.encode(), digest_size=8).digest(), "big", signed=True)


def _json(model: Any) -> Any:
    return Jsonb(model.model_dump(mode="json"))


def _literal(vector: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(value)) for value in vector) + "]"


def _hydrate(row: tuple[Any, ...]) -> SessionHeader:
    header = SessionHeader.model_validate(row[0])
    header.last_seq = row[1]
    header.lease = Lease.model_validate(row[2]) if row[2] is not None else None
    return header


def _parse(sid: str, raw: Any) -> Stamped:
    try:
        return Stamped.model_validate(raw)
    except ValidationError as exc:
        raise CorruptLog(f"{sid}: unreadable event log row") from exc


def _parse_row(mid: str, raw: Any) -> MemoryRecord:
    try:
        return MemoryRecord.model_validate(raw)
    except ValidationError as exc:
        raise CorruptLog(f"{mid}: unreadable memory row") from exc
