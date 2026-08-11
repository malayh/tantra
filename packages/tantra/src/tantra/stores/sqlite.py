from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from tantra.errors import CorruptLog, SeqConflict, SessionExists, SessionNotFound
from tantra.events import Lease, SessionEvent, SessionHeader, Stamped
from tantra.memory import MemoryRecord
from tantra.stores.base import select_headers

BUSY_TIMEOUT_MS = 30000

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        header TEXT NOT NULL,
        created_at TEXT NOT NULL,
        parent_id TEXT,
        last_seq INTEGER NOT NULL DEFAULT 0,
        lease TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        session_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        stamped TEXT NOT NULL,
        PRIMARY KEY (session_id, seq)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memories (
        id TEXT PRIMARY KEY,
        row TEXT NOT NULL
    )
    """,
)


class SQLiteStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    async def setup(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            for statement in SCHEMA:
                conn.execute(statement)
            conn.commit()

    async def create(self, header: SessionHeader) -> None:
        stored = header.model_copy(deep=True)
        stored.lease = None
        with self._write() as conn:
            existing = conn.execute("SELECT 1 FROM sessions WHERE id = ?", (header.id,)).fetchone()
            if existing is not None:
                raise SessionExists(header.id)
            conn.execute(
                "INSERT INTO sessions (id, header, created_at, parent_id, last_seq, lease) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    stored.id,
                    stored.model_dump_json(),
                    stored.created_at.isoformat(),
                    stored.parent_id,
                    stored.last_seq,
                    None,
                ),
            )

    async def header(self, sid: str) -> SessionHeader | None:
        with self._connect() as conn:
            row = conn.execute("SELECT header, last_seq, lease FROM sessions WHERE id = ?", (sid,)).fetchone()
        return _hydrate(row) if row is not None else None

    async def put_header(self, h: SessionHeader) -> None:
        stored = h.model_copy(deep=True)
        stored.lease = None
        with self._write() as conn:
            row = conn.execute("SELECT last_seq FROM sessions WHERE id = ?", (h.id,)).fetchone()
            if row is None:
                raise SessionNotFound(h.id)
            stored.last_seq = row[0]
            conn.execute(
                "UPDATE sessions SET header = ?, created_at = ?, parent_id = ? WHERE id = ?",
                (stored.model_dump_json(), stored.created_at.isoformat(), stored.parent_id, stored.id),
            )

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        with self._write() as conn:
            row = conn.execute("SELECT header, last_seq FROM sessions WHERE id = ?", (sid,)).fetchone()
            if row is None:
                raise SessionNotFound(sid)
            last_seq = row[1]
            if expect_seq is not None and expect_seq != last_seq:
                raise SeqConflict(f"{sid}: expected seq {last_seq}, got {expect_seq}")
            header = SessionHeader.model_validate_json(row[0])
            seq = last_seq
            rows = []
            for event in events:
                seq += 1
                rows.append((sid, seq, Stamped(seq=seq, event=event).model_dump_json()))
            if rows:
                conn.executemany("INSERT INTO events (session_id, seq, stamped) VALUES (?, ?, ?)", rows)
            header.last_seq = seq
            header.updated_at = datetime.now(UTC)
            conn.execute(
                "UPDATE sessions SET header = ?, last_seq = ? WHERE id = ?",
                (header.model_dump_json(), seq, sid),
            )
            return seq

    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT stamped FROM events WHERE session_id = ? AND seq > ? ORDER BY seq",
                (sid, from_seq),
            ).fetchall()
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
        with self._connect() as conn:
            rows = conn.execute("SELECT header, last_seq, lease FROM sessions").fetchall()
        headers = [_hydrate(row) for row in rows]
        return select_headers(headers, metadata=metadata, parent_id=parent_id, limit=limit, before=before)

    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool:
        with self._write() as conn:
            row = conn.execute("SELECT lease FROM sessions WHERE id = ?", (sid,)).fetchone()
            if row is None:
                raise SessionNotFound(sid)
            now = datetime.now(UTC)
            lease = _lease(row[0])
            if lease is not None and lease.holder != holder and lease.expires_at > now:
                return False
            fresh = Lease(holder=holder, expires_at=now + timedelta(seconds=ttl))
            conn.execute("UPDATE sessions SET lease = ? WHERE id = ?", (fresh.model_dump_json(), sid))
            return True

    async def release_lease(self, sid: str, holder: str) -> None:
        with self._write() as conn:
            row = conn.execute("SELECT lease FROM sessions WHERE id = ?", (sid,)).fetchone()
            if row is None:
                return
            lease = _lease(row[0])
            if lease is not None and lease.holder == holder:
                conn.execute("UPDATE sessions SET lease = NULL WHERE id = ?", (sid,))

    async def memory_put(self, row: MemoryRecord) -> None:
        with self._write() as conn:
            conn.execute(
                "INSERT INTO memories (id, row) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET row = excluded.row",
                (row.id, row.model_dump_json()),
            )

    async def memory_get(self, mid: str) -> MemoryRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT row FROM memories WHERE id = ?", (mid,)).fetchone()
        return _parse_row(mid, row[0]) if row is not None else None

    async def memory_all(self) -> list[MemoryRecord]:
        with self._connect() as conn:
            rows = conn.execute("SELECT id, row FROM memories ORDER BY id").fetchall()
        return [_parse_row(mid, raw) for mid, raw in rows]

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=BUSY_TIMEOUT_MS / 1000)
        try:
            conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            conn.commit()


def _hydrate(row: tuple[Any, ...]) -> SessionHeader:
    header = SessionHeader.model_validate_json(row[0])
    header.last_seq = row[1]
    header.lease = _lease(row[2])
    return header


def _lease(raw: str | None) -> Lease | None:
    return Lease.model_validate_json(raw) if raw else None


def _parse(sid: str, raw: str) -> Stamped:
    try:
        return Stamped.model_validate_json(raw)
    except ValidationError as exc:
        raise CorruptLog(f"{sid}: unreadable event log row") from exc


def _parse_row(mid: str, raw: str) -> MemoryRecord:
    try:
        return MemoryRecord.model_validate_json(raw)
    except ValidationError as exc:
        raise CorruptLog(f"{mid}: unreadable memory row") from exc
