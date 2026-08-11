from __future__ import annotations

import threading
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from tantra.errors import SeqConflict, SessionExists, SessionNotFound
from tantra.events import Lease, SessionEvent, SessionHeader, SessionStatus, Stamped, Usage
from tantra.memory import MemoryRecord
from tantra.stores.base import UNSET, apply_patch, select_headers, select_memories


class MemoryStore:
    def __init__(self) -> None:
        self._headers: dict[str, SessionHeader] = {}
        self._events: dict[str, list[Stamped]] = {}
        self._leases: dict[str, Lease] = {}
        self._memories: dict[str, MemoryRecord] = {}
        self._lock = threading.Lock()

    async def setup(self) -> None:
        return None

    async def create(self, header: SessionHeader) -> None:
        with self._lock:
            if header.id in self._headers:
                raise SessionExists(header.id)
            stored = header.model_copy(deep=True)
            stored.lease = None
            self._headers[stored.id] = stored
            self._events[stored.id] = []

    async def header(self, sid: str) -> SessionHeader | None:
        with self._lock:
            header = self._headers.get(sid)
            return self._with_lease(header) if header is not None else None

    async def put_header(self, h: SessionHeader) -> None:
        with self._lock:
            current = self._headers.get(h.id)
            if current is None:
                raise SessionNotFound(h.id)
            stored = h.model_copy(deep=True)
            stored.last_seq = current.last_seq
            stored.lease = None
            self._headers[h.id] = stored

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
        with self._lock:
            current = self._headers.get(sid)
            if current is None:
                raise SessionNotFound(sid)
            stored = apply_patch(
                current,
                title=title,
                status=status,
                pending_ask=pending_ask,
                usage=usage,
                metadata=metadata,
            )
            self._headers[sid] = stored
            return self._with_lease(stored)

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int | None) -> int:
        with self._lock:
            header = self._headers.get(sid)
            if header is None:
                raise SessionNotFound(sid)
            if expect_seq is not None and expect_seq != header.last_seq:
                raise SeqConflict(f"{sid}: expected seq {header.last_seq}, got {expect_seq}")
            log = self._events.setdefault(sid, [])
            seq = header.last_seq
            for event in events:
                seq += 1
                log.append(Stamped(seq=seq, event=event.model_copy(deep=True)))
            header.last_seq = seq
            header.updated_at = datetime.now(UTC)
            return seq

    async def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        with self._lock:
            log = list(self._events.get(sid, []))
        for stamped in log:
            if stamped.seq > from_seq:
                yield stamped.model_copy(deep=True)

    async def list(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        limit: int = 50,
        before: str | None = None,
    ) -> list[SessionHeader]:
        with self._lock:
            rows = select_headers(
                self._headers.values(),
                metadata=metadata,
                parent_id=parent_id,
                limit=limit,
                before=before,
            )
            return [self._with_lease(h) for h in rows]

    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool:
        with self._lock:
            if sid not in self._headers:
                raise SessionNotFound(sid)
            now = datetime.now(UTC)
            lease = self._leases.get(sid)
            if lease is not None and lease.holder != holder and lease.expires_at > now:
                return False
            self._leases[sid] = Lease(holder=holder, expires_at=now + timedelta(seconds=ttl))
            return True

    async def release_lease(self, sid: str, holder: str) -> None:
        with self._lock:
            lease = self._leases.get(sid)
            if lease is not None and lease.holder == holder:
                del self._leases[sid]

    async def memory_put(self, row: MemoryRecord) -> None:
        with self._lock:
            self._memories[row.id] = row.model_copy(deep=True)

    async def memory_get(self, mid: str) -> MemoryRecord | None:
        with self._lock:
            row = self._memories.get(mid)
            return row.model_copy(deep=True) if row is not None else None

    async def memory_all(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        include_dead: bool = False,
    ) -> list[MemoryRecord]:
        with self._lock:
            rows = select_memories(self._memories.values(), metadata=metadata, include_dead=include_dead)
            return [row.model_copy(deep=True) for row in rows]

    async def memory_search(self, vector: list[float], k: int) -> list[tuple[MemoryRecord, float]] | None:
        return None

    def _with_lease(self, header: SessionHeader) -> SessionHeader:
        snapshot = header.model_copy(deep=True)
        lease = self._leases.get(header.id)
        snapshot.lease = lease.model_copy() if lease is not None else None
        return snapshot
