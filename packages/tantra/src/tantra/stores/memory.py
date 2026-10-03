from __future__ import annotations

import threading
from collections.abc import AsyncIterator, Callable, Sequence
from typing import Any

from tantra.errors import InvalidCommandReuse, SessionBusy, SessionExists, SessionNotFound, TantraError
from tantra.events import InputQueued, SessionEvent, SessionHeader, SessionStatus, Stamped, Usage
from tantra.memory import MemoryRecord
from tantra.stores.base import (
    UNSET,
    EnqueueResult,
    apply_patch,
    reduce_header,
    reduce_journal,
    select_headers,
    select_memories,
)


class MemoryStore:
    def __init__(self) -> None:
        self._headers: dict[str, SessionHeader] = {}
        self._events: dict[str, list[Stamped]] = {}
        self._memories: dict[str, MemoryRecord] = {}
        self._deleted: set[str] = set()
        self._lock = threading.Lock()

    async def setup(self) -> None:
        return None

    async def create(self, header: SessionHeader) -> None:
        with self._lock:
            if header.id in self._headers or header.id in self._deleted:
                raise SessionExists(header.id)
            if header.root_id in self._deleted or header.parent_id in self._deleted:
                raise SessionNotFound(header.root_id or header.parent_id)
            stored = header.model_copy(deep=True)
            self._headers[stored.id] = stored
            self._events[stored.id] = []

    async def delete_tree(
        self,
        sid: str,
        *,
        allow_active: bool = False,
        before_delete: Callable[[list[str]], None] | None = None,
    ) -> list[str]:
        with self._lock:
            root = self._headers.get(sid)
            if root is None:
                return []
            if root.parent_id is not None or root.root_id not in (None, sid):
                raise TantraError(f"session {sid} is not a root session")
            ids = [sid]
            for current in ids:
                ids.extend(sorted(h.id for h in self._headers.values() if h.parent_id == current))
            if not allow_active:
                for actor_id in ids:
                    header = self._headers[actor_id]
                    journal = reduce_journal(self._events.get(actor_id, []))
                    if (
                        header.pending_ask is not None
                        or header.status in ("queued", "running", "awaiting_input")
                        or header.current_turn_id is not None
                        or journal.pending
                        or journal.incomplete is not None
                    ):
                        raise SessionBusy(sid)
            if before_delete is not None:
                before_delete(ids)
            for actor_id in ids:
                del self._headers[actor_id]
                self._events.pop(actor_id, None)
                self._deleted.add(actor_id)
            return ids

    async def header(self, sid: str) -> SessionHeader | None:
        with self._lock:
            header = self._headers.get(sid)
            return header.model_copy(deep=True) if header is not None else None

    async def is_deleted(self, sid: str) -> bool:
        with self._lock:
            return sid in self._deleted

    async def put_header(self, h: SessionHeader) -> None:
        with self._lock:
            current = self._headers.get(h.id)
            if current is None:
                raise SessionNotFound(h.id)
            stored = h.model_copy(deep=True)
            stored.last_seq = current.last_seq
            self._headers[h.id] = stored

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
        with self._lock:
            current = self._headers.get(sid)
            if current is None:
                raise SessionNotFound(sid)
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
            self._headers[sid] = stored
            return stored.model_copy(deep=True)

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int:
        with self._lock:
            header = self._headers.get(sid)
            if header is None:
                raise SessionNotFound(sid)
            log = self._events.setdefault(sid, [])
            seq = header.last_seq
            for event in events:
                seq += 1
                log.append(Stamped(seq=seq, event=event.model_copy(deep=True)))
            stored = reduce_header(header, events)
            stored.last_seq = seq
            self._headers[sid] = stored
            return seq

    async def enqueue(self, sid: str, event: InputQueued) -> EnqueueResult:
        with self._lock:
            header = self._headers.get(sid)
            if header is None:
                raise SessionNotFound(sid)
            log = self._events.setdefault(sid, [])
            for stamped in log:
                existing = stamped.event
                if not isinstance(existing, InputQueued) or existing.command_id != event.command_id:
                    continue
                if existing == event:
                    return EnqueueResult(seq=stamped.seq, duplicate=True)
                raise InvalidCommandReuse(event.command_id)
            seq = header.last_seq + 1
            log.append(Stamped(seq=seq, event=event.model_copy(deep=True)))
            stored = reduce_header(header, [event])
            stored.last_seq = seq
            self._headers[sid] = stored
            return EnqueueResult(seq=seq, duplicate=False)

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Stamped]:
        with self._lock:
            start = max(after, 0)
            page = self._events.get(sid, [])[start : start + max(limit, 0)]
            return [item.model_copy(deep=True) for item in page]

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
            return [h.model_copy(deep=True) for h in rows]

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
