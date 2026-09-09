from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel

from tantra.events import (
    InputQueued,
    SessionEvent,
    SessionHeader,
    SessionStatus,
    Stamped,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    Usage,
)

if TYPE_CHECKING:
    from tantra.memory import MemoryRecord

_MISSING = object()

UNSET: Any = object()


@dataclass(frozen=True)
class EnqueueResult:
    seq: int
    duplicate: bool


@dataclass(frozen=True)
class JournalState:
    pending: list[InputQueued]
    incomplete: TurnStarted | None


def matches_metadata(metadata: dict[str, Any], wanted: dict[str, Any] | None) -> bool:
    if not wanted:
        return True
    return all(metadata.get(key, _MISSING) == value for key, value in wanted.items())


class Store(Protocol):
    """Append-only session event log plus a mutable session header."""

    async def setup(self) -> None:
        """Prepare the backend. Idempotent."""

    async def create(self, header: SessionHeader) -> None:
        """Register a new session. Raises `SessionExists` when the id is already taken."""

    async def header(self, sid: str) -> SessionHeader | None:
        """Return the session header, or None when the session is unknown."""

    async def put_header(self, h: SessionHeader) -> None:
        """Overwrite the session header. `last_seq` is store-owned and preserved."""

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
        """Apply one atomic edit to the header and return the result.

        Only the fields passed change, so a concurrent writer touching other fields is not lost.
        `metadata` merges shallowly — the given keys overwrite, the rest survive, nothing is
        deleted. `updated_at` is stamped on every patch; `last_seq` is store-owned and left alone.
        Raises `SessionNotFound` when the session is unknown.
        """

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int:
        """Append events and return the new last seq."""

    async def enqueue(self, sid: str, event: InputQueued) -> EnqueueResult: ...

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Stamped]: ...

    def read(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Stamped]:
        """Yield every stamped event with `seq > from_seq`, in seq order.

        Raises `CorruptLog` rather than skipping a stored event it cannot decode: a gap in the
        suffix would silently rewrite history.
        """

    async def list(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        parent_id: str | None = None,
        limit: int = 50,
        before: str | None = None,
    ) -> list[SessionHeader]:
        """Return headers newest first. `metadata` matches as a subset; `before` is a session id cursor."""

    async def memory_put(self, row: MemoryRecord) -> None:
        """Store a memory row, overwriting any row already held under its id."""

    async def memory_get(self, mid: str) -> MemoryRecord | None:
        """Return one memory row by id, deleted and superseded ones included, or None when unknown."""

    async def memory_all(
        self,
        *,
        metadata: dict[str, Any] | None = None,
        include_dead: bool = False,
    ) -> list[MemoryRecord]:
        """Return memory rows. `metadata` matches as a subset; a key the row lacks never matches.

        Deleted and superseded rows are left out unless `include_dead`. Keep the filter values
        scalar: `PostgresStore` matches with jsonb containment, which reads a list or dict value as
        a recursive subset rather than an equality test.
        """

    async def memory_search(self, vector: list[float], k: int) -> list[tuple[MemoryRecord, float]] | None:
        """Return up to `k` live rows nearest `vector` with their distances, or None without a vector path."""


def reduce_journal(items: Iterable[SessionEvent | Stamped]) -> JournalState:
    events = [item.event if isinstance(item, Stamped) else item for item in items]
    terminal = {
        event.turn_id
        for event in events
        if isinstance(event, TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted)
    }
    started = {event.turn_id for event in events if isinstance(event, TurnStarted)}
    pending = [
        event
        for event in events
        if isinstance(event, InputQueued) and event.command_id not in started and event.command_id not in terminal
    ]
    incomplete = next(
        (event for event in reversed(events) if isinstance(event, TurnStarted) and event.turn_id not in terminal),
        None,
    )
    return JournalState(pending=pending, incomplete=incomplete)


def apply_patch(header: SessionHeader, **fields: Any) -> SessionHeader:
    updated = header.model_copy(deep=True)
    for name, value in fields.items():
        if value is UNSET:
            continue
        patched = {**updated.metadata, **value} if name == "metadata" else value
        setattr(updated, name, patched.model_copy(deep=True) if isinstance(patched, BaseModel) else patched)
    updated.updated_at = datetime.now(UTC)
    return updated


def select_headers(
    headers: Iterable[SessionHeader],
    *,
    metadata: dict[str, Any] | None = None,
    parent_id: str | None = None,
    limit: int = 50,
    before: str | None = None,
) -> list[SessionHeader]:
    rows = sorted(headers, key=lambda h: (h.created_at, h.id), reverse=True)
    if before is not None:
        cursor = next((h for h in rows if h.id == before), None)
        if cursor is not None:
            rows = [h for h in rows if (h.created_at, h.id) < (cursor.created_at, cursor.id)]
    if parent_id is not None:
        rows = [h for h in rows if h.parent_id == parent_id]
    if metadata:
        rows = [h for h in rows if all(h.metadata.get(k) == v for k, v in metadata.items())]
    return rows[:limit]


def select_memories(
    rows: Iterable[MemoryRecord],
    *,
    metadata: dict[str, Any] | None = None,
    include_dead: bool = False,
) -> list[MemoryRecord]:
    live = list(rows) if include_dead else [r for r in rows if not r.deleted and r.superseded_by is None]
    return [row for row in live if matches_metadata(row.metadata, metadata)]
