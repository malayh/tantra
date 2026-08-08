from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from typing import Any, Protocol

from tantra.events import SessionEvent, SessionHeader, Stamped


class Store(Protocol):
    """Append-only session event log plus a mutable session header."""

    async def setup(self) -> None:
        """Prepare the backend. Idempotent."""

    async def create(self, header: SessionHeader) -> None:
        """Register a new session. Raises `SessionExists` when the id is already taken."""

    async def header(self, sid: str) -> SessionHeader | None:
        """Return the session header, or None when the session is unknown.

        `lease` is reported as stored, expired or not — compare `lease.expires_at` against now
        to spot a turn abandoned by a dead worker.
        """

    async def put_header(self, h: SessionHeader) -> None:
        """Overwrite the session header. `last_seq` and `lease` are store-owned and preserved."""

    async def append(self, sid: str, events: Sequence[SessionEvent], *, expect_seq: int) -> int:
        """Append events and return the new last seq.

        Optimistic concurrency: raises `SeqConflict` unless `expect_seq` equals the session's
        current last seq. The first event of a session gets seq 1.
        """

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

    async def acquire_lease(self, sid: str, holder: str, ttl: float) -> bool:
        """Take or refresh the single-writer lease for `ttl` seconds.

        Returns False when a live lease is held by someone else. An expired lease is acquirable by
        anyone and is never cleared on expiry — it stays readable on the header as evidence of who
        held the session last and when it lapsed.
        """

    async def release_lease(self, sid: str, holder: str) -> None:
        """Drop the lease when `holder` owns it, otherwise do nothing."""


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
