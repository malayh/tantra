from __future__ import annotations

import asyncio
import sys
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel

from tantra.errors import SessionNotFound
from tantra.events import Stamped

ACTOR_EVENTS = 256
ACTOR_BYTES = 1024 * 1024
CACHE_ACTORS = 64
CACHE_BYTES = 16 * 1024 * 1024


def _size(value: Any) -> int:
    pending = [value]
    seen: set[int] = set()
    total = 256
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        total += sys.getsizeof(item)
        if isinstance(item, BaseModel):
            pending.extend((item.__dict__, item.__pydantic_extra__, item.__pydantic_fields_set__))
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list | tuple | set | frozenset):
            pending.extend(item)
    return total


@dataclass
class _Recent:
    items: list[tuple[Stamped, int]] = field(default_factory=list)
    size: int = 1024


@dataclass
class _Readers:
    users: int = 1
    high_water: int = 0
    deleted: bool = False
    flight: asyncio.Task[tuple[int, list[Stamped]]] | None = None


class CommittedPages:
    def __init__(self, read: Callable[..., Awaitable[list[Stamped]]]) -> None:
        self.read = read
        self.recent: OrderedDict[str, _Recent] = OrderedDict()
        self.readers: dict[str, _Readers] = {}
        self.size = 0
        self.closed = False

    def retain(self, sid: str) -> None:
        state = self.readers.get(sid)
        if state is None:
            self.readers[sid] = _Readers()
        else:
            state.users += 1

    def check(self, sid: str) -> None:
        state = self.readers.get(sid)
        if state is not None and state.deleted:
            raise SessionNotFound(sid)

    def advance(self, sid: str, seq: int) -> None:
        state = self.readers.get(sid)
        if state is not None:
            state.high_water = max(state.high_water, seq)

    def high_water(self, sid: str) -> int:
        state = self.readers.get(sid)
        return state.high_water if state is not None else 0

    def _drop(self, sid: str) -> None:
        entry = self.recent.pop(sid, None)
        if entry is not None:
            self.size -= entry.size

    def invalidate(self, sid: str) -> None:
        self._drop(sid)
        state = self.readers.get(sid)
        if state is not None:
            state.deleted = True

    async def release(self, sid: str) -> None:
        state = self.readers.get(sid)
        if state is None:
            return
        state.users -= 1
        if state.users:
            return
        self.readers.pop(sid)
        self._drop(sid)
        if state.flight is not None:
            state.flight.cancel()
            await asyncio.gather(state.flight, return_exceptions=True)

    def publish(self, sid: str, items: Sequence[Stamped]) -> None:
        state = self.readers.get(sid)
        if state is None or state.deleted or self.closed or not items:
            return
        self.advance(sid, items[-1].seq)
        entry = self.recent.pop(sid, _Recent())
        self.size -= entry.size if entry.items else 0
        for item in items:
            if entry.items and item.seq <= entry.items[-1][0].seq:
                continue
            if entry.items and item.seq != entry.items[-1][0].seq + 1:
                entry = _Recent()
            snapshot = item.model_copy(deep=True)
            size = _size(snapshot)
            if size + 1024 > ACTOR_BYTES:
                entry = _Recent()
                continue
            entry.items.append((snapshot, size))
            entry.size += size
            while len(entry.items) > ACTOR_EVENTS or entry.size > ACTOR_BYTES:
                entry.size -= entry.items.pop(0)[1]
        if entry.items:
            self.recent[sid] = entry
            self.size += entry.size
        while len(self.recent) > CACHE_ACTORS or self.size > CACHE_BYTES:
            self._drop(next(iter(self.recent)))

    def _cached(self, sid: str, after: int) -> list[Stamped] | None:
        entry = self.recent.get(sid)
        if entry is None or not entry.items or not entry.items[0][0].seq <= after + 1 <= entry.items[-1][0].seq:
            return None
        self.recent.move_to_end(sid)
        return [item.model_copy(deep=True) for item, _ in entry.items if item.seq > after]

    async def _fetch(self, sid: str, state: _Readers, after: int) -> tuple[int, list[Stamped]]:
        page = await self.read(sid, after=after, limit=ACTOR_EVENTS)
        if self.readers.get(sid) is state and not state.deleted and not self.closed:
            self.publish(sid, page)
        return after, page

    async def page(self, sid: str, *, after: int) -> list[Stamped]:
        self.check(sid)
        if self.closed:
            return []
        state = self.readers[sid]
        if state.high_water - after > ACTOR_EVENTS:
            page = await self.read(sid, after=after)
            self.check(sid)
            return page
        while not self.closed:
            cached = self._cached(sid, after)
            if cached is not None:
                return cached
            task = state.flight
            if task is None:
                task = asyncio.create_task(self._fetch(sid, state, after))
                state.flight = task

                def finished(done: asyncio.Task[Any], state: _Readers = state) -> None:
                    if state.flight is done:
                        state.flight = None
                    if not done.cancelled():
                        done.exception()

                task.add_done_callback(finished)
            try:
                begin, page = await asyncio.shield(task)
            except asyncio.CancelledError:
                if self.closed and not asyncio.current_task().cancelling():
                    return []
                raise
            self.check(sid)
            if begin == after or not page and begin <= after:
                return [item.model_copy(deep=True) for item in page]
            if page and page[0].seq <= after + 1 <= page[-1].seq:
                return [item.model_copy(deep=True) for item in page if item.seq > after]
        return []

    async def close(self) -> None:
        self.closed = True
        tasks = [state.flight for state in self.readers.values() if state.flight is not None]
        self.recent.clear()
        self.size = 0
        self.readers.clear()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
