from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tantra._pages import ACTOR_BYTES, ACTOR_EVENTS, CACHE_ACTORS, CACHE_BYTES, CommittedPages, _size
from tantra.errors import SessionNotFound
from tantra.events import Stamped, TextDelta, ToolCallCompleted


def items(start: int, count: int, text: str = "x") -> list[Stamped]:
    return [Stamped(seq=seq, event=TextDelta(text=f"{text}{seq}")) for seq in range(start, start + count)]


class Reader:
    def __init__(self, events: list[Stamped]) -> None:
        self.events = events
        self.calls: list[tuple[str, int, int]] = []
        self.started = asyncio.Event()
        self.gate = asyncio.Event()
        self.gate.set()
        self.cancelled = asyncio.Event()

    async def read(self, sid: str, *, after: int, limit: int = 1000) -> list[Stamped]:
        self.calls.append((sid, after, limit))
        self.started.set()
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return [item.model_copy(deep=True) for item in self.events if item.seq > after][:limit]


async def test_concurrent_readers_share_a_page_and_own_nested_payloads() -> None:
    event = Stamped(seq=1, event=ToolCallCompleted(call_id="c", result={"rows": [{"value": 3}]}))
    reader = Reader([event])
    reader.gate.clear()
    cache = CommittedPages(reader.read)
    for _ in range(8):
        cache.retain("root")
    tasks = [asyncio.create_task(cache.page("root", after=0)) for _ in range(8)]
    await reader.started.wait()
    await asyncio.sleep(0)
    reader.gate.set()
    pages = await asyncio.gather(*tasks)
    assert reader.calls == [("root", 0, 256)]
    pages[0][0].event.result["rows"][0]["value"] = 99
    assert all(page[0].event.result == {"rows": [{"value": 3}]} for page in pages[1:])
    assert (await cache.page("root", after=0))[0].event.result == {"rows": [{"value": 3}]}
    assert event.event.result == {"rows": [{"value": 3}]}
    for _ in range(8):
        await cache.release("root")
    assert not cache.readers and not cache.recent and cache.size == 0


async def test_cancelling_one_waiter_does_not_cancel_the_shared_read() -> None:
    reader = Reader(items(1, 3))
    reader.gate.clear()
    cache = CommittedPages(reader.read)
    cache.retain("root")
    cache.retain("root")
    first = asyncio.create_task(cache.page("root", after=0))
    second = asyncio.create_task(cache.page("root", after=1))
    await reader.started.wait()
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    await cache.release("root")
    assert not reader.cancelled.is_set()
    reader.gate.set()
    assert [item.seq for item in await second] == [2, 3]
    assert len(reader.calls) == 1
    await cache.release("root")


async def test_final_departure_cancels_read_and_replacement_cannot_be_removed() -> None:
    reader = Reader(items(1, 3))
    reader.gate.clear()
    cache = CommittedPages(reader.read)
    cache.retain("root")
    pending = asyncio.create_task(cache.page("root", after=0))
    await reader.started.wait()
    release = asyncio.create_task(cache.release("root"))
    await asyncio.sleep(0)
    cache.retain("root")
    reader.gate.set()
    await release
    await asyncio.gather(pending, return_exceptions=True)
    assert not cache.recent and cache.readers["root"].users == 1
    assert [item.seq for item in await cache.page("root", after=0)] == [1, 2, 3]
    await cache.release("root")


async def test_deletion_during_read_never_publishes_or_delivers_its_page() -> None:
    reader = Reader(items(1, 3))
    reader.gate.clear()
    cache = CommittedPages(reader.read)
    cache.retain("root")
    task = asyncio.create_task(cache.page("root", after=0))
    await reader.started.wait()
    cache.invalidate("root")
    reader.gate.set()
    with pytest.raises(SessionNotFound):
        await task
    assert not cache.recent and cache.size == 0
    with pytest.raises(SessionNotFound):
        await cache.page("root", after=0)
    await cache.release("root")


async def test_shutdown_cleans_pending_reads_and_cached_state() -> None:
    reader = Reader(items(1, 3))
    reader.gate.clear()
    cache = CommittedPages(reader.read)
    cache.retain("root")
    pending = asyncio.create_task(cache.page("root", after=0))
    await reader.started.wait()
    await cache.close()
    assert await pending == []
    assert reader.cancelled.is_set()
    assert not cache.readers and not cache.recent and cache.size == 0


async def test_sql_failures_are_not_cached_or_retried() -> None:
    calls = 0

    async def failed(*args: Any, **kwargs: Any) -> list[Stamped]:
        nonlocal calls
        calls += 1
        raise RuntimeError("read failed")

    cache = CommittedPages(failed)
    cache.retain("root")
    with pytest.raises(RuntimeError, match="read failed"):
        await cache.page("root", after=0)
    assert calls == 1 and not cache.recent
    await cache.release("root")


async def test_gaps_old_cursors_and_oversized_events_use_sql_without_changing_pagination() -> None:
    reader = Reader(items(1, 4000))
    cache = CommittedPages(reader.read)
    cache.retain("root")
    cache.advance("root", 4000)
    page = await cache.page("root", after=0)
    assert len(page) == 1000 and reader.calls == [("root", 0, 1000)]
    assert not cache.recent
    cache.publish("root", items(3999, 2))
    assert [item.seq for item in await cache.page("root", after=3998)] == [3999, 4000]
    assert len(reader.calls) == 1
    assert [item.seq for item in await cache.page("root", after=3997)] == [3998, 3999, 4000]
    assert len(reader.calls) == 2
    huge = items(4001, 1, "😀" * ACTOR_BYTES)
    reader.events.extend(huge)
    cache.publish("root", huge)
    assert not cache.recent and cache.size == 0
    assert (await cache.page("root", after=4000)) == huge
    assert not cache.recent
    await cache.release("root")


async def test_count_byte_and_lru_bounds_account_for_owned_objects() -> None:
    cache = CommittedPages(Reader([]).read)
    cache.retain("small")
    cache.publish("small", items(1, 400))
    assert len(cache.recent["small"].items) == ACTOR_EVENTS
    assert cache.recent["small"].items[0][0].seq == 145
    assert cache.recent["small"].size >= sum(_size(item) for item, _ in cache.recent["small"].items)
    for index in range(70):
        sid = str(index)
        cache.retain(sid)
        cache.publish(sid, items(1, 4))
    assert len(cache.recent) == CACHE_ACTORS and "small" not in cache.recent and "0" not in cache.recent
    await cache.page("6", after=0)
    cache.retain("new")
    cache.publish("new", items(1, 4))
    assert "6" in cache.recent and "7" not in cache.recent
    for sid in list(cache.readers):
        cache.publish(sid, items(10, 16, "x" * 64_000))
        assert cache.size <= CACHE_BYTES
        assert len(cache.recent) <= CACHE_ACTORS
        assert all(entry.size <= ACTOR_BYTES and len(entry.items) <= ACTOR_EVENTS for entry in cache.recent.values())
    assert cache.size == sum(entry.size for entry in cache.recent.values())
    await cache.close()


async def test_stale_authoritative_page_does_not_replace_newer_committed_events() -> None:
    reader = Reader(items(1, 3))
    reader.gate.clear()
    cache = CommittedPages(reader.read)
    cache.retain("root")
    pending = asyncio.create_task(cache.page("root", after=0))
    await reader.started.wait()
    committed = items(4, 2)
    cache.publish("root", committed)
    committed[0].event.text = "mutated after commit"
    reader.gate.set()
    assert [item.seq for item in await pending] == [1, 2, 3]
    assert [item.event.text for item in await cache.page("root", after=3)] == ["x4", "x5"]
    await cache.release("root")
