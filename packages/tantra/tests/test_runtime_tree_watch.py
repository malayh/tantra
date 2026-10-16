from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from acp_support import Scripted

from tantra import Agent, Runtime, SessionHeader, SessionNotFound, TantraError, TreeChange
from tantra.coordinator import ChangeNotice, RootObservation
from tantra.errors import CoordinatorUnavailable
from tantra.providers.base import ProviderEvent, SampleRequest, ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.runtime import TREE_INTERVAL
from tantra.stores.memory import MemoryStore


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    subagents = [Worker]


class Bot(Agent):
    pass


class Held(Scripted):
    def __init__(self, **scripts: list[Sample]) -> None:
        super().__init__(**scripts)
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if req.model == "worker":
            await self.release.wait()
        async for event in self.providers[req.model].stream(req):
            yield event


HUB = ContextVar("hub", default=False)


class JournalCountingStore(MemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.reads = {"hub": 0, "runtime": 0}

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Any]:
        self.reads["hub" if HUB.get() else "runtime"] += 1
        return await super().read_page(sid, after=after, limit=limit)

    def read(self, sid: str, *, from_seq: int = 0) -> Any:
        self.reads["hub" if HUB.get() else "runtime"] += 1
        return super().read(sid, from_seq=from_seq)


class Observed:
    catch_up_interval = 0.01

    def __init__(self) -> None:
        self.observations: asyncio.Queue[RootObservation] = asyncio.Queue()
        self.notices: list[ChangeNotice] = []
        self.interests: dict[str, Any] = {}
        self.pages = 0
        self.outages = 0
        self.actives = 0
        self.live = False

    async def start(self, handler: Any = None) -> None:
        return None

    async def close(self) -> None:
        return None

    def observation(self, root_id: str) -> RootObservation | None:
        return None

    async def observe(self, root_id: str) -> AsyncIterator[RootObservation]:
        while True:
            yield await self.observations.get()

    def _set_actor_interests(self, root_id: str, actors: Any) -> None:
        self.interests[root_id] = actors

    async def _changes(self, root_id: str, after: int) -> list[ChangeNotice]:
        self.pages += 1
        return [notice for notice in self.notices if notice.change_id > after][:1000]

    async def active(self, root_id: str, actor_id: str | None = None) -> bool:
        self.actives += 1
        if self.outages:
            self.outages -= 1
            raise CoordinatorUnavailable("coordinator down")
        return self.live

    async def emit(
        self,
        root_id: str,
        change_id: int,
        kind: str | None = None,
        *,
        owner_instance: str | None = None,
        owner_valid: bool = False,
    ) -> None:
        if kind is not None:
            self.notices.append(ChangeNotice(change_id=change_id, root_id=root_id, kind=kind))
        await self.observations.put(
            RootObservation(
                root_id=root_id,
                sample=0,
                change_id=change_id,
                writer_connection=None,
                owner_instance=owner_instance,
                owner_generation=0,
                owner_valid=owner_valid,
                recovery={},
                actors={},
                expires_at=datetime.now(UTC),
            )
        )


class Watched:
    catch_up_interval = 0.01

    def __init__(self) -> None:
        self.live = True
        self.notices: asyncio.Queue[ChangeNotice] = asyncio.Queue()

    async def start(self, handler: Any = None) -> None:
        return None

    async def close(self) -> None:
        return None

    async def active(self, root_id: str, actor_id: str | None = None) -> bool:
        return self.live

    async def watch(self, root_id: str, *, after: int = 0) -> AsyncIterator[ChangeNotice]:
        while True:
            yield await self.notices.get()


def spawn(input: str) -> Sample:
    return Sample(
        tool_calls=[ToolCall(id="s1", name="spawn", args=json.dumps({"agent_name": "worker", "input": input}))]
    )


def finish() -> Sample:
    return Sample(tool_calls=[ToolCall(id="f1", name="finish", args=json.dumps({"result": "ok"}))])


async def expected(runtime: Runtime, root_id: UUID) -> TreeChange:
    root = await runtime.store.header(root_id.hex)
    assert root is not None
    return TreeChange(title=root.title, model=root.model, actors=tuple(await runtime.tree_status(root_id)))


async def until(tree: AsyncIterator[TreeChange], seen: list[TreeChange], done: Callable[[TreeChange], bool]) -> None:
    async with asyncio.timeout(5):
        while not seen or not done(seen[-1]):
            seen.append(await anext(tree))


async def coordinated(coordinator: Any) -> tuple[Runtime, UUID]:
    store = MemoryStore()
    root_id = uuid4()
    await store.create(SessionHeader(id=root_id.hex, root_id=root_id.hex, agent="bot", model="m"))
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    return runtime, root_id


async def test_the_first_snapshot_equals_tree_status_and_changes_arrive_in_order() -> None:
    provider = Held(root=[spawn("dig"), Sample(text="spawned"), Sample(text="noted")], worker=[finish()])
    runtime = Runtime(provider, MemoryStore(), [Root])
    root_id = await runtime.create(Root)
    tree = runtime.watch_tree(root_id)
    seen = [await anext(tree)]
    assert seen[0] == await expected(runtime, root_id)
    async with runtime.connect(root_id, writable=True) as connection:
        await connection.send("go", command_id=uuid4())
        await until(tree, seen, lambda change: len(change.actors) == 2 and change.actors[1].state == "running")
        assert seen[-1].actors[1].active
        provider.release.set()

        async def quiet() -> None:
            while any(not task.done() for task in runtime.active.values()):
                await asyncio.sleep(0.01)

        await asyncio.wait_for(quiet(), 5)
        final = await expected(runtime, root_id)
        await until(tree, seen, lambda change: change == final)
    await tree.aclose()
    assert final.actors[1].state == "finished" and final.actors[0].state == "idle"
    assert not any(actor.active for actor in final.actors)
    assert all(left != right for left, right in zip(seen, seen[1:], strict=False))
    child = [change.actors[1].state for change in seen if len(change.actors) == 2]
    assert child.index("running") < child.index("finished")
    assert [len(change.actors) for change in seen] == sorted(len(change.actors) for change in seen)
    assert seen[0].actors[0].state == "idle" and not seen[0].actors[0].active
    await runtime.aclose()


async def test_patch_header_signals_title_and_model_and_a_slow_consumer_gets_the_latest() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    root_id = await runtime.create(Bot)
    tree = runtime.watch_tree(root_id)
    first = await anext(tree)
    patched = await runtime.patch_header(root_id, title="One")
    titled = await anext(tree)
    await runtime.patch_header(root_id, model="other")
    modelled = await anext(tree)
    for title in ("Two", "Three", "Four"):
        await runtime.patch_header(root_id, title=title)
    latest = await anext(tree)
    pending = asyncio.ensure_future(anext(tree))
    await asyncio.sleep(0.05)
    assert not pending.done()
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    await tree.aclose()
    assert (first.title, first.model) == (None, "m")
    assert patched.title == "One" and (titled.title, titled.model) == ("One", "m")
    assert (modelled.title, modelled.model) == ("One", "other")
    assert latest == await expected(runtime, root_id) and latest.title == "Four"
    await runtime.aclose()


async def test_two_consumers_share_one_read_per_change() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    root_id = await runtime.create(Bot)
    initial = await expected(runtime, root_id)
    reads = 0
    tree_headers = runtime._tree_headers

    async def counted(root: str) -> Any:
        nonlocal reads
        reads += 1
        return await tree_headers(root)

    runtime._tree_headers = counted
    first, second = runtime.watch_tree(root_id), runtime.watch_tree(root_id)
    assert await asyncio.gather(anext(first), anext(second)) == [initial, initial]
    assert reads == 1
    await runtime.patch_header(root_id, title="Shared")
    left, right = await asyncio.gather(anext(first), anext(second))
    assert left == right and left.title == "Shared"
    assert reads == 2
    await first.aclose()
    await second.aclose()
    assert runtime._trees == {}
    await runtime.aclose()


async def test_start_errors_deletion_and_close() -> None:
    runtime = Runtime(Scripted(root=[spawn("x"), Sample(text="a"), Sample(text="b")], worker=[]), MemoryStore(), [Root])
    root_id = await runtime.create(Root)
    with pytest.raises(SessionNotFound):
        await anext(runtime.watch_tree(uuid4()))
    with pytest.raises(TypeError):
        await anext(runtime.watch_tree(root_id.hex))
    root = await runtime.store.header(root_id.hex)
    assert root is not None
    child = SessionHeader(id=uuid4().hex, root_id=root.id, parent_id=root.id, agent="worker", depth=1, model="m")
    await runtime.store.create(child)
    with pytest.raises(TantraError, match="not a root"):
        await anext(runtime.watch_tree(UUID(hex=child.id)))
    deleted = runtime.watch_tree(root_id)
    await anext(deleted)
    waiting = asyncio.ensure_future(anext(deleted))
    await asyncio.sleep(0)
    assert await runtime.delete(root_id)
    with pytest.raises(SessionNotFound):
        await asyncio.wait_for(waiting, 2)
    other = await runtime.create(Root)
    closing = runtime.watch_tree(other)
    await anext(closing)
    waiting = asyncio.ensure_future(anext(closing))
    await asyncio.sleep(0)
    await runtime.aclose()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(waiting, 2)
    assert runtime._trees == {}


async def test_watching_turns_spawns_and_finishes_reads_no_journal() -> None:
    store = JournalCountingStore()
    provider = Held(root=[spawn("dig"), Sample(text="spawned"), Sample(text="noted")], worker=[finish()])
    runtime = Runtime(provider, store, [Root])
    snapshot = runtime._tree_snapshot

    async def marked(root: str, tree: Any) -> TreeChange:
        token = HUB.set(True)
        try:
            return await snapshot(root, tree)
        finally:
            HUB.reset(token)

    runtime._tree_snapshot = marked
    root_id = await runtime.create(Root)
    tree = runtime.watch_tree(root_id)
    seen = [await anext(tree)]
    await runtime.patch_header(root_id, title="No journal")
    await until(tree, seen, lambda change: change.title == "No journal")
    async with runtime.connect(root_id, writable=True) as connection:
        await connection.send("go", command_id=uuid4())
        await until(tree, seen, lambda change: len(change.actors) == 2 and change.actors[1].state == "running")
        provider.release.set()
        await until(
            tree,
            seen,
            lambda change: (
                change.actors[1].state == "finished"
                and change.actors[0].state == "idle"
                and change.actors[0].last_turn is not None
                and not any(actor.active for actor in change.actors)
            ),
        )
    await tree.aclose()
    assert store.reads["hub"] == 0 and store.reads["runtime"] > 0
    await runtime.aclose()


async def test_reads_are_spaced_and_never_lose_the_newest_change() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    root_id = await runtime.create(Bot)
    loop = asyncio.get_running_loop()
    reads: list[float] = []
    tree_headers = runtime._tree_headers
    entered, hold = asyncio.Event(), asyncio.Event()
    holding = False

    async def timed(root: str) -> Any:
        reads.append(loop.time())
        headers = await tree_headers(root)
        if holding:
            entered.set()
            await hold.wait()
        return headers

    runtime._tree_headers = timed
    started = loop.time()
    tree = runtime.watch_tree(root_id)
    await anext(tree)
    assert reads[0] - started < TREE_INTERVAL
    for title in ("a", "b"):
        await runtime.patch_header(root_id, title=title)
        assert (await anext(tree)).title == title
    await runtime.patch_header(root_id, title="c")
    pending = asyncio.ensure_future(anext(tree))
    await asyncio.sleep(0.05)
    await runtime.patch_header(root_id, title="d")
    assert (await pending).title == "d"
    holding = True
    await runtime.patch_header(root_id, title="e")
    pending = asyncio.ensure_future(anext(tree))
    await asyncio.wait_for(entered.wait(), 2)
    holding = False
    await runtime.patch_header(root_id, title="f")
    hold.set()
    assert (await pending).title == "e"
    assert (await asyncio.wait_for(anext(tree), 2)).title == "f"
    await tree.aclose()
    assert all(right - left >= TREE_INTERVAL - 0.01 for left, right in zip(reads, reads[1:], strict=False))
    assert len(reads) == 6
    await runtime.aclose()


async def test_a_consumer_cancelled_mid_read_releases_its_share_of_the_hub() -> None:
    coordinator = Observed()
    runtime, root_id = await coordinated(coordinator)
    root = root_id.hex
    tree_headers = runtime._tree_headers
    entered, hold = asyncio.Event(), asyncio.Event()

    async def held(root: str) -> Any:
        entered.set()
        await hold.wait()
        return await tree_headers(root)

    runtime._tree_headers = held
    first, second = runtime.watch_tree(root_id), runtime.watch_tree(root_id)
    reading = asyncio.ensure_future(anext(first))
    await asyncio.wait_for(entered.wait(), 2)
    waiting = asyncio.ensure_future(anext(second))
    await asyncio.sleep(0.01)
    assert runtime._trees[root].users == 2 and coordinator.interests[root] == set()
    reading.cancel()
    await asyncio.gather(reading, return_exceptions=True)
    assert runtime._trees[root].users == 1
    hold.set()
    assert await asyncio.wait_for(waiting, 2) == await expected(runtime, root_id)
    await second.aclose()
    assert runtime._trees == {} and root not in runtime._watchers
    assert coordinator.interests[root] is None
    await runtime.aclose()


async def test_coordinator_unavailable_is_retried_until_a_snapshot_is_read() -> None:
    coordinator = Observed()
    runtime, root_id = await coordinated(coordinator)
    coordinator.outages = 2
    tree = runtime.watch_tree(root_id)
    first = await asyncio.wait_for(anext(tree), 2)
    assert first == await expected(runtime, root_id)
    assert coordinator.actives == 4
    await tree.aclose()
    await runtime.aclose()


async def test_observations_page_changes_and_bump_only_for_tree_kinds() -> None:
    coordinator = Observed()
    runtime, root_id = await coordinated(coordinator)
    root = root_id.hex
    reads = 0
    tree_headers = runtime._tree_headers

    async def counted(root: str) -> Any:
        nonlocal reads
        reads += 1
        return await tree_headers(root)

    runtime._tree_headers = counted
    tree = runtime.watch_tree(root_id)
    await anext(tree)
    assert root in runtime._watchers and coordinator.interests[root] == set()
    pending = asyncio.ensure_future(anext(tree))

    async def settled(count: int) -> None:
        async with asyncio.timeout(2):
            while coordinator.observations.qsize() or reads != count:
                await asyncio.sleep(0.01)

    await coordinator.emit(root, 0)
    await settled(2)
    await coordinator.emit(root, 1, "journal")
    await coordinator.emit(root, 2, "writer")
    await asyncio.sleep(0.05)
    assert (reads, coordinator.pages) == (2, 2)
    await runtime.store.patch_header(root, title="Remote")
    await coordinator.emit(root, 3, "header")
    change = await asyncio.wait_for(pending, 2)
    assert change.title == "Remote" and (reads, coordinator.pages) == (3, 3)
    await coordinator.emit(root, 0)
    await asyncio.sleep(0.05)
    assert (reads, coordinator.pages) == (3, 3)
    pending = asyncio.ensure_future(anext(tree))
    await coordinator.emit(root, 4, "activity")
    await settled(4)
    assert not pending.done()
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    await tree.aclose()
    assert runtime._trees == {} and root not in runtime._watchers
    assert coordinator.interests[root] is None
    await runtime.aclose()


async def test_a_coordinator_without_a_change_log_bumps_on_every_advance() -> None:
    coordinator = Observed()
    coordinator._changes = None
    runtime, root_id = await coordinated(coordinator)
    tree = runtime.watch_tree(root_id)
    await anext(tree)
    await coordinator.emit(root_id.hex, 0)
    await runtime.store.patch_header(root_id.hex, title="Advanced")
    await coordinator.emit(root_id.hex, 1)
    assert (await asyncio.wait_for(anext(tree), 2)).title == "Advanced"
    await tree.aclose()
    await runtime.aclose()


async def test_patch_header_refuses_after_close() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    root_id = await runtime.create(Bot)
    await runtime.aclose()
    with pytest.raises(TantraError, match="runtime is closed"):
        await runtime.patch_header(root_id, title="late")
    header = await runtime.store.header(root_id.hex)
    assert header is not None and header.title is None


def counting(runtime: Runtime) -> list[int]:
    reads = [0]
    tree_headers = runtime._tree_headers

    async def counted(root: str) -> Any:
        reads[0] += 1
        return await tree_headers(root)

    runtime._tree_headers = counted
    return reads


async def test_a_lost_lease_without_a_change_turns_the_tree_inactive() -> None:
    coordinator = Observed()
    coordinator.live = True
    runtime, root_id = await coordinated(coordinator)
    root = root_id.hex
    reads = counting(runtime)
    tree = runtime.watch_tree(root_id)
    assert (await anext(tree)).actors[0].active
    pending = asyncio.ensure_future(anext(tree))
    await coordinator.emit(root, 1, owner_instance="owner", owner_valid=True)
    async with asyncio.timeout(2):
        while coordinator.observations.qsize() or reads[0] != 2:
            await asyncio.sleep(0.01)
    coordinator.live = False
    await coordinator.emit(root, 1, owner_instance="owner", owner_valid=False)
    change = await asyncio.wait_for(pending, 2)
    assert not any(actor.active for actor in change.actors)
    assert change == await expected(runtime, root_id)
    count, generation = reads[0], runtime._trees[root].generation
    pending = asyncio.ensure_future(anext(tree))
    await coordinator.emit(root, 1, owner_instance="owner", owner_valid=False)
    await asyncio.sleep(TREE_INTERVAL * 2)
    assert coordinator.observations.qsize() == 0 and reads[0] == count
    assert runtime._trees[root].generation == generation
    assert not pending.done()
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    await tree.aclose()
    await runtime.aclose()


async def test_the_fallback_watch_re_reads_on_timeout_only_while_an_actor_is_active() -> None:
    coordinator = Watched()
    runtime, root_id = await coordinated(coordinator)
    reads = counting(runtime)
    tree = runtime.watch_tree(root_id)
    assert (await anext(tree)).actors[0].active
    coordinator.live = False
    change = await asyncio.wait_for(anext(tree), 2)
    assert not any(actor.active for actor in change.actors)
    assert change == await expected(runtime, root_id)
    count, generation = reads[0], runtime._trees[root_id.hex].generation
    pending = asyncio.ensure_future(anext(tree))
    await asyncio.sleep(TREE_INTERVAL * 2)
    assert reads[0] == count
    assert runtime._trees[root_id.hex].generation == generation
    assert not pending.done()
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    await tree.aclose()
    await runtime.aclose()


async def test_no_snapshot_is_yielded_once_close_starts_mid_read() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    root_id = await runtime.create(Bot)
    tree_headers = runtime._tree_headers
    entered, hold = asyncio.Event(), asyncio.Event()

    async def held(root: str) -> Any:
        headers = await tree_headers(root)
        entered.set()
        await hold.wait()
        return headers

    runtime._tree_headers = held
    tree = runtime.watch_tree(root_id)
    pending = asyncio.ensure_future(anext(tree))
    await asyncio.wait_for(entered.wait(), 2)
    closing = asyncio.create_task(runtime.aclose())
    await asyncio.sleep(0.01)
    hold.set()
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending, 2)
    await asyncio.wait_for(closing, 2)
    assert runtime._trees == {}
