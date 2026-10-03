from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import Agent, CoordinatorUnavailable, RemoteExecutionError, Runtime
from tantra.coordinator import RootObservation, WriterToken
from tantra.providers.fake import FakeProvider
from tantra.stores.memory import MemoryStore


class Bot(Agent):
    pass


class ObservationCoordinator:
    catch_up_interval = 0.01

    def __init__(self) -> None:
        self.latest: dict[str, RootObservation] = {}
        self.queues: dict[str, set[asyncio.Queue[RootObservation]]] = {}
        self.users: dict[str, int] = {}
        self.started = False
        self.closed = False

    async def start(self, handler: Any = None) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    def observation(self, root_id: str) -> RootObservation | None:
        return self.latest.get(root_id)

    async def observe(self, root_id: str) -> AsyncIterator[RootObservation]:
        queue: asyncio.Queue[RootObservation] = asyncio.Queue()
        self.queues.setdefault(root_id, set()).add(queue)
        self.users[root_id] = self.users.get(root_id, 0) + 1
        if root_id in self.latest:
            queue.put_nowait(self.latest[root_id])
        try:
            while not self.closed:
                yield await queue.get()
        finally:
            queues = self.queues[root_id]
            queues.remove(queue)
            if not queues:
                self.queues.pop(root_id)
            users = self.users[root_id] - 1
            if users:
                self.users[root_id] = users
            else:
                self.users.pop(root_id)

    async def emit(self, observation: RootObservation) -> None:
        self.latest[observation.root_id] = observation
        for queue in self.queues.get(observation.root_id, ()):
            await queue.put(observation)


def snapshot(
    root_id: str,
    sample: int,
    *,
    seq: int = 0,
    active: bool = False,
    error: CoordinatorUnavailable | None = None,
    writer_connection: str | None = None,
    owner_instance: str | None = None,
    owner_generation: int = 0,
    owner_valid: bool = False,
    recovery: dict[str, Any] | None = None,
) -> RootObservation:
    return RootObservation(
        root_id=root_id,
        sample=sample,
        change_id=seq,
        writer_connection=writer_connection,
        owner_instance=owner_instance,
        owner_generation=owner_generation,
        owner_valid=owner_valid,
        recovery=recovery or {},
        actors={root_id: (seq, active)},
        expires_at=datetime.now(UTC),
        error=error,
    )


async def wait_until(predicate: Any) -> None:
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0)


async def test_root_observer_is_shared_until_connection_stream_and_wait_release() -> None:
    root_id = uuid4().hex
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    runtime._retain_connection_interest(root_id)
    runtime._retain_actor_interest(runtime._stream_interests, root_id, root_id)
    runtime._retain_actor_interest(runtime._wait_interests, root_id, root_id)
    await wait_until(lambda: coordinator.users.get(root_id) == 1)
    watcher = runtime._watchers[root_id]

    await runtime._release_connection_interest(root_id)
    await runtime._release_actor_interest(runtime._stream_interests, root_id, root_id)
    assert runtime._watchers[root_id] is watcher
    assert coordinator.users[root_id] == 1

    await runtime._release_actor_interest(runtime._wait_interests, root_id, root_id)
    await wait_until(lambda: root_id not in coordinator.users)
    assert root_id not in runtime._watchers
    await runtime.aclose()


async def test_observation_samples_wake_waiters_without_waking_idle_streams() -> None:
    root_id = uuid4().hex
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    runtime._retain_actor_interest(runtime._stream_interests, root_id, root_id)
    runtime._retain_actor_interest(runtime._wait_interests, root_id, root_id)
    stream_signal = runtime._signal(root_id)
    wait_signal = runtime._wait_signal(root_id)

    await wait_until(lambda: coordinator.users.get(root_id) == 1)
    await coordinator.emit(snapshot(root_id, 1, seq=4))
    await wait_until(
        lambda: (
            runtime._observations.get(root_id) is not None
            and runtime._observations[root_id].sample == 1
            and runtime._observations[root_id].actors[root_id][0] == 4
        )
    )
    stream_generation = stream_signal.generation
    wait_generation = wait_signal.generation

    await coordinator.emit(snapshot(root_id, 2, seq=4))
    await wait_until(
        lambda: runtime._observations.get(root_id) is not None and runtime._observations[root_id].sample == 2
    )
    assert stream_signal.generation == stream_generation
    assert wait_signal.generation == wait_generation + 1

    await coordinator.emit(snapshot(root_id, 2, seq=5))
    await wait_until(lambda: runtime._observations[root_id].actors[root_id][0] == 5)
    assert stream_signal.generation == stream_generation + 1
    assert wait_signal.generation == wait_generation + 2

    await runtime._release_actor_interest(runtime._stream_interests, root_id, root_id)
    await runtime._release_actor_interest(runtime._wait_interests, root_id, root_id)
    await runtime.aclose()


async def test_result_wait_counts_samples_without_repeated_result_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    root_id = uuid4().hex
    command_id = uuid4()
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    calls = 0

    async def no_result(agent_id: str, requested: UUID) -> None:
        nonlocal calls
        assert agent_id == root_id
        assert requested == command_id
        calls += 1
        return None

    monkeypatch.setattr(runtime, "_result", no_result)
    waiter = asyncio.create_task(runtime._wait_result(root_id, command_id))
    await wait_until(lambda: coordinator.users.get(root_id) == 1 and calls == 1)
    await coordinator.emit(snapshot(root_id, 0))
    await wait_until(lambda: calls == 2)
    assert not waiter.done()
    await coordinator.emit(snapshot(root_id, 1))
    await wait_until(lambda: runtime._observations[root_id].sample == 1)
    assert not waiter.done()
    await coordinator.emit(snapshot(root_id, 2))
    with pytest.raises(RemoteExecutionError, match="no active owner execution"):
        await asyncio.wait_for(waiter, 1)
    assert calls == 2
    assert root_id not in runtime._wait_interests
    await runtime.aclose()


async def test_result_wait_ignores_cached_inactive_sample_from_before_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_id = uuid4().hex
    coordinator = ObservationCoordinator()
    coordinator.latest[root_id] = snapshot(root_id, 4)
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()

    async def no_result(agent_id: str, command_id: UUID) -> None:
        return None

    monkeypatch.setattr(runtime, "_result", no_result)
    waiter = asyncio.create_task(runtime._wait_result(root_id, uuid4()))
    await wait_until(lambda: coordinator.users.get(root_id) == 1)
    await asyncio.sleep(0)
    assert not waiter.done()
    await coordinator.emit(snapshot(root_id, 5))
    await wait_until(lambda: runtime._observations[root_id].sample == 5)
    assert not waiter.done()
    await coordinator.emit(snapshot(root_id, 6))
    with pytest.raises(RemoteExecutionError, match="no active owner execution"):
        await asyncio.wait_for(waiter, 1)
    await runtime.aclose()


async def test_result_wait_bounds_recovered_but_inactive_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_id = uuid4().hex
    owner_id = str(uuid4())
    recovery = {"generation": 3, "phase": "complete"}
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()

    async def no_result(agent_id: str, command_id: UUID) -> None:
        return None

    monkeypatch.setattr(runtime, "_result", no_result)
    waiter = asyncio.create_task(runtime._wait_result(root_id, uuid4()))
    await wait_until(lambda: coordinator.users.get(root_id) == 1)
    await coordinator.emit(
        snapshot(
            root_id,
            1,
            owner_instance=owner_id,
            owner_generation=3,
            owner_valid=True,
            recovery=recovery,
        )
    )
    await wait_until(
        lambda: runtime._observations.get(root_id) is not None and runtime._observations[root_id].sample == 1
    )
    await asyncio.sleep(0)
    await coordinator.emit(
        snapshot(
            root_id,
            2,
            owner_instance=owner_id,
            owner_generation=3,
            owner_valid=True,
            recovery=recovery,
        )
    )
    with pytest.raises(RemoteExecutionError, match="no active owner execution"):
        await asyncio.wait_for(waiter, 1)
    await runtime.aclose()


async def test_active_hint_resets_inactive_sample_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    root_id = uuid4().hex
    owner_id = str(uuid4())
    recovery = {"generation": 3, "phase": "complete"}
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()

    async def no_result(agent_id: str, command_id: UUID) -> None:
        return None

    monkeypatch.setattr(runtime, "_result", no_result)
    waiter = asyncio.create_task(runtime._wait_result(root_id, uuid4()))
    await wait_until(lambda: coordinator.users.get(root_id) == 1)
    await coordinator.emit(snapshot(root_id, 1))
    await wait_until(
        lambda: runtime._observations.get(root_id) is not None and runtime._observations[root_id].sample == 1
    )
    await asyncio.sleep(0)
    await coordinator.emit(
        snapshot(
            root_id,
            1,
            active=True,
            owner_instance=owner_id,
            owner_generation=3,
            owner_valid=True,
            recovery=recovery,
        )
    )
    await wait_until(lambda: runtime._observations[root_id].actors[root_id][1])
    await asyncio.sleep(0)
    await coordinator.emit(snapshot(root_id, 2))
    await wait_until(
        lambda: runtime._observations.get(root_id) is not None and runtime._observations[root_id].sample == 2
    )
    await asyncio.sleep(0)
    assert not waiter.done()
    await coordinator.emit(snapshot(root_id, 3))
    with pytest.raises(RemoteExecutionError, match="no active owner execution"):
        await asyncio.wait_for(waiter, 1)
    await runtime.aclose()


async def test_raised_observation_outage_propagates_to_result_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    root_id = uuid4().hex

    class FailingCoordinator(ObservationCoordinator):
        async def observe(self, observed_root: str) -> AsyncIterator[RootObservation]:
            self.users[observed_root] = self.users.get(observed_root, 0) + 1
            try:
                raise CoordinatorUnavailable("iterator failed")
                yield snapshot(observed_root, 0)
            finally:
                users = self.users[observed_root] - 1
                if users:
                    self.users[observed_root] = users
                else:
                    self.users.pop(observed_root)

    coordinator = FailingCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()

    async def no_result(agent_id: str, command_id: UUID) -> None:
        return None

    monkeypatch.setattr(runtime, "_result", no_result)
    with pytest.raises(CoordinatorUnavailable, match="iterator failed"):
        await asyncio.wait_for(runtime._wait_result(root_id, uuid4()), 1)
    assert root_id not in runtime._wait_interests
    await runtime.aclose()


async def test_observation_error_propagates_and_releases_result_interest(monkeypatch: pytest.MonkeyPatch) -> None:
    root_id = uuid4().hex
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()

    async def no_result(agent_id: str, command_id: UUID) -> None:
        return None

    monkeypatch.setattr(runtime, "_result", no_result)
    waiter = asyncio.create_task(runtime._wait_result(root_id, uuid4()))
    await wait_until(lambda: coordinator.users.get(root_id) == 1)
    await coordinator.emit(snapshot(root_id, 1, error=CoordinatorUnavailable("observation failed")))
    with pytest.raises(CoordinatorUnavailable, match="observation failed"):
        await asyncio.wait_for(waiter, 1)
    await wait_until(lambda: root_id not in coordinator.users)
    assert root_id not in runtime._watchers
    await runtime.aclose()


async def test_connection_exit_cleans_state_when_remote_release_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    root_id = uuid4().hex
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    connection = runtime.connect(UUID(hex=root_id), writable=True)
    connection._entered = True
    connection._watching = True
    connection._writer_token = WriterToken(root_id=root_id, connection_id=connection._connection_id)
    runtime._connections[connection._connection_id] = connection
    runtime._retain_connection_interest(root_id)

    class Iterator:
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    iterator = Iterator()
    connection._iterator = iterator

    async def failed_release(*args: Any) -> dict[str, Any]:
        raise CoordinatorUnavailable("release failed")

    monkeypatch.setattr(runtime, "_request_command", failed_release)
    with pytest.raises(CoordinatorUnavailable, match="release failed"):
        await connection.__aexit__()
    assert not connection._entered
    assert connection._iterator is None
    assert iterator.closed
    assert connection._connection_id not in runtime._connections
    assert root_id not in runtime._connection_interests
    await runtime.aclose()


async def test_writer_observation_respects_claim_commit_watermark_and_errors() -> None:
    root_id = uuid4().hex
    coordinator = ObservationCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    connection = runtime.connect(UUID(hex=root_id), writable=True)
    token = WriterToken(root_id=root_id, connection_id=connection._connection_id)
    connection._writer_token = token
    connection._writer_change_id = 5
    runtime._connections[connection._connection_id] = connection

    await runtime._apply_observation(root_id, snapshot(root_id, 1, seq=4))
    assert connection._writer_token == token
    await runtime._apply_observation(
        root_id,
        snapshot(root_id, 1, seq=6, error=CoordinatorUnavailable("catch-up failed")),
    )
    assert connection._writer_token == token
    await runtime._apply_observation(
        root_id,
        snapshot(root_id, 1, seq=6, writer_connection=str(token.connection_id)),
    )
    assert connection._writer_token == token
    assert connection._writer_change_id is None
    await runtime._apply_observation(root_id, snapshot(root_id, 1, seq=0, writer_connection=str(uuid4())))
    assert connection._writer_token is None
    assert connection._writer_change_id is None


async def test_maintenance_drains_full_batches_before_idle_sleep() -> None:
    class CleanupCoordinator:
        def __init__(self) -> None:
            self.calls: list[int] = []
            self.drained = asyncio.Event()

        async def start(self, handler: Any = None) -> None:
            return None

        async def cleanup(self, limit: int) -> int:
            self.calls.append(limit)
            if len(self.calls) == 3:
                self.drained.set()
                return 0
            return limit

        async def close(self) -> None:
            return None

    coordinator = CleanupCoordinator()
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    await asyncio.wait_for(coordinator.drained.wait(), 1)
    assert coordinator.calls == [100, 100, 100]
    await runtime.aclose()
