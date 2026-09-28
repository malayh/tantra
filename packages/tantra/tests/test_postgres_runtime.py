from __future__ import annotations

import asyncio
import multiprocessing
import queue
import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import (
    Agent,
    CommandTimeout,
    CoordinatorUnavailable,
    FreeText,
    FreeTextResponse,
    PostgresCoordinator,
    PostgresStore,
    RemoteExecutionError,
    Runtime,
    Sample,
    SessionNotFound,
    WriterReplaced,
    tool,
)
from tantra.events import (
    AskRaised,
    CancellationRequested,
    InputQueued,
    TurnCancelled,
    TurnCompleted,
    TurnInterrupted,
    TurnStarted,
)
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS, FakeProvider
from tantra.tools import Context

psycopg = pytest.importorskip("psycopg")


class Bot(Agent):
    pass


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="ok")


class ProcessGateProvider:
    def __init__(self, started: Any, release: Any) -> None:
        self.started = started
        self.release = release

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        await asyncio.to_thread(self.release.wait)
        yield StreamEnd(text="late")


def _coordinator(store: PostgresStore, lease_ttl: float = 0.6) -> PostgresCoordinator:
    return PostgresCoordinator(store, lease_ttl=lease_ttl, request_timeout=2.0, catch_up_interval=0.02)


async def _owner_main(
    dsn: str,
    schema: str,
    root_id: UUID,
    command_id: UUID,
    started: Any,
    release: Any,
    inspect_writer: Any,
    stop: Any,
    reports: Any,
) -> None:
    store = PostgresStore(dsn, schema=schema)
    runtime = Runtime(
        ProcessGateProvider(started, release),
        store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(store),
    )
    await runtime.start()
    try:
        async with runtime.connect(root_id, writable=True) as connection:
            await connection.send("blocked", command_id=command_id)
            reports.put("accepted")
            await asyncio.to_thread(inspect_writer.wait)
            deadline = time.monotonic() + 2
            while connection._writer_token is not None and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            try:
                connection._check_iteration()
            except WriterReplaced:
                reports.put("replaced")
            else:
                reports.put("writer-still-valid")
            await asyncio.to_thread(stop.wait)
    finally:
        await runtime.aclose()
        await store.close()


def _owner_process(*args: Any) -> None:
    asyncio.run(_owner_main(*args))


async def _runtime(dsn: str, schema: str, *, lease_ttl: float = 0.6) -> tuple[PostgresStore, Runtime]:
    store = PostgresStore(dsn, schema=schema)
    runtime = Runtime(
        EchoProvider(),
        store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(store, lease_ttl),
    )
    await runtime.start()
    return store, runtime


async def test_coordinated_runtime_requires_start_and_identical_store(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    other = PostgresStore(postgres_dsn, schema=pg_schema)
    runtime = Runtime(EchoProvider(), store, [Bot], default_model="m", coordinator=_coordinator(store))
    mismatched = Runtime(EchoProvider(), store, [Bot], default_model="m", coordinator=_coordinator(other))
    try:
        with pytest.raises(CoordinatorUnavailable, match="not started"):
            await runtime.status(uuid4())
        with pytest.raises(TypeError, match="same PostgresStore"):
            await mismatched.start()
        await runtime.start()
        await runtime.start()
        with pytest.raises(SessionNotFound):
            await runtime.status(uuid4())
    finally:
        await runtime.aclose()
        await store.close()
        await other.close()


async def test_create_is_fenced_atomic_and_readonly_does_not_activate(postgres_dsn: str, pg_schema: str) -> None:
    store, runtime = await _runtime(postgres_dsn, pg_schema)
    bare = PostgresStore(postgres_dsn, schema=pg_schema)
    root_id = await runtime.create(Bot)
    try:
        assert await runtime.coordinator.locate(root_id.hex) is None
        async with runtime.connect(root_id):
            assert await runtime.coordinator.locate(root_id.hex) is None
        with pytest.raises(psycopg.Error):
            await bare.append(root_id.hex, [InputQueued(command_id=uuid4().hex, input="bare")])
    finally:
        await runtime.aclose()
        await store.close()
        await bare.close()


async def test_two_process_writer_replacement_status_cancel_and_observation(postgres_dsn: str, pg_schema: str) -> None:
    creator_store, creator = await _runtime(postgres_dsn, pg_schema)
    root_id = await creator.create(Bot)
    await creator.aclose()
    await creator_store.close()

    ctx = multiprocessing.get_context("spawn")
    started = ctx.Event()
    release = ctx.Event()
    inspect_writer = ctx.Event()
    stop = ctx.Event()
    reports = ctx.Queue()
    command_id = uuid4()
    process = ctx.Process(
        target=_owner_process,
        args=(
            postgres_dsn,
            pg_schema,
            root_id,
            command_id,
            started,
            release,
            inspect_writer,
            stop,
            reports,
        ),
    )
    process.start()
    second_store: PostgresStore | None = None
    second: Runtime | None = None
    try:
        assert await asyncio.to_thread(reports.get, True, 5) == "accepted"
        assert await asyncio.to_thread(started.wait, 5)
        second_store, second = await _runtime(postgres_dsn, pg_schema)
        async with second.connect(root_id, writable=True) as connection:
            assert (await second.status(root_id)).active
            inspect_writer.set()
            assert await asyncio.to_thread(reports.get, True, 5) == "replaced"
            await connection.cancel(command_id=uuid4())
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                events = [item async for item in second_store.read(root_id.hex)]
                if any(
                    isinstance(item.event, TurnCancelled) and item.event.turn_id == command_id.hex for item in events
                ):
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("remote cancellation did not become durable")
    finally:
        release.set()
        stop.set()
        if second is not None:
            await second.aclose()
        if second_store is not None:
            await second_store.close()
        process.join(5)
        if process.is_alive():
            process.terminate()
            process.join(5)
        try:
            while True:
                reports.get_nowait()
        except queue.Empty:
            pass
        assert process.exitcode == 0


async def test_remote_answer_and_closing_nonowner_preserve_execution(postgres_dsn: str, pg_schema: str) -> None:
    @tool
    async def interview(ctx: Context) -> str:
        response = await ctx.ask(FreeText(prompt="name?"))
        return response.text

    class Asker(Agent):
        tools = [interview]

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="q", name="interview", args="{}")]),
                Sample(text="done"),
            ]
        ),
        owner_store,
        [Asker],
        default_model="m",
        coordinator=_coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Asker)
    turn_id = uuid4()
    observer_store: PostgresStore | None = None
    observer: Runtime | None = None
    finisher_store: PostgresStore | None = None
    finisher: Runtime | None = None
    try:
        async with owner.connect(root_id, writable=True) as first:
            await first.send("go", command_id=turn_id)
            deadline = time.monotonic() + 2
            raised: AskRaised | None = None
            while raised is None and time.monotonic() < deadline:
                for item in [entry async for entry in owner_store.read(root_id.hex)]:
                    if isinstance(item.event, AskRaised):
                        raised = item.event
                        break
                await asyncio.sleep(0.01)
            assert raised is not None

            observer_store = PostgresStore(postgres_dsn, schema=pg_schema)
            observer = Runtime(
                EchoProvider(),
                observer_store,
                [Asker],
                default_model="m",
                coordinator=_coordinator(observer_store),
            )
            await observer.start()
            async with observer.connect(root_id, writable=True):
                assert (await observer.status(root_id)).active
            await observer.aclose()
            observer = None
            assert (await owner.status(root_id)).active

            finisher_store = PostgresStore(postgres_dsn, schema=pg_schema)
            finisher = Runtime(
                EchoProvider(),
                finisher_store,
                [Asker],
                default_model="m",
                coordinator=_coordinator(finisher_store),
            )
            await finisher.start()
            async with finisher.connect(root_id, writable=True) as connection:
                await connection.answer(
                    UUID(hex=raised.ask_id),
                    FreeTextResponse(text="Malay"),
                    command_id=uuid4(),
                )
                result = await finisher._wait_result(root_id.hex, turn_id)
            assert result.text == "done"
    finally:
        if observer is not None:
            await observer.aclose()
        if observer_store is not None:
            await observer_store.close()
        if finisher is not None:
            await finisher.aclose()
        if finisher_store is not None:
            await finisher_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_fresh_runtime_takeover_interrupts_started_turn_once(postgres_dsn: str, pg_schema: str) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class GateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            started.set()
            await release.wait()
            yield StreamEnd(text="late")

    old_store = PostgresStore(postgres_dsn, schema=pg_schema)
    old_coordinator = _coordinator(old_store, lease_ttl=0.12)
    old = Runtime(
        GateProvider(),
        old_store,
        [Bot],
        default_model="m",
        coordinator=old_coordinator,
    )
    await old.start()
    root_id = await old.create(Bot)
    interrupted = uuid4()
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        old_connection = old.connect(root_id, writable=True)
        await old_connection.__aenter__()
        await old_connection.send("old", command_id=interrupted)
        await asyncio.wait_for(started.wait(), 1)
        await old_coordinator.close()
        await asyncio.sleep(0.15)

        replacement_store, replacement = await _runtime(postgres_dsn, pg_schema, lease_ttl=0.12)
        async with replacement.connect(root_id, writable=True) as connection:
            result = await connection.prompt("fresh", command_id=uuid4())
        assert result.text == "ok"
        events = [item.event async for item in replacement_store.read(root_id.hex)]
        matching = [
            event for event in events if isinstance(event, TurnInterrupted) and event.turn_id == interrupted.hex
        ]
        assert len(matching) == 1
        release.set()
        await asyncio.sleep(0.05)
        events = [item.event async for item in replacement_store.read(root_id.hex)]
        assert not any(isinstance(event, TurnCompleted) and event.turn_id == interrupted.hex for event in events)
        await old_connection.__aexit__(None, None, None)
    finally:
        release.set()
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        await old.aclose()
        await old_store.close()


async def test_writer_revalidation_recovers_from_transient_failure(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_store, owner = await _runtime(postgres_dsn, pg_schema)
    root_id = await owner.create(Bot)
    second_store: PostgresStore | None = None
    second: Runtime | None = None
    try:
        async with owner.connect(root_id, writable=True) as stale:
            assert owner.coordinator is not None
            original = owner.coordinator.writer_matches
            failed = asyncio.Event()

            async def flaky(token: Any) -> bool:
                matches = await original(token)
                if not matches and not failed.is_set():
                    failed.set()
                    raise CoordinatorUnavailable("transient writer validation failure")
                return matches

            monkeypatch.setattr(owner.coordinator, "writer_matches", flaky)
            second_store, second = await _runtime(postgres_dsn, pg_schema)
            async with second.connect(root_id, writable=True):
                await asyncio.wait_for(failed.wait(), 1)
                deadline = time.monotonic() + 1
                while stale._writer_token is not None and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                with pytest.raises(WriterReplaced):
                    stale._check_iteration()
    finally:
        if second is not None:
            await second.aclose()
        if second_store is not None:
            await second_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_closing_owner_rejects_command_waiting_for_root_lock(postgres_dsn: str, pg_schema: str) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class GateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            started.set()
            await release.wait()
            yield StreamEnd(text="late")

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        GateProvider(),
        owner_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    async with owner.connect(root_id, writable=True) as first:
        await first.send("running", command_id=uuid4())
    await asyncio.wait_for(started.wait(), 1)

    second_store, second = await _runtime(postgres_dsn, pg_schema)
    connection = second.connect(root_id, writable=True)
    await connection.__aenter__()
    command_id = uuid4()
    reached = asyncio.Event()
    assert owner.coordinator is not None
    original_handler = owner.coordinator._handler
    assert original_handler is not None

    async def observing_handler(envelope: Any, transact: Any) -> None:
        if getattr(envelope.payload, "command_id", None) == command_id:
            reached.set()
        await original_handler(envelope, transact)

    owner.coordinator._handler = observing_handler
    lock = owner._lock(root_id.hex)
    await lock.acquire()
    send_task = asyncio.create_task(connection.send("must not queue", command_id=command_id))
    close_task: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(reached.wait(), 1)
        close_task = asyncio.create_task(owner.aclose())
        deadline = time.monotonic() + 1
        while not owner._closing and time.monotonic() < deadline:
            await asyncio.sleep(0)
        assert owner._closing
        lock.release()
        with pytest.raises(CoordinatorUnavailable):
            await send_task
        await close_task
        events = [item.event async for item in second_store.read(root_id.hex)]
        assert not any(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events)
    finally:
        if lock.locked():
            lock.release()
        release.set()
        if not send_task.done():
            send_task.cancel()
            await asyncio.gather(send_task, return_exceptions=True)
        if close_task is not None and not close_task.done():
            await close_task
        await connection.__aexit__(None, None, None)
        await second.aclose()
        await second_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_cancellation_acceptance_commit_boundary_and_retry_targets(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class GateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            started.set()
            await release.wait()
            yield StreamEnd(text="done")

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(GateProvider(), owner_store, [Bot], default_model="m", coordinator=_coordinator(owner_store))
    await owner.start()
    root_id = await owner.create(Bot)
    first_id = uuid4()
    cancel_id = uuid4()
    later_id = uuid4()
    second_store: PostgresStore | None = None
    second: Runtime | None = None
    try:
        async with owner.connect(root_id, writable=True) as connection:
            await connection.send("first", command_id=first_id)
        await asyncio.wait_for(started.wait(), 1)

        second_store = PostgresStore(postgres_dsn, schema=pg_schema)
        second = Runtime(GateProvider(), second_store, [Bot], default_model="m", coordinator=_coordinator(second_store))
        await second.start()
        async with second.connect(root_id, writable=True) as connection:
            assert owner.coordinator is not None
            assert second.coordinator is not None
            original_apply = owner.coordinator._apply_request
            failed = False
            precommit_visible = False

            async def fail_before_commit(envelope: Any, ownership: Any, operation: Any) -> Any:
                async def wrapped(locked: Any, store: Any) -> Any:
                    nonlocal failed, precommit_visible
                    reply = await operation(locked, store)
                    if envelope.operation == "cancel" and not failed:
                        failed = True
                        visible = [item.event async for item in second_store.read(root_id.hex)]
                        precommit_visible = any(isinstance(event, CancellationRequested) for event in visible)
                        raise RuntimeError("injected before cancellation commit")
                    return reply

                return await original_apply(envelope, ownership, wrapped)

            monkeypatch.setattr(owner.coordinator, "_apply_request", fail_before_commit)
            with pytest.raises(CoordinatorUnavailable, match="owner could not apply"):
                await connection.cancel(command_id=cancel_id)
            monkeypatch.setattr(owner.coordinator, "_apply_request", original_apply)
            events = [item.event async for item in second_store.read(root_id.hex)]
            assert failed
            assert not precommit_visible
            assert not any(isinstance(event, CancellationRequested) for event in events)
            assert not any(isinstance(event, TurnCancelled) and event.turn_id == first_id.hex for event in events)

            original_request = second.coordinator.request
            lost = False

            async def lose_committed_reply(envelope: Any) -> Any:
                nonlocal lost
                reply = await original_request(envelope)
                if envelope.operation == "cancel" and not lost:
                    lost = True
                    raise CommandTimeout("injected response loss")
                return reply

            monkeypatch.setattr(second.coordinator, "request", lose_committed_reply)
            with pytest.raises(CommandTimeout):
                await connection.cancel(command_id=cancel_id)
            monkeypatch.setattr(second.coordinator, "request", original_request)
            events = [item.event async for item in second_store.read(root_id.hex)]
            cancellations = [event for event in events if isinstance(event, CancellationRequested)]
            assert lost
            assert len(cancellations) == 1
            assert cancellations[0].command_id == cancel_id.hex
            assert cancellations[0].targets == {root_id.hex: [first_id.hex]}
            assert sum(isinstance(event, TurnCancelled) and event.turn_id == first_id.hex for event in events) == 1

            await connection.send("later", command_id=later_id)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                events = [item.event async for item in second_store.read(root_id.hex)]
                if any(isinstance(event, TurnStarted) and event.turn_id == later_id.hex for event in events):
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("later turn did not start")

            assert (await connection.cancel(command_id=cancel_id)).duplicate
            events = [item.event async for item in second_store.read(root_id.hex)]
            cancellations = [event for event in events if isinstance(event, CancellationRequested)]
            assert len(cancellations) == 1
            assert cancellations[0].targets == {root_id.hex: [first_id.hex]}
            assert not any(isinstance(event, TurnCancelled) and event.turn_id == later_id.hex for event in events)
            release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                events = [item.event async for item in second_store.read(root_id.hex)]
                if any(isinstance(event, TurnCompleted) and event.turn_id == later_id.hex for event in events):
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("later turn did not complete")
    finally:
        release.set()
        if second is not None:
            await second.aclose()
        if second_store is not None:
            await second_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_tree_recovery_preserves_cancellation_targets_and_interrupts_only_started(
    postgres_dsn: str, pg_schema: str
) -> None:
    class Grandchild(Agent):
        model = "grandchild"

    class Child(Agent):
        model = "child"
        subagents = [Grandchild]

    class Root(Agent):
        subagents = [Child]

    class TreeProvider(EchoProvider):
        def __init__(self) -> None:
            self.started = {"child": asyncio.Event(), "grandchild": asyncio.Event()}
            self.release = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model in self.started:
                self.started[req.model].set()
                await self.release.wait()
            yield StreamEnd(text="done")

    async def emit(_: str) -> None:
        return None

    def actor_context(session_id: str, turn_id: str, call_id: str, depth: int) -> Context:
        return Context(
            session_id=session_id,
            turn_id=turn_id,
            call_id=call_id,
            depth=depth,
            deps=None,
            store=old_store,
            emit=emit,
        )

    provider = TreeProvider()
    old_store = PostgresStore(postgres_dsn, schema=pg_schema)
    old_coordinator = _coordinator(old_store, lease_ttl=0.12)
    old = Runtime(
        provider,
        old_store,
        [Root],
        default_model="root",
        coordinator=old_coordinator,
    )
    await old.start()
    root_id = await old.create(Root)
    root = await old_store.header(root_id.hex)
    assert root is not None
    async with old._lock(root.id):
        await old._ensure_owner_locked(root.id)
    child_public = await old._actor_spawn(
        root,
        Root,
        actor_context(root.id, "child-turn", "child-spawn", 0),
        "child",
        "work",
    )
    child_id = UUID(child_public).hex
    await asyncio.wait_for(provider.started["child"].wait(), 1)
    child = await old_store.header(child_id)
    assert child is not None
    grandchild_public = await old._actor_spawn(
        child,
        Child,
        actor_context(child.id, "grandchild-turn", "grandchild-spawn", 1),
        "grandchild",
        "work",
    )
    grandchild_id = UUID(grandchild_public).hex
    await asyncio.wait_for(provider.started["grandchild"].wait(), 1)
    assert not (await old.status(root_id)).active
    assert (await old.status(UUID(hex=child_id))).active
    assert (await old.status(UUID(hex=grandchild_id))).active
    assert old._connections == {}
    assert await old_coordinator.locate(root.id) is not None

    child_events = [item.event async for item in old_store.read(child_id)]
    grandchild_events = [item.event async for item in old_store.read(grandchild_id)]
    child_turn = next(event.turn_id for event in reversed(child_events) if isinstance(event, TurnStarted))
    grandchild_turn = next(event.turn_id for event in reversed(grandchild_events) if isinstance(event, TurnStarted))
    cancellation = CancellationRequested(
        command_id=uuid4().hex,
        targets={child_id: [child_turn]},
    )
    ownership = old._ownerships[root.id]
    async with old_coordinator.transaction(ownership) as store:
        await store.append(root.id, [cancellation])

    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        await old_coordinator.close()
        await asyncio.sleep(0.15)
        replacement_store = PostgresStore(postgres_dsn, schema=pg_schema)
        replacement = Runtime(
            EchoProvider(),
            replacement_store,
            [Root],
            default_model="root",
            coordinator=_coordinator(replacement_store, lease_ttl=0.12),
        )
        await replacement.start()
        async with replacement.connect(root_id, writable=True):
            pass
        async with replacement._lock(root.id):
            current = replacement._ownerships.get(root.id)
            if current is not None:
                await replacement._recover_locked(root.id, current)

        recovered_root = [item.event async for item in replacement_store.read(root.id)]
        recovered_child = [item.event async for item in replacement_store.read(child_id)]
        recovered_grandchild = [item.event async for item in replacement_store.read(grandchild_id)]
        durable = next(event for event in recovered_root if isinstance(event, CancellationRequested))
        assert durable.targets == {child_id: [child_turn]}
        assert sum(isinstance(event, TurnCancelled) and event.turn_id == child_turn for event in recovered_child) == 1
        assert (
            sum(
                isinstance(event, TurnInterrupted) and event.turn_id == grandchild_turn
                for event in recovered_grandchild
            )
            == 1
        )
        assert not any(isinstance(event, TurnInterrupted) and event.turn_id == child_turn for event in recovered_child)
    finally:
        provider.release.set()
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        await old.aclose()
        await old_store.close()


async def test_graceful_owner_stop_leaves_queued_input_for_next_owner(postgres_dsn: str, pg_schema: str) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class GateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            started.set()
            await release.wait()
            yield StreamEnd(text="late")

    old_store = PostgresStore(postgres_dsn, schema=pg_schema)
    old = Runtime(
        GateProvider(),
        old_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(old_store),
    )
    await old.start()
    root_id = await old.create(Bot)
    first = uuid4()
    queued = uuid4()
    async with old.connect(root_id, writable=True) as connection:
        await connection.send("running", command_id=first)
        await asyncio.wait_for(started.wait(), 1)
        await connection.send("queued", command_id=queued)
    await old.aclose()

    next_store, next_owner = await _runtime(postgres_dsn, pg_schema)
    assert next_owner.coordinator is not None
    assert await next_owner.coordinator.locate(root_id.hex) is None
    try:
        async with next_owner.connect(root_id, writable=True):
            result = await next_owner._wait_result(root_id.hex, queued)
        assert result.outcome == "completed"
        events = [item.event async for item in next_store.read(root_id.hex)]
        assert sum(isinstance(event, TurnInterrupted) and event.turn_id == first.hex for event in events) == 1
        assert sum(isinstance(event, TurnCompleted) and event.turn_id == queued.hex for event in events) == 1
    finally:
        release.set()
        await next_owner.aclose()
        await next_store.close()
        await old_store.close()


async def test_remote_prompt_runs_on_owner_and_infrastructure_failure_is_bounded(
    postgres_dsn: str, pg_schema: str
) -> None:
    class SequencedProvider(EchoProvider):
        def __init__(self) -> None:
            self.calls = 0
            self.first_started = asyncio.Event()
            self.third_started = asyncio.Event()
            self.release_first = asyncio.Event()
            self.release_third = asyncio.Event()

        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
                await self.release_first.wait()
                yield StreamEnd(text="first")
            elif self.calls == 2:
                yield StreamEnd(text="remote result")
            else:
                self.third_started.set()
                await self.release_third.wait()
                yield StreamEnd(text="third")

    class NoExecutionProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            raise AssertionError("remote observer executed owner work")
            yield StreamEnd(text="unreachable")

    provider = SequencedProvider()
    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        provider,
        owner_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    first = uuid4()
    async with owner.connect(root_id, writable=True) as connection:
        await connection.send("first", command_id=first)
    await asyncio.wait_for(provider.first_started.wait(), 1)

    remote_store = PostgresStore(postgres_dsn, schema=pg_schema)
    remote = Runtime(
        NoExecutionProvider(),
        remote_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(remote_store),
    )
    await remote.start()
    second = uuid4()
    third = uuid4()
    failed = uuid4()
    connection = remote.connect(root_id, writable=True)
    await connection.__aenter__()
    prompt_task = asyncio.create_task(connection.prompt("second", command_id=second))
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            events = [item.event async for item in owner_store.read(root_id.hex)]
            if any(isinstance(event, InputQueued) and event.command_id == second.hex for event in events):
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("remote prompt was not durably accepted")
        await connection.send("third", command_id=third)
        provider.release_first.set()
        result = await asyncio.wait_for(prompt_task, 1)
        assert result.text == "remote result"
        await asyncio.wait_for(provider.third_started.wait(), 1)

        assert owner.coordinator is not None
        original_handler = owner.coordinator._handler
        assert original_handler is not None

        async def failing_handler(envelope: Any, transact: Any) -> None:
            if getattr(envelope.payload, "command_id", None) == failed:
                raise RuntimeError("injected owner handler failure")
            await original_handler(envelope, transact)

        owner.coordinator._handler = failing_handler
        with pytest.raises(CoordinatorUnavailable, match="owner could not apply"):
            await asyncio.wait_for(connection.prompt("fail", command_id=failed), 1)
        events = [item.event async for item in owner_store.read(root_id.hex)]
        assert not any(isinstance(event, InputQueued) and event.command_id == failed.hex for event in events)
    finally:
        provider.release_first.set()
        provider.release_third.set()
        if not prompt_task.done():
            prompt_task.cancel()
            await asyncio.gather(prompt_task, return_exceptions=True)
        await connection.__aexit__(None, None, None)
        await remote.aclose()
        await remote_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_first_input_prestart_failure_relinquishes_and_recovers_same_uuid(
    postgres_dsn: str, pg_schema: str
) -> None:
    class NoExecutionProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            raise AssertionError("remote runtime executed owner work")
            yield StreamEnd(text="unreachable")

    deps_entered = asyncio.Event()
    release_deps = asyncio.Event()

    async def fail_deps(_: Any) -> None:
        deps_entered.set()
        await release_deps.wait()
        raise RuntimeError("first input deps failure")

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        EchoProvider(),
        owner_store,
        [Bot],
        default_model="m",
        deps_factory=fail_deps,
        coordinator=_coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    command_id = uuid4()
    async with owner.connect(root_id, writable=True) as owner_connection:
        await owner_connection.send("first fails", command_id=command_id)
    await asyncio.wait_for(deps_entered.wait(), 1)

    remote_store = PostgresStore(postgres_dsn, schema=pg_schema)
    remote = Runtime(
        NoExecutionProvider(),
        remote_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(remote_store),
    )
    await remote.start()
    connection = remote.connect(root_id, writable=True)
    await connection.__aenter__()
    prompt_task = asyncio.create_task(connection.prompt("first fails", command_id=command_id))
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if owner._activations[root_id.hex] >= 2:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("duplicate command was not accepted")
        release_deps.set()
        with pytest.raises(RemoteExecutionError, match="no active owner execution"):
            await asyncio.wait_for(prompt_task, 1)
        header = await owner_store.header(root_id.hex)
        assert header is not None and header.status == "queued"
        events = [item.event async for item in owner_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
        assert not any(isinstance(event, TurnStarted) and event.turn_id == command_id.hex for event in events)
        assert owner.coordinator is not None
        assert await owner.coordinator.locate(root_id.hex) is None

        recovery_store, recovery = await _runtime(postgres_dsn, pg_schema)
        try:
            async with recovery.connect(root_id, writable=True):
                result = await recovery._wait_result(root_id.hex, command_id)
            assert result.outcome == "completed"
            recovered = [item.event async for item in recovery_store.read(root_id.hex)]
            assert (
                sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in recovered) == 1
            )
            assert sum(isinstance(event, TurnCompleted) and event.turn_id == command_id.hex for event in recovered) == 1
        finally:
            await recovery.aclose()
            await recovery_store.close()
    finally:
        release_deps.set()
        if not prompt_task.done():
            prompt_task.cancel()
            await asyncio.gather(prompt_task, return_exceptions=True)
        await connection.__aexit__(None, None, None)
        await remote.aclose()
        await remote_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_writer_replacement_between_submission_and_acceptance_rejects_command(
    postgres_dsn: str, pg_schema: str
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class GateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            started.set()
            await release.wait()
            yield StreamEnd(text="done")

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        GateProvider(),
        owner_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    async with owner.connect(root_id, writable=True) as first:
        await first.send("running", command_id=uuid4())
    await asyncio.wait_for(started.wait(), 1)

    remote_store, remote = await _runtime(postgres_dsn, pg_schema)
    connection = remote.connect(root_id, writable=True)
    await connection.__aenter__()
    command_id = uuid4()
    reached = asyncio.Event()
    assert owner.coordinator is not None
    original_handler = owner.coordinator._handler
    assert original_handler is not None

    async def observing_handler(envelope: Any, transact: Any) -> None:
        if getattr(envelope.payload, "command_id", None) == command_id:
            reached.set()
        await original_handler(envelope, transact)

    owner.coordinator._handler = observing_handler
    lock = owner._lock(root_id.hex)
    await lock.acquire()
    send_task = asyncio.create_task(connection.send("stale writer", command_id=command_id))
    try:
        await asyncio.wait_for(reached.wait(), 1)
        ownership = owner._ownerships[root_id.hex]
        async with owner.coordinator.transaction(ownership) as store:
            await store.claim_writer(uuid4())
        lock.release()
        with pytest.raises(WriterReplaced):
            await send_task
        events = [item.event async for item in owner_store.read(root_id.hex)]
        assert not any(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events)
    finally:
        if lock.locked():
            lock.release()
        release.set()
        if not send_task.done():
            send_task.cancel()
            await asyncio.gather(send_task, return_exceptions=True)
        await connection.__aexit__(None, None, None)
        await remote.aclose()
        await remote_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_command_retries_same_envelope_after_idle_owner_releases(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_store, owner = await _runtime(postgres_dsn, pg_schema)
    root_id = await owner.create(Bot)
    async with owner._lock(root_id.hex):
        await owner._ensure_owner_locked(root_id.hex)
    assert owner.coordinator is not None
    original_release = owner.coordinator.release
    release_entered = asyncio.Event()
    allow_release = asyncio.Event()
    released = asyncio.Event()

    async def gated_release(ownership: Any) -> bool:
        release_entered.set()
        await allow_release.wait()
        result = await original_release(ownership)
        released.set()
        return result

    monkeypatch.setattr(owner.coordinator, "release", gated_release)
    remote_store, remote = await _runtime(postgres_dsn, pg_schema)
    assert remote.coordinator is not None
    connection = remote.connect(root_id, writable=True)
    enter_task = asyncio.create_task(connection.__aenter__())
    command_id = uuid4()
    send_task: asyncio.Task[Any] | None = None
    initial_locate = remote.coordinator.locate
    locate_entered = asyncio.Event()
    allow_locate = asyncio.Event()
    gated = False

    async def stale_locate(requested_root_id: str) -> Any:
        nonlocal gated
        result = await initial_locate(requested_root_id)
        if result is not None and not gated:
            gated = True
            locate_entered.set()
            await allow_locate.wait()
        return result

    try:
        await asyncio.wait_for(release_entered.wait(), 1)
        await asyncio.wait_for(enter_task, 1)
        monkeypatch.setattr(remote.coordinator, "locate", stale_locate)
        send_task = asyncio.create_task(connection.send("race", command_id=command_id))
        await asyncio.wait_for(locate_entered.wait(), 1)
        allow_release.set()
        await asyncio.wait_for(released.wait(), 1)
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if await initial_locate(root_id.hex) is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("idle owner did not release")
        allow_locate.set()
        receipt = await asyncio.wait_for(send_task, 1)
        assert receipt.command_id == command_id
        result = await asyncio.wait_for(remote._wait_result(root_id.hex, command_id), 1)
        assert result.outcome == "completed"
        events = [item.event async for item in remote_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
    finally:
        allow_release.set()
        allow_locate.set()
        if not enter_task.done():
            enter_task.cancel()
        if send_task is not None and not send_task.done():
            send_task.cancel()
        await asyncio.gather(
            enter_task,
            *(tuple([send_task]) if send_task is not None else ()),
            return_exceptions=True,
        )
        await connection.__aexit__(None, None, None)
        await remote.aclose()
        await remote_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_remote_prompt_bounds_owner_local_execution_failure(postgres_dsn: str, pg_schema: str) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    deps_calls = 0

    class FirstGateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            started.set()
            await release.wait()
            yield StreamEnd(text="first")

    class NoExecutionProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            raise AssertionError("remote runtime executed owner work")
            yield StreamEnd(text="unreachable")

    def deps_factory(_: Any) -> None:
        nonlocal deps_calls
        deps_calls += 1
        if deps_calls > 1:
            raise RuntimeError("injected deps failure")

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        FirstGateProvider(),
        owner_store,
        [Bot],
        default_model="m",
        deps_factory=deps_factory,
        coordinator=_coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    async with owner.connect(root_id, writable=True) as first:
        await first.send("first", command_id=uuid4())
    await asyncio.wait_for(started.wait(), 1)

    remote_store = PostgresStore(postgres_dsn, schema=pg_schema)
    remote = Runtime(
        NoExecutionProvider(),
        remote_store,
        [Bot],
        default_model="m",
        coordinator=_coordinator(remote_store),
    )
    await remote.start()
    command_id = uuid4()
    connection = remote.connect(root_id, writable=True)
    await connection.__aenter__()
    prompt_task = asyncio.create_task(connection.prompt("fails remotely", command_id=command_id))
    recovery_store: PostgresStore | None = None
    recovery: Runtime | None = None
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            events = [item.event async for item in owner_store.read(root_id.hex)]
            if any(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events):
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("remote prompt was not durably accepted")
        release.set()
        with pytest.raises(RemoteExecutionError, match="no active owner execution"):
            await asyncio.wait_for(prompt_task, 1)
        events = [item.event async for item in owner_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
        assert not any(isinstance(event, TurnStarted) and event.turn_id == command_id.hex for event in events)
        assert await owner.coordinator.locate(root_id.hex) is None

        recovery_store, recovery = await _runtime(postgres_dsn, pg_schema)
        async with recovery.connect(root_id, writable=True):
            result = await recovery._wait_result(root_id.hex, command_id)
        assert result.outcome == "completed"
        recovered = [item.event async for item in recovery_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in recovered) == 1
        assert sum(isinstance(event, TurnCompleted) and event.turn_id == command_id.hex for event in recovered) == 1
    finally:
        release.set()
        if recovery is not None:
            await recovery.aclose()
        if recovery_store is not None:
            await recovery_store.close()
        if not prompt_task.done():
            prompt_task.cancel()
            await asyncio.gather(prompt_task, return_exceptions=True)
        await connection.__aexit__(None, None, None)
        await remote.aclose()
        await remote_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_failure_before_turn_selection_relinquishes_and_durable_result_wins(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_store, owner = await _runtime(postgres_dsn, pg_schema)
    root_id = await owner.create(Bot)
    entered = asyncio.Event()
    release = asyncio.Event()
    failed = False
    original_reconcile = owner._reconcile_lifecycle

    async def fail_once(header: Any) -> None:
        nonlocal failed
        if not failed:
            failed = True
            entered.set()
            await release.wait()
            raise RuntimeError("failure before turn selection")
        await original_reconcile(header)

    monkeypatch.setattr(owner, "_reconcile_lifecycle", fail_once)
    command_id = uuid4()
    async with owner.connect(root_id, writable=True) as connection:
        await connection.send("recover me", command_id=command_id)
    await asyncio.wait_for(entered.wait(), 1)
    failed_wait = asyncio.create_task(owner._wait_result(root_id.hex, command_id))
    recovery_store: PostgresStore | None = None
    recovery: Runtime | None = None
    try:
        release.set()
        with pytest.raises(RuntimeError, match="failure before turn selection"):
            await asyncio.wait_for(failed_wait, 1)
        assert owner.coordinator is not None
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if await owner.coordinator.locate(root_id.hex) is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("failed owner did not relinquish")
        events = [item.event async for item in owner_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
        assert not any(isinstance(event, TurnStarted) and event.turn_id == command_id.hex for event in events)

        recovery_store, recovery = await _runtime(postgres_dsn, pg_schema)
        async with recovery.connect(root_id, writable=True) as connection:
            result = await connection.prompt("recover me", command_id=command_id)
        assert result.text == "ok"
        durable_result = await owner._wait_result(root_id.hex, command_id)
        assert durable_result.outcome == "completed"
        recovered = [item.event async for item in recovery_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in recovered) == 1
        assert sum(isinstance(event, TurnCompleted) and event.turn_id == command_id.hex for event in recovered) == 1

        async with owner.connect(root_id, writable=True) as connection:
            old_owner_result = await connection.prompt("recover me", command_id=command_id)
        assert old_owner_result.text == "ok"
    finally:
        release.set()
        if not failed_wait.done():
            failed_wait.cancel()
            await asyncio.gather(failed_wait, return_exceptions=True)
        if recovery is not None:
            await recovery.aclose()
        if recovery_store is not None:
            await recovery_store.close()
        await owner.aclose()
        await owner_store.close()


async def test_failed_root_relinquishes_after_active_child_becomes_idle(postgres_dsn: str, pg_schema: str) -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    child_started = asyncio.Event()
    release_child = asyncio.Event()
    root_failed = asyncio.Event()
    root_id: UUID | None = None

    class ChildGateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            if req.model == "child":
                child_started.set()
                await release_child.wait()
            yield StreamEnd(text="ok")

    async def deps_factory(header: Any) -> None:
        if root_id is not None and header.id == root_id.hex:
            root_failed.set()
            raise RuntimeError("root prestart failure")

    async def emit(_: str) -> None:
        return None

    store = PostgresStore(postgres_dsn, schema=pg_schema)
    runtime = Runtime(
        ChildGateProvider(),
        store,
        [Root],
        default_model="root",
        deps_factory=deps_factory,
        coordinator=_coordinator(store),
    )
    await runtime.start()
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    async with runtime._lock(root.id):
        await runtime._ensure_owner_locked(root.id)
    context = Context(
        session_id=root.id,
        turn_id=uuid4().hex,
        call_id=uuid4().hex,
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )
    await runtime._actor_spawn(root, Root, context, "child", "work")
    await asyncio.wait_for(child_started.wait(), 1)
    command_id = uuid4()
    async with runtime.connect(root_id, writable=True) as connection:
        await connection.send("fail while child runs", command_id=command_id)
    await asyncio.wait_for(root_failed.wait(), 1)
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        assert runtime.coordinator is not None
        await asyncio.sleep(0)
        assert await runtime.coordinator.locate(root.id) is not None
        release_child.set()
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if await runtime.coordinator.locate(root.id) is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("failed root was not relinquished after child became idle")

        replacement_store = PostgresStore(postgres_dsn, schema=pg_schema)
        replacement = Runtime(
            EchoProvider(),
            replacement_store,
            [Root],
            default_model="root",
            coordinator=_coordinator(replacement_store),
        )
        await replacement.start()
        async with replacement.connect(root_id, writable=True) as connection:
            result = await connection.prompt("fail while child runs", command_id=command_id)
        assert result.text == "ok"
        events = [item.event async for item in replacement_store.read(root.id)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
        assert sum(isinstance(event, TurnCompleted) and event.turn_id == command_id.hex for event in events) == 1
    finally:
        release_child.set()
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        await runtime.aclose()
        await store.close()


@pytest.mark.parametrize("failed_method", ["header", "read_page"])
async def test_persistent_store_read_failure_before_selection_relinquishes(
    postgres_dsn: str,
    pg_schema: str,
    monkeypatch: pytest.MonkeyPatch,
    failed_method: str,
) -> None:
    store, owner = await _runtime(postgres_dsn, pg_schema)
    root_id = await owner.create(Bot)
    original_activate = owner._activate
    accept_only = True

    def gated_activate(agent_id: str, actor_root_id: str) -> None:
        if not accept_only:
            original_activate(agent_id, actor_root_id)

    monkeypatch.setattr(owner, "_activate", gated_activate)
    command_id = uuid4()
    async with owner.connect(root_id, writable=True) as connection:
        await connection.send("recover persistent read failure", command_id=command_id)
    accept_only = False
    original_method = getattr(store, failed_method)

    async def fail_read(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(f"persistent {failed_method} failure")

    monkeypatch.setattr(store, failed_method, fail_read)
    original_activate(root_id.hex, root_id.hex)
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        assert owner.coordinator is not None
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if await owner.coordinator.locate(root_id.hex) is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("owner with persistent Store read failure did not relinquish")
        monkeypatch.setattr(store, failed_method, original_method)

        replacement_store, replacement = await _runtime(postgres_dsn, pg_schema)
        async with replacement.connect(root_id, writable=True) as connection:
            result = await connection.prompt("recover persistent read failure", command_id=command_id)
        assert result.text == "ok"
        events = [item.event async for item in replacement_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
        assert sum(isinstance(event, TurnCompleted) and event.turn_id == command_id.hex for event in events) == 1
    finally:
        monkeypatch.setattr(store, failed_method, original_method)
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        await owner.aclose()
        await store.close()


async def test_persistent_read_failure_after_selection_relinquishes(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    deps_entered = asyncio.Event()
    release_deps = asyncio.Event()

    async def fail_deps(_: Any) -> None:
        deps_entered.set()
        await release_deps.wait()
        raise RuntimeError("failure after command selection")

    store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        EchoProvider(),
        store,
        [Bot],
        default_model="m",
        deps_factory=fail_deps,
        coordinator=_coordinator(store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    command_id = uuid4()
    async with owner.connect(root_id, writable=True) as connection:
        await connection.send("recover after selection", command_id=command_id)
    await asyncio.wait_for(deps_entered.wait(), 1)
    original_read_page = store.read_page

    async def fail_read(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("persistent read failure after selection")

    monkeypatch.setattr(store, "read_page", fail_read)
    release_deps.set()
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    try:
        assert owner.coordinator is not None
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if await owner.coordinator.locate(root_id.hex) is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("post-selection Store failure did not relinquish")
        monkeypatch.setattr(store, "read_page", original_read_page)

        replacement_store, replacement = await _runtime(postgres_dsn, pg_schema)
        async with replacement.connect(root_id, writable=True) as connection:
            result = await connection.prompt("recover after selection", command_id=command_id)
        assert result.text == "ok"
        events = [item.event async for item in replacement_store.read(root_id.hex)]
        assert sum(isinstance(event, InputQueued) and event.command_id == command_id.hex for event in events) == 1
        assert sum(isinstance(event, TurnCompleted) and event.turn_id == command_id.hex for event in events) == 1
    finally:
        release_deps.set()
        monkeypatch.setattr(store, "read_page", original_read_page)
        if replacement is not None:
            await replacement.aclose()
        if replacement_store is not None:
            await replacement_store.close()
        await owner.aclose()
        await store.close()


async def test_unknown_failed_prestart_marker_falls_back_after_terminal_and_sibling_idle(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    root_started = asyncio.Event()
    release_root = asyncio.Event()
    child_deps_entered = asyncio.Event()
    release_child_deps = asyncio.Event()

    class GateProvider(EchoProvider):
        async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
            root_started.set()
            await release_root.wait()
            yield StreamEnd(text="ok")

    async def deps_factory(header: Any) -> None:
        if header.parent_id is not None:
            child_deps_entered.set()
            await release_child_deps.wait()
            raise RuntimeError("child prestart failure")

    async def emit(_: str) -> None:
        return None

    store = PostgresStore(postgres_dsn, schema=pg_schema)
    runtime = Runtime(
        GateProvider(),
        store,
        [Root],
        default_model="root",
        deps_factory=deps_factory,
        coordinator=_coordinator(store),
    )
    await runtime.start()
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    async with runtime.connect(root_id, writable=True) as connection:
        await connection.send("keep sibling active", command_id=uuid4())
    await asyncio.wait_for(root_started.wait(), 1)
    context = Context(
        session_id=root.id,
        turn_id=uuid4().hex,
        call_id=uuid4().hex,
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )
    child_public = await runtime._actor_spawn(root, Root, context, "child", "fail")
    child_id = UUID(child_public).hex
    await asyncio.wait_for(child_deps_entered.wait(), 1)
    child_events = [item.event async for item in store.read(child_id)]
    child_command = next(event.command_id for event in child_events if isinstance(event, InputQueued))
    original_read_page = store.read_page

    async def fail_read(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("persistent child read failure")

    monkeypatch.setattr(store, "read_page", fail_read)
    release_child_deps.set()
    try:
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            markers = runtime._failed_prestarts.get(root.id, {})
            if child_id in markers:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("unknown child marker was not recorded")
        monkeypatch.setattr(store, "read_page", original_read_page)
        assert runtime.coordinator is not None
        ownership = runtime._ownerships[root.id]
        async with runtime._lock(root.id):
            async with runtime.coordinator.transaction(ownership) as guarded:
                await guarded.append(child_id, [TurnCancelled(turn_id=child_command, reason="cancelled")])
        assert await runtime.coordinator.locate(root.id) is not None
        release_root.set()
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if await runtime.coordinator.locate(root.id) is None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("ordinary idle release did not clear terminal failed marker")
    finally:
        release_root.set()
        release_child_deps.set()
        monkeypatch.setattr(store, "read_page", original_read_page)
        await runtime.aclose()
        await store.close()


async def test_multiple_failed_markers_preserve_started_work_then_release_queued(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Child(Agent):
        model = "child"

    class Root(Agent):
        subagents = [Child]

    async def emit(_: str) -> None:
        return None

    store = PostgresStore(postgres_dsn, schema=pg_schema)
    runtime = Runtime(
        EchoProvider(),
        store,
        [Root],
        default_model="root",
        coordinator=_coordinator(store, lease_ttl=5.0),
    )
    await runtime.start()
    root_id = await runtime.create(Root)
    root = await store.header(root_id.hex)
    assert root is not None
    original_activate = runtime._activate
    monkeypatch.setattr(runtime, "_activate", lambda agent_id, actor_root_id: None)
    root_command = uuid4()
    async with runtime.connect(root_id, writable=True) as connection:
        await connection.send("started but abandoned", command_id=root_command)
    assert runtime.coordinator is not None
    ownership = runtime._ownerships[root.id]
    async with runtime.coordinator.transaction(ownership) as guarded:
        await guarded.append(root.id, [TurnStarted(turn_id=root_command.hex, input="started but abandoned")])
    context = Context(
        session_id=root.id,
        turn_id=uuid4().hex,
        call_id=uuid4().hex,
        depth=0,
        deps=None,
        store=store,
        emit=emit,
    )
    child_public = await runtime._actor_spawn(root, Root, context, "child", "queued")
    child_id = UUID(child_public).hex
    runtime._failed_prestarts[root.id] = {root.id: root_command.hex, child_id: None}
    original_relinquish = runtime.coordinator.relinquish_failed_prestart
    attempts: list[tuple[str, bool]] = []

    async def tracked_relinquish(current: Any, agent_id: str) -> bool:
        result = await original_relinquish(current, agent_id)
        attempts.append((agent_id, result))
        return result

    monkeypatch.setattr(runtime.coordinator, "relinquish_failed_prestart", tracked_relinquish)
    original_read_page = store.read_page

    async def fail_read(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("marker journals unavailable")

    monkeypatch.setattr(store, "read_page", fail_read)
    try:
        async with runtime._lock(root.id):
            await runtime._release_if_idle_locked(root.id)
        assert attempts == [(root.id, False), (child_id, False)]
        assert await runtime.coordinator.locate(root.id) is not None
        attempts.clear()
        monkeypatch.setattr(store, "read_page", original_read_page)
        async with runtime.coordinator.transaction(ownership) as guarded:
            await guarded.append(root.id, [TurnInterrupted(turn_id=root_command.hex, reason="owner_lost")])
            await guarded.set_active(root.id, False)
            await guarded.set_active(child_id, False)
        root_header = await store.header(root.id)
        child_header = await store.header(child_id)
        assert root_header is not None and root_header.status == "interrupted"
        assert child_header is not None and child_header.status == "queued"
        assert not await runtime.coordinator.active(root.id)
        async with store._lock:
            conn = await store._connection()
            cursor = await conn.execute(
                store._sql(
                    "SELECT owner_instance, generation, expires_at > clock_timestamp(),"
                    " (SELECT count(*) FROM {schema}.coordinator_requests"
                    " WHERE root_id = %s AND reply IS NULL AND deadline > clock_timestamp())"
                    " FROM {schema}.coordinator_roots WHERE root_id = %s"
                ),
                (root.id, root.id),
            )
            owner_row = await cursor.fetchone()
        assert owner_row == (str(ownership.instance_id), ownership.generation, True, 0)
        async with runtime._lock(root.id):
            await runtime._release_if_idle_locked(root.id)
        assert attempts == [(root.id, False), (child_id, True)]
        assert await runtime.coordinator.locate(root.id) is None
    finally:
        monkeypatch.setattr(runtime, "_activate", original_activate)
        monkeypatch.setattr(store, "read_page", original_read_page)
        await runtime.aclose()
        await store.close()
