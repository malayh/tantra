from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import Agent, CommandTimeout, PostgresCoordinator, PostgresStore, Runtime, Sample, tool
from tantra.events import AskAnswered, LoggedEvent, SessionEvent, TurnCompleted
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS, FakeProvider
from tantra.websocket import InputCodec, SessionSocket, SocketAccess, SocketAuth, SocketBinding

pytest.importorskip("psycopg")


class Bot(Agent):
    pass


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="ok")


class BlockingProvider(EchoProvider):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        await self.release.wait()
        yield StreamEnd(text="done")


class GatedFakeProvider(FakeProvider):
    def __init__(self, samples: list[Sample]) -> None:
        super().__init__(samples)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if self._next == 0:
            self.started.set()
            await self.release.wait()
        async for event in super().stream(req):
            yield event


class SocketClient:
    def __init__(self, endpoint: SessionSocket) -> None:
        self.endpoint = endpoint
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.frames: list[dict[str, Any]] = []
        self.task: asyncio.Task[None] | None = None

    async def open(self) -> None:
        scope = {
            "type": "websocket",
            "headers": [(b"origin", b"https://app.test")],
            "subprotocols": ["tantra.session.v1"],
        }
        await self.incoming.put({"type": "websocket.connect"})
        self.task = asyncio.create_task(self.endpoint(scope, self.incoming.get, self.outgoing.put))
        accepted = await asyncio.wait_for(self.outgoing.get(), 2)
        assert accepted == {"type": "websocket.accept", "subprotocol": "tantra.session.v1"}

    async def send(self, frame: dict[str, Any]) -> None:
        await self.incoming.put({"type": "websocket.receive", "text": json.dumps(frame)})

    async def wait(self, predicate: Callable[[dict[str, Any]], bool], *, after: int = 0) -> dict[str, Any]:
        while True:
            for frame in self.frames[after:]:
                if predicate(frame):
                    return frame
            message = await asyncio.wait_for(self.outgoing.get(), 3)
            if message["type"] == "websocket.send":
                frame = json.loads(message["text"])
            else:
                frame = {"type": "close", "code": message["code"], "reason": message["reason"]}
            self.frames.append(frame)

    async def disconnect(self) -> None:
        if self.task is None or self.task.done():
            return
        await self.incoming.put({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(self.task, 3)


def coordinator(store: PostgresStore) -> PostgresCoordinator:
    return PostgresCoordinator(store, lease_ttl=0.3, request_timeout=2, catch_up_interval=0.02)


async def runtime(
    dsn: str,
    schema: str,
    provider: Any,
    agents: list[type[Agent]] | None = None,
) -> tuple[PostgresStore, Runtime]:
    store = PostgresStore(dsn, schema=schema)
    instance = Runtime(provider, store, agents or [Bot], default_model="m", coordinator=coordinator(store))
    await instance.start()
    return store, instance


def endpoint(
    instance: Runtime,
    root_id: UUID,
    policy: Callable[[SocketAccess], Any] | None = None,
) -> SessionSocket:
    async def revalidate() -> bool:
        return True

    async def bind(scope: Any) -> SocketBinding:
        return SocketBinding(root_id, SocketAuth(principal="user", revalidate=revalidate, submitted_by="user"))

    async def allow(access: SocketAccess) -> bool:
        if policy is None:
            return True
        return bool(await policy(access))

    async def views(context: Any) -> Callable[[LoggedEvent], Any]:
        async def project(event: LoggedEvent) -> Any:
            return event.event.model_dump(mode="json")

        return project

    return SessionSocket(
        instance,
        bind=bind,
        policy=allow,
        input_codec=InputCodec(str, lambda value: value),
        views=views,
        allowed_origins={"https://app.test"},
    )


async def subscribe(
    client: SocketClient, root_id: UUID, subscription_id: UUID, after: int, mode: str
) -> dict[str, Any]:
    mark = len(client.frames)
    await client.send(
        {
            "type": "subscribe",
            "subscription_id": str(subscription_id),
            "actor_id": str(root_id),
            "after": after,
            "mode": mode,
        }
    )
    return await client.wait(
        lambda frame: frame["type"] == "ready" and frame["subscription_id"] == str(subscription_id),
        after=mark,
    )


async def events(store: PostgresStore, root_id: UUID) -> list[SessionEvent]:
    return [item.event async for item in store.read(root_id.hex)]


async def wait_for_event(store: PostgresStore, root_id: UUID, event_type: type[SessionEvent]) -> SessionEvent:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        for event in await events(store, root_id):
            if isinstance(event, event_type):
                return event
        await asyncio.sleep(0.01)
    raise AssertionError(f"{event_type.__name__} did not become durable")


async def close_all(clients: list[SocketClient], instances: list[tuple[PostgresStore, Runtime]]) -> None:
    await asyncio.gather(*(client.disconnect() for client in clients))
    for store, instance in instances:
        await instance.aclose()
        await store.close()


async def test_remote_writer_replacement_downgrades_replays_and_requires_explicit_reclaim(
    postgres_dsn: str,
    pg_schema: str,
) -> None:
    first_store, first = await runtime(postgres_dsn, pg_schema, EchoProvider())
    root_id = await first.create(Bot)
    second_store, second = await runtime(postgres_dsn, pg_schema, EchoProvider())
    first_client = SocketClient(endpoint(first, root_id))
    second_client = SocketClient(endpoint(second, root_id))
    clients = [first_client, second_client]
    instances = [(second_store, second), (first_store, first)]
    first_subscription, second_subscription = uuid4(), uuid4()
    try:
        await first_client.open()
        first_ready = await subscribe(first_client, root_id, first_subscription, 0, "write")
        assert first_ready["mode"] == "write"
        initial_cursor = first_ready["cursor"]

        first_mark = len(first_client.frames)
        await second_client.open()
        second_ready = await subscribe(second_client, root_id, second_subscription, initial_cursor, "write")
        assert second_ready["mode"] == "write"
        await first_client.wait(lambda frame: frame["type"] == "writer_lost", after=first_mark)
        downgraded = await first_client.wait(
            lambda frame: frame["type"] == "ready" and frame["mode"] == "read",
            after=first_mark,
        )
        assert downgraded["cursor"] == initial_cursor

        command_id = uuid4()
        replay_mark = len(first_client.frames)
        receipt_mark = len(second_client.frames)
        await second_client.send(
            {
                "type": "send",
                "subscription_id": str(second_subscription),
                "command_id": str(command_id),
                "input": "remote",
            }
        )
        receipt = await second_client.wait(
            lambda frame: frame["type"] == "receipt" and frame["command_id"] == str(command_id),
            after=receipt_mark,
        )
        assert not receipt["duplicate"]
        completed = await first_client.wait(
            lambda frame: frame["type"] == "event" and frame["body"]["type"] == "turn_completed",
            after=replay_mark,
        )
        replay = [frame for frame in first_client.frames[replay_mark:] if frame["type"] == "event"]
        assert [frame["seq"] for frame in replay] == list(range(initial_cursor + 1, completed["seq"] + 1))
        queued = next(frame for frame in replay if frame["body"]["type"] == "input_queued")
        assert queued["body"]["submitted_by"] == "user"

        first_mark = len(first_client.frames)
        second_mark = len(second_client.frames)
        await first_client.send({"type": "acquire_writer", "subscription_id": str(first_subscription)})
        reclaimed = await first_client.wait(
            lambda frame: frame["type"] == "ready" and frame["mode"] == "write",
            after=first_mark,
        )
        assert reclaimed["cursor"] == completed["seq"]
        await second_client.wait(lambda frame: frame["type"] == "writer_lost", after=second_mark)
        await second_client.wait(
            lambda frame: frame["type"] == "ready" and frame["mode"] == "read",
            after=second_mark,
        )
    finally:
        await close_all(clients, instances)


async def test_accepted_work_survives_disconnect_and_reconnect_cursor_is_exact(
    postgres_dsn: str,
    pg_schema: str,
) -> None:
    provider = BlockingProvider()
    owner_store, owner = await runtime(postgres_dsn, pg_schema, EchoProvider())
    root_id = await owner.create(Bot)
    remote_store, remote = await runtime(postgres_dsn, pg_schema, provider)
    connected = SocketClient(endpoint(remote, root_id))
    reconnected = SocketClient(endpoint(remote, root_id))
    instances = [(remote_store, remote), (owner_store, owner)]
    subscription_id = uuid4()
    try:
        await connected.open()
        ready = await subscribe(connected, root_id, subscription_id, 0, "write")
        cursor = ready["cursor"]
        command_id = uuid4()
        mark = len(connected.frames)
        await connected.send(
            {
                "type": "send",
                "subscription_id": str(subscription_id),
                "command_id": str(command_id),
                "input": "continue",
            }
        )
        receipt = await connected.wait(
            lambda frame: frame["type"] == "receipt" and frame["command_id"] == str(command_id),
            after=mark,
        )
        assert not receipt["duplicate"]
        await asyncio.wait_for(provider.started.wait(), 2)
        await connected.disconnect()
        provider.release.set()
        completed = await wait_for_event(owner_store, root_id, TurnCompleted)
        assert completed.turn_id == command_id.hex

        await reconnected.open()
        replay_mark = len(reconnected.frames)
        replay_ready = await subscribe(reconnected, root_id, uuid4(), cursor, "read")
        replay = [frame for frame in reconnected.frames[replay_mark:] if frame["type"] == "event"]
        assert replay_ready["mode"] == "read"
        assert [frame["seq"] for frame in replay] == list(range(cursor + 1, replay_ready["cursor"] + 1))
        assert len({frame["seq"] for frame in replay}) == len(replay)
        assert any(frame["body"]["type"] == "input_queued" for frame in replay)
        assert any(frame["body"]["type"] == "turn_completed" for frame in replay)
    finally:
        provider.release.set()
        await close_all([connected, reconnected], instances)


async def test_deletion_with_missed_notification_terminates_and_cleans_subscriptions(
    postgres_dsn: str,
    pg_schema: str,
) -> None:
    owner_store, owner = await runtime(postgres_dsn, pg_schema, EchoProvider())
    root_id = await owner.create(Bot)
    remote_store, remote = await runtime(postgres_dsn, pg_schema, EchoProvider())
    client = SocketClient(endpoint(remote, root_id))
    try:
        await client.open()
        await subscribe(client, root_id, uuid4(), 0, "read")
        assert remote.coordinator is not None
        listener = remote.coordinator._listener_task
        assert listener is not None
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        remote.coordinator._listener_task = None

        mark = len(client.frames)
        assert await owner.delete(root_id, allow_active=True)
        closed = await client.wait(lambda frame: frame["type"] == "close", after=mark)
        assert closed["code"] == 4404
        assert client.task is not None and client.task.done()
        assert root_id.hex not in remote._connection_interests
        assert root_id.hex not in remote._stream_interests
        assert root_id.hex not in remote._watchers
        assert root_id.hex not in remote._pages.readers
        assert root_id.hex not in remote.coordinator._actor_interests
        assert root_id.hex not in remote.coordinator._observations
    finally:
        await close_all([client], [(remote_store, remote), (owner_store, owner)])


async def test_approval_policy_uses_durable_descriptor_and_exact_retry_survives_rollback_and_lost_receipt(
    postgres_dsn: str,
    pg_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invoked: list[str] = []

    @tool(permission="ask")
    async def deploy(target: str) -> str:
        invoked.append(target)
        return target

    class Approver(Agent):
        tools = [deploy]

    provider = GatedFakeProvider(
        [
            Sample(text="held"),
            Sample(tool_calls=[ToolCall(id="deploy-call", name="deploy", args='{"target":"prod"}')]),
            Sample(text="done"),
        ]
    )
    owner_store, owner = await runtime(postgres_dsn, pg_schema, provider, [Approver])
    root_id = await owner.create(Approver)
    remote_store, remote = await runtime(postgres_dsn, pg_schema, EchoProvider(), [Approver])
    anchor = owner.connect(root_id, writable=True)
    await anchor.__aenter__()
    await anchor.send("hold", command_id=uuid4())
    await asyncio.wait_for(provider.started.wait(), 2)
    approved: list[SocketAccess] = []

    async def policy(access: SocketAccess) -> bool:
        if access.operation == "answer":
            approved.append(access)
            return access.ask is not None and access.ask.event.request.extra == {
                "permission": "deploy",
                "arguments": {"target": "prod"},
            }
        return True

    client = SocketClient(endpoint(remote, root_id, policy))
    subscription_id = uuid4()
    try:
        await client.open()
        await subscribe(client, root_id, subscription_id, 0, "write")
        turn_id = uuid4()
        send_mark = len(client.frames)
        await client.send(
            {
                "type": "send",
                "subscription_id": str(subscription_id),
                "command_id": str(turn_id),
                "input": "deploy",
            }
        )
        await client.wait(
            lambda frame: frame["type"] == "receipt" and frame["command_id"] == str(turn_id),
            after=send_mark,
        )
        provider.release.set()
        raised = await client.wait(
            lambda frame: frame["type"] == "event" and frame["body"]["type"] == "ask_raised",
            after=send_mark,
        )
        descriptor = raised["body"]["request"]["extra"]
        assert descriptor == {"permission": "deploy", "arguments": {"target": "prod"}}
        ask_id = str(UUID(hex=raised["body"]["ask_id"]))
        answer_id = uuid4()
        answer = {
            "type": "answer",
            "subscription_id": str(subscription_id),
            "command_id": str(answer_id),
            "ask_id": ask_id,
            "response": {"kind": "approval", "allow": True},
        }

        assert owner.coordinator is not None
        original_apply = owner.coordinator._apply_request
        rolled_back = False

        async def fail_before_commit(envelope: Any, ownership: Any, operation: Any) -> Any:
            async def wrapped(locked: Any, store: Any) -> Any:
                nonlocal rolled_back
                reply = await operation(locked, store)
                if envelope.operation == "answer" and not rolled_back:
                    rolled_back = True
                    raise RuntimeError("injected before answer commit")
                return reply

            return await original_apply(envelope, ownership, wrapped)

        monkeypatch.setattr(owner.coordinator, "_apply_request", fail_before_commit)
        rollback_mark = len(client.frames)
        await client.send(answer)
        rolled_back_error = await client.wait(
            lambda frame: frame["type"] == "error" and frame["command_id"] == str(answer_id),
            after=rollback_mark,
        )
        assert rolled_back_error["code"] == "unavailable"
        assert rolled_back_error["uncertain"]
        assert rolled_back
        assert not any(isinstance(event, AskAnswered) for event in await events(owner_store, root_id))
        monkeypatch.setattr(owner.coordinator, "_apply_request", original_apply)

        assert remote.coordinator is not None
        original_request = remote.coordinator.request
        lost = False

        async def lose_committed_reply(envelope: Any) -> Any:
            nonlocal lost
            reply = await original_request(envelope)
            if envelope.operation == "answer" and not lost:
                lost = True
                raise CommandTimeout("injected answer receipt loss")
            return reply

        monkeypatch.setattr(remote.coordinator, "request", lose_committed_reply)
        lost_mark = len(client.frames)
        await client.send(answer)
        lost_error = await client.wait(
            lambda frame: frame["type"] == "error" and frame["command_id"] == str(answer_id),
            after=lost_mark,
        )
        assert lost_error["code"] == "unknown_outcome"
        assert lost_error["retryable"] and lost_error["uncertain"] and lost
        monkeypatch.setattr(remote.coordinator, "request", original_request)

        retry_mark = len(client.frames)
        await client.send(answer)
        receipt = await client.wait(
            lambda frame: frame["type"] == "receipt" and frame["command_id"] == str(answer_id),
            after=retry_mark,
        )
        assert receipt["duplicate"]
        await client.wait(
            lambda frame: frame["type"] == "event" and frame["body"]["type"] == "turn_completed",
            after=lost_mark,
        )
        answered = [event for event in await events(owner_store, root_id) if isinstance(event, AskAnswered)]
        assert len(answered) == 1
        assert answered[0].submitted_by == "user"
        assert invoked == ["prod"]
        assert len(approved) == 3
        assert all(access.ask is not None and access.ask.event.request.extra == descriptor for access in approved)
    finally:
        provider.release.set()
        await anchor.__aexit__(None, None, None)
        await close_all([client], [(remote_store, remote), (owner_store, owner)])
