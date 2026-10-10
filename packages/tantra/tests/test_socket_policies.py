from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, ConfigDict

from tantra import Agent, Runtime, tool
from tantra.ask import ApprovalResponse
from tantra.context import TurnContext
from tantra.events import InputQueued, LoggedEvent, SessionHeader, TextPart, ToolCallRequested
from tantra.hooks import Hook
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS, FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.websocket import InputCodec, SessionSocket, SocketAccess, SocketAuth, SocketBinding, SocketLimits


class Bot(Agent):
    pass


class Snapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    document: str
    version: int


class GateProvider:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        await self.release.wait()
        yield StreamEnd(text="done")


class SocketHarness:
    def __init__(
        self,
        endpoint: SessionSocket,
        *,
        origins: tuple[str, ...] = ("https://app.test",),
        subprotocols: tuple[str, ...] = ("tantra.session.v1",),
    ) -> None:
        self.endpoint = endpoint
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.scope = {
            "type": "websocket",
            "headers": [(b"origin", origin.encode()) for origin in origins],
            "subprotocols": list(subprotocols),
        }
        self.task: asyncio.Task[None] | None = None

    async def start(self) -> dict[str, Any]:
        await self.incoming.put({"type": "websocket.connect"})
        self.task = asyncio.create_task(self.endpoint(self.scope, self.incoming.get, self.outgoing.put))
        return await self.message()

    async def message(self, timeout: float = 1.0) -> dict[str, Any]:
        return await asyncio.wait_for(self.outgoing.get(), timeout)

    async def frame(self, frame_type: str | None = None, timeout: float = 1.0) -> dict[str, Any]:
        while True:
            message = await self.message(timeout)
            if message["type"] == "websocket.close":
                return message
            payload = json.loads(message["text"])
            if frame_type is None or payload["type"] == frame_type:
                return payload

    async def send(self, payload: Mapping[str, Any]) -> None:
        await self.incoming.put({"type": "websocket.receive", "text": json.dumps(payload)})

    async def subscribe(self, root_id: UUID, *, mode: str = "read", actor_id: UUID | None = None) -> UUID:
        subscription_id = uuid4()
        await self.send(
            {
                "type": "subscribe",
                "subscription_id": str(subscription_id),
                "actor_id": str(actor_id or root_id),
                "after": 0,
                "mode": mode,
            }
        )
        ready = await self.frame("ready")
        assert ready["subscription_id"] == str(subscription_id)
        return subscription_id

    async def disconnect(self) -> None:
        if self.task is None:
            return
        if not self.task.done():
            await self.incoming.put({"type": "websocket.disconnect"})
        await asyncio.wait_for(self.task, 1)


async def allow(_: SocketAccess) -> bool:
    return True


async def event_view(_: Any) -> Callable[[LoggedEvent], Awaitable[Any]]:
    async def view(event: LoggedEvent) -> Any:
        return event.event.model_dump(mode="json")

    return view


def codec() -> InputCodec[dict[str, Any]]:
    return InputCodec(dict[str, Any], lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")))


async def socket_fixture(
    *,
    metadata: Mapping[str, Any] | None = None,
    principal: Any = "user-1",
    anonymous: bool = False,
    revalidate: Callable[[], Awaitable[bool]] | None = None,
    revoked: asyncio.Event | None = None,
    expires_at: datetime | None = None,
    submitted_by: str | None = "user-1",
    policy: Callable[[SocketAccess], Awaitable[bool]] = allow,
    views: Callable[[Any], Awaitable[Callable[[LoggedEvent], Awaitable[Any]]]] = event_view,
    provider: Any | None = None,
    allow_missing_origin: bool = False,
    limits: SocketLimits | None = None,
    input_codec: InputCodec[Any] | None = None,
) -> tuple[MemoryStore, Runtime, UUID, SessionSocket]:
    store = MemoryStore()
    runtime = Runtime(provider or FakeProvider([]), store, [Bot], default_model="m")
    root_id = await runtime.create(Bot, metadata=metadata)

    async def valid() -> bool:
        return True

    binding = SocketBinding(
        root_id,
        SocketAuth(
            principal=principal,
            anonymous=anonymous,
            revalidate=revalidate or valid,
            revoked=revoked,
            expires_at=expires_at,
            submitted_by=submitted_by,
        ),
    )

    async def bind(_: Mapping[str, Any]) -> SocketBinding:
        return binding

    endpoint = SessionSocket(
        runtime,
        bind=bind,
        policy=policy,
        input_codec=input_codec or codec(),
        views=views,
        allowed_origins={"https://app.test"},
        allow_missing_origin=allow_missing_origin,
        limits=limits,
    )
    return store, runtime, root_id, endpoint


async def close(runtime: Runtime, socket: SocketHarness) -> None:
    await socket.disconnect()
    await runtime.aclose()


async def test_private_and_cross_user_access_is_denied_before_accept() -> None:
    async def private(access: SocketAccess) -> bool:
        return access.binding.auth.principal == access.root.metadata["owner"]

    _, runtime, _, endpoint = await socket_fixture(metadata={"owner": "user-1"}, principal="user-2", policy=private)
    socket = SocketHarness(endpoint)
    first = await socket.start()

    assert first == {"type": "websocket.close", "code": 1008, "reason": "access denied"}
    assert socket.task is not None
    await socket.task
    await runtime.aclose()


async def test_anonymous_access_must_be_explicit_and_keeps_browser_origin_requirement() -> None:
    async def valid() -> bool:
        return True

    with pytest.raises(ValueError, match="anonymous access must be explicit"):
        SocketAuth(principal=None, revalidate=valid)

    _, runtime, _, endpoint = await socket_fixture(principal=None, anonymous=True, submitted_by=None)
    socket = SocketHarness(endpoint)
    assert await socket.start() == {"type": "websocket.accept", "subprotocol": "tantra.session.v1"}
    await close(runtime, socket)


@pytest.mark.parametrize(
    ("origins", "subprotocols", "code"),
    [
        (("https://evil.test",), ("tantra.session.v1",), 1008),
        (("https://app.test", "https://app.test"), ("tantra.session.v1",), 1008),
        (("https://app.test",), (), 1002),
        ((), ("tantra.session.v1",), 1008),
    ],
)
async def test_origin_and_subprotocol_checks_run_before_accept(
    origins: tuple[str, ...], subprotocols: tuple[str, ...], code: int
) -> None:
    _, runtime, _, endpoint = await socket_fixture()
    socket = SocketHarness(endpoint, origins=origins, subprotocols=subprotocols)
    first = await socket.start()

    assert first["type"] == "websocket.close"
    assert first["code"] == code
    assert socket.task is not None
    await socket.task
    await runtime.aclose()


@pytest.mark.parametrize(("anonymous", "accepted"), [(False, True), (True, False)])
async def test_missing_origin_is_only_allowed_for_authenticated_service_clients(
    anonymous: bool, accepted: bool
) -> None:
    principal = None if anonymous else "service-1"
    _, runtime, _, endpoint = await socket_fixture(
        principal=principal,
        anonymous=anonymous,
        allow_missing_origin=True,
        submitted_by=None,
    )
    socket = SocketHarness(endpoint, origins=())
    first = await socket.start()

    assert first["type"] == ("websocket.accept" if accepted else "websocket.close")
    if accepted:
        await close(runtime, socket)
    else:
        assert first["code"] == 1008
        assert socket.task is not None
        await socket.task
        await runtime.aclose()


async def test_team_role_allows_reads_denies_mutations_and_expires_periodically() -> None:
    roles = {"user-1": "viewer"}

    async def team(access: SocketAccess) -> bool:
        role = roles.get(access.binding.auth.principal)
        if access.operation == "subscribe":
            return role in {"viewer", "editor"} and access.root.metadata["team"] == "alpha"
        return role == "editor"

    limits = SocketLimits(revalidate_interval=0.01)
    _, runtime, root_id, endpoint = await socket_fixture(metadata={"team": "alpha"}, policy=team, limits=limits)
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = await socket.subscribe(root_id)
    await socket.send({"type": "acquire_writer", "subscription_id": str(subscription_id)})

    denied = await socket.frame("error")
    assert denied["code"] == "forbidden"
    roles.clear()
    closed = await socket.frame()
    assert closed["type"] == "websocket.close"
    assert closed["code"] == 1008
    assert socket.task is not None
    await socket.task
    await runtime.aclose()


async def test_authentication_revalidates_before_each_mutation_and_on_interval() -> None:
    calls = 0
    periodic = asyncio.Event()
    policy_counts: list[int] = []

    async def revalidate() -> bool:
        nonlocal calls
        calls += 1
        if calls >= 5:
            periodic.set()
        return True

    async def policy(access: SocketAccess) -> bool:
        if access.operation == "send":
            policy_counts.append(calls)
        return True

    provider = GateProvider()
    limits = SocketLimits(revalidate_interval=0.01)
    _, runtime, root_id, endpoint = await socket_fixture(
        revalidate=revalidate, policy=policy, provider=provider, limits=limits
    )
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = await socket.subscribe(root_id, mode="write")
    before = calls
    await socket.send(
        {
            "type": "send",
            "subscription_id": str(subscription_id),
            "command_id": str(uuid4()),
            "input": {"message": "hello"},
        }
    )

    assert (await socket.frame("receipt"))["operation"] == "send"
    assert policy_counts == [before + 1]
    await asyncio.wait_for(periodic.wait(), 1)
    provider.release.set()
    await close(runtime, socket)


async def test_expiry_closes_socket_while_revalidation_callback_is_stalled() -> None:
    calls = 0
    stalled = asyncio.Event()

    async def revalidate() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        stalled.set()
        await asyncio.Future()

    expires_at = datetime.now(UTC) + timedelta(seconds=0.08)
    _, runtime, _, endpoint = await socket_fixture(
        revalidate=revalidate,
        expires_at=expires_at,
        limits=SocketLimits(revalidate_interval=0.01),
    )
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    await asyncio.wait_for(stalled.wait(), 1)

    closed = await socket.message()
    assert closed == {"type": "websocket.close", "code": 1008, "reason": "authentication expired"}
    assert socket.task is not None
    await socket.task
    await runtime.aclose()


async def test_revocation_interrupts_replay_before_protected_events_are_delivered() -> None:
    revoked = asyncio.Event()
    replay_started = asyncio.Event()
    release = asyncio.Event()

    async def views(_: Any) -> Callable[[LoggedEvent], Awaitable[Any]]:
        async def view(event: LoggedEvent) -> Any:
            replay_started.set()
            await release.wait()
            return event.event.model_dump(mode="json")

        return view

    store, runtime, root_id, endpoint = await socket_fixture(revoked=revoked, views=views)
    await store.append(root_id.hex, [TextPart(sample_id="sample", text="secret")])
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = uuid4()
    await socket.send(
        {
            "type": "subscribe",
            "subscription_id": str(subscription_id),
            "actor_id": str(root_id),
            "mode": "read",
        }
    )
    await asyncio.wait_for(replay_started.wait(), 1)
    revoked.set()

    closed = await socket.message()
    assert closed == {"type": "websocket.close", "code": 1008, "reason": "authentication revoked"}
    release.set()
    assert socket.task is not None
    await socket.task
    await runtime.aclose()


async def test_protected_delivery_waits_while_revalidation_is_uncertain() -> None:
    calls = 0
    stalled = asyncio.Event()
    release = asyncio.Event()

    async def revalidate() -> bool:
        nonlocal calls
        calls += 1
        if calls <= 3:
            return True
        stalled.set()
        await release.wait()
        return True

    _, runtime, root_id, endpoint = await socket_fixture(
        revalidate=revalidate, limits=SocketLimits(revalidate_interval=0.01)
    )
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = await socket.subscribe(root_id)
    await asyncio.wait_for(stalled.wait(), 1)
    await runtime._append(root_id.hex, [TextPart(sample_id="sample", text="protected")])

    with pytest.raises(TimeoutError):
        await socket.frame(timeout=0.04)
    release.set()
    delivered = await socket.frame("event")
    assert delivered["subscription_id"] == str(subscription_id)
    assert delivered["body"]["text"] == "protected"
    await close(runtime, socket)


async def test_observer_can_read_team_root_and_descendant_but_cannot_write() -> None:
    async def observer(access: SocketAccess) -> bool:
        return access.operation == "subscribe" and access.root.metadata.get("team") == "alpha"

    store, runtime, root_id, endpoint = await socket_fixture(metadata={"team": "alpha"}, policy=observer)
    child_id = uuid4()
    await store.create(
        SessionHeader(
            id=child_id.hex,
            root_id=root_id.hex,
            parent_id=root_id.hex,
            depth=1,
            agent="bot",
            model="m",
        )
    )
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    root_subscription = await socket.subscribe(root_id)
    child_subscription = await socket.subscribe(root_id, actor_id=child_id)
    await socket.send({"type": "acquire_writer", "subscription_id": str(root_subscription)})

    assert (await socket.frame("error"))["code"] == "forbidden"
    await socket.send({"type": "acquire_writer", "subscription_id": str(child_subscription)})
    assert (await socket.frame("error"))["code"] == "forbidden"
    await close(runtime, socket)


async def test_descendant_subscription_rejects_actor_outside_bound_tree() -> None:
    store, runtime, root_id, endpoint = await socket_fixture()
    other_id = uuid4()
    await store.create(SessionHeader(id=other_id.hex, root_id=other_id.hex, agent="bot", model="m"))
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = uuid4()
    await socket.send(
        {
            "type": "subscribe",
            "subscription_id": str(subscription_id),
            "actor_id": str(other_id),
            "mode": "read",
        }
    )

    assert (await socket.frame("error"))["code"] == "forbidden"
    await close(runtime, socket)


async def test_editor_codec_validates_snapshot_and_encodes_frozen_runtime_input() -> None:
    provider = GateProvider()
    policy_inputs: list[Snapshot] = []

    async def editor(access: SocketAccess) -> bool:
        if access.operation == "send":
            policy_inputs.append(access.input)
        return True

    snapshot_codec = InputCodec(Snapshot, lambda value: value.model_dump_json())
    store, runtime, root_id, endpoint = await socket_fixture(
        provider=provider, policy=editor, input_codec=snapshot_codec
    )
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = await socket.subscribe(root_id, mode="write")
    await socket.send(
        {
            "type": "send",
            "subscription_id": str(subscription_id),
            "command_id": str(uuid4()),
            "input": {"document": "hello", "version": "1"},
        }
    )
    assert (await socket.frame("error"))["code"] == "invalid_input"
    command_id = uuid4()
    await socket.send(
        {
            "type": "send",
            "subscription_id": str(subscription_id),
            "command_id": str(command_id),
            "input": {"document": "hello", "version": 1},
        }
    )
    assert (await socket.frame("receipt"))["command_id"] == str(command_id)
    queued = [item.event for item in await store.read_page(root_id.hex) if isinstance(item.event, InputQueued)]

    assert policy_inputs == [Snapshot(document="hello", version=1)]
    assert queued[-1].input == '{"document":"hello","version":1}'
    assert queued[-1].submitted_by == "user-1"
    provider.release.set()
    await close(runtime, socket)


async def test_approval_policy_uses_durable_post_hook_arguments_and_server_attribution() -> None:
    invoked: list[str] = []

    @tool(permission="ask")
    async def deploy(resource: str) -> str:
        invoked.append(resource)
        return resource

    class Transform(Hook):
        async def before_tool(self, event: ToolCallRequested, turn: TurnContext) -> ToolCallRequested | None:
            if event.name == "deploy":
                return event.model_copy(update={"args": {"resource": "team:alpha"}})
            return None

    class Guarded(Agent):
        tools = [deploy]

    approvals: list[SocketAccess] = []

    async def policy(access: SocketAccess) -> bool:
        if access.operation != "answer":
            return True
        approvals.append(access)
        return (
            access.ask is not None
            and access.ask.event.request.extra.get("arguments") == {"resource": "team:alpha"}
            and access.ask.event.request.extra.get("permission") == "deploy"
            and isinstance(access.response, ApprovalResponse)
        )

    store = MemoryStore()
    runtime = Runtime(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="call", name="deploy", args='{"resource":"client:forged"}')]),
                Sample(text="done"),
            ]
        ),
        store,
        [Guarded],
        default_model="m",
        hooks=[Transform()],
    )
    root_id = await runtime.create(Guarded)

    async def valid() -> bool:
        return True

    binding = SocketBinding(
        root_id,
        SocketAuth(principal="reviewer", submitted_by="reviewer-id", revalidate=valid),
    )

    async def bind(_: Mapping[str, Any]) -> SocketBinding:
        return binding

    endpoint = SessionSocket(
        runtime,
        bind=bind,
        policy=policy,
        input_codec=codec(),
        views=event_view,
        allowed_origins={"https://app.test"},
    )
    socket = SocketHarness(endpoint)
    assert (await socket.start())["type"] == "websocket.accept"
    subscription_id = await socket.subscribe(root_id, mode="write")
    await socket.send(
        {
            "type": "send",
            "subscription_id": str(subscription_id),
            "command_id": str(uuid4()),
            "input": {"message": "deploy"},
        }
    )
    event = None
    receipt = None
    while event is None or receipt is None:
        frame = await socket.frame()
        if frame["type"] == "receipt":
            receipt = frame
        elif frame["type"] == "event" and frame["body"]["type"] == "ask_raised":
            event = frame
    assert event is not None
    assert event["body"]["request"]["extra"] == {
        "permission": "deploy",
        "arguments": {"resource": "team:alpha"},
    }
    command_id = uuid4()
    await socket.send(
        {
            "type": "answer",
            "subscription_id": str(subscription_id),
            "command_id": str(command_id),
            "ask_id": str(UUID(hex=event["body"]["ask_id"])),
            "response": {"kind": "approval", "allow": True},
        }
    )

    assert (await socket.frame("receipt"))["operation"] == "answer"
    assert len(approvals) == 1
    assert approvals[0].ask is not None
    assert approvals[0].ask.event.request.title == "Run deploy?"
    answered = [item.event for item in await store.read_page(root_id.hex) if item.event.type == "ask_answered"]
    assert answered[-1].submitted_by == "reviewer-id"
    await close(runtime, socket)
    assert invoked == ["team:alpha"]
