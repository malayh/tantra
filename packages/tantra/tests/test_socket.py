from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import Agent, FakeProvider, MemoryStore, Runtime
from tantra._socket_protocol import NoticeFrame
from tantra.events import LoggedEvent, SessionCreated, SessionHeader, TextDelta
from tantra.websocket import (
    InputCodec,
    SessionSocket,
    SocketAuth,
    SocketBinding,
    SocketLimits,
    SocketViewContext,
    _Outbound,
    _Queue,
)


class Bot(Agent):
    pass


async def allowed(*args: Any) -> bool:
    return True


async def views(context: SocketViewContext) -> Any:
    async def project(event: LoggedEvent) -> Any:
        return event.event.model_dump(mode="json")

    return project


class Socket:
    def __init__(self, endpoint: SessionSocket, *, blocked: bool = False) -> None:
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.messages: list[dict[str, Any]] = []
        self.gate = asyncio.Event()
        if not blocked:
            self.gate.set()
        self.incoming.put_nowait({"type": "websocket.connect"})
        self.task = asyncio.create_task(
            endpoint(
                {
                    "type": "websocket",
                    "headers": [(b"origin", b"https://app.test")],
                    "subprotocols": ["tantra.session.v1"],
                },
                self.incoming.get,
                self.send,
            )
        )

    async def send(self, message: dict[str, Any]) -> None:
        if message["type"] == "websocket.send":
            await self.gate.wait()
            message = json.loads(message["text"])
        self.messages.append(message)
        self.outgoing.put_nowait(message)

    async def write(self, **frame: Any) -> None:
        await self.incoming.put({"type": "websocket.receive", "text": json.dumps(frame)})

    async def until(self, kind: str) -> dict[str, Any]:
        async with asyncio.timeout(3):
            while True:
                message = await self.outgoing.get()
                if message["type"] == kind:
                    return message

    async def close(self) -> None:
        self.gate.set()
        await self.incoming.put({"type": "websocket.disconnect"})
        await asyncio.wait_for(self.task, 3)


async def make_endpoint(runtime: Runtime, root: UUID, **options: Any) -> SessionSocket:
    async def bind(scope: Any) -> SocketBinding:
        return SocketBinding(root, SocketAuth(principal="user", revalidate=allowed))

    return SessionSocket(
        runtime,
        bind=bind,
        policy=options.pop("policy", allowed),
        input_codec=InputCodec(str, lambda value: value),
        views=options.pop("views", views),
        allowed_origins={"https://app.test"},
        **options,
    )


async def test_replay_is_paged_hidden_events_advance_and_view_is_seeded() -> None:
    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    await store.append(root.hex, [TextDelta(text=str(index)) for index in range(3000)])
    seeds = []

    async def filtered(context: SocketViewContext) -> Any:
        seeds.append(context.after)

        async def project(event: LoggedEvent) -> Any:
            return None if event.seq % 2 else {"seq": event.seq}

        return project

    socket = Socket(await make_endpoint(runtime, root, views=filtered))
    subscription = str(uuid4())
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=subscription, actor_id=str(root), after=100)
        ready = await socket.until("ready")
        assert ready["cursor"] == 3001
        assert ready["mode"] == "read"
        assert seeds == [100]
        events = [message for message in socket.messages if message["type"] == "event"]
        assert [event["seq"] for event in events] == list(range(101, 3002))
        assert all(event["body"] is None for event in events if event["seq"] % 2)
        assert all(event["subscription_id"] == subscription for event in events)
    finally:
        await socket.close()
        await runtime.aclose()
    assert not runtime._stream_interests
    assert not runtime._connection_interests
    assert not runtime._connections


async def test_commands_wait_for_delivered_ready_and_reconnect_is_read_only() -> None:
    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    projected = asyncio.Event()
    release = asyncio.Event()

    async def delayed(context: SocketViewContext) -> Any:
        async def project(event: LoggedEvent) -> Any:
            projected.set()
            await release.wait()
            return None

        return project

    endpoint = await make_endpoint(runtime, root, views=delayed)
    socket = Socket(endpoint)
    subscription = str(uuid4())
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=subscription, actor_id=str(root), mode="write")
        await projected.wait()
        await socket.write(type="cancel", subscription_id=subscription, command_id=str(uuid4()))
        assert (await socket.until("error"))["code"] == "writer_required"
        release.set()
        assert (await socket.until("ready"))["mode"] == "write"
        await socket.close()
        socket = Socket(endpoint)
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(root))
        assert (await socket.until("ready"))["mode"] == "read"
        assert not runtime.writers.get(root.hex) or not any(c.writable for c in runtime._connections.values())
    finally:
        release.set()
        await socket.close()
        await runtime.aclose()


async def test_small_queue_backpressures_healthy_replay_and_times_out_stalled_client() -> None:
    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    await store.append(root.hex, [TextDelta(text="payload") for _ in range(40)])
    endpoint = await make_endpoint(
        runtime, root, limits=SocketLimits(queue_frames=2, queue_bytes=1024, queue_timeout=0.1)
    )
    socket = Socket(endpoint)
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(root))
        await socket.until("ready")
        assert len([frame for frame in socket.messages if frame["type"] == "event"]) == 41
        await socket.close()
        socket = Socket(endpoint, blocked=True)
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(root))
        assert (await socket.until("websocket.close"))["code"] == 1013
        await socket.task
    finally:
        await socket.close()
        await runtime.aclose()
    assert not runtime._stream_interests
    assert not runtime._connections


async def test_queue_capacity_includes_inflight_and_payload_bytes() -> None:
    limits = SocketLimits(queue_frames=2, queue_bytes=14, outbound_bytes=14, queue_timeout=0.02)
    queue = _Queue(limits)
    item = _Outbound(NoticeFrame(body=None), "1234567", 7)
    await queue.put(item)
    await queue.put(item)
    first = await queue.get()
    assert queue.frames == 2 and queue.bytes == 14
    with pytest.raises(Exception, match="client is too slow"):
        await queue.put(item)
    await queue.done(first)
    await queue.put(item)
    assert queue.frames == 2 and queue.bytes == 14


async def test_unsubscribe_replacement_drops_late_frames_and_closes_views() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed(context: SocketViewContext) -> Any:
        async def project(event: LoggedEvent) -> Any:
            started.set()
            await release.wait()
            return event.event.model_dump(mode="json")

        return project

    socket = Socket(await make_endpoint(runtime, root, views=delayed))
    first, second = str(uuid4()), str(uuid4())
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=first, actor_id=str(root))
        await started.wait()
        await socket.write(type="unsubscribe", subscription_id=first)
        assert (await socket.until("unsubscribed"))["subscription_id"] == first
        release.set()
        await socket.write(type="subscribe", subscription_id=second, actor_id=str(root))
        await socket.until("ready")
        events = [message for message in socket.messages if message["type"] == "event"]
        assert len(events) == 1
        assert events[0]["subscription_id"] == second
    finally:
        release.set()
        await socket.close()
        await runtime.aclose()


async def test_notices_are_nondurable_and_generator_is_closed() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    closed = asyncio.Event()

    async def notices(binding: SocketBinding) -> Any:
        try:
            yield {"title": "hello"}
            await asyncio.Future()
        finally:
            closed.set()

    socket = Socket(await make_endpoint(runtime, root, notices=notices))
    try:
        await socket.until("websocket.accept")
        notice = await socket.until("notice")
        assert notice == {"type": "notice", "body": {"title": "hello"}}
        assert (await runtime.store.header(root.hex)).last_seq == 1
    finally:
        await socket.close()
        await runtime.aclose()
    assert closed.is_set()


async def test_runtime_shutdown_terminates_stream_and_releases_interest() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    socket = Socket(await make_endpoint(runtime, root))
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(root))
        await socket.until("ready")
        await runtime.aclose()
        assert (await socket.until("websocket.close"))["code"] == 1012
        await socket.task
    finally:
        await socket.close()
        await runtime.aclose()


async def test_projection_restart_reseeds_after_cancelled_mapping_and_closes_old_view() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    started = asyncio.Event()
    seeds: list[int] = []
    closed: list[int] = []

    class View:
        def __init__(self, generation: int) -> None:
            self.generation = generation
            self.count = 0

        async def __call__(self, event: LoggedEvent) -> Any:
            self.count += 1
            if self.generation == 0:
                started.set()
                await asyncio.Future()
            event.event.agent = "modified by projection"
            return {"count": self.count}

        async def aclose(self) -> None:
            closed.append(self.generation)

    async def factory(context: SocketViewContext) -> Any:
        generation = len(seeds)
        seeds.append(context.after)
        return View(generation)

    socket = Socket(await make_endpoint(runtime, root, views=factory))
    subscription = str(uuid4())
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=subscription, actor_id=str(root))
        await started.wait()
        await socket.write(type="acquire_writer", subscription_id=subscription)
        await socket.until("ready")
        events = [message for message in socket.messages if message["type"] == "event"]
        assert events[0]["body"] == {"count": 1}
        assert seeds == [0, 0]
        assert closed == [0]
        original = (await runtime.store.read_page(root.hex))[0].event
        assert isinstance(original, SessionCreated) and original.agent != "modified by projection"
    finally:
        await socket.close()
        await runtime.aclose()
    assert closed == [0, 1]


@pytest.mark.parametrize("payload,code", [(b"binary", 1002), ("{", 1002), ("x" * 257, 1009)])
async def test_invalid_frames_close_and_release_every_subscription(payload: Any, code: int) -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    socket = Socket(await make_endpoint(runtime, root, limits=SocketLimits(inbound_bytes=256)))
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(root), mode="write")
        await socket.until("ready")
        await socket.incoming.put(
            {"type": "websocket.receive", "bytes" if isinstance(payload, bytes) else "text": payload}
        )
        assert (await socket.until("websocket.close"))["code"] == code
        await socket.task
        assert not runtime._connections and not runtime._stream_interests
    finally:
        await socket.close()
        await runtime.aclose()


async def test_outbound_limit_rejects_oversized_projection() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)

    async def factory(context: SocketViewContext) -> Any:
        async def view(event: LoggedEvent) -> str:
            return "x" * 1024

        return view

    socket = Socket(await make_endpoint(runtime, root, views=factory, limits=SocketLimits(outbound_bytes=256)))
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(root))
        assert (await socket.until("websocket.close"))["code"] == 1009
        await socket.task
    finally:
        await socket.close()
        await runtime.aclose()


async def test_periodic_read_check_classifies_deletion_without_subscriptions() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    socket = Socket(await make_endpoint(runtime, root, limits=SocketLimits(revalidate_interval=0.01)))
    try:
        await socket.until("websocket.accept")
        assert await runtime.delete(root)
        assert (await socket.until("websocket.close"))["code"] == 4404
        await socket.task
    finally:
        await socket.close()
        await runtime.aclose()


async def test_periodic_writer_permission_loss_keeps_reading() -> None:
    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    writer_allowed = True

    async def policy(access: Any) -> bool:
        return access.operation == "subscribe" or writer_allowed

    socket = Socket(await make_endpoint(runtime, root, policy=policy, limits=SocketLimits(revalidate_interval=0.01)))
    subscription = str(uuid4())
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=subscription, actor_id=str(root), mode="write")
        assert (await socket.until("ready"))["mode"] == "write"
        writer_allowed = False
        assert (await socket.until("writer_lost"))["subscription_id"] == subscription
        assert (await socket.until("ready"))["mode"] == "read"
        await socket.write(type="cancel", subscription_id=subscription, command_id=str(uuid4()))
        assert (await socket.until("error"))["code"] == "writer_required"
        assert not socket.task.done()
    finally:
        await socket.close()
        await runtime.aclose()


async def test_subscription_limit_and_future_cursor_fail_without_leaking_interest() -> None:
    store = MemoryStore()
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m")
    await runtime.start()
    root = await runtime.create(Bot)
    child = uuid4()
    await store.create(SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, depth=1, agent="bot"))
    socket = Socket(await make_endpoint(runtime, root, limits=SocketLimits(subscriptions=1)))
    subscription = str(uuid4())
    try:
        await socket.until("websocket.accept")
        await socket.write(type="subscribe", subscription_id=subscription, actor_id=str(root), after=2)
        assert (await socket.until("error"))["code"] == "invalid_request"
        assert not runtime._connections
        await socket.write(type="subscribe", subscription_id=subscription, actor_id=str(root))
        await socket.until("ready")
        await socket.write(type="subscribe", subscription_id=str(uuid4()), actor_id=str(child))
        assert (await socket.until("error"))["code"] == "invalid_request"
        assert runtime._stream_interests == {root.hex: {root.hex: 1}}
    finally:
        await socket.close()
        await runtime.aclose()
    assert not runtime._stream_interests


@pytest.mark.parametrize("field,value", [("subscriptions", 0), ("queue_bytes", True), ("queue_timeout", float("nan"))])
def test_invalid_limits(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        SocketLimits(**{field: value})
