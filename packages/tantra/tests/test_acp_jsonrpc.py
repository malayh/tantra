from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest

from tantra.acp.jsonrpc import Peer, Reply, RpcError
from tantra.acp.transport import MemoryTransport, memory_pair


async def receive(transport: MemoryTransport) -> Any:
    frame = await asyncio.wait_for(transport.receive(), timeout=2)
    assert frame is not None
    return json.loads(frame)


async def send(transport: MemoryTransport, message: Any) -> None:
    await transport.send(message if isinstance(message, str) else json.dumps(message))


@asynccontextmanager
async def serving(handlers: dict[str, Callable[[Any], Awaitable[Any]]]) -> AsyncIterator[tuple[Peer, MemoryTransport]]:
    left, right = memory_pair()
    peer = Peer(left, handlers)
    task = asyncio.create_task(peer.run())
    try:
        yield peer, right
    finally:
        await right.close()
        await asyncio.wait_for(task, timeout=2)


async def test_a_long_request_does_not_block_a_notification_or_another_request() -> None:
    release = asyncio.Event()
    seen: list[str] = []

    async def slow(params: Any) -> Any:
        await release.wait()
        return "slow"

    async def fast(params: Any) -> Any:
        return "fast"

    async def note(params: Any) -> None:
        seen.append(params["value"])
        release.set()

    async with serving({"slow": slow, "fast": fast, "note": note}) as (_, wire):
        await send(wire, {"jsonrpc": "2.0", "id": 1, "method": "slow"})
        await send(wire, {"jsonrpc": "2.0", "id": 2, "method": "fast"})
        assert await receive(wire) == {"jsonrpc": "2.0", "id": 2, "result": "fast"}
        await send(wire, {"jsonrpc": "2.0", "method": "note", "params": {"value": "x"}})
        assert await receive(wire) == {"jsonrpc": "2.0", "id": 1, "result": "slow"}
    assert seen == ["x"]


async def test_incoming_cancel_request_cancels_the_handler_and_replies_cancelled() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def wait(params: Any) -> Any:
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with serving({"wait": wait}) as (_, wire):
        await send(wire, {"jsonrpc": "2.0", "id": "a", "method": "wait"})
        await asyncio.wait_for(started.wait(), timeout=2)
        await send(wire, {"jsonrpc": "2.0", "method": "$/cancel_request", "params": {"requestId": "a"}})
        reply = await receive(wire)
        await send(wire, {"jsonrpc": "2.0", "method": "$/cancel_request", "params": {"requestId": "unknown"}})
    assert reply == {"jsonrpc": "2.0", "id": "a", "error": {"code": -32800, "message": "Request cancelled"}}
    assert cancelled.is_set()


async def test_a_handler_that_finishes_despite_cancellation_still_replies_its_result() -> None:
    started = asyncio.Event()

    async def durable(params: Any) -> Any:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            assert current is not None
            current.uncancel()
        return {"messageId": "m"}

    async with serving({"durable": durable}) as (_, wire):
        await send(wire, {"jsonrpc": "2.0", "id": 7, "method": "durable"})
        await asyncio.wait_for(started.wait(), timeout=2)
        await send(wire, {"jsonrpc": "2.0", "method": "$/cancel_request", "params": {"requestId": 7}})
        assert await receive(wire) == {"jsonrpc": "2.0", "id": 7, "result": {"messageId": "m"}}


async def test_cancelling_an_outgoing_request_sends_cancel_request() -> None:
    async with serving({}) as (peer, wire):
        call = asyncio.create_task(peer.send_request("session/request_permission", {"x": 1}))
        request = await receive(wire)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        cancel = await receive(wire)
        await send(wire, {"jsonrpc": "2.0", "id": request["id"], "error": {"code": -32800, "message": "late"}})
    assert request == {
        "jsonrpc": "2.0",
        "id": request["id"],
        "method": "session/request_permission",
        "params": {"x": 1},
    }
    assert cancel == {"jsonrpc": "2.0", "method": "$/cancel_request", "params": {"requestId": request["id"]}}


async def test_outgoing_requests_resolve_results_and_errors() -> None:
    async with serving({}) as (peer, wire):
        first = asyncio.create_task(peer.send_request("one"))
        second = asyncio.create_task(peer.send_request("two", {"a": 1}))
        one, two = await receive(wire), await receive(wire)
        await send(wire, {"jsonrpc": "2.0", "id": two["id"], "error": {"code": -32002, "message": "gone", "data": 3}})
        await send(wire, {"jsonrpc": "2.0", "id": one["id"], "result": {"ok": True}})
        assert await first == {"ok": True}
        with pytest.raises(RpcError) as raised:
            await second
    assert "params" not in one
    assert (raised.value.code, raised.value.message, raised.value.data) == (-32002, "gone", 3)


async def test_pending_outgoing_requests_fail_when_the_peer_closes() -> None:
    left, right = memory_pair()
    peer = Peer(left, {})
    task = asyncio.create_task(peer.run())
    call = asyncio.create_task(peer.send_request("wait"))
    await receive(right)
    await right.close()
    await asyncio.wait_for(task, timeout=2)
    with pytest.raises(ConnectionError):
        await call
    with pytest.raises(ConnectionError):
        await peer.send_request("late")


async def test_batches_reply_with_an_array_and_skip_all_notification_batches() -> None:
    seen: list[Any] = []

    async def echo(params: Any) -> Any:
        return params

    async def note(params: Any) -> None:
        seen.append(params)

    async with serving({"echo": echo, "note": note}) as (_, wire):
        await send(
            wire,
            [
                {"jsonrpc": "2.0", "id": 1, "method": "echo", "params": [1]},
                {"jsonrpc": "2.0", "method": "note", "params": {"n": 1}},
                {"jsonrpc": "2.0", "id": 2, "method": "missing"},
                {"foo": "bar"},
            ],
        )
        replies = await receive(wire)
        await send(wire, [{"jsonrpc": "2.0", "method": "note", "params": {"n": 2}}])
        await send(wire, [])
        empty = await receive(wire)
    assert sorted(replies, key=lambda reply: str(reply["id"])) == [
        {"jsonrpc": "2.0", "id": 1, "result": [1]},
        {"jsonrpc": "2.0", "id": 2, "error": {"code": -32601, "message": "Method not found"}},
        {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid request"}},
    ]
    assert seen == [{"n": 1}, {"n": 2}]
    assert empty == {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid request"}}


@pytest.mark.parametrize(
    ("message", "code", "request_id"),
    [
        ("{not json", -32700, None),
        ('{"jsonrpc": "2.0", "id": 1, "method": "boom", "params": NaN}', -32700, None),
        ({"jsonrpc": "1.0", "id": 1, "method": "ok"}, -32600, 1),
        ({"jsonrpc": "2.0", "id": 1, "method": 5}, -32600, 1),
        ({"jsonrpc": "2.0", "id": 1, "method": "ok", "params": "text"}, -32600, 1),
        ({"jsonrpc": "2.0", "id": {"bad": 1}, "method": "ok"}, -32600, None),
        (42, -32600, None),
        ({"jsonrpc": "2.0", "id": 1, "method": "nope"}, -32601, 1),
        ({"jsonrpc": "2.0", "id": 1, "method": "invalid"}, -32602, 1),
        ({"jsonrpc": "2.0", "id": 1, "method": "boom"}, -32603, 1),
        ({"jsonrpc": "2.0", "id": 1, "method": "missing"}, -32002, 1),
    ],
)
async def test_every_error_code_without_leaking_exception_text(message: Any, code: int, request_id: Any) -> None:
    async def ok(params: Any) -> Any:
        return True

    async def invalid(params: Any) -> Any:
        raise RpcError(-32602)

    async def boom(params: Any) -> Any:
        raise RuntimeError("secret database password in /etc/tantra")

    async def missing(params: Any) -> Any:
        raise RpcError(-32002, "Session not found")

    async with serving({"ok": ok, "invalid": invalid, "boom": boom, "missing": missing}) as (_, wire):
        await send(wire, message)
        reply = await receive(wire)
    assert reply["id"] == request_id
    assert reply["error"]["code"] == code
    assert "secret" not in json.dumps(reply)
    assert set(reply["error"]) == {"code", "message"}


async def test_a_failing_notification_handler_sends_nothing() -> None:
    async def boom(params: Any) -> None:
        raise RuntimeError("secret")

    async def ok(params: Any) -> Any:
        return "ok"

    async with serving({"boom": boom, "ok": ok}) as (_, wire):
        await send(wire, {"jsonrpc": "2.0", "method": "boom"})
        await send(wire, {"jsonrpc": "2.0", "method": "unknown"})
        await send(wire, {"jsonrpc": "2.0", "id": 1, "method": "ok"})
        assert await receive(wire) == {"jsonrpc": "2.0", "id": 1, "result": "ok"}


async def test_reply_follow_up_runs_after_the_response_is_written() -> None:
    async def opened(params: Any) -> Any:
        async def after() -> None:
            await peer.send_notification("session/update", {"after": True})

        return Reply({"sessionId": "s"}, after)

    left, right = memory_pair()
    peer = Peer(left, {"open": opened})
    task = asyncio.create_task(peer.run())
    await send(right, {"jsonrpc": "2.0", "id": 1, "method": "open"})
    assert await receive(right) == {"jsonrpc": "2.0", "id": 1, "result": {"sessionId": "s"}}
    assert await receive(right) == {"jsonrpc": "2.0", "method": "session/update", "params": {"after": True}}
    await right.close()
    await asyncio.wait_for(task, timeout=2)


async def test_writes_are_serialized_in_call_order() -> None:
    async with serving({}) as (peer, wire):
        await asyncio.gather(*(peer.send_notification("n", {"i": index}) for index in range(50)))
        received = [await receive(wire) for _ in range(50)]
    assert [message["params"]["i"] for message in received] == list(range(50))


async def test_cancel_request_targets_one_entry_of_a_batch() -> None:
    started = asyncio.Event()

    async def slow(params: Any) -> Any:
        started.set()
        await asyncio.Future()

    async def fast(params: Any) -> Any:
        await started.wait()
        return "fast"

    async with serving({"slow": slow, "fast": fast}) as (_, wire):
        await send(wire, [{"jsonrpc": "2.0", "id": 1, "method": "slow"}, {"jsonrpc": "2.0", "id": 2, "method": "fast"}])
        await asyncio.wait_for(started.wait(), timeout=2)
        await send(wire, {"jsonrpc": "2.0", "method": "$/cancel_request", "params": {"requestId": 1}})
        replies = await receive(wire)
    assert sorted(replies, key=lambda reply: reply["id"]) == [
        {"jsonrpc": "2.0", "id": 1, "error": {"code": -32800, "message": "Request cancelled"}},
        {"jsonrpc": "2.0", "id": 2, "result": "fast"},
    ]


async def test_a_late_cancel_request_spares_the_response_and_its_follow_up() -> None:
    release = asyncio.Event()

    async def opened(params: Any) -> Any:
        async def after() -> None:
            await release.wait()
            await peer.send_notification("session/update", {"live": True})

        return Reply({"opened": True}, after)

    left, right = memory_pair()
    peer = Peer(left, {"open": opened})
    task = asyncio.create_task(peer.run())
    await send(right, {"jsonrpc": "2.0", "id": 9, "method": "open"})
    assert await receive(right) == {"jsonrpc": "2.0", "id": 9, "result": {"opened": True}}
    await send(right, {"jsonrpc": "2.0", "method": "$/cancel_request", "params": {"requestId": 9}})
    await asyncio.sleep(0.01)
    release.set()
    assert await receive(right) == {"jsonrpc": "2.0", "method": "session/update", "params": {"live": True}}
    await right.close()
    await asyncio.wait_for(task, timeout=2)
