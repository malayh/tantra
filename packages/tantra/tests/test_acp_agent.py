from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from uuid import UUID, uuid4

import pytest
from acp_support import Client, reduce

from tantra import Agent, Choice, FreeText, Runtime, tool
from tantra.acp import AcpAccess, AcpAuth, AcpServer
from tantra.acp.jsonrpc import RpcError
from tantra.acp.transport import Transport
from tantra.ask import ApprovalResponse
from tantra.errors import WriterReplaced
from tantra.events import (
    AskAnswered,
    CompactionApplied,
    InputQueued,
    LoggedEvent,
    SessionHeader,
    TextPart,
    TurnCompleted,
    TurnStarted,
    Usage,
)
from tantra.providers.base import ToolCall
from tantra.providers.fake import FakeProvider, Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context


class Bot(Agent):
    pass


def call(name: str, args: str = "{}", call_id: str = "c1") -> ToolCall:
    return ToolCall(id=call_id, name=name, args=args)


async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
    return {"user": "u"}


async def allow(access: AcpAccess) -> bool:
    return True


def serve(provider: Any, agents: list[type[Agent]] | None = None, **options: Any) -> tuple[Runtime, AcpServer]:
    runtime_options = {key: options.pop(key) for key in ("compactor",) if key in options}
    runtime = Runtime(provider, MemoryStore(), agents or [Bot], default_model="m", **runtime_options)
    options.setdefault("policy", allow)
    server = AcpServer(runtime, agent=(agents or [Bot])[0], scope=scope, name="bot", **options)
    return runtime, server


async def journal(runtime: Runtime, session_id: str) -> list[Any]:
    return [item.event for item in await runtime.store.read_page(UUID(session_id).hex)]


async def eventually(condition: Any, timeout: float = 5.0) -> None:
    async def poll() -> None:
        while not await condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout)


async def test_initialize_returns_v2_capabilities_and_info() -> None:
    runtime, server = serve(FakeProvider([]))
    async with Client(server) as client:
        result = await client.call("initialize", {"protocolVersion": 7, "info": {"name": "x", "version": "1"}})
    assert result["protocolVersion"] == 2
    assert result["capabilities"] == {
        "session": {"prompt": {}, "mcp": {}, "delete": {}},
        "_meta": {"tantra": {"version": 1}},
    }
    assert result["info"]["name"] == "bot"
    assert isinstance(result["info"]["version"], str)
    assert result["authMethods"] == []
    await runtime.aclose()


async def test_new_list_and_resume_from_start_reduce_to_the_live_capture() -> None:
    @tool
    async def lookup(query: str) -> str:
        return f"found {query}"

    class Researcher(Agent):
        tools = [lookup]

    samples = [
        Sample(reasoning="plan the search", text="Looking.", tool_calls=[call("lookup", '{"query": "acp"}')]),
        Sample(text="It is a protocol."),
    ]
    runtime, server = serve(FakeProvider(samples), [Researcher], models=["m", "n"])
    async with Client(server) as live:
        created = await live.call("session/new", {"cwd": "/work", "mcpServers": []})
        session_id = created["sessionId"]
        assert created["configOptions"][0]["currentValue"] == "m"
        await live.prompt(session_id, "what is acp?")
        await live.idle(session_id)
        listed = await live.call("session/list", {"cwd": "/work"})
        assert [item["sessionId"] for item in listed["sessions"]] == [session_id]
        assert listed["sessions"][0]["cwd"] == "/work"
        assert (await live.call("session/list", {"cwd": "/elsewhere"}))["sessions"] == []
        async with Client(server) as replayed:
            result = await replayed.resume(session_id, replayFrom={"type": "start"})
            await replayed.until(lambda: replayed.of(session_id, "session_info_update"))
        assert result == {"configOptions": created["configOptions"]}
    live_view, replay_view = reduce(live.updates, session_id), reduce(replayed.updates, session_id)
    assert replay_view == live_view
    kinds = [update["sessionUpdate"] for update in replayed.of(session_id)]
    assert "agent_message_chunk" not in kinds
    assert kinds[-2:] == ["session_info_update", "state_update"]
    [tool_entity] = [entity for key, entity in live_view["entities"].items() if key[1] == "tool"]
    assert tool_entity["status"] == "completed"
    assert tool_entity["rawInput"] == {"query": "acp"}
    assert tool_entity["rawOutput"] == "found acp"
    await runtime.aclose()


async def test_resume_without_replay_sends_only_the_current_snapshot() -> None:
    runtime, server = serve(FakeProvider([Sample(text="hello")]))
    async with Client(server) as first:
        session_id = await first.new()
        await first.prompt(session_id, "hi")
        await first.idle(session_id)
        async with Client(server) as second:
            assert await second.resume(session_id) == {}
            await second.until(lambda: second.of(session_id, "state_update"))
            updates = second.of(session_id)
    assert [update["sessionUpdate"] for update in updates] == ["session_info_update", "state_update"]
    assert updates[1] == {"sessionUpdate": "state_update", "state": "idle", "stopReason": "end_turn"}
    await runtime.aclose()


async def test_a_queued_prompt_responds_on_acceptance_and_its_user_message_follows_turn_start(
    gated_provider: Any,
) -> None:
    provider = gated_provider([Sample(text="first answer"), Sample(text="second answer")])
    provider.gate.clear()
    runtime, server = serve(provider)
    async with Client(server) as client:
        session_id = await client.new()
        first = await client.prompt(session_id, "first")
        await client.until(lambda: client.of(session_id, "agent_message_chunk"))
        second = await client.prompt(session_id, "second")
        messages = client.of(session_id, "user_message")
        assert [message["messageId"] for message in messages] == [first["messageId"]]
        provider.gate.set()
        await client.idle(session_id, 2)
        messages = client.of(session_id, "user_message")
    assert [message["messageId"] for message in messages] == [first["messageId"], second["messageId"]]
    assert messages[1]["content"] == [{"type": "text", "text": "second"}]
    assert client.states(session_id) == [
        {"state": "idle"},
        {"state": "running"},
        {"state": "idle", "stopReason": "end_turn"},
        {"state": "running"},
        {"state": "idle", "stopReason": "end_turn"},
    ]
    await runtime.aclose()


async def test_max_steps_and_provider_failure_map_to_stop_reasons() -> None:
    @tool
    async def noop() -> str:
        return "ok"

    class Capped(Agent):
        tools = [noop]
        max_steps = 1

    runtime, server = serve(FakeProvider([Sample(tool_calls=[call("noop")])]), [Capped])
    async with Client(server) as client:
        session_id = await client.new()
        await client.prompt(session_id, "loop")
        await client.idle(session_id)
        await client.prompt(session_id, "again")
        await client.idle(session_id, 2)
        states = client.of(session_id, "state_update")
    assert states[2] == {"sessionUpdate": "state_update", "state": "idle", "stopReason": "max_turn_requests"}
    failed = states[-1]
    assert failed["stopReason"] == "error"
    assert failed["error"]["code"] == -32603
    assert "no scripted sample" in failed["error"]["message"]
    await runtime.aclose()


async def test_cancel_reports_cancelled_and_the_session_is_reusable(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="slow answer"), Sample(text="after")])
    provider.gate.clear()
    runtime, server = serve(provider)
    async with Client(server) as client:
        session_id = await client.new()
        await client.prompt(session_id, "slow")
        await client.until(lambda: client.of(session_id, "agent_message_chunk"))
        await client.notify("session/cancel", {"sessionId": session_id})
        await client.idle(session_id)
        provider.gate.set()
        await client.prompt(session_id, "next")
        await client.idle(session_id, 2)
    assert [state.get("stopReason") for state in client.states(session_id)] == [
        None,
        None,
        "cancelled",
        None,
        "end_turn",
    ]
    await runtime.aclose()


async def test_an_interrupted_turn_reports_tantra_interrupted() -> None:
    runtime, server = serve(FakeProvider([Sample(text="fresh")]))
    async with Client(server) as client:
        session_id = await client.new()
        stale = uuid4().hex
        await runtime.store.enqueue(UUID(session_id).hex, InputQueued(command_id=stale, input="stale"))
        await runtime.store.append(UUID(session_id).hex, [TurnStarted(turn_id=stale, input="stale")])
        await client.prompt(session_id, "fresh")
        await client.idle(session_id, 2)
    assert [state.get("stopReason") for state in client.states(session_id)] == [
        None,
        None,
        "_tantra_interrupted",
        None,
        "end_turn",
    ]
    await runtime.aclose()


async def test_usage_and_compaction_updates() -> None:
    class OnceCompactor:
        def __init__(self) -> None:
            self.done = False

        async def compact(self, ctx: Any) -> list[Any]:
            if self.done:
                return []
            self.done = True
            return [CompactionApplied(strategy="test", tokens_before=100, tokens_after=10, summary="the brief")]

    usage = Usage(input_tokens=100, output_tokens=20, cache_read_tokens=5)
    runtime, server = serve(FakeProvider([Sample(text="ok", usage=usage)]), compactor=OnceCompactor())
    async with Client(server) as client:
        session_id = await client.new()
        await client.prompt(session_id, "go")
        await client.idle(session_id)
    assert client.of(session_id, "usage_update") == [{"sessionUpdate": "usage_update", "used": 125, "size": 1_000_000}]
    compaction = client.of(session_id, "compaction_update")[0]
    assert compaction["status"] == "completed"
    assert client.of(session_id, "compaction_summary_chunk") == [
        {
            "sessionUpdate": "compaction_summary_chunk",
            "compactionId": compaction["compactionId"],
            "content": {"type": "text", "text": "the brief"},
        }
    ]
    await runtime.aclose()


async def test_close_detaches_without_cancelling_work(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="kept running")])
    provider.gate.clear()
    runtime, server = serve(provider)
    async with Client(server) as client:
        session_id = await client.new()
        await client.prompt(session_id, "work")
        await client.until(lambda: client.of(session_id, "agent_message_chunk"))
        assert await client.call("session/close", {"sessionId": session_id}) == {}
        seen = len(client.updates)
        provider.gate.set()

        async def completed() -> bool:
            return any(isinstance(event, TurnCompleted) for event in await journal(runtime, session_id))

        await eventually(completed)
        with pytest.raises(RpcError) as closed:
            await client.call("session/close", {"sessionId": session_id})
        assert len(client.updates) == seen
    assert closed.value.code == -32002
    await runtime.aclose()


async def test_delete_stops_tails_on_every_connection() -> None:
    runtime, server = serve(FakeProvider([]))
    async with Client(server) as first, Client(server) as second:
        session_id = await first.new()
        await second.resume(session_id)
        assert await first.call("session/delete", {"sessionId": session_id}) == {}

        async def stopped() -> bool:
            return not first.connection.sessions and not second.connection.sessions

        await eventually(stopped)
        with pytest.raises(RpcError) as missing:
            await second.resume(session_id)
    assert missing.value.code == -32002
    assert await runtime.store.header(UUID(session_id).hex) is None
    await runtime.aclose()


async def test_two_clients_fence_each_other_and_writer_required_fails_once() -> None:
    runtime, server = serve(FakeProvider([Sample(text="one"), Sample(text="two"), Sample(text="three")]))
    async with Client(server) as first, Client(server) as second:
        session_id = await first.new()
        await first.prompt(session_id, "one")
        await first.idle(session_id)
        await second.resume(session_id)
        await second.prompt(session_id, "two")
        await first.idle(session_id, 2)
        with pytest.raises(RpcError) as fenced:
            await first.prompt(session_id, "three")
        await first.prompt(session_id, "three")
        await first.idle(session_id, 3)
        with pytest.raises(RpcError) as reclaimed:
            await second.prompt(session_id, "four")
    assert fenced.value.code == -32603
    assert fenced.value.data == {"tantra": {"code": "writer_required"}}
    assert reclaimed.value.data == {"tantra": {"code": "writer_required"}}
    inputs = [event.input for event in await journal(runtime, session_id) if isinstance(event, TurnStarted)]
    assert inputs == ["one", "two", "three"]
    await runtime.aclose()


async def test_requests_reach_only_the_writer_and_move_on_takeover() -> None:
    ran: list[str] = []

    @tool(permission="ask")
    async def dangerous() -> str:
        ran.append("yes")
        return "done"

    class Guarded(Agent):
        tools = [dangerous]

    accesses: list[AcpAccess] = []

    async def record(access: AcpAccess) -> bool:
        accesses.append(access)
        return True

    async def approve(method: str, params: dict[str, Any]) -> Any:
        return {"outcome": {"outcome": "selected", "optionId": "allow"}}

    samples = [Sample(tool_calls=[call("dangerous")]), Sample(text="finished"), Sample(text="second done")]
    runtime, server = serve(FakeProvider(samples), [Guarded], policy=record)
    async with (
        Client(server, AcpAuth(principal="one", submitted_by="one")) as first,
        Client(server, AcpAuth(principal="two", submitted_by="two"), responder=approve) as second,
    ):
        session_id = await first.new()
        await first.prompt(session_id, "go")
        await first.until(lambda: first.requests)
        await second.resume(session_id)
        assert second.requests == []
        await second.prompt(session_id, "second")
        await first.until(lambda: first.cancelled)
        await second.idle(session_id, 2)
    method, params = second.requests[0]
    assert method == "session/request_permission"
    assert params["sessionId"] == session_id
    [announced] = [update for update in first.of(session_id, "tool_call_update") if update.get("name") == "dangerous"]
    assert params["subject"] == {"type": "tool_call", "toolCall": {"toolCallId": announced["toolCallId"]}}
    assert [option["optionId"] for option in params["options"]] == ["allow", "deny"]
    assert first.requests[0][1]["_meta"] == params["_meta"]
    assert any(frame.get("method") == "$/cancel_request" for frame in first.transport.received)
    assert ran == ["yes"]
    answered = [event for event in await journal(runtime, session_id) if isinstance(event, AskAnswered)]
    assert [event.submitted_by for event in answered] == ["two"]
    answers = [access for access in accesses if access.operation == "answer"]
    assert [access.response for access in answers if access.auth.principal == "two"] == [
        None,
        ApprovalResponse(allow=True),
    ]
    assert all(access.ask is not None and access.ask.event.request.kind == "approval" for access in answers)
    assert "claim_writer" in [access.operation for access in accesses]
    await runtime.aclose()


async def test_elicitation_is_sent_only_to_clients_with_the_form_capability() -> None:
    @tool
    async def survey(ctx: Context) -> str:
        name = await ctx.ask(FreeText(prompt="Your name?"))
        color = await ctx.ask(Choice(title="Color?", options=["red", "blue"]))
        return f"{name.text} likes {color.selected}"

    class Surveyor(Agent):
        tools = [survey]

    async def answer(method: str, params: dict[str, Any]) -> Any:
        if "text" in params["requestedSchema"]["properties"]:
            return {"action": "accept", "content": {"text": "Malay"}}
        return {"action": "accept", "content": {"choice": "blue"}}

    samples = [
        Sample(tool_calls=[call("survey")]),
        Sample(text="thanks"),
        Sample(tool_calls=[call("survey", call_id="c2")]),
    ]
    runtime, server = serve(FakeProvider(samples), [Surveyor])
    async with Client(server, capabilities={"elicitation": {"form": {}}}, responder=answer) as capable:
        session_id = await capable.new()
        await capable.prompt(session_id, "survey")
        await capable.idle(session_id)
    methods = [method for method, _ in capable.requests]
    assert methods == ["elicitation/create", "elicitation/create"]
    choice = capable.requests[1][1]
    assert choice["mode"] == "form"
    assert choice["requestedSchema"]["properties"]["choice"]["enum"] == ["red", "blue"]
    announced = [update for update in capable.of(session_id, "tool_call_update") if update.get("name") == "survey"]
    assert choice["toolCallId"] == announced[0]["toolCallId"]
    tool_output = [update for update in capable.of(session_id, "tool_call_update") if "rawOutput" in update]
    assert tool_output[0]["rawOutput"] == "Malay likes blue"

    async with Client(server) as plain:
        await plain.resume(session_id)
        await plain.prompt(session_id, "survey again")
        await plain.until(lambda: [s for s in plain.states(session_id) if s["state"] == "requires_action"])
        assert plain.requests == []
        await plain.notify("session/cancel", {"sessionId": session_id})
        await plain.until(lambda: plain.states(session_id)[-1].get("stopReason") == "cancelled")
    requires = next(update for update in plain.of(session_id, "state_update") if update["state"] == "requires_action")
    assert requires["_meta"]["tantra"]["ask"]["state"] == "pending"
    await runtime.aclose()


async def test_policy_denial_and_unknown_sessions_return_resource_not_found() -> None:
    async def deny_resume(access: AcpAccess) -> bool:
        return access.operation != "resume"

    runtime, server = serve(FakeProvider([]), policy=deny_resume)
    async with Client(server) as client:
        session_id = await client.new()
        with pytest.raises(RpcError) as denied:
            await client.resume(session_id)
        with pytest.raises(RpcError) as unknown:
            await client.prompt(str(uuid4()), "hi")
        with pytest.raises(RpcError) as malformed:
            await client.resume("not-a-uuid")
        with pytest.raises(RpcError) as invalid:
            await client.call("session/new", {"cwd": "relative"})
    assert denied.value.code == unknown.value.code == malformed.value.code == -32002
    assert denied.value.message == "Session not found"
    assert invalid.value.code == -32602
    await runtime.aclose()


async def test_prompt_needing_writer_is_denied_by_the_prompt_policy() -> None:
    async def read_only(access: AcpAccess) -> bool:
        return access.operation in ("new", "resume")

    runtime, server = serve(FakeProvider([]), policy=read_only)
    async with Client(server) as client:
        session_id = await client.new()
        with pytest.raises(RpcError) as denied:
            await client.prompt(session_id, "hi")
    assert denied.value.code == -32002
    assert not [event for event in await journal(runtime, session_id) if isinstance(event, InputQueued)]
    await runtime.aclose()


async def test_a_read_only_policy_cannot_create_sessions() -> None:
    accesses: list[AcpAccess] = []

    async def read_only(access: AcpAccess) -> bool:
        accesses.append(access)
        return access.operation == "resume"

    runtime, server = serve(FakeProvider([]), policy=read_only)
    async with Client(server) as plain, Client(server, profile=True) as profiled:
        answers = []
        for client in (plain, profiled):
            with pytest.raises(RpcError) as denied:
                await client.new("/work/project")
            answers.append((denied.value.code, denied.value.message, denied.value.data))
        assert not plain.connection.sessions and not profiled.connection.sessions
    assert answers == [(-32002, "Session not found", None)] * 2
    assert await runtime.store.list() == []
    [first, _] = accesses
    assert first.operation == "new"
    assert first.root is not first.actor and first.root == first.actor
    assert (first.root.parent_id, first.root.agent) == (None, "bot")
    assert first.root.id == first.root.root_id
    assert first.root.metadata == {"user": "u", "acp_cwd": "/work/project"}
    await runtime.aclose()


async def test_non_text_prompt_content_is_rejected_by_the_default_encoder() -> None:
    runtime, server = serve(FakeProvider([]))
    async with Client(server) as client:
        session_id = await client.new()
        with pytest.raises(RpcError) as rejected:
            await client.call(
                "session/prompt",
                {"sessionId": session_id, "prompt": [{"type": "image", "data": "x", "mimeType": "image/png"}]},
            )
    assert rejected.value.code == -32602
    await runtime.aclose()


async def test_set_config_option_patches_the_model() -> None:
    runtime, server = serve(FakeProvider([]), models=["m", "n"])
    async with Client(server) as client:
        session_id = await client.new()
        result = await client.call(
            "session/set_config_option", {"sessionId": session_id, "configId": "model", "type": "id", "value": "n"}
        )
        with pytest.raises(RpcError) as invalid:
            await client.call(
                "session/set_config_option", {"sessionId": session_id, "configId": "model", "type": "id", "value": "x"}
            )
    assert result["configOptions"][0]["currentValue"] == "n"
    assert (await runtime.store.header(UUID(session_id).hex)).model == "n"
    assert invalid.value.code == -32602
    await runtime.aclose()


async def test_title_and_model_reach_other_connections_without_a_coordinator() -> None:
    runtime, server = serve(FakeProvider([]), models=["m", "n"])
    async with Client(server) as setter, Client(server, profile=True) as watcher:
        session_id = await setter.new()
        await watcher.resume(session_id)
        await setter.until(lambda: setter.of(session_id, "state_update"))
        await watcher.until(lambda: watcher.of(session_id, "state_update"))
        await runtime.patch_header(UUID(session_id), title="Renamed")
        for client in (setter, watcher):
            await client.until(
                lambda client=client: [
                    update
                    for update in client.of(session_id, "session_info_update")
                    if update.get("title") == "Renamed"
                ]
            )
        await setter.call(
            "session/set_config_option", {"sessionId": session_id, "configId": "model", "type": "id", "value": "n"}
        )
        [update] = await watcher.until(lambda: watcher.of(session_id, "config_option_update"))
        await runtime.patch_header(UUID(session_id), title="Again")
        await setter.until(
            lambda: [u for u in setter.of(session_id, "session_info_update") if u.get("title") == "Again"]
        )
    assert update["configOptions"][0]["currentValue"] == "n"
    assert setter.of(session_id, "config_option_update") == []
    await runtime.aclose()


async def test_disconnect_leaves_the_turn_running(gated_provider: Any) -> None:
    provider = gated_provider([Sample(text="survives disconnect")])
    provider.gate.clear()
    runtime, server = serve(provider)
    client = Client(server)
    await client.open()
    session_id = await client.new()
    await client.prompt(session_id, "work")
    await client.until(lambda: client.of(session_id, "agent_message_chunk"))
    await client.close()
    provider.gate.set()

    async def completed() -> bool:
        return any(isinstance(event, TurnCompleted) for event in await journal(runtime, session_id))

    await eventually(completed)
    await runtime.aclose()


async def test_too_many_open_sessions_is_rejected() -> None:
    runtime, server = serve(FakeProvider([]))
    async with Client(server) as client:
        for _ in range(8):
            await client.new()
        with pytest.raises(RpcError) as full:
            await client.new()
    assert full.value.code == -32602
    await runtime.aclose()


async def test_a_failing_view_closes_the_connection() -> None:
    async def broken(item: LoggedEvent) -> LoggedEvent:
        if isinstance(item.event, TextPart):
            raise RuntimeError("view failed")
        return item

    runtime, server = serve(FakeProvider([Sample(text="hello")]), view=broken)
    async with Client(server) as client:
        session_id = await client.new()
        with contextlib.suppress(ConnectionError, RpcError):
            await client.prompt(session_id, "hi")
        await client.wait_closed()
    assert not client.connection.sessions
    await runtime.aclose()


async def test_closing_the_runtime_closes_the_connection() -> None:
    runtime, server = serve(FakeProvider([]))
    async with Client(server) as client:
        await client.new()
        await runtime.aclose()
        await client.wait_closed()
    assert not client.connection.sessions


class HeldTransport:
    def __init__(self, inner: Transport) -> None:
        self.inner = inner
        self.held = asyncio.Event()
        self.release = asyncio.Event()

    async def send(self, frame: str) -> None:
        if '"session_info_update"' in frame and not self.release.is_set():
            self.held.set()
            await self.release.wait()
        await self.inner.send(frame)

    async def receive(self) -> str | None:
        return await self.inner.receive()

    async def close(self) -> None:
        await self.inner.close()


async def test_a_cancel_racing_the_resume_response_still_goes_live() -> None:
    runtime, server = serve(FakeProvider([Sample(text="after the race")]))
    async with Client(server) as creator:
        session_id = await creator.new()
    held: list[HeldTransport] = []

    def hold(inner: Transport) -> Transport:
        held.append(HeldTransport(inner))
        return held[0]

    async with Client(server, server_transport=hold) as client:
        assert await client.resume(session_id) == {}
        await asyncio.wait_for(held[0].held.wait(), timeout=2)
        await client.notify("$/cancel_request", {"requestId": client.request_id("session/resume")})
        await asyncio.sleep(0.05)
        held[0].release.set()
        await client.until(lambda: client.of(session_id, "state_update"))
        await client.prompt(session_id, "hi")
        await client.idle(session_id)
    assert client.of(session_id, "agent_message")[-1]["content"] == [{"type": "text", "text": "after the race"}]
    await runtime.aclose()


async def test_parallel_asks_keep_requires_action_until_all_are_answered() -> None:
    @tool
    async def ask_name(ctx: Context) -> str:
        return (await ctx.ask(FreeText(prompt="Name?"))).text

    @tool
    async def ask_color(ctx: Context) -> str:
        return (await ctx.ask(Choice(title="Color?", options=["red", "blue"]))).selected

    class Asker(Agent):
        tools = [ask_name, ask_color]

    both = asyncio.Event()

    async def answer(method: str, params: dict[str, Any]) -> Any:
        if len(client.requests) == 2:
            both.set()
        await both.wait()
        if "text" in params["requestedSchema"]["properties"]:
            return {"action": "accept", "content": {"text": "Ada"}}
        return {"action": "accept", "content": {"choice": "red"}}

    samples = [Sample(tool_calls=[call("ask_name", call_id="a"), call("ask_color", call_id="b")]), Sample(text="ok")]
    runtime, server = serve(FakeProvider(samples), [Asker])
    async with Client(server, capabilities={"elicitation": {"form": {}}}, responder=answer) as client:
        session_id = await client.new()
        await client.prompt(session_id, "ask twice")
        await client.idle(session_id)
    states = [
        (update["state"], update.get("_meta", {}).get("tantra", {}).get("ask", {}).get("state"))
        for update in client.of(session_id, "state_update")
    ]
    assert states[2:] == [
        ("requires_action", "pending"),
        ("requires_action", "pending"),
        ("requires_action", "answered"),
        ("running", "answered"),
        ("idle", None),
    ]
    await runtime.aclose()


async def test_cancel_during_writer_swaps_keeps_a_working_tail_and_releases_the_writer() -> None:
    runtime, server = serve(FakeProvider([Sample(text="one"), Sample(text="two")]))
    async with Client(server) as client:
        session_id = await client.new()
        await client.prompt(session_id, "one")
        await client.idle(session_id)
        session = client.connection.sessions[UUID(session_id)]
        tail = session.tails[UUID(session_id)]
        writer = session.writer
        assert writer is not None
        release, exiting = asyncio.Event(), asyncio.Event()
        exit_writer = writer.__aexit__

        async def slow_exit(*args: Any) -> None:
            exiting.set()
            await release.wait()
            await exit_writer(*args)

        async def replaced(*args: Any, **kwargs: Any) -> Any:
            raise WriterReplaced("replaced in test")

        writer.send = replaced
        writer.__aexit__ = slow_exit
        downgrading = asyncio.create_task(client.prompt(session_id, "lost"))
        await asyncio.wait_for(exiting.wait(), timeout=2)
        await client.notify("$/cancel_request", {"requestId": client.request_id("session/prompt")})
        with pytest.raises(RpcError) as downgraded:
            await downgrading
        assert session.writer is None
        assert tail.task is not None and not tail.task.done()
        release.set()
        await eventually(lambda: _async(not writer._entered))

        connect = runtime.connect
        entering = asyncio.Event()

        def slow_connect(root_id: UUID, **kwargs: Any) -> Any:
            connection = connect(root_id, **kwargs)

            async def slow_enter() -> Any:
                entering.set()
                await asyncio.Future()

            connection.__aenter__ = slow_enter
            return connection

        runtime.connect = slow_connect
        claiming = asyncio.create_task(client.prompt(session_id, "lost again"))
        await asyncio.wait_for(entering.wait(), timeout=2)
        await client.notify("$/cancel_request", {"requestId": client.request_id("session/prompt")})
        with pytest.raises(RpcError) as claimed:
            await claiming
        del runtime.connect
        assert session.writer is None
        assert tail.task is not None and not tail.task.done()
        await client.prompt(session_id, "two")
        await client.idle(session_id, 2)
    assert downgraded.value.code == claimed.value.code == -32800
    assert client.of(session_id, "agent_message")[-1]["content"] == [{"type": "text", "text": "two"}]
    assert not runtime._connections
    await runtime.aclose()


async def _async(value: bool) -> bool:
    return value


async def test_concurrent_opens_never_orphan_a_session_or_exceed_the_cap() -> None:
    calls = 0

    async def slow_first(access: AcpAccess) -> bool:
        nonlocal calls
        if access.operation == "resume":
            calls += 1
            if calls == 1:
                await asyncio.sleep(0.05)
        return True

    runtime, server = serve(FakeProvider([]), policy=slow_first)
    async with Client(server) as creator:
        session_id = await creator.new()
    async with Client(server) as client:
        await asyncio.gather(client.resume(session_id), client.resume(session_id))
        session = client.connection.sessions[UUID(session_id)]

        async def followed() -> bool:
            return session.tree is not None

        await eventually(followed)
        running = {task for task in client.connection.tasks if not task.done()}
        assert running == {session.tails[UUID(session_id)].task, session.tree}
    await runtime.aclose()

    class YieldingStore(MemoryStore):
        async def create(self, header: Any) -> None:
            await asyncio.sleep(0)
            await super().create(header)

    runtime = Runtime(FakeProvider([]), YieldingStore(), [Bot], default_model="m")
    server = AcpServer(runtime, agent=Bot, scope=scope, policy=allow, name="bot")
    async with Client(server) as client:
        results = await asyncio.gather(*(client.new() for _ in range(9)), return_exceptions=True)
        assert len(client.connection.sessions) == 8
    assert [type(result) for result in results if not isinstance(result, str)] == [RpcError]
    await runtime.aclose()


async def test_session_list_pages_roots_with_a_cursor() -> None:
    runtime, server = serve(FakeProvider([]))
    roots = [await runtime.create(Bot, metadata={"user": "u", "acp_cwd": "/work"}) for _ in range(55)]
    await runtime.create(Bot, metadata={"user": "other", "acp_cwd": "/work"})
    for root in roots[:10]:
        child = SessionHeader(
            id=uuid4().hex,
            root_id=root.hex,
            parent_id=root.hex,
            agent="bot",
            metadata={"user": "u", "acp_cwd": "/work"},
        )
        await runtime.store.create(child)
    async with Client(server) as client:
        first = await client.call("session/list", {})
        second = await client.call("session/list", {"cursor": first["nextCursor"]})
        with pytest.raises(RpcError) as invalid:
            await client.call("session/list", {"cursor": "nope"})
    assert len(first["sessions"]) == 50
    assert len(second["sessions"]) == 5
    assert "nextCursor" not in second
    listed = [item["sessionId"] for item in first["sessions"] + second["sessions"]]
    assert listed == [str(root) for root in reversed(roots)]
    assert invalid.value.code == -32602
    await runtime.aclose()


async def test_a_list_cursor_outside_the_callers_scope_is_invalid() -> None:
    class Other(Agent):
        pass

    runtime = Runtime(FakeProvider([]), MemoryStore(), [Bot, Other], default_model="m")
    server = AcpServer(runtime, agent=Bot, scope=scope, policy=allow, name="bot")
    mine = await runtime.create(Bot, metadata={"user": "u", "acp_cwd": "/elsewhere"})
    foreign = await runtime.create(Bot, metadata={"user": "other", "acp_cwd": "/work"})
    alien = await runtime.create(Other, metadata={"user": "u", "acp_cwd": "/work"})
    async with Client(server) as client:
        answers = []
        for cursor in (foreign.hex, alien.hex, uuid4().hex, "nope"):
            with pytest.raises(RpcError) as invalid:
                await client.call("session/list", {"cursor": cursor})
            answers.append((invalid.value.code, invalid.value.message, invalid.value.data))
        listed = await client.call("session/list", {"cwd": "/work", "cursor": mine.hex})
    assert set(answers) == {(-32602, "Invalid cursor", None)}
    assert listed == {"sessions": []}
    await runtime.aclose()


async def test_replay_reports_only_the_latest_usage() -> None:
    samples = [Sample(text="a", usage=Usage(input_tokens=10)), Sample(text="b", usage=Usage(input_tokens=20))]
    runtime, server = serve(FakeProvider(samples))
    async with Client(server) as live:
        session_id = await live.new()
        await live.prompt(session_id, "one")
        await live.idle(session_id)
        await live.prompt(session_id, "two")
        await live.idle(session_id, 2)
        async with Client(server) as replayed:
            await replayed.resume(session_id, replayFrom={"type": "start"})
            await replayed.until(lambda: replayed.of(session_id, "session_info_update"))
    assert [update["used"] for update in live.of(session_id, "usage_update")] == [10, 20]
    assert [update["used"] for update in replayed.of(session_id, "usage_update")] == [20]
    await runtime.aclose()


async def test_tree_watches_are_released_on_every_teardown_path() -> None:
    runtime, server = serve(FakeProvider([]))

    def watchers(session_id: str, count: int) -> Any:
        async def check() -> bool:
            tree = runtime._trees.get(UUID(session_id).hex)
            return (tree.users if tree is not None else 0) == count

        return check

    async def released() -> bool:
        return runtime._trees == {}

    async with Client(server, profile=True) as client:
        session_id = await client.new()
        await eventually(watchers(session_id, 1))
        await client.call("session/close", {"sessionId": session_id})
        await eventually(released)
        for _ in range(5):
            await client.resume(session_id)
        await eventually(watchers(session_id, 1))
        await client.call("session/close", {"sessionId": session_id})
        await eventually(released)
        await client.resume(session_id)
        await eventually(watchers(session_id, 1))
        await client.call("session/delete", {"sessionId": session_id})
        await eventually(released)
        other = await client.new()
        await eventually(watchers(other, 1))
        spawn = client.connection._spawn

        def cancel_follow(coroutine: Any, *, cleanup: bool = False) -> Any:
            task = spawn(coroutine, cleanup=cleanup)
            if getattr(coroutine, "__name__", None) == "_follow":
                task.cancel()
            return task

        client.connection._spawn = cancel_follow
        cancelled = await client.new()

        async def cancelled_early() -> bool:
            session = client.connection.sessions.get(UUID(cancelled))
            return session is not None and session.tree is not None and session.tree.done()

        await eventually(cancelled_early)
        await client.call("session/close", {"sessionId": cancelled})
        await eventually(watchers(cancelled, 0))
    await eventually(released)
    await runtime.aclose()
