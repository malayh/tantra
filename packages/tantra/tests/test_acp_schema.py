from __future__ import annotations

import json
from typing import Any

from acp_support import DIVERGENCES, SCHEMA_DIR, VALIDATOR, Client, Scripted

from tantra import Agent, Choice, FreeText, Runtime, tool
from tantra.acp import AcpAccess, AcpAuth, AcpServer
from tantra.events import CompactionApplied
from tantra.providers.base import ToolCall
from tantra.providers.fake import Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context

EMITTED_UPDATES = {
    "user_message",
    "agent_message",
    "agent_message_chunk",
    "agent_thought",
    "agent_thought_chunk",
    "state_update",
    "tool_call_update",
    "tool_call_content_chunk",
    "subagent_update",
    "session_message",
    "usage_update",
    "compaction_update",
    "compaction_summary_chunk",
    "session_info_update",
}
RESPONSES = {
    "initialize",
    "session/new",
    "session/list",
    "session/resume",
    "session/prompt",
    "session/set_config_option",
    "session/close",
    "session/delete",
}


def update(body: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "s", "update": body}}


def test_the_vendored_schema_is_the_alpha8_reference() -> None:
    notice = (SCHEMA_DIR / "NOTICE").read_text()
    assert "schema-v2.0.0-alpha.8" in notice
    assert "Apache License, Version 2.0" in notice
    stable = json.loads((SCHEMA_DIR / "schema.json").read_text())["$defs"]
    unstable = json.loads((SCHEMA_DIR / "schema.unstable.json").read_text())["$defs"]
    assert {"SubagentUpdate", "SessionMessage"} <= unstable.keys() - stable.keys()
    assert "ErrorStopReason" in stable


def test_the_validator_accepts_conforming_messages_and_extensions() -> None:
    accepted = [
        update({"sessionUpdate": "agent_message", "messageId": "m", "content": [{"type": "text", "text": "x"}]}),
        update({"sessionUpdate": "state_update", "state": "idle", "stopReason": "_tantra_finished"}),
        update(
            {
                "sessionUpdate": "state_update",
                "state": "idle",
                "stopReason": "error",
                "error": {"code": 1, "message": "m"},
            }
        ),
        update({"sessionUpdate": "_tantra_writer", "writer": True, "reason": "claimed"}),
        update({"sessionUpdate": "tool_call_update", "toolCallId": "c", "_meta": {"tantra": {"cursor": "1.0"}}}),
        {"jsonrpc": "2.0", "method": "_tantra/session/claim_writer", "params": {"anything": 1}},
    ]
    assert [VALIDATOR.frame_errors(frame, {}) for frame in accepted] == [[]] * len(accepted)


def test_the_validator_rejects_structural_violations() -> None:
    rejected = {
        "missing": update({"sessionUpdate": "agent_message"}),
        "unknown": update({"sessionUpdate": "agent_message", "messageId": "m", "bogus": 1}),
        "nested": update({"sessionUpdate": "agent_message", "messageId": "m", "content": [{"type": "text"}]}),
        "type": update({"sessionUpdate": "usage_update", "used": "many", "size": 1}),
        "envelope": {"jsonrpc": "2.0", "method": "session/update", "params": {"update": {}}},
        "response": {"jsonrpc": "2.0", "id": 1, "result": {"sessionId": 3}},
    }
    methods = {1: "session/new"}
    assert {name: bool(VALIDATOR.frame_errors(frame, methods)) for name, frame in rejected.items()} == {
        name: True for name in rejected
    }


def test_divergences_are_documented_with_reasons() -> None:
    assert all(isinstance(reason, str) and reason for reason in DIVERGENCES.values())


async def test_every_emitted_message_is_valid_against_the_alpha8_schema() -> None:
    @tool(permission="ask")
    async def dangerous() -> str:
        return "ran"

    @tool
    async def survey(ctx: Context) -> str:
        await ctx.emit("asking")
        name = await ctx.ask(FreeText(prompt="Name?"))
        color = await ctx.ask(Choice(title="Color?", options=["red", "blue"]))
        return f"{name.text}:{color.selected}"

    class Worker(Agent):
        model = "worker"

    class Root(Agent):
        model = "root"
        tools = [dangerous, survey]
        subagents = [Worker]

    class OnceCompactor:
        def __init__(self) -> None:
            self.done = False

        async def compact(self, ctx: Any) -> list[Any]:
            if self.done:
                return []
            self.done = True
            return [CompactionApplied(strategy="t", tokens_before=9, tokens_after=1, summary="brief")]

    async def respond(method: str, params: dict[str, Any]) -> Any:
        if method == "session/request_permission":
            return {"outcome": {"outcome": "selected", "optionId": "allow"}}
        if "text" in params["requestedSchema"]["properties"]:
            return {"action": "accept", "content": {"text": "Ada"}}
        return {"action": "accept", "content": {"choice": "red"}}

    async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
        return {"user": "u"}

    async def allow(access: AcpAccess) -> bool:
        return True

    def calls(call_id: str, name: str, **args: Any) -> list[ToolCall]:
        return [ToolCall(id=call_id, name=name, args=json.dumps(args))]

    provider = Scripted(
        root=[
            Sample(reasoning="plan", text="Starting.", tool_calls=calls("c1", "dangerous")),
            Sample(tool_calls=calls("c2", "survey")),
            Sample(tool_calls=calls("s1", "spawn", agent_name="worker", input="dig")),
            Sample(text="Spawned."),
            Sample(text="Got message."),
            Sample(text="Got result."),
        ],
        worker=[],
    )
    runtime = Runtime(provider, MemoryStore(), [Root], compactor=OnceCompactor())
    server = AcpServer(runtime, agent=Root, scope=scope, policy=allow, models=["root", "other"], name="schema")
    capabilities = {"elicitation": {"form": {}}}
    async with Client(server, capabilities=capabilities, responder=respond) as client:
        session_id = await client.new()
        provider.add(
            "worker",
            Sample(tool_calls=calls("w1", "send", agent_id=session_id, input="hi")),
            Sample(tool_calls=calls("w2", "finish", result={"ok": True})),
        )
        await client.prompt(session_id, "go")
        await client.idle(session_id, 3)
        await client.prompt(session_id, "fail please")
        await client.idle(session_id, 4)
        await client.call("session/list", {})
        await client.call(
            "session/set_config_option", {"sessionId": session_id, "configId": "model", "type": "id", "value": "other"}
        )
        async with Client(server) as replay:
            await replay.resume(session_id, replayFrom={"type": "start"})
            await replay.until(lambda: replay.of(session_id, "session_info_update"))
            await replay.call("session/close", {"sessionId": session_id})
            assert replay.schema_errors() == []
        await client.call("session/delete", {"sessionId": session_id})
        assert client.schema_errors() == []
    kinds = {params["update"]["sessionUpdate"] for params in client.updates}
    assert EMITTED_UPDATES <= kinds
    requests = {method for method, _ in client.requests}
    assert requests == {"session/request_permission", "elicitation/create"}
    sent = [*client.transport.sent, *replay.transport.sent]
    assert RESPONSES <= {frame["method"] for frame in sent if "method" in frame and "id" in frame}
    assert client.states(session_id)[-1]["stopReason"] == "error"
    await runtime.aclose()
