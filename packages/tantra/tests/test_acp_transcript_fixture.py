from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

from acp_support import Client, Scripted, cursor_of

from tantra import Agent, Choice, FreeText, Runtime, tool
from tantra.acp import AcpAccess, AcpAuth, AcpServer
from tantra.providers.base import ProviderEvent, SampleRequest, TextDelta, ToolCall
from tantra.providers.fake import Sample
from tantra.stores.memory import MemoryStore
from tantra.tools import Context

FIXTURE = Path(__file__).parents[2] / "tantra-acp-client" / "test" / "fixtures" / "session.json"
UUID_TEXT = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
UPDATED_AT = "2026-01-01T00:00:00+00:00"
RUNNING = {"state": "running"}
END_TURN = {"state": "idle", "stopReason": "end_turn"}


@tool(permission="ask")
async def deploy() -> str:
    return "deployed"


@tool
async def survey(ctx: Context) -> str:
    await ctx.emit("Collecting answers")
    name = await ctx.ask(FreeText(prompt="Name?"))
    color = await ctx.ask(Choice(title="Color?", options=["red", "blue"]))
    return f"{name.text}:{color.selected}"


class Scout(Agent):
    model = "scout"


class Lead(Agent):
    model = "lead"
    tools = [deploy, survey]
    subagents = [Scout]


class Choreographed(Scripted):
    def __init__(self, **scripts: list[Sample]) -> None:
        super().__init__(**scripts)
        self.gates: dict[tuple[str, int], asyncio.Event] = {}
        self.stalls: set[tuple[str, int]] = set()
        self.calls: dict[str, int] = {}

    def gate(self, model: str, index: int) -> asyncio.Event:
        return self.gates.setdefault((model, index), asyncio.Event())

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        key = (req.model, self.calls.get(req.model, 0))
        self.calls[req.model] = key[1] + 1
        if key in self.gates:
            await self.gates[key].wait()
        async for event in self.providers[req.model].stream(req):
            yield event
            if key in self.stalls and isinstance(event, TextDelta):
                await asyncio.Future()


def answer(method: str, params: dict[str, Any]) -> dict[str, Any]:
    if method == "session/request_permission":
        return {"outcome": {"outcome": "selected", "optionId": "allow"}}
    if "choice" in params["requestedSchema"]["properties"]:
        return {"action": "accept", "content": {"choice": "blue"}}
    return {"action": "accept", "content": {"text": "Ada"}}


async def respond(method: str, params: dict[str, Any]) -> Any:
    return answer(method, params)


async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
    return {"user": "u"}


async def allow(access: AcpAccess) -> bool:
    return True


def calls(call_id: str, function: str, **args: Any) -> list[ToolCall]:
    return [ToolCall(id=call_id, name=function, args=json.dumps(args))]


def mirrored(update: dict[str, Any]) -> bool:
    return update["sessionUpdate"] == "subagent_update" and cursor_of(update) is None


def states(client: Client, session_id: str) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in update.items() if key in ("state", "stopReason")}
        for update in client.of(session_id, "state_update")
        if cursor_of(update) is not None
    ]


async def checkpoint(
    client: Client,
    root: str,
    record: list[dict[str, Any]],
    turn: Callable[[list[dict[str, Any]]], bool],
    child: dict[str, Any],
) -> None:
    def reached() -> bool:
        shown = [params["update"] for params in client.updates if mirrored(params["update"])]
        return turn(states(client, root)) and bool(shown) and shown[-1]["state"] == child

    await client.until(reached)
    live = [params for params in client.updates if not mirrored(params["update"])]
    latest = [params for params in client.updates if mirrored(params["update"])][-1]
    record.append({"at": len(live), **latest})


def idle(count: int) -> Callable[[list[dict[str, Any]]], bool]:
    def check(seen: list[dict[str, Any]]) -> bool:
        return sum(state["state"] == "idle" for state in seen) == count and seen[-1]["state"] == "idle"

    return check


def running_after(count: int) -> Callable[[list[dict[str, Any]]], bool]:
    def check(seen: list[dict[str, Any]]) -> bool:
        return sum(state["state"] == "idle" for state in seen) == count and seen[-1] == RUNNING

    return check


async def replay(server: AcpServer, root: str, replay_from: dict[str, Any]) -> list[dict[str, Any]]:
    async with Client(server, profile=True) as client:
        await client.resume(root, replayFrom=replay_from)
        await client.until(lambda: [u for u in client.of(root, "state_update") if cursor_of(u) is None])
    return [params for params in client.updates if cursor_of(params["update"]) is not None]


def normalize(value: Any, ids: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: UPDATED_AT if key == "updatedAt" else normalize(item, ids) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize(item, ids) for item in value]
    if isinstance(value, str):
        return UUID_TEXT.sub(
            lambda found: ids.setdefault(found[0], f"00000000-0000-4000-8000-{len(ids) + 1:012d}"), value
        )
    return value


async def test_the_transcript_fixture_matches_a_scripted_session() -> None:
    provider = Choreographed(
        lead=[
            Sample(reasoning="Plan the checks first.", text="Checking the system.", tool_calls=calls("c1", "deploy")),
            Sample(tool_calls=calls("c2", "survey")),
            Sample(tool_calls=calls("c3", "spawn", agent_name="scout", input="Look around.", name="scout")),
            Sample(text="Spawned the scout."),
        ],
        scout=[Sample(text="First pass done."), Sample(text="The answer is 42.")],
    )
    first, second, wake = provider.gate("scout", 0), provider.gate("scout", 1), provider.gate("lead", 4)
    provider.stalls.add(("lead", 7))
    runtime = Runtime(provider, MemoryStore(), [Lead])
    server = AcpServer(runtime, agent=Lead, scope=scope, policy=allow, name="transcript")
    mirror: list[dict[str, Any]] = []
    elicitation = {"elicitation": {"form": {}}}
    async with Client(server, capabilities=elicitation, responder=respond, profile=True) as live:
        root = await live.new()
        await live.until(lambda: live.of(root, "_tantra_writer"))
        await live.prompt(root, "Run the checks.")
        await checkpoint(live, root, mirror, idle(1), RUNNING)
        [spawned] = [update for update in live.of(root, "subagent_update") if cursor_of(update) is not None]
        provider.add(
            "lead",
            Sample(tool_calls=calls("c4", "send", agent_id=spawned["sessionId"], input="Look again.")),
            Sample(text="Sent it."),
            Sample(text="The scout replied."),
            Sample(text="Working on it."),
        )
        first.set()
        await checkpoint(live, root, mirror, running_after(1), END_TURN)
        wake.set()
        await checkpoint(live, root, mirror, idle(2), RUNNING)
        second.set()
        await checkpoint(live, root, mirror, idle(3), END_TURN)
        await live.prompt(root, "Start a long task.")
        await live.until(
            lambda: [u for u in live.of(root, "agent_message_chunk") if u["content"]["text"] == "Working "]
        )
        await live.notify("session/cancel", {"sessionId": root})
        await live.until(lambda: idle(4)(states(live, root)))
    stream = [params for params in live.updates if not mirrored(params["update"])]
    cut = cursor_of(spawned)
    start = await replay(server, root, {"type": "start"})
    after = await replay(server, root, {"type": "_tantra_after", "cursor": cut})
    await runtime.aclose()
    cursored = [params for params in stream if cursor_of(params["update"]) is not None]
    assert start == cursored
    assert after == cursored[[cursor_of(params["update"]) for params in cursored].index(cut) + 1 :]
    assert states(live, root)[-1] == {"state": "idle", "stopReason": "cancelled"}
    assert [method for method, _ in live.requests] == ["session/request_permission", *["elicitation/create"] * 2]
    recorded = {
        "live": stream,
        "mirror": mirror,
        "start": start,
        "after": {"cursor": cut, "updates": after},
        "requests": [
            {"method": method, "params": params, "result": answer(method, params)} for method, params in live.requests
        ],
    }
    expected = normalize(recorded, {})
    if os.environ.get("TANTRA_UPDATE_FIXTURES") == "1":
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.write_text(json.dumps(expected, indent=2) + "\n")
    assert json.loads(FIXTURE.read_text()) == expected
