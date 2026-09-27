from __future__ import annotations

import asyncio
import json
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

DIRECTIVE = re.compile(r"\[gate:(provider|tool|ask|child|child-tool|finish|drop)=([A-Za-z0-9_.-]+)\]")
ACTION_ORDER = ("ask", "child-tool", "child", "tool", "finish")


@dataclass
class Gate:
    opened: asyncio.Event = field(default_factory=asyncio.Event)
    waiters: int = 0
    passes: int = 0

    def __post_init__(self) -> None:
        self.opened.set()


@dataclass(frozen=True)
class Plan:
    action: Literal["text", "ask", "tool", "child", "child-tool", "finish"] = "text"
    name: str = "response"
    wait_for: str | None = None
    drop: bool = False


class GateState:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._gates: dict[str, Gate] = {}
        self._requests: deque[dict[str, Any]] = deque(maxlen=200)
        self._next_request = 1
        self._drop_next_send_reply = False
        self._reply_drops: deque[dict[str, str]] = deque(maxlen=50)

    async def reset(self) -> None:
        async with self._lock:
            gates = list(self._gates.values())
            self._gates.clear()
            self._requests.clear()
            self._next_request = 1
            self._drop_next_send_reply = False
            self._reply_drops.clear()
        for gate in gates:
            gate.opened.set()

    async def set_open(self, name: str, opened: bool) -> None:
        async with self._lock:
            gate = self._gates.setdefault(name, Gate())
            if opened:
                gate.opened.set()
            else:
                gate.opened.clear()

    async def wait(self, name: str) -> None:
        async with self._lock:
            gate = self._gates.setdefault(name, Gate())
            gate.waiters += 1
        try:
            await gate.opened.wait()
            async with self._lock:
                gate.passes += 1
        finally:
            async with self._lock:
                gate.waiters -= 1

    async def start_request(self, *, source: str, model: str, plan: Plan) -> int:
        async with self._lock:
            request_id = self._next_request
            self._next_request += 1
            self._requests.append(
                {
                    "id": request_id,
                    "source": source,
                    "model": model,
                    "action": plan.action,
                    "name": plan.name,
                    "wait_for": plan.wait_for,
                    "drop": plan.drop,
                    "status": "running",
                }
            )
            return request_id

    async def finish_request(self, request_id: int, status: str) -> None:
        async with self._lock:
            for item in self._requests:
                if item["id"] == request_id:
                    item["status"] = status
                    break

    async def arm_send_reply_drop(self) -> None:
        async with self._lock:
            self._drop_next_send_reply = True

    async def consume_send_reply_drop(self, evidence: dict[str, str]) -> bool:
        async with self._lock:
            if not self._drop_next_send_reply:
                return False
            self._drop_next_send_reply = False
            self._reply_drops.append(evidence)
            return True

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "gates": {
                    name: {"open": gate.opened.is_set(), "waiters": gate.waiters, "passes": gate.passes}
                    for name, gate in sorted(self._gates.items())
                },
                "requests": list(self._requests),
                "command_reply": {
                    "armed": self._drop_next_send_reply,
                    "drops": list(self._reply_drops),
                },
            }


state = GateState()
app = FastAPI(title="Sarathi deterministic E2E gate")


def _latest_user(messages: list[dict[str, Any]]) -> str:
    return next(
        (str(message.get("content") or "") for message in reversed(messages) if message.get("role") == "user"),
        "",
    )


def _last_tool(messages: list[dict[str, Any]]) -> str | None:
    if not messages or messages[-1].get("role") != "tool":
        return None
    call_id = messages[-1].get("tool_call_id")
    for message in reversed(messages[:-1]):
        for call in message.get("tool_calls") or []:
            if call.get("id") == call_id:
                return str(call.get("function", {}).get("name") or "")
    return None


def _plan(payload: dict[str, Any]) -> Plan:
    system = "\n".join(
        str(message.get("content") or "") for message in payload.get("messages", []) if message.get("role") == "system"
    )
    if "You are a title generator" in system:
        return Plan(name="title")
    messages = payload.get("messages") or []
    user = _latest_user(messages)
    directives = dict(DIRECTIVE.findall(user))
    last_tool = _last_tool(messages)
    if last_tool is not None:
        if last_tool == "e2e_gate" and "finish" in directives:
            return Plan(action="finish", name=directives["finish"])
        return Plan(name=last_tool)
    action = next((candidate for candidate in ACTION_ORDER if candidate in directives), "text")
    name = directives.get(action, directives.get("provider", directives.get("drop", "response")))
    return Plan(
        action=action,
        name=name,
        wait_for=directives.get("provider") or directives.get("drop"),
        drop="drop" in directives,
    )


def _tool(plan: Plan) -> tuple[str, dict[str, Any]] | None:
    if plan.action == "ask":
        return (
            "memory_write",
            {
                "kind": "e2e",
                "title": f"Gate {plan.name}",
                "body": f"Deterministic approval {plan.name}",
                "tags": ["e2e"],
                "entities": [plan.name],
            },
        )
    if plan.action == "tool":
        return "e2e_gate", {"name": plan.name}
    if plan.action in {"child", "child-tool"}:
        directive = "tool" if plan.action == "child-tool" else "provider"
        return (
            "spawn",
            {
                "agent_name": "subagent",
                "input": f"[gate:{directive}={plan.name}] [gate:finish={plan.name}]",
                "name": f"Gate {plan.name}",
            },
        )
    if plan.action == "finish":
        return "finish", {"result": f"Child {plan.name} finished"}
    return None


def _text(plan: Plan, payload: dict[str, Any]) -> str:
    if plan.name == "title":
        user = _latest_user(payload.get("messages") or [])
        source = user.rsplit("\n", 1)[-1]
        clean = DIRECTIVE.sub("", source).strip()
        return (clean or "Deterministic E2E")[:50]
    if plan.name in {"memory_write", "e2e_gate", "spawn"}:
        return f"{plan.name} completed deterministically."
    return f"Deterministic response for {plan.name}."


def _chunk(request_id: int, model: str, delta: dict[str, Any], finish: str | None = None) -> bytes:
    payload = {
        "id": f"chatcmpl-e2e-{request_id}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


def _usage(request_id: int, model: str) -> bytes:
    payload = {
        "id": f"chatcmpl-e2e-{request_id}",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [],
        "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
    }
    return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n".encode()


async def _stream(payload: dict[str, Any], source: str):
    model = str(payload.get("model") or "e2e-model")
    plan = _plan(payload)
    request_id = await state.start_request(source=source, model=model, plan=plan)
    status = "completed"
    try:
        if plan.wait_for is not None:
            await state.wait(plan.wait_for)
        call = _tool(plan)
        if call is not None:
            name, arguments = call
            delta = {
                "role": "assistant",
                "tool_calls": [
                    {
                        "index": 0,
                        "id": f"call_{plan.action}_{plan.name}",
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(arguments, separators=(",", ":"))},
                    }
                ],
            }
            yield _chunk(request_id, model, delta)
            yield _chunk(request_id, model, {}, "tool_calls")
        else:
            words = _text(plan, payload).split(" ")
            for index, word in enumerate(words):
                content = word if index == 0 else f" {word}"
                yield _chunk(request_id, model, {"role": "assistant", "content": content})
                await asyncio.sleep(0.04)
            if plan.drop:
                status = "dropped"
                raise RuntimeError(f"dropped provider stream {plan.name}")
            yield _chunk(request_id, model, {}, "stop")
        yield _usage(request_id, model)
        yield b"data: [DONE]\n\n"
    except asyncio.CancelledError:
        status = "cancelled"
        raise
    except BaseException:
        status = "failed" if status == "completed" else status
        raise
    finally:
        await state.finish_request(request_id, status)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/models")
async def models() -> dict[str, Any]:
    return {"object": "list", "data": [{"id": "e2e-model", "object": "model", "owned_by": "e2e"}]}


@app.post("/v1/chat/completions")
async def completions(request: Request) -> Response:
    payload = await request.json()
    if not payload.get("stream"):
        return JSONResponse({"error": {"message": "the E2E provider requires stream=true"}}, status_code=400)
    source = request.client.host if request.client is not None else "unknown"
    return StreamingResponse(_stream(payload, source), media_type="text/event-stream")


@app.get("/control/state")
async def control_state() -> dict[str, Any]:
    return await state.snapshot()


@app.post("/control/reset")
async def control_reset() -> dict[str, str]:
    await state.reset()
    return {"status": "reset"}


@app.post("/control/gates/{name}/close")
async def close_gate(name: str) -> dict[str, Any]:
    await state.set_open(name, False)
    return {"name": name, "open": False}


@app.post("/control/gates/{name}/open")
async def open_gate(name: str) -> dict[str, Any]:
    await state.set_open(name, True)
    return {"name": name, "open": True}


@app.get("/control/gates/{name}/wait")
async def wait_gate(name: str) -> dict[str, str]:
    await state.wait(name)
    return {"name": name, "status": "released"}


@app.post("/control/command-replies/drop-next-send")
async def drop_next_send_reply() -> dict[str, bool]:
    await state.arm_send_reply_drop()
    return {"armed": True}


@app.post("/control/command-replies/consume")
async def consume_send_reply(request: Request) -> dict[str, bool]:
    payload = await request.json()
    evidence = {
        "request_id": str(payload.get("request_id") or ""),
        "command_id": str(payload.get("command_id") or ""),
        "operation": str(payload.get("operation") or ""),
    }
    return {"drop": await state.consume_send_reply_drop(evidence)}
