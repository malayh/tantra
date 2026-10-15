from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from tantra.acp.agent import AcpConnection
from tantra.acp.jsonrpc import Peer
from tantra.acp.server import AcpAuth, AcpServer
from tantra.acp.transport import Transport, memory_pair
from tantra.providers.base import ModelLimits, SampleRequest
from tantra.providers.fake import FAKE_LIMITS, FakeProvider, Sample

SCHEMA_DIR = Path(__file__).parent / "fixtures" / "acp_v2_alpha8"
ANY = frozenset({"*"})
DIVERGENCES: dict[str, str] = {}
CONTENT_KINDS = ("user_message", "agent_message", "agent_thought", "session_message")
IDENTITY = ("sessionUpdate", "sessionId", "messageId", "toolCallId", "compactionId")
PROFILE = {"_meta": {"tantra": {"version": 1}}}


class SchemaValidator:
    def __init__(self, path: Path = SCHEMA_DIR / "schema.unstable.json") -> None:
        self.defs: dict[str, Any] = json.loads(path.read_text())["$defs"]
        self.requests: dict[str, Any] = {}
        self.responses: dict[str, Any] = {}
        for name, definition in self.defs.items():
            method = definition.get("x-method")
            if method is not None:
                target = self.responses if name.endswith("Response") else self.requests
                target[method] = definition

    def frame_errors(self, frame: Any, methods: dict[Any, str]) -> list[str]:
        if isinstance(frame, list):
            return [problem for item in frame for problem in self.frame_errors(item, methods)]
        if frame.get("jsonrpc") != "2.0":
            return ["$: jsonrpc must be 2.0"]
        if "method" in frame:
            method = frame["method"]
            if method not in self.requests:
                return [] if method.startswith("_") else [f"{method}: unknown method"]
            return self._allowed(self.errors(frame.get("params"), self.requests[method], method))
        if "error" in frame:
            return self._allowed(self.errors(frame["error"], self.defs["Error"], "error"))
        method = methods.get(frame.get("id"))
        if method is None or method not in self.responses:
            return [] if method is not None and method.startswith("_") else [f"response {frame.get('id')}: unknown"]
        return self._allowed(self.errors(frame.get("result"), self.responses[method], f"{method}.result"))

    def errors(self, value: Any, schema: Any, path: str = "$") -> list[str]:
        problems = self._structure(value, schema, path)
        if not problems and isinstance(value, dict):
            known = self._known(value, schema)
            if known is not ANY:
                problems = [f"{path}.{key}: not in schema" for key in value if key not in known and key != "_meta"]
        return problems

    def _allowed(self, problems: list[str]) -> list[str]:
        return [problem for problem in problems if not any(problem.startswith(prefix) for prefix in DIVERGENCES)]

    def _resolve(self, reference: str) -> Any:
        return self.defs[reference.rsplit("/", 1)[-1]]

    def _structure(self, value: Any, schema: Any, path: str) -> list[str]:
        if not isinstance(schema, dict):
            return []
        problems: list[str] = []
        types = schema.get("type")
        if types is not None and not any(_is(value, kind) for kind in ([types] if isinstance(types, str) else types)):
            return [f"{path}: expected {types}"]
        if "const" in schema and value != schema["const"]:
            return [f"{path}: expected {schema['const']!r}"]
        if "enum" in schema and value not in schema["enum"]:
            return [f"{path}: expected one of {schema['enum']!r}"]
        if isinstance(value, dict):
            problems += [f"{path}.{key}: required" for key in schema.get("required", []) if key not in value]
            for key, subschema in schema.get("properties", {}).items():
                if key in value:
                    problems += self.errors(value[key], subschema, f"{path}.{key}")
            extra = schema.get("additionalProperties")
            if isinstance(extra, dict):
                for key, item in value.items():
                    if key not in schema.get("properties", {}):
                        problems += self.errors(item, extra, f"{path}.{key}")
        if isinstance(value, list):
            if len(value) < schema.get("minItems", 0):
                problems.append(f"{path}: too few items")
            if isinstance(schema.get("items"), dict):
                for index, item in enumerate(value):
                    problems += self.errors(item, schema["items"], f"{path}[{index}]")
        if "$ref" in schema:
            problems += self._structure(value, self._resolve(schema["$ref"]), path)
        for subschema in schema.get("allOf", []):
            problems += self._structure(value, subschema, path)
        for key in ("anyOf", "oneOf"):
            if key in schema:
                branches = [self._structure(value, subschema, path) for subschema in schema[key]]
                if all(branches):
                    problems += min(branches, key=len)
        if "not" in schema and not self._structure(value, schema["not"], path):
            problems.append(f"{path}: matches an excluded variant")
        return problems

    def _known(self, value: dict[str, Any], schema: Any) -> frozenset[str]:
        if not isinstance(schema, dict):
            return ANY
        if schema.get("additionalProperties") not in (None, False) or schema.get("unevaluatedProperties") not in (
            None,
            False,
        ):
            return ANY
        parts = [frozenset(schema.get("properties", {}))]
        constrained = "properties" in schema
        if "$ref" in schema:
            constrained = True
            parts.append(self._known(value, self._resolve(schema["$ref"])))
        for subschema in schema.get("allOf", []):
            constrained = True
            parts.append(self._known(value, subschema))
        for key in ("anyOf", "oneOf"):
            for subschema in schema.get(key, []):
                constrained = True
                if not self._structure(value, subschema, "$"):
                    parts.append(self._known(value, subschema))
        if not constrained or any(part is ANY for part in parts):
            return ANY
        return frozenset().union(*parts)


def _is(value: Any, kind: str) -> bool:
    if kind == "object":
        return isinstance(value, dict)
    if kind == "array":
        return isinstance(value, list)
    if kind == "string":
        return isinstance(value, str)
    if kind == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "number":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if kind == "boolean":
        return isinstance(value, bool)
    return kind == "null" and value is None


VALIDATOR = SchemaValidator()


class RecordingTransport:
    def __init__(self, inner: Transport) -> None:
        self.inner = inner
        self.sent: list[Any] = []
        self.received: list[Any] = []

    async def send(self, frame: str) -> None:
        self.sent.append(json.loads(frame))
        await self.inner.send(frame)

    async def receive(self) -> str | None:
        frame = await self.inner.receive()
        if frame is not None:
            self.received.append(json.loads(frame))
        return frame

    async def close(self) -> None:
        await self.inner.close()


Responder = Callable[[str, dict[str, Any]], Awaitable[Any]]


async def _never(method: str, params: dict[str, Any]) -> Any:
    await asyncio.Future()


class Client:
    def __init__(
        self,
        server: AcpServer,
        auth: AcpAuth | None = None,
        *,
        capabilities: dict[str, Any] | None = None,
        responder: Responder = _never,
        server_transport: Callable[[Transport], Transport] | None = None,
        profile: bool = False,
    ) -> None:
        left, right = memory_pair()
        self.transport = RecordingTransport(left)
        served = server_transport(right) if server_transport is not None else right
        self.connection = AcpConnection(server, served, auth or AcpAuth(principal="user", submitted_by="user"))
        self.capabilities = {**(capabilities or {}), **(PROFILE if profile else {})}
        self.responder = responder
        self.updates: list[dict[str, Any]] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.cancelled: list[tuple[str, dict[str, Any]]] = []
        self.peer = Peer(
            self.transport,
            {
                "session/update": self._update,
                "session/request_permission": self._handler("session/request_permission"),
                "elicitation/create": self._handler("elicitation/create"),
            },
        )
        self._changed = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    async def __aenter__(self) -> Client:
        await self.open()
        return self

    async def __aexit__(self, *_args: Any) -> None:
        await self.close()

    async def open(self) -> None:
        self._tasks = [asyncio.create_task(self.connection.run()), asyncio.create_task(self.peer.run())]
        await self.call(
            "initialize",
            {"protocolVersion": 2, "info": {"name": "test", "version": "0"}, "capabilities": self.capabilities},
        )

    async def close(self) -> None:
        await self.transport.close()
        await asyncio.wait_for(asyncio.gather(*self._tasks, return_exceptions=True), timeout=5)
        assert self.schema_errors() == []

    async def wait_closed(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(asyncio.shield(self._tasks[1]), timeout)

    def request_id(self, method: str) -> Any:
        return [frame["id"] for frame in self.transport.sent if frame.get("method") == method and "id" in frame][-1]

    def schema_errors(self) -> list[str]:
        methods = {frame["id"]: frame["method"] for frame in self.transport.sent if "method" in frame and "id" in frame}
        return [problem for frame in self.transport.received for problem in VALIDATOR.frame_errors(frame, methods)]

    async def call(self, method: str, params: Any = None) -> Any:
        return await asyncio.wait_for(self.peer.send_request(method, params), timeout=5)

    async def notify(self, method: str, params: Any = None) -> None:
        await self.peer.send_notification(method, params)

    async def new(self, cwd: str = "/work", **params: Any) -> str:
        result = await self.call("session/new", {"cwd": cwd, "mcpServers": [], **params})
        return result["sessionId"]

    async def prompt(self, session_id: str, text: str, **params: Any) -> Any:
        return await self.call(
            "session/prompt", {"sessionId": session_id, "prompt": [{"type": "text", "text": text}], **params}
        )

    async def resume(self, session_id: str, **params: Any) -> Any:
        return await self.call("session/resume", {"sessionId": session_id, "cwd": "/work", **params})

    async def _update(self, params: dict[str, Any]) -> None:
        self.updates.append(params)
        self._changed.set()

    def _handler(self, method: str) -> Callable[[Any], Awaitable[Any]]:
        async def handle(params: Any) -> Any:
            self.requests.append((method, params))
            self._changed.set()
            try:
                return await self.responder(method, params)
            except asyncio.CancelledError:
                self.cancelled.append((method, params))
                self._changed.set()
                raise

        return handle

    def of(self, session_id: str, kind: str | None = None) -> list[dict[str, Any]]:
        return [
            params["update"]
            for params in self.updates
            if params["sessionId"] == session_id and (kind is None or params["update"]["sessionUpdate"] == kind)
        ]

    def states(self, session_id: str) -> list[dict[str, Any]]:
        return [
            {key: value for key, value in update.items() if key in ("state", "stopReason")}
            for update in self.of(session_id, "state_update")
        ]

    async def until(self, condition: Callable[[], Any], timeout: float = 5.0) -> Any:
        async def poll() -> Any:
            while True:
                self._changed.clear()
                found = condition()
                if found:
                    return found
                await self._changed.wait()

        return await asyncio.wait_for(poll(), timeout)

    async def idle(self, session_id: str, count: int = 1, timeout: float = 5.0) -> list[dict[str, Any]]:
        def settled() -> Any:
            states = self.states(session_id)
            return (
                states if sum(state["state"] == "idle" and "stopReason" in state for state in states) >= count else None
            )

        return await self.until(settled, timeout)


def merge(content: list[Any], blocks: list[Any]) -> list[Any]:
    merged = [dict(block) for block in content]
    for block in blocks:
        if merged and merged[-1].keys() == {"type", "text"} and block.keys() == {"type", "text"}:
            merged[-1] = {"type": "text", "text": merged[-1]["text"] + block["text"]}
        else:
            merged.append(dict(block))
    return merged


def cursor_of(update: dict[str, Any]) -> str | None:
    return update.get("_meta", {}).get("tantra", {}).get("cursor")


def uncursored(update: dict[str, Any]) -> dict[str, Any]:
    if cursor_of(update) is None:
        return update
    tantra = {key: value for key, value in update["_meta"]["tantra"].items() if key != "cursor"}
    meta = {key: value for key, value in update["_meta"].items() if key != "tantra"}
    if tantra:
        meta["tantra"] = tantra
    stripped = {key: value for key, value in update.items() if key != "_meta"}
    if meta:
        stripped["_meta"] = meta
    return stripped


def reduce(updates: list[dict[str, Any]], session_id: str | None = None) -> dict[str, Any]:
    entities: dict[tuple[str, str, str], dict[str, Any]] = {}
    states: dict[str, Any] = {}
    queues: dict[str, Any] = {}
    for params in updates:
        sid = params["sessionId"]
        if session_id is not None and sid != session_id:
            continue
        update = uncursored(params["update"])
        kind = update["sessionUpdate"]
        if kind in CONTENT_KINDS:
            entity = entities.setdefault((sid, kind, update["messageId"]), {"content": []})
            entity.update({key: value for key, value in update.items() if key not in IDENTITY + ("content",)})
            if "content" in update:
                entity["content"] = merge([], update["content"] or [])
        elif kind in ("agent_message_chunk", "agent_thought_chunk"):
            entity = entities.setdefault((sid, kind.removesuffix("_chunk"), update["messageId"]), {"content": []})
            entity["content"] = merge(entity["content"], [update["content"]])
        elif kind == "tool_call_update":
            entity = entities.setdefault((sid, "tool", update["toolCallId"]), {"content": []})
            entity.update({key: value for key, value in update.items() if key not in IDENTITY + ("content",)})
            if "content" in update:
                entity["content"] = list(update["content"] or [])
        elif kind == "tool_call_content_chunk":
            entity = entities.setdefault((sid, "tool", update["toolCallId"]), {"content": []})
            entity["content"] = [*entity["content"], update["content"]]
        elif kind == "compaction_update":
            entity = entities.setdefault((sid, "compaction", update["compactionId"]), {"summary": []})
            entity.update({key: value for key, value in update.items() if key not in IDENTITY + ("summary",)})
            if "summary" in update:
                entity["summary"] = merge([], update["summary"] or [])
        elif kind == "compaction_summary_chunk":
            entity = entities.setdefault((sid, "compaction", update["compactionId"]), {"summary": []})
            entity["summary"] = merge(entity["summary"], [update["content"]])
        elif kind == "subagent_update":
            entity = entities.setdefault((sid, "subagent", update["sessionId"]), {})
            entity.update({key: value for key, value in update.items() if key not in IDENTITY})
        elif kind == "state_update":
            states[sid] = {key: value for key, value in update.items() if key != "sessionUpdate"}
        elif kind == "_tantra_queue_update":
            queues[sid] = update["entries"]
    return {"order": list(entities), "entities": entities, "states": states, "queues": queues}


class Scripted:
    def __init__(self, **scripts: list[Sample]) -> None:
        self.providers = {model: FakeProvider(samples) for model, samples in scripts.items()}

    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    def stream(self, req: SampleRequest) -> Any:
        return self.providers[req.model].stream(req)

    def add(self, model: str, *samples: Sample) -> None:
        self.providers[model].samples.extend(samples)
