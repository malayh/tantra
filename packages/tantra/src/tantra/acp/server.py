from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import TypeAdapter

from tantra.acp.transport import StdioTransport, Transport
from tantra.acp.wire import ContentBlock, text_block
from tantra.agent import Agent, agent_name
from tantra.ask import AskResponse
from tantra.events import LoggedEvent, SessionHeader, SubmittedBy
from tantra.runtime import LocatedAsk, Runtime

Operation = Literal["resume", "prompt", "cancel", "claim_writer", "answer", "set_model", "delete"]


@dataclass(frozen=True)
class AcpAuth:
    principal: Any
    submitted_by: str | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        TypeAdapter(SubmittedBy | None).validate_python(self.submitted_by)
        if self.expires_at is not None and (self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None):
            raise ValueError("expires_at must be timezone-aware")


@dataclass(frozen=True)
class AcpAccess:
    operation: Operation
    auth: AcpAuth
    root: SessionHeader
    actor: SessionHeader
    meta: Mapping[str, Any]
    ask: LocatedAsk | None = None
    response: AskResponse | None = None


Scope = Callable[[AcpAuth, Mapping[str, Any]], Awaitable[Mapping[str, str]]]
Policy = Callable[[AcpAccess], Awaitable[bool]]
EncodePrompt = Callable[[AcpAuth, list[ContentBlock], Mapping[str, Any]], Awaitable[str]]
DecodeInput = Callable[[str], list[ContentBlock]]
View = Callable[[LoggedEvent], Awaitable[LoggedEvent | None]]


async def encode_text(auth: AcpAuth, blocks: list[ContentBlock], meta: Mapping[str, Any]) -> str:
    if any(block.get("type") != "text" or not isinstance(block.get("text"), str) for block in blocks):
        raise ValueError("only text content is supported")
    return "\n".join(block["text"] for block in blocks)


def decode_text(input: str) -> list[ContentBlock]:
    return [text_block(input)]


class AcpServer:
    def __init__(
        self,
        runtime: Runtime,
        *,
        agent: type[Agent] | str,
        scope: Scope,
        policy: Policy,
        encode_prompt: EncodePrompt = encode_text,
        decode_input: DecodeInput = decode_text,
        view: View | None = None,
        models: Sequence[str] | None = None,
        name: str,
    ) -> None:
        if not all(callable(value) for value in (scope, policy, encode_prompt, decode_input)):
            raise TypeError("scope, policy, encode_prompt, and decode_input must be callable")
        if view is not None and not callable(view):
            raise TypeError("view must be callable")
        self.runtime = runtime
        self.agent = agent if isinstance(agent, str) else agent_name(agent)
        if self.agent not in runtime.agents:
            raise ValueError(f"agent {self.agent!r} is not registered with the runtime")
        self.scope = scope
        self.policy = policy
        self.encode_prompt = encode_prompt
        self.decode_input = decode_input
        self.view = view
        self.models = list(models) if models is not None else None
        self.name = name
        self._windows: dict[str, int] = {}

    async def connect(self, transport: Transport, auth: AcpAuth) -> None:
        from tantra.acp.agent import AcpConnection

        await AcpConnection(self, transport, auth).run()

    async def serve_stdio(self, auth: AcpAuth) -> None:
        transport = await StdioTransport.open()
        try:
            await self.connect(transport, auth)
        finally:
            await transport.close()

    async def context_window(self, model: str) -> int | None:
        if model in self._windows:
            return self._windows[model]
        try:
            limits = self.runtime.provider.limits(model)
            if inspect.isawaitable(limits):
                limits = await limits
            window = int(limits.context_window)
        except Exception:
            return None
        self._windows[model] = window
        return window
