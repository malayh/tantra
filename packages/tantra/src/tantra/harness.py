from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import aclosing
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from tantra.agent import Agent, agent_name, build_name_table
from tantra.context import TurnContext, resolve_model
from tantra.errors import SessionBusy, SessionNotFound, TantraError, TurnIncomplete
from tantra.events import SessionCreated, SessionEvent, SessionHeader, TurnCompleted, TurnFailed, TurnStarted
from tantra.loop import DEFAULT_RETRY, Emitted, RetryConfig, TurnLoop
from tantra.providers.base import Provider
from tantra.stores.base import Store
from tantra.tools import Tool

TYPED_KEYS = frozenset({"type", "anyOf", "allOf", "oneOf", "$ref", "enum", "const"})


def _check_schema(label: str, entry: Tool) -> None:
    parameters = entry.schema.parameters
    if not isinstance(parameters, dict) or parameters.get("type") != "object":
        raise TantraError(f"{label}: tool {entry.name!r} produced an invalid JSON schema: {parameters!r}")
    properties = parameters.get("properties") or {}
    if "ctx" in properties:
        raise TantraError(
            f"{label}: tool {entry.name!r} has an unannotated 'ctx' parameter; annotate it `ctx: Context`"
        )
    for name in parameters.get("required") or []:
        spec = properties.get(name)
        if not isinstance(spec, dict) or not TYPED_KEYS & set(spec):
            raise TantraError(f"{label}: tool {entry.name!r} parameter {name!r} has no inferable JSON type: {spec!r}")


def _tool_table(agent: type[Agent]) -> dict[str, Tool]:
    label = f"agent {agent_name(agent)!r}"
    table: dict[str, Tool] = {}
    if agent.max_steps < 1:
        raise TantraError(f"{label}: max_steps must be at least 1, got {agent.max_steps}")
    for entry in agent.tools:
        if not isinstance(entry, Tool):
            raise TantraError(f"{label}: {entry!r} is not decorated with @tool")
        _check_schema(label, entry)
        if entry.name in table:
            raise TantraError(f"{label}: duplicate tool name {entry.name!r}")
        table[entry.name] = entry
    return table


def _turn_incomplete(events: Sequence[SessionEvent]) -> bool:
    for event in reversed(events):
        if isinstance(event, TurnCompleted | TurnFailed):
            return False
        if isinstance(event, TurnStarted):
            return True
    return False


async def _resolve(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class Harness:
    """The runtime: provider, store, deps and the name→agent table. Many agents, one harness."""

    def __init__(
        self,
        provider: Provider,
        store: Store,
        agents: Iterable[type[Agent]],
        *,
        default_model: str | None = None,
        deps_factory: Callable[[SessionHeader], Any] | None = None,
        retry: RetryConfig = DEFAULT_RETRY,
        lease_ttl: float = 60.0,
    ) -> None:
        self.provider = provider
        self.store = store
        self.default_model = default_model
        self.deps_factory = deps_factory
        self.retry = retry
        self.lease_ttl = lease_ttl
        self.agents = build_name_table(agents)
        self.tools = {name: _tool_table(agent) for name, agent in self.agents.items()}

    def agent_for(self, name: str) -> type[Agent]:
        agent = self.agents.get(name)
        if agent is None:
            raise TantraError(f"unknown agent {name!r}; known: {sorted(self.agents)}")
        return agent

    def _name_of(self, agent: type[Agent] | str) -> str:
        name = agent if isinstance(agent, str) else agent_name(agent)
        self.agent_for(name)
        return name

    async def create_session(self, agent: type[Agent] | str, metadata: dict[str, Any] | None = None) -> SessionHeader:
        name = self._name_of(agent)
        header = SessionHeader(id=uuid4().hex, agent=name, metadata=dict(metadata or {}))
        await self.store.create(header)
        header.last_seq = await self.store.append(
            header.id,
            [SessionCreated(agent=name, parent_id=None, depth=0, metadata=header.metadata)],
            expect_seq=0,
        )
        return header

    async def run(self, sid: str, input: str) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        holder = uuid4().hex
        if not await self.store.acquire_lease(sid, holder, self.lease_ttl):
            raise SessionBusy(sid)

        loop: TurnLoop | None = None
        try:
            history = [stamped.event async for stamped in self.store.read(sid)]
            if _turn_incomplete(history):
                raise TurnIncomplete(sid)

            agent = self.agent_for(header.agent)
            model = resolve_model(agent, self.default_model)
            deps = await _resolve(self.deps_factory(header)) if self.deps_factory is not None else None

            header.status = "running"
            header.updated_at = datetime.now(UTC)
            await self.store.put_header(header)

            turn = TurnContext(
                session_id=sid,
                turn_id=uuid4().hex,
                agent=header.agent,
                depth=header.depth,
                input=input,
                metadata=header.metadata,
                deps=deps,
            )
            started = TurnStarted(turn_id=turn.turn_id, input=input)
            header.last_seq = await self.store.append(sid, [started], expect_seq=header.last_seq)
            history.append(started)
            yield Emitted(session_id=sid, depth=header.depth, seq=header.last_seq, event=started)

            loop = TurnLoop(
                store=self.store,
                provider=self.provider,
                header=header,
                agent=agent,
                tools=self.tools[header.agent],
                model=model,
                turn=turn,
                history=history,
                retry=self.retry,
                holder=holder,
                lease_ttl=self.lease_ttl,
            )
            async with aclosing(loop.run()) as turn_stream:
                async for emitted in turn_stream:
                    yield emitted
        finally:
            if loop is not None and not loop.lease_lost:
                header.status = "failed" if loop.failed else "idle"
                header.updated_at = datetime.now(UTC)
                await self.store.put_header(header)
            await self.store.release_lease(sid, holder)

    async def replay(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        async for stamped in self.store.read(sid, from_seq=from_seq):
            yield Emitted(session_id=sid, depth=header.depth, seq=stamped.seq, event=stamped.event)
