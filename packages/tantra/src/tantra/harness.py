from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import aclosing
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from tantra.agent import Agent, agent_name, build_name_table
from tantra.ask import ApprovalResponse, AskResponse
from tantra.context import TurnContext, resolve_model
from tantra.errors import SeqConflict, SessionBusy, SessionNotFound, TantraError, TurnIncomplete
from tantra.events import (
    AskAnswered,
    AskRaised,
    CancelRequested,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    Stamped,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.loop import DEFAULT_RETRY, Emitted, RetryConfig, TurnLoop
from tantra.permissions import check_permission
from tantra.providers.base import Provider
from tantra.stores.base import Store
from tantra.tools import Tool

TYPED_KEYS = frozenset({"type", "anyOf", "allOf", "oneOf", "$ref", "enum", "const"})

CANCEL_ATTEMPTS = 5


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
    for pattern, value in agent.permissions.items():
        check_permission(f"{label}: permission rule {pattern!r}", value)
    for entry in agent.tools:
        if not isinstance(entry, Tool):
            raise TantraError(f"{label}: {entry!r} is not decorated with @tool")
        _check_schema(label, entry)
        if entry.permission is not None:
            check_permission(f"{label}: tool {entry.name!r}", entry.permission)
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


def _last_turn(events: Sequence[SessionEvent]) -> TurnStarted:
    return next(event for event in reversed(events) if isinstance(event, TurnStarted))


def _turn_slice(stamped: Sequence[Stamped]) -> list[Stamped]:
    start = 0
    for index, item in enumerate(stamped):
        if isinstance(item.event, TurnStarted):
            start = index
    return list(stamped[start:])


def _pending_ask(turn: Sequence[Stamped]) -> Stamped | None:
    answered = {item.event.ask_id for item in turn if isinstance(item.event, AskAnswered)}
    for item in reversed(turn):
        if isinstance(item.event, AskRaised) and item.event.ask_id not in answered:
            return item
    return None


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
        hooks: Sequence[Hook] = (),
        default_permission: str = "allow",
    ) -> None:
        self.provider = provider
        self.store = store
        self.default_model = default_model
        self.deps_factory = deps_factory
        self.retry = retry
        self.lease_ttl = lease_ttl
        self.hooks = list(hooks)
        self.default_permission = check_permission("harness default_permission", default_permission)
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

    def _build_loop(
        self,
        *,
        header: SessionHeader,
        agent: type[Agent],
        model: str,
        turn: TurnContext,
        history: Sequence[SessionEvent],
        holder: str,
    ) -> TurnLoop:
        return TurnLoop(
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
            hooks=self.hooks,
            default_permission=self.default_permission,
        )

    async def _notify(self, emitted: Emitted) -> None:
        for hook in self.hooks:
            await hook.on_event(emitted)

    async def _settle(self, header: SessionHeader, loop: TurnLoop | None, holder: str) -> None:
        if loop is not None and not loop.lease_lost:
            if loop.suspended is not None:
                header.status = "awaiting_input"
                header.pending_ask = loop.suspended
            else:
                header.status = "failed" if loop.failed else "idle"
                header.pending_ask = None
            header.updated_at = datetime.now(UTC)
            await self.store.put_header(header)
        await self.store.release_lease(header.id, holder)

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
            emitted = Emitted(session_id=sid, depth=header.depth, seq=header.last_seq, event=started)
            await self._notify(emitted)
            yield emitted

            for hook in self.hooks:
                await hook.before_turn(turn)

            loop = self._build_loop(header=header, agent=agent, model=model, turn=turn, history=history, holder=holder)
            async with aclosing(loop.run()) as turn_stream:
                async for emitted in turn_stream:
                    yield emitted
        finally:
            await self._settle(header, loop, holder)

    async def resume(
        self, sid: str, ask_id: str | None = None, response: AskResponse | None = None
    ) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        holder = uuid4().hex
        if not await self.store.acquire_lease(sid, holder, self.lease_ttl):
            raise SessionBusy(sid)

        loop: TurnLoop | None = None
        try:
            stamped = [item async for item in self.store.read(sid)]
            history = [item.event for item in stamped]
            if not _turn_incomplete(history):
                raise TantraError(f"session {sid} has no incomplete turn to resume")
            if (ask_id is None) != (response is None):
                raise TantraError("resume takes an ask_id and a response together, or neither")

            turn_log = _turn_slice(stamped)
            pending = _pending_ask(turn_log)
            cancelled = any(isinstance(item.event, CancelRequested) for item in turn_log)

            if ask_id is None and pending is not None and not cancelled:
                replayed = Emitted(session_id=sid, depth=header.depth, seq=pending.seq, event=pending.event)
                await self._notify(replayed)
                yield replayed
                return

            if ask_id is not None:
                if pending is None or pending.event.ask_id != ask_id:
                    raise TantraError(f"ask {ask_id!r} is unknown or already answered in session {sid}")
                if pending.event.request.extra.get("permission") and not isinstance(response, ApprovalResponse):
                    raise TantraError(
                        f"ask {ask_id!r} is a permission request and needs an ApprovalResponse, got {response!r}"
                    )
                answered = AskAnswered(ask_id=ask_id, response=response)
                header.last_seq = await self.store.append(sid, [answered], expect_seq=header.last_seq)
                history.append(answered)
                emitted = Emitted(session_id=sid, depth=header.depth, seq=header.last_seq, event=answered)
                await self._notify(emitted)
                yield emitted

            agent = self.agent_for(header.agent)
            model = resolve_model(agent, self.default_model)
            deps = await _resolve(self.deps_factory(header)) if self.deps_factory is not None else None

            header.status = "running"
            header.updated_at = datetime.now(UTC)
            await self.store.put_header(header)

            started = _last_turn(history)
            turn = TurnContext(
                session_id=sid,
                turn_id=started.turn_id,
                agent=header.agent,
                depth=header.depth,
                input=started.input,
                metadata=header.metadata,
                deps=deps,
            )
            loop = self._build_loop(header=header, agent=agent, model=model, turn=turn, history=history, holder=holder)
            async with aclosing(loop.run()) as turn_stream:
                async for emitted in turn_stream:
                    yield emitted
        finally:
            await self._settle(header, loop, holder)

    async def cancel(self, sid: str) -> bool:
        """Flag the running turn for cancellation. The loop stops at its next store boundary."""
        if await self.store.header(sid) is None:
            raise SessionNotFound(sid)
        for _ in range(CANCEL_ATTEMPTS):
            last_seq = 0
            history: list[SessionEvent] = []
            async for stamped in self.store.read(sid):
                last_seq = stamped.seq
                history.append(stamped.event)
            if not _turn_incomplete(history):
                return False
            request = CancelRequested(turn_id=_last_turn(history).turn_id)
            try:
                await self.store.append(sid, [request], expect_seq=last_seq)
            except SeqConflict:
                continue
            return True
        raise SeqConflict(f"{sid}: could not append CancelRequested after {CANCEL_ATTEMPTS} attempts")

    async def replay(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        async for stamped in self.store.read(sid, from_seq=from_seq):
            yield Emitted(session_id=sid, depth=header.depth, seq=stamped.seq, event=stamped.event)
