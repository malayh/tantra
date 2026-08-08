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
    SampleStarted,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextPart,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.loop import DEFAULT_RETRY, ChildOutcome, Emitted, RetryConfig, TurnLoop
from tantra.permissions import check_permission
from tantra.providers.base import Provider
from tantra.skills import SkillInfo, Skills
from tantra.stores.base import Store
from tantra.tools import Context, Tool

TYPED_KEYS = frozenset({"type", "anyOf", "allOf", "oneOf", "$ref", "enum", "const"})

CANCEL_ATTEMPTS = 5

SKILL_TOOL = "skill"


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


def _subagent_tool(sub: type[Agent]) -> Tool:
    name = agent_name(sub)
    described = (sub.__doc__ or "").strip()

    async def delegate(task: str, ctx: Context) -> Any:
        return await ctx.spawn(name, task)

    return Tool(delegate, name=name, description=described or f"Delegate a task to the {name} sub-agent.")


def _skill_tool(skills: Skills, allowed: list[str] | None) -> Tool:
    async def skill(name: str) -> str:
        """Load one skill's full instructions by name.

        The system prompt lists only each skill's name and description; call this to pull the body,
        and the paths of any reference files shipped alongside it, into context before acting.
        """
        if allowed is not None and name not in allowed:
            raise TantraError(f"skill {name!r} is not available to this agent")
        loaded = await skills.load(name)
        if not loaded.files:
            return loaded.body
        return loaded.body + "\n\n## Files\n" + "\n".join(loaded.files)

    return Tool(skill, permission="allow")


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
    for sub in agent.subagents:
        delegate = _subagent_tool(sub)
        if delegate.name in table:
            raise TantraError(f"{label}: duplicate tool name {delegate.name!r}")
        table[delegate.name] = delegate
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


def _turn_tail(events: Sequence[SessionEvent]) -> list[SessionEvent]:
    start = 0
    for index, event in enumerate(events):
        if isinstance(event, TurnStarted):
            start = index
    return list(events[start:])


def _final_text(events: Sequence[SessionEvent]) -> str:
    texts: dict[str, list[str]] = {}
    last = ""
    for event in events:
        if isinstance(event, SampleStarted):
            last = event.sample_id
            texts.setdefault(last, [])
        elif isinstance(event, TextPart):
            texts.setdefault(event.sample_id, []).append(event.text)
    return "".join(texts.get(last, []))


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
        max_depth: int = 3,
        skills: Skills | None = None,
    ) -> None:
        self.provider = provider
        self.store = store
        self.default_model = default_model
        self.deps_factory = deps_factory
        self.retry = retry
        self.lease_ttl = lease_ttl
        self.max_depth = max_depth
        self.hooks = list(hooks)
        self.skills = skills
        self.default_permission = check_permission("harness default_permission", default_permission)
        self.agents = build_name_table(agents)
        self.tools = {name: _tool_table(agent) for name, agent in self.agents.items()}
        if skills is not None:
            for name, agent in self.agents.items():
                if agent.skills == []:
                    continue
                if SKILL_TOOL in self.tools[name]:
                    raise TantraError(f"agent {name!r}: duplicate tool name {SKILL_TOOL!r}")
                self.tools[name][SKILL_TOOL] = _skill_tool(skills, agent.skills)

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

    async def _permission_chain(self, header: SessionHeader) -> list[dict[str, str]]:
        chain: list[dict[str, str]] = []
        parent_id = header.parent_id
        while parent_id is not None:
            parent = await self.store.header(parent_id)
            if parent is None:
                raise TantraError(
                    f"session {header.id}: parent session {parent_id!r} is missing; cannot derive permissions"
                )
            chain.append(self.agent_for(parent.agent).permissions)
            parent_id = parent.parent_id
        chain.reverse()
        return chain

    async def _skill_index(self, agent: type[Agent]) -> list[SkillInfo]:
        if self.skills is None or agent.skills == []:
            return []
        index = list(await self.skills.index())
        if agent.skills is None:
            return index
        wanted = set(agent.skills)
        missing = sorted(wanted - {info.name for info in index})
        if missing:
            raise TantraError(f"agent {agent_name(agent)!r}: unknown skills {missing}")
        return [info for info in index if info.name in wanted]

    def _build_loop(
        self,
        *,
        header: SessionHeader,
        agent: type[Agent],
        model: str,
        turn: TurnContext,
        history: Sequence[SessionEvent],
        holder: str,
        chain: Sequence[dict[str, str]],
        skills_index: Sequence[SkillInfo],
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
            permission_chain=chain,
            spawner=_ChildRunner(self, header),
            skills_index=skills_index,
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
            chain = await self._permission_chain(header)
            skills_index = await self._skill_index(agent)
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

            loop = self._build_loop(
                header=header,
                agent=agent,
                model=model,
                turn=turn,
                history=history,
                holder=holder,
                chain=chain,
                skills_index=skills_index,
            )
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

            agent = self.agent_for(header.agent)
            model = resolve_model(agent, self.default_model)
            chain = await self._permission_chain(header)
            skills_index = await self._skill_index(agent)
            deps = await _resolve(self.deps_factory(header)) if self.deps_factory is not None else None

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
            loop = self._build_loop(
                header=header,
                agent=agent,
                model=model,
                turn=turn,
                history=history,
                holder=holder,
                chain=chain,
                skills_index=skills_index,
            )
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


class _ChildRunner:
    def __init__(self, harness: Harness, parent: SessionHeader) -> None:
        self.harness = harness
        self.parent = parent

    def resolve(self, agent: type[Agent] | str) -> str:
        name = self.harness._name_of(agent)
        depth = self.parent.depth + 1
        if depth > self.harness.max_depth:
            raise TantraError(f"max_depth {self.harness.max_depth} exceeded: cannot spawn {name!r} at depth {depth}")
        return name

    async def create(self, agent: str) -> str:
        depth = self.parent.depth + 1
        header = SessionHeader(
            id=uuid4().hex,
            agent=agent,
            parent_id=self.parent.id,
            depth=depth,
            metadata=dict(self.parent.metadata),
        )
        await self.harness.store.create(header)
        await self.harness.store.append(
            header.id,
            [SessionCreated(agent=agent, parent_id=self.parent.id, depth=depth, metadata=header.metadata)],
            expect_seq=0,
        )
        return header.id

    async def _log(self, sid: str) -> list[SessionEvent]:
        return [stamped.event async for stamped in self.harness.store.read(sid)]

    async def drive(self, sid: str, input: str) -> AsyncIterator[Emitted]:
        events = await self._log(sid)
        if not any(isinstance(event, TurnStarted) for event in events):
            stream = self.harness.run(sid, input)
        elif _turn_incomplete(events):
            stream = self.harness.resume(sid)
        else:
            return
        async with aclosing(stream) as child:
            async for emitted in child:
                yield emitted

    async def outcome(self, sid: str) -> ChildOutcome:
        events = await self._log(sid)
        if _turn_incomplete(events):
            header = await self.harness.store.header(sid)
            pending = header.pending_ask if header is not None else None
            if pending is None:
                return ChildOutcome(error=TantraError(f"child session {sid} left its turn incomplete"))
            return ChildOutcome(pending_ask=pending)
        tail = _turn_tail(events)
        terminal = next((event for event in reversed(tail) if isinstance(event, TurnCompleted | TurnFailed)), None)
        if isinstance(terminal, TurnFailed):
            return ChildOutcome(error=TantraError(f"child session {sid} failed: {terminal.error}"))
        if terminal is None or terminal.stop_reason == "cancelled":
            return ChildOutcome(error=TantraError(f"child session {sid}: sub-agent turn cancelled"))
        if terminal.output is not None:
            return ChildOutcome(result=terminal.output)
        text = _final_text(tail)
        if not text and terminal.stop_reason == "max_steps":
            return ChildOutcome(
                error=TantraError(f"child session {sid}: sub-agent hit max_steps without producing output")
            )
        return ChildOutcome(result=text)
