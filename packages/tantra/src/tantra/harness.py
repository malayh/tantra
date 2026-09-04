from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import aclosing
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from tantra.agent import Agent, agent_name, build_name_table
from tantra.ask import ApprovalResponse, AskResponse
from tantra.context import TurnContext, resolve_model
from tantra.errors import SeqConflict, SessionBusy, SessionNotFound, TantraError, TurnIncomplete
from tantra.events import (
    AgentMessageQueued,
    AskAnswered,
    AskRaised,
    CancelRequested,
    ChildSessionSpawned,
    KillRequested,
    SampleStarted,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    Stamped,
    TaskNoticeQueued,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.loop import DEFAULT_RETRY, KILLED_RESULT, SUBMIT_OUTPUT, Emitted, RetryConfig, TurnLoop
from tantra.permissions import check_permission
from tantra.providers.base import Provider
from tantra.skills import SKILL_TOOL, SkillInfo, Skills
from tantra.stores.base import Store
from tantra.tasking import TASK_TOOL_NAMES, TASK_TOOLS, TaskSpawner, TaskSupervisor
from tantra.tools import Context, Tool
from tantra.tracing import NULL_TRACER, Tracer, current_span

if TYPE_CHECKING:
    from tantra.compaction import Compactor
    from tantra.memory import Memory

TYPED_KEYS = frozenset({"type", "anyOf", "allOf", "oneOf", "$ref", "enum", "const"})
MAX_MESSAGE_CHARS = 32_768
CONTROL_EVENTS = (CancelRequested, AgentMessageQueued, TaskNoticeQueued, KillRequested)


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
        ref = await ctx.spawn(name, task)
        return {"task_id": ref.task_id, "agent": ref.agent}

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
        if entry.name == SUBMIT_OUTPUT or entry.name in TASK_TOOL_NAMES:
            raise TantraError(f"{label}: reserved tool name {entry.name!r}")
        if entry.permission is not None:
            check_permission(f"{label}: tool {entry.name!r}", entry.permission)
        if entry.name in table:
            raise TantraError(f"{label}: duplicate tool name {entry.name!r}")
        table[entry.name] = entry
    for sub in agent.subagents:
        delegate = _subagent_tool(sub)
        if delegate.name == SUBMIT_OUTPUT or delegate.name in TASK_TOOL_NAMES:
            raise TantraError(f"{label}: reserved sub-agent name {delegate.name!r}")
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
        memory: Memory | None = None,
        compactor: Compactor | None = None,
        telemetry: Tracer | None = None,
        max_concurrency: int = 4,
    ) -> None:
        self.provider = provider
        self.store = store
        self.default_model = default_model
        self.deps_factory = deps_factory
        self.retry = retry
        self.lease_ttl = lease_ttl
        self.max_depth = max_depth
        if max_concurrency < 1:
            raise TantraError(f"max_concurrency must be at least 1, got {max_concurrency}")
        self.max_concurrency = max_concurrency
        self.hooks = list(hooks)
        self.skills = skills
        self.memory = memory
        self.compactor = compactor
        self.tracer = telemetry if telemetry is not None else NULL_TRACER
        self.default_permission = check_permission("harness default_permission", default_permission)
        self.agents = build_name_table(agents)
        self.tools = {name: _tool_table(agent) for name, agent in self.agents.items()}
        for table in self.tools.values():
            table.update((entry.name, entry) for entry in TASK_TOOLS)
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
        turn_span: Any,
        supervisor: TaskSupervisor,
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
            spawner=TaskSpawner(supervisor, header, holder),
            skills_index=skills_index,
            memory=self.memory,
            compactor=self.compactor,
            tracer=self.tracer,
            turn_span=turn_span,
            event_claim=supervisor.claim,
        )

    async def _notify(self, emitted: Emitted) -> None:
        for hook in self.hooks:
            await hook.on_event(emitted)

    def _end_turn(
        self,
        span: Any,
        loop: TurnLoop | None,
        raised: BaseException | None,
        setup_terminal: TurnCompleted | None = None,
    ) -> None:
        terminal = loop.terminal if loop is not None and loop.terminal is not None else setup_terminal
        outcome_error: BaseException | str | None
        if isinstance(terminal, TurnCompleted):
            outcome = "cancelled" if terminal.stop_reason == "killed" else terminal.stop_reason
            stop_reason, output = terminal.stop_reason, terminal.output
            outcome_error, ask_id = None, None
        elif isinstance(terminal, TurnFailed):
            outcome, stop_reason, output, outcome_error, ask_id = "failed", None, None, terminal.error, None
        elif loop is not None and loop.suspended is not None:
            outcome, stop_reason, output, outcome_error, ask_id = "suspended", None, None, None, loop.suspended
        else:
            outcome, stop_reason, output, outcome_error, ask_id = "aborted", None, None, raised, None
        self.tracer.end_turn(
            span,
            outcome=outcome,
            stop_reason=stop_reason,
            output=output,
            final_text=_final_text(_turn_tail(loop.history)) if loop is not None else None,
            error=outcome_error,
            ask_id=ask_id,
        )

    async def _settle(
        self,
        header: SessionHeader,
        loop: TurnLoop | None,
        holder: str,
        setup_terminal: TurnCompleted | None = None,
    ) -> None:
        if loop is not None and not loop.lease_lost:
            if loop.suspended is not None:
                await self.store.patch_header(header.id, status="awaiting_input", pending_ask=loop.suspended)
            else:
                await self.store.patch_header(header.id, status="failed" if loop.failed else "idle", pending_ask=None)
        elif setup_terminal is not None:
            await self.store.patch_header(header.id, status="idle", pending_ask=None)
        await self.store.release_lease(header.id, holder)

    async def _finalize_setup_kill(
        self,
        header: SessionHeader,
        supervisor: TaskSupervisor,
        turn: TurnContext | None,
    ) -> tuple[TurnCompleted, list[Emitted]] | None:
        while True:
            stamped = [item async for item in self.store.read(header.id)]
            history = [item.event for item in stamped]
            if not any(isinstance(event, KillRequested) for event in history) or not _turn_incomplete(history):
                return None
            started = _last_turn(history)
            tail = _turn_tail(history)
            completed = {event.call_id for event in tail if isinstance(event, ToolCallCompleted)}
            begun = {event.call_id for event in tail if isinstance(event, ToolCallStarted)}
            additions: list[SessionEvent] = []
            for call in (event for event in tail if isinstance(event, ToolCallRequested)):
                if call.call_id in completed:
                    continue
                if call.call_id not in begun:
                    additions.append(ToolCallStarted(call_id=call.call_id))
                additions.append(ToolCallCompleted(call_id=call.call_id, result=KILLED_RESULT, is_error=True))
            terminal = TurnCompleted(turn_id=started.turn_id, stop_reason="killed")
            additions.append(terminal)
            expect_seq = stamped[-1].seq if stamped else 0
            try:
                last = await self.store.append(header.id, additions, expect_seq=expect_seq)
                break
            except SeqConflict:
                continue
        first = last - len(additions) + 1
        header.last_seq = last
        emitted = [
            Emitted(session_id=header.id, depth=header.depth, seq=first + index, event=event)
            for index, event in enumerate(additions)
        ]
        visible: list[Emitted] = []
        for item in emitted:
            if not supervisor.claim(item):
                continue
            await self._notify(item)
            visible.append(item)
        await supervisor.notify_terminal(header.id)
        if turn is None:
            turn = TurnContext(
                session_id=header.id,
                turn_id=started.turn_id,
                agent=header.agent,
                depth=header.depth,
                input=started.input,
                metadata=header.metadata,
                deps=None,
            )
        for hook in self.hooks:
            await hook.after_turn(turn, terminal)
        return terminal, visible

    async def run(self, sid: str, input: str) -> AsyncIterator[Emitted]:
        supervisor = TaskSupervisor(self, sid)
        await supervisor.start()
        try:
            async with aclosing(supervisor.merge(self._run_one(sid, input, supervisor, current_span.get()))) as stream:
                async for emitted in stream:
                    yield emitted
        finally:
            await supervisor.close()

    async def _run_one(
        self,
        sid: str,
        input: str,
        supervisor: TaskSupervisor,
        trace_parent: Any,
    ) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        holder = uuid4().hex
        if not await self.store.acquire_lease(sid, holder, self.lease_ttl):
            raise SessionBusy(sid)

        loop: TurnLoop | None = None
        span: Any = None
        traced = False
        raised: BaseException | None = None
        setup_terminal: TurnCompleted | None = None
        turn: TurnContext | None = None
        try:
            history = [stamped.event async for stamped in self.store.read(sid)]
            if any(isinstance(event, KillRequested) for event in history):
                return
            if _turn_incomplete(history):
                raise TurnIncomplete(sid)

            agent = self.agent_for(header.agent)
            model = resolve_model(agent, self.default_model)
            chain = await self._permission_chain(header)
            skills_index = await self._skill_index(agent)
            deps = await _resolve(self.deps_factory(header)) if self.deps_factory is not None else None

            await self.store.patch_header(sid, status="running")

            turn = TurnContext(
                session_id=sid,
                turn_id=uuid4().hex,
                agent=header.agent,
                depth=header.depth,
                input=input,
                metadata=header.metadata,
                deps=deps,
            )
            span = self.tracer.start_turn(turn, resumed=False, ask_id=None, parent=trace_parent)
            traced = True
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
                turn_span=span,
                supervisor=supervisor,
            )
            async with aclosing(loop.run()) as turn_stream:
                async for emitted in turn_stream:
                    yield emitted
        except asyncio.CancelledError as exc:
            raised = exc
            finalized = await self._finalize_setup_kill(header, supervisor, turn)
            if finalized is None:
                raise
            setup_terminal, emitted = finalized
            raised = None
            for item in emitted:
                yield item
        except BaseException as exc:
            raised = exc
            raise
        finally:
            await self._settle(header, loop, holder, setup_terminal)
            if traced:
                self._end_turn(span, loop, raised, setup_terminal)

    async def resume(
        self, sid: str, ask_id: str | None = None, response: AskResponse | None = None
    ) -> AsyncIterator[Emitted]:
        supervisor = TaskSupervisor(self, sid)
        await supervisor.start()
        try:
            async with aclosing(
                supervisor.merge(self._resume_one(sid, ask_id, response, supervisor, current_span.get()))
            ) as stream:
                async for emitted in stream:
                    yield emitted
        finally:
            await supervisor.close()

    async def _resume_one(
        self,
        sid: str,
        ask_id: str | None,
        response: AskResponse | None,
        supervisor: TaskSupervisor,
        trace_parent: Any,
    ) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        holder = uuid4().hex
        if not await self.store.acquire_lease(sid, holder, self.lease_ttl):
            raise SessionBusy(sid)

        loop: TurnLoop | None = None
        span: Any = None
        traced = False
        raised: BaseException | None = None
        setup_terminal: TurnCompleted | None = None
        turn: TurnContext | None = None
        try:
            stamped = [item async for item in self.store.read(sid)]
            history = [item.event for item in stamped]
            if any(isinstance(event, KillRequested) for event in history):
                finalized = await self._finalize_setup_kill(header, supervisor, turn)
                if finalized is not None:
                    setup_terminal, emitted = finalized
                    for item in emitted:
                        yield item
                return
            if not _turn_incomplete(history):
                raise TantraError(f"session {sid} has no incomplete turn to resume")
            if (ask_id is None) != (response is None):
                raise TantraError("resume takes an ask_id and a response together, or neither")

            turn_log = _turn_slice(stamped)
            pending = _pending_ask(turn_log)
            cancelled = any(isinstance(item.event, CancelRequested) for item in turn_log)

            if ask_id is None and pending is not None and not cancelled:
                replayed = Emitted(session_id=sid, depth=header.depth, seq=None, event=pending.event)
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
                for emitted in await self._append_absorbing(header, history, answered):
                    await self._notify(emitted)
                    yield emitted

            await self.store.patch_header(sid, status="running")

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
            span = self.tracer.start_turn(turn, resumed=True, ask_id=ask_id, parent=trace_parent)
            traced = True
            loop = self._build_loop(
                header=header,
                agent=agent,
                model=model,
                turn=turn,
                history=history,
                holder=holder,
                chain=chain,
                skills_index=skills_index,
                turn_span=span,
                supervisor=supervisor,
            )
            async with aclosing(loop.run()) as turn_stream:
                async for emitted in turn_stream:
                    yield emitted
        except asyncio.CancelledError as exc:
            raised = exc
            finalized = await self._finalize_setup_kill(header, supervisor, turn)
            if finalized is None:
                raise
            setup_terminal, emitted = finalized
            raised = None
            for item in emitted:
                yield item
        except BaseException as exc:
            raised = exc
            raise
        finally:
            await self._settle(header, loop, holder, setup_terminal)
            if traced:
                self._end_turn(span, loop, raised, setup_terminal)

    async def _append_absorbing(
        self,
        header: SessionHeader,
        history: list[SessionEvent],
        event: SessionEvent,
    ) -> list[Emitted]:
        absorbed: list[Emitted] = []
        while True:
            try:
                last = await self.store.append(header.id, [event], expect_seq=header.last_seq)
                break
            except SeqConflict:
                stamped = [item async for item in self.store.read(header.id, from_seq=header.last_seq)]
                if not stamped or any(not isinstance(item.event, CONTROL_EVENTS) for item in stamped):
                    raise
                for item in stamped:
                    header.last_seq = item.seq
                    history.append(item.event)
                    absorbed.append(Emitted(session_id=header.id, depth=header.depth, seq=item.seq, event=item.event))
        header.last_seq = last
        history.append(event)
        return [*absorbed, Emitted(session_id=header.id, depth=header.depth, seq=last, event=event)]

    async def send_user_message(self, root_session_id: str, message: str) -> str:
        header = await self.store.header(root_session_id)
        if header is None:
            raise SessionNotFound(root_session_id)
        if header.parent_id is not None:
            raise TantraError(f"session {root_session_id} is a child session; user messages target roots only")
        if not message.strip():
            raise TantraError("user message must not be blank")
        if len(message) > MAX_MESSAGE_CHARS:
            raise TantraError(f"user message exceeds {MAX_MESSAGE_CHARS} characters")

        message_id = uuid4().hex
        queued = AgentMessageQueued(
            message_id=message_id,
            sender_session_id=None,
            source="user",
            text=message,
        )
        while True:
            stamped = [item async for item in self.store.read(root_session_id)]
            history = [item.event for item in stamped]
            if not _turn_incomplete(history):
                raise TantraError(f"session {root_session_id} has no incomplete turn to receive a user message")
            expect_seq = stamped[-1].seq if stamped else 0
            try:
                await self.store.append(root_session_id, [queued], expect_seq=expect_seq)
                return message_id
            except SeqConflict:
                continue

    async def _cancel_one(self, sid: str) -> bool:
        history = [stamped.event async for stamped in self.store.read(sid)]
        if not _turn_incomplete(history):
            return False
        request = CancelRequested(turn_id=_last_turn(history).turn_id)
        await self.store.append(sid, [request], expect_seq=None)
        return True

    async def _descendants(self, sid: str) -> list[str]:
        found: list[str] = []
        async for stamped in self.store.read(sid):
            if isinstance(stamped.event, ChildSessionSpawned):
                child = stamped.event.child_session_id
                found.extend(await self._descendants(child))
                found.append(child)
        return found

    async def cancel(self, sid: str, *, recursive: bool = False) -> bool:
        """Flag the running turn for cancellation. The loop stops at its next store boundary.

        The request is appended blind, so it lands in one attempt against a log the running turn is
        still writing to. With `recursive`, every descendant session is flagged deepest-first before
        the target; returns True when at least one session had a turn to cancel.
        """
        if await self.store.header(sid) is None:
            raise SessionNotFound(sid)
        targets = [*await self._descendants(sid), sid] if recursive else [sid]
        flagged = False
        for target in targets:
            flagged = await self._cancel_one(target) or flagged
        return flagged

    async def replay(self, sid: str, *, from_seq: int = 0) -> AsyncIterator[Emitted]:
        header = await self.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        async for stamped in self.store.read(sid, from_seq=from_seq):
            yield Emitted(session_id=sid, depth=header.depth, seq=stamped.seq, event=stamped.event)
