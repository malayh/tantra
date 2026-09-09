from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4, uuid5

from tantra.agent import Agent, agent_name, build_name_table
from tantra.ask import ApprovalResponse, AskResponse
from tantra.errors import (
    AskExpired,
    InvalidCommandReuse,
    MaxDepthExceeded,
    SessionNotFound,
    TantraError,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import (
    AgentFinished,
    AskAnswered,
    AskRaised,
    CancellationRequested,
    ChildCreated,
    InputQueued,
    SampleCompleted,
    SessionCreated,
    SessionEvent,
    SessionHeader,
    Stamped,
    TextPart,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    Usage,
)
from tantra.harness import _skill_tool, _tool_table
from tantra.hooks import Hook
from tantra.loop import DEFAULT_RETRY, FinishResult, RetryConfig, TurnEngine
from tantra.permissions import check_permission
from tantra.providers.base import Provider
from tantra.skills import SKILL_TOOL, SkillInfo, Skills
from tantra.stores.base import Store, reduce_journal
from tantra.tools import Context, Tool
from tantra.tracing import NULL_TRACER, Tracer

if TYPE_CHECKING:
    from tantra.compaction import Compactor
    from tantra.memory import Memory


@dataclass(frozen=True)
class LoggedEvent:
    agent_id: UUID
    seq: int
    event: SessionEvent


@dataclass(frozen=True)
class CommandReceipt:
    command_id: UUID
    duplicate: bool


@dataclass(frozen=True)
class TurnResult:
    agent_id: UUID
    command_id: UUID
    outcome: Literal["completed", "failed", "cancelled", "interrupted"]
    stop_reason: str | None
    text: str
    output: Any
    usage: Usage
    error: str | None


@dataclass
class _Signal:
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    generation: int = 0


@dataclass
class _LiveAsk:
    root_id: str
    agent_id: str
    event: AskRaised
    future: asyncio.Future[AskResponse]


def _id(value: UUID, label: str) -> tuple[UUID, str]:
    if not isinstance(value, UUID):
        raise TypeError(f"{label} must be a UUID")
    return value, value.hex


def _consume(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()


async def _shielded[T](awaitable: Awaitable[T]) -> T:
    task = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        task.add_done_callback(_consume)
        raise


class Runtime:
    def __init__(
        self,
        provider: Provider,
        store: Store,
        agents: Iterable[type[Agent]],
        *,
        default_model: str | None = None,
        max_depth: int = 3,
        deps_factory: Callable[[SessionHeader], Any] | None = None,
        retry: RetryConfig = DEFAULT_RETRY,
        hooks: Sequence[Hook] = (),
        default_permission: str = "allow",
        skills: Skills | None = None,
        memory: Memory | None = None,
        compactor: Compactor | None = None,
        telemetry: Tracer | None = None,
    ) -> None:
        self.provider = provider
        self.store = store
        self.default_model = default_model
        self.max_depth = max_depth
        self.deps_factory = deps_factory
        self.retry = retry
        self.hooks = list(hooks)
        self.default_permission = check_permission("runtime default_permission", default_permission)
        self.skills = skills
        self.memory = memory
        self.compactor = compactor
        self.tracer = telemetry if telemetry is not None else NULL_TRACER
        self.agents = build_name_table(agents)
        self.tools = {name: _tool_table(agent, include_subagents=False) for name, agent in self.agents.items()}
        if skills is not None:
            for name, agent in self.agents.items():
                if agent.skills == []:
                    continue
                if SKILL_TOOL in self.tools[name]:
                    raise TantraError(f"agent {name!r}: duplicate tool name {SKILL_TOOL!r}")
                self.tools[name][SKILL_TOOL] = _skill_tool(skills, agent.skills)
        child_agents = {agent_name(child) for parent in self.agents.values() for child in parent.subagents}
        for name, agent in self.agents.items():
            reserved = set()
            if agent.subagents:
                reserved.update(("spawn", "send"))
            if name in child_agents:
                reserved.update(("send", "finish"))
            collision = sorted(reserved & self.tools[name].keys())
            if collision:
                raise TantraError(f"agent {name!r}: duplicate tool name {collision[0]!r}")
        self.active: dict[str, asyncio.Task[None]] = {}
        self.asks: dict[str, _LiveAsk] = {}
        self.conditions: dict[str, _Signal] = {}
        self.writers: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._activations: dict[str, int] = {}
        self._turn_generations: dict[str, int] = {}
        self._task_reasons: dict[asyncio.Task[None], tuple[str, str]] = {}
        self._task_turns: dict[asyncio.Task[None], str] = {}
        self._errors: dict[str, dict[str, BaseException]] = {}
        self._known_roots: dict[str, str] = {}
        self._closed = False

    def _agent_for(self, name: str) -> type[Agent]:
        agent = self.agents.get(name)
        if agent is None:
            raise TantraError(f"unknown agent {name!r}; known: {sorted(self.agents)}")
        return agent

    def _name_of(self, agent: type[Agent] | str) -> str:
        name = agent if isinstance(agent, str) else agent_name(agent)
        self._agent_for(name)
        return name

    def _lock(self, root_id: str) -> asyncio.Lock:
        return self._locks.setdefault(root_id, asyncio.Lock())

    def _signal(self, agent_id: str) -> _Signal:
        return self.conditions.setdefault(agent_id, _Signal())

    def _ensure_open(self) -> None:
        if self._closed:
            raise TantraError("runtime is closed")

    async def _notify(self, agent_id: str) -> None:
        signal = self._signal(agent_id)
        async with signal.condition:
            signal.generation += 1
            signal.condition.notify_all()

    async def _append(self, agent_id: str, events: Sequence[SessionEvent]) -> list[Stamped]:
        if not events:
            return []
        last = await self.store.append(agent_id, events, expect_seq=None)
        first = last - len(events) + 1
        stamped = [Stamped(seq=first + index, event=event) for index, event in enumerate(events)]
        await self._notify(agent_id)
        return stamped

    async def _engine_append(
        self,
        agent_id: str,
        root_id: str,
        generation: int,
        events: Sequence[SessionEvent],
    ) -> list[Stamped]:
        async with self._lock(root_id):
            if self._turn_generations.get(agent_id) != generation:
                raise asyncio.CancelledError
            return await self._append(agent_id, events)

    async def _journal(self, agent_id: str) -> list[Stamped]:
        items: list[Stamped] = []
        after = 0
        while True:
            page = await self.store.read_page(agent_id, after=after)
            if not page:
                return items
            items.extend(page)
            after = page[-1].seq

    async def _header(self, agent_id: str) -> SessionHeader:
        header = await self.store.header(agent_id)
        if header is None:
            raise SessionNotFound(agent_id)
        return header

    async def _root_header(self, root_id: str) -> SessionHeader:
        header = await self._header(root_id)
        if header.parent_id is not None or header.root_id not in (None, header.id):
            raise TantraError(f"session {root_id} is not a root")
        return header

    async def _children(self, parent_id: str) -> list[SessionHeader]:
        children: list[SessionHeader] = []
        before: str | None = None
        while True:
            page = await self.store.list(parent_id=parent_id, limit=50, before=before)
            if not page:
                return children
            children.extend(page)
            if len(page) < 50:
                return children
            before = page[-1].id

    async def _tree_headers(self, root_id: str) -> list[SessionHeader]:
        root = await self._root_header(root_id)
        headers = [root]
        pending = [root_id]
        while pending:
            children = await self._children(pending.pop(0))
            headers.extend(children)
            pending.extend(child.id for child in children)
        return headers

    async def _tree_command(self, root_id: str, command_id: str) -> tuple[str, SessionEvent] | None:
        for header in await self._tree_headers(root_id):
            for item in await self._journal(header.id):
                event = item.event
                if isinstance(event, InputQueued | AskAnswered | CancellationRequested):
                    if getattr(event, "command_id", None) == command_id:
                        return header.id, event
        return None

    @staticmethod
    def _finished(journal: Sequence[Stamped]) -> AgentFinished | None:
        return next((item.event for item in reversed(journal) if isinstance(item.event, AgentFinished)), None)

    async def create(
        self,
        agent: type[Agent] | str,
        *,
        session_id: UUID | None = None,
        model: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> UUID:
        self._ensure_open()
        name = self._name_of(agent)
        resolved = self.agents[name].model or model or self.default_model
        if not resolved:
            raise TantraError(f"agent {name!r} sets no model and the runtime has no default_model")
        public_id = session_id or uuid4()
        _, sid = _id(public_id, "session_id")
        header = SessionHeader(
            id=sid,
            root_id=sid,
            agent=name,
            model=resolved,
            metadata=dict(metadata or {}),
        )
        async with self._lock(sid):
            self._ensure_open()
            await self.store.create(header)
            await self._append(
                sid,
                [
                    SessionCreated(
                        agent=name,
                        root_id=sid,
                        parent_id=None,
                        depth=0,
                        model=resolved,
                        metadata=header.metadata,
                    )
                ],
            )
            self._known_roots[sid] = sid
        return public_id

    def connect(self, root_id: UUID, *, after: int = 0, writable: bool = False) -> Connection:
        _, sid = _id(root_id, "root_id")
        if not isinstance(after, int):
            raise TypeError("after must be an integer")
        if after < 0:
            raise ValueError("after must be non-negative")
        return Connection(self, root_id, sid, after=after, writable=writable)

    async def events(self, agent_id: UUID, *, after: int = 0) -> AsyncIterator[LoggedEvent]:
        public_id, sid = _id(agent_id, "agent_id")
        if not isinstance(after, int):
            raise TypeError("after must be an integer")
        if after < 0:
            raise ValueError("after must be non-negative")
        await self._header(sid)
        async for item in self._stream(public_id, sid, after, None):
            yield item

    async def _stream(
        self,
        public_id: UUID,
        sid: str,
        after: int,
        connection: Connection | None,
    ) -> AsyncIterator[LoggedEvent]:
        cursor = after
        signal = self._signal(sid)
        while True:
            if connection is not None:
                connection._check_iteration()
            page = await self.store.read_page(sid, after=cursor)
            if page:
                for item in page:
                    if connection is not None:
                        connection._check_iteration()
                    cursor = item.seq
                    yield LoggedEvent(agent_id=public_id, seq=item.seq, event=item.event)
                continue
            async with signal.condition:
                generation = signal.generation
            page = await self.store.read_page(sid, after=cursor)
            if page:
                continue
            if connection is not None:
                connection._check_iteration()
            if self._closed:
                return
            async with signal.condition:
                if signal.generation == generation and not self._closed:
                    await signal.condition.wait()

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

    async def _deps(self, header: SessionHeader) -> Any:
        if self.deps_factory is None:
            return None
        value = self.deps_factory(header)
        return await value if inspect.isawaitable(value) else value

    def _register_ask(self, root_id: str, agent_id: str, event: AskRaised) -> asyncio.Future[AskResponse]:
        future: asyncio.Future[AskResponse] = asyncio.get_running_loop().create_future()
        live = _LiveAsk(root_id=root_id, agent_id=agent_id, event=event, future=future)
        self.asks[event.ask_id] = live

        def discard(_: asyncio.Future[AskResponse]) -> None:
            if self.asks.get(event.ask_id) is live:
                del self.asks[event.ask_id]

        future.add_done_callback(discard)
        return future

    def _activate(self, agent_id: str, root_id: str) -> None:
        if self._closed:
            return
        self._known_roots[agent_id] = root_id
        task = self.active.get(agent_id)
        if task is not None and not task.done():
            return
        generation = self._activations.get(agent_id, 0)
        self.active[agent_id] = asyncio.create_task(self._drain(agent_id, root_id, generation))

    async def _interrupt_if_incomplete(
        self,
        agent_id: str,
        event: TurnInterrupted | TurnCancelled,
    ) -> bool:
        root_id = self._known_roots.get(agent_id, agent_id)
        async with self._lock(root_id):
            state = reduce_journal(await self._journal(agent_id))
            if state.incomplete is None or state.incomplete.turn_id != event.turn_id:
                return False
            await self._append(agent_id, [event])
            return True

    async def _close_finished(
        self,
        agent_id: str,
        header: SessionHeader,
        journal: list[Stamped],
        finished: AgentFinished,
    ) -> None:
        state = reduce_journal(journal)
        events: list[SessionEvent] = [
            TurnCancelled(turn_id=item.command_id, reason="agent_finished") for item in state.pending
        ]
        if state.incomplete is not None:
            events.append(
                TurnCompleted(
                    turn_id=state.incomplete.turn_id,
                    stop_reason="finished",
                    output=finished.result,
                )
            )
        await self._append(agent_id, events)
        try:
            await self.store.patch_header(agent_id, status="idle", pending_ask=None, finished=True)
        except Exception:
            pass

    def _framework_tools(self, header: SessionHeader, agent: type[Agent]) -> dict[str, Tool]:
        tools = dict(self.tools[header.agent])

        async def spawn(agent_name: str, input: str, ctx: Context) -> str:
            return await self._actor_spawn(header, agent, ctx, agent_name, input)

        async def send(agent_id: UUID, input: str, ctx: Context) -> dict[str, Any]:
            return await self._actor_send(header, ctx, agent_id, input)

        async def finish(result: Any, ctx: Context) -> FinishResult:
            return await self._actor_finish(header, agent, ctx, result)

        if agent.subagents:
            tools["spawn"] = Tool(
                spawn,
                description="Create a declared child agent, queue its input, and return its ID.",
            )
        if header.parent_id is not None or agent.subagents:
            tools["send"] = Tool(
                send,
                description="Queue a message for a direct parent or child agent.",
            )
        if header.parent_id is not None:
            tools["finish"] = Tool(
                finish,
                description="Finish this agent and deliver its result to its parent.",
            )
        return tools

    @staticmethod
    def _internal_id(header: SessionHeader, ctx: Context, operation: str) -> UUID:
        return uuid5(UUID(hex=header.id), f"{ctx.turn_id}:{ctx.call_id}:{operation}")

    async def _actor_spawn(
        self,
        header: SessionHeader,
        agent: type[Agent],
        ctx: Context,
        requested: str,
        input: str,
    ) -> str:
        declared = {agent_name(child): child for child in agent.subagents}
        child_agent = declared.get(requested)
        if child_agent is None:
            raise TantraError(f"agent {header.agent!r} cannot spawn {requested!r}; declared: {sorted(declared)}")
        depth = header.depth + 1
        if depth > self.max_depth:
            raise MaxDepthExceeded(f"maximum agent depth {self.max_depth} exceeded")
        child_uuid = self._internal_id(header, ctx, "spawn")
        input_uuid = self._internal_id(header, ctx, "spawn_input")
        child_id = child_uuid.hex
        root_id = header.root_id or header.id
        async with self._lock(root_id):
            self._ensure_open()
            await self._header(header.id)
            current_journal = await self._journal(header.id)
            if self._finished(current_journal) is not None:
                raise TantraError(f"agent {UUID(hex=header.id)} is finished")
            root = await self._root_header(root_id)
            model = child_agent.model or root.model or self.default_model
            if not model:
                raise TantraError(f"agent {requested!r} sets no model and the runtime has no default_model")
            child = await self.store.header(child_id)
            if child is None:
                child = SessionHeader(
                    id=child_id,
                    root_id=root_id,
                    parent_id=header.id,
                    agent=requested,
                    depth=depth,
                    model=model,
                    metadata=dict(root.metadata),
                )
                await self.store.create(child)
            elif (
                child.root_id != root_id
                or child.parent_id != header.id
                or child.agent != requested
                or child.depth != depth
            ):
                raise InvalidCommandReuse(child_id)
            child_journal = await self._journal(child_id)
            created = SessionCreated(
                agent=requested,
                root_id=root_id,
                parent_id=header.id,
                depth=depth,
                model=model,
                metadata=dict(root.metadata),
            )
            if not any(isinstance(item.event, SessionCreated) for item in child_journal):
                await self._append(child_id, [created])
            event = ChildCreated(
                child_id=child_id,
                agent=requested,
                turn_id=ctx.turn_id,
                call_id=ctx.call_id,
            )
            existing = next(
                (
                    item.event
                    for item in current_journal
                    if isinstance(item.event, ChildCreated) and item.event.child_id == child_id
                ),
                None,
            )
            if existing is None:
                await self._append(header.id, [event])
            elif existing != event:
                raise InvalidCommandReuse(child_id)
            queued = InputQueued(command_id=input_uuid.hex, input=input)
            accepted = await self.store.enqueue(child_id, queued)
            if not accepted.duplicate:
                await self._notify(child_id)
            self._activations[child_id] = self._activations.get(child_id, 0) + 1
            self._activate(child_id, root_id)
        return str(child_uuid)

    async def _actor_send(
        self,
        header: SessionHeader,
        ctx: Context,
        target_uuid: UUID,
        input: str,
    ) -> dict[str, Any]:
        target_id = target_uuid.hex
        root_id = header.root_id or header.id
        command = self._internal_id(header, ctx, "send")
        message = f"[agent {UUID(hex=header.id)}] {input}"
        queued = InputQueued(command_id=command.hex, input=message)
        async with self._lock(root_id):
            self._ensure_open()
            sender = await self._header(header.id)
            target = await self._header(target_id)
            if target.root_id != root_id or not (sender.parent_id == target_id or target.parent_id == sender.id):
                raise TantraError("send is allowed only across a direct parent-child edge")
            journal = await self._journal(target_id)
            existing = await self._tree_command(root_id, command.hex)
            if existing is not None:
                if existing != (target_id, queued):
                    raise InvalidCommandReuse(command.hex)
                await self._notify(target_id)
                self._activations[target_id] = self._activations.get(target_id, 0) + 1
                self._activate(target_id, root_id)
                return {"command_id": str(command), "duplicate": True}
            if self._finished(journal) is not None:
                raise TantraError(f"agent {target_uuid} is finished")
            accepted = await self.store.enqueue(target_id, queued)
            await self._notify(target_id)
            self._activations[target_id] = self._activations.get(target_id, 0) + 1
            self._activate(target_id, root_id)
            return {"command_id": str(command), "duplicate": accepted.duplicate}

    async def _descendants(self, agent_id: str) -> list[SessionHeader]:
        descendants: list[SessionHeader] = []
        pending = [agent_id]
        while pending:
            children = await self._children(pending.pop(0))
            descendants.extend(children)
            pending.extend(child.id for child in children)
        return descendants

    async def _actor_finish(
        self,
        header: SessionHeader,
        agent: type[Agent],
        ctx: Context,
        result: Any,
    ) -> FinishResult:
        if header.parent_id is None:
            raise TantraError("finish is available only to child agents")
        if agent.output_schema is not None:
            normalized = agent.output_schema.model_validate(result).model_dump(mode="json")
        else:
            normalized = result
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        root_id = header.root_id or header.id
        command = self._internal_id(header, ctx, "finish")
        async with self._lock(root_id):
            self._ensure_open()
            current = await self._header(header.id)
            journal = await self._journal(header.id)
            prior = self._finished(journal)
            if prior is not None:
                if prior.result != normalized:
                    raise InvalidCommandReuse(command.hex)
                parent_id = current.parent_id
                assert parent_id is not None
                await self._notify(parent_id)
                self._activations[parent_id] = self._activations.get(parent_id, 0) + 1
                self._activate(parent_id, root_id)
                return FinishResult(normalized)
            unfinished = []
            for descendant in await self._descendants(header.id):
                descendant_journal = await self._journal(descendant.id)
                if self._finished(descendant_journal) is None:
                    unfinished.append(str(UUID(hex=descendant.id)))
            if unfinished:
                raise TantraError(f"unfinished descendants: {unfinished}")
            parent_id = current.parent_id
            assert parent_id is not None
            await self._header(parent_id)
            parent_journal = await self._journal(parent_id)
            parent_input = InputQueued(
                command_id=command.hex,
                input=f"[agent {UUID(hex=header.id)} finished] {encoded}",
            )
            existing = next(
                (
                    item.event
                    for item in parent_journal
                    if isinstance(item.event, InputQueued) and item.event.command_id == command.hex
                ),
                None,
            )
            if existing is None:
                if self._finished(parent_journal) is not None:
                    raise TantraError(f"agent {UUID(hex=parent_id)} is finished")
                await self.store.enqueue(parent_id, parent_input)
            elif existing != parent_input:
                raise InvalidCommandReuse(command.hex)
            await self._notify(parent_id)
            self._activations[parent_id] = self._activations.get(parent_id, 0) + 1
            self._activate(parent_id, root_id)
            await self._append(header.id, [AgentFinished(result=normalized)])
            state = reduce_journal(journal)
            await self._append(
                header.id,
                [TurnCancelled(turn_id=item.command_id, reason="agent_finished") for item in state.pending],
            )
            try:
                await self.store.patch_header(header.id, status="idle", pending_ask=None, finished=True)
            except Exception:
                pass
            return FinishResult(normalized)

    async def _drain(self, agent_id: str, root_id: str, generation: int) -> None:
        task = asyncio.current_task()
        assert task is not None
        current: str | None = None
        turn_generation: int | None = None
        failed = False
        aborted = False
        try:
            while True:
                if self._closed:
                    return
                header = await self._header(agent_id)
                journal = await self._journal(agent_id)
                finished = self._finished(journal)
                if finished is not None:
                    async with self._lock(root_id):
                        fresh = await self._journal(agent_id)
                        durable = self._finished(fresh)
                        if durable is not None:
                            await self._close_finished(agent_id, header, fresh, durable)
                        if self.active.get(agent_id) is task:
                            del self.active[agent_id]
                    return
                state = reduce_journal(journal)
                if state.incomplete is not None:
                    current = state.incomplete.turn_id
                    await self._interrupt_if_incomplete(
                        agent_id,
                        TurnInterrupted(turn_id=current, reason="process_stopped"),
                    )
                    self._errors.get(agent_id, {}).pop(current, None)
                    current = None
                    continue
                if not state.pending:
                    async with self._lock(root_id):
                        state = reduce_journal(await self._journal(agent_id))
                        if state.pending:
                            continue
                        if self.active.get(agent_id) is task:
                            del self.active[agent_id]
                        return
                queued = state.pending[0]
                current = queued.command_id
                agent = self._agent_for(header.agent)
                model = header.model or agent.model or self.default_model
                if not model:
                    raise TantraError(f"agent {header.agent!r} sets no model and the runtime has no default_model")
                deps = await self._deps(header)
                skills_index = await self._skill_index(agent)
                async with self._lock(root_id):
                    fresh = reduce_journal(await self._journal(agent_id))
                    if not any(item.command_id == queued.command_id for item in fresh.pending):
                        current = None
                        continue
                    turn_generation = self._turn_generations.get(agent_id, 0) + 1
                    self._turn_generations[agent_id] = turn_generation
                    self._task_turns[task] = current
                    await self.store.patch_header(agent_id, status="running")
                tools = self._framework_tools(header, agent)
                engine = TurnEngine(
                    store=self.store,
                    provider=self.provider,
                    header=header,
                    agent=agent,
                    tools=tools,
                    model=model,
                    history=[item.event for item in journal],
                    deps=deps,
                    retry=self.retry,
                    hooks=self.hooks,
                    default_permission=self.default_permission,
                    skills_index=skills_index,
                    memory=self.memory,
                    compactor=self.compactor,
                    tracer=self.tracer,
                    ask_future=lambda event: self._register_ask(root_id, agent_id, event),
                    append_events=lambda events, generation=turn_generation: self._engine_append(
                        agent_id, root_id, generation, events
                    ),
                    terminal_tool="finish" if header.parent_id is not None else None,
                )
                terminal = await engine.run(queued)
                self._task_turns.pop(task, None)
                async with self._lock(root_id):
                    if self._turn_generations.get(agent_id) == turn_generation:
                        await self.store.patch_header(
                            agent_id,
                            status="failed" if isinstance(terminal, TurnFailed) else "idle",
                            pending_ask=None,
                        )
                self._errors.get(agent_id, {}).pop(current, None)
                current = None
        except asyncio.CancelledError:
            aborted = True
            outcome, reason = self._task_reasons.pop(task, ("cancelled", "cancelled"))
            if current is not None:
                event: TurnInterrupted | TurnCancelled
                if outcome == "interrupted":
                    event = TurnInterrupted(turn_id=current, reason=reason)
                else:
                    event = TurnCancelled(turn_id=current, reason=reason)
                try:
                    await self._interrupt_if_incomplete(agent_id, event)
                    async with self._lock(root_id):
                        if (
                            self.active.get(agent_id) is task
                            and self._turn_generations.get(agent_id) == turn_generation
                        ):
                            await self.store.patch_header(agent_id, status="idle", pending_ask=None)
                except BaseException as exc:
                    self._errors.setdefault(agent_id, {})[current] = exc
                    await self._notify(agent_id)
        except BaseException as exc:
            failed = True
            if current is not None:
                self._errors.setdefault(agent_id, {})[current] = exc
            await self._notify(agent_id)
        finally:
            self._task_turns.pop(task, None)
            async with self._lock(root_id):
                if self.active.get(agent_id) is task:
                    del self.active[agent_id]
                if (failed or aborted) and not self._closed and self._activations.get(agent_id, 0) > generation:
                    self._activate(agent_id, root_id)

    async def _claim(self, connection: Connection) -> None:
        self._ensure_open()
        header = await self._root_header(connection._root)
        self._known_roots[connection._root] = connection._root
        if connection.writable:
            self._agent_for(header.agent)
            async with self._lock(connection._root):
                self._ensure_open()
                generation = self.writers.get(connection._root, 0) + 1
                self.writers[connection._root] = generation
                connection._generation = generation
                connection._entered = True
            await self._notify(connection._root)
            return
        connection._entered = True

    def _check_writer(self, connection: Connection) -> None:
        if not connection._entered or not connection.writable:
            raise WriterRequired("an active writable connection is required")
        if self.writers.get(connection._root) != connection._generation:
            raise WriterReplaced(f"writer for {connection.root_id} was replaced")
        self._ensure_open()

    async def _send(self, connection: Connection, input: str, command_id: UUID) -> CommandReceipt:
        return await _shielded(self._accept_send(connection, input, command_id))

    async def _accept_send(self, connection: Connection, input: str, command_id: UUID) -> CommandReceipt:
        public_id, cid = _id(command_id, "command_id")
        event = InputQueued(command_id=cid, input=input)
        async with self._lock(connection._root):
            self._check_writer(connection)
            existing = await self._tree_command(connection._root, cid)
            if existing is not None:
                if existing != (connection._root, event):
                    raise InvalidCommandReuse(cid)
                duplicate = True
            else:
                await self._root_header(connection._root)
                if self._finished(await self._journal(connection._root)) is not None:
                    raise TantraError(f"agent {connection.root_id} is finished")
                accepted = await self.store.enqueue(connection._root, event)
                duplicate = accepted.duplicate
                await self._notify(connection._root)
            self._activations[connection._root] = self._activations.get(connection._root, 0) + 1
            self._activate(connection._root, connection._root)
            return CommandReceipt(command_id=public_id, duplicate=duplicate)

    async def _answer(
        self,
        connection: Connection,
        ask_id: UUID,
        response: AskResponse,
        command_id: UUID,
    ) -> CommandReceipt:
        return await _shielded(self._accept_answer(connection, ask_id, response, command_id))

    async def _accept_answer(
        self,
        connection: Connection,
        ask_id: UUID,
        response: AskResponse,
        command_id: UUID,
    ) -> CommandReceipt:
        public_command, cid = _id(command_id, "command_id")
        _, aid = _id(ask_id, "ask_id")
        event = AskAnswered(
            ask_id=aid,
            response=response,
            command_id=cid,
            answered_by=connection._root,
        )
        async with self._lock(connection._root):
            self._check_writer(connection)
            existing = await self._tree_command(connection._root, cid)
            if existing is not None:
                previous = existing[1]
                if (
                    not isinstance(previous, AskAnswered)
                    or previous.model_copy(update={"answered_by": connection._root}) != event
                ):
                    raise InvalidCommandReuse(cid)
                return CommandReceipt(command_id=public_command, duplicate=True)
            live = self.asks.get(aid)
            if live is None or live.root_id != connection._root or live.future.done():
                raise AskExpired(aid)
            if live.event.request.kind != response.kind:
                raise TantraError(f"ask {aid!r} needs a {live.event.request.kind!r} response, got {response.kind!r}")
            if live.event.request.extra.get("permission") and not isinstance(response, ApprovalResponse):
                raise TantraError(f"ask {aid!r} is a permission request and needs an ApprovalResponse")
            await self._append(live.agent_id, [event])
            live.future.set_result(response)
            return CommandReceipt(command_id=public_command, duplicate=False)

    async def _cancel(self, connection: Connection, command_id: UUID) -> CommandReceipt:
        return await _shielded(self._accept_cancel(connection, command_id))

    async def _accept_cancel(self, connection: Connection, command_id: UUID) -> CommandReceipt:
        public_id, cid = _id(command_id, "command_id")
        async with self._lock(connection._root):
            self._check_writer(connection)
            existing = await self._tree_command(connection._root, cid)
            if existing is not None:
                if existing[0] != connection._root or not isinstance(existing[1], CancellationRequested):
                    raise InvalidCommandReuse(cid)
                event = existing[1]
                duplicate = True
            else:
                actor_ids = [connection._root]
                actor_ids.extend(
                    agent_id
                    for agent_id, task in self.active.items()
                    if agent_id != connection._root
                    and not task.done()
                    and self._known_roots.get(agent_id) == connection._root
                )
                targets: dict[str, list[str]] = {}
                for agent_id in actor_ids:
                    actor_journal = await self._journal(agent_id)
                    if self._finished(actor_journal) is not None:
                        continue
                    state = reduce_journal(actor_journal)
                    turn_ids = [item.command_id for item in state.pending]
                    if state.incomplete is not None and state.incomplete.turn_id not in turn_ids:
                        turn_ids.append(state.incomplete.turn_id)
                    if turn_ids:
                        targets[agent_id] = turn_ids
                event = CancellationRequested(command_id=cid, targets=targets)
                await self._append(connection._root, [event])
                duplicate = False
            for agent_id, target_turns in event.targets.items():
                actor_journal = await self._journal(agent_id)
                if self._finished(actor_journal) is not None:
                    continue
                state = reduce_journal(actor_journal)
                live_turns = {item.command_id for item in state.pending}
                if state.incomplete is not None:
                    live_turns.add(state.incomplete.turn_id)
                terminal = [
                    TurnCancelled(turn_id=turn_id, reason="cancelled")
                    for turn_id in target_turns
                    if turn_id in live_turns
                ]
                task = self.active.get(agent_id)
                task_turn = self._task_turns.get(task) if task is not None else None
                cancel_task = (
                    task is not None
                    and not task.done()
                    and (task_turn in target_turns or task_turn is None and bool(terminal))
                )
                if not terminal and not cancel_task:
                    continue
                await self._append(agent_id, terminal)
                if cancel_task:
                    assert task is not None
                    self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
                    self._task_reasons[task] = ("cancelled", "cancelled")
                    if self.active.get(agent_id) is task:
                        del self.active[agent_id]
                    task.cancel()
                await self.store.patch_header(agent_id, status="idle", pending_ask=None)
            return CommandReceipt(command_id=public_id, duplicate=duplicate)

    async def _wait_result(self, agent_id: str, command_id: UUID) -> TurnResult:
        cid = command_id.hex
        signal = self._signal(agent_id)
        while True:
            journal = await self._journal(agent_id)
            result = _turn_result(UUID(hex=agent_id), command_id, journal)
            if result is not None:
                return result
            error = self._errors.get(agent_id, {}).get(cid)
            if error is not None:
                raise error
            if self._closed:
                raise TantraError(f"runtime closed before command {command_id} finished")
            async with signal.condition:
                generation = signal.generation
            journal = await self._journal(agent_id)
            result = _turn_result(UUID(hex=agent_id), command_id, journal)
            if result is not None:
                return result
            error = self._errors.get(agent_id, {}).get(cid)
            if error is not None:
                raise error
            async with signal.condition:
                if signal.generation == generation and not self._closed:
                    await signal.condition.wait()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = [task for task in self.active.values() if not task.done()]
        for root_id in set(self._known_roots.values()) | set(self.writers):
            async with self._lock(root_id):
                self.writers[root_id] = self.writers.get(root_id, 0) + 1
                for agent_id, task in list(self.active.items()):
                    if task.done() or self._known_roots.get(agent_id) != root_id:
                        continue
                    self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
                    self._task_reasons[task] = ("interrupted", "runtime_closed")
                    state = None
                    try:
                        state = reduce_journal(await self._journal(agent_id))
                        if state.incomplete is not None:
                            await self._append(
                                agent_id,
                                [TurnInterrupted(turn_id=state.incomplete.turn_id, reason="runtime_closed")],
                            )
                        await self.store.patch_header(agent_id, status="idle", pending_ask=None)
                    except BaseException as exc:
                        if state is not None and state.incomplete is not None:
                            self._errors.setdefault(agent_id, {})[state.incomplete.turn_id] = exc
        self.active.clear()
        for agent_id in list(self.conditions):
            await self._notify(agent_id)
        for task in tasks:
            task.cancel()
        for ask_id, live in list(self.asks.items()):
            if not live.future.done():
                live.future.set_exception(AskExpired(ask_id))
        for agent_id in list(self.conditions):
            await self._notify(agent_id)


class Connection:
    def __init__(
        self,
        runtime: Runtime,
        root_id: UUID,
        root: str,
        *,
        after: int,
        writable: bool,
    ) -> None:
        self.runtime = runtime
        self.root_id = root_id
        self.writable = writable
        self._root = root
        self._after = after
        self._generation: int | None = None
        self._entered = False
        self._iterator: AsyncIterator[LoggedEvent] | None = None

    async def __aenter__(self) -> Connection:
        if self._entered:
            raise TantraError("connection is already entered")
        await self.runtime._claim(self)
        return self

    async def __aexit__(self, *_args: Any) -> None:
        self._entered = False
        if self._iterator is not None:
            await self._iterator.aclose()
            self._iterator = None

    def _check_iteration(self) -> None:
        if not self._entered:
            raise WriterRequired("connection must be entered before use")
        if self.writable and self.runtime.writers.get(self._root) != self._generation:
            raise WriterReplaced(f"writer for {self.root_id} was replaced")

    def __aiter__(self) -> Connection:
        return self

    async def __anext__(self) -> LoggedEvent:
        self._check_iteration()
        if self._iterator is None:
            self._iterator = self.runtime._stream(self.root_id, self._root, self._after, self)
        return await anext(self._iterator)

    async def send(self, input: str, *, command_id: UUID) -> CommandReceipt:
        return await self.runtime._send(self, input, command_id)

    async def prompt(self, input: str, *, command_id: UUID) -> TurnResult:
        await self.runtime._send(self, input, command_id)
        return await self.runtime._wait_result(self._root, command_id)

    async def answer(
        self,
        ask_id: UUID,
        response: AskResponse,
        *,
        command_id: UUID,
    ) -> CommandReceipt:
        return await self.runtime._answer(self, ask_id, response, command_id)

    async def cancel(self, *, command_id: UUID) -> CommandReceipt:
        return await self.runtime._cancel(self, command_id)


def _turn_result(
    agent_id: UUID,
    command_id: UUID,
    journal: Sequence[Stamped],
) -> TurnResult | None:
    cid = command_id.hex
    start = next(
        (
            index
            for index, item in enumerate(journal)
            if isinstance(item.event, TurnStarted) and item.event.turn_id == cid
        ),
        None,
    )
    terminal_index = next(
        (
            index
            for index, item in enumerate(journal)
            if isinstance(item.event, TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted)
            and item.event.turn_id == cid
        ),
        None,
    )
    if terminal_index is None:
        return None
    begin = start if start is not None else terminal_index
    turn = [item.event for item in journal[begin : terminal_index + 1]]
    terminal = turn[-1]
    usage = Usage()
    completed_samples: list[str] = []
    texts: dict[str, list[str]] = {}
    for event in turn:
        if isinstance(event, TextPart):
            texts.setdefault(event.sample_id, []).append(event.text)
        elif isinstance(event, SampleCompleted):
            completed_samples.append(event.sample_id)
            usage = Usage(**{name: getattr(usage, name) + getattr(event.usage, name) for name in Usage.model_fields})
    text = "".join(texts.get(completed_samples[-1], [])) if completed_samples else ""
    if isinstance(terminal, TurnCompleted):
        outcome = "completed"
        stop_reason = terminal.stop_reason
        output = terminal.output
        error = None
    elif isinstance(terminal, TurnFailed):
        outcome = "failed"
        stop_reason = None
        output = None
        error = terminal.error
    elif isinstance(terminal, TurnCancelled):
        outcome = "cancelled"
        stop_reason = terminal.reason
        output = None
        error = None
    else:
        outcome = "interrupted"
        stop_reason = terminal.reason
        output = None
        error = None
    return TurnResult(
        agent_id=agent_id,
        command_id=command_id,
        outcome=outcome,
        stop_reason=stop_reason,
        text=text,
        output=output,
        usage=usage,
        error=error,
    )
