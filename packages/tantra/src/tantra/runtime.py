from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

from tantra.agent import Agent, agent_name, build_name_table
from tantra.ask import ApprovalResponse, AskResponse
from tantra.errors import (
    AskExpired,
    InvalidCommandReuse,
    SessionNotFound,
    TantraError,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import (
    AskAnswered,
    AskRaised,
    CancellationRequested,
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
from tantra.loop import DEFAULT_RETRY, RetryConfig, TurnEngine
from tantra.permissions import check_permission
from tantra.providers.base import Provider
from tantra.skills import SKILL_TOOL, SkillInfo, Skills
from tantra.stores.base import Store, reduce_journal
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
        self.active: dict[str, asyncio.Task[None]] = {}
        self.asks: dict[str, _LiveAsk] = {}
        self.conditions: dict[str, _Signal] = {}
        self.writers: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._activations: dict[str, int] = {}
        self._turn_generations: dict[str, int] = {}
        self._task_reasons: dict[asyncio.Task[None], tuple[str, str]] = {}
        self._errors: dict[str, dict[str, BaseException]] = {}
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
        lock = self._locks.get(root_id)
        if lock is not None:
            return lock
        self._ensure_open()
        lock = asyncio.Lock()
        self._locks[root_id] = lock
        return lock

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
        root_id: str,
        generation: int,
        events: Sequence[SessionEvent],
    ) -> list[Stamped]:
        async with self._lock(root_id):
            if self._turn_generations.get(root_id) != generation:
                raise asyncio.CancelledError
            return await self._append(root_id, events)

    async def _journal(self, agent_id: str) -> list[Stamped]:
        items: list[Stamped] = []
        after = 0
        while True:
            page = await self.store.read_page(agent_id, after=after)
            if not page:
                return items
            items.extend(page)
            after = page[-1].seq

    async def _command(self, root_id: str, command_id: str) -> SessionEvent | None:
        for item in await self._journal(root_id):
            event = item.event
            if isinstance(event, InputQueued | AskAnswered | CancellationRequested):
                if getattr(event, "command_id", None) == command_id:
                    return event
        return None

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

    def _register_ask(self, root_id: str, event: AskRaised) -> asyncio.Future[AskResponse]:
        future: asyncio.Future[AskResponse] = asyncio.get_running_loop().create_future()
        live = _LiveAsk(root_id=root_id, event=event, future=future)
        self.asks[event.ask_id] = live

        def discard(_: asyncio.Future[AskResponse]) -> None:
            if self.asks.get(event.ask_id) is live:
                del self.asks[event.ask_id]

        future.add_done_callback(discard)
        return future

    def _activate(self, root_id: str) -> None:
        if self._closed:
            return
        task = self.active.get(root_id)
        if task is not None and not task.done():
            return
        generation = self._activations.get(root_id, 0)
        self.active[root_id] = asyncio.create_task(self._drain(root_id, generation))

    async def _interrupt_if_incomplete(self, root_id: str, event: TurnInterrupted | TurnCancelled) -> bool:
        async with self._lock(root_id):
            state = reduce_journal(await self._journal(root_id))
            if state.incomplete is None or state.incomplete.turn_id != event.turn_id:
                return False
            await self._append(root_id, [event])
            return True

    async def _drain(self, root_id: str, generation: int) -> None:
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
                journal = await self._journal(root_id)
                state = reduce_journal(journal)
                if state.incomplete is not None:
                    current = state.incomplete.turn_id
                    await self._interrupt_if_incomplete(
                        root_id,
                        TurnInterrupted(turn_id=current, reason="process_stopped"),
                    )
                    self._errors.get(root_id, {}).pop(current, None)
                    current = None
                    continue
                if not state.pending:
                    async with self._lock(root_id):
                        state = reduce_journal(await self._journal(root_id))
                        if state.pending:
                            continue
                        if self.active.get(root_id) is task:
                            del self.active[root_id]
                        return
                queued = state.pending[0]
                current = queued.command_id
                header = await self._root_header(root_id)
                agent = self._agent_for(header.agent)
                model = header.model or agent.model or self.default_model
                if not model:
                    raise TantraError(f"agent {header.agent!r} sets no model and the runtime has no default_model")
                deps = await self._deps(header)
                skills_index = await self._skill_index(agent)
                async with self._lock(root_id):
                    fresh = reduce_journal(await self._journal(root_id))
                    if not any(item.command_id == queued.command_id for item in fresh.pending):
                        current = None
                        continue
                    turn_generation = self._turn_generations.get(root_id, 0) + 1
                    self._turn_generations[root_id] = turn_generation
                    await self.store.patch_header(root_id, status="running")
                engine = TurnEngine(
                    store=self.store,
                    provider=self.provider,
                    header=header,
                    agent=agent,
                    tools=self.tools[header.agent],
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
                    ask_future=lambda event: self._register_ask(root_id, event),
                    append_events=lambda events, generation=turn_generation: self._engine_append(
                        root_id, generation, events
                    ),
                )
                terminal = await engine.run(queued)
                async with self._lock(root_id):
                    if self._turn_generations.get(root_id) == turn_generation:
                        await self.store.patch_header(
                            root_id,
                            status="failed" if isinstance(terminal, TurnFailed) else "idle",
                            pending_ask=None,
                        )
                self._errors.get(root_id, {}).pop(current, None)
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
                    await self._interrupt_if_incomplete(root_id, event)
                    async with self._lock(root_id):
                        if self.active.get(root_id) is task and self._turn_generations.get(root_id) == turn_generation:
                            await self.store.patch_header(root_id, status="idle", pending_ask=None)
                except BaseException as exc:
                    self._errors.setdefault(root_id, {})[current] = exc
                    await self._notify(root_id)
        except BaseException as exc:
            failed = True
            if current is not None:
                self._errors.setdefault(root_id, {})[current] = exc
            await self._notify(root_id)
        finally:
            async with self._lock(root_id):
                if self.active.get(root_id) is task:
                    del self.active[root_id]
                if (failed or aborted) and not self._closed and self._activations.get(root_id, 0) > generation:
                    self._activate(root_id)

    async def _claim(self, connection: Connection) -> None:
        self._ensure_open()
        header = await self._root_header(connection._root)
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
            existing = await self._command(connection._root, cid)
            if existing is not None:
                if existing != event:
                    raise InvalidCommandReuse(cid)
                duplicate = True
            else:
                accepted = await self.store.enqueue(connection._root, event)
                duplicate = accepted.duplicate
                await self._notify(connection._root)
            self._activations[connection._root] = self._activations.get(connection._root, 0) + 1
            self._activate(connection._root)
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
        event = AskAnswered(ask_id=aid, response=response, command_id=cid)
        async with self._lock(connection._root):
            self._check_writer(connection)
            existing = await self._command(connection._root, cid)
            if existing is not None:
                if existing != event:
                    raise InvalidCommandReuse(cid)
                return CommandReceipt(command_id=public_command, duplicate=True)
            live = self.asks.get(aid)
            if live is None or live.root_id != connection._root or live.future.done():
                raise AskExpired(aid)
            if live.event.request.kind != response.kind:
                raise TantraError(f"ask {aid!r} needs a {live.event.request.kind!r} response, got {response.kind!r}")
            if live.event.request.extra.get("permission") and not isinstance(response, ApprovalResponse):
                raise TantraError(f"ask {aid!r} is a permission request and needs an ApprovalResponse")
            await self._append(connection._root, [event])
            live.future.set_result(response)
            return CommandReceipt(command_id=public_command, duplicate=False)

    async def _cancel(self, connection: Connection, command_id: UUID) -> CommandReceipt:
        return await _shielded(self._accept_cancel(connection, command_id))

    async def _accept_cancel(self, connection: Connection, command_id: UUID) -> CommandReceipt:
        public_id, cid = _id(command_id, "command_id")
        event = CancellationRequested(command_id=cid)
        async with self._lock(connection._root):
            self._check_writer(connection)
            existing = await self._command(connection._root, cid)
            if existing is not None:
                if existing != event:
                    raise InvalidCommandReuse(cid)
                return CommandReceipt(command_id=public_id, duplicate=True)
            state = reduce_journal(await self._journal(connection._root))
            queued = [TurnCancelled(turn_id=item.command_id, reason="cancelled") for item in state.pending]
            if state.incomplete is not None:
                queued.append(TurnCancelled(turn_id=state.incomplete.turn_id, reason="cancelled"))
            await self._append(connection._root, [event, *queued])
            task = self.active.get(connection._root)
            if task is not None and not task.done():
                self._turn_generations[connection._root] = self._turn_generations.get(connection._root, 0) + 1
                self._task_reasons[task] = ("cancelled", "cancelled")
                task.cancel()
                if self.active.get(connection._root) is task:
                    del self.active[connection._root]
            await self.store.patch_header(connection._root, status="idle", pending_ask=None)
            return CommandReceipt(command_id=public_id, duplicate=False)

    async def _wait_result(self, root_id: str, command_id: UUID) -> TurnResult:
        cid = command_id.hex
        signal = self._signal(root_id)
        while True:
            journal = await self._journal(root_id)
            result = _turn_result(UUID(hex=root_id), command_id, journal)
            if result is not None:
                return result
            error = self._errors.get(root_id, {}).get(cid)
            if error is not None:
                raise error
            if self._closed:
                raise TantraError(f"runtime closed before command {command_id} finished")
            async with signal.condition:
                generation = signal.generation
            journal = await self._journal(root_id)
            result = _turn_result(UUID(hex=root_id), command_id, journal)
            if result is not None:
                return result
            error = self._errors.get(root_id, {}).get(cid)
            if error is not None:
                raise error
            async with signal.condition:
                if signal.generation == generation and not self._closed:
                    await signal.condition.wait()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        roots = list(self._locks.items())
        held: list[asyncio.Lock] = []
        tasks: list[asyncio.Task[None]] = []
        try:
            for _, lock in roots:
                await lock.acquire()
                held.append(lock)
            tasks = [task for task in self.active.values() if not task.done()]
            for task in tasks:
                self._task_reasons[task] = ("interrupted", "runtime_closed")
            for root_id, _ in roots:
                self.writers[root_id] = self.writers.get(root_id, 0) + 1
                self._turn_generations[root_id] = self._turn_generations.get(root_id, 0) + 1
                state = None
                try:
                    state = reduce_journal(await self._journal(root_id))
                    if state.incomplete is not None:
                        await self._append(
                            root_id,
                            [TurnInterrupted(turn_id=state.incomplete.turn_id, reason="runtime_closed")],
                        )
                    await self.store.patch_header(root_id, status="idle", pending_ask=None)
                except BaseException as exc:
                    if state is not None and state.incomplete is not None:
                        self._errors.setdefault(root_id, {})[state.incomplete.turn_id] = exc
            self.active.clear()
        finally:
            for lock in reversed(held):
                lock.release()
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
