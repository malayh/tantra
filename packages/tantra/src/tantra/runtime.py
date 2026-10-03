from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4, uuid5

from pydantic import TypeAdapter

from tantra.agent import Agent, agent_name, build_name_table
from tantra.ask import ApprovalResponse, AskResponse
from tantra.context import compacted_history
from tantra.coordinator import (
    AnswerPayload,
    CancelPayload,
    ClaimWriterPayload,
    CommandEnvelope,
    CommandReply,
    CoordinatedStoreProtocol,
    Coordinator,
    DeletePayload,
    Ownership,
    PostgresCoordinator,
    ReleaseWriterPayload,
    SendPayload,
    WriterToken,
)
from tantra.errors import (
    AskExpired,
    CommandTimeout,
    CoordinatorUnavailable,
    InvalidCommandReuse,
    LeaseLost,
    MaxDepthExceeded,
    RemoteExecutionError,
    SessionBusy,
    SessionExists,
    SessionNotFound,
    TantraError,
    WriterReplaced,
    WriterRequired,
)
from tantra.events import (
    ActorStatus,
    AgentFinished,
    AskAnswered,
    AskRaised,
    CancellationRequested,
    ChildCreated,
    InputQueued,
    LoggedEvent,
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
    TurnSummary,
    Usage,
)
from tantra.hooks import Hook
from tantra.loop import DEFAULT_RETRY, FinishResult, RetryConfig, TurnEngine
from tantra.permissions import check_permission, decide
from tantra.providers.base import Provider
from tantra.skills import SKILL_TOOL, SkillInfo, Skills
from tantra.stores.base import HistorySnapshot, OperationalState, Store, reduce_journal
from tantra.tools import Context, Tool
from tantra.tracing import NULL_TRACER, Tracer

ACTOR_STATUS_ADAPTER: TypeAdapter[ActorStatus] = TypeAdapter(ActorStatus)

if TYPE_CHECKING:
    from tantra.compaction import Compactor
    from tantra.memory import Memory

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


def _skill_tool(skills: Skills, allowed: list[str] | None) -> Tool:
    async def skill(name: str) -> str:
        if allowed is not None and name not in allowed:
            raise TantraError(f"skill {name!r} is not available to this agent")
        loaded = await skills.load(name)
        if not loaded.files:
            return loaded.body
        return loaded.body + "\n\n## Files\n" + "\n".join(loaded.files)

    return Tool(
        skill,
        description="Load one available skill by name and return its full instructions plus its bundled file list.",
        permission="allow",
    )


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


def _terminal_summary(event: TurnCompleted | TurnFailed | TurnCancelled | TurnInterrupted) -> TurnSummary:
    if isinstance(event, TurnCompleted):
        return TurnSummary(UUID(hex=event.turn_id), "completed", event.stop_reason, None)
    if isinstance(event, TurnFailed):
        return TurnSummary(UUID(hex=event.turn_id), "failed", None, event.error)
    if isinstance(event, TurnCancelled):
        return TurnSummary(UUID(hex=event.turn_id), "cancelled", event.reason, None)
    return TurnSummary(UUID(hex=event.turn_id), "interrupted", event.reason, None)


def _lifecycle_input(child_id: str, turn: TurnSummary) -> InputQueued | None:
    if turn.outcome == "completed" and turn.stop_reason == "finished":
        return None
    child_uuid = UUID(hex=child_id)
    payload = {
        "child_id": str(child_uuid),
        "outcome": turn.outcome,
        "stop_reason": turn.stop_reason,
        "turn_id": str(turn.turn_id),
    }
    if turn.error is not None:
        payload["error"] = turn.error
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return InputQueued(
        command_id=uuid5(child_uuid, turn.turn_id.hex).hex,
        input=f"[agent {child_uuid} turn ended] {encoded}",
    )


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
        coordinator: Coordinator | None = None,
        history_mode: Literal["full", "compacted"] = "full",
    ) -> None:
        if history_mode not in ("full", "compacted"):
            raise ValueError("history_mode must be full or compacted")
        self.history_mode = history_mode
        self.provider = provider
        self.store = store
        self.coordinator = coordinator
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
        self.tools = {name: _tool_table(agent) for name, agent in self.agents.items()}
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
                reserved.update(("spawn", "send", "status"))
            if name in child_agents:
                reserved.update(("send", "finish"))
            collision = sorted(reserved & self.tools[name].keys())
            if collision:
                raise TantraError(f"agent {name!r}: duplicate tool name {collision[0]!r}")
            if name not in child_agents:
                continue
            effective = {tool.name: tool.permission for tool in self.tools[name].values()}
            effective.update({"send": None, "finish": None})
            if agent.subagents:
                effective.update({"spawn": None, "status": None})
            for tool_name, tool_permission in sorted(effective.items()):
                if decide(tool_name, agent.permissions, tool_permission, self.default_permission) == "ask":
                    raise TantraError(f"agent {name!r}: child tool {tool_name!r} cannot use 'ask' permission")

        self.active: dict[str, asyncio.Task[None]] = {}
        self.asks: dict[str, _LiveAsk] = {}
        self.conditions: dict[str, _Signal] = {}
        self._wait_conditions: dict[str, _Signal] = {}
        self.writers: dict[str, int] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._activations: dict[str, int] = {}
        self._turn_generations: dict[str, int] = {}
        self._task_reasons: dict[asyncio.Task[None], tuple[str, str]] = {}
        self._task_turns: dict[asyncio.Task[None], str] = {}
        self._errors: dict[str, dict[str, BaseException]] = {}
        self._failed_prestarts: dict[str, dict[str, str | None]] = {}
        self._known_roots: dict[str, str] = {}
        self._ownerships: dict[str, Ownership] = {}
        self._unrecovered: set[str] = set()
        self._deleting: set[str] = set()
        self._renewals: dict[str, asyncio.Task[None]] = {}
        self._watchers: dict[str, asyncio.Task[None]] = {}
        self._connection_interests: dict[str, int] = {}
        self._stream_interests: dict[str, dict[str, int]] = {}
        self._wait_interests: dict[str, dict[str, int]] = {}
        self._observations: dict[str, Any] = {}
        self._observation_errors: dict[str, CoordinatorUnavailable] = {}
        self._connections: dict[UUID, Connection] = {}
        self._maintenance_task: asyncio.Task[None] | None = None
        self._started = coordinator is None
        self._closing = False
        self._closed = False
        self._close_complete = asyncio.Event()

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

    def _wait_signal(self, agent_id: str) -> _Signal:
        return self._wait_conditions.setdefault(agent_id, _Signal())

    def _ensure_open(self) -> None:
        if self._closed or self._closing:
            raise TantraError("runtime is closed")

    async def start(self) -> None:
        self._ensure_open()
        if self._started:
            return
        assert self.coordinator is not None
        if isinstance(self.coordinator, PostgresCoordinator) and self.coordinator.store is not self.store:
            raise TypeError("PostgresCoordinator and Runtime must use the same PostgresStore")
        await self.coordinator.start(self._handle_request)
        self._started = True
        if callable(getattr(self.coordinator, "cleanup", None)):
            self._maintenance_task = asyncio.create_task(self._maintain())

    async def _maintain(self) -> None:
        assert self.coordinator is not None
        cleanup = getattr(self.coordinator, "cleanup", None)
        while not self._closed:
            try:
                started = asyncio.get_running_loop().time()
                backlog = False
                while asyncio.get_running_loop().time() - started < 1.0:
                    deleted = await cleanup(100)
                    backlog = deleted == 100
                    if not backlog:
                        break
                    await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except CoordinatorUnavailable:
                backlog = False
            await asyncio.sleep(0.1 if backlog else 60.0)

    def _ensure_started(self) -> None:
        if self.coordinator is not None and not self._started:
            raise CoordinatorUnavailable("coordinated runtime is not started")

    async def _ensure_owner_locked(self, root_id: str) -> Ownership:
        self._ensure_started()
        ownership = self._ownerships.get(root_id)
        if ownership is not None:
            return ownership
        assert self.coordinator is not None
        ownership = await self.coordinator.acquire(root_id)
        if ownership is None:
            raise CoordinatorUnavailable(f"root {root_id} is owned by another runtime")
        self._ownerships[root_id] = ownership
        self._unrecovered.add(root_id)
        renewal = asyncio.create_task(self._renew(root_id, ownership))
        self._renewals[root_id] = renewal
        try:
            await self._recover_locked(root_id, ownership)
            self._unrecovered.discard(root_id)
        except BaseException:
            if await self.coordinator.release(ownership):
                self._ownerships.pop(root_id, None)
                self._unrecovered.discard(root_id)
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
                self._renewals.pop(root_id, None)
            raise
        return ownership

    async def _renew(self, root_id: str, ownership: Ownership) -> None:
        assert self.coordinator is not None
        current = ownership
        interval = float(getattr(self.coordinator, "lease_ttl", 60.0)) / 3
        try:
            while not self._closed and self._ownerships.get(root_id) == current:
                await asyncio.sleep(interval)
                current = await self.coordinator.renew(current)
                self._ownerships[root_id] = current
        except asyncio.CancelledError:
            raise
        except BaseException:
            async with self._lock(root_id):
                if self._ownerships.get(root_id) is not None:
                    self._invalidate_root_locked(root_id)

    def _invalidate_root_locked(self, root_id: str) -> None:
        self._ownerships.pop(root_id, None)
        for agent_id, task in list(self.active.items()):
            if self._known_roots.get(agent_id) != root_id or task.done():
                continue
            self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
            self._task_reasons[task] = ("interrupted", "lease_lost")
            if self.active.get(agent_id) is task:
                del self.active[agent_id]
            task.cancel()
        for ask_id, live in list(self.asks.items()):
            if live.root_id == root_id and not live.future.done():
                live.future.set_exception(AskExpired(ask_id))

    def _observation_capable(self) -> bool:
        return (
            self.coordinator is not None
            and callable(getattr(self.coordinator, "observe", None))
            and callable(getattr(self.coordinator, "observation", None))
        )

    def _current_observation(self, root_id: str) -> Any:
        observation = self._observations.get(root_id)
        if observation is None and self._observation_capable():
            assert self.coordinator is not None
            observation = self.coordinator.observation(root_id)
            if observation is not None:
                self._observations[root_id] = observation
        return observation

    def _watch_root(self, root_id: str) -> None:
        if self.coordinator is None or self._closed:
            return
        current = self._watchers.get(root_id)
        if current is not None and not current.done() and not current.cancelling():
            return
        self._watchers[root_id] = asyncio.create_task(self._watch(root_id))

    def _retain_connection_interest(self, root_id: str) -> None:
        self._connection_interests[root_id] = self._connection_interests.get(root_id, 0) + 1
        self._watch_root(root_id)

    def _retain_actor_interest(self, interests: dict[str, dict[str, int]], root_id: str, actor_id: str) -> None:
        actors = interests.setdefault(root_id, {})
        actors[actor_id] = actors.get(actor_id, 0) + 1
        self._watch_root(root_id)

    def _interested(self, root_id: str) -> bool:
        return bool(
            self._connection_interests.get(root_id)
            or self._stream_interests.get(root_id)
            or self._wait_interests.get(root_id)
        )

    async def _stop_watcher_if_unused(self, root_id: str) -> None:
        if self._interested(root_id):
            return
        task = self._watchers.pop(root_id, None)
        self._observations.pop(root_id, None)
        self._observation_errors.pop(root_id, None)
        if task is asyncio.current_task():
            return
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _release_connection_interest(self, root_id: str) -> None:
        count = self._connection_interests.get(root_id, 0)
        if count <= 1:
            self._connection_interests.pop(root_id, None)
        else:
            self._connection_interests[root_id] = count - 1
        await self._stop_watcher_if_unused(root_id)

    async def _release_actor_interest(
        self,
        interests: dict[str, dict[str, int]],
        root_id: str,
        actor_id: str,
    ) -> None:
        actors = interests.get(root_id)
        if actors is not None:
            count = actors.get(actor_id, 0)
            if count <= 1:
                actors.pop(actor_id, None)
            else:
                actors[actor_id] = count - 1
            if not actors:
                interests.pop(root_id, None)
        await self._stop_watcher_if_unused(root_id)

    async def _watch(self, root_id: str) -> None:
        assert self.coordinator is not None
        try:
            if self._observation_capable():
                await self._watch_observations(root_id)
            else:
                await self._watch_fallback(root_id)
        except asyncio.CancelledError:
            raise
        finally:
            task = asyncio.current_task()
            if self._watchers.get(root_id) is task:
                self._watchers.pop(root_id, None)

    async def _watch_observations(self, root_id: str) -> None:
        assert self.coordinator is not None
        interval = float(getattr(self.coordinator, "catch_up_interval", 2.0))
        while not self._closed:
            iterator = self.coordinator.observe(root_id)
            try:
                async for observation in iterator:
                    await self._apply_observation(root_id, observation)
                    if getattr(observation, "deleted", False):
                        return
            except asyncio.CancelledError:
                raise
            except CoordinatorUnavailable as exc:
                self._observation_errors[root_id] = exc
                await self._notify_root_interests(root_id)
                await asyncio.sleep(interval)
            finally:
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    await close()

    async def _apply_observation(self, root_id: str, observation: Any) -> None:
        if getattr(observation, "deleted", False):
            self._observations[root_id] = observation
            await self._finish_deletion(root_id, [])
            return
        previous = self._observations.get(root_id)
        self._observations[root_id] = observation
        self._observation_errors.pop(root_id, None)
        await self._refresh_writers_from_observation(root_id, observation)
        streams = set(self._stream_interests.get(root_id, {}))
        waiters = set(self._wait_interests.get(root_id, {}))
        if previous is None:
            stream_wakes = streams
            wait_wakes = waiters
        else:
            stream_wakes = {
                actor_id
                for actor_id in streams
                if previous.actors.get(actor_id, (None, False))[0] != observation.actors.get(actor_id, (None, False))[0]
            }
            root_changed = (
                previous.owner_instance != observation.owner_instance
                or previous.owner_generation != observation.owner_generation
                or previous.owner_valid != observation.owner_valid
                or previous.recovery != observation.recovery
                or previous.error != observation.error
            )
            wait_wakes = {
                actor_id
                for actor_id in waiters
                if previous.sample != observation.sample
                or previous.actors.get(actor_id) != observation.actors.get(actor_id)
                or root_changed
            }
        if observation.error is not None:
            stream_wakes = streams
            wait_wakes = waiters
        for actor_id in stream_wakes:
            await self._notify(actor_id)
        for actor_id in wait_wakes - stream_wakes:
            await self._notify_waiter(actor_id)

    async def _notify_root_interests(self, root_id: str) -> None:
        streams = set(self._stream_interests.get(root_id, {}))
        waiters = set(self._wait_interests.get(root_id, {}))
        for actor_id in streams:
            await self._notify(actor_id)
        for actor_id in waiters - streams:
            await self._notify_waiter(actor_id)

    async def _watch_fallback(self, root_id: str) -> None:
        assert self.coordinator is not None
        cursor = 0
        interval = float(getattr(self.coordinator, "catch_up_interval", 2.0))
        while not self._closed:
            iterator = self.coordinator.watch(root_id, after=cursor)
            pending: asyncio.Task[Any] | None = None
            try:
                await self._refresh_writers(root_id)
                pending = asyncio.create_task(anext(iterator))
                while not self._closed:
                    done, _ = await asyncio.wait({pending}, timeout=interval)
                    if not done:
                        await self._refresh_writers(root_id)
                        continue
                    notice = pending.result()
                    if notice.kind == "writer":
                        await self._refresh_writers(root_id)
                    if notice.actor_id is not None:
                        await self._notify(notice.actor_id)
                    else:
                        await self._notify_root_interests(root_id)
                    cursor = notice.change_id
                    pending = asyncio.create_task(anext(iterator))
            except asyncio.CancelledError:
                raise
            except BaseException:
                await self._notify_root_interests(root_id)
                await asyncio.sleep(interval)
            finally:
                if pending is not None:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    await close()

    async def _refresh_writers(self, root_id: str) -> None:
        assert self.coordinator is not None
        for connection in list(self._connections.values()):
            token = connection._writer_token
            if connection._root != root_id or token is None:
                continue
            if not await self.coordinator.writer_matches(token):
                connection._writer_token = None
                connection._writer_change_id = None
                await self._notify(root_id)

    async def _refresh_writers_from_observation(self, root_id: str, observation: Any) -> None:
        if observation.error is not None:
            return
        for connection in list(self._connections.values()):
            token = connection._writer_token
            if connection._root != root_id or token is None:
                continue
            if connection._writer_change_id is not None and observation.change_id < connection._writer_change_id:
                continue
            connection._writer_change_id = None
            if observation.writer_connection != str(token.connection_id):
                connection._writer_token = None
                await self._notify(root_id)

    async def _initialize_root_locked(self, header: SessionHeader, created: SessionCreated) -> None:
        assert self.coordinator is not None
        is_deleted = getattr(self.store, "is_deleted", None)
        if is_deleted is not None and await is_deleted(header.id):
            raise SessionExists(header.id)
        if await self.store.header(header.id) is not None:
            raise SessionExists(header.id)
        ownership = await self.coordinator.acquire(header.id)
        if ownership is None:
            if await self.store.header(header.id) is not None:
                raise SessionExists(header.id)
            raise CoordinatorUnavailable(f"root {header.id} is owned by another runtime")
        self._ownerships[header.id] = ownership
        self._known_roots[header.id] = header.id
        self._unrecovered.add(header.id)
        renewal = asyncio.create_task(self._renew(header.id, ownership))
        self._renewals[header.id] = renewal
        try:
            async with self.coordinator.transaction(ownership) as store:
                await store.create(header)
                await store.append(header.id, [created])
            self._unrecovered.discard(header.id)
            await self._notify(header.id)
        finally:
            if await self.coordinator.release(ownership):
                self._ownerships.pop(header.id, None)
                self._unrecovered.discard(header.id)
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
                self._renewals.pop(header.id, None)

    async def _notify(self, agent_id: str) -> None:
        signal = self._signal(agent_id)
        async with signal.condition:
            signal.generation += 1
            signal.condition.notify_all()
        if agent_id in self._wait_conditions:
            await self._notify_waiter(agent_id)

    async def _notify_waiter(self, agent_id: str) -> None:
        signal = self._wait_signal(agent_id)
        async with signal.condition:
            signal.generation += 1
            signal.condition.notify_all()

    async def _append(
        self,
        agent_id: str,
        events: Sequence[SessionEvent],
        store: CoordinatedStoreProtocol | None = None,
    ) -> list[Stamped]:
        if not events:
            return []
        root_id = self._known_roots.get(agent_id, agent_id)
        if store is not None:
            last = await store.append(agent_id, events)
        elif self.coordinator is not None and root_id in self._known_roots:
            ownership = self._ownerships.get(root_id)
            if ownership is None:
                raise LeaseLost(root_id)
            async with self.coordinator.transaction(ownership) as guarded:
                last = await guarded.append(agent_id, events)
        else:
            last = await self.store.append(agent_id, events)
        first = last - len(events) + 1
        stamped = [Stamped(seq=first + index, event=event) for index, event in enumerate(events)]
        await self._notify(agent_id)
        return stamped

    async def _patch_header(self, agent_id: str, **fields: Any) -> SessionHeader:
        root_id = self._known_roots.get(agent_id, agent_id)
        if self.coordinator is None or root_id not in self._known_roots:
            return await self.store.patch_header(agent_id, **fields)
        ownership = self._ownerships.get(root_id)
        if ownership is None:
            raise LeaseLost(root_id)
        async with self.coordinator.transaction(ownership) as store:
            return await store.patch_header(agent_id, **fields)

    async def _create_session(self, header: SessionHeader) -> None:
        root_id = header.root_id or header.id
        if self.coordinator is None:
            await self.store.create(header)
            return
        ownership = self._ownerships.get(root_id)
        if ownership is None:
            raise LeaseLost(root_id)
        async with self.coordinator.transaction(ownership) as store:
            await store.create(header)

    async def _enqueue(self, agent_id: str, event: InputQueued) -> Any:
        root_id = self._known_roots.get(agent_id, agent_id)
        if self.coordinator is None:
            return await self.store.enqueue(agent_id, event)
        ownership = self._ownerships.get(root_id)
        if ownership is None:
            raise LeaseLost(root_id)
        async with self.coordinator.transaction(ownership) as store:
            return await store.enqueue(agent_id, event)

    async def _set_active(self, root_id: str, agent_id: str, active: bool) -> None:
        if self.coordinator is None:
            return
        ownership = self._ownerships.get(root_id)
        if ownership is None:
            if active:
                raise LeaseLost(root_id)
            return
        async with self.coordinator.transaction(ownership) as store:
            await store.set_active(agent_id, active)

    async def _release_if_idle_locked(self, root_id: str) -> None:
        if self.coordinator is None:
            return
        if self._closing:
            return
        ownership = self._ownerships.get(root_id)
        if ownership is None:
            return
        if any(
            not task.done() and self._known_roots.get(agent_id) == root_id for agent_id, task in self.active.items()
        ):
            return
        if any(live.root_id == root_id and not live.future.done() for live in self.asks.values()):
            return
        failed = self._failed_prestarts.get(root_id, {})
        for agent_id in list(failed):
            if await self.coordinator.relinquish_failed_prestart(ownership, agent_id):
                self._failed_prestarts.pop(root_id, None)
                self._ownerships.pop(root_id, None)
                renewal = self._renewals.pop(root_id, None)
                if renewal is not None and renewal is not asyncio.current_task():
                    renewal.cancel()
                return
            continue
        if not failed:
            self._failed_prestarts.pop(root_id, None)
        if await self.coordinator.release(ownership):
            self._failed_prestarts.pop(root_id, None)
            self._ownerships.pop(root_id, None)
            renewal = self._renewals.pop(root_id, None)
            if renewal is not None and renewal is not asyncio.current_task():
                renewal.cancel()

    async def _engine_patch_header(
        self,
        agent_id: str,
        root_id: str,
        generation: int,
        sid: str,
        **fields: Any,
    ) -> SessionHeader:
        async with self._lock(root_id):
            if self._turn_generations.get(agent_id) != generation:
                raise asyncio.CancelledError
            return await self._patch_header(sid, **fields)

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
            batch = list(events)
            if any(isinstance(event, AgentFinished) for event in batch):
                state = await self._operational(agent_id)
                cancelled = [TurnCancelled(turn_id=item.command_id, reason="agent_finished") for item in state.pending]
                batch = [*cancelled, *batch]
            return await self._append(agent_id, batch)

    async def _journal(self, agent_id: str, *, store: Any = None) -> list[Stamped]:
        source = self.store if store is None else store
        items: list[Stamped] = []
        after = 0
        while True:
            page = await source.read_page(agent_id, after=after)
            if not page:
                return items
            items.extend(page)
            after = page[-1].seq

    @staticmethod
    def _reduce_operational(journal: Sequence[Stamped]) -> OperationalState:
        state = reduce_journal(journal)
        cancellations: dict[str, list[str]] = {}
        for item in journal:
            if isinstance(item.event, CancellationRequested):
                for actor_id, turns in item.event.targets.items():
                    for turn_id in turns:
                        if turn_id not in cancellations.setdefault(actor_id, []):
                            cancellations[actor_id].append(turn_id)
        return OperationalState(
            pending=state.pending,
            incomplete=state.incomplete,
            finished=Runtime._finished(journal),
            cancellations=cancellations,
            last_seq=journal[-1].seq if journal else 0,
        )

    async def _operational(self, agent_id: str, *, store: Any = None) -> OperationalState:
        if store is None and isinstance(self.coordinator, PostgresCoordinator):
            root_id = self._known_roots.get(agent_id, agent_id)
            ownership = self._ownerships.get(root_id)
            if ownership is None:
                raise LeaseLost(root_id)
            async with self.coordinator.transaction(ownership) as guarded:
                return await self._operational(agent_id, store=guarded)
        source = self.store if store is None else store
        read = getattr(source, "read_operational", None)
        if read is not None:
            return await read(agent_id)
        if getattr(source, "read_page", None) is None:
            source = self.store
        return self._reduce_operational(await self._journal(agent_id, store=source))

    async def _history(self, agent_id: str) -> HistorySnapshot:
        read = getattr(self.store, "read_compacted", None)
        if self.history_mode == "compacted" and read is not None:
            return await read(agent_id)
        items = await self._journal(agent_id)
        last_seq = items[-1].seq if items else 0
        if self.history_mode == "compacted":
            retained = compacted_history([item.event for item in items])
            items = items[len(items) - len(retained) :]
        return HistorySnapshot(items=items, last_seq=last_seq)

    async def _result(self, agent_id: str, command_id: UUID) -> TurnResult | None:
        read = getattr(self.store, "read_turn", None)
        journal = await read(agent_id, command_id.hex) if read is not None else await self._journal(agent_id)
        return _turn_result(UUID(hex=agent_id), command_id, journal) if journal is not None else None

    async def _header(self, agent_id: str) -> SessionHeader:
        header = await self.store.header(agent_id)
        if header is None:
            raise SessionNotFound(agent_id)
        return header

    async def _check_deleted(self, agent_id: str) -> None:
        check = getattr(self.store, "is_deleted", None)
        if check is not None and await check(agent_id):
            raise SessionNotFound(agent_id)

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
        level = [root]
        while level:
            children = []
            for parent in level:
                children.extend(await self._children(parent.id))
            children.sort(key=lambda child: (child.created_at, child.id))
            headers.extend(children)
            level = children
        return headers

    async def _tree_command(self, root_id: str, command_id: str) -> tuple[str, SessionEvent] | None:
        lookup = getattr(self.store, "lookup_command", None)
        if lookup is not None:
            found = await lookup(root_id, command_id)
            return (found[0], found[1].event) if found is not None else None
        for header in await self._tree_headers(root_id):
            for item in await self._journal(header.id):
                event = item.event
                if isinstance(event, InputQueued | AskAnswered | CancellationRequested):
                    if getattr(event, "command_id", None) == command_id:
                        return header.id, event
        return None

    async def _actor_finished(self, agent_id: str) -> AgentFinished | None:
        lookup = getattr(self.store, "lookup_finished", None)
        if lookup is not None:
            found = await lookup(agent_id)
            return found.event if found is not None else None
        return self._finished(await self._journal(agent_id))

    async def _recover_locked(self, root_id: str, ownership: Ownership) -> None:
        assert self.coordinator is not None
        headers = await self._tree_headers(root_id)
        changed: set[str] = set()
        async with self.coordinator.transaction(ownership) as store:
            await store.set_recovery({"generation": ownership.generation, "phase": "running"})
            states = {header.id: await self._operational(header.id, store=store) for header in headers}
            cancelled = states[root_id].cancellations
            for header in headers:
                state = states[header.id]
                live = {item.command_id for item in state.pending}
                if state.incomplete is not None:
                    live.add(state.incomplete.turn_id)
                terminals = [
                    TurnCancelled(turn_id=turn_id, reason="cancelled")
                    for turn_id in cancelled.get(header.id, [])
                    if turn_id in live
                ]
                terminal_ids = {event.turn_id for event in terminals}
                if state.incomplete is not None and state.incomplete.turn_id not in terminal_ids:
                    terminals.append(TurnInterrupted(turn_id=state.incomplete.turn_id, reason="owner_lost"))
                if terminals:
                    await store.append(header.id, terminals)
                    changed.add(header.id)
                await store.set_active(header.id, False)
            await store.set_recovery({"generation": ownership.generation, "phase": "complete"})
        for agent_id in changed:
            await self._notify(agent_id)
        headers = await self._tree_headers(root_id)
        for header in headers:
            self._known_roots[header.id] = root_id
            if header.last_turn is not None:
                await self._deliver_lifecycle_locked(header, header.last_turn)
        for header in headers:
            state = await self._operational(header.id)
            if state.pending and state.finished is None and header.agent in self.agents:
                self._activations[header.id] = self._activations.get(header.id, 0) + 1
                self._activate(header.id, root_id)

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
        self._ensure_started()
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
        created = SessionCreated(
            agent=name,
            root_id=sid,
            parent_id=None,
            depth=0,
            model=resolved,
            metadata=header.metadata,
        )
        async with self._lock(sid):
            self._ensure_open()
            if self.coordinator is None:
                await self.store.create(header)
                await self._append(sid, [created])
                self._known_roots[sid] = sid
            else:
                await self._initialize_root_locked(header, created)
        return public_id

    async def delete(self, root_id: UUID, *, allow_active: bool = False) -> bool:
        return await _shielded(self._accept_delete(root_id, allow_active=allow_active))

    async def _accept_delete(self, root_id: UUID, *, allow_active: bool) -> bool:
        self._ensure_open()
        self._ensure_started()
        _, sid = _id(root_id, "root_id")
        if not isinstance(allow_active, bool):
            raise TypeError("allow_active must be a boolean")
        delete_tree = getattr(self.store, "delete_tree", None)
        if (
            not callable(delete_tree)
            or self.coordinator is not None
            and not isinstance(self.coordinator, PostgresCoordinator)
        ):
            raise NotImplementedError("this store/coordinator does not support session deletion")
        if self.coordinator is not None:
            header = await self.store.header(sid)
            if header is None:
                return False
            try:
                await self._root_header(sid)
                result = await self._request_command(sid, "delete", DeletePayload(allow_active=allow_active), None)
            except SessionNotFound:
                return False
            finally:
                await self._release_delete_ownership(sid)
            return bool(result["deleted"])
        tasks: list[asyncio.Task[None]] = []
        async with self._lock(sid):
            try:
                ids = await delete_tree(
                    sid,
                    allow_active=allow_active,
                    before_delete=lambda ids: tasks.extend(self._begin_deletion(sid, ids, allow_active)),
                )
            except BaseException:
                self._deleting.discard(sid)
                raise
        if ids:
            await self._finish_deletion(sid, ids, tasks)
        return bool(ids)

    async def _acquire_for_deletion(self, root_id: str) -> None:
        assert self.coordinator is not None
        ownership = await self.coordinator.acquire(root_id)
        if ownership is None:
            raise CoordinatorUnavailable(f"root {root_id} is owned by another runtime")
        self._ownerships[root_id] = ownership
        self._unrecovered.add(root_id)
        renewal = self._renewals.pop(root_id, None)
        if renewal is not None:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
        self._renewals[root_id] = asyncio.create_task(self._renew(root_id, ownership))

    async def _release_delete_ownership(self, root_id: str) -> None:
        async with self._lock(root_id):
            if root_id not in self._unrecovered:
                return
            ownership = self._ownerships.get(root_id)
            if ownership is not None:
                assert self.coordinator is not None
                try:
                    async with self.coordinator.transaction(ownership) as store:
                        await store.relinquish_unrecovered()
                except LeaseLost:
                    pass
                finally:
                    renewal = self._renewals.pop(root_id, None)
                    if renewal is not None:
                        renewal.cancel()
                        await asyncio.gather(renewal, return_exceptions=True)
            self._ownerships.pop(root_id, None)
            self._unrecovered.discard(root_id)

    def _begin_deletion(self, root_id: str, ids: list[str], allow_active: bool) -> list[asyncio.Task[None]]:
        tasks = [task for actor_id, task in self.active.items() if actor_id in ids and not task.done()]
        if not allow_active and (
            tasks or any(live.root_id == root_id and not live.future.done() for live in self.asks.values())
        ):
            raise SessionBusy(root_id)
        self._deleting.add(root_id)
        for actor_id in ids:
            self._turn_generations[actor_id] = self._turn_generations.get(actor_id, 0) + 1
            task = self.active.get(actor_id)
            if task is not None and not task.done():
                self._task_reasons[task] = ("interrupted", "session_deleted")
                task.cancel()
        for ask_id, live in list(self.asks.items()):
            if live.root_id == root_id and not live.future.done():
                live.future.set_exception(AskExpired(ask_id))
        return tasks

    async def _finish_deletion(self, root_id: str, ids: list[str], tasks: Sequence[asyncio.Task[None]] = ()) -> None:
        ids = list(set(ids) | {actor for actor, root in self._known_roots.items() if root == root_id})
        self._deleting.add(root_id)
        pending = list(tasks)
        for actor_id in ids:
            task = self.active.get(actor_id)
            if task is not None and not task.done() and task not in pending:
                pending.append(task)
        self._invalidate_root_locked(root_id)
        self._unrecovered.discard(root_id)
        renewal = self._renewals.pop(root_id, None)
        if renewal is not None:
            renewal.cancel()
            pending.append(renewal)
        for connection in list(self._connections.values()):
            if connection._root != root_id:
                continue
            connection._failure = SessionNotFound(root_id)
            connection._writer_token = None
            await self._release_connection(connection)
        await self._notify_root_interests(root_id)
        for actor_id in ids:
            await self._notify(actor_id)
        if pending:
            timeout = float(getattr(self.coordinator, "request_timeout", 10.0))
            done, unfinished = await asyncio.wait(pending, timeout=timeout)
            for task in done:
                _consume(task)
            for task in unfinished:
                task.add_done_callback(_consume)
        self._failed_prestarts.pop(root_id, None)
        self.writers.pop(root_id, None)
        for actor_id in ids:
            self._known_roots.pop(actor_id, None)
            self._errors.pop(actor_id, None)
            self._activations.pop(actor_id, None)
            self._turn_generations.pop(actor_id, None)
        self._deleting.discard(root_id)

    def connect(self, root_id: UUID, *, after: int = 0, writable: bool = False) -> Connection:
        _, sid = _id(root_id, "root_id")
        if not isinstance(after, int):
            raise TypeError("after must be an integer")
        if after < 0:
            raise ValueError("after must be non-negative")
        return Connection(self, root_id, sid, after=after, writable=writable)

    def _status_from_header(self, header: SessionHeader, active: bool | None = None) -> ActorStatus:
        task = self.active.get(header.id)
        return ActorStatus(
            agent_id=UUID(hex=header.id),
            root_id=UUID(hex=header.root_id or header.id),
            parent_id=UUID(hex=header.parent_id) if header.parent_id is not None else None,
            agent=header.agent,
            name=header.name or header.agent,
            state="finished" if header.finished else header.status,
            active=active if active is not None else task is not None and not task.done(),
            current_turn_id=UUID(hex=header.current_turn_id) if header.current_turn_id is not None else None,
            last_turn=header.last_turn,
            last_seq=header.last_seq,
            updated_at=header.updated_at,
        )

    async def status(self, agent_id: UUID) -> ActorStatus:
        self._ensure_started()
        _, sid = _id(agent_id, "agent_id")
        header = await self._header(sid)
        if self.coordinator is None:
            return self._status_from_header(header)
        self._ensure_started()
        return self._status_from_header(header, await self.coordinator.active(header.root_id or header.id, sid))

    async def tree_status(self, root_id: UUID) -> list[ActorStatus]:
        self._ensure_started()
        _, sid = _id(root_id, "root_id")
        headers = await self._tree_headers(sid)
        if self.coordinator is None:
            return [self._status_from_header(header) for header in headers]
        self._ensure_started()
        active = {header.id: await self.coordinator.active(sid, header.id) for header in headers}
        return [self._status_from_header(header, active[header.id]) for header in headers]

    async def events(self, agent_id: UUID, *, after: int = 0) -> AsyncIterator[LoggedEvent]:
        self._ensure_started()
        public_id, sid = _id(agent_id, "agent_id")
        if not isinstance(after, int):
            raise TypeError("after must be an integer")
        if after < 0:
            raise ValueError("after must be non-negative")
        header = await self._header(sid)
        self._known_roots[sid] = header.root_id or header.id
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
        root_id = connection._root if connection is not None else self._known_roots.get(sid, sid)
        self._retain_actor_interest(self._stream_interests, root_id, sid)
        try:
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
                await self._header(sid)
                async with signal.condition:
                    generation = signal.generation
                page = await self.store.read_page(sid, after=cursor)
                if page:
                    continue
                await self._header(sid)
                if connection is not None:
                    connection._check_iteration()
                if self._closing or self._closed:
                    return
                async with signal.condition:
                    if signal.generation == generation and not self._closing and not self._closed:
                        if self.coordinator is None or not self._observation_capable():
                            if self.coordinator is None:
                                await signal.condition.wait()
                            else:
                                interval = float(getattr(self.coordinator, "catch_up_interval", 2.0))
                                try:
                                    await asyncio.wait_for(signal.condition.wait(), interval)
                                except TimeoutError:
                                    pass
                        else:
                            await signal.condition.wait()
        finally:
            await self._release_actor_interest(self._stream_interests, root_id, sid)

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
        if self._closed or self._closing or root_id in self._deleting:
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
            state = await self._operational(agent_id)
            if state.incomplete is None or state.incomplete.turn_id != event.turn_id:
                return False
            await self._append(agent_id, [event])
            return True

    async def _finalize_finished(self, agent_id: str, header: SessionHeader) -> None:
        if not header.finished:
            try:
                await self._patch_header(agent_id, status="finished", pending_ask=None, finished=True)
            except Exception:
                pass
        parent_id = header.parent_id
        if parent_id is not None:
            await self._notify(parent_id)
            self._activations[parent_id] = self._activations.get(parent_id, 0) + 1
            self._activate(parent_id, header.root_id or header.id)

    async def _deliver_lifecycle_locked(self, child: SessionHeader, turn: TurnSummary) -> None:
        queued = _lifecycle_input(child.id, turn)
        if child.parent_id is None or queued is None:
            return
        root_id = child.root_id or child.id
        await self._header(child.parent_id)
        accepted = await self._enqueue(child.parent_id, queued)
        await self._notify(child.parent_id)
        parent_task = self.active.get(child.parent_id)
        if self.coordinator is not None and accepted.duplicate and parent_task is not None and not parent_task.done():
            return
        self._activations[child.parent_id] = self._activations.get(child.parent_id, 0) + 1
        self._activate(child.parent_id, root_id)

    async def _deliver_lifecycle(self, child: SessionHeader, turn: TurnSummary) -> None:
        root_id = child.root_id or child.id
        async with self._lock(root_id):
            await self._deliver_lifecycle_locked(child, turn)

    async def _reconcile_lifecycle(self, header: SessionHeader) -> None:
        if header.last_turn is not None:
            await self._deliver_lifecycle(header, header.last_turn)
            if header.parent_id is not None and _lifecycle_input(header.id, header.last_turn) is not None:
                self._errors.get(header.id, {}).pop(header.last_turn.turn_id.hex, None)
        for child in await self._children(header.id):
            if child.last_turn is not None:
                await self._deliver_lifecycle(child, child.last_turn)
                if _lifecycle_input(child.id, child.last_turn) is not None:
                    self._errors.get(child.id, {}).pop(child.last_turn.turn_id.hex, None)

    def _framework_tools(self, header: SessionHeader, agent: type[Agent]) -> dict[str, Tool]:
        tools = dict(self.tools[header.agent])

        async def spawn(agent_name: str, input: str, ctx: Context, name: str | None = None) -> str:
            return await self._actor_spawn(header, agent, ctx, agent_name, input, name)

        async def send(agent_id: UUID, input: str, ctx: Context) -> dict[str, Any]:
            return await self._actor_send(header, ctx, agent_id, input)

        async def status(agent_id: UUID) -> dict[str, Any]:
            child_status = await self._child_status(header, agent_id)
            return ACTOR_STATUS_ADAPTER.dump_python(child_status, mode="json")

        async def finish(result: Any, ctx: Context) -> FinishResult:
            return await self._actor_finish(header, agent, ctx, result)

        if agent.subagents:
            available = list(dict.fromkeys(agent_name(child) for child in agent.subagents))
            spawn_tool = Tool(
                spawn,
                description=(
                    "Create a declared child agent, queue its input, and return its ID. "
                    "Turn-ended messages contain status only. A finished child's result is delivered later as a new "
                    f"parent input. Available agent types: {', '.join(available)}."
                ),
            )
            spawn_tool.schema.parameters["properties"]["agent_name"]["enum"] = available
            tools["spawn"] = spawn_tool
            tools["status"] = Tool(
                status,
                description="Return durable status for one direct child agent. It returns no child output.",
            )
        if header.parent_id is not None or agent.subagents:
            send_description = "Queue a message for a direct parent or child agent."
            if header.parent_id is not None:
                send_description += f" Your direct parent agent ID is {UUID(hex=header.parent_id)}."
            tools["send"] = Tool(
                send,
                description=send_description,
            )
        if header.parent_id is not None:
            tools["finish"] = Tool(
                finish,
                description="Permanently close this child agent and deliver its result to its parent.",
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
        name: str | None = None,
    ) -> str:
        name = name.strip() or None if name is not None else None
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
        self._known_roots[child_id] = root_id
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
                    name=name,
                    depth=depth,
                    model=model,
                    metadata=dict(root.metadata),
                )
                await self._create_session(child)
            elif (
                child.root_id != root_id
                or child.parent_id != header.id
                or child.agent != requested
                or child.name != name
                or child.depth != depth
            ):
                raise InvalidCommandReuse(child_id)
            child_journal = await self._journal(child_id)
            created = SessionCreated(
                agent=requested,
                name=name,
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
                name=name,
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
            accepted = await self._enqueue(child_id, queued)
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
            self._agent_for(target.agent)
            existing = await self._tree_command(root_id, command.hex)
            if existing is not None:
                if existing != (target_id, queued):
                    raise InvalidCommandReuse(command.hex)
                await self._notify(target_id)
                self._activations[target_id] = self._activations.get(target_id, 0) + 1
                self._activate(target_id, root_id)
                return {"command_id": str(command), "duplicate": True}
            if await self._actor_finished(target_id) is not None:
                raise TantraError(f"agent {target_uuid} is finished")
            accepted = await self._enqueue(target_id, queued)
            await self._notify(target_id)
            self._activations[target_id] = self._activations.get(target_id, 0) + 1
            self._activate(target_id, root_id)
            return {"command_id": str(command), "duplicate": accepted.duplicate}

    async def _child_status(self, header: SessionHeader, child_uuid: UUID) -> ActorStatus:
        child = await self._header(child_uuid.hex)
        if child.parent_id != header.id:
            raise TantraError("status is allowed only for direct child agents")
        if self.coordinator is None:
            return self._status_from_header(child)
        return self._status_from_header(child, await self.coordinator.active(child.root_id or child.id, child.id))

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
            prior = await self._actor_finished(header.id)
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
                if await self._actor_finished(descendant.id) is None:
                    unfinished.append(str(UUID(hex=descendant.id)))
            if unfinished:
                raise TantraError(f"unfinished descendants: {unfinished}")
            parent_id = current.parent_id
            assert parent_id is not None
            await self._header(parent_id)
            parent_input = InputQueued(
                command_id=command.hex,
                input=f"[agent {UUID(hex=header.id)} finished] {encoded}",
            )
            lookup = getattr(self.store, "lookup_command", None)
            if lookup is not None:
                found = await lookup(parent_id, command.hex)
                existing = found[1].event if found is not None and found[0] == parent_id else None
            else:
                existing = next(
                    (
                        item.event
                        for item in await self._journal(parent_id)
                        if isinstance(item.event, InputQueued) and item.event.command_id == command.hex
                    ),
                    None,
                )
            if existing is None:
                if await self._actor_finished(parent_id) is not None:
                    raise TantraError(f"agent {UUID(hex=parent_id)} is finished")
                await self._enqueue(parent_id, parent_input)
            elif existing != parent_input:
                raise InvalidCommandReuse(command.hex)
            return FinishResult(normalized, (AgentFinished(result=normalized),))

    async def _drain(self, agent_id: str, root_id: str, generation: int) -> None:
        task = asyncio.current_task()
        assert task is not None
        current: str | None = None
        turn_generation: int | None = None
        failed = False
        failed_prestart = False
        failed_prestart_unknown = False
        aborted = False
        try:
            async with self._lock(root_id):
                await self._set_active(root_id, agent_id, True)
            while True:
                if self._closed:
                    return
                header = await self._header(agent_id)
                await self._reconcile_lifecycle(header)
                state = await self._operational(agent_id)
                finished = state.finished
                if finished is not None:
                    async with self._lock(root_id):
                        fresh = await self._operational(agent_id)
                        durable = fresh.finished
                        if durable is not None:
                            await self._finalize_finished(agent_id, header)
                        if self.active.get(agent_id) is task:
                            del self.active[agent_id]
                    return
                if state.incomplete is not None:
                    current = state.incomplete.turn_id
                    event = TurnInterrupted(turn_id=current, reason="process_stopped")
                    interrupted = await self._interrupt_if_incomplete(
                        agent_id,
                        event,
                    )
                    if interrupted:
                        await self._deliver_lifecycle(header, _terminal_summary(event))
                    self._errors.get(agent_id, {}).pop(current, None)
                    current = None
                    continue
                if not state.pending:
                    async with self._lock(root_id):
                        state = await self._operational(agent_id)
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
                    fresh = await self._operational(agent_id)
                    if not any(item.command_id == queued.command_id for item in fresh.pending):
                        current = None
                        continue
                    turn_generation = self._turn_generations.get(agent_id, 0) + 1
                    self._turn_generations[agent_id] = turn_generation
                    self._task_turns[task] = current
                history = await self._history(agent_id)
                header = header.model_copy(update={"last_seq": history.last_seq})
                tools = self._framework_tools(header, agent)
                engine = TurnEngine(
                    store=self.store,
                    provider=self.provider,
                    header=header,
                    agent=agent,
                    tools=tools,
                    model=model,
                    history=[item.event for item in history.items],
                    history_mode=self.history_mode,
                    deps=deps,
                    retry=self.retry,
                    hooks=self.hooks,
                    default_permission=self.default_permission,
                    skills_index=skills_index,
                    memory=self.memory,
                    compactor=self.compactor,
                    tracer=self.tracer,
                    ask_future=lambda event: self._register_ask(root_id, agent_id, event),
                    allow_asks=header.parent_id is None,
                    append_events=lambda events, generation=turn_generation: self._engine_append(
                        agent_id, root_id, generation, events
                    ),
                    patch_header=lambda sid, generation=turn_generation, **fields: self._engine_patch_header(
                        agent_id, root_id, generation, sid, **fields
                    ),
                    terminal_tool="finish" if header.parent_id is not None else None,
                )
                terminal = await engine.run(queued)
                self._task_turns.pop(task, None)
                finished = isinstance(terminal, TurnCompleted) and terminal.stop_reason == "finished"
                if finished:
                    self._errors.get(agent_id, {}).pop(current, None)
                    current = None
                    continue
                await self._deliver_lifecycle(header, _terminal_summary(terminal))
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
                    interrupted = await self._interrupt_if_incomplete(agent_id, event)
                    if interrupted:
                        await self._deliver_lifecycle(header, _terminal_summary(event))
                except BaseException as exc:
                    if isinstance(exc, SessionNotFound):
                        return
                    self._errors.setdefault(agent_id, {})[current] = exc
                    await self._notify(agent_id)
        except SessionNotFound:
            return
        except BaseException as exc:
            error: BaseException = exc
            reconciled = False
            try:
                async with self._lock(root_id):
                    header = await self._header(agent_id)
                    state = await self._operational(agent_id)
                    if state.finished is not None:
                        await self._finalize_finished(agent_id, header)
                        reconciled = True
                    else:
                        if current is None and state.incomplete is None and state.pending:
                            current = state.pending[0].command_id
                        if current is not None:
                            pending = any(item.command_id == current for item in state.pending)
                            failed_prestart = state.incomplete is None and pending
            except BaseException as finalize_exc:
                error = finalize_exc
                failed_prestart = True
                failed_prestart_unknown = True
            if reconciled:
                if current is not None:
                    self._errors.get(agent_id, {}).pop(current, None)
                current = None
                return
            failed = True
            if current is not None:
                self._errors.setdefault(agent_id, {})[current] = error
            await self._notify(agent_id)
        finally:
            self._task_turns.pop(task, None)
            async with self._lock(root_id):
                try:
                    await self._set_active(root_id, agent_id, False)
                except Exception:
                    pass
                if self.active.get(agent_id) is task:
                    del self.active[agent_id]
                reactivate = (
                    (failed or aborted)
                    and not self._closed
                    and root_id not in self._deleting
                    and self._activations.get(agent_id, 0) > generation
                )
                if reactivate and await self.store.header(agent_id) is None:
                    reactivate = False
                if reactivate:
                    self._activate(agent_id, root_id)
                if failed_prestart and not reactivate and root_id not in self._deleting:
                    marker = None if failed_prestart_unknown else current
                    self._failed_prestarts.setdefault(root_id, {})[agent_id] = marker
                try:
                    await self._release_if_idle_locked(root_id)
                except Exception:
                    pass

    async def _request_command(
        self,
        root_id: str,
        operation: str,
        payload: Any,
        writer_token: WriterToken | None,
    ) -> dict[str, Any]:
        self._ensure_started()
        assert self.coordinator is not None
        if await self.coordinator.locate(root_id) is None:
            async with self._lock(root_id):
                if await self.coordinator.locate(root_id) is None:
                    if operation == "delete":
                        await self._acquire_for_deletion(root_id)
                    else:
                        await self._ensure_owner_locked(root_id)
        timeout = float(getattr(self.coordinator, "request_timeout", 10.0))
        envelope = CommandEnvelope(
            request_id=uuid4(),
            root_id=root_id,
            operation=operation,
            writer_token=writer_token,
            payload=payload,
            deadline=datetime.now(UTC) + timedelta(seconds=timeout),
        )
        try:
            while True:
                try:
                    reply = await self.coordinator.request(envelope)
                    break
                except CoordinatorUnavailable:
                    async with self._lock(root_id):
                        if await self.coordinator.locate(root_id) is not None:
                            if operation == "delete":
                                continue
                            raise
                        if operation == "delete":
                            await self._acquire_for_deletion(root_id)
                        else:
                            await self._ensure_owner_locked(root_id)
        except CommandTimeout as exc:
            command_id = getattr(payload, "command_id", None)
            raise CommandTimeout(str(command_id or envelope.request_id), command_id=command_id) from exc
        if reply.status == "ok":
            return reply.result or {}
        errors: dict[str, type[TantraError]] = {
            "ask_expired": AskExpired,
            "invalid_command_reuse": InvalidCommandReuse,
            "writer_replaced": WriterReplaced,
            "writer_required": WriterRequired,
            "session_busy": SessionBusy,
            "session_not_found": SessionNotFound,
        }
        error = errors.get(reply.error_code or "")
        if error is not None:
            raise error(reply.message or reply.error_code or operation)
        message = reply.message or reply.error_code or "remote command failed"
        if reply.retryable:
            raise CoordinatorUnavailable(message)
        raise RemoteExecutionError(message)

    async def _handle_request(self, envelope: CommandEnvelope, transact: Any) -> None:
        self._ensure_open()
        if isinstance(envelope.payload, DeletePayload):
            await self._handle_delete(envelope, transact)
            return
        after: list[Callable[[], None]] = []
        async with self._lock(envelope.root_id):
            self._ensure_open()
            if envelope.root_id in self._unrecovered:
                ownership = self._ownerships.get(envelope.root_id)
                if ownership is None:
                    raise CoordinatorUnavailable(f"root {envelope.root_id} has no recovered owner")
                if envelope.root_id not in self._renewals:
                    self._renewals[envelope.root_id] = asyncio.create_task(self._renew(envelope.root_id, ownership))
                await self._recover_locked(envelope.root_id, ownership)
                self._unrecovered.discard(envelope.root_id)
            headers: list[SessionHeader] = []
            if isinstance(envelope.payload, CancelPayload) or (
                isinstance(envelope.payload, SendPayload | AnswerPayload)
                and (
                    not isinstance(self.coordinator, PostgresCoordinator)
                    or getattr(self.store, "lookup_command", None) is None
                    or isinstance(envelope.payload, SendPayload)
                    and getattr(self.store, "lookup_finished", None) is None
                )
            ):
                headers = await self._tree_headers(envelope.root_id)
            fallback_journals = (
                {header.id: await self._journal(header.id) for header in headers}
                if not isinstance(self.coordinator, PostgresCoordinator)
                else {}
            )

            async def apply(request: CommandEnvelope, store: CoordinatedStoreProtocol) -> CommandReply:
                journals = fallback_journals
                states: dict[str, OperationalState] = {}

                async def find_command(command_id: str) -> tuple[str, SessionEvent] | None:
                    lookup = getattr(store, "lookup_command", None)
                    if lookup is None:
                        return self._find_tree_command(journals, command_id)
                    found = await lookup(request.root_id, command_id)
                    return (found[0], found[1].event) if found is not None else None

                try:
                    payload = request.payload
                    result: dict[str, Any]
                    if isinstance(payload, ClaimWriterPayload):
                        token = await store.claim_writer(payload.connection_id)
                        result = {"writer_token": token.model_dump(mode="json")}
                        writer_change_id = getattr(store, "writer_change_id", None)
                        if writer_change_id is not None:
                            result["writer_change_id"] = writer_change_id
                    else:
                        assert request.writer_token is not None
                        await store.validate_writer(request.writer_token)
                        if isinstance(payload, CancelPayload):
                            states = {header.id: await self._operational(header.id, store=store) for header in headers}
                        elif isinstance(self.coordinator, PostgresCoordinator):
                            journals = {header.id: await self._journal(header.id, store=store) for header in headers}
                        if isinstance(payload, ReleaseWriterPayload):
                            result = {"released": await store.release_writer(request.writer_token)}
                        elif isinstance(payload, SendPayload):
                            event = InputQueued(command_id=payload.command_id.hex, input=payload.input)
                            existing = await find_command(payload.command_id.hex)
                            if existing is not None:
                                if existing != (request.root_id, event):
                                    raise InvalidCommandReuse(payload.command_id.hex)
                                duplicate = True
                            else:
                                lookup = getattr(store, "lookup_finished", None)
                                finished = (
                                    await lookup(request.root_id)
                                    if lookup is not None
                                    else self._finished(journals[request.root_id])
                                )
                                if finished is not None:
                                    raise TantraError(f"agent {UUID(hex=request.root_id)} is finished")
                                duplicate = (await store.enqueue(request.root_id, event)).duplicate
                            after.append(lambda: self._accepted_input(request.root_id))
                            result = {"command_id": str(payload.command_id), "duplicate": duplicate}
                        elif isinstance(payload, AnswerPayload):
                            event = AskAnswered(
                                ask_id=payload.ask_id.hex,
                                response=payload.response,
                                command_id=payload.command_id.hex,
                                answered_by=request.root_id,
                            )
                            existing = await find_command(payload.command_id.hex)
                            if existing is not None:
                                previous = existing[1]
                                if (
                                    not isinstance(previous, AskAnswered)
                                    or previous.model_copy(update={"answered_by": request.root_id}) != event
                                ):
                                    raise InvalidCommandReuse(payload.command_id.hex)
                                duplicate = True
                            else:
                                live = self.asks.get(payload.ask_id.hex)
                                if live is None or live.root_id != request.root_id or live.future.done():
                                    raise AskExpired(payload.ask_id.hex)
                                if live.event.request.kind != payload.response.kind:
                                    raise TantraError(
                                        f"ask {payload.ask_id.hex!r} needs a {live.event.request.kind!r} response, "
                                        f"got {payload.response.kind!r}"
                                    )
                                if live.event.request.extra.get("permission") and not isinstance(
                                    payload.response, ApprovalResponse
                                ):
                                    message = (
                                        f"ask {payload.ask_id.hex!r} is a permission request and needs "
                                        "an ApprovalResponse"
                                    )
                                    raise TantraError(message)
                                await store.append(live.agent_id, [event])
                                after.append(
                                    lambda live=live, response=payload.response: live.future.set_result(response)
                                )
                                duplicate = False
                            result = {"command_id": str(payload.command_id), "duplicate": duplicate}
                        elif isinstance(payload, CancelPayload):
                            existing = await find_command(payload.command_id.hex)
                            if existing is not None:
                                if existing[0] != request.root_id or not isinstance(existing[1], CancellationRequested):
                                    raise InvalidCommandReuse(payload.command_id.hex)
                                cancellation = existing[1]
                                duplicate = True
                            else:
                                targets = self._operational_targets(states)
                                cancellation = CancellationRequested(command_id=payload.command_id.hex, targets=targets)
                                await store.append(request.root_id, [cancellation])
                                duplicate = False
                            for actor_id, turn_ids in cancellation.targets.items():
                                state = states[actor_id]
                                live_turns = {item.command_id for item in state.pending}
                                if state.incomplete is not None:
                                    live_turns.add(state.incomplete.turn_id)
                                terminals = [
                                    TurnCancelled(turn_id=turn_id, reason="cancelled")
                                    for turn_id in turn_ids
                                    if turn_id in live_turns
                                ]
                                if terminals:
                                    await store.append(actor_id, terminals)
                            after.append(lambda cancellation=cancellation: self._cancel_local(cancellation))
                            result = {"command_id": str(payload.command_id), "duplicate": duplicate}
                        else:
                            raise TantraError(f"unsupported coordinator operation {request.operation!r}")
                    return CommandReply(request_id=request.request_id, result=result)
                except TantraError as exc:
                    code = type(exc).__name__
                    code = "".join(("_" + char.lower()) if char.isupper() else char for char in code).lstrip("_")
                    return CommandReply(
                        request_id=request.request_id,
                        status="error",
                        error_code=code,
                        message=str(exc),
                    )

            reply = await transact(apply)
        if reply is not None and reply.status == "ok":
            for effect in after:
                effect()
        async with self._lock(envelope.root_id):
            await self._release_if_idle_locked(envelope.root_id)

    async def _handle_delete(self, envelope: CommandEnvelope, transact: Any) -> None:
        ids: list[str] = []
        tasks: list[asyncio.Task[None]] = []

        async def apply(request: CommandEnvelope, store: Any) -> CommandReply:
            nonlocal ids
            payload = request.payload
            assert isinstance(payload, DeletePayload)
            store.delete_request_id = request.request_id
            try:
                ids = await store.delete_tree(
                    request.root_id,
                    allow_active=payload.allow_active,
                    before_delete=lambda actors: tasks.extend(
                        self._begin_deletion(request.root_id, actors, payload.allow_active)
                    ),
                )
                return CommandReply(request_id=request.request_id, result={"deleted": bool(ids)})
            except SessionBusy:
                return CommandReply(
                    request_id=request.request_id, status="error", error_code="session_busy", message=request.root_id
                )

        try:
            async with self._lock(envelope.root_id):
                reply = await transact(apply)
        except BaseException:
            self._deleting.discard(envelope.root_id)
            if ids and await self.store.is_deleted(envelope.root_id):
                await self._finish_deletion(envelope.root_id, ids, tasks)
            raise
        if reply is not None and reply.status == "ok" and ids:
            await self._finish_deletion(envelope.root_id, ids, tasks)
        else:
            self._deleting.discard(envelope.root_id)
            async with self._lock(envelope.root_id):
                await self._release_if_idle_locked(envelope.root_id)

    @staticmethod
    def _find_tree_command(
        journals: Mapping[str, Sequence[Stamped]], command_id: str
    ) -> tuple[str, SessionEvent] | None:
        for actor_id, journal in journals.items():
            for item in journal:
                event = item.event
                if (
                    isinstance(event, InputQueued | AskAnswered | CancellationRequested)
                    and getattr(event, "command_id", None) == command_id
                ):
                    return actor_id, event
        return None

    @staticmethod
    def _operational_targets(states: Mapping[str, OperationalState]) -> dict[str, list[str]]:
        targets: dict[str, list[str]] = {}
        for actor_id, state in states.items():
            if state.finished is not None:
                continue
            turns = [item.command_id for item in state.pending]
            if state.incomplete is not None and state.incomplete.turn_id not in turns:
                turns.append(state.incomplete.turn_id)
            if turns:
                targets[actor_id] = turns
        return targets

    def _accepted_input(self, root_id: str) -> None:
        self._activations[root_id] = self._activations.get(root_id, 0) + 1
        self._activate(root_id, root_id)
        asyncio.create_task(self._notify(root_id))

    def _cancel_local(self, cancellation: CancellationRequested) -> None:
        for actor_id, turn_ids in cancellation.targets.items():
            task = self.active.get(actor_id)
            if task is None or task.done():
                continue
            task_turn = self._task_turns.get(task)
            if task_turn is not None and task_turn not in turn_ids:
                continue
            self._turn_generations[actor_id] = self._turn_generations.get(actor_id, 0) + 1
            self._task_reasons[task] = ("cancelled", "cancelled")
            if self.active.get(actor_id) is task:
                del self.active[actor_id]
            task.cancel()

    async def _claim(self, connection: Connection) -> None:
        self._ensure_open()
        self._ensure_started()
        header = await self._root_header(connection._root)
        self._known_roots[connection._root] = connection._root
        self._retain_connection_interest(connection._root)
        connection._watching = True
        try:
            if self.coordinator is not None:
                if connection.writable:
                    self._agent_for(header.agent)
                    if await self.coordinator.locate(connection._root) is None:
                        async with self._lock(connection._root):
                            if await self.coordinator.locate(connection._root) is None:
                                await self._ensure_owner_locked(connection._root)
                    result = await self._request_command(
                        connection._root,
                        "claim_writer",
                        ClaimWriterPayload(connection_id=connection._connection_id),
                        None,
                    )
                    connection._writer_token = WriterToken.model_validate(result["writer_token"])
                    connection._writer_change_id = result.get("writer_change_id")
                    self._connections[connection._connection_id] = connection
                    observation = self._current_observation(connection._root)
                    if observation is not None:
                        await self._refresh_writers_from_observation(connection._root, observation)
                    if connection._writer_token is None:
                        raise WriterReplaced(f"writer for {connection.root_id} was replaced")
                connection._entered = True
                self._connections[connection._connection_id] = connection
                await self._root_header(connection._root)
                observation = self._current_observation(connection._root)
                if observation is not None and getattr(observation, "deleted", False):
                    raise SessionNotFound(connection._root)
                return
            if connection.writable:
                self._agent_for(header.agent)
                async with self._lock(connection._root):
                    self._ensure_open()
                    await self._root_header(connection._root)
                    generation = self.writers.get(connection._root, 0) + 1
                    self.writers[connection._root] = generation
                    connection._generation = generation
                    connection._entered = True
                    self._connections[connection._connection_id] = connection
                await self._notify(connection._root)
                return
            async with self._lock(connection._root):
                self._ensure_open()
                await self._root_header(connection._root)
                connection._entered = True
                self._connections[connection._connection_id] = connection
        except BaseException:
            if connection._connection_id in self._connections:
                try:
                    await self._release_connection(connection)
                except BaseException:
                    pass
            else:
                connection._watching = False
                await self._release_connection_interest(connection._root)
            raise

    async def _release_connection(self, connection: Connection) -> None:
        try:
            self._connections.pop(connection._connection_id, None)
            if self.coordinator is None or connection._writer_token is None:
                return
            token = connection._writer_token
            connection._writer_token = None
            connection._writer_change_id = None
            if self._closed or getattr(self.coordinator, "_closed", False):
                return
            try:
                await self._request_command(
                    connection._root,
                    "release_writer",
                    ReleaseWriterPayload(),
                    token,
                )
            except WriterReplaced:
                pass
        finally:
            if connection._watching:
                connection._watching = False
                await self._release_connection_interest(connection._root)

    def _check_writer(self, connection: Connection) -> None:
        if connection._failure is not None:
            raise connection._failure
        if not connection._entered or not connection.writable:
            raise WriterRequired("an active writable connection is required")
        if self.coordinator is not None:
            if connection._writer_token is None:
                raise WriterRequired("an active writable connection is required")
            self._ensure_open()
            return
        if self.writers.get(connection._root) != connection._generation:
            raise WriterReplaced(f"writer for {connection.root_id} was replaced")
        self._ensure_open()

    async def _send(self, connection: Connection, input: str, command_id: UUID) -> CommandReceipt:
        return await _shielded(self._accept_send(connection, input, command_id))

    async def _accept_send(self, connection: Connection, input: str, command_id: UUID) -> CommandReceipt:
        public_id, cid = _id(command_id, "command_id")
        if self.coordinator is not None:
            self._check_writer(connection)
            result = await self._request_command(
                connection._root,
                "send",
                SendPayload(command_id=public_id, input=input),
                connection._writer_token,
            )
            return CommandReceipt(public_id, bool(result["duplicate"]))
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
                if await self._actor_finished(connection._root) is not None:
                    raise TantraError(f"agent {connection.root_id} is finished")
                accepted = await self._enqueue(connection._root, event)
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
        if self.coordinator is not None:
            self._check_writer(connection)
            result = await self._request_command(
                connection._root,
                "answer",
                AnswerPayload(command_id=public_command, ask_id=ask_id, response=response),
                connection._writer_token,
            )
            return CommandReceipt(public_command, bool(result["duplicate"]))
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
        if self.coordinator is not None:
            self._check_writer(connection)
            result = await self._request_command(
                connection._root,
                "cancel",
                CancelPayload(command_id=public_id),
                connection._writer_token,
            )
            return CommandReceipt(public_id, bool(result["duplicate"]))
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
                    state = await self._operational(agent_id)
                    if state.finished is not None:
                        continue
                    turn_ids = [item.command_id for item in state.pending]
                    if state.incomplete is not None and state.incomplete.turn_id not in turn_ids:
                        turn_ids.append(state.incomplete.turn_id)
                    if turn_ids:
                        targets[agent_id] = turn_ids
                event = CancellationRequested(command_id=cid, targets=targets)
                await self._append(connection._root, [event])
                duplicate = False
            for agent_id, target_turns in event.targets.items():
                state = await self._operational(agent_id)
                if state.finished is not None:
                    continue
                header = await self._header(agent_id)
                if agent_id != connection._root and header.last_turn is not None:
                    await self._deliver_lifecycle_locked(header, header.last_turn)
                    if _lifecycle_input(header.id, header.last_turn) is not None:
                        self._errors.get(header.id, {}).pop(header.last_turn.turn_id.hex, None)
                live_turns = {item.command_id for item in state.pending}
                if state.incomplete is not None:
                    live_turns.add(state.incomplete.turn_id)
                task = self.active.get(agent_id)
                task_turn = self._task_turns.get(task) if task is not None else None
                cancel_task = (
                    task is not None
                    and not task.done()
                    and (task_turn in target_turns or task_turn is None and bool(live_turns & set(target_turns)))
                )
                for turn_id in target_turns:
                    if turn_id not in live_turns:
                        continue
                    terminal = TurnCancelled(turn_id=turn_id, reason="cancelled")
                    await self._append(agent_id, [terminal])
                    if cancel_task and (task_turn is None or task_turn == turn_id):
                        assert task is not None
                        self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
                        self._task_reasons[task] = ("cancelled", "cancelled")
                        if self.active.get(agent_id) is task:
                            del self.active[agent_id]
                        task.cancel()
                        cancel_task = False
                    if agent_id != connection._root:
                        await self._deliver_lifecycle_locked(header, _terminal_summary(terminal))
                        self._errors.get(agent_id, {}).pop(turn_id, None)
                if cancel_task:
                    assert task is not None
                    self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
                    self._task_reasons[task] = ("cancelled", "cancelled")
                    if self.active.get(agent_id) is task:
                        del self.active[agent_id]
                    task.cancel()
            return CommandReceipt(command_id=public_id, duplicate=duplicate)

    async def _wait_result(self, agent_id: str, command_id: UUID) -> TurnResult:
        root_id = self._known_roots.get(agent_id, agent_id)
        self._retain_actor_interest(self._wait_interests, root_id, agent_id)
        try:
            if self.coordinator is not None and self._observation_capable():
                return await self._wait_observed_result(root_id, agent_id, command_id)
            return await self._wait_polled_result(root_id, agent_id, command_id)
        finally:
            await self._release_actor_interest(self._wait_interests, root_id, agent_id)

    async def _wait_observed_result(self, root_id: str, agent_id: str, command_id: UUID) -> TurnResult:
        await self._check_deleted(agent_id)
        cid = command_id.hex
        signal = self._wait_signal(agent_id)
        inactive = 0
        observation = self._current_observation(root_id)
        seen_observation = observation is not None
        last_seq = observation.actors.get(agent_id, (None, False))[0] if observation is not None else None
        last_sample = observation.sample if observation is not None else None
        result = await self._result(agent_id, command_id)
        error = self._errors.get(agent_id, {}).get(cid)
        if error is not None and result is None:
            raise error
        if result is not None:
            return result
        while True:
            async with signal.condition:
                generation = signal.generation
            if self._closing:
                await self._close_complete.wait()
            if self._closed:
                result = await self._result(agent_id, command_id)
                error = self._errors.get(agent_id, {}).get(cid)
                if error is not None and result is None:
                    raise error
                if result is not None:
                    return result
                raise TantraError(f"runtime closed before command {command_id} finished")
            observation = self._current_observation(root_id)
            observed_error = self._observation_errors.get(root_id)
            if observed_error is not None:
                if not self._closing and not self._closed:
                    raise observed_error
                await self._close_complete.wait()
                continue
            if observation is not None:
                if getattr(observation, "deleted", False):
                    raise SessionNotFound(agent_id)
                if observation.error is not None:
                    if not self._closing and not self._closed:
                        raise observation.error
                    await self._close_complete.wait()
                    continue
                actor = observation.actors.get(agent_id)
                seq = actor[0] if actor is not None else None
                if not seen_observation or seq != last_seq:
                    seen_observation = True
                    last_seq = seq
                    result = await self._result(agent_id, command_id)
                    error = self._errors.get(agent_id, {}).get(cid)
                    if error is not None and result is None:
                        raise error
                    if result is not None:
                        return result
                active = actor is not None and actor[1]
                owner = observation.owner_instance is not None and observation.owner_valid
                recovered = (
                    owner
                    and observation.recovery.get("generation") == observation.owner_generation
                    and observation.recovery.get("phase") == "complete"
                )
                if active or owner and not recovered:
                    inactive = 0
                if observation.sample > 0 and observation.sample != last_sample:
                    last_sample = observation.sample
                    if not active and (not owner or recovered):
                        inactive += 1
                        if inactive >= 2:
                            raise RemoteExecutionError(f"command {command_id} has no active owner execution")
            error = self._errors.get(agent_id, {}).get(cid)
            if error is not None:
                raise error
            async with signal.condition:
                if signal.generation == generation and not self._closed:
                    await signal.condition.wait()

    async def _wait_polled_result(self, root_id: str, agent_id: str, command_id: UUID) -> TurnResult:
        cid = command_id.hex
        signal = self._wait_signal(agent_id)
        inactive = 0
        while True:
            await self._check_deleted(agent_id)
            result = await self._result(agent_id, command_id)
            error = self._errors.get(agent_id, {}).get(cid)
            if error is not None and (self.coordinator is None or result is None):
                raise error
            if result is not None:
                return result
            if self._closing:
                await self._close_complete.wait()
                continue
            if self._closed:
                result = await self._result(agent_id, command_id)
                error = self._errors.get(agent_id, {}).get(cid)
                if error is not None and (self.coordinator is None or result is None):
                    raise error
                if result is not None:
                    return result
                raise TantraError(f"runtime closed before command {command_id} finished")
            if self.coordinator is not None:
                try:
                    active = await self.coordinator.active(root_id, agent_id)
                    owner = await self.coordinator.locate(root_id)
                    recovered = False
                    if owner is not None and not active:
                        recovery = await self.coordinator.recovery(root_id)
                        recovered = (
                            recovery.get("generation") == owner.generation and recovery.get("phase") == "complete"
                        )
                except CoordinatorUnavailable:
                    if not self._closing and not self._closed:
                        raise
                    await self._close_complete.wait()
                    continue
                if self._closing or self._closed:
                    continue
                if active or owner is not None and not recovered:
                    inactive = 0
                else:
                    inactive += 1
                    if inactive >= 2:
                        raise RemoteExecutionError(f"command {command_id} has no active owner execution")
            async with signal.condition:
                generation = signal.generation
            result = await self._result(agent_id, command_id)
            error = self._errors.get(agent_id, {}).get(cid)
            if error is not None and (self.coordinator is None or result is None):
                raise error
            if result is not None:
                return result
            async with signal.condition:
                if signal.generation == generation and not self._closed:
                    if self.coordinator is None:
                        await signal.condition.wait()
                    else:
                        interval = float(getattr(self.coordinator, "catch_up_interval", 2.0))
                        try:
                            await asyncio.wait_for(signal.condition.wait(), interval)
                        except TimeoutError:
                            pass

    async def _close_coordinated(self) -> None:
        assert self.coordinator is not None
        tasks = [task for task in self.active.values() if not task.done()]
        for root_id in set(self._connection_interests) | set(self._stream_interests) | set(self._wait_interests):
            await self._notify_root_interests(root_id)
        control_tasks = list(self._watchers.values())
        if self._maintenance_task is not None:
            control_tasks.append(self._maintenance_task)
        for task in control_tasks:
            task.cancel()
        await asyncio.gather(*control_tasks, return_exceptions=True)
        for agent_id, task in list(self.active.items()):
            if task.done():
                continue
            self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
            self._task_reasons[task] = ("interrupted", "runtime_closed")
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        first_error: BaseException | None = None
        for root_id, ownership in list(self._ownerships.items()):
            async with self._lock(root_id):
                headers = await self._tree_headers(root_id)
                for agent_id, task in list(self.active.items()):
                    if task.done() or self._known_roots.get(agent_id) != root_id:
                        continue
                    self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
                    self._task_reasons[task] = ("interrupted", "runtime_closed")
                try:
                    async with self.coordinator.transaction(ownership) as store:
                        for header in headers:
                            state = await self._operational(header.id, store=store)
                            if state.incomplete is not None:
                                await store.append(
                                    header.id,
                                    [TurnInterrupted(turn_id=state.incomplete.turn_id, reason="runtime_closed")],
                                )
                            await store.set_active(header.id, False)
                except LeaseLost:
                    pass
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
                for header in headers:
                    await self._notify(header.id)
                self.active = {
                    agent_id: task
                    for agent_id, task in self.active.items()
                    if self._known_roots.get(agent_id) != root_id
                }
                try:
                    await self.coordinator.release(ownership)
                except LeaseLost:
                    pass
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
                self._ownerships.pop(root_id, None)
        for task in tasks:
            task.cancel()
        for ask_id, live in list(self.asks.items()):
            if not live.future.done():
                live.future.set_exception(AskExpired(ask_id))
        renewal_tasks = list(self._renewals.values())
        for task in renewal_tasks:
            task.cancel()
        await asyncio.gather(*renewal_tasks, return_exceptions=True)
        self._renewals.clear()
        self._watchers.clear()
        self._connection_interests.clear()
        self._stream_interests.clear()
        self._wait_interests.clear()
        self._observations.clear()
        self._observation_errors.clear()
        self._connections.clear()
        self._maintenance_task = None
        await self.coordinator.close()
        if first_error is not None:
            raise first_error

    async def aclose(self) -> None:
        if self._closed or self._closing:
            return
        if self.coordinator is not None:
            self._closing = True
            try:
                await self._close_coordinated()
            finally:
                self._closed = True
                self._closing = False
                self._close_complete.set()
            return
        self._closing = True
        self._closed = True
        try:
            await self._close_local()
        finally:
            self._closing = False
            self._close_complete.set()

    async def _close_local(self) -> None:
        tasks = [task for task in self.active.values() if not task.done()]
        first_error: BaseException | None = None
        for root_id in set(self._known_roots.values()) | set(self.writers):
            async with self._lock(root_id):
                self.writers[root_id] = self.writers.get(root_id, 0) + 1
                for agent_id, task in list(self.active.items()):
                    if task.done() or self._known_roots.get(agent_id) != root_id:
                        continue
                    self._turn_generations[agent_id] = self._turn_generations.get(agent_id, 0) + 1
                    self._task_reasons[task] = ("interrupted", "runtime_closed")
                    header = None
                    last_turn = None
                    if agent_id != root_id:
                        try:
                            header = await self._header(agent_id)
                            last_turn = header.last_turn
                            if last_turn is not None and _lifecycle_input(header.id, last_turn) is not None:
                                await self._deliver_lifecycle_locked(header, last_turn)
                                self._errors.get(agent_id, {}).pop(last_turn.turn_id.hex, None)
                        except BaseException as exc:
                            if first_error is None:
                                first_error = exc
                            if last_turn is not None:
                                self._errors.setdefault(agent_id, {})[last_turn.turn_id.hex] = exc
                    state = None
                    try:
                        state = await self._operational(agent_id)
                        if state.incomplete is not None:
                            event = TurnInterrupted(turn_id=state.incomplete.turn_id, reason="runtime_closed")
                            await self._append(agent_id, [event])
                            if agent_id != root_id:
                                if header is None:
                                    header = await self._header(agent_id)
                                await self._deliver_lifecycle_locked(header, _terminal_summary(event))
                    except BaseException as exc:
                        if first_error is None:
                            first_error = exc
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
        if first_error is not None:
            raise first_error


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
        self._connection_id = uuid4()
        self._writer_token: WriterToken | None = None
        self._writer_change_id: int | None = None
        self._generation: int | None = None
        self._entered = False
        self._watching = False
        self._iterator: AsyncIterator[LoggedEvent] | None = None
        self._failure: SessionNotFound | None = None

    async def __aenter__(self) -> Connection:
        if self._entered:
            raise TantraError("connection is already entered")
        await self.runtime._claim(self)
        return self

    async def __aexit__(self, *_args: Any) -> None:
        try:
            await self.runtime._release_connection(self)
        finally:
            self._entered = False
            iterator = self._iterator
            self._iterator = None
            if iterator is not None:
                await iterator.aclose()

    def _check_iteration(self) -> None:
        if self._failure is not None:
            raise self._failure
        if not self._entered:
            raise WriterRequired("connection must be entered before use")
        if self.writable and self.runtime.coordinator is not None and self._writer_token is None:
            raise WriterReplaced(f"writer for {self.root_id} was replaced")
        if (
            self.writable
            and self.runtime.coordinator is None
            and self.runtime.writers.get(self._root) != self._generation
        ):
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
