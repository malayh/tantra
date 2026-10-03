from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Annotated, Any, Literal, Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from tantra.ask import AskResponse
from tantra.errors import (
    CommandTimeout,
    CoordinatorUnavailable,
    InvalidCommandReuse,
    LeaseLost,
    ModelChangeBusy,
    RemoteExecutionError,
    SessionExists,
    SessionNotFound,
    TantraError,
    WriterReplaced,
)
from tantra.events import InputQueued, SessionEvent, SessionHeader, SessionStatus, Stamped, Usage
from tantra.stores.base import UNSET, EnqueueResult, HistorySnapshot, OperationalState, apply_patch, reduce_header
from tantra.stores.postgres import PostgresStore, _event_json, _hydrate, _json, _parse

try:
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb
except ImportError:
    psycopg = None
    sql = None
    Jsonb = None

DEFAULT_LEASE_TTL = 60.0
DEFAULT_REQUEST_TIMEOUT = 10.0
DEFAULT_CATCH_UP_INTERVAL = 2.0
RETENTION = timedelta(hours=24)


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class Ownership(FrozenModel):
    root_id: str
    instance_id: UUID
    generation: int
    expires_at: datetime


class WriterToken(FrozenModel):
    root_id: str
    connection_id: UUID


class ClaimWriterPayload(FrozenModel):
    type: Literal["claim_writer"] = "claim_writer"
    connection_id: UUID


class ReleaseWriterPayload(FrozenModel):
    type: Literal["release_writer"] = "release_writer"


class SendPayload(FrozenModel):
    type: Literal["send"] = "send"
    command_id: UUID
    input: str


class AnswerPayload(FrozenModel):
    type: Literal["answer"] = "answer"
    command_id: UUID
    ask_id: UUID
    response: AskResponse


class CancelPayload(FrozenModel):
    type: Literal["cancel"] = "cancel"
    command_id: UUID


class DeletePayload(FrozenModel):
    type: Literal["delete"] = "delete"
    allow_active: bool = False


CommandPayload = Annotated[
    ClaimWriterPayload | ReleaseWriterPayload | SendPayload | AnswerPayload | CancelPayload | DeletePayload,
    Field(discriminator="type"),
]
CommandOperation = Literal["claim_writer", "release_writer", "send", "answer", "cancel", "delete"]


class CommandEnvelope(FrozenModel):
    version: Literal[1] = 1
    request_id: UUID
    root_id: str
    operation: CommandOperation
    writer_token: WriterToken | None = None
    payload: CommandPayload
    deadline: datetime

    @model_validator(mode="after")
    def validate_operation(self) -> CommandEnvelope:
        if self.operation != self.payload.type:
            raise ValueError("operation does not match payload")
        if self.operation in ("claim_writer", "delete") and self.writer_token is not None:
            raise ValueError(f"{self.operation} cannot include a writer token")
        if self.operation not in ("claim_writer", "delete") and self.writer_token is None:
            raise ValueError(f"{self.operation} requires a writer token")
        if self.writer_token is not None and self.writer_token.root_id != self.root_id:
            raise ValueError("writer token belongs to another root")
        if self.deadline.tzinfo is None:
            raise ValueError("deadline must be timezone-aware")
        return self


class CommandReply(FrozenModel):
    version: Literal[1] = 1
    request_id: UUID
    status: Literal["ok", "error", "ownership_changed"] = "ok"
    result: dict[str, Any] | None = None
    error_code: str | None = None
    message: str | None = None
    retryable: bool = False

    def unwrap(self) -> dict[str, Any]:
        if self.status != "ok":
            raise RemoteExecutionError(self.message or self.error_code or self.status)
        return self.result or {}


class ChangeNotice(FrozenModel):
    version: Literal[1] = 1
    change_id: int
    root_id: str
    kind: Literal["journal", "header", "ownership", "writer", "activity", "recovery", "request", "deleted"]
    actor_id: str | None = None
    seq: int | None = None


@dataclass(frozen=True)
class RootObservation:
    root_id: str
    sample: int
    change_id: int
    writer_connection: str | None
    owner_instance: str | None
    owner_generation: int
    owner_valid: bool
    recovery: dict[str, Any]
    actors: dict[str, tuple[int, bool]]
    expires_at: datetime | None = None
    error: CoordinatorUnavailable | None = None
    deleted: bool = False


@dataclass
class _Observation:
    users: int = 0
    generation: int = 0
    snapshot: RootObservation | None = None
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)


class CoordinatedStoreProtocol(Protocol):
    ownership: Ownership

    async def create(self, header: SessionHeader) -> None: ...

    async def header(self, sid: str) -> SessionHeader | None: ...

    async def put_header(self, header: SessionHeader) -> None: ...

    async def patch_header(self, sid: str, **fields: Any) -> SessionHeader: ...

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int: ...

    async def enqueue(self, sid: str, event: InputQueued) -> EnqueueResult: ...

    async def claim_writer(self, connection_id: UUID) -> WriterToken: ...

    async def release_writer(self, token: WriterToken) -> bool: ...

    async def validate_writer(self, token: WriterToken) -> None: ...

    async def set_active(self, actor_id: str, active: bool) -> None: ...

    async def recovery(self) -> dict[str, Any]: ...

    async def set_recovery(self, state: dict[str, Any]) -> None: ...

    async def lock_request(self, request_id: UUID) -> CommandEnvelope | None: ...

    async def reply(self, reply: CommandReply) -> None: ...


RequestOperation = Callable[[CommandEnvelope, CoordinatedStoreProtocol], Awaitable[CommandReply]]
RequestTransaction = Callable[[RequestOperation], Awaitable[CommandReply | None]]
RequestHandler = Callable[[CommandEnvelope, RequestTransaction], Awaitable[None]]


class Coordinator(Protocol):
    async def start(self, handler: RequestHandler | None = None) -> None: ...

    async def close(self) -> None: ...

    async def locate(self, root_id: str) -> Ownership | None: ...

    async def acquire(self, root_id: str) -> Ownership | None: ...

    async def renew(self, ownership: Ownership) -> Ownership: ...

    async def release(self, ownership: Ownership) -> bool: ...

    async def relinquish_failed_prestart(self, ownership: Ownership, actor_id: str) -> bool: ...

    def transaction(self, ownership: Ownership) -> AbstractAsyncContextManager[CoordinatedStoreProtocol]: ...

    async def request(self, envelope: CommandEnvelope) -> CommandReply: ...

    async def active(self, root_id: str, actor_id: str | None = None) -> bool: ...

    async def recovery(self, root_id: str) -> dict[str, Any]: ...

    async def writer_matches(self, token: WriterToken) -> bool: ...

    def watch(self, root_id: str, *, after: int = 0) -> AsyncIterator[ChangeNotice]: ...


class PostgresCoordinator:
    def __init__(
        self,
        store: PostgresStore,
        lease_ttl: float = DEFAULT_LEASE_TTL,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        catch_up_interval: float = DEFAULT_CATCH_UP_INTERVAL,
    ) -> None:
        if not isinstance(store, PostgresStore):
            raise TypeError("PostgresCoordinator requires a PostgresStore")
        if lease_ttl <= 0 or request_timeout <= 0 or catch_up_interval <= 0:
            raise ValueError("coordinator timeouts must be positive")
        self.store = store
        self.lease_ttl = float(lease_ttl)
        self.request_timeout = float(request_timeout)
        self.catch_up_interval = float(catch_up_interval)
        self.instance_id = uuid4()
        self._ident = store._ident
        self._channel = f"tantra_{abs(store._key):x}"[:63]
        self._control_conn: Any = None
        self._control_lock = asyncio.Lock()
        self._listener_conn: Any = None
        self._listener_task: asyncio.Task[None] | None = None
        self._observations: dict[str, _Observation] = {}
        self._dirty_roots: set[str] = set()
        self._observation_event = asyncio.Event()
        self._observation_task: asyncio.Task[None] | None = None
        self._observation_sample = 0
        self._pending_requests: dict[UUID, tuple[str, int]] = {}
        self._observed_replies: dict[UUID, CommandReply | None] = {}
        self._dispatch_event = asyncio.Event()
        self._dispatch_task: asyncio.Task[None] | None = None
        self._dispatchers: dict[str, asyncio.Task[None]] = {}
        self._cleanup_first = 0
        self.observation_checks = 0
        self.observation_ticks = 0
        self.routed_wakeups = 0
        self.dispatch_peak = 0
        self._handler: RequestHandler | None = None
        self._started = False
        self._closed = False

    async def start(self, handler: RequestHandler | None = None) -> None:
        if self._started:
            raise CoordinatorUnavailable("coordinator is already started")
        if self._closed:
            raise CoordinatorUnavailable("coordinator is closed")
        await self.store.setup()
        self._handler = handler
        self._started = True
        self._listener_task = asyncio.create_task(self._listen())
        self._observation_task = asyncio.create_task(self._observe())
        self._dispatch_task = asyncio.create_task(self._dispatch())

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._started = False
        tasks = [self._listener_task, self._observation_task, self._dispatch_task, *self._dispatchers.values()]
        self._listener_task = self._observation_task = self._dispatch_task = None
        for task in tasks:
            if task is not None:
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task is not None), return_exceptions=True)
        self._dispatchers.clear()
        if self._listener_conn is not None:
            await self._listener_conn.close()
            self._listener_conn = None
        async with self._control_lock:
            if self._control_conn is not None:
                await self._control_conn.close()
                self._control_conn = None
        for state in self._observations.values():
            async with state.condition:
                state.generation += 1
                state.condition.notify_all()

    async def locate(self, root_id: str) -> Ownership | None:
        try:
            async with self._data_connection() as conn:
                cursor = await conn.execute(
                    self._sql(
                        "SELECT root_id, owner_instance, generation, expires_at"
                        " FROM {schema}.coordinator_roots"
                        " WHERE root_id = %s AND owner_instance IS NOT NULL AND expires_at > clock_timestamp()"
                    ),
                    (root_id,),
                )
                row = await cursor.fetchone()
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot locate root ownership") from exc
        return _ownership(row) if row is not None else None

    async def acquire(self, root_id: str) -> Ownership | None:
        try:
            async with self._control_lock:
                conn = await self._control_connection()
                async with conn.transaction():
                    await self._configure_transaction(conn)
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (root_id,))
                    deleted = await conn.execute(
                        self._sql("SELECT 1 FROM {schema}.deleted_sessions WHERE actor_id = %s"), (root_id,)
                    )
                    if await deleted.fetchone() is not None:
                        raise SessionNotFound(root_id)
                    await conn.execute(
                        self._sql(
                            "INSERT INTO {schema}.coordinator_roots (root_id) VALUES (%s)"
                            " ON CONFLICT (root_id) DO NOTHING"
                        ),
                        (root_id,),
                    )
                    cursor = await conn.execute(
                        self._sql(
                            "SELECT owner_instance, generation, expires_at"
                            " FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"
                        ),
                        (root_id,),
                    )
                    owner, generation, expires_at = await cursor.fetchone()
                    valid = expires_at is not None and await self._not_expired(conn, expires_at)
                    if valid and owner != str(self.instance_id):
                        return None
                    if valid:
                        return Ownership(
                            root_id=root_id,
                            instance_id=self.instance_id,
                            generation=generation,
                            expires_at=expires_at,
                        )
                    cursor = await conn.execute(
                        self._sql(
                            "UPDATE {schema}.coordinator_roots"
                            " SET owner_instance = %s, generation = generation + 1,"
                            " expires_at = clock_timestamp() + %s * interval '1 second',"
                            " updated_at = clock_timestamp() WHERE root_id = %s"
                            " RETURNING root_id, owner_instance, generation, expires_at"
                        ),
                        (str(self.instance_id), self.lease_ttl, root_id),
                    )
                    ownership = _ownership(await cursor.fetchone())
                    await conn.execute(
                        self._sql("DELETE FROM {schema}.coordinator_activity WHERE root_id = %s"),
                        (root_id,),
                    )
                    await self._publish(conn, root_id, "ownership")
                    return ownership
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot acquire root ownership") from exc

    async def renew(self, ownership: Ownership) -> Ownership:
        try:
            async with self._control_lock:
                conn = await self._control_connection()
                async with conn.transaction():
                    await self._configure_transaction(conn)
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (ownership.root_id,))
                    cursor = await conn.execute(
                        self._sql(
                            "SELECT owner_instance, generation, expires_at"
                            " FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"
                        ),
                        (ownership.root_id,),
                    )
                    row = await cursor.fetchone()
                    if (
                        row is None
                        or row[:2] != (str(ownership.instance_id), ownership.generation)
                        or not await self._not_expired(conn, row[2])
                    ):
                        raise LeaseLost(ownership.root_id)
                    cursor = await conn.execute(
                        self._sql(
                            "UPDATE {schema}.coordinator_roots"
                            " SET expires_at = clock_timestamp() + %s * interval '1 second',"
                            " updated_at = clock_timestamp()"
                            " WHERE root_id = %s"
                            " RETURNING root_id, owner_instance, generation, expires_at"
                        ),
                        (
                            self.lease_ttl,
                            ownership.root_id,
                        ),
                    )
                    row = await cursor.fetchone()
                    if row is None:
                        raise LeaseLost(ownership.root_id)
                    return _ownership(row)
        except LeaseLost:
            raise
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot renew root ownership") from exc

    async def release(self, ownership: Ownership) -> bool:
        try:
            async with self._control_lock:
                conn = await self._control_connection()
                async with conn.transaction():
                    await self._configure_transaction(conn)
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (ownership.root_id,))
                    cursor = await conn.execute(
                        self._sql(
                            "SELECT owner_instance, generation, expires_at"
                            " FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"
                        ),
                        (ownership.root_id,),
                    )
                    row = await cursor.fetchone()
                    if (
                        row is None
                        or row[:2] != (str(ownership.instance_id), ownership.generation)
                        or not await self._not_expired(conn, row[2])
                    ):
                        return False
                    cursor = await conn.execute(
                        self._sql(
                            "SELECT EXISTS ("
                            " SELECT 1 FROM {schema}.coordinator_activity WHERE root_id = %s AND active"
                            " UNION ALL SELECT 1 FROM {schema}.coordinator_requests"
                            " WHERE root_id = %s AND reply IS NULL AND deadline > clock_timestamp()"
                            " UNION ALL SELECT 1 FROM {schema}.sessions"
                            " WHERE COALESCE(NULLIF(header->>'root_id', ''), id) = %s"
                            " AND header->>'status' IN ('queued', 'running', 'awaiting_input')"
                            ")"
                        ),
                        (ownership.root_id, ownership.root_id, ownership.root_id),
                    )
                    if (await cursor.fetchone())[0]:
                        return False
                    cursor = await conn.execute(
                        self._sql(
                            "UPDATE {schema}.coordinator_roots"
                            " SET owner_instance = NULL, expires_at = NULL, updated_at = clock_timestamp()"
                            " WHERE root_id = %s AND owner_instance = %s AND generation = %s"
                            " AND expires_at > clock_timestamp()"
                        ),
                        (ownership.root_id, str(ownership.instance_id), ownership.generation),
                    )
                    if cursor.rowcount:
                        await self._publish(conn, ownership.root_id, "ownership")
                    return bool(cursor.rowcount)
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot release root ownership") from exc

    async def relinquish_failed_prestart(self, ownership: Ownership, actor_id: str) -> bool:
        try:
            async with self._control_lock:
                conn = await self._control_connection()
                async with conn.transaction():
                    await self._configure_transaction(conn)
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (ownership.root_id,))
                    cursor = await conn.execute(
                        self._sql(
                            "SELECT owner_instance, generation, expires_at"
                            " FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"
                        ),
                        (ownership.root_id,),
                    )
                    row = await cursor.fetchone()
                    if (
                        row is None
                        or row[:2] != (str(ownership.instance_id), ownership.generation)
                        or not await self._not_expired(conn, row[2])
                    ):
                        return False
                    cursor = await conn.execute(
                        self._sql(
                            "SELECT EXISTS ("
                            " SELECT 1 FROM {schema}.coordinator_activity"
                            " WHERE root_id = %s AND active"
                            " UNION ALL SELECT 1 FROM {schema}.coordinator_requests"
                            " WHERE root_id = %s AND reply IS NULL AND deadline > clock_timestamp()"
                            " UNION ALL SELECT 1 FROM {schema}.sessions"
                            " WHERE COALESCE(NULLIF(header->>'root_id', ''), id) = %s"
                            " AND header->>'status' IN ('running', 'awaiting_input')"
                            ") OR NOT EXISTS ("
                            " SELECT 1 FROM {schema}.sessions WHERE id = %s"
                            " AND COALESCE(NULLIF(header->>'root_id', ''), id) = %s"
                            " AND header->>'status' IN ('queued', 'idle')"
                            ")"
                        ),
                        (
                            ownership.root_id,
                            ownership.root_id,
                            ownership.root_id,
                            actor_id,
                            ownership.root_id,
                        ),
                    )
                    if (await cursor.fetchone())[0]:
                        return False
                    cursor = await conn.execute(
                        self._sql(
                            "UPDATE {schema}.coordinator_roots"
                            " SET owner_instance = NULL, expires_at = NULL, updated_at = clock_timestamp()"
                            " WHERE root_id = %s AND owner_instance = %s AND generation = %s"
                            " AND expires_at > clock_timestamp()"
                        ),
                        (ownership.root_id, str(ownership.instance_id), ownership.generation),
                    )
                    if cursor.rowcount:
                        await self._publish(conn, ownership.root_id, "ownership")
                    return bool(cursor.rowcount)
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot relinquish failed pre-start ownership") from exc

    def transaction(self, ownership: Ownership) -> _TransactionContext:
        self._ensure_started()
        if ownership.instance_id != self.instance_id:
            raise LeaseLost(ownership.root_id)
        return _TransactionContext(self, ownership)

    async def request(self, envelope: CommandEnvelope) -> CommandReply:
        self._ensure_started()
        existing = self._pending_requests.get(envelope.request_id)
        if existing is not None and existing[0] != envelope.root_id:
            raise InvalidCommandReuse(str(envelope.request_id))
        self._pending_requests[envelope.request_id] = (envelope.root_id, (existing[1] if existing else 0) + 1)
        submitted = False
        destination: tuple[UUID, int] | None = None
        budget = min(
            self.request_timeout,
            max((envelope.deadline.astimezone(UTC) - datetime.now(UTC)).total_seconds(), 0),
        )
        iterator = self.observe(envelope.root_id)
        located = False
        try:
            async with asyncio.timeout(budget):
                async for observed in iterator:
                    if observed.error is not None:
                        raise observed.error
                    reply = self._observed_replies.get(envelope.request_id)
                    if submitted and reply is not None:
                        return reply
                    if observed.deleted:
                        raise SessionNotFound(envelope.root_id)
                    if not observed.owner_valid or observed.owner_instance is None or observed.expires_at is None:
                        if not submitted:
                            if located:
                                raise CoordinatorUnavailable(f"root {envelope.root_id} has no owner")
                            located = True
                            ownership = await self.locate(envelope.root_id)
                            if ownership is None:
                                raise CoordinatorUnavailable(f"root {envelope.root_id} has no owner")
                            current = (ownership.instance_id, ownership.generation)
                            try:
                                await self._put_request(envelope, ownership)
                            except LeaseLost:
                                self._schedule(envelope.root_id)
                                continue
                            submitted = True
                            destination = current
                            reply = await self._request_reply(envelope.request_id)
                            if reply is not None:
                                return reply
                            self._schedule(envelope.root_id, dispatch=True)
                        continue
                    ownership = Ownership(
                        root_id=envelope.root_id,
                        instance_id=UUID(observed.owner_instance),
                        generation=observed.owner_generation,
                        expires_at=observed.expires_at,
                    )
                    current = (ownership.instance_id, ownership.generation)
                    if destination != current:
                        try:
                            await self._put_request(envelope, ownership)
                        except LeaseLost:
                            self._schedule(envelope.root_id)
                            continue
                        submitted = True
                        destination = current
                        reply = await self._request_reply(envelope.request_id)
                        if reply is not None:
                            return reply
                        self._schedule(envelope.root_id, dispatch=True)
                raise CoordinatorUnavailable("coordinator closed before request completed")
        except TimeoutError as exc:
            raise CommandTimeout(str(envelope.request_id)) from exc
        finally:
            await iterator.aclose()
            root, users = self._pending_requests[envelope.request_id]
            if users == 1:
                self._pending_requests.pop(envelope.request_id, None)
                self._observed_replies.pop(envelope.request_id, None)
            else:
                self._pending_requests[envelope.request_id] = (root, users - 1)

    def observation(self, root_id: str) -> RootObservation | None:
        state = self._observations.get(root_id)
        return state.snapshot if state is not None else None

    async def observe(self, root_id: str) -> AsyncIterator[RootObservation]:
        self._ensure_started()
        state = self._observations.setdefault(root_id, _Observation())
        state.users += 1
        self._schedule(root_id)
        generation = -1
        try:
            while not self._closed:
                async with state.condition:
                    await state.condition.wait_for(
                        lambda generation=generation: state.generation != generation or self._closed
                    )
                    generation = state.generation
                    snapshot = state.snapshot
                if snapshot is not None:
                    yield snapshot
        finally:
            state.users -= 1
            if state.users == 0 and self._observations.get(root_id) is state:
                self._observations.pop(root_id, None)
                self._dirty_roots.discard(root_id)

    async def watch(self, root_id: str, *, after: int = 0) -> AsyncIterator[ChangeNotice]:
        cursor = max(after, 0)
        iterator = self.observe(root_id)
        first = True
        try:
            async for snapshot in iterator:
                if snapshot.error is not None:
                    raise snapshot.error
                if not first and snapshot.change_id <= cursor:
                    continue
                first = False
                while True:
                    page = await self._changes(root_id, cursor)
                    for notice in page:
                        cursor = notice.change_id
                        yield notice
                    if len(page) < 1000:
                        break
        finally:
            await iterator.aclose()

    async def active(self, root_id: str, actor_id: str | None = None) -> bool:
        try:
            async with self._data_connection() as conn:
                condition = "a.root_id = %s AND a.active"
                params: tuple[Any, ...] = (root_id,)
                if actor_id is not None:
                    condition += " AND a.actor_id = %s"
                    params += (actor_id,)
                cursor = await conn.execute(
                    self._sql(
                        "SELECT EXISTS ("
                        " SELECT 1 FROM {schema}.coordinator_activity a"
                        " JOIN {schema}.coordinator_roots r ON r.root_id = a.root_id"
                        f" WHERE {condition} AND r.owner_instance IS NOT NULL"
                        " AND r.expires_at > clock_timestamp()"
                        ")"
                    ),
                    params,
                )
                return bool((await cursor.fetchone())[0])
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot read actor activity") from exc

    async def recovery(self, root_id: str) -> dict[str, Any]:
        try:
            async with self._data_connection() as conn:
                cursor = await conn.execute(
                    self._sql("SELECT recovery FROM {schema}.coordinator_roots WHERE root_id = %s"),
                    (root_id,),
                )
                row = await cursor.fetchone()
                return dict(row[0]) if row is not None else {}
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot read root recovery state") from exc

    async def writer_matches(self, token: WriterToken) -> bool:
        try:
            async with self._data_connection() as conn:
                cursor = await conn.execute(
                    self._sql("SELECT writer_connection = %s FROM {schema}.coordinator_roots WHERE root_id = %s"),
                    (str(token.connection_id), token.root_id),
                )
                row = await cursor.fetchone()
                return row is not None and bool(row[0])
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot validate root writer") from exc

    async def cleanup(self, limit: int = 100) -> int:
        remaining = max(limit, 0)
        if remaining == 0:
            return 0
        statements = (
            "WITH doomed AS (SELECT request_id FROM {schema}.coordinator_requests"
            " WHERE coalesce(completed_at, deadline) < clock_timestamp() - interval '24 hours'"
            " ORDER BY coalesce(completed_at, deadline), request_id LIMIT %s FOR UPDATE SKIP LOCKED)"
            " DELETE FROM {schema}.coordinator_requests r USING doomed d WHERE r.request_id = d.request_id",
            "WITH doomed AS (SELECT id FROM {schema}.coordinator_changes"
            " WHERE created_at < clock_timestamp() - interval '24 hours'"
            " ORDER BY created_at, id LIMIT %s FOR UPDATE SKIP LOCKED)"
            " DELETE FROM {schema}.coordinator_changes c USING doomed d WHERE c.id = d.id",
        )
        first = self._cleanup_first
        self._cleanup_first = 1 - first
        try:
            async with self._data_connection() as conn, conn.transaction():
                await self._configure_transaction(conn)
                removed = 0
                for index in (first, 1 - first):
                    if remaining == 0:
                        break
                    result = await conn.execute(self._sql(statements[index]), (remaining,))
                    removed += result.rowcount
                    remaining -= result.rowcount
                return removed
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("coordinator cleanup failed") from exc

    def _ensure_started(self) -> None:
        if not self._started or self._closed:
            raise CoordinatorUnavailable("coordinator is not started")

    async def _put_request(self, envelope: CommandEnvelope, ownership: Ownership) -> None:
        raw = envelope.model_dump(mode="json")
        try:
            async with self._data_connection() as conn:
                async with conn.transaction():
                    await self._configure_transaction(conn)
                    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (envelope.root_id,))
                    deleted = await conn.execute(
                        self._sql("SELECT 1 FROM {schema}.deleted_sessions WHERE actor_id = %s"), (envelope.root_id,)
                    )
                    if await deleted.fetchone() is not None:
                        raise SessionNotFound(envelope.root_id)
                    root = await conn.execute(
                        self._sql(
                            "SELECT owner_instance, generation, expires_at"
                            " FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"
                        ),
                        (envelope.root_id,),
                    )
                    row = await root.fetchone()
                    if (
                        row is None
                        or row[:2] != (str(ownership.instance_id), ownership.generation)
                        or not await self._not_expired(conn, row[2])
                    ):
                        raise LeaseLost(envelope.root_id)
                    cursor = await conn.execute(
                        self._sql(
                            "INSERT INTO {schema}.coordinator_requests"
                            " (request_id, root_id, destination_instance, destination_generation, envelope, deadline)"
                            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (request_id) DO NOTHING"
                        ),
                        (
                            envelope.request_id,
                            envelope.root_id,
                            str(ownership.instance_id),
                            ownership.generation,
                            Jsonb(raw),
                            envelope.deadline,
                        ),
                    )
                    existing = await conn.execute(
                        self._sql(
                            "SELECT root_id, envelope, reply FROM {schema}.coordinator_requests"
                            " WHERE request_id = %s FOR UPDATE"
                        ),
                        (envelope.request_id,),
                    )
                    root_id, stored, reply = await existing.fetchone()
                    if root_id != envelope.root_id or stored != raw:
                        raise InvalidCommandReuse(str(envelope.request_id))
                    if reply is None:
                        await conn.execute(
                            self._sql(
                                "UPDATE {schema}.coordinator_requests"
                                " SET destination_instance = %s, destination_generation = %s"
                                " WHERE request_id = %s"
                            ),
                            (str(ownership.instance_id), ownership.generation, envelope.request_id),
                        )
                        await self._publish(conn, envelope.root_id, "request")
                    elif cursor.rowcount:
                        await self._publish(conn, envelope.root_id, "request")
        except InvalidCommandReuse:
            raise
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot persist coordinator request") from exc

    async def _request_reply(self, request_id: UUID) -> CommandReply | None:
        try:
            async with self._data_connection() as conn:
                cursor = await conn.execute(
                    self._sql("SELECT reply FROM {schema}.coordinator_requests WHERE request_id = %s"),
                    (request_id,),
                )
                row = await cursor.fetchone()
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot read coordinator reply") from exc
        if row is None or row[0] is None:
            return None
        return CommandReply.model_validate(row[0])

    async def _changes(self, root_id: str, after: int) -> list[ChangeNotice]:
        try:
            async with self._data_connection() as conn:
                cursor = await conn.execute(
                    self._sql(
                        "SELECT id, root_id, kind, actor_id, seq FROM {schema}.coordinator_changes"
                        " WHERE root_id = %s AND id > %s ORDER BY id LIMIT 1000"
                    ),
                    (root_id, after),
                )
                rows = await cursor.fetchall()
        except psycopg.Error as exc:
            raise CoordinatorUnavailable("cannot catch up coordinator changes") from exc
        return [
            ChangeNotice(change_id=row[0], root_id=row[1], kind=row[2], actor_id=row[3], seq=row[4]) for row in rows
        ]

    def _schedule(self, root_id: str, *, dispatch: bool = False) -> None:
        if root_id in self._observations:
            self._dirty_roots.add(root_id)
            self._observation_event.set()
        if dispatch:
            self._dispatch_event.set()

    async def _listen(self) -> None:
        while not self._closed:
            try:
                self._listener_conn = await psycopg.AsyncConnection.connect(self.store.dsn, autocommit=True)
                await self._listener_conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self._channel)))
                for root_id in self._observations:
                    self._schedule(root_id)
                self._dispatch_event.set()
                while not self._closed:
                    async for notice in self._listener_conn.notifies(timeout=self.catch_up_interval, stop_after=100):
                        try:
                            payload = json.loads(notice.payload)
                            root_id = payload["root_id"]
                            if not isinstance(root_id, str):
                                continue
                        except (ValueError, KeyError, TypeError):
                            continue
                        if root_id in self._observations:
                            self.routed_wakeups += 1
                        self._schedule(root_id, dispatch=payload.get("kind") in ("request", "ownership"))
            except asyncio.CancelledError:
                raise
            except psycopg.Error:
                if self._listener_conn is not None:
                    await self._listener_conn.close()
                    self._listener_conn = None
                await asyncio.sleep(min(self.catch_up_interval, 1.0))

    async def _observe(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.catch_up_interval
        while not self._closed:
            try:
                await asyncio.wait_for(self._observation_event.wait(), max(deadline - loop.time(), 0))
            except TimeoutError:
                pass
            self._observation_event.clear()
            periodic = loop.time() >= deadline
            roots = set(self._observations) if periodic else self._dirty_roots.copy()
            self._dirty_roots.difference_update(roots)
            if periodic:
                deadline = loop.time() + self.catch_up_interval
            if roots:
                await self._catch_up(roots, periodic=periodic)

    async def _catch_up(self, roots: set[str], *, periodic: bool) -> None:
        request_ids = [str(key) for key, (root, _) in self._pending_requests.items() if root in roots]
        try:
            async with self._data_connection() as conn:
                self.observation_checks += 1
                cursor = await conn.execute(
                    self._sql(
                        "WITH interested AS (SELECT unnest(%s::text[]) AS root_id)"
                        " SELECT 'root', i.root_id, NULL::text, jsonb_build_object("
                        " 'change_id', coalesce(c.id, 0), 'writer', r.writer_connection,"
                        " 'owner', r.owner_instance, 'generation', coalesce(r.generation, 0),"
                        " 'valid', coalesce(r.owner_instance IS NOT NULL AND r.expires_at > clock_timestamp(), false),"
                        " 'expires', r.expires_at, 'recovery', coalesce(r.recovery, '{{}}'::jsonb),"
                        " 'deleted', d.actor_id IS NOT NULL)"
                        " FROM interested i LEFT JOIN {schema}.coordinator_roots r ON r.root_id = i.root_id"
                        " LEFT JOIN {schema}.deleted_sessions d ON d.actor_id = i.root_id"
                        " LEFT JOIN LATERAL (SELECT id FROM {schema}.coordinator_changes"
                        " WHERE root_id = i.root_id ORDER BY id DESC LIMIT 1) c ON true"
                        " UNION ALL SELECT 'actor', i.root_id, s.id, jsonb_build_array(s.last_seq,"
                        " coalesce(a.active AND r.owner_instance IS NOT NULL"
                        " AND r.expires_at > clock_timestamp(), false))"
                        " FROM interested i JOIN {schema}.sessions s"
                        " ON COALESCE(NULLIF(s.header->>'root_id', ''), s.id) = i.root_id"
                        " LEFT JOIN {schema}.coordinator_activity a ON a.root_id = i.root_id AND a.actor_id = s.id"
                        " LEFT JOIN {schema}.coordinator_roots r ON r.root_id = i.root_id"
                        " UNION ALL SELECT 'reply', request_id::text, NULL::text, reply"
                        " FROM {schema}.coordinator_requests WHERE request_id = ANY(%s::uuid[])"
                    ),
                    (sorted(roots), request_ids),
                )
                rows = await cursor.fetchall()
            if periodic:
                self._observation_sample += 1
                self.observation_ticks += 1
            headers: dict[str, dict[str, Any]] = {}
            actors: dict[str, dict[str, tuple[int, bool]]] = {root: {} for root in roots}
            for kind, key, actor, data in rows:
                if kind == "root":
                    headers[key] = data
                elif kind == "actor":
                    actors[key][actor] = (int(data[0]), bool(data[1]))
                elif UUID(key) in self._pending_requests:
                    self._observed_replies[UUID(key)] = CommandReply.model_validate(data) if data else None
            for root_id, data in headers.items():
                state = self._observations.get(root_id)
                if state is None:
                    continue
                snapshot = RootObservation(
                    root_id=root_id,
                    sample=self._observation_sample,
                    change_id=int(data["change_id"]),
                    writer_connection=data["writer"],
                    owner_instance=data["owner"],
                    owner_generation=int(data["generation"]),
                    owner_valid=bool(data["valid"]),
                    recovery=dict(data["recovery"]),
                    actors=actors[root_id],
                    expires_at=datetime.fromisoformat(data["expires"]) if data["expires"] else None,
                    deleted=bool(data["deleted"]),
                )
                async with state.condition:
                    state.snapshot = snapshot
                    state.generation += 1
                    state.condition.notify_all()
        except psycopg.Error as exc:
            error = CoordinatorUnavailable("cannot catch up coordinator observations")
            error.__cause__ = exc
            for root_id in roots:
                state = self._observations.get(root_id)
                if state is None:
                    continue
                snapshot = state.snapshot or RootObservation(
                    root_id, self._observation_sample, 0, None, None, 0, False, {}, {}
                )
                async with state.condition:
                    state.snapshot = replace(snapshot, error=error)
                    state.generation += 1
                    state.condition.notify_all()

    async def _dispatch(self) -> None:
        while not self._closed:
            try:
                await asyncio.wait_for(self._dispatch_event.wait(), self.catch_up_interval)
            except TimeoutError:
                pass
            self._dispatch_event.clear()
            await self._process_requests()

    async def _process_requests(self) -> None:
        if self._handler is None:
            return
        while not self._closed and len(self._dispatchers) < 4:
            candidate = await self._next_request()
            if candidate is None:
                return
            envelope, ownership = candidate
            task = asyncio.create_task(self._dispatch_request(envelope, ownership))
            self._dispatchers[envelope.root_id] = task
            self.dispatch_peak = max(self.dispatch_peak, len(self._dispatchers))

    async def _dispatch_request(self, envelope: CommandEnvelope, ownership: Ownership) -> None:
        try:
            assert self._handler is not None
            await self._handler(
                envelope,
                lambda operation: self._apply_request(envelope, ownership, operation),
            )
            if await self._request_reply(envelope.request_id) is None:
                await self._infrastructure_reply(envelope, ownership)
        except LeaseLost:
            pass
        except Exception:
            await self._infrastructure_reply(envelope, ownership)
        finally:
            if self._dispatchers.get(envelope.root_id) is asyncio.current_task():
                self._dispatchers.pop(envelope.root_id, None)
            self._dispatch_event.set()

    async def _apply_request(
        self,
        envelope: CommandEnvelope,
        ownership: Ownership,
        operation: RequestOperation,
    ) -> CommandReply | None:
        async with self.transaction(ownership) as store:
            locked = await store.lock_request(envelope.request_id)
            if locked is None:
                return None
            reply = await operation(locked, store)
            if reply.request_id != locked.request_id:
                raise ValueError("handler replied to a different request")
            await store.reply(reply)
            if store.deleted_ids:
                await store.finish_deletion()
            return reply

    async def _next_request(self) -> tuple[CommandEnvelope, Ownership] | None:
        try:
            async with self._data_connection() as conn:
                cursor = await conn.execute(
                    self._sql(
                        "SELECT q.envelope, r.root_id, r.owner_instance, r.generation, r.expires_at"
                        " FROM {schema}.coordinator_requests q"
                        " JOIN {schema}.coordinator_roots r ON r.root_id = q.root_id"
                        " WHERE q.reply IS NULL AND q.deadline > clock_timestamp()"
                        " AND NOT (q.root_id = ANY(%s::text[]))"
                        " AND q.destination_instance = %s AND q.destination_generation = r.generation"
                        " AND r.owner_instance = %s AND r.expires_at > clock_timestamp()"
                        " ORDER BY q.created_at, q.request_id LIMIT 1"
                    ),
                    (list(self._dispatchers), str(self.instance_id), str(self.instance_id)),
                )
                row = await cursor.fetchone()
        except psycopg.Error:
            return None
        if row is None:
            return None
        return CommandEnvelope.model_validate(row[0]), _ownership(row[1:])

    async def _infrastructure_reply(self, envelope: CommandEnvelope, ownership: Ownership) -> None:
        try:
            async with self.transaction(ownership) as store:
                locked = await store.lock_request(envelope.request_id)
                if locked is None:
                    return
                await store.reply(
                    CommandReply(
                        request_id=envelope.request_id,
                        status="error",
                        error_code="coordinator_handler_failed",
                        message="owner could not apply the request",
                        retryable=True,
                    )
                )
        except Exception:
            return

    async def _publish(
        self,
        conn: Any,
        root_id: str,
        kind: str,
        actor_id: str | None = None,
        seq: int | None = None,
    ) -> int:
        cursor = await conn.execute(
            self._sql(
                "INSERT INTO {schema}.coordinator_changes (root_id, kind, actor_id, seq)"
                " VALUES (%s, %s, %s, %s) RETURNING id"
            ),
            (root_id, kind, actor_id, seq),
        )
        change_id = (await cursor.fetchone())[0]
        payload = json.dumps(
            {"id": change_id, "root_id": root_id, "kind": kind, "actor_id": actor_id, "seq": seq},
            separators=(",", ":"),
        )
        await conn.execute("SELECT pg_notify(%s, %s)", (self._channel, payload))
        return change_id

    @asynccontextmanager
    async def _data_connection(self) -> AsyncIterator[Any]:
        self._ensure_started()
        async with self.store._connection() as conn:
            yield conn

    async def _control_connection(self) -> Any:
        self._ensure_started()
        if self._control_conn is not None and not self._control_conn.closed:
            try:
                await self._control_conn.execute("SELECT 1")
            except psycopg.Error:
                try:
                    await self._control_conn.close()
                except psycopg.Error:
                    pass
                self._control_conn = None
        if self._control_conn is None or self._control_conn.closed:
            self._control_conn = await psycopg.AsyncConnection.connect(self.store.dsn, autocommit=True)
        return self._control_conn

    async def _configure_transaction(self, conn: Any) -> None:
        timeout = str(max(int(self.request_timeout * 1000), 1))
        await conn.execute("SELECT set_config('statement_timeout', %s, true)", (timeout,))
        await conn.execute("SELECT set_config('lock_timeout', %s, true)", (timeout,))
        await conn.execute("SELECT set_config('idle_in_transaction_session_timeout', %s, true)", (timeout,))

    async def _not_expired(self, conn: Any, expires_at: datetime) -> bool:
        cursor = await conn.execute("SELECT %s > clock_timestamp()", (expires_at,))
        return bool((await cursor.fetchone())[0])

    def _sql(self, statement: str) -> Any:
        return sql.SQL(statement).format(schema=self._ident)


class _TransactionContext:
    def __init__(self, coordinator: PostgresCoordinator, ownership: Ownership) -> None:
        self.coordinator = coordinator
        self.ownership = ownership
        self._transaction: Any = None
        self._transaction_entered = False
        self._checkout: Any = None
        self._store: CoordinatedStore | None = None

    async def __aenter__(self) -> CoordinatedStore:
        self.coordinator._ensure_started()
        self._checkout = self.coordinator.store._connection()
        conn = await self._checkout.__aenter__()
        try:
            self._transaction = conn.transaction()
            await self._transaction.__aenter__()
            self._transaction_entered = True
            await self.coordinator._configure_transaction(conn)
            await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (self.ownership.root_id,))
            cursor = await conn.execute(
                self.coordinator._sql(
                    "SELECT owner_instance, generation, expires_at"
                    " FROM {schema}.coordinator_roots WHERE root_id = %s FOR UPDATE"
                ),
                (self.ownership.root_id,),
            )
            row = await cursor.fetchone()
            if (
                row is None
                or row[:2] != (str(self.ownership.instance_id), self.ownership.generation)
                or not await self.coordinator._not_expired(conn, row[2])
            ):
                raise LeaseLost(self.ownership.root_id)
            await conn.execute("SELECT set_config('tantra.root_id', %s, true)", (self.ownership.root_id,))
            await conn.execute("SELECT set_config('tantra.instance_id', %s, true)", (str(self.ownership.instance_id),))
            await conn.execute("SELECT set_config('tantra.generation', %s, true)", (str(self.ownership.generation),))
            self._store = CoordinatedStore(self.coordinator, self.ownership, conn)
            return self._store
        except BaseException as exc:
            try:
                if self._transaction_entered:
                    await self._transaction.__aexit__(type(exc), exc, exc.__traceback__)
            finally:
                await self._checkout.__aexit__(type(exc), exc, exc.__traceback__)
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._store is not None:
            self._store._active = False
        try:
            await self._transaction.__aexit__(exc_type, exc, traceback)
        finally:
            await self._checkout.__aexit__(exc_type, exc, traceback)


class CoordinatedStore:
    def __init__(self, coordinator: PostgresCoordinator, ownership: Ownership, conn: Any) -> None:
        self.coordinator = coordinator
        self.ownership = ownership
        self.conn = conn
        self._active = True
        self.writer_change_id: int | None = None
        self.deleted_ids: list[str] = []
        self.delete_request_id: UUID | None = None

    async def delete_tree(
        self, sid: str, *, allow_active: bool = False, before_delete: Callable[[list[str]], None] | None = None
    ) -> list[str]:
        await self._assert_fence()
        if sid != self.ownership.root_id:
            raise LeaseLost(sid)
        if self.delete_request_id is None:
            raise TantraError("coordinated deletion requires a delete control request")
        self.deleted_ids = await self.coordinator.store._delete_tree(
            self.conn,
            sid,
            allow_active=allow_active,
            before_delete=before_delete,
            request_id=self.delete_request_id,
        )
        return self.deleted_ids

    async def finish_deletion(self) -> None:
        await self._assert_fence()
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.coordinator_roots SET owner_instance = NULL, expires_at = NULL,"
                " writer_connection = NULL, recovery = '{{}}'::jsonb, generation = generation + 1,"
                " updated_at = clock_timestamp() WHERE root_id = %s"
            ),
            (self.ownership.root_id,),
        )
        await self.coordinator._publish(self.conn, self.ownership.root_id, "deleted")

    async def relinquish_unrecovered(self) -> None:
        await self._assert_fence()
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.coordinator_roots SET owner_instance = NULL, expires_at = NULL,"
                " updated_at = clock_timestamp() WHERE root_id = %s"
            ),
            (self.ownership.root_id,),
        )
        await self.coordinator._publish(self.conn, self.ownership.root_id, "ownership")

    async def create(self, header: SessionHeader) -> None:
        self._ensure_active()
        root_id = header.root_id or header.id
        if root_id != self.ownership.root_id:
            raise LeaseLost(root_id)
        await self.coordinator.store._check_create(self.conn, header)
        stored = header.model_copy(deep=True)
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "INSERT INTO {schema}.sessions (id, header, metadata, parent_id, created_at, last_seq)"
                " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING"
            ),
            (
                stored.id,
                _json(stored),
                Jsonb(stored.metadata),
                stored.parent_id,
                stored.created_at,
                stored.last_seq,
            ),
        )
        if cursor.rowcount == 0:
            raise SessionExists(header.id)
        await self.change("header", header.id)

    async def header(self, sid: str) -> SessionHeader | None:
        self._ensure_active()
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s"),
            (sid,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        header = _hydrate(row)
        self._check_header(header)
        return header

    async def put_header(self, header: SessionHeader) -> None:
        self._ensure_active()
        self._check_header(header)
        stored = header.model_copy(deep=True)
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
            (header.id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(header.id)
        stored.last_seq = row[0]
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.sessions SET header = %s, metadata = %s, parent_id = %s, created_at = %s WHERE id = %s"
            ),
            (_json(stored), Jsonb(stored.metadata), stored.parent_id, stored.created_at, stored.id),
        )
        await self.change("header", header.id)

    async def patch_header(
        self,
        sid: str,
        *,
        title: str | None = UNSET,
        model: str | None = UNSET,
        status: SessionStatus = UNSET,
        pending_ask: str | None = UNSET,
        usage: Usage = UNSET,
        metadata: dict[str, Any] = UNSET,
        finished: bool = UNSET,
    ) -> SessionHeader:
        self._ensure_active()
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
            (sid,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(sid)
        current = _hydrate(row)
        self._check_header(current)
        if model is not UNSET and model != current.model and await self._has_work():
            raise ModelChangeBusy("model cannot change while coordinated work is active")
        stored = apply_patch(
            current,
            title=title,
            model=model,
            status=status,
            pending_ask=pending_ask,
            usage=usage,
            metadata=metadata,
            finished=finished,
        )
        await self.conn.execute(
            self.coordinator._sql("UPDATE {schema}.sessions SET header = %s, metadata = %s WHERE id = %s"),
            (_json(stored), Jsonb(stored.metadata), sid),
        )
        await self.change("header", sid)
        return stored

    async def append(self, sid: str, events: Sequence[SessionEvent]) -> int:
        self._ensure_active()
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
            (sid,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(sid)
        header = _hydrate(row)
        self._check_header(header)
        seq = header.last_seq
        start_seq = seq
        rows = []
        for event in events:
            seq += 1
            rows.append((sid, seq, _event_json(Stamped(seq=seq, event=event))))
        if rows:
            await cursor.executemany(
                self.coordinator._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"),
                rows,
            )
            await self.coordinator.store._index_events(
                self.conn, [(sid, start_seq + offset, event) for offset, event in enumerate(events, start=1)]
            )
        header = reduce_header(header, events)
        header.last_seq = seq
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.sessions SET header = %s, last_seq = %s,"
                " operational_seq = CASE WHEN operational_version = 1 AND operational_seq = last_seq"
                " THEN %s ELSE operational_seq END WHERE id = %s"
            ),
            (_json(header), seq, seq, sid),
        )
        if rows:
            await self.change("journal", sid, seq)
        return seq

    async def enqueue(self, sid: str, event: InputQueued) -> EnqueueResult:
        self._ensure_active()
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT header, last_seq FROM {schema}.sessions WHERE id = %s FOR UPDATE"),
            (sid,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise SessionNotFound(sid)
        header = _hydrate(row)
        self._check_header(header)
        existing = await self.coordinator.store._lookup_input(self.conn, sid, event.command_id)
        if existing is not None:
            if existing.event == event:
                if await self.coordinator.store._input_pending(self.conn, sid, event.command_id):
                    await self.set_active(sid, True)
                return EnqueueResult(seq=existing.seq, duplicate=True)
            raise InvalidCommandReuse(event.command_id)
        seq = header.last_seq + 1
        await self.conn.execute(
            self.coordinator._sql("INSERT INTO {schema}.events (session_id, seq, stamped) VALUES (%s, %s, %s)"),
            (sid, seq, _event_json(Stamped(seq=seq, event=event))),
        )
        await self.coordinator.store._index_events(self.conn, [(sid, seq, event)])
        header = reduce_header(header, [event])
        header.last_seq = seq
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.sessions SET header = %s, last_seq = %s,"
                " operational_seq = CASE WHEN operational_version = 1 AND operational_seq = last_seq"
                " THEN %s ELSE operational_seq END WHERE id = %s"
            ),
            (_json(header), seq, seq, sid),
        )
        await self.change("journal", sid, seq)
        await self.set_active(sid, True)
        return EnqueueResult(seq=seq, duplicate=False)

    async def lookup_command(self, root_id: str, command_id: str) -> tuple[str, Stamped] | None:
        self._ensure_active()
        if root_id != self.ownership.root_id:
            raise LeaseLost(root_id)
        return await self.coordinator.store._lookup_command(self.conn, root_id, command_id)

    async def lookup_finished(self, actor_id: str) -> Stamped | None:
        self._ensure_active()
        if await self.header(actor_id) is None:
            raise SessionNotFound(actor_id)
        return await self.coordinator.store._lookup_finished(self.conn, actor_id)

    async def read_operational(self, actor_id: str) -> OperationalState:
        self._ensure_active()
        if await self.header(actor_id) is None:
            raise SessionNotFound(actor_id)
        return await self.coordinator.store._read_operational(self.conn, actor_id)

    async def read_turn(self, actor_id: str, turn_id: str) -> list[Stamped] | None:
        self._ensure_active()
        if await self.header(actor_id) is None:
            raise SessionNotFound(actor_id)
        return await self.coordinator.store._read_turn(self.conn, actor_id, turn_id)

    async def read_compacted(self, actor_id: str) -> HistorySnapshot:
        self._ensure_active()
        if await self.header(actor_id) is None:
            raise SessionNotFound(actor_id)
        return await self.coordinator.store._read_compacted(self.conn, actor_id)

    async def read_page(self, sid: str, *, after: int = 0, limit: int = 1000) -> list[Stamped]:
        self._ensure_active()
        header = await self.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "SELECT stamped FROM {schema}.events WHERE session_id = %s AND seq > %s ORDER BY seq LIMIT %s"
            ),
            (sid, after, max(limit, 0)),
        )
        return [_parse(sid, row[0]) for row in await cursor.fetchall()]

    async def claim_writer(self, connection_id: UUID) -> WriterToken:
        await self._assert_fence()
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.coordinator_roots SET writer_connection = %s, updated_at = clock_timestamp()"
                " WHERE root_id = %s"
            ),
            (str(connection_id), self.ownership.root_id),
        )
        token = WriterToken(root_id=self.ownership.root_id, connection_id=connection_id)
        self.writer_change_id = await self.change("writer")
        return token

    async def release_writer(self, token: WriterToken) -> bool:
        await self._assert_fence()
        if token.root_id != self.ownership.root_id:
            return False
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.coordinator_roots SET writer_connection = NULL, updated_at = clock_timestamp()"
                " WHERE root_id = %s AND writer_connection = %s"
            ),
            (token.root_id, str(token.connection_id)),
        )
        if cursor.rowcount:
            await self.change("writer")
        return bool(cursor.rowcount)

    async def validate_writer(self, token: WriterToken) -> None:
        await self._assert_fence()
        if token.root_id != self.ownership.root_id:
            raise WriterReplaced(f"writer for {token.root_id} was replaced")
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT writer_connection = %s FROM {schema}.coordinator_roots WHERE root_id = %s"),
            (str(token.connection_id), token.root_id),
        )
        row = await cursor.fetchone()
        if row is None or not row[0]:
            raise WriterReplaced(f"writer for {token.root_id} was replaced")

    async def set_active(self, actor_id: str, active: bool) -> None:
        await self._assert_fence()
        await self.conn.execute(
            self.coordinator._sql(
                "INSERT INTO {schema}.coordinator_activity (root_id, actor_id, active) VALUES (%s, %s, %s)"
                " ON CONFLICT (root_id, actor_id) DO UPDATE"
                " SET active = excluded.active, updated_at = clock_timestamp()"
            ),
            (self.ownership.root_id, actor_id, active),
        )
        await self.change("activity", actor_id)

    async def recovery(self) -> dict[str, Any]:
        await self._assert_fence()
        cursor = await self.conn.execute(
            self.coordinator._sql("SELECT recovery FROM {schema}.coordinator_roots WHERE root_id = %s"),
            (self.ownership.root_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise LeaseLost(self.ownership.root_id)
        return dict(row[0])

    async def set_recovery(self, state: dict[str, Any]) -> None:
        await self._assert_fence()
        await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.coordinator_roots SET recovery = %s, updated_at = clock_timestamp() WHERE root_id = %s"
            ),
            (Jsonb(state), self.ownership.root_id),
        )
        await self.change("recovery")

    async def lock_request(self, request_id: UUID) -> CommandEnvelope | None:
        await self._assert_fence()
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "SELECT envelope FROM {schema}.coordinator_requests"
                " WHERE request_id = %s AND root_id = %s AND reply IS NULL"
                " AND deadline > clock_timestamp()"
                " AND destination_instance = %s AND destination_generation = %s FOR UPDATE"
            ),
            (
                request_id,
                self.ownership.root_id,
                str(self.ownership.instance_id),
                self.ownership.generation,
            ),
        )
        row = await cursor.fetchone()
        return CommandEnvelope.model_validate(row[0]) if row is not None else None

    async def reply(self, reply: CommandReply) -> None:
        await self._assert_fence()
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "UPDATE {schema}.coordinator_requests SET reply = %s, completed_at = clock_timestamp()"
                " WHERE request_id = %s AND root_id = %s AND reply IS NULL"
                " AND destination_instance = %s AND destination_generation = %s"
            ),
            (
                Jsonb(reply.model_dump(mode="json")),
                reply.request_id,
                self.ownership.root_id,
                str(self.ownership.instance_id),
                self.ownership.generation,
            ),
        )
        if cursor.rowcount == 0:
            raise LeaseLost(self.ownership.root_id)
        await self.change("request")

    async def change(
        self,
        kind: Literal["journal", "header", "ownership", "writer", "activity", "recovery", "request", "deleted"],
        actor_id: str | None = None,
        seq: int | None = None,
    ) -> int:
        await self._assert_fence()
        return await self.coordinator._publish(self.conn, self.ownership.root_id, kind, actor_id, seq)

    async def _has_work(self) -> bool:
        self._ensure_active()
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "SELECT EXISTS ("
                " SELECT 1 FROM {schema}.coordinator_activity WHERE root_id = %s AND active"
                " UNION ALL SELECT 1 FROM {schema}.coordinator_requests"
                " WHERE root_id = %s AND reply IS NULL AND deadline > clock_timestamp()"
                " UNION ALL SELECT 1 FROM {schema}.sessions"
                " WHERE COALESCE(NULLIF(header->>'root_id', ''), id) = %s"
                " AND header->>'status' IN ('queued', 'running', 'awaiting_input')"
                ")"
            ),
            (self.ownership.root_id, self.ownership.root_id, self.ownership.root_id),
        )
        return bool((await cursor.fetchone())[0])

    def _check_header(self, header: SessionHeader) -> None:
        root_id = header.root_id or header.id
        if root_id != self.ownership.root_id:
            raise LeaseLost(root_id)

    def _ensure_active(self) -> None:
        if not self._active:
            raise CoordinatorUnavailable("coordinated transaction view is closed")

    async def _assert_fence(self) -> None:
        self._ensure_active()
        cursor = await self.conn.execute(
            self.coordinator._sql(
                "SELECT owner_instance = %s AND generation = %s AND expires_at > clock_timestamp()"
                " FROM {schema}.coordinator_roots WHERE root_id = %s"
            ),
            (str(self.ownership.instance_id), self.ownership.generation, self.ownership.root_id),
        )
        row = await cursor.fetchone()
        if row is None or not row[0]:
            raise LeaseLost(self.ownership.root_id)


def _ownership(row: Sequence[Any]) -> Ownership:
    return Ownership(
        root_id=row[0],
        instance_id=UUID(str(row[1])),
        generation=row[2],
        expires_at=row[3],
    )
