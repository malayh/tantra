from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import aclosing, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, ValidationError

from tantra.agent import Agent
from tantra.ask import Approval, ApprovalResponse, AskRequest, AskResponse
from tantra.context import TurnContext, build_sample_request, resolve_prompt
from tantra.errors import ProviderError, SeqConflict, SessionBusy, TantraError
from tantra.events import (
    AskAnswered,
    AskRaised,
    CancelRequested,
    ChildSessionSpawned,
    ReasoningPart,
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    ToolProgress,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
    Usage,
)
from tantra.hooks import Denial, Escalation, Hook
from tantra.permissions import decide, strictest
from tantra.providers.base import (
    Provider,
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    TextDelta,
    ToolCallDelta,
    ToolSchema,
)
from tantra.skills import SkillInfo
from tantra.stores.base import Store
from tantra.tools import Context, Tool

if TYPE_CHECKING:
    from tantra.compaction import Compactor
    from tantra.memory import Memory

SUBMIT_OUTPUT = "submit_output"

CANCELLED_RESULT = "not executed: turn cancelled"
COMPLETED_RESULT = "not executed: turn completed"


class Emitted(BaseModel):
    model_config = ConfigDict(extra="allow")

    session_id: str
    depth: int = 0
    seq: int | None = None
    event: SessionEvent | TextDelta | ReasoningDelta | ToolCallDelta


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 8.0


DEFAULT_RETRY = RetryConfig()


@dataclass
class Ask:
    ask_id: str
    request: AskRequest
    response: AskResponse | None = None


@dataclass(frozen=True)
class ChildOutcome:
    result: Any = None
    error: Exception | None = None
    pending_ask: str | None = None


class Spawner(Protocol):
    """Child-session operations the loop delegates to the harness."""

    def resolve(self, agent: type[Agent] | str) -> str:
        """Return the registered name of `agent`.

        Raises when it is absent from the name table or when spawning it would exceed `max_depth`.
        Both are functions of `(agent, parent depth)` alone, so a replayed turn fails identically
        and a failing spawn never consumes a child-attachment slot.
        """

    async def create(self, agent: str) -> str:
        """Create a child session one level below the running one and return its id."""

    def drive(self, sid: str, input: str) -> AsyncIterator[Emitted]:
        """Run, resume or skip the child's turn, yielding its events for live forwarding."""

    async def outcome(self, sid: str) -> ChildOutcome:
        """Read the child's log and report its result, its failure, or the ask it waits on."""


@dataclass
class TurnState:
    turn_id: str = ""
    input: str = ""
    samples_used: int = 0
    batch: list[ToolCallRequested] = field(default_factory=list)
    started: set[str] = field(default_factory=set)
    results: dict[str, ToolCallCompleted] = field(default_factory=dict)
    asks: dict[str, list[Ask]] = field(default_factory=dict)
    children: dict[str, list[ChildSessionSpawned]] = field(default_factory=dict)
    cancelled: bool = False

    def observe(self, event: SessionEvent) -> None:
        if isinstance(event, TurnStarted):
            self.turn_id, self.input = event.turn_id, event.input
        elif isinstance(event, SampleStarted):
            self.samples_used += 1
            self.batch = []
        elif isinstance(event, ToolCallRequested):
            self.batch.append(event)
        elif isinstance(event, ToolCallStarted):
            self.started.add(event.call_id)
        elif isinstance(event, ToolCallCompleted):
            self.results[event.call_id] = event
        elif isinstance(event, ChildSessionSpawned):
            self.children.setdefault(event.call_id, []).append(event)
        elif isinstance(event, AskRaised):
            self.asks.setdefault(event.call_id or "", []).append(Ask(ask_id=event.ask_id, request=event.request))
        elif isinstance(event, AskAnswered):
            for records in self.asks.values():
                for record in records:
                    if record.ask_id == event.ask_id:
                        record.response = event.response
        elif isinstance(event, CancelRequested):
            self.cancelled = True

    def unanswered(self) -> list[ToolCallRequested]:
        return [call for call in self.batch if call.call_id not in self.results]


def derive_turn_state(history: Sequence[SessionEvent]) -> TurnState:
    start = 0
    for index, event in enumerate(history):
        if isinstance(event, TurnStarted):
            start = index
    state = TurnState()
    for event in history[start:]:
        state.observe(event)
    return state


def is_retryable(exc: ProviderError) -> bool:
    if exc.retryable is True:
        return True
    status = exc.status_code
    return status is not None and (status == 429 or status >= 500)


def submit_output_schema(output_schema: type[BaseModel]) -> ToolSchema:
    return ToolSchema(
        name=SUBMIT_OUTPUT,
        description="Submit the final structured result for this turn. Calling this ends the turn.",
        parameters=output_schema.model_json_schema(),
    )


def accumulate(total: Usage, sample: Usage) -> Usage:
    return Usage(**{name: getattr(total, name) + getattr(sample, name) for name in Usage.model_fields})


class TurnLoop:
    def __init__(
        self,
        *,
        store: Store,
        provider: Provider,
        header: SessionHeader,
        agent: type[Agent],
        tools: dict[str, Tool],
        model: str,
        turn: TurnContext,
        history: Sequence[SessionEvent],
        retry: RetryConfig,
        holder: str,
        lease_ttl: float,
        hooks: Sequence[Hook] = (),
        default_permission: str = "allow",
        permission_chain: Sequence[Mapping[str, str]] = (),
        spawner: Spawner | None = None,
        skills_index: Sequence[SkillInfo] = (),
        memory: Memory | None = None,
        compactor: Compactor | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.header = header
        self.agent = agent
        self.tools = tools
        self.model = model
        self.turn = turn
        self.history: list[SessionEvent] = list(history)
        self.retry = retry
        self.holder = holder
        self.lease_ttl = lease_ttl
        self.hooks = list(hooks)
        self.default_permission = default_permission
        self.permission_chain = list(permission_chain)
        self.spawner = spawner
        self.skills_index = list(skills_index)
        self.memory = memory
        self.compactor = compactor
        self.turn.history = self.history
        self.turn.model = model
        self.turn.limits = provider.limits(model)
        self.turn.provider = provider
        self.failed = False
        self.lease_lost = False
        self.suspended: str | None = None
        self.stop: tuple[str, Any] | None = None
        self.done = False
        self.state = derive_turn_state(self.history)
        self.cursor: dict[str, int] = {}
        self.spawns: dict[str, int] = {}
        self.schemas = [t.schema for t in tools.values()]
        if agent.output_schema is not None:
            self.schemas = [*self.schemas, submit_output_schema(agent.output_schema)]

    async def _absorb(self) -> list[SessionEvent]:
        absorbed: list[SessionEvent] = []
        async for stamped in self.store.read(self.header.id, from_seq=self.header.last_seq):
            self.header.last_seq = stamped.seq
            self.history.append(stamped.event)
            self.state.observe(stamped.event)
            absorbed.append(stamped.event)
        return absorbed

    async def _append(self, events: Sequence[SessionEvent]) -> list[Emitted]:
        while True:
            try:
                last = await self.store.append(self.header.id, events, expect_seq=self.header.last_seq)
                break
            except SeqConflict:
                absorbed = await self._absorb()
                if not absorbed or any(not isinstance(event, CancelRequested) for event in absorbed):
                    raise
        first = last - len(events) + 1
        self.header.last_seq = last
        self.history.extend(events)
        for event in events:
            self.state.observe(event)
        return [
            Emitted(session_id=self.header.id, depth=self.header.depth, seq=first + offset, event=event)
            for offset, event in enumerate(events)
        ]

    async def _completed(self, call_id: str, result: Any, *, is_error: bool = False) -> list[Emitted]:
        completed = ToolCallCompleted(call_id=call_id, result=result, is_error=is_error)
        if call_id in self.state.started:
            return await self._append([completed])
        return await self._append([ToolCallStarted(call_id=call_id), completed])

    def _live(self, event: TextDelta | ReasoningDelta | ToolCallDelta) -> Emitted:
        return Emitted(session_id=self.header.id, depth=self.header.depth, seq=None, event=event)

    async def _ask(self, call_id: str, request: AskRequest) -> tuple[AskResponse | None, list[Emitted]]:
        index = self.cursor.get(call_id, 0)
        self.cursor[call_id] = index + 1
        records = self.state.asks.get(call_id, [])
        if index < len(records):
            record = records[index]
            if record.response is None:
                raise TantraError(f"ask {record.ask_id!r} is unanswered; resume with an answer for it")
            return record.response, []
        ask_id = uuid4().hex
        emitted = await self._append([AskRaised(ask_id=ask_id, call_id=call_id, request=request)])
        self.suspended = ask_id
        return None, emitted

    def _verdict(self, name: str, tool_permission: str | None) -> str:
        verdict = decide(name, self.agent.permissions, tool_permission, self.default_permission)
        for rules in self.permission_chain:
            verdict = strictest(verdict, decide(name, rules, None, self.default_permission))
        return verdict

    async def _attach(self, call_id: str, agent: Any) -> tuple[str, Exception | None, list[Emitted]]:
        try:
            name = self.spawner.resolve(agent)
        except Exception as exc:
            return "", exc, []
        index = self.spawns.get(call_id, 0)
        records = self.state.children.get(call_id, [])
        self.spawns[call_id] = index + 1
        if index < len(records):
            return records[index].child_session_id, None, []
        child = await self.spawner.create(name)
        spawned = ChildSessionSpawned(call_id=call_id, child_session_id=child, agent=name)
        return child, None, await self._append([spawned])

    async def _spawn(self, call_id: str, agent: Any, input: str, future: asyncio.Future[Any]) -> AsyncIterator[Emitted]:
        child, error, spawned = await self._attach(call_id, agent)
        for emitted in spawned:
            yield emitted
        if error is not None:
            future.set_exception(error)
            return
        try:
            async with aclosing(self.spawner.drive(child, input)) as stream:
                async for emitted in stream:
                    yield emitted
            outcome = await self.spawner.outcome(child)
        except SessionBusy:
            raise
        except Exception as exc:
            future.set_exception(exc)
            return
        if outcome.pending_ask is not None:
            self.suspended = outcome.pending_ask
        elif outcome.error is not None:
            future.set_exception(outcome.error)
        else:
            future.set_result(outcome.result)

    async def _merge(
        self,
        plan: Sequence[tuple[int, str, str]],
        max_concurrency: int,
        slots: list[Any],
        waiting: dict[int, str],
    ) -> AsyncIterator[Emitted]:
        queue: asyncio.Queue[Emitted] = asyncio.Queue()
        gate = asyncio.Semaphore(max(1, max_concurrency))

        async def child(index: int, sid: str, task_input: str) -> None:
            async with gate:
                try:
                    async with aclosing(self.spawner.drive(sid, task_input)) as stream:
                        async for emitted in stream:
                            await queue.put(emitted)
                    outcome = await self.spawner.outcome(sid)
                except SessionBusy:
                    raise
                except Exception as exc:
                    slots[index] = exc
                    return
            if outcome.pending_ask is not None:
                waiting[index] = outcome.pending_ask
            elif outcome.error is not None:
                slots[index] = outcome.error
            else:
                slots[index] = outcome.result

        workers = [asyncio.ensure_future(child(*entry)) for entry in plan]
        gathered = asyncio.ensure_future(asyncio.gather(*workers))
        getter: asyncio.Future[Emitted] | None = None
        try:
            while True:
                getter = asyncio.ensure_future(queue.get())
                finished, _ = await asyncio.wait({getter, gathered}, return_when=asyncio.FIRST_COMPLETED)
                if getter in finished:
                    yield getter.result()
                    continue
                getter.cancel()
                getter = None
                break
            while not queue.empty():
                yield queue.get_nowait()
            await gathered
        finally:
            for pending in (*workers, gathered, getter):
                if pending is not None:
                    pending.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*workers, gathered, return_exceptions=True)

    async def _fan_out(
        self,
        call_id: str,
        tasks: Sequence[tuple[Any, str]],
        max_concurrency: int,
        future: asyncio.Future[Any],
    ) -> AsyncIterator[Emitted]:
        slots: list[Any] = [None] * len(tasks)
        plan: list[tuple[int, str, str]] = []
        for index, (agent, task_input) in enumerate(tasks):
            child, error, spawned = await self._attach(call_id, agent)
            for emitted in spawned:
                yield emitted
            if error is not None:
                slots[index] = error
            else:
                plan.append((index, child, task_input))
        waiting: dict[int, str] = {}
        async with aclosing(self._merge(plan, max_concurrency, slots, waiting)) as merged:
            async for emitted in merged:
                yield emitted
        if waiting:
            self.suspended = waiting[min(waiting)]
            return
        future.set_result(slots)

    async def _sample(self, req: SampleRequest) -> AsyncIterator[Emitted | StreamEnd]:
        for attempt in range(self.retry.max_attempts):
            end: StreamEnd | None = None
            try:
                async for event in self.provider.stream(req):
                    if isinstance(event, TextDelta | ReasoningDelta | ToolCallDelta):
                        yield self._live(event)
                    elif isinstance(event, StreamEnd):
                        end = event
                if end is None:
                    raise ProviderError("provider stream ended without a StreamEnd")
            except ProviderError as exc:
                if attempt + 1 >= self.retry.max_attempts or not is_retryable(exc):
                    raise
                await asyncio.sleep(min(self.retry.base_delay * 2**attempt, self.retry.max_delay))
                continue
            yield end
            return

    async def _execute(self, tool: Tool, call: ToolCallRequested, args: dict[str, Any]) -> AsyncIterator[Emitted]:
        call_id = call.call_id
        queue: asyncio.Queue[tuple[str, Any, asyncio.Future[Any] | None]] = asyncio.Queue()

        async def emit(message: str) -> None:
            await queue.put(("progress", message, None))

        async def request(kind: str, payload: Any) -> Any:
            future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            await queue.put((kind, payload, future))
            return await future

        async def ask(asked: AskRequest) -> AskResponse:
            return await request("ask", asked)

        async def spawn(agent: Any, input: str) -> Any:
            return await request("spawn", (agent, input))

        async def fan_out(tasks: Any, max_concurrency: int = 4) -> list[Any]:
            return await request("fan_out", (tasks, max_concurrency))

        delegates = self.spawner is not None
        ctx = Context(
            session_id=self.header.id,
            turn_id=self.turn.turn_id,
            call_id=call_id,
            depth=self.header.depth,
            deps=self.turn.deps,
            store=self.store,
            emit=emit,
            ask=ask,
            spawn=spawn if delegates else None,
            fan_out=fan_out if delegates else None,
            memory=self.memory,
        )
        task = asyncio.ensure_future(tool.invoke(args, ctx))
        getter: asyncio.Future[tuple[str, Any, asyncio.Future[Any] | None]] | None = None
        try:
            while True:
                getter = asyncio.ensure_future(queue.get())
                finished, _ = await asyncio.wait({task, getter}, return_when=asyncio.FIRST_COMPLETED)
                if getter in finished:
                    kind, payload, future = getter.result()
                    if kind == "progress":
                        for emitted in await self._append([ToolProgress(call_id=call_id, message=payload)]):
                            yield emitted
                        continue
                    if kind in ("spawn", "fan_out"):
                        delegation = (
                            self._spawn(call_id, *payload, future)
                            if kind == "spawn"
                            else self._fan_out(call_id, *payload, future)
                        )
                        async with aclosing(delegation) as delegated:
                            async for emitted in delegated:
                                yield emitted
                        if self.suspended is not None:
                            return
                        continue
                    response, raised = await self._ask(call_id, payload)
                    for emitted in raised:
                        yield emitted
                    if response is None:
                        return
                    assert future is not None
                    future.set_result(response)
                    continue
                getter.cancel()
                break
            while not queue.empty():
                kind, payload, _future = queue.get_nowait()
                if kind != "progress":
                    continue
                for emitted in await self._append([ToolProgress(call_id=call_id, message=payload)]):
                    yield emitted
            try:
                result: Any = await task
                is_error = False
            except Exception as exc:
                result, is_error = str(exc), True
            for hook in self.hooks:
                transformed = await hook.after_tool(call, result, is_error, self.turn)
                if transformed is not None:
                    result = transformed
            for emitted in await self._completed(call_id, result, is_error=is_error):
                yield emitted
        finally:
            pending = [future for future in (task, getter) if future is not None]
            for future in pending:
                future.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*pending, return_exceptions=True)

    def _parts(self, sample_id: str, end: StreamEnd) -> list[SessionEvent]:
        parts: list[SessionEvent] = []
        rejected: list[SessionEvent] = []
        for block in end.reasoning:
            parts.append(ReasoningPart(sample_id=sample_id, text=block.text, signature=block.signature))
        if end.text:
            parts.append(TextPart(sample_id=sample_id, text=end.text))
        for call in end.tool_calls:
            try:
                args = json.loads(call.args) if call.args.strip() else {}
                if not isinstance(args, dict):
                    raise ValueError("arguments must be a JSON object")
            except ValueError as exc:
                args = {}
                if call.id not in self.state.started:
                    rejected.append(ToolCallStarted(call_id=call.id))
                rejected.append(
                    ToolCallCompleted(call_id=call.id, result=f"invalid JSON arguments: {exc}", is_error=True)
                )
            parts.append(ToolCallRequested(sample_id=sample_id, call_id=call.id, name=call.name, args=args))
        parts.append(SampleCompleted(sample_id=sample_id, usage=end.usage, finish_reason=end.finish_reason))
        return parts + rejected

    async def _submit(self, call_id: str, args: dict[str, Any]) -> tuple[list[Emitted], Any, bool]:
        schema = self.agent.output_schema
        assert schema is not None
        try:
            value = schema.model_validate(args)
        except ValidationError as exc:
            return await self._completed(call_id, f"invalid output: {exc}", is_error=True), None, False
        output = value.model_dump(mode="json")
        return await self._completed(call_id, output), output, True

    def _submitted_output(self) -> tuple[bool, Any]:
        if self.agent.output_schema is None:
            return False, None
        for call in self.state.batch:
            result = self.state.results.get(call.call_id)
            if call.name == SUBMIT_OUTPUT and result is not None and not result.is_error:
                return True, result.result
        return False, None

    async def _synthesize(self, reason: str) -> AsyncIterator[Emitted]:
        pending = self.state.unanswered()
        if not pending:
            return
        events: list[SessionEvent] = []
        for call in pending:
            if call.call_id not in self.state.started:
                events.append(ToolCallStarted(call_id=call.call_id))
            events.append(ToolCallCompleted(call_id=call.call_id, result=reason, is_error=True))
        for emitted in await self._append(events):
            yield emitted

    async def _failed(self, exc: ProviderError) -> AsyncIterator[Emitted]:
        self.failed = True
        failure = TurnFailed(turn_id=self.turn.turn_id, error=str(exc))
        for emitted in await self._append([failure]):
            yield emitted
        for hook in self.hooks:
            await hook.after_turn(self.turn, failure)

    async def _terminal(self, reason: str, output: Any) -> AsyncIterator[Emitted]:
        event = TurnCompleted(turn_id=self.turn.turn_id, stop_reason=reason, output=output)
        for emitted in await self._append([event]):
            yield emitted
        for hook in self.hooks:
            await hook.after_turn(self.turn, event)

    async def _cancel(self) -> AsyncIterator[Emitted]:
        async with aclosing(self._synthesize(CANCELLED_RESULT)) as orphans:
            async for emitted in orphans:
                yield emitted
        async with aclosing(self._terminal("cancelled", None)) as terminal:
            async for emitted in terminal:
                yield emitted

    async def _batch(self, capped: bool) -> AsyncIterator[Emitted]:
        stopped = False
        for call in list(self.state.batch):
            if call.call_id in self.state.results:
                continue
            if not stopped:
                await self._absorb()
                if self.state.cancelled:
                    return
            if stopped:
                for emitted in await self._completed(call.call_id, COMPLETED_RESULT, is_error=True):
                    yield emitted
                continue
            if call.name == SUBMIT_OUTPUT and self.agent.output_schema is not None:
                events, output, stopped = await self._submit(call.call_id, call.args)
                for emitted in events:
                    yield emitted
                if stopped:
                    self.stop = ("output", output)
                continue
            if capped:
                for emitted in await self._completed(call.call_id, "not executed: max steps reached", is_error=True):
                    yield emitted
                continue
            tool = self.tools.get(call.name)
            if tool is None:
                for emitted in await self._completed(call.call_id, f"unknown tool {call.name!r}", is_error=True):
                    yield emitted
                continue
            effective = call
            denial: Denial | None = None
            escalation: Escalation | None = None
            for hook in self.hooks:
                outcome = await hook.before_tool(effective, self.turn)
                if isinstance(outcome, Denial):
                    denial = outcome
                    break
                if isinstance(outcome, Escalation):
                    escalation = escalation or outcome
                    continue
                if outcome is not None:
                    effective = outcome
            if denial is not None:
                for emitted in await self._completed(call.call_id, f"denied by hook: {denial.reason}", is_error=True):
                    yield emitted
                continue
            verdict = self._verdict(call.name, tool.permission)
            if escalation is not None:
                verdict = strictest(verdict, "ask")
            if verdict == "deny":
                denied = f"denied by permissions: {call.name}"
                for emitted in await self._completed(call.call_id, denied, is_error=True):
                    yield emitted
                continue
            if verdict == "ask":
                body = json.dumps(effective.args, default=str)
                if escalation is not None:
                    body = f"{escalation.reason}\n\n{body}"
                request = Approval(
                    title=f"Run {call.name}?",
                    body=body,
                    extra={"permission": call.name},
                )
                response, raised = await self._ask(call.call_id, request)
                for emitted in raised:
                    yield emitted
                if response is None:
                    return
                if not (isinstance(response, ApprovalResponse) and response.allow):
                    for emitted in await self._completed(call.call_id, "denied by user", is_error=True):
                        yield emitted
                    continue
            if call.call_id not in self.state.started:
                for emitted in await self._append([ToolCallStarted(call_id=call.call_id)]):
                    yield emitted
            async with aclosing(self._execute(tool, call, effective.args)) as execution:
                async for emitted in execution:
                    yield emitted
            if self.suspended is not None:
                return

    async def _after_batch(self, capped: bool) -> AsyncIterator[Emitted]:
        if self.suspended is not None or self.failed:
            self.done = True
            return
        if self.state.cancelled:
            self.done = True
            async for emitted in self._cancel():
                yield emitted
            return
        if self.stop is not None:
            self.done = True
            async for emitted in self._terminal(*self.stop):
                yield emitted
            return
        if capped:
            self.done = True
            async for emitted in self._terminal("max_steps", None):
                yield emitted

    async def _drive(self) -> AsyncIterator[Emitted]:
        state = self.state
        if state.cancelled:
            async for emitted in self._cancel():
                yield emitted
            return

        if state.batch:
            submitted, output = self._submitted_output()
            if submitted:
                async with aclosing(self._synthesize(COMPLETED_RESULT)) as orphans:
                    async for emitted in orphans:
                        yield emitted
                if state.cancelled:
                    async for emitted in self._cancel():
                        yield emitted
                    return
                async for emitted in self._terminal("output", output):
                    yield emitted
                return
            if state.unanswered():
                capped = state.samples_used >= self.agent.max_steps
                async with aclosing(self._batch(capped)) as batch:
                    async for emitted in batch:
                        yield emitted
                async for emitted in self._after_batch(capped):
                    yield emitted
                if self.done:
                    return

        while state.samples_used < self.agent.max_steps:
            if not await self.store.acquire_lease(self.header.id, self.holder, self.lease_ttl):
                self.lease_lost = True
                raise TantraError(f"lease lost: session {self.header.id} is held by another writer")
            await self._absorb()
            if state.cancelled:
                async for emitted in self._cancel():
                    yield emitted
                return

            for hook in self.hooks:
                await hook.before_sample(self.turn)

            if self.compactor is not None:
                try:
                    compacted = await self.compactor.compact(self.turn)
                except ProviderError as exc:
                    async for emitted in self._failed(exc):
                        yield emitted
                    return
                if compacted:
                    for emitted in await self._append(compacted):
                        yield emitted

            sample_id = uuid4().hex
            prompt = await resolve_prompt(self.agent.prompt, self.turn)
            req = build_sample_request(
                model=self.model,
                prompt=prompt,
                events=self.history,
                tools=self.schemas,
                skills=self.skills_index,
            )
            for emitted in await self._append(
                [SampleStarted(turn_id=self.turn.turn_id, sample_id=sample_id, model=self.model)]
            ):
                yield emitted

            end: StreamEnd | None = None
            try:
                async with aclosing(self._sample(req)) as stream:
                    async for item in stream:
                        if isinstance(item, StreamEnd):
                            end = item
                        else:
                            yield item
            except ProviderError as exc:
                async for emitted in self._failed(exc):
                    yield emitted
                return

            for emitted in await self._append(self._parts(sample_id, end)):
                yield emitted
            self.header.usage = accumulate(self.header.usage, end.usage)
            self.header.updated_at = datetime.now(UTC)
            await self.store.put_header(self.header)

            if not end.tool_calls:
                if state.cancelled:
                    async for emitted in self._cancel():
                        yield emitted
                    return
                async for emitted in self._terminal("completed", None):
                    yield emitted
                return

            capped = state.samples_used >= self.agent.max_steps
            async with aclosing(self._batch(capped)) as batch:
                async for emitted in batch:
                    yield emitted
            async for emitted in self._after_batch(capped):
                yield emitted
            if self.done:
                return

        async for emitted in self._terminal("max_steps", None):
            yield emitted

    async def run(self) -> AsyncIterator[Emitted]:
        async with aclosing(self._drive()) as stream:
            async for emitted in stream:
                if emitted.session_id == self.header.id:
                    for hook in self.hooks:
                        await hook.on_event(emitted)
                yield emitted
