from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import aclosing, suppress
from dataclasses import dataclass, field
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
    CompactionApplied,
    InputQueued,
    ReasoningPart,
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    Stamped,
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
from tantra.tracing import NULL_TRACER, Tracer, current_span

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
        tracer: Tracer = NULL_TRACER,
        turn_span: Any = None,
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
        self.tracer = tracer
        self.turn_span = turn_span
        self.tool_spans: dict[str, Any] = {}
        self.terminal: TurnCompleted | TurnFailed | None = None
        self.turn.history = self.history
        self.turn.model = model
        self.turn.limits = provider.limits(model)
        self.turn.provider = provider
        self.turn.tracer = tracer
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

    def _stub_span(self, call: ToolCallRequested, result: Any, *, is_error: bool, error_type: str | None) -> None:
        span = self.tracer.start_tool(
            self.turn_span,
            call,
            args=call.args,
            tool=self.tools.get(call.name),
            replayed=call.call_id in self.state.started,
        )
        self.tracer.end_tool(
            span,
            result=result,
            is_error=is_error,
            outcome="error" if is_error else "completed",
            error_type=error_type,
            ask_id=None,
        )

    async def _completed(
        self,
        call_id: str,
        result: Any,
        *,
        is_error: bool = False,
        error_type: str | None = None,
        call: ToolCallRequested | None = None,
    ) -> list[Emitted]:
        if is_error and error_type is None:
            error_type = "_OTHER"
        if call_id in self.tool_spans:
            self.tracer.end_tool(
                self.tool_spans.pop(call_id),
                result=result,
                is_error=is_error,
                outcome="error" if is_error else "completed",
                error_type=error_type,
                ask_id=None,
            )
        elif call is not None:
            self._stub_span(call, result, is_error=is_error, error_type=error_type)
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
        previous = current_span.get()
        current_span.set(self.tool_spans.get(call_id))
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
        finally:
            current_span.set(previous)
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
        previous = current_span.get()
        current_span.set(self.tool_spans.get(call_id))
        try:
            async with aclosing(self._merge(plan, max_concurrency, slots, waiting)) as merged:
                async for emitted in merged:
                    yield emitted
        finally:
            current_span.set(previous)
        if waiting:
            self.suspended = waiting[min(waiting)]
            return
        future.set_result(slots)

    async def _sample(
        self, req: SampleRequest, *, sample_id: str, compacted: bool
    ) -> AsyncIterator[Emitted | StreamEnd]:
        span = self.tracer.start_sample(
            self.turn_span, req, sample_id=sample_id, provider=self.provider, compacted=compacted
        )
        end: StreamEnd | None = None
        error: BaseException | None = None
        attempts = 0
        try:
            for attempt in range(self.retry.max_attempts):
                attempts = attempt + 1
                end = None
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
        except BaseException as exc:
            error = exc
            raise
        finally:
            self.tracer.end_sample(span, end=end, error=error, attempts=attempts)

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
                error_type: str | None = None
            except Exception as exc:
                result, is_error, error_type = str(exc), True, type(exc).__qualname__
            for hook in self.hooks:
                transformed = await hook.after_tool(call, result, is_error, self.turn)
                if transformed is not None:
                    result = transformed
            for emitted in await self._completed(call_id, result, is_error=is_error, error_type=error_type, call=call):
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
                invalid = f"invalid JSON arguments: {exc}"
                requested = ToolCallRequested(sample_id=sample_id, call_id=call.id, name=call.name, args=args)
                self._stub_span(requested, invalid, is_error=True, error_type="_OTHER")
                if call.id not in self.state.started:
                    rejected.append(ToolCallStarted(call_id=call.id))
                rejected.append(ToolCallCompleted(call_id=call.id, result=invalid, is_error=True))
            parts.append(ToolCallRequested(sample_id=sample_id, call_id=call.id, name=call.name, args=args))
        parts.append(SampleCompleted(sample_id=sample_id, usage=end.usage, finish_reason=end.finish_reason))
        return parts + rejected

    async def _submit(self, call: ToolCallRequested) -> tuple[list[Emitted], Any, bool]:
        schema = self.agent.output_schema
        assert schema is not None
        try:
            value = schema.model_validate(call.args)
        except ValidationError as exc:
            events = await self._completed(call.call_id, f"invalid output: {exc}", is_error=True, call=call)
            return events, None, False
        output = value.model_dump(mode="json")
        return await self._completed(call.call_id, output, call=call), output, True

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
            self._stub_span(call, reason, is_error=True, error_type="_OTHER")
            if call.call_id not in self.state.started:
                events.append(ToolCallStarted(call_id=call.call_id))
            events.append(ToolCallCompleted(call_id=call.call_id, result=reason, is_error=True))
        for emitted in await self._append(events):
            yield emitted

    async def _failed(self, exc: ProviderError) -> AsyncIterator[Emitted]:
        self.failed = True
        failure = TurnFailed(turn_id=self.turn.turn_id, error=str(exc))
        appended = await self._append([failure])
        self.terminal = failure
        for emitted in appended:
            yield emitted
        for hook in self.hooks:
            await hook.after_turn(self.turn, failure)

    async def _terminal(self, reason: str, output: Any) -> AsyncIterator[Emitted]:
        event = TurnCompleted(turn_id=self.turn.turn_id, stop_reason=reason, output=output)
        appended = await self._append([event])
        self.terminal = event
        for emitted in appended:
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
                for emitted in await self._completed(call.call_id, COMPLETED_RESULT, is_error=True, call=call):
                    yield emitted
                continue
            if call.name == SUBMIT_OUTPUT and self.agent.output_schema is not None:
                events, output, stopped = await self._submit(call)
                for emitted in events:
                    yield emitted
                if stopped:
                    self.stop = ("output", output)
                continue
            if capped:
                capped_result = "not executed: max steps reached"
                for emitted in await self._completed(call.call_id, capped_result, is_error=True, call=call):
                    yield emitted
                continue
            tool = self.tools.get(call.name)
            if tool is None:
                unknown = f"unknown tool {call.name!r}"
                for emitted in await self._completed(call.call_id, unknown, is_error=True, call=call):
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
                refused = f"denied by hook: {denial.reason}"
                for emitted in await self._completed(call.call_id, refused, is_error=True, call=effective):
                    yield emitted
                continue
            verdict = self._verdict(call.name, tool.permission)
            if escalation is not None:
                verdict = strictest(verdict, "ask")
            if verdict == "deny":
                denied = f"denied by permissions: {call.name}"
                for emitted in await self._completed(call.call_id, denied, is_error=True, call=effective):
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
                    for emitted in await self._completed(call.call_id, "denied by user", is_error=True, call=effective):
                        yield emitted
                    continue
            replayed = call.call_id in self.state.started
            if call.call_id not in self.state.started:
                for emitted in await self._append([ToolCallStarted(call_id=call.call_id)]):
                    yield emitted
            self.tool_spans[call.call_id] = self.tracer.start_tool(
                self.turn_span, call, args=effective.args, tool=tool, replayed=replayed
            )
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

            compacted: list[SessionEvent] = []
            if self.compactor is not None:
                span = self.tracer.start_compaction(self.turn_span)
                previous = current_span.get()
                current_span.set(span)
                failure: ProviderError | None = None
                error: BaseException | None = None
                try:
                    compacted = await self.compactor.compact(self.turn)
                except ProviderError as exc:
                    failure = error = exc
                except BaseException as exc:
                    error = exc
                    raise
                finally:
                    current_span.set(previous)
                    applied = next((event for event in compacted if isinstance(event, CompactionApplied)), None)
                    self.tracer.end_compaction(span, applied=applied, error=error)
                if failure is not None:
                    async for emitted in self._failed(failure):
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
                async with aclosing(self._sample(req, sample_id=sample_id, compacted=bool(compacted))) as stream:
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
            await self.store.patch_header(self.header.id, usage=self.header.usage)

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
        try:
            async with aclosing(self._drive()) as stream:
                async for emitted in stream:
                    if emitted.session_id == self.header.id:
                        for hook in self.hooks:
                            await hook.on_event(emitted)
                    yield emitted
        finally:
            outcome = "suspended" if self.suspended is not None else "aborted"
            while self.tool_spans:
                _, span = self.tool_spans.popitem()
                self.tracer.end_tool(
                    span,
                    result=None,
                    is_error=False,
                    outcome=outcome,
                    error_type=None,
                    ask_id=self.suspended,
                )


@dataclass
class _PreparedCall:
    call: ToolCallRequested
    effective: ToolCallRequested
    tool: Tool
    kwargs: dict[str, Any]
    span: Any = None


_NO_OUTPUT = object()


class TurnEngine:
    def __init__(
        self,
        *,
        store: Store,
        provider: Provider,
        header: SessionHeader,
        agent: type[Agent],
        tools: dict[str, Tool],
        model: str,
        history: Sequence[SessionEvent] | None = None,
        deps: Any = None,
        retry: RetryConfig = DEFAULT_RETRY,
        hooks: Sequence[Hook] = (),
        default_permission: str = "allow",
        permission_chain: Sequence[Mapping[str, str]] = (),
        skills_index: Sequence[SkillInfo] = (),
        memory: Memory | None = None,
        compactor: Compactor | None = None,
        tracer: Tracer = NULL_TRACER,
        notify: Callable[[Stamped], Any] | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.header = header
        self.agent = agent
        self.tools = tools
        self.model = model
        self.history = list(history) if history is not None else None
        self.deps = deps
        self.retry = retry
        self.hooks = list(hooks)
        self.default_permission = default_permission
        self.permission_chain = list(permission_chain)
        self.skills_index = list(skills_index)
        self.memory = memory
        self.compactor = compactor
        self.tracer = tracer
        self.notify = notify
        self.schemas = [tool.schema for tool in tools.values()]
        if agent.output_schema is not None:
            self.schemas.append(submit_output_schema(agent.output_schema))
        self._append_lock = asyncio.Lock()
        self.turn: TurnContext | None = None
        self.turn_span: Any = None
        self.terminal: TurnCompleted | TurnFailed | None = None
        self.tool_spans: dict[object, Any] = {}

    async def _load_history(self) -> list[SessionEvent]:
        if self.history is not None:
            return self.history
        history: list[SessionEvent] = []
        after = 0
        while True:
            page = await self.store.read_page(self.header.id, after=after)
            if not page:
                break
            history.extend(item.event for item in page)
            after = page[-1].seq
        self.history = history
        return history

    async def _append(self, events: Sequence[SessionEvent]) -> list[Stamped]:
        if not events:
            return []
        async with self._append_lock:
            last = await self.store.append(self.header.id, events, expect_seq=None)
            first = last - len(events) + 1
            stamped = [Stamped(seq=first + index, event=event) for index, event in enumerate(events)]
            self.header.last_seq = last
            assert self.history is not None
            self.history.extend(events)
            if self.notify is not None:
                for item in stamped:
                    notified = self.notify(item)
                    if inspect.isawaitable(notified):
                        await notified
            for item in stamped:
                emitted = Emitted(
                    session_id=self.header.id,
                    depth=self.header.depth,
                    seq=item.seq,
                    event=item.event,
                )
                for hook in self.hooks:
                    await hook.on_event(emitted)
            return stamped

    def _verdict(self, name: str, tool_permission: str | None) -> str:
        verdict = decide(name, self.agent.permissions, tool_permission, self.default_permission)
        for rules in self.permission_chain:
            verdict = strictest(verdict, decide(name, rules, None, self.default_permission))
        return verdict

    async def _finish(self, terminal: TurnCompleted | TurnFailed) -> TurnCompleted | TurnFailed:
        await self._append([terminal])
        self.terminal = terminal
        assert self.turn is not None
        for hook in self.hooks:
            await hook.after_turn(self.turn, terminal)
        return terminal

    async def _sample(self, req: SampleRequest, sample_id: str, compacted: bool) -> StreamEnd:
        span = self.tracer.start_sample(
            self.turn_span,
            req,
            sample_id=sample_id,
            provider=self.provider,
            compacted=compacted,
        )
        end: StreamEnd | None = None
        error: BaseException | None = None
        attempts = 0
        try:
            for attempt in range(self.retry.max_attempts):
                attempts = attempt + 1
                end = None
                try:
                    async for event in self.provider.stream(req):
                        if isinstance(event, TextDelta | ReasoningDelta | ToolCallDelta):
                            await self._append([event])
                        elif isinstance(event, StreamEnd):
                            end = event
                    if end is None:
                        raise ProviderError("provider stream ended without a StreamEnd")
                except ProviderError as exc:
                    if attempt + 1 >= self.retry.max_attempts or not is_retryable(exc):
                        raise
                    await asyncio.sleep(min(self.retry.base_delay * 2**attempt, self.retry.max_delay))
                    continue
                return end
            raise ProviderError("provider retry loop exhausted")
        except BaseException as exc:
            error = exc
            raise
        finally:
            self.tracer.end_sample(span, end=end, error=error, attempts=attempts)

    def _parts(
        self,
        sample_id: str,
        end: StreamEnd,
    ) -> tuple[list[SessionEvent], list[ToolCallRequested], set[str]]:
        events: list[SessionEvent] = []
        calls: list[ToolCallRequested] = []
        invalid: set[str] = set()
        for block in end.reasoning:
            events.append(ReasoningPart(sample_id=sample_id, text=block.text, signature=block.signature))
        if end.text:
            events.append(TextPart(sample_id=sample_id, text=end.text))
        rejected: list[SessionEvent] = []
        for call in end.tool_calls:
            try:
                args = json.loads(call.args) if call.args.strip() else {}
                if not isinstance(args, dict):
                    raise ValueError("arguments must be a JSON object")
            except ValueError as exc:
                args = {}
                invalid.add(call.id)
                rejected.extend(
                    [
                        ToolCallStarted(call_id=call.id),
                        ToolCallCompleted(
                            call_id=call.id,
                            result=f"invalid JSON arguments: {exc}",
                            is_error=True,
                        ),
                    ]
                )
            requested = ToolCallRequested(
                sample_id=sample_id,
                call_id=call.id,
                name=call.name,
                args=args,
            )
            events.append(requested)
            calls.append(requested)
        events.append(SampleCompleted(sample_id=sample_id, usage=end.usage, finish_reason=end.finish_reason))
        events.extend(rejected)
        return events, calls, invalid

    async def _complete(
        self,
        call: ToolCallRequested,
        result: Any,
        *,
        is_error: bool,
        tool: Tool | None = None,
        args: dict[str, Any] | None = None,
        error_type: str | None = None,
    ) -> None:
        span = self.tracer.start_tool(
            self.turn_span,
            call,
            args=call.args if args is None else args,
            tool=tool,
            replayed=False,
        )
        span_key = object()
        self.tool_spans[span_key] = span
        await self._append(
            [
                ToolCallStarted(call_id=call.call_id),
                ToolCallCompleted(call_id=call.call_id, result=result, is_error=is_error),
            ]
        )
        span = self.tool_spans.pop(span_key)
        self.tracer.end_tool(
            span,
            result=result,
            is_error=is_error,
            outcome="error" if is_error else "completed",
            error_type=error_type or ("_OTHER" if is_error else None),
            ask_id=None,
        )

    async def _preflight(
        self,
        calls: Sequence[ToolCallRequested],
        invalid: set[str],
        capped: bool,
    ) -> tuple[list[_PreparedCall], Any]:
        prepared: list[_PreparedCall] = []
        output: Any = _NO_OUTPUT
        stopped = False
        for call in calls:
            if call.call_id in invalid:
                continue
            if stopped:
                await self._complete(call, COMPLETED_RESULT, is_error=True)
                continue
            if call.name == SUBMIT_OUTPUT and self.agent.output_schema is not None:
                try:
                    value = self.agent.output_schema.model_validate(call.args)
                except ValidationError as exc:
                    await self._complete(call, f"invalid output: {exc}", is_error=True)
                else:
                    output = value.model_dump(mode="json")
                    await self._complete(call, output, is_error=False)
                    stopped = True
                continue
            if capped:
                await self._complete(call, "not executed: max steps reached", is_error=True)
                continue
            tool = self.tools.get(call.name)
            if tool is None:
                await self._complete(call, f"unknown tool {call.name!r}", is_error=True)
                continue
            effective = call
            denial: Denial | None = None
            escalation: Escalation | None = None
            assert self.turn is not None
            for hook in self.hooks:
                outcome = await hook.before_tool(effective, self.turn)
                if isinstance(outcome, Denial):
                    denial = outcome
                    break
                if isinstance(outcome, Escalation):
                    escalation = escalation or outcome
                elif outcome is not None:
                    effective = outcome
            if denial is not None:
                await self._complete(
                    call,
                    f"denied by hook: {denial.reason}",
                    is_error=True,
                    tool=tool,
                    args=effective.args,
                )
                continue
            verdict = self._verdict(call.name, tool.permission)
            if escalation is not None:
                verdict = strictest(verdict, "ask")
            if verdict == "deny":
                await self._complete(
                    call,
                    f"denied by permissions: {call.name}",
                    is_error=True,
                    tool=tool,
                    args=effective.args,
                )
                continue
            if verdict == "ask":
                await self._complete(
                    call,
                    "not executed: approval unavailable",
                    is_error=True,
                    tool=tool,
                    args=effective.args,
                )
                continue
            try:
                validated = tool.args_model.model_validate(effective.args)
            except ValidationError as exc:
                await self._complete(
                    call,
                    str(exc),
                    is_error=True,
                    tool=tool,
                    args=effective.args,
                    error_type=type(exc).__qualname__,
                )
                continue
            kwargs = {field: getattr(validated, field) for field in tool.args_model.model_fields}
            prepared.append(_PreparedCall(call=call, effective=effective, tool=tool, kwargs=kwargs))
        return prepared, output

    async def _invoke(self, prepared: _PreparedCall) -> None:
        call = prepared.call

        async def emit(message: str) -> None:
            await self._append([ToolProgress(call_id=call.call_id, message=message)])

        assert self.turn is not None
        ctx = Context(
            session_id=self.header.id,
            turn_id=self.turn.turn_id,
            call_id=call.call_id,
            depth=self.header.depth,
            deps=self.turn.deps,
            store=self.store,
            emit=emit,
            memory=self.memory,
        )
        kwargs = dict(prepared.kwargs)
        if prepared.tool.ctx_param is not None:
            kwargs[prepared.tool.ctx_param] = ctx
        result: Any
        is_error = False
        error_type: str | None = None
        try:
            if inspect.iscoroutinefunction(prepared.tool.fn):
                result = prepared.tool.fn(**kwargs)
            else:
                result = await asyncio.to_thread(prepared.tool.fn, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            result = str(exc)
            is_error = True
            error_type = type(exc).__qualname__
        for hook in self.hooks:
            transformed = await hook.after_tool(call, result, is_error, self.turn)
            if transformed is not None:
                result = transformed
        await self._append([ToolCallCompleted(call_id=call.call_id, result=result, is_error=is_error)])
        span = self.tool_spans.pop(id(prepared))
        self.tracer.end_tool(
            span,
            result=result,
            is_error=is_error,
            outcome="error" if is_error else "completed",
            error_type=error_type,
            ask_id=None,
        )

    async def _batch(
        self,
        calls: Sequence[ToolCallRequested],
        invalid: set[str],
        capped: bool,
    ) -> Any:
        prepared, output = await self._preflight(calls, invalid, capped)
        if prepared:
            await self._append([ToolCallStarted(call_id=item.call.call_id) for item in prepared])
            for item in prepared:
                item.span = self.tracer.start_tool(
                    self.turn_span,
                    item.call,
                    args=item.effective.args,
                    tool=item.tool,
                    replayed=False,
                )
                self.tool_spans[id(item)] = item.span
            tasks = [asyncio.create_task(self._invoke(item)) for item in prepared]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        return output

    async def _compact(self) -> bool:
        if self.compactor is None:
            return False
        span = self.tracer.start_compaction(self.turn_span)
        previous = current_span.get()
        current_span.set(span)
        events: list[SessionEvent] = []
        error: BaseException | None = None
        try:
            assert self.turn is not None
            events = await self.compactor.compact(self.turn)
        except BaseException as exc:
            error = exc
            raise
        finally:
            current_span.set(previous)
            applied = next((event for event in events if isinstance(event, CompactionApplied)), None)
            self.tracer.end_compaction(span, applied=applied, error=error)
        await self._append(events)
        return bool(events)

    async def _drive(self) -> TurnCompleted | TurnFailed:
        assert self.turn is not None
        assert self.history is not None
        for sample_number in range(self.agent.max_steps):
            for hook in self.hooks:
                await hook.before_sample(self.turn)
            compacted = await self._compact()
            sample_id = uuid4().hex
            prompt = await resolve_prompt(self.agent.prompt, self.turn)
            req = build_sample_request(
                model=self.model,
                prompt=prompt,
                events=self.history,
                tools=self.schemas,
                skills=self.skills_index,
            )
            await self._append([SampleStarted(turn_id=self.turn.turn_id, sample_id=sample_id, model=self.model)])
            end = await self._sample(req, sample_id, compacted)
            parts, calls, invalid = self._parts(sample_id, end)
            await self._append(parts)
            self.header.usage = accumulate(self.header.usage, end.usage)
            await self.store.patch_header(self.header.id, usage=self.header.usage)
            if not calls:
                return await self._finish(TurnCompleted(turn_id=self.turn.turn_id, stop_reason="completed"))
            capped = sample_number + 1 >= self.agent.max_steps
            output = await self._batch(calls, invalid, capped)
            if output is not _NO_OUTPUT:
                return await self._finish(
                    TurnCompleted(
                        turn_id=self.turn.turn_id,
                        stop_reason="output",
                        output=output,
                    )
                )
            if capped:
                return await self._finish(TurnCompleted(turn_id=self.turn.turn_id, stop_reason="max_steps"))
        return await self._finish(TurnCompleted(turn_id=self.turn.turn_id, stop_reason="max_steps"))

    def _final_text(self) -> str:
        assert self.history is not None
        sample_id = ""
        texts: dict[str, list[str]] = {}
        for event in self.history:
            if isinstance(event, SampleStarted):
                sample_id = event.sample_id
                texts.setdefault(sample_id, [])
            elif isinstance(event, TextPart):
                texts.setdefault(event.sample_id, []).append(event.text)
        return "".join(texts.get(sample_id, []))

    async def run(self, queued: InputQueued) -> TurnCompleted | TurnFailed:
        await self._load_history()
        if self.terminal is not None:
            raise TantraError("TurnEngine instances run one turn")
        self.turn = TurnContext(
            session_id=self.header.id,
            turn_id=queued.command_id,
            agent=self.header.agent,
            depth=self.header.depth,
            input=queued.input,
            metadata=self.header.metadata,
            deps=self.deps,
        )
        self.turn.history = self.history
        self.turn.model = self.model
        self.turn.limits = self.provider.limits(self.model)
        self.turn.provider = self.provider
        self.turn.tracer = self.tracer
        self.turn_span = self.tracer.start_turn(
            self.turn,
            resumed=False,
            ask_id=None,
            parent=current_span.get(),
        )
        raised: BaseException | None = None
        try:
            await self._append([TurnStarted(turn_id=queued.command_id, input=queued.input)])
            for hook in self.hooks:
                await hook.before_turn(self.turn)
            try:
                return await self._drive()
            except ProviderError as exc:
                return await self._finish(TurnFailed(turn_id=queued.command_id, error=str(exc)))
        except BaseException as exc:
            raised = exc
            raise
        finally:
            while self.tool_spans:
                _, span = self.tool_spans.popitem()
                self.tracer.end_tool(
                    span,
                    result=None,
                    is_error=False,
                    outcome="aborted",
                    error_type=None,
                    ask_id=None,
                )
            terminal = self.terminal
            if isinstance(terminal, TurnCompleted):
                outcome = terminal.stop_reason
                stop_reason = terminal.stop_reason
                output = terminal.output
                error: BaseException | str | None = None
            elif isinstance(terminal, TurnFailed):
                outcome = "failed"
                stop_reason = None
                output = None
                error = terminal.error
            else:
                outcome = "aborted"
                stop_reason = None
                output = None
                error = raised
            self.tracer.end_turn(
                self.turn_span,
                outcome=outcome,
                stop_reason=stop_reason,
                output=output,
                final_text=self._final_text(),
                error=error,
                ask_id=None,
            )
