from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import aclosing, suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, ValidationError

from tantra.agent import Agent
from tantra.ask import Approval, ApprovalResponse, AskRequest, AskResponse
from tantra.context import TurnContext, build_sample_request, pending_inbox, resolve_prompt
from tantra.errors import ProviderError, SeqConflict, TantraError
from tantra.events import (
    AgentMessageQueued,
    AskAnswered,
    AskRaised,
    CancelRequested,
    ChildSessionSpawned,
    CompactionApplied,
    KillRequested,
    ReasoningPart,
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    SessionHeader,
    TaskNoticeQueued,
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
from tantra.tools import Context, TaskRef, Tool
from tantra.tracing import NULL_TRACER, Tracer, current_span

if TYPE_CHECKING:
    from tantra.compaction import Compactor
    from tantra.memory import Memory

SUBMIT_OUTPUT = "submit_output"

CANCELLED_RESULT = "not executed: turn cancelled"
COMPLETED_RESULT = "not executed: turn completed"
INBOX_RESULT = "skipped: newer agent message"
CONTROL_EVENTS = (CancelRequested, AgentMessageQueued, TaskNoticeQueued, KillRequested)


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


class Spawner(Protocol):
    """Child-session operations the loop delegates to the harness."""

    def resolve(self, agent: type[Agent] | str) -> str:
        """Return the registered name of `agent`.

        Raises when it is absent from the name table or when spawning it would exceed `max_depth`.
        Both are functions of `(agent, parent depth)` alone, so a replayed turn fails identically
        and a failing spawn never consumes a child-attachment slot.
        """

    async def create(self, agent: str, call_id: str, index: int, input: str) -> TaskRef:
        """Create a deterministic child session and return its task reference."""

    def launch(self, ref: TaskRef, trace_parent: Any) -> None:
        """Admit the linked child through the root supervisor."""

    async def status(self, task_id: str, after_seq: int | None, limit: int) -> dict[str, Any]: ...

    async def messages(self, task_id: str, limit: int) -> list[dict[str, Any]]: ...

    async def result(self, task_id: str) -> dict[str, Any]: ...

    async def wait(self, task_ids: list[str] | None) -> dict[str, Any]: ...

    async def unfinished(self) -> list[str]: ...

    async def cancelling(self) -> list[str]: ...

    async def notify_terminal(self) -> None: ...


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
        event_claim: Callable[[Emitted], bool] | None = None,
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
        self.event_claim = event_claim
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

    async def _refresh(self) -> list[Emitted]:
        stamped = [item async for item in self.store.read(self.header.id, from_seq=self.header.last_seq)]
        if any(not isinstance(item.event, CONTROL_EVENTS) for item in stamped):
            foreign = next(item.event for item in stamped if not isinstance(item.event, CONTROL_EVENTS))
            raise SeqConflict(f"session {self.header.id}: cannot absorb foreign {foreign.type!r} event")
        absorbed: list[Emitted] = []
        for item in stamped:
            self.header.last_seq = item.seq
            self.history.append(item.event)
            self.state.observe(item.event)
            absorbed.append(Emitted(session_id=self.header.id, depth=self.header.depth, seq=item.seq, event=item.event))
        return absorbed

    async def _write(
        self,
        events: Sequence[SessionEvent],
        *,
        abort_on_inbox: bool = False,
        abort_on_cancel: bool = False,
    ) -> tuple[list[Emitted], bool]:
        absorbed: list[Emitted] = []
        while True:
            if abort_on_inbox and pending_inbox(self.history):
                return absorbed, False
            if abort_on_cancel and self.state.cancelled:
                return absorbed, False
            try:
                last = await self.store.append(self.header.id, events, expect_seq=self.header.last_seq)
                break
            except SeqConflict:
                refreshed = await self._refresh()
                if not refreshed:
                    raise
                absorbed.extend(refreshed)
        first = last - len(events) + 1
        self.header.last_seq = last
        self.history.extend(events)
        for event in events:
            self.state.observe(event)
        appended = [
            Emitted(session_id=self.header.id, depth=self.header.depth, seq=first + offset, event=event)
            for offset, event in enumerate(events)
        ]
        return [*absorbed, *appended], True

    async def _append(self, events: Sequence[SessionEvent]) -> list[Emitted]:
        emitted, _ = await self._write(events)
        return emitted

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

    async def _attach(
        self, call_id: str, agent: Any, input: str
    ) -> tuple[TaskRef | None, Exception | None, list[Emitted]]:
        try:
            name = self.spawner.resolve(agent)
        except Exception as exc:
            return None, exc, []
        index = self.spawns.get(call_id, 0)
        records = self.state.children.get(call_id, [])
        self.spawns[call_id] = index + 1
        ref = await self.spawner.create(name, call_id, index, input)
        if index < len(records):
            record = records[index]
            if (record.child_session_id, record.agent) != (ref.task_id, ref.agent):
                raise TantraError(f"task {ref.task_id}: persisted child link does not match its deterministic identity")
            self.spawner.launch(ref, self.tool_spans.get(call_id))
            return ref, None, []
        spawned = ChildSessionSpawned(call_id=call_id, child_session_id=ref.task_id, agent=name)
        emitted = await self._append([spawned])
        self.spawner.launch(ref, self.tool_spans.get(call_id))
        return ref, None, emitted

    async def _spawn(self, call_id: str, agent: Any, input: str, future: asyncio.Future[Any]) -> AsyncIterator[Emitted]:
        ref, error, spawned = await self._attach(call_id, agent, input)
        for emitted in spawned:
            yield emitted
        if error is not None:
            future.set_exception(error)
            return
        assert ref is not None
        future.set_result(ref)

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
            task_status=self.spawner.status if delegates else None,
            task_messages=self.spawner.messages if delegates else None,
            task_result=self.spawner.result if delegates else None,
            task_wait=self.spawner.wait if delegates else None,
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
                    if kind == "spawn":
                        delegation = self._spawn(call_id, *payload, future)
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
        unfinished = await self.spawner.unfinished() if self.spawner is not None else []
        if unfinished:
            result = f"cannot submit output while descendant tasks are unfinished: {unfinished}; call task_wait"
            return await self._completed(call.call_id, result, is_error=True, call=call), None, False
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

    async def _synthesize(
        self,
        reason: str,
        *,
        only_unstarted: bool = False,
        started: set[str] | None = None,
    ) -> AsyncIterator[Emitted]:
        pending = self.state.unanswered()
        if only_unstarted:
            known_started = self.state.started if started is None else started
            pending = [call for call in pending if call.call_id not in known_started]
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
        if self.spawner is not None:
            await self.spawner.notify_terminal()
        self.terminal = failure
        for emitted in appended:
            yield emitted
        for hook in self.hooks:
            await hook.after_turn(self.turn, failure)

    async def _terminal(
        self,
        reason: str,
        output: Any,
        *,
        abort_on_inbox: bool = True,
    ) -> AsyncIterator[Emitted]:
        for emitted in await self._refresh():
            yield emitted
        if reason != "cancelled" and self.state.cancelled:
            async for emitted in self._cancel():
                yield emitted
            return
        if reason == "cancelled" and self.spawner is not None:
            cancelling = await self.spawner.cancelling()
            while cancelling:
                await self.spawner.wait(cancelling)
                cancelling = await self.spawner.cancelling()
        unfinished = await self.spawner.unfinished() if self.spawner is not None and reason != "cancelled" else []
        if unfinished:
            sample_id = next(event.sample_id for event in reversed(self.history) if isinstance(event, SampleStarted))
            call_id = uuid5(NAMESPACE_URL, f"tantra:completion-wait:{self.turn.turn_id}:{sample_id}").hex
            requested = ToolCallRequested(sample_id=sample_id, call_id=call_id, name="task_wait", args={})
            for emitted in await self._append([requested, ToolCallStarted(call_id=call_id)]):
                yield emitted
            result: dict[str, Any] = {"reason": "all_terminal", "task_ids": []}
            while unfinished:
                result = await self.spawner.wait(unfinished)
                if reason != "max_steps":
                    break
                unfinished = await self.spawner.unfinished()
            for emitted in await self._completed(call_id, result):
                yield emitted
            return
        event = TurnCompleted(turn_id=self.turn.turn_id, stop_reason=reason, output=output)
        appended, written = await self._write(
            [event],
            abort_on_inbox=abort_on_inbox,
            abort_on_cancel=reason != "cancelled",
        )
        if written and self.spawner is not None:
            await self.spawner.notify_terminal()
        for emitted in appended:
            yield emitted
        if not written:
            if self.state.cancelled:
                async for emitted in self._cancel():
                    yield emitted
            return
        self.terminal = event
        for hook in self.hooks:
            await hook.after_turn(self.turn, event)

    async def _cancel(self) -> AsyncIterator[Emitted]:
        async with aclosing(self._synthesize(CANCELLED_RESULT)) as orphans:
            async for emitted in orphans:
                yield emitted
        async with aclosing(self._terminal("cancelled", None, abort_on_inbox=False)) as terminal:
            async for emitted in terminal:
                yield emitted

    async def _batch(self, capped: bool) -> AsyncIterator[Emitted]:
        stopped = False
        for call in list(self.state.batch):
            if call.call_id in self.state.results:
                continue
            started_before = set(self.state.started)
            replayed = call.call_id in started_before
            if not stopped:
                for emitted in await self._refresh():
                    yield emitted
                if self.state.cancelled:
                    return
                if pending_inbox(self.history) and not replayed:
                    async with aclosing(
                        self._synthesize(INBOX_RESULT, only_unstarted=True, started=started_before)
                    ) as stale:
                        async for emitted in stale:
                            yield emitted
                    return
            if stopped:
                for emitted in await self._completed(call.call_id, COMPLETED_RESULT, is_error=True, call=call):
                    yield emitted
                continue
            if call.name == SUBMIT_OUTPUT and self.agent.output_schema is not None:
                events, output, stopped = await self._submit(call)
                for emitted in events:
                    yield emitted
                for emitted in await self._refresh():
                    yield emitted
                if self.state.cancelled:
                    return
                if pending_inbox(self.history):
                    async with aclosing(self._synthesize(INBOX_RESULT, only_unstarted=True)) as stale:
                        async for emitted in stale:
                            yield emitted
                    return
                if stopped:
                    self.stop = ("output", output)
                continue
            if capped and call.name != "task_wait":
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
            for emitted in await self._refresh():
                yield emitted
            if self.state.cancelled:
                return
            if pending_inbox(self.history) and not replayed:
                async with aclosing(
                    self._synthesize(INBOX_RESULT, only_unstarted=True, started=started_before)
                ) as stale:
                    async for emitted in stale:
                        yield emitted
                return
            if call.call_id not in self.state.started:
                for emitted in await self._append([ToolCallStarted(call_id=call.call_id)]):
                    yield emitted
            for emitted in await self._refresh():
                yield emitted
            if pending_inbox(self.history) and not replayed:
                async with aclosing(
                    self._synthesize(INBOX_RESULT, only_unstarted=True, started=started_before)
                ) as stale:
                    async for emitted in stale:
                        yield emitted
                return
            self.tool_spans[call.call_id] = self.tracer.start_tool(
                self.turn_span, call, args=effective.args, tool=tool, replayed=replayed
            )
            async with aclosing(self._execute(tool, call, effective.args)) as execution:
                async for emitted in execution:
                    yield emitted
            for emitted in await self._refresh():
                yield emitted
            if self.suspended is not None or self.state.cancelled:
                return
            if pending_inbox(self.history):
                async with aclosing(self._synthesize(INBOX_RESULT, only_unstarted=True)) as stale:
                    async for emitted in stale:
                        yield emitted
                return

    async def _after_batch(self, capped: bool) -> AsyncIterator[Emitted]:
        for emitted in await self._refresh():
            yield emitted
        if self.suspended is not None or self.failed:
            self.done = True
            return
        if self.state.cancelled:
            self.done = True
            async for emitted in self._cancel():
                yield emitted
            return
        if pending_inbox(self.history) and not capped:
            self.stop = None
        if self.stop is not None:
            async for emitted in self._terminal(*self.stop):
                yield emitted
            self.done = self.terminal is not None
            if not self.done:
                self.stop = None
            return
        if pending_inbox(self.history):
            return
        if capped:
            async for emitted in self._terminal("max_steps", None, abort_on_inbox=False):
                yield emitted
            self.done = True

    async def _drive(self) -> AsyncIterator[Emitted]:
        state = self.state
        for emitted in await self._refresh():
            yield emitted
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
                for emitted in await self._refresh():
                    yield emitted
                if state.cancelled:
                    async for emitted in self._cancel():
                        yield emitted
                    return
                if not pending_inbox(self.history):
                    async for emitted in self._terminal("output", output):
                        yield emitted
                    if self.terminal is not None:
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
            for emitted in await self._refresh():
                yield emitted
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

            for emitted in await self._refresh():
                yield emitted
            if state.cancelled:
                async for emitted in self._cancel():
                    yield emitted
                return

            sample_id = uuid4().hex
            prompt = await resolve_prompt(self.agent.prompt, self.turn)
            for emitted in await self._append(
                [SampleStarted(turn_id=self.turn.turn_id, sample_id=sample_id, model=self.model)]
            ):
                yield emitted
            if state.cancelled:
                async for emitted in self._cancel():
                    yield emitted
                return
            req = build_sample_request(
                model=self.model,
                prompt=prompt,
                events=self.history,
                tools=self.schemas,
                skills=self.skills_index,
            )

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

            for emitted in await self._refresh():
                yield emitted
            for emitted in await self._append(self._parts(sample_id, end)):
                yield emitted
            self.header.usage = accumulate(self.header.usage, end.usage)
            await self.store.patch_header(self.header.id, usage=self.header.usage)

            if not end.tool_calls:
                if state.cancelled:
                    async for emitted in self._cancel():
                        yield emitted
                    return
                if pending_inbox(self.history):
                    continue
                async for emitted in self._terminal("completed", None):
                    yield emitted
                if self.terminal is not None:
                    return
                continue

            capped = state.samples_used >= self.agent.max_steps
            async with aclosing(self._batch(capped)) as batch:
                async for emitted in batch:
                    yield emitted
            async for emitted in self._after_batch(capped):
                yield emitted
            if self.done:
                return

        async for emitted in self._terminal("max_steps", None, abort_on_inbox=False):
            yield emitted

    async def run(self) -> AsyncIterator[Emitted]:
        try:
            async with aclosing(self._drive()) as stream:
                async for emitted in stream:
                    if self.event_claim is not None and not self.event_claim(emitted):
                        continue
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
