from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ValidationError

from tantra.agent import Agent
from tantra.ask import Approval, ApprovalResponse, AskRequest, AskResponse
from tantra.context import TurnContext, build_sample_request, resolve_prompt
from tantra.errors import ProviderError, TantraError
from tantra.events import (
    AskRaised,
    CompactionApplied,
    InputQueued,
    LoggedEvent,
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
COMPLETED_RESULT = "not executed: turn completed"


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 8.0


DEFAULT_RETRY = RetryConfig()


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


@dataclass
class _PreparedCall:
    call: ToolCallRequested
    effective: ToolCallRequested
    tool: Tool
    kwargs: dict[str, Any]
    span: Any = None


_NO_OUTPUT = object()


@dataclass(frozen=True)
class FinishResult:
    output: Any
    final_events: tuple[SessionEvent, ...] = ()


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
        ask_future: Callable[[AskRaised], asyncio.Future[AskResponse]] | None = None,
        append_events: Callable[[Sequence[SessionEvent]], Awaitable[list[Stamped]]] | None = None,
        terminal_tool: str | None = None,
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
        self.ask_future = ask_future
        self.append_events = append_events
        self.terminal_tool = terminal_tool
        self.schemas = [tool.schema for tool in tools.values()]
        if agent.output_schema is not None:
            self.schemas.append(submit_output_schema(agent.output_schema))
        self._append_lock = asyncio.Lock()
        self.turn: TurnContext | None = None
        self.turn_span: Any = None
        self.terminal: TurnCompleted | TurnFailed | None = None
        self.tool_spans: dict[object, Any] = {}

    async def _absorb(self) -> None:
        while True:
            page = await self.store.read_page(self.header.id, after=self.header.last_seq)
            if not page:
                return
            assert self.history is not None
            self.history.extend(item.event for item in page)
            self.header.last_seq = page[-1].seq

    async def _ask(self, call_id: str, request: AskRequest) -> AskResponse:
        if self.ask_future is None:
            raise TantraError("ctx.ask is unavailable outside Runtime")
        raised = AskRaised(ask_id=uuid4().hex, call_id=call_id, request=request)
        future = self.ask_future(raised)
        try:
            await self._append([raised])
        except BaseException:
            future.cancel()
            raise
        response = await future
        await self._absorb()
        return response

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
            if self.append_events is None:
                last = await self.store.append(self.header.id, events)
                first = last - len(events) + 1
                stamped = [Stamped(seq=first + index, event=event) for index, event in enumerate(events)]
            else:
                stamped = await self.append_events(events)
                last = stamped[-1].seq
            self.header.last_seq = last
            assert self.history is not None
            self.history.extend(events)
            if self.notify is not None:
                for item in stamped:
                    notified = self.notify(item)
                    if inspect.isawaitable(notified):
                        await notified
            for item in stamped:
                logged = LoggedEvent(agent_id=UUID(hex=self.header.id), seq=item.seq, event=item.event)
                for hook in self.hooks:
                    await hook.on_event(logged)
            return stamped

    def _verdict(self, name: str, tool_permission: str | None) -> str:
        verdict = decide(name, self.agent.permissions, tool_permission, self.default_permission)
        for rules in self.permission_chain:
            verdict = strictest(verdict, decide(name, rules, None, self.default_permission))
        return verdict

    async def _finish(
        self,
        terminal: TurnCompleted | TurnFailed,
        final_events: Sequence[SessionEvent] = (),
    ) -> TurnCompleted | TurnFailed:
        await self._append([terminal, *final_events])
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
        terminal_seen = False
        for call in end.tool_calls:
            try:
                args = json.loads(call.args) if call.args.strip() else {}
                if not isinstance(args, dict):
                    raise ValueError("arguments must be a JSON object")
            except ValueError as exc:
                args = {}
                if not terminal_seen:
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
            terminal_seen = terminal_seen or call.name == self.terminal_tool
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
        terminal_index = next(
            (index for index, call in enumerate(calls) if call.name == self.terminal_tool),
            None,
        )
        for index, call in enumerate(calls):
            if terminal_index is not None and index > terminal_index:
                await self._complete(call, COMPLETED_RESULT, is_error=True)
                continue
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
                body = json.dumps(effective.args, default=str)
                if escalation is not None:
                    body = f"{escalation.reason}\n\n{body}"
                response = await self._ask(
                    call.call_id,
                    Approval(
                        title=f"Run {call.name}?",
                        body=body,
                        extra={"permission": call.name},
                    ),
                )
                if not (isinstance(response, ApprovalResponse) and response.allow):
                    await self._complete(
                        call,
                        "denied by user",
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

    async def _invoke(self, prepared: _PreparedCall) -> FinishResult | None:
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
            ask=lambda request: self._ask(call.call_id, request),
            memory=self.memory,
        )
        kwargs = dict(prepared.kwargs)
        if prepared.tool.ctx_param is not None:
            kwargs[prepared.tool.ctx_param] = ctx
        result: Any
        is_error = False
        finished: FinishResult | None = None
        error_type: str | None = None
        try:
            if inspect.iscoroutinefunction(prepared.tool.fn):
                result = prepared.tool.fn(**kwargs)
            else:
                result = await asyncio.to_thread(prepared.tool.fn, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, FinishResult):
                finished = result
                result = result.output
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
        return finished

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
            finished = next((result for result in results if isinstance(result, FinishResult)), None)
            if finished is not None:
                return finished
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
            if isinstance(output, FinishResult):
                return await self._finish(
                    TurnCompleted(
                        turn_id=self.turn.turn_id,
                        stop_reason="finished",
                        output=output.output,
                    ),
                    output.final_events,
                )
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
