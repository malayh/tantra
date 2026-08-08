from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import aclosing, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, ValidationError

from tantra.agent import Agent
from tantra.context import TurnContext, build_sample_request, resolve_prompt
from tantra.errors import ProviderError, TantraError
from tantra.events import (
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
    Usage,
)
from tantra.providers.base import (
    Provider,
    ReasoningDelta,
    SampleRequest,
    StreamEnd,
    TextDelta,
    ToolCallDelta,
    ToolSchema,
)
from tantra.stores.base import Store
from tantra.tools import Context, Tool

SUBMIT_OUTPUT = "submit_output"


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
        self.failed = False
        self.lease_lost = False
        self.schemas = [t.schema for t in tools.values()]
        if agent.output_schema is not None:
            self.schemas = [*self.schemas, submit_output_schema(agent.output_schema)]

    async def _append(self, events: Sequence[SessionEvent]) -> list[Emitted]:
        last = await self.store.append(self.header.id, events, expect_seq=self.header.last_seq)
        first = last - len(events) + 1
        self.header.last_seq = last
        self.history.extend(events)
        return [
            Emitted(session_id=self.header.id, depth=self.header.depth, seq=first + offset, event=event)
            for offset, event in enumerate(events)
        ]

    async def _completed(self, call_id: str, result: Any, *, is_error: bool = False) -> list[Emitted]:
        return await self._append([ToolCallCompleted(call_id=call_id, result=result, is_error=is_error)])

    def _live(self, event: TextDelta | ReasoningDelta | ToolCallDelta) -> Emitted:
        return Emitted(session_id=self.header.id, depth=self.header.depth, seq=None, event=event)

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

    async def _execute(self, tool: Tool, call_id: str, args: dict[str, Any]) -> AsyncIterator[Emitted]:
        queue: asyncio.Queue[str] = asyncio.Queue()
        ctx = Context(
            session_id=self.header.id,
            turn_id=self.turn.turn_id,
            call_id=call_id,
            depth=self.header.depth,
            deps=self.turn.deps,
            store=self.store,
            emit=queue.put,
        )
        task = asyncio.ensure_future(tool.invoke(args, ctx))
        getter: asyncio.Future[str] | None = None
        try:
            while True:
                getter = asyncio.ensure_future(queue.get())
                finished, _ = await asyncio.wait({task, getter}, return_when=asyncio.FIRST_COMPLETED)
                if getter in finished:
                    for emitted in await self._append([ToolProgress(call_id=call_id, message=getter.result())]):
                        yield emitted
                    continue
                getter.cancel()
                break
            while not queue.empty():
                for emitted in await self._append([ToolProgress(call_id=call_id, message=queue.get_nowait())]):
                    yield emitted
            try:
                result: Any = await task
                is_error = False
            except Exception as exc:
                result, is_error = str(exc), True
            for emitted in await self._completed(call_id, result, is_error=is_error):
                yield emitted
        finally:
            pending = [future for future in (task, getter) if future is not None]
            for future in pending:
                future.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*pending, return_exceptions=True)

    def _parts(self, sample_id: str, end: StreamEnd) -> tuple[list[SessionEvent], dict[str, Any], dict[str, str]]:
        parts: list[SessionEvent] = []
        args_by_call: dict[str, Any] = {}
        invalid: dict[str, str] = {}
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
                invalid[call.id] = f"invalid JSON arguments: {exc}"
                args = {}
            args_by_call[call.id] = args
            parts.append(ToolCallRequested(sample_id=sample_id, call_id=call.id, name=call.name, args=args))
        parts.append(SampleCompleted(sample_id=sample_id, usage=end.usage, finish_reason=end.finish_reason))
        return parts, args_by_call, invalid

    async def _submit(self, call_id: str, args: dict[str, Any]) -> tuple[list[Emitted], Any, bool]:
        schema = self.agent.output_schema
        assert schema is not None
        try:
            value = schema.model_validate(args)
        except ValidationError as exc:
            return await self._completed(call_id, f"invalid output: {exc}", is_error=True), None, False
        output = value.model_dump(mode="json")
        return await self._completed(call_id, output), output, True

    async def run(self) -> AsyncIterator[Emitted]:
        for step in range(self.agent.max_steps):
            if not await self.store.acquire_lease(self.header.id, self.holder, self.lease_ttl):
                self.lease_lost = True
                raise TantraError(f"lease lost: session {self.header.id} is held by another writer")

            sample_id = uuid4().hex
            prompt = await resolve_prompt(self.agent.prompt, self.turn)
            req = build_sample_request(model=self.model, prompt=prompt, events=self.history, tools=self.schemas)
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
                self.failed = True
                for emitted in await self._append([TurnFailed(turn_id=self.turn.turn_id, error=str(exc))]):
                    yield emitted
                return

            parts, args_by_call, invalid = self._parts(sample_id, end)
            for emitted in await self._append(parts):
                yield emitted
            self.header.usage = accumulate(self.header.usage, end.usage)
            self.header.updated_at = datetime.now(UTC)
            await self.store.put_header(self.header)

            if not end.tool_calls:
                for emitted in await self._append(
                    [TurnCompleted(turn_id=self.turn.turn_id, stop_reason="completed", output=None)]
                ):
                    yield emitted
                return

            capped = step + 1 >= self.agent.max_steps
            output: Any = None
            stopped = False
            for call in end.tool_calls:
                args = args_by_call[call.id]
                if stopped:
                    for emitted in await self._completed(call.id, "not executed: turn completed", is_error=True):
                        yield emitted
                    continue
                reason = invalid.get(call.id)
                if reason is not None:
                    for emitted in await self._completed(call.id, reason, is_error=True):
                        yield emitted
                    continue
                if call.name == SUBMIT_OUTPUT and self.agent.output_schema is not None:
                    events, output, stopped = await self._submit(call.id, args)
                    for emitted in events:
                        yield emitted
                    continue
                if capped:
                    for emitted in await self._completed(call.id, "not executed: max steps reached", is_error=True):
                        yield emitted
                    continue
                tool = self.tools.get(call.name)
                if tool is None:
                    for emitted in await self._completed(call.id, f"unknown tool {call.name!r}", is_error=True):
                        yield emitted
                    continue
                for emitted in await self._append([ToolCallStarted(call_id=call.id)]):
                    yield emitted
                async with aclosing(self._execute(tool, call.id, args)) as execution:
                    async for emitted in execution:
                        yield emitted

            if stopped:
                for emitted in await self._append(
                    [TurnCompleted(turn_id=self.turn.turn_id, stop_reason="output", output=output)]
                ):
                    yield emitted
                return

            if capped:
                for emitted in await self._append(
                    [TurnCompleted(turn_id=self.turn.turn_id, stop_reason="max_steps", output=None)]
                ):
                    yield emitted
                return
