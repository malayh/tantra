from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import aclosing, suppress
from typing import TYPE_CHECKING, Any, Literal
from uuid import NAMESPACE_URL, uuid5

from tantra.context import build_messages, pending_inbox
from tantra.errors import SessionBusy, SessionExists, SessionNotFound, TantraError
from tantra.events import (
    AgentMessageQueued,
    AskAnswered,
    AskRaised,
    CancelRequested,
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
    TurnCompleted,
    TurnFailed,
    TurnStarted,
)
from tantra.loop import Emitted
from tantra.providers.base import AssistantMessage
from tantra.tools import Context, TaskRef, Tool, tool

if TYPE_CHECKING:
    from tantra.agent import Agent
    from tantra.harness import Harness

TaskState = Literal["queued", "running", "waiting", "awaiting_input", "completed", "failed", "killed"]
TASK_TOOL_NAMES = frozenset(
    {"task_status", "task_messages", "task_result", "task_wait", "task_send", "task_kill", "notify_parent"}
)
TERMINAL_STATES = frozenset({"completed", "failed", "killed"})


@tool
async def task_status(ctx: Context, task_id: str, after_seq: int | None = None, limit: int = 20) -> Any:
    """Inspect a descendant task's state and a bounded page of its durable events."""
    return await ctx.task_status(task_id, after_seq, limit)


@tool
async def task_messages(ctx: Context, task_id: str, limit: int = 20) -> Any:
    """Read the latest provider-visible messages from a descendant task."""
    return await ctx.task_messages(task_id, limit)


@tool
async def task_result(ctx: Context, task_id: str) -> Any:
    """Read the terminal result of a descendant task."""
    return await ctx.task_result(task_id)


@tool
async def task_wait(ctx: Context, task_ids: list[str] | None = None) -> Any:
    """Wait for task activity without spending another model sample."""
    return await ctx.task_wait(task_ids)


TASK_TOOLS: tuple[Tool, ...] = (task_status, task_messages, task_result, task_wait)


def task_id(parent_id: str, call_id: str, index: int) -> str:
    return uuid5(NAMESPACE_URL, f"tantra:task:{parent_id}:{call_id}:{index}").hex


def notice_id(task_session_id: str, terminal_seq: int) -> str:
    return uuid5(NAMESPACE_URL, f"tantra:notice:{task_session_id}:{terminal_seq}").hex


def derive_task_state(events: Sequence[SessionEvent]) -> TaskState:
    if any(isinstance(event, KillRequested) for event in events):
        return "killed"
    if any(isinstance(event, TurnFailed) for event in events):
        return "failed"
    if any(isinstance(event, TurnCompleted) for event in events):
        return "completed"
    if not any(isinstance(event, TurnStarted) for event in events):
        return "queued"
    completed = {event.call_id for event in events if isinstance(event, ToolCallCompleted)}
    if any(
        isinstance(event, ToolCallRequested) and event.name == "task_wait" and event.call_id not in completed
        for event in events
    ):
        return "waiting"
    answered = {event.ask_id for event in events if isinstance(event, AskAnswered)}
    if any(isinstance(event, AskRaised) and event.ask_id not in answered for event in events):
        return "awaiting_input"
    return "running"


def terminal(events: Sequence[Stamped]) -> tuple[TaskState, int] | None:
    killed = next((item for item in reversed(events) if isinstance(item.event, KillRequested)), None)
    if killed is not None:
        return "killed", killed.seq
    failed = next((item for item in reversed(events) if isinstance(item.event, TurnFailed)), None)
    if failed is not None:
        return "failed", failed.seq
    completed = next((item for item in reversed(events) if isinstance(item.event, TurnCompleted)), None)
    if completed is not None:
        return "completed", completed.seq
    return None


class TaskSpawner:
    def __init__(self, supervisor: TaskSupervisor, parent: SessionHeader, holder: str) -> None:
        self.supervisor = supervisor
        self.parent = parent
        self.holder = holder

    def resolve(self, agent: type[Agent] | str) -> str:
        name = self.supervisor.harness._name_of(agent)
        depth = self.parent.depth + 1
        if depth > self.supervisor.harness.max_depth:
            raise TantraError(
                f"max_depth {self.supervisor.harness.max_depth} exceeded: cannot spawn {name!r} at depth {depth}"
            )
        return name

    async def create(self, agent: str, call_id: str, index: int, input: str) -> TaskRef:
        return await self.supervisor.create(self.parent, agent, call_id, index, input)

    def launch(self, ref: TaskRef, trace_parent: Any) -> None:
        self.supervisor.launch(ref.task_id, trace_parent)

    async def status(self, task_id: str, after_seq: int | None, limit: int) -> dict[str, Any]:
        return await self.supervisor.status(self.parent.id, task_id, after_seq, limit)

    async def messages(self, task_id: str, limit: int) -> list[dict[str, Any]]:
        return await self.supervisor.messages(self.parent.id, task_id, limit)

    async def result(self, task_id: str) -> dict[str, Any]:
        return await self.supervisor.result(self.parent.id, task_id)

    async def wait(self, task_ids: list[str] | None) -> dict[str, Any]:
        return await self.supervisor.wait(self.parent.id, self.holder, task_ids)

    async def unfinished(self) -> list[str]:
        return await self.supervisor.unfinished(self.parent.id, recursive=False)

    async def cancelling(self) -> list[str]:
        return await self.supervisor.cancelling(self.parent.id)

    async def notify_terminal(self) -> None:
        await self.supervisor.notify_terminal(self.parent.id)


class TaskSupervisor:
    def __init__(self, harness: Harness, root_id: str) -> None:
        self.harness = harness
        self.root_id = root_id
        self.gate = asyncio.Semaphore(harness.max_concurrency)
        self.events: asyncio.Queue[Emitted | BaseException] = asyncio.Queue()
        self.runners: dict[str, asyncio.Task[None]] = {}
        self.held: set[str] = set()
        self.seen: set[tuple[str, int]] = set()
        self.wakeup = asyncio.Event()
        self.closed = False

    def claim(self, emitted: Emitted) -> bool:
        if emitted.seq is None:
            return True
        key = emitted.session_id, emitted.seq
        if key in self.seen:
            return False
        self.seen.add(key)
        return True

    async def children(self, parent_id: str) -> list[SessionHeader]:
        found: list[SessionHeader] = []
        before: str | None = None
        while True:
            page = await self.harness.store.list(parent_id=parent_id, limit=50, before=before)
            found.extend(page)
            if len(page) < 50:
                return found
            before = page[-1].id

    async def descendants(self, parent_id: str) -> list[SessionHeader]:
        found: list[SessionHeader] = []
        for child in await self.children(parent_id):
            found.append(child)
            found.extend(await self.descendants(child.id))
        return found

    async def _log(self, sid: str) -> list[Stamped]:
        return [item async for item in self.harness.store.read(sid)]

    async def start(self) -> None:
        for header in await self.descendants(self.root_id):
            stamped = await self._log(header.id)
            state = derive_task_state([item.event for item in stamped])
            if state in {"queued", "running", "waiting"}:
                self.launch(header.id, None)
            elif state in TERMINAL_STATES:
                await self._notice(header, stamped)

    async def create(
        self,
        parent: SessionHeader,
        agent: str,
        call_id: str,
        index: int,
        input: str,
    ) -> TaskRef:
        sid = task_id(parent.id, call_id, index)
        depth = parent.depth + 1
        header = SessionHeader(
            id=sid,
            agent=agent,
            parent_id=parent.id,
            depth=depth,
            metadata=dict(parent.metadata),
            task_input=input,
        )
        try:
            await self.harness.store.create(header)
            await self.harness.store.append(
                sid,
                [SessionCreated(agent=agent, parent_id=parent.id, depth=depth, metadata=header.metadata)],
                expect_seq=0,
            )
        except SessionExists:
            existing = await self.harness.store.header(sid)
            if existing is None or (
                existing.agent,
                existing.parent_id,
                existing.depth,
                existing.task_input,
            ) != (agent, parent.id, depth, input):
                raise TantraError(
                    f"task {sid}: deterministic child identity does not match its persisted header"
                ) from None
        return TaskRef(task_id=sid, agent=agent)

    def launch(self, sid: str, trace_parent: Any) -> None:
        current = self.runners.get(sid)
        if self.closed or current is not None and not current.done():
            return
        self.runners[sid] = asyncio.create_task(self._run(sid, trace_parent))

    async def _run(self, sid: str, trace_parent: Any) -> None:
        acquired = False
        try:
            await self.gate.acquire()
            acquired = True
            self.held.add(sid)
            header = await self.harness.store.header(sid)
            if header is None:
                raise SessionNotFound(sid)
            stamped = await self._log(sid)
            state = derive_task_state([item.event for item in stamped])
            if state == "queued":
                stream = self.harness._run_one(sid, header.task_input or "", self, trace_parent)
            elif state in {"running", "waiting"}:
                stream = self.harness._resume_one(sid, None, None, self, trace_parent)
            else:
                stream = None
            if stream is not None:
                async with aclosing(stream) as child:
                    async for emitted in child:
                        await self.events.put(emitted)
            stamped = await self._log(sid)
            header = await self.harness.store.header(sid)
            if header is not None:
                await self._notice(header, stamped)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await self.events.put(exc)
        finally:
            if sid in self.held:
                self.held.remove(sid)
                self.gate.release()
            elif acquired:
                self.gate.release()
            self.wakeup.set()

    async def _notice(self, header: SessionHeader, stamped: Sequence[Stamped], *, forward: bool = True) -> None:
        outcome = terminal(stamped)
        if outcome is None or header.parent_id is None:
            return
        state, terminal_seq = outcome
        nid = notice_id(header.id, terminal_seq)
        parent_log = [item async for item in self.harness.store.read(header.parent_id)]
        if any(isinstance(item.event, TaskNoticeQueued) and item.event.notice_id == nid for item in parent_log):
            self.wakeup.set()
            return
        notice = TaskNoticeQueued(
            notice_id=nid,
            task_session_id=header.id,
            state=state,
            terminal_seq=terminal_seq,
        )
        seq = await self.harness.store.append(header.parent_id, [notice], expect_seq=None)
        if forward:
            emitted = Emitted(session_id=header.parent_id, depth=header.depth - 1, seq=seq, event=notice)
            if self.claim(emitted):
                await self.harness._notify(emitted)
                await self.events.put(emitted)
        self.wakeup.set()

    async def notify_terminal(self, sid: str) -> None:
        header = await self.harness.store.header(sid)
        if header is None:
            raise SessionNotFound(sid)
        stamped = await self._log(sid)
        await self._notice(header, stamped, forward=sid != self.root_id)

    async def authorize(self, caller_id: str, target_id: str) -> SessionHeader:
        target = await self.harness.store.header(target_id)
        if target is None:
            raise SessionNotFound(target_id)
        parent_id = target.parent_id
        while parent_id is not None:
            if parent_id == caller_id:
                return target
            parent = await self.harness.store.header(parent_id)
            if parent is None:
                raise TantraError(f"task {target_id}: ancestor session {parent_id!r} is missing")
            parent_id = parent.parent_id
        raise TantraError(f"task {target_id} is not a descendant of session {caller_id}")

    @staticmethod
    def _limit(limit: int) -> None:
        if not 1 <= limit <= 100:
            raise TantraError(f"limit must be between 1 and 100, got {limit}")

    async def status(self, caller_id: str, target_id: str, after_seq: int | None, limit: int) -> dict[str, Any]:
        self._limit(limit)
        if after_seq is not None and after_seq < 0:
            raise TantraError(f"after_seq must be non-negative, got {after_seq}")
        header = await self.authorize(caller_id, target_id)
        stamped = await self._log(target_id)
        latest = stamped[-1].seq if stamped else 0
        page = [item for item in stamped if item.seq > (after_seq or 0)][:limit]
        next_seq = page[-1].seq if page else after_seq or 0
        return {
            "task_id": target_id,
            "state": derive_task_state([item.event for item in stamped]),
            "agent": header.agent,
            "parent": header.parent_id,
            "depth": header.depth,
            "latest_seq": latest,
            "events": [{"seq": item.seq, "event": item.event.model_dump(mode="json")} for item in page],
            "next_seq": next_seq,
            "has_more": next_seq < latest,
        }

    async def messages(self, caller_id: str, target_id: str, limit: int) -> list[dict[str, Any]]:
        self._limit(limit)
        await self.authorize(caller_id, target_id)
        events = [item.event for item in await self._log(target_id)]
        visible: list[dict[str, Any]] = []
        for message in build_messages(events):
            if isinstance(message, AssistantMessage):
                visible.append(message.model_dump(mode="json", exclude={"reasoning"}))
            else:
                visible.append(message.model_dump(mode="json"))
        return visible[-limit:]

    async def result(self, caller_id: str, target_id: str) -> dict[str, Any]:
        await self.authorize(caller_id, target_id)
        stamped = await self._log(target_id)
        events = [item.event for item in stamped]
        state = derive_task_state(events)
        if state not in TERMINAL_STATES:
            raise TantraError(f"task {target_id} is {state}; result is available only after termination")
        answer: dict[str, Any] = {"task_id": target_id, "state": state}
        if state == "failed":
            failed = next(event for event in reversed(events) if isinstance(event, TurnFailed))
            answer["error"] = failed.error
        elif state == "completed":
            completed = next(event for event in reversed(events) if isinstance(event, TurnCompleted))
            if completed.output is not None:
                answer["output"] = completed.output
            else:
                sample = ""
                for event in reversed(events):
                    if isinstance(event, SampleStarted):
                        sample = event.sample_id
                        break
                answer["text"] = "".join(
                    event.text for event in events if isinstance(event, TextPart) and event.sample_id == sample
                )
        return answer

    async def unfinished(self, parent_id: str, *, recursive: bool) -> list[str]:
        headers = await self.descendants(parent_id) if recursive else await self.children(parent_id)
        pending: list[str] = []
        for header in headers:
            state = derive_task_state([item.event for item in await self._log(header.id)])
            if state not in TERMINAL_STATES:
                pending.append(header.id)
        return pending

    async def cancelling(self, parent_id: str) -> list[str]:
        pending: list[str] = []
        for header in await self.children(parent_id):
            events = [item.event for item in await self._log(header.id)]
            if (
                any(isinstance(event, TurnStarted) for event in events)
                and derive_task_state(events) not in TERMINAL_STATES
            ):
                if any(isinstance(event, CancelRequested) for event in events):
                    self.launch(header.id, None)
                pending.append(header.id)
        return pending

    async def _pending(self, parent_id: str, selected: set[str]) -> list[str] | None:
        events = [item.event async for item in self.harness.store.read(parent_id)]
        inbox = pending_inbox(events)
        if any(isinstance(event, AgentMessageQueued) for event in inbox):
            return []
        matched = [
            event.task_session_id
            for event in inbox
            if isinstance(event, TaskNoticeQueued) and event.task_session_id in selected
        ]
        return matched or None

    async def wait(self, parent_id: str, holder: str, task_ids: list[str] | None) -> dict[str, Any]:
        if task_ids is None:
            selected = set(await self.unfinished(parent_id, recursive=True))
        else:
            selected = set(task_ids)
            for sid in selected:
                await self.authorize(parent_id, sid)
        if not selected:
            return {"reason": "all_terminal", "task_ids": []}
        pending = await self._pending(parent_id, selected)
        if pending is not None:
            return {"reason": "inbox", "task_ids": pending}
        states = {sid: derive_task_state([item.event for item in await self._log(sid)]) for sid in selected}
        if all(state in TERMINAL_STATES for state in states.values()):
            return {"reason": "all_terminal", "task_ids": sorted(selected)}
        released = parent_id in self.held
        if released:
            self.held.remove(parent_id)
            self.gate.release()
        try:
            while True:
                self.wakeup.clear()
                try:
                    await asyncio.wait_for(self.wakeup.wait(), timeout=min(1.0, self.harness.lease_ttl / 3))
                except TimeoutError:
                    pass
                if not await self.harness.store.acquire_lease(parent_id, holder, self.harness.lease_ttl):
                    raise SessionBusy(parent_id)
                pending = await self._pending(parent_id, selected)
                if pending is not None:
                    return {"reason": "inbox", "task_ids": pending}
                states = {sid: derive_task_state([item.event for item in await self._log(sid)]) for sid in selected}
                if all(state in TERMINAL_STATES for state in states.values()):
                    return {"reason": "all_terminal", "task_ids": sorted(selected)}
        finally:
            if released:
                await self.gate.acquire()
                self.held.add(parent_id)

    async def merge(self, root: AsyncIterator[Emitted]) -> AsyncIterator[Emitted]:
        root_next: asyncio.Task[Emitted] | None = None
        getter: asyncio.Task[Emitted | BaseException] | None = None
        try:
            while True:
                root_next = root_next or asyncio.create_task(anext(root))
                getter = getter or asyncio.create_task(self.events.get())
                done, _ = await asyncio.wait({root_next, getter}, return_when=asyncio.FIRST_COMPLETED)
                if getter in done:
                    item = getter.result()
                    getter = None
                    if isinstance(item, BaseException):
                        raise item
                    yield item
                    continue
                try:
                    emitted = root_next.result()
                except StopAsyncIteration:
                    root_next = None
                    getter.cancel()
                    getter = None
                    while not self.events.empty():
                        item = self.events.get_nowait()
                        if isinstance(item, BaseException):
                            raise item from None
                        yield item
                    return
                root_next = None
                yield emitted
        finally:
            if getter is not None:
                getter.cancel()
            if root_next is not None:
                root_next.cancel()
            pending = [task for task in (getter, root_next) if task is not None]
            if pending:
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(*pending, return_exceptions=True)
            await root.aclose()

    async def close(self) -> None:
        self.closed = True
        runners = list(self.runners.values())
        for runner in runners:
            if not runner.done():
                runner.cancel()
        if runners:
            await asyncio.gather(*runners, return_exceptions=True)
