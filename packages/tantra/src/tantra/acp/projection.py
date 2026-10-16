from __future__ import annotations

import json
import re
from collections.abc import Callable, Collection
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid5

from tantra.acp.wire import (
    CompactionSummaryChunk,
    CompactionUpdate,
    ContentBlock,
    ErrorObject,
    MessageChunk,
    MessageUpdate,
    QueueEntry,
    SessionMessage,
    State,
    SubagentUpdate,
    ToolCallContentChunk,
    ToolCallUpdate,
    dump,
    text_block,
)
from tantra.ask import AskRequest
from tantra.events import (
    AskAnswered,
    AskRaised,
    ChildCreated,
    CompactionApplied,
    InputQueued,
    LoggedEvent,
    ReasoningDelta,
    ReasoningPart,
    SampleCompleted,
    SampleStarted,
    SessionEvent,
    TextDelta,
    TextPart,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    ToolProgress,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    Usage,
)

FRAMEWORK_TOOLS = frozenset({"spawn", "send", "finish"})
STOP_REASONS = {"max_steps": "max_turn_requests", "finished": "_tantra_finished"}
_UUID = r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
_TURN_ENDED = re.compile(rf"\[agent {_UUID} turn ended\] ")
_FINISHED = re.compile(rf"\[agent {_UUID} finished\] (.*)\Z", re.S)
_MESSAGE = re.compile(rf"\[agent {_UUID}\] (.*)\Z", re.S)


@dataclass(frozen=True)
class AskPending:
    ask_id: str
    tool_call_id: str | None
    request: AskRequest


@dataclass(frozen=True)
class AskResolved:
    ask_id: str


@dataclass(frozen=True)
class ChildAnnounced:
    child_id: UUID
    title: str


@dataclass(frozen=True)
class TurnState:
    state: dict[str, Any]


@dataclass(frozen=True)
class UsageSample:
    used: int
    model: str


@dataclass(frozen=True)
class QueueChanged:
    entries: list[dict[str, Any]]


@dataclass(frozen=True)
class Withheld:
    pass


Output = dict[str, Any] | AskPending | AskResolved | ChildAnnounced | TurnState | UsageSample | QueueChanged | Withheld


def entity_id(value: str) -> str:
    try:
        return str(UUID(hex=value))
    except ValueError:
        return value


def context_tokens(usage: Usage) -> int:
    return usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens + usage.output_tokens


def _render(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)


def _encoded(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Projector:
    def __init__(
        self,
        actor_id: UUID,
        parent_id: UUID | None,
        decode_input: Callable[[str], list[ContentBlock]],
        *,
        hidden: Collection[str] = (),
        profile: bool = False,
    ) -> None:
        self.actor_id = actor_id
        self.parent_id = parent_id
        self.decode_input = decode_input
        self.hidden = FRAMEWORK_TOOLS & set(hidden)
        self.profile = profile
        self.state: dict[str, Any] = {"state": "idle"}
        self.asks: dict[str, AskPending] = {}
        self._id = str(actor_id)
        self._turn: str | None = None
        self._sample: str | None = None
        self._model: str | None = None
        self._thoughts: dict[str, list[str]] = {}
        self._calls: dict[str, ToolCallRequested] = {}
        self._tools: dict[str, str] = {}
        self._children: set[str] = set()
        self._withheld: set[str] = set()
        self._withheld_calls: set[str] = set()
        self._open: list[tuple[str, str]] = []
        self._deltas: dict[tuple[str, str], str] = {}
        self._queue: dict[str, dict[str, Any]] = {}
        self._origins: dict[str, tuple[str, str | None]] = {}

    @property
    def pending_asks(self) -> list[AskPending]:
        return list(self.asks.values())

    @property
    def queue(self) -> list[dict[str, Any]]:
        return list(self._queue.values())

    def withhold(self, created: ChildCreated) -> None:
        self._withheld.add(entity_id(created.child_id))
        self._withheld_calls.add(created.call_id)

    def feed(self, item: LoggedEvent, *, replay: bool = False) -> list[Output]:
        event = item.event
        if replay and isinstance(event, TextDelta | ReasoningDelta):
            if self._sample is not None:
                key = ("agent_message" if isinstance(event, TextDelta) else "agent_thought", self._sample)
                if key not in self._open:
                    self._open.append(key)
                self._deltas[key] = self._deltas.get(key, "") + event.text
            return []
        outputs = self._project(item.seq, event)
        if (
            isinstance(event, ToolCallRequested | ToolCallStarted | ToolProgress | ToolCallCompleted)
            and event.call_id in self._withheld_calls
        ):
            outputs = [Withheld() for _ in outputs]
        if not replay or not outputs:
            return outputs
        if isinstance(event, TextPart | ReasoningPart):
            key = ("agent_message" if isinstance(event, TextPart) else "agent_thought", event.sample_id)
            if key in self._open:
                index = self._open.index(key)
                flushed = self._emit(self._open[:index])
                self._open = self._open[index + 1 :]
                return [*flushed, *outputs]
        return [*self.flush(), *outputs]

    def flush(self) -> list[Output]:
        flushed = self._emit(self._open)
        self._open = []
        return flushed

    def _emit(self, keys: list[tuple[str, str]]) -> list[Output]:
        return [
            dump(
                MessageUpdate(
                    session_update=kind,
                    message_id=entity_id(sample),
                    content=[text_block(self._deltas[(kind, sample)])],
                )
            )
            for kind, sample in keys
        ]

    def _project(self, seq: int, event: SessionEvent) -> list[Output]:
        if isinstance(event, InputQueued):
            if event.origin is not None:
                self._origins[event.command_id] = (event.origin, event.sender_id)
            return self._enqueue(event)
        if isinstance(event, TurnStarted):
            self._turn = event.turn_id
            origin, sender = self._origins.pop(event.turn_id, (None, None))
            return [
                *self._input(event, origin, sender),
                *self._set_state(State(state="running")),
                *self._dequeue(event.turn_id),
            ]
        if isinstance(event, SampleStarted):
            self._sample = event.sample_id
            self._model = event.model
            self._thoughts = {}
            self._deltas = {key: text for key, text in self._deltas.items() if key in self._open}
            return []
        if isinstance(event, TextDelta | ReasoningDelta):
            if self._sample is None:
                return []
            kind = "agent_message_chunk" if isinstance(event, TextDelta) else "agent_thought_chunk"
            return [
                dump(
                    MessageChunk(
                        session_update=kind, message_id=entity_id(self._sample), content=text_block(event.text)
                    )
                )
            ]
        if isinstance(event, TextPart):
            update = MessageUpdate(
                session_update="agent_message", message_id=entity_id(event.sample_id), content=[text_block(event.text)]
            )
            return [dump(update)]
        if isinstance(event, ReasoningPart):
            blocks = self._thoughts.setdefault(event.sample_id, [])
            blocks.append(event.text)
            update = MessageUpdate(
                session_update="agent_thought",
                message_id=entity_id(event.sample_id),
                content=[text_block(text) for text in blocks],
            )
            return [dump(update)]
        if isinstance(event, ToolCallRequested):
            self._tools[event.call_id] = self._tool_id(event.sample_id, event.call_id)
            self._withheld_calls.discard(event.call_id)
            if event.name in ("send", "status") and entity_id(str(event.args.get("agent_id"))) in self._withheld:
                self._withheld_calls.add(event.call_id)
            if event.name in self.hidden:
                self._calls[event.call_id] = event
                return []
            self._calls.pop(event.call_id, None)
            return [dump(self._announce(event, status="pending"))]
        if isinstance(event, ToolCallStarted):
            if event.call_id in self._calls:
                return []
            return [dump(ToolCallUpdate(tool_call_id=self._tool(event.call_id), status="in_progress"))]
        if isinstance(event, ToolProgress):
            if event.call_id in self._calls:
                return []
            return [dump(self._content(self._tool(event.call_id), event.message))]
        if isinstance(event, ToolCallCompleted):
            return self._completed(event)
        if isinstance(event, ChildCreated):
            return self._child(event)
        if isinstance(event, AskRaised):
            tool_call_id = self._tool(event.call_id) if event.call_id is not None else None
            pending = AskPending(event.ask_id, tool_call_id, event.request)
            self.asks[event.ask_id] = pending
            meta = {"tantra": {"ask": {"askId": entity_id(event.ask_id), "state": "pending"}}}
            return [*self._set_state(State(state="requires_action", meta=meta)), pending]
        if isinstance(event, AskAnswered):
            self.asks.pop(event.ask_id, None)
            meta = {"tantra": {"ask": {"askId": entity_id(event.ask_id), "state": "answered"}}}
            state = State(state="requires_action" if self.asks else "running", meta=meta)
            return [AskResolved(event.ask_id), *self._set_state(state)]
        if isinstance(event, SampleCompleted):
            return [UsageSample(context_tokens(event.usage), self._model)] if self._model is not None else []
        if isinstance(event, CompactionApplied):
            compaction_id = str(uuid5(self.actor_id, f"compaction:{seq}"))
            outputs: list[Output] = [
                dump(CompactionUpdate(compaction_id=compaction_id, status="completed", summary=[]))
            ]
            if event.summary:
                outputs.append(
                    dump(CompactionSummaryChunk(compaction_id=compaction_id, content=text_block(event.summary)))
                )
            return outputs
        if isinstance(event, TurnCompleted | TurnCancelled | TurnFailed | TurnInterrupted):
            self._origins.pop(event.turn_id, None)
            return [*self._terminal(event), *self._dequeue(event.turn_id)]
        return []

    def _synthetic(self, text: str, message_id: str) -> list[Output] | None:
        for pattern in (_TURN_ENDED, _FINISHED, _MESSAGE):
            match = pattern.match(text)
            if match is not None and match[1] in self._withheld:
                return [Withheld()]
        ended = _TURN_ENDED.match(text)
        if ended is not None and ended[1] in self._children:
            return []
        finished = _FINISHED.match(text)
        if finished is not None and finished[1] in self._children:
            return [self._message(message_id, finished[1], self._id, finished[2], finished=True)]
        sent = _MESSAGE.match(text)
        if sent is not None and (sent[1] in self._children or sent[1] == str(self.parent_id)):
            return [self._message(message_id, sent[1], self._id, sent[2])]
        if self.parent_id is not None:
            return [self._message(message_id, str(self.parent_id), self._id, text)]
        return None

    def _classify(self, text: str, message_id: str, origin: str | None, sender: str | None) -> list[Output] | None:
        if origin == "user":
            return None
        sender_id = entity_id(sender) if sender is not None else None
        if origin in ("message", "finished", "turn_ended") and sender_id in self._withheld:
            return [Withheld()]
        parent = str(self.parent_id) if self.parent_id is not None else None
        child = sender_id in self._children
        if origin == "spawn" and sender_id is not None and sender_id == parent:
            return [self._message(message_id, sender_id, self._id, text)]
        if origin in ("message", "finished") and sender_id is not None and (child or sender_id == parent):
            finished = origin == "finished"
            prefix = f"[agent {sender_id} finished] " if finished else f"[agent {sender_id}] "
            if text.startswith(prefix):
                return [self._message(message_id, sender_id, self._id, text[len(prefix) :], finished=finished)]
        if origin == "turn_ended" and child:
            return []
        return self._synthetic(text, message_id)

    def _input(self, event: TurnStarted, origin: str | None, sender: str | None) -> list[Output]:
        message_id = entity_id(event.turn_id)
        synthetic = self._classify(event.input, message_id, origin, sender)
        if synthetic is not None:
            return synthetic
        update = MessageUpdate(
            session_update="user_message", message_id=message_id, content=self.decode_input(event.input)
        )
        return [dump(update)]

    def _enqueue(self, event: InputQueued) -> list[Output]:
        command_id = entity_id(event.command_id)
        if not self.profile or self._classify(event.input, command_id, event.origin, event.sender_id) is not None:
            return []
        entry = QueueEntry(
            command_id=command_id, content=self.decode_input(event.input), submitted_by=event.submitted_by
        )
        self._queue[event.command_id] = dump(entry)
        return [QueueChanged(self.queue)]

    def _dequeue(self, command_id: str) -> list[Output]:
        if self._queue.pop(command_id, None) is None:
            return []
        return [QueueChanged(self.queue)]

    def _message(self, message_id: str, sender: str, recipient: str, text: str, *, finished: bool = False) -> Output:
        message = SessionMessage(
            message_id=message_id,
            sender_session_id=sender,
            recipient_session_id=recipient,
            content=[text_block(text)],
            meta={"tantra": {"finished": True}} if finished else None,
        )
        return dump(message)

    def _outgoing(self, call: ToolCallRequested, recipient: str, text: str, *, finished: bool = False) -> Output:
        message_id = str(uuid5(self.actor_id, f"{call.sample_id}:{call.call_id}"))
        return self._message(message_id, self._id, recipient, text, finished=finished)

    def _tool_id(self, sample_id: str | None, call_id: str) -> str:
        return str(uuid5(self.actor_id, f"{sample_id}:{call_id}"))

    def _tool(self, call_id: str) -> str:
        return self._tools.get(call_id) or self._tool_id(self._sample, call_id)

    def _announce(self, call: ToolCallRequested, *, status: str) -> ToolCallUpdate:
        return ToolCallUpdate(
            tool_call_id=self._tool(call.call_id),
            name=call.name,
            title=call.name,
            kind="other",
            status=status,
            content=[],
            raw_input=call.args,
        )

    def _content(self, call_id: str, text: str) -> ToolCallContentChunk:
        return ToolCallContentChunk(tool_call_id=call_id, content={"type": "content", "content": text_block(text)})

    def _completed(self, event: ToolCallCompleted) -> list[Output]:
        call = self._calls.pop(event.call_id, None)
        status = "failed" if event.is_error else "completed"
        if call is not None and not event.is_error:
            if call.name == "send":
                recipient = call.args.get("agent_id")
                return [self._outgoing(call, entity_id(str(recipient)), str(call.args.get("input", "")))]
            if call.name == "finish":
                return [self._outgoing(call, str(self.parent_id), _encoded(event.result), finished=True)]
            return []
        tool_call_id = self._tool(event.call_id)
        if call is not None:
            update = self._announce(call, status=status).model_copy(update={"raw_output": event.result})
        else:
            update = ToolCallUpdate(tool_call_id=tool_call_id, status=status, raw_output=event.result)
        outputs: list[Output] = [dump(update)]
        if event.result is not None and event.result != "":
            outputs.append(dump(self._content(tool_call_id, _render(event.result))))
        return outputs

    def _child(self, event: ChildCreated) -> list[Output]:
        child_id = UUID(hex=event.child_id)
        self._children.add(str(child_id))
        title = event.name or event.agent
        outputs: list[Output] = [
            dump(SubagentUpdate(session_id=str(child_id), title=title)),
            ChildAnnounced(child_id, title),
        ]
        call = self._calls.get(event.call_id)
        if call is not None and call.name == "spawn":
            outputs.append(self._outgoing(call, str(child_id), str(call.args.get("input", ""))))
        return outputs

    def _terminal(self, event: TurnCompleted | TurnCancelled | TurnFailed | TurnInterrupted) -> list[Output]:
        if event.turn_id != self._turn:
            return []
        self._turn = None
        self._calls = {}
        self._tools = {}
        self._withheld_calls = set()
        resolved: list[Output] = [AskResolved(ask_id) for ask_id in self.asks]
        self.asks = {}
        if isinstance(event, TurnCompleted):
            state = State(state="idle", stop_reason=STOP_REASONS.get(event.stop_reason, "end_turn"))
        elif isinstance(event, TurnCancelled):
            state = State(state="idle", stop_reason="cancelled")
        elif isinstance(event, TurnFailed):
            state = State(state="idle", stop_reason="error", error=ErrorObject(code=-32603, message=event.error))
        else:
            state = State(state="idle", stop_reason="_tantra_interrupted")
        return [*resolved, *self._set_state(state)]

    def _set_state(self, state: State) -> list[Output]:
        self.state = dump(state)
        return [{"sessionUpdate": "state_update", **self.state}, TurnState(self.state)]
