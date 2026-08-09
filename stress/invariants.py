from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from tantra import CompactionConfig, Lease, ModelLimits, SampleRequest, Store

WINDOW_SLACK = 1.25

TURN_EVENTS = ("turn_started", "turn_completed", "turn_failed")

ROLES = ("user", "assistant", "tool")

_MISSING = object()


def event_type(event: Any) -> str:
    return str(getattr(event, "type", ""))


async def log(store: Store, sid: str) -> list[Any]:
    """Every persisted event for `sid`, in seq order."""
    return [stamped.event async for stamped in store.read(sid)]


def picks(events: Sequence[Any], kind: str) -> list[Any]:
    return [event for event in events if event_type(event) == kind]


def _attr(message: Any, name: str, where: str) -> Any:
    value = getattr(message, name, _MISSING)
    if value is _MISSING:
        raise AssertionError(f"{where}: message exposes no {name!r}; the message shape changed under the checker")
    return value


def check_pairs(requests: Sequence[SampleRequest], *, min_pairs: int = 1) -> int:
    """Every assistant tool call is answered before the next turn boundary, and never twice.

    Returns the number of (call, result) pairs verified. Raises on an empty request list or on
    fewer than `min_pairs` pairs, so a shape mismatch can never pass as a clean sweep.
    """
    if not requests:
        raise AssertionError("check_pairs got no requests: nothing was verified")
    total = sum(_check_request(index, request) for index, request in enumerate(requests))
    if total < min_pairs:
        raise AssertionError(
            f"check_pairs verified {total} call/result pairs across {len(requests)} requests, wanted {min_pairs}"
        )
    return total


def _check_request(index: int, request: SampleRequest) -> int:
    pending: list[str] = []
    seen: set[str] = set()
    matched = 0
    for position, message in enumerate(request.messages):
        where = f"request {index} message {position}"
        role = _attr(message, "role", where)
        if role not in ROLES:
            raise AssertionError(f"{where}: unknown message role {role!r}")
        if role == "tool":
            call_id = _attr(message, "call_id", where)
            if call_id not in pending:
                raise AssertionError(f"{where}: tool result {call_id!r} answers no open call (open: {pending})")
            pending.remove(call_id)
            matched += 1
            continue
        if pending:
            raise AssertionError(f"{where}: {role} message orphans unanswered tool calls {pending}")
        if role != "assistant":
            continue
        ids = [call.id for call in _attr(message, "tool_calls", where)]
        repeated = sorted(call_id for call_id in ids if call_id in seen)
        if repeated:
            raise AssertionError(f"{where}: tool call ids {repeated} already appeared in this request")
        seen.update(ids)
        pending = list(ids)
    if pending:
        raise AssertionError(f"request {index}: ends with unanswered tool calls {pending}")
    return matched


def pairs_intact(events: Sequence[Any], *, min_calls: int = 1) -> int:
    """Every `ToolCallRequested` in a log has a result, including the ones never executed."""
    requested = picks(events, "tool_call_requested")
    if len(requested) < min_calls:
        raise AssertionError(f"pairs_intact saw {len(requested)} tool calls in the log, wanted {min_calls}")
    answered = {event.call_id for event in picks(events, "tool_call_completed")}
    missing = [event.call_id for event in requested if event.call_id not in answered]
    if missing:
        raise AssertionError(f"log ends with unanswered tool calls {missing}")
    return len(requested)


def check_window(
    requests: Sequence[SampleRequest],
    limits: ModelLimits,
    config: CompactionConfig,
    *,
    slack: float = WINDOW_SLACK,
) -> None:
    """No assembled request may exceed the usable window by more than the estimator's error margin."""
    if not requests:
        raise AssertionError("check_window got no requests: nothing was verified")
    budget = int(config.usable(limits) * slack)
    for index, request in enumerate(requests):
        size = sum(len(json.dumps(message.model_dump(), default=str)) for message in request.messages) // 4
        if size > budget:
            raise AssertionError(f"request {index}: assembled messages estimate {size} tokens, budget is {budget}")


def _live(lease: Lease) -> bool:
    expires = lease.expires_at
    now = datetime.now(UTC) if expires.tzinfo is not None else datetime.now()
    return expires > now


async def check_log(store: Store, sid: str) -> None:
    """Seqs are contiguous from 1, the header agrees, and a terminal turn leaves no live writer."""
    stamped = [item async for item in store.read(sid)]
    if not stamped:
        raise AssertionError(f"session {sid}: log is empty")

    seqs = [item.seq for item in stamped]
    expected = list(range(1, len(seqs) + 1))
    if seqs != expected:
        broken = next(seq for seq, want in zip(seqs, expected, strict=True) if seq != want)
        raise AssertionError(f"session {sid}: seqs are not contiguous from 1, first break at {broken}")

    header = await store.header(sid)
    if header is None:
        raise AssertionError(f"session {sid}: log has {len(seqs)} events but no header")
    if header.last_seq != seqs[-1]:
        raise AssertionError(f"session {sid}: header.last_seq {header.last_seq} but log ends at {seqs[-1]}")

    turns = [event_type(item.event) for item in stamped if event_type(item.event) in TURN_EVENTS]
    if not turns or turns[-1] == "turn_started":
        return
    if header.status == "running":
        raise AssertionError(f"session {sid}: turn is terminal ({turns[-1]}) but header status is 'running'")
    if header.lease is not None and _live(header.lease):
        raise AssertionError(f"session {sid}: turn is terminal but lease {header.lease.holder!r} is still live")
