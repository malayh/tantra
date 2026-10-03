from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid5

import psycopg

from stress.driver import call_policy, last_user, turn_step
from tantra import (
    Agent,
    CommandTimeout,
    Context,
    FreeText,
    PostgresCoordinator,
    Runtime,
    WriterReplaced,
    WriterRequired,
    tool,
)
from tantra.events import (
    AgentFinished,
    AskAnswered,
    AskRaised,
    CancellationRequested,
    InputQueued,
    TurnCancelled,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
)
from tantra.providers.base import ToolCall
from tantra.providers.fake import Sample


@tool(description="Wait for a durable fault-campaign answer.")
async def fault_wait(ctx: Context) -> str:
    response = await ctx.ask(FreeText(prompt="fault campaign approval"))
    return response.text


class FaultChild(Agent):
    model = "bench/fault-child"


class FaultRoot(Agent):
    model = "bench/fault-root"
    tools = [fault_wait]
    subagents = [FaultChild]


FAULT_AGENTS = [FaultRoot]
FAULT_SCENARIOS = (
    "writer-reconnect",
    "owner-sigkill",
    "cancel-boundaries",
    "lost-notifications",
    "slow-reader",
    "database-outage",
)


def _fault_policy(req: Any, state: Any) -> Sample:
    if req.model == FaultChild.model:
        return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name="finish", args='{"result":"child complete"}')])
    if req.model == FaultRoot.model:
        if last_user(req) == "hold" and turn_step(req) < 1:
            return Sample(tool_calls=[ToolCall(id=state.next_call_id(), name="fault_wait", args="{}")])
        return Sample(text="drained")
    return call_policy("fixture_total", {}, answer="60")(req, state)


async def _events(runtime: Runtime, sid: str) -> list[Any]:
    return [item.event async for item in runtime.store.read(sid)]


async def _eventually(check: Callable[[], Any], timeout: float = 10) -> Any:
    async with asyncio.timeout(timeout):
        while True:
            result = await check()
            if result:
                return result
            await asyncio.sleep(0.01)


class FaultState:
    def __init__(self, runtime: Runtime, provider: Any) -> None:
        self.runtime = runtime
        self.provider = provider
        self.connections: dict[str, Any] = {}
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        self.observed: dict[str, list[int]] = {}
        self.slow: dict[str, tuple[asyncio.Task[Any], Any, asyncio.Queue[Any], int]] = {}
        self.original_apply: Any = None
        self.original_request: Any = None
        self.lost_reply: Any = None
        self.source = getattr(provider, "source", None)

    async def operation(self, request: dict[str, Any]) -> dict[str, Any] | None:
        op = request["op"]
        sid = request.get("sid", "")
        if op in ("fault_setup", "fault_create"):
            if self.source is None or not hasattr(self.source, "policy"):
                raise AssertionError("fault campaign requires a synthetic provider")
            self.source.policy = _fault_policy
            if op == "fault_setup":
                return {"synthetic": True}
            await self.runtime.create(FaultRoot, session_id=UUID(hex=sid))
            return {"sid": sid}
        if op == "fault_claim":
            connection = self.runtime.connect(UUID(hex=sid), writable=True)
            await connection.__aenter__()
            self.connections[sid] = connection
            return {"writable": True}
        if op == "fault_release":
            connection = self.connections.pop(sid)
            await connection.__aexit__(None, None, None)
            return {"released": True}
        if op == "fault_gate":
            self.provider.gate.set() if request["open"] else self.provider.gate.clear()
            return {"open": request["open"]}
        if op == "fault_send":
            try:
                receipt = await self.connections[sid].send(
                    request.get("input", "fault"), command_id=UUID(hex=request["command"])
                )
                return {"accepted": True, "duplicate": receipt.duplicate}
            except Exception as exc:
                if not request.get("expect_error"):
                    raise
                return {"accepted": False, "error": f"{type(exc).__name__}: {exc}"}
        if op == "fault_prompt":
            result = await self.connections[sid].prompt(
                request.get("input", "fault"), command_id=UUID(hex=request["command"])
            )
            if result.outcome != "completed":
                raise AssertionError(f"command did not complete: {result.outcome}: {result.error}")
            return {"outcome": result.outcome, "text": result.text}
        if op == "fault_retry":
            command = UUID(hex=request["command"])
            receipt = await self.connections[sid].send(request.get("input", "fault"), command_id=command)
            result = await self.connections[sid].prompt(request.get("input", "fault"), command_id=command)
            allowed = request.get("allowed", ["completed"])
            if result.outcome not in allowed:
                raise AssertionError(f"retry outcome {result.outcome!r} not in {allowed!r}")
            return {
                "duplicate": receipt.duplicate,
                "outcome": result.outcome,
                "text": result.text,
                "error": result.error,
            }
        if op == "fault_wait_started":
            command = request["command"]

            async def started() -> bool:
                status = await self.runtime.status(UUID(hex=sid))
                return status.current_turn_id == UUID(hex=command)

            await _eventually(started)
            return {"started": True}
        if op == "fault_old_writer":
            try:
                await self.connections[sid].send("stale", command_id=UUID(hex=request["command"]))
            except (WriterReplaced, WriterRequired):
                return {"denied": True}
            raise AssertionError("old writer was allowed")
        if op == "fault_journal":
            after = int(request.get("after", 0))
            cursor, items = after, []
            while True:
                page = await self.runtime.store.read_page(sid, after=cursor)
                if page:
                    items.extend(page)
                    cursor = page[-1].seq
                    continue
                header = await self.runtime.store.header(sid)
                if header is None:
                    raise AssertionError("missing session")
                if cursor >= header.last_seq:
                    break
            expected = after + 1
            digest = hashlib.sha256()
            for item in items:
                if item.seq != expected:
                    raise AssertionError(f"cursor gap at {item.seq}, expected {expected}")
                expected += 1
                digest.update(item.event.model_dump_json().encode())
            return {
                "after": after,
                "cursor": header.last_seq,
                "events": len(items),
                "first": items[0].seq if items else None,
                "sha256": digest.hexdigest(),
            }
        if op == "fault_reconnect":
            after = int(request["after"])
            header = await self.runtime.store.header(sid)
            if header is None:
                raise AssertionError("missing session")
            target = header.last_seq
            connection = self.runtime.connect(UUID(hex=sid), after=after)
            await connection.__aenter__()
            items = []
            try:
                async for item in connection:
                    items.append(item)
                    if item.seq == target:
                        break
            finally:
                await connection.__aexit__(None, None, None)
            if [item.seq for item in items] != list(range(after + 1, target + 1)):
                raise AssertionError("reconnected Runtime stream was not contiguous")
            digest = hashlib.sha256()
            for item in items:
                digest.update(item.event.model_dump_json().encode())
            suffix_sha256 = digest.hexdigest()
            full = self.runtime.connect(UUID(hex=sid))
            await full.__aenter__()
            replay = []
            try:
                async for item in full:
                    replay.append(item)
                    if item.seq == target:
                        break
            finally:
                await full.__aexit__(None, None, None)
            if [item.seq for item in replay] != list(range(1, target + 1)):
                raise AssertionError("full Runtime replay was not contiguous")
            prefix_bytes = b"".join(item.event.model_dump_json().encode() for item in replay[:after])
            suffix_bytes = b"".join(item.event.model_dump_json().encode() for item in replay[after:])
            prefix_sha256 = hashlib.sha256(prefix_bytes).hexdigest()
            if prefix_sha256 != request["prefix_sha256"]:
                raise AssertionError("disconnected replay prefix changed")
            if hashlib.sha256(suffix_bytes).hexdigest() != suffix_sha256:
                raise AssertionError("reconnected suffix differs from full public replay")
            full_sha256 = hashlib.sha256(prefix_bytes + suffix_bytes).hexdigest()
            return {
                "after": after,
                "cursor": target,
                "events": len(items),
                "first": items[0].seq if items else None,
                "prefix_sha256": prefix_sha256,
                "suffix_sha256": suffix_sha256,
                "full_sha256": full_sha256,
                "digest_parity": True,
            }
        if op == "fault_prepare_owner_death":
            running, queued = request["running"], request["queued"]
            await self.connections[sid].send("hold", command_id=UUID(hex=running))

            async def raised() -> AskRaised | None:
                return next((event for event in await _events(self.runtime, sid) if isinstance(event, AskRaised)), None)

            ask = await _eventually(raised)
            header = await self.runtime.store.header(sid)
            if header is None:
                raise AssertionError("missing root")

            async def emit(_: str) -> None:
                return None

            child = await self.runtime._actor_spawn(
                header,
                FaultRoot,
                Context(
                    session_id=sid,
                    turn_id=running,
                    call_id=request["child_call"],
                    depth=0,
                    deps=None,
                    store=self.runtime.store,
                    emit=emit,
                ),
                "fault_child",
                "finish",
            )
            child_id = UUID(child).hex

            async def notified() -> bool:
                prefix = f"[agent {UUID(hex=child_id)} "
                return any(
                    isinstance(event, InputQueued)
                    and event.input.startswith(prefix)
                    and (" finished]" in event.input or " turn ended]" in event.input)
                    for event in await _events(self.runtime, sid)
                )

            await _eventually(notified)
            await self.connections[sid].send("drain", command_id=UUID(hex=queued))
            return {"ask": ask.ask_id, "child": child_id, "running": running, "queued": queued}
        if op == "fault_verify_recovery":
            running, queued, child_id = request["running"], request["queued"], request["child"]

            async def drained() -> bool:
                events = await _events(self.runtime, sid)
                lifecycle = [
                    event
                    for event in events
                    if isinstance(event, InputQueued)
                    and event.input.startswith(f"[agent {UUID(hex=child_id)} ")
                    and (" finished]" in event.input or " turn ended]" in event.input)
                ]
                return bool(
                    any(isinstance(event, TurnCompleted) and event.turn_id == queued for event in events)
                    and lifecycle
                    and any(
                        isinstance(event, TurnCompleted) and event.turn_id == lifecycle[0].command_id
                        for event in events
                    )
                )

            await _eventually(drained)
            events = await _events(self.runtime, sid)
            child_events = await _events(self.runtime, child_id)
            lifecycle = [
                event
                for event in events
                if isinstance(event, InputQueued)
                and event.input.startswith(f"[agent {UUID(hex=child_id)} ")
                and (" finished]" in event.input or " turn ended]" in event.input)
            ]
            header = await self.runtime.store.header(sid)
            checks = {
                "interrupted_once": sum(
                    isinstance(event, TurnInterrupted) and event.turn_id == running for event in events
                )
                == 1,
                "ask_unanswered": sum(isinstance(event, AskRaised) for event in events) == 1
                and not any(isinstance(event, AskAnswered) for event in events),
                "queued_once": sum(isinstance(event, InputQueued) and event.command_id == queued for event in events)
                == 1,
                "queued_completed": sum(
                    isinstance(event, TurnCompleted) and event.turn_id == queued for event in events
                )
                == 1,
                "child_notice_once": len(lifecycle) == 1,
                "child_finished_once": sum(isinstance(event, AgentFinished) for event in child_events) == 1,
                "pending_ask_cleared": header is not None and header.pending_ask is None,
            }
            if not all(checks.values()):
                raise AssertionError(checks)
            return checks
        if op == "fault_cancel_precommit_on":
            coordinator = self.runtime.coordinator
            if not isinstance(coordinator, PostgresCoordinator):
                raise AssertionError("PostgresCoordinator required")
            self.original_apply = coordinator._apply_request
            failed = False

            async def fail(envelope: Any, ownership: Any, operation: Any) -> Any:
                async def wrapped(locked: Any, store: Any) -> Any:
                    nonlocal failed
                    reply = await operation(locked, store)
                    if envelope.operation == "cancel" and not failed:
                        failed = True
                        raise RuntimeError("injected before cancellation commit")
                    return reply

                return await self.original_apply(envelope, ownership, wrapped)

            coordinator._apply_request = fail
            return {"armed": True}
        if op == "fault_cancel_precommit_off":
            if self.original_apply is not None:
                self.runtime.coordinator._apply_request = self.original_apply
                self.original_apply = None
            return {"armed": False}
        if op == "fault_cancel_lost_reply_on":
            coordinator = self.runtime.coordinator
            if not isinstance(coordinator, PostgresCoordinator):
                raise AssertionError("PostgresCoordinator required")
            self.original_request = coordinator.request
            lost = False

            async def lose(envelope: Any) -> Any:
                nonlocal lost
                reply = await self.original_request(envelope)
                if envelope.operation == "cancel" and not lost:
                    lost = True
                    self.lost_reply = reply.model_dump(mode="json")
                    raise CommandTimeout("injected committed reply loss")
                return reply

            coordinator.request = lose
            return {"armed": True}
        if op == "fault_cancel_lost_reply_off":
            if self.original_request is not None:
                self.runtime.coordinator.request = self.original_request
                self.original_request = None
            return {"armed": False, "reply": self.lost_reply}
        if op == "fault_cancel":
            try:
                receipt = await self.connections[sid].cancel(command_id=UUID(hex=request["command"]))
                return {"accepted": True, "duplicate": receipt.duplicate}
            except Exception as exc:
                if not request.get("expect_error"):
                    raise
                return {"accepted": False, "error": f"{type(exc).__name__}: {exc}"}
        if op == "fault_verify_cancel":
            expected_acceptances = request.get("count", 1)
            expected_cancelled = request.get("cancelled", 1)
            cancel, first, later = request["cancel"], request["first"], request.get("later")
            if expected_acceptances or expected_cancelled:

                async def durable() -> bool:
                    current = await _events(self.runtime, sid)
                    return (
                        sum(
                            isinstance(event, CancellationRequested) and event.command_id == cancel for event in current
                        )
                        == expected_acceptances
                        and sum(isinstance(event, TurnCancelled) and event.turn_id == first for event in current)
                        == expected_cancelled
                    )

                await _eventually(durable)
            events = await _events(self.runtime, sid)
            cancellations = [
                event for event in events if isinstance(event, CancellationRequested) and event.command_id == cancel
            ]
            checks = {
                "one_acceptance": len(cancellations) == expected_acceptances,
                "original_target": not cancellations or cancellations[0].targets == {sid: [first]},
                "first_cancelled_once": sum(
                    isinstance(event, TurnCancelled) and event.turn_id == first for event in events
                )
                == expected_cancelled,
                "later_spared": later is None
                or not any(isinstance(event, TurnCancelled) and event.turn_id == later for event in events),
            }
            if not all(checks.values()):
                raise AssertionError(
                    {
                        **checks,
                        "acceptances": len(cancellations),
                        "targets": [event.targets for event in cancellations],
                        "cancelled": sum(
                            isinstance(event, TurnCancelled) and event.turn_id == first for event in events
                        ),
                    }
                )
            return checks
        if op == "fault_verify_command":
            command = request["command"]
            events = await _events(self.runtime, sid)
            inputs = [event for event in events if isinstance(event, InputQueued) and event.command_id == command]
            terminals = [
                event
                for event in events
                if isinstance(event, TurnCompleted | TurnInterrupted | TurnCancelled | TurnFailed)
                and event.turn_id == command
            ]
            allowed = request.get("allowed", ["turn_completed"])
            checks = {
                "one_input": len(inputs) == 1,
                "one_terminal": len(terminals) == 1,
                "terminal_allowed": len(terminals) == 1 and terminals[0].type in allowed,
            }
            if not all(checks.values()):
                raise AssertionError(
                    {
                        **checks,
                        "inputs": len(inputs),
                        "terminals": [event.type for event in terminals],
                    }
                )
            return {**checks, "terminal": terminals[0].type}
        if op == "fault_listener_disconnect":
            coordinator = self.runtime.coordinator
            task = getattr(coordinator, "_listener_task", None)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                coordinator._listener_task = None
            return {"disconnected": True}
        if op == "fault_drop_changes":
            async with self.runtime.store._connection() as conn:
                result = await conn.execute(
                    self.runtime.store._sql("DELETE FROM {schema}.coordinator_changes WHERE root_id = %s"),
                    (sid,),
                )
            return {"deleted": result.rowcount}
        if op == "fault_observe_start":
            after = request.get("after", 0)
            connection = self.runtime.connect(UUID(hex=sid), after=after)
            await connection.__aenter__()
            seen = self.observed.setdefault(sid, [])

            async def observe() -> None:
                try:
                    async for item in connection:
                        seen.append(item.seq)
                finally:
                    await connection.__aexit__(None, None, None)

            self.tasks[f"observe:{sid}"] = asyncio.create_task(observe())
            return {"after": after}
        if op == "fault_observe_wait":
            cursor = request["cursor"]

            async def observed() -> bool:
                return bool(self.observed.get(sid) and self.observed[sid][-1] >= cursor)

            await _eventually(observed)
            return {"seen": self.observed[sid][-1], "events": len(self.observed[sid])}
        if op == "fault_observe_stop":
            task = self.tasks.pop(f"observe:{sid}")
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await self._wait_cleanup()
            coordinator = self.runtime.coordinator
            if isinstance(coordinator, PostgresCoordinator) and coordinator._listener_task is None:
                coordinator._listener_task = asyncio.create_task(coordinator._listen())
            return {
                "watchers": len(self.runtime._watchers),
                "subscriptions": len(self.runtime.coordinator._observations),
                "listener": self.runtime.coordinator._listener_task is not None,
            }
        if op == "fault_resources":
            await self._wait_cleanup()
            return {
                "watchers": len(self.runtime._watchers),
                "subscriptions": len(self.runtime.coordinator._observations),
            }
        if op == "fault_expire_transport":
            old = datetime.now(UTC) - timedelta(hours=25)
            async with self.runtime.store._connection() as conn:
                await conn.execute(
                    self.runtime.store._sql(
                        "UPDATE {schema}.coordinator_changes SET created_at = %s WHERE root_id = %s"
                    ),
                    (old, sid),
                )
                await conn.execute(
                    self.runtime.store._sql(
                        "UPDATE {schema}.coordinator_requests SET completed_at = %s"
                        " WHERE root_id = %s AND reply IS NOT NULL"
                    ),
                    (old, sid),
                )
            removed = await self.runtime.coordinator.cleanup(10_000)
            events = await _events(self.runtime, sid)
            command = request["command"]
            if sum(isinstance(event, InputQueued) and event.command_id == command for event in events) != 1:
                raise AssertionError("transport cleanup changed durable command identity")
            return {"removed": removed, "identity": command}
        if op == "fault_slow_start":
            after = request.get("after", 0)
            connection = self.runtime.connect(UUID(hex=sid), after=after)
            await connection.__aenter__()
            queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)

            async def consume() -> None:
                async for item in connection:
                    await queue.put(item)

            task = asyncio.create_task(consume())
            self.slow[sid] = (task, connection, queue, after)
            return {"after": after, "capacity": queue.maxsize}
        if op == "fault_slow_stop":
            task, connection, queue, after = self.slow.pop(sid)
            await _eventually(lambda: asyncio.sleep(0, result=queue.full()))
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await connection.__aexit__(None, None, None)
            header = await self.runtime.store.header(sid)
            if header is None:
                raise AssertionError("missing session")
            cursor, items = after, []
            while cursor < header.last_seq:
                page = await self.runtime.store.read_page(sid, after=cursor)
                if not page:
                    raise AssertionError("slow-reader catch-up stopped before the committed cursor")
                items.extend(page)
                cursor = page[-1].seq
            if [item.seq for item in items] != list(range(after + 1, header.last_seq + 1)):
                raise AssertionError("slow-reader cursor catch-up was incomplete")
            await self._wait_cleanup()
            return {
                "events": len(items),
                "cursor": header.last_seq,
                "watchers": len(self.runtime._watchers),
                "subscriptions": len(self.runtime.coordinator._observations),
            }
        if op == "fault_fixture_total":
            async with await psycopg.AsyncConnection.connect(self.runtime.store.dsn, autocommit=True) as conn:
                row = await (
                    await conn.execute(self.runtime.store._sql("SELECT sum(amount) FROM {schema}.fixture"))
                ).fetchone()
            return {"total": int(row[0])}
        return None

    async def _wait_cleanup(self) -> None:
        async with asyncio.timeout(5):
            while self.runtime._watchers or self.runtime.coordinator._observations:
                await asyncio.sleep(0.01)

    async def close(self) -> None:
        if self.original_apply is not None:
            self.runtime.coordinator._apply_request = self.original_apply
        if self.original_request is not None:
            self.runtime.coordinator.request = self.original_request
        for task in [*self.tasks.values(), *(item[0] for item in self.slow.values())]:
            task.cancel()
        await asyncio.gather(*self.tasks.values(), *(item[0] for item in self.slow.values()), return_exceptions=True)
        for connection in list(self.connections.values()):
            try:
                await connection.__aexit__(None, None, None)
            except Exception:
                pass
        for _, connection, _, _ in self.slow.values():
            try:
                await connection.__aexit__(None, None, None)
            except Exception:
                pass
        self.connections.clear()
        self.tasks.clear()
        self.slow.clear()


def _id(base: str, name: str) -> str:
    return uuid5(UUID(hex=base), f"faults-v1/{name}").hex


def _scenario(report: dict[str, Any], name: str, run: Callable[[], dict[str, Any] | None]) -> None:
    started = time.perf_counter()
    try:
        evidence = run()
    except Exception as exc:
        report.setdefault("scenarios", []).append(
            {
                "name": name,
                "status": "failed",
                "classification": "oracle" if isinstance(exc, AssertionError) else "runtime",
                "elapsed_ms": (time.perf_counter() - started) * 1_000,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        print(f"fault/{name}: FAILED {type(exc).__name__}: {exc}", flush=True)
    else:
        report.setdefault("scenarios", []).append(
            {
                "name": name,
                "status": "passed",
                "classification": "postconditions",
                "elapsed_ms": (time.perf_counter() - started) * 1_000,
                "evidence": {"postconditions": "passed", **(evidence or {})},
            }
        )
        print(f"fault/{name}: PASSED", flush=True)


def fault_campaign(
    report: dict[str, Any],
    workers: list[Any],
    settings: dict[str, Any],
    ids: list[str],
    args: Any,
    database: Any,
    measured: Callable[..., dict[str, Any]],
    Worker: type[Any],
) -> None:
    names = set(FAULT_SCENARIOS)
    selected = set(getattr(args, "scenario", None) or names)
    unknown = selected - names
    if unknown:
        raise ValueError(f"unknown fault scenarios: {sorted(unknown)}")
    if len(ids) < 2:
        raise ValueError("fault campaign requires at least two seeded sessions")
    roots = {name: _id(ids[index % len(ids)], name) for index, name in enumerate(sorted(names))}
    a, b = workers

    def call(worker: Any, label: str, operation: str, **kwargs: Any) -> dict[str, Any]:
        return measured(report, worker, label, operation, **kwargs)

    call(a, "fault/worker-0/setup", "fault_setup")
    call(b, "fault/worker-1/setup", "fault_setup")

    def writer_reconnect() -> None:
        sid = ids[0]
        command = _id(sid, "writer-command")
        stale = _id(sid, "stale-command")
        call(a, "fault/writer/claim", "fault_claim", sid=sid)
        call(a, "fault/writer/gate", "fault_gate", open=False)
        call(a, "fault/writer/send", "fault_send", sid=sid, command=command, input="total")
        call(a, "fault/writer/started", "fault_wait_started", sid=sid, command=command)
        before = call(a, "fault/writer/disconnect", "fault_journal", sid=sid)
        call(b, "fault/writer/replace", "fault_claim", sid=sid)
        denied = call(a, "fault/writer/old-denied", "fault_old_writer", sid=sid, command=stale)
        if not denied["denied"]:
            raise AssertionError("old writer was not denied")
        call(a, "fault/writer/open", "fault_gate", open=True)
        call(b, "fault/writer/complete", "fault_prompt", sid=sid, command=command, input="total")
        after = call(
            b,
            "fault/writer/reconnect",
            "fault_reconnect",
            sid=sid,
            after=before["cursor"],
            prefix_sha256=before["sha256"],
        )
        if after["events"] < 1 or after["first"] != before["cursor"] + 1 or not after["digest_parity"]:
            raise AssertionError("reconnect did not preserve contiguous replay parity")
        call(a, "fault/writer/release-old", "fault_release", sid=sid)
        call(b, "fault/writer/release", "fault_release", sid=sid)

    def owner_sigkill() -> None:
        nonlocal a
        sid = roots["owner-sigkill"]
        running, queued, child_call = (_id(sid, name) for name in ("running", "queued", "child"))
        call(a, "fault/owner/create", "fault_create", sid=sid)
        call(a, "fault/owner/claim", "fault_claim", sid=sid)
        evidence = call(
            a,
            "fault/owner/prepare",
            "fault_prepare_owner_death",
            sid=sid,
            running=running,
            queued=queued,
            child_call=child_call,
        )
        a.stop(force=True)
        try:
            time.sleep(float(settings.get("coordinator", {}).get("lease_ttl", 10.0)) + 0.25)
            call(b, "fault/owner/takeover", "fault_claim", sid=sid)
            call(
                b,
                "fault/owner/verify",
                "fault_verify_recovery",
                sid=sid,
                running=running,
                queued=queued,
                child=evidence["child"],
            )
            call(b, "fault/owner/release", "fault_release", sid=sid)
        finally:
            workers[0] = a = Worker(settings)
            call(a, "fault/owner/replacement-setup", "fault_setup")

    def cancel_boundaries() -> None:
        sid = ids[1]
        first, cancel, later = (_id(sid, name) for name in ("cancel-first", "cancel", "cancel-later"))
        call(a, "fault/cancel/claim-owner", "fault_claim", sid=sid)
        call(a, "fault/cancel/gate", "fault_gate", open=False)
        call(a, "fault/cancel/send-first", "fault_send", sid=sid, command=first)
        call(a, "fault/cancel/started", "fault_wait_started", sid=sid, command=first)
        call(b, "fault/cancel/claim-writer", "fault_claim", sid=sid)
        call(a, "fault/cancel/precommit-on", "fault_cancel_precommit_on")
        failed = call(b, "fault/cancel/precommit", "fault_cancel", sid=sid, command=cancel, expect_error=True)
        call(a, "fault/cancel/precommit-off", "fault_cancel_precommit_off")
        if failed["accepted"] or "owner could not apply" not in failed["error"]:
            raise AssertionError("precommit fault committed")
        call(
            b,
            "fault/cancel/rollback",
            "fault_verify_cancel",
            sid=sid,
            cancel=cancel,
            first=first,
            count=0,
            cancelled=0,
        )
        call(b, "fault/cancel/lost-on", "fault_cancel_lost_reply_on")
        lost = call(b, "fault/cancel/lost-reply", "fault_cancel", sid=sid, command=cancel, expect_error=True)
        restored = call(b, "fault/cancel/lost-off", "fault_cancel_lost_reply_off")
        if lost["accepted"] or restored["reply"]["result"]["duplicate"]:
            raise AssertionError("lost reply was delivered")
        call(b, "fault/cancel/send-later", "fault_send", sid=sid, command=later)
        duplicate = call(b, "fault/cancel/retry", "fault_cancel", sid=sid, command=cancel)
        if not duplicate["duplicate"]:
            raise AssertionError("same cancellation id was not deduplicated")
        call(
            b,
            "fault/cancel/targets",
            "fault_verify_cancel",
            sid=sid,
            cancel=cancel,
            first=first,
            later=later,
        )
        call(a, "fault/cancel/open", "fault_gate", open=True)
        call(b, "fault/cancel/complete-later", "fault_prompt", sid=sid, command=later)
        call(a, "fault/cancel/release-owner", "fault_release", sid=sid)
        call(b, "fault/cancel/release", "fault_release", sid=sid)

    def lost_notifications() -> None:
        sid = roots["lost-notifications"]
        command = _id(sid, "transport-command")
        call(a, "fault/transport/create", "fault_create", sid=sid)
        start = call(b, "fault/transport/cursor", "fault_journal", sid=sid)
        call(b, "fault/transport/observe", "fault_observe_start", sid=sid, after=start["cursor"])
        call(b, "fault/transport/disconnect", "fault_listener_disconnect")
        call(a, "fault/transport/claim", "fault_claim", sid=sid)
        call(a, "fault/transport/send", "fault_prompt", sid=sid, command=command, input="drain")
        original = call(
            a,
            "fault/transport/original",
            "fault_verify_command",
            sid=sid,
            command=command,
            allowed=["turn_completed"],
        )
        call(a, "fault/transport/drop", "fault_drop_changes", sid=sid)
        cursor = call(a, "fault/transport/current", "fault_journal", sid=sid)["cursor"]
        call(b, "fault/transport/recovered", "fault_observe_wait", sid=sid, cursor=cursor)
        call(a, "fault/transport/expire", "fault_expire_transport", sid=sid, command=command)
        retried = call(
            a,
            "fault/transport/retry",
            "fault_retry",
            sid=sid,
            command=command,
            input="drain",
        )
        if not retried["duplicate"] or retried["outcome"] != "completed" or retried["text"] != "drained":
            raise AssertionError("transport cleanup changed the durable result")
        repeated = call(
            a,
            "fault/transport/identity",
            "fault_verify_command",
            sid=sid,
            command=command,
            allowed=[original["terminal"]],
        )
        if repeated["terminal"] != original["terminal"]:
            raise AssertionError("transport cleanup changed command identity")
        stopped = call(b, "fault/transport/stop", "fault_observe_stop", sid=sid)
        if stopped["watchers"] or stopped["subscriptions"] or not stopped["listener"]:
            raise AssertionError("observer resources retained")
        call(a, "fault/transport/release", "fault_release", sid=sid)

    def slow_reader() -> dict[str, Any]:
        sid = roots["slow-reader"]
        other = ids[-1]
        call(a, "fault/slow/create", "fault_create", sid=sid)
        call(b, "fault/slow/claim", "fault_claim", sid=sid)
        cursor = call(a, "fault/slow/cursor", "fault_journal", sid=sid)["cursor"]
        call(a, "fault/slow/resources-before", "fault_resources")
        rss_before = report["samples"][-1]["rss_mb"]
        slow = call(a, "fault/slow/start", "fault_slow_start", sid=sid, after=cursor)
        if slow["capacity"] != 1:
            raise AssertionError("slow consumer is unbounded")
        call(b, "fault/slow/turn-one", "fault_prompt", sid=sid, command=_id(sid, "slow-one"), input="drain")
        call(b, "fault/slow/turn-two", "fault_prompt", sid=sid, command=_id(sid, "slow-two"), input="drain")
        call(b, "fault/slow/other-claim", "fault_claim", sid=other)
        call(b, "fault/slow/other-progress", "fault_prompt", sid=other, command=_id(other, "independent"))
        caught = call(a, "fault/slow/catch-up", "fault_slow_stop", sid=sid)
        if caught["events"] < 2 or caught["watchers"] or caught["subscriptions"]:
            raise AssertionError("slow consumer cleanup failed")
        call(b, "fault/slow/release", "fault_release", sid=sid)
        call(b, "fault/slow/other-release", "fault_release", sid=other)
        call(a, "fault/slow/resources-after", "fault_resources")
        rss_after = report["samples"][-1]["rss_mb"]
        return {
            "queue_capacity": slow["capacity"],
            "rss_before_mb": rss_before,
            "rss_after_mb": rss_after,
            "retained_rss_delta_mb": rss_after - rss_before,
        }

    def database_outage() -> None:
        sid = roots["database-outage"]
        committed, uncertain, stale = (_id(sid, name) for name in ("committed", "uncertain", "stale"))
        call(a, "fault/db/create", "fault_create", sid=sid)
        call(a, "fault/db/claim", "fault_claim", sid=sid)
        call(a, "fault/db/committed", "fault_prompt", sid=sid, command=committed, input="drain")
        call(
            a,
            "fault/db/committed-evidence",
            "fault_verify_command",
            sid=sid,
            command=committed,
            allowed=["turn_completed"],
        )
        paused = False
        try:
            database.command("pause", "db")
            paused = True
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = executor.submit(
                    a.call,
                    "fault_send",
                    sid=sid,
                    command=uncertain,
                    input="drain",
                    expect_error=True,
                )
                time.sleep(float(settings.get("coordinator", {}).get("lease_ttl", 10.0)) + 0.25)
                database.command("unpause", "db")
                paused = False
                database.command("restart", "db")
                database.command("up", "-d", "--wait", "--wait-timeout", "90")
                reply = pending.result(timeout=120)
            reply["label"] = "fault/db/outage-command"
            reply["pid"] = a.pid
            report["samples"].append(reply)
            if reply.get("error"):
                raise RuntimeError(reply["error"])
            call(b, "fault/db/takeover", "fault_claim", sid=sid)
            denied = call(a, "fault/db/stale-denied", "fault_old_writer", sid=sid, command=stale)
            if not denied["denied"]:
                raise AssertionError("stale database-outage writer was allowed")
            retry = call(
                b,
                "fault/db/retry",
                "fault_retry",
                sid=sid,
                command=uncertain,
                input="drain",
                allowed=["completed", "interrupted"],
            )
            terminal = call(
                b,
                "fault/db/uncertain-evidence",
                "fault_verify_command",
                sid=sid,
                command=uncertain,
                allowed=["turn_completed", "turn_interrupted"],
            )
            expected = {"completed": "turn_completed", "interrupted": "turn_interrupted"}[retry["outcome"]]
            if terminal["terminal"] != expected:
                raise AssertionError("unknown-commit retry disagrees with durable terminal")
            call(
                b,
                "fault/db/committed-stable",
                "fault_verify_command",
                sid=sid,
                command=committed,
                allowed=["turn_completed"],
            )
            journal = call(b, "fault/db/journal", "fault_journal", sid=sid)
            if journal["events"] < 1:
                raise AssertionError("database recovery lost committed work")
            fixture = call(b, "fault/db/fixture", "fault_fixture_total")
            if fixture["total"] != 60:
                raise AssertionError("database fixture changed across outage")
            call(a, "fault/db/release-old", "fault_release", sid=sid)
            call(b, "fault/db/release", "fault_release", sid=sid)
        finally:
            if paused:
                try:
                    database.command("unpause", "db")
                except Exception:
                    database.command("restart", "db")

    scenarios = {
        "writer-reconnect": writer_reconnect,
        "owner-sigkill": owner_sigkill,
        "cancel-boundaries": cancel_boundaries,
        "lost-notifications": lost_notifications,
        "slow-reader": slow_reader,
        "database-outage": database_outage,
    }
    for name, run in scenarios.items():
        if name in selected:
            _scenario(report, name, run)
    if getattr(args, "inject_oracle_failure", False):
        _scenario(report, "oracle-injection", lambda: (_ for _ in ()).throw(AssertionError("injected oracle failure")))
    failures = [item for item in report.get("scenarios", []) if item["status"] == "failed"]
    if failures:
        raise RuntimeError("fault scenarios failed: " + ", ".join(item["name"] for item in failures))
