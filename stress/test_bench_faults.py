from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
import pytest
from psycopg import sql

from stress.bench.__main__ import Worker, measured
from stress.bench.faults import FAULT_AGENTS, FaultState, _scenario, fault_campaign
from stress.bench.worker import BenchAgent, GatedProvider, seed
from tantra import PostgresCoordinator, PostgresStore, Runtime


async def _runtime(dsn: str, schema: str, lease_ttl: float = 0.4) -> tuple[Runtime, GatedProvider, FaultState]:
    store = PostgresStore(dsn, schema=schema)
    provider = GatedProvider({"mode": "scale", "model": "bench/synthetic"})
    slots = asyncio.Semaphore(4)
    runtime = Runtime(
        provider,
        store,
        [BenchAgent, *FAULT_AGENTS],
        default_model="bench/synthetic",
        deps_factory=lambda _: (dsn, schema, slots),
        coordinator=PostgresCoordinator(store, lease_ttl=lease_ttl, request_timeout=2, catch_up_interval=0.03),
    )
    await runtime.start()
    return runtime, provider, FaultState(runtime, provider)


@pytest.fixture
async def fault_database(postgres_dsn: str) -> AsyncIterator[tuple[str, str, list[str]]]:
    schema = f"fault_{uuid5(NAMESPACE_URL, str(asyncio.get_running_loop().time())).hex[:12]}"
    ids = await seed(postgres_dsn, schema, 2, [], "bench/synthetic")
    try:
        yield postgres_dsn, schema, ids
    finally:
        async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


async def _close(items: list[tuple[Runtime, GatedProvider, FaultState]]) -> None:
    for runtime, provider, state in items:
        await state.close()
        await runtime.aclose()
        await provider.aclose()
        await runtime.store.close()


async def test_fault_writer_replacement_reconnects_from_contiguous_cursor(
    fault_database: tuple[str, str, list[str]],
) -> None:
    dsn, schema, ids = fault_database
    first = await _runtime(dsn, schema)
    second = await _runtime(dsn, schema)
    a, b = first[2], second[2]
    sid = ids[0]
    command = uuid5(UUID(hex=sid), "fault-writer").hex
    try:
        await a.operation({"op": "fault_claim", "sid": sid})
        await a.operation({"op": "fault_gate", "open": False})
        await a.operation({"op": "fault_send", "sid": sid, "command": command})
        await a.operation({"op": "fault_wait_started", "sid": sid, "command": command})
        before = await a.operation({"op": "fault_journal", "sid": sid})
        assert before is not None
        await b.operation({"op": "fault_claim", "sid": sid})
        denied = await a.operation({"op": "fault_old_writer", "sid": sid, "command": uuid5(UUID(hex=sid), "stale").hex})
        assert denied == {"denied": True}
        await a.operation({"op": "fault_gate", "open": True})
        completed = await b.operation({"op": "fault_prompt", "sid": sid, "command": command})
        assert completed is not None and completed["outcome"] == "completed"
        replay = await b.operation(
            {
                "op": "fault_reconnect",
                "sid": sid,
                "after": before["cursor"],
                "prefix_sha256": before["sha256"],
            }
        )
        assert replay is not None
        assert replay["events"] > 0
        assert replay["first"] == before["cursor"] + 1
        assert replay["digest_parity"]
        with pytest.raises(AssertionError, match="prefix changed"):
            await b.operation(
                {
                    "op": "fault_reconnect",
                    "sid": sid,
                    "after": before["cursor"],
                    "prefix_sha256": "0" * 64,
                }
            )
    finally:
        await _close([first, second])


async def test_fault_cancellation_rolls_back_then_deduplicates_committed_targets(
    fault_database: tuple[str, str, list[str]],
) -> None:
    dsn, schema, ids = fault_database
    first = await _runtime(dsn, schema)
    second = await _runtime(dsn, schema)
    a, b = first[2], second[2]
    sid = ids[1]
    running, cancel, later = (uuid5(UUID(hex=sid), name).hex for name in ("running", "cancel", "later"))
    try:
        await a.operation({"op": "fault_claim", "sid": sid})
        await a.operation({"op": "fault_gate", "open": False})
        await a.operation({"op": "fault_send", "sid": sid, "command": running})
        await a.operation({"op": "fault_wait_started", "sid": sid, "command": running})
        await b.operation({"op": "fault_claim", "sid": sid})
        await a.operation({"op": "fault_cancel_precommit_on"})
        rejected = await b.operation({"op": "fault_cancel", "sid": sid, "command": cancel, "expect_error": True})
        assert rejected is not None and not rejected["accepted"]
        assert "owner could not apply" in rejected["error"]
        await a.operation({"op": "fault_cancel_precommit_off"})
        rollback = await b.operation(
            {
                "op": "fault_verify_cancel",
                "sid": sid,
                "cancel": cancel,
                "first": running,
                "count": 0,
                "cancelled": 0,
            }
        )
        assert rollback is not None and all(rollback.values())
        await b.operation({"op": "fault_cancel_lost_reply_on"})
        lost = await b.operation({"op": "fault_cancel", "sid": sid, "command": cancel, "expect_error": True})
        assert lost is not None and not lost["accepted"]
        restored = await b.operation({"op": "fault_cancel_lost_reply_off"})
        assert restored is not None and restored["reply"]["result"]["duplicate"] is False
        committed = await b.operation({"op": "fault_verify_cancel", "sid": sid, "cancel": cancel, "first": running})
        assert committed is not None and all(committed.values())
        await b.operation({"op": "fault_send", "sid": sid, "command": later})
        duplicate = await b.operation({"op": "fault_cancel", "sid": sid, "command": cancel})
        assert duplicate is not None and duplicate["duplicate"]
        evidence = await b.operation(
            {"op": "fault_verify_cancel", "sid": sid, "cancel": cancel, "first": running, "later": later}
        )
        assert evidence is not None and all(evidence.values())
        await a.operation({"op": "fault_gate", "open": True})
        completed = await b.operation({"op": "fault_prompt", "sid": sid, "command": later})
        assert completed is not None and completed["outcome"] == "completed"
    finally:
        await _close([first, second])


async def test_fault_owner_loss_expires_ask_reconciles_child_once_and_drains_queue(
    fault_database: tuple[str, str, list[str]],
) -> None:
    dsn, schema, ids = fault_database
    first = await _runtime(dsn, schema, lease_ttl=0.2)
    second = await _runtime(dsn, schema, lease_ttl=0.2)
    a, b = first[2], second[2]
    sid = uuid5(UUID(hex=ids[0]), "owner-loss").hex
    running, queued, child_call = (uuid5(UUID(hex=sid), name).hex for name in ("running", "queued", "child"))
    try:
        await a.operation({"op": "fault_create", "sid": sid})
        await b.operation({"op": "fault_setup"})
        await a.operation({"op": "fault_claim", "sid": sid})
        prepared = await a.operation(
            {
                "op": "fault_prepare_owner_death",
                "sid": sid,
                "running": running,
                "queued": queued,
                "child_call": child_call,
            }
        )
        assert prepared is not None
        abandoned = list(first[0].active.values())
        await first[0].coordinator.close()
        await asyncio.sleep(0.25)
        await asyncio.gather(*abandoned, return_exceptions=True)
        await b.operation({"op": "fault_claim", "sid": sid})
        evidence = await b.operation(
            {
                "op": "fault_verify_recovery",
                "sid": sid,
                "running": running,
                "queued": queued,
                "child": prepared["child"],
            }
        )
        assert evidence is not None and all(evidence.values())
    finally:
        await _close([first, second])


@pytest.mark.parametrize("scenario", ["writer-reconnect", "lost-notifications", "slow-reader"])
async def test_fault_campaign_runs_selected_scenario_across_processes(
    fault_database: tuple[str, str, list[str]], scenario: str
) -> None:
    dsn, schema, ids = fault_database
    settings = {
        "mode": "scale",
        "model": "bench/synthetic",
        "history_mode": "full",
        "dsn": dsn,
        "schema": schema,
    }
    report: dict[str, Any] = {"samples": []}
    workers = [Worker(settings), Worker(settings)]
    try:
        fault_campaign(
            report,
            workers,
            settings,
            ids,
            SimpleNamespace(scenario=[scenario], inject_oracle_failure=False),
            object(),
            measured,
            Worker,
        )
    finally:
        for worker in workers:
            worker.stop(force=True)
    assert report["scenarios"][0]["name"] == scenario
    assert report["scenarios"][0]["status"] == "passed"


def test_fault_oracle_failure_is_reported_as_failed() -> None:
    report: dict[str, Any] = {}

    def fail() -> None:
        raise AssertionError("deliberate")

    _scenario(report, "oracle-injection", fail)
    assert report["scenarios"] == [
        {
            "name": "oracle-injection",
            "status": "failed",
            "classification": "oracle",
            "elapsed_ms": report["scenarios"][0]["elapsed_ms"],
            "error": "AssertionError: deliberate",
        }
    ]
