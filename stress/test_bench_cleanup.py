from __future__ import annotations

from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from stress.bench import __main__ as bench_main
from stress.bench.__main__ import Worker, measured
from stress.bench.cleanup import CLEANUP_SCENARIOS
from stress.bench.faults import FAULT_SCENARIOS, LEGACY_FAULT_SCENARIOS, fault_campaign
from stress.bench.worker import seed


@pytest.fixture
async def cleanup_database(postgres_dsn: str) -> AsyncIterator[tuple[str, str, list[str]]]:
    schema = f"cleanup_bench_{uuid4().hex[:12]}"
    ids = await seed(postgres_dsn, schema, 6, [15, 25], "bench/synthetic")
    try:
        yield postgres_dsn, schema, ids
    finally:
        async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def _settings(dsn: str, schema: str) -> dict[str, Any]:
    return {
        "mode": "scale",
        "model": "bench/synthetic",
        "history_mode": "full",
        "suite": "smoke",
        "dsn": dsn,
        "schema": schema,
    }


def test_cleanup_scenarios_append_without_changing_legacy_order() -> None:
    assert FAULT_SCENARIOS == (*LEGACY_FAULT_SCENARIOS, *CLEANUP_SCENARIOS)
    assert LEGACY_FAULT_SCENARIOS == (
        "writer-reconnect",
        "owner-sigkill",
        "cancel-boundaries",
        "lost-notifications",
        "slow-reader",
        "database-outage",
    )


@pytest.mark.parametrize(
    ("selected", "expected"),
    (
        ([], list(FAULT_SCENARIOS)),
        (["cleanup-pagination", "writer-reconnect"], ["cleanup-pagination", "writer-reconnect"]),
    ),
)
def test_fault_workload_identity_records_effective_scenario_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    selected: list[str],
    expected: list[str],
) -> None:
    captured = {}

    class Database:
        dsn = "postgresql://fixture"
        metadata = {"fixture": True}

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

    class Process:
        pid = 1

        def __init__(self, _settings: dict[str, Any]) -> None:
            pass

        def call(self, operation: str, **_kwargs: Any) -> dict[str, Any]:
            return {
                "operation": operation,
                "result": {},
                "error": None,
                "elapsed_ms": 0,
                "sql_calls": 0,
                "fetched_rows": 0,
            }

        def stop(self, *, force: bool = False) -> None:
            return None

    async def seeded(*_args: Any, **_kwargs: Any) -> list[str]:
        return [uuid4().hex for _ in range(4)]

    monkeypatch.setattr(bench_main, "Database", Database)
    monkeypatch.setattr(bench_main, "Worker", Process)
    monkeypatch.setattr(bench_main, "seed", seeded)
    monkeypatch.setattr(bench_main, "write_report", lambda report, _directory: captured.update(report))
    monkeypatch.setattr("stress.bench.faults.fault_campaign", lambda *_args, **_kwargs: None)
    scenarios = [item for name in selected for item in ("--scenario", name)]
    result = bench_main.main(
        [
            "scale",
            "--faults",
            "--sessions",
            "4",
            "--histories",
            "15",
            "15",
            "--observers",
            "1",
            "--active",
            "1",
            "--output",
            str(tmp_path),
            *scenarios,
        ]
    )
    assert result == 0
    assert captured["workload"]["scenarios"] == expected


@pytest.mark.parametrize("scenario", CLEANUP_SCENARIOS)
async def test_cleanup_campaign_runs_named_scenario_across_processes(
    cleanup_database: tuple[str, str, list[str]], scenario: str
) -> None:
    dsn, schema, ids = cleanup_database
    settings = _settings(dsn, schema)
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
    result = report["scenarios"][0]
    assert result["name"] == scenario
    assert result["status"] == "passed"
    if scenario == "cleanup-long-history":
        assert result["evidence"]["preview"]["event_body_queries"] == 0
        assert result["evidence"]["preview"]["event_body_rows"] == 0
        assert result["evidence"]["delete"]["event_body_queries"] == 0
        assert result["evidence"]["delete"]["event_body_rows"] == 0
        plan = result["evidence"]["plan"]
        assert all(plan["query_features"].values())
        assert plan["candidate_roots"] == ids[:2]
        assert plan["candidate_count"] == 2
        assert plan["root_rows"] == plan["tree_rows"] == 2
        assert not any(node["relation"] == "events" for node in plan["nodes"])


async def test_cleanup_campaign_injected_oracle_failure_raises(
    cleanup_database: tuple[str, str, list[str]],
) -> None:
    dsn, schema, ids = cleanup_database
    settings = _settings(dsn, schema)
    report: dict[str, Any] = {"samples": []}
    workers = [Worker(settings), Worker(settings)]
    try:
        with pytest.raises(RuntimeError, match="oracle-injection"):
            fault_campaign(
                report,
                workers,
                settings,
                ids,
                SimpleNamespace(scenario=["cleanup-dry-run"], inject_oracle_failure=True),
                object(),
                measured,
                Worker,
            )
    finally:
        for worker in workers:
            worker.stop(force=True)
    assert report["scenarios"][-1]["status"] == "failed"
    assert report["scenarios"][-1]["classification"] == "oracle"
