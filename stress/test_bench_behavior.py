from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from stress.bench.__main__ import Worker, measured
from stress.bench.behavior import (
    AGENTS,
    SCENARIOS,
    BehaviorState,
    ModelBehaviorError,
    OracleError,
    behavior_fixture_identity,
    behavior_gated_fixture_stats,
    behavior_policy,
    behavior_session_id,
    run_behavior,
    seed_behavior,
)
from stress.bench.providers import RecordedProvider
from stress.bench.worker import RuntimeTimings, seed
from stress.conftest import drop_schema
from stress.driver import SyntheticProvider, last_user
from tantra import ModelLimits, PostgresStore, Runtime, Sample
from tantra.events import (
    InputQueued,
    LoggedEvent,
    SampleCompleted,
    SampleStarted,
    ToolCallCompleted,
    ToolCallRequested,
    ToolCallStarted,
    TurnCompleted,
)


def test_behavior_identity_is_stable_and_scenario_specific() -> None:
    assert behavior_fixture_identity("sql_read") == behavior_fixture_identity("sql_read")
    assert len({behavior_fixture_identity(scenario) for scenario in SCENARIOS}) == len(SCENARIOS)
    assert behavior_session_id("sql_read") == behavior_session_id("sql_read")
    assert behavior_session_id("sql_read") != behavior_session_id("skill")
    assert behavior_fixture_identity("denied_write", "unanswered") != behavior_fixture_identity("denied_write")


async def test_runtime_timings_use_committed_event_boundaries() -> None:
    moments = iter((1.0, 1.25, 2.0, 2.5))
    timings = RuntimeTimings(lambda: next(moments))
    actor = uuid4()
    events = (
        SampleStarted(turn_id="turn-1", sample_id="sample-1", model="bench/strict"),
        ToolCallRequested(sample_id="sample-1", call_id="call-1", name="fixture", args={}),
        ToolCallStarted(call_id="call-1"),
        ToolCallCompleted(call_id="call-1", result=60),
        SampleCompleted(sample_id="sample-1"),
        TurnCompleted(turn_id="turn-1", stop_reason="completed"),
    )
    for seq, event in enumerate(events, start=1):
        await timings.on_event(LoggedEvent(agent_id=actor, seq=seq, event=event))
    assert timings.completed == [
        {
            "kind": "tool",
            "boundary": "committed_tool_started_to_committed_tool_completed",
            "elapsed_ms": 250.0,
            "actor_id": str(actor),
            "turn_id": "turn-1",
            "tool": "fixture",
        },
        {
            "kind": "post_provider",
            "boundary": "committed_sample_completed_to_committed_turn_terminal",
            "elapsed_ms": 500.0,
            "actor_id": str(actor),
            "turn_id": "turn-1",
        },
    ]


async def _strict_child_run(
    postgres_dsn: str,
    schema: str,
    provider: RecordedProvider,
    policy: object | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    store = PostgresStore(postgres_dsn, schema=schema)
    runtime = Runtime(
        provider,
        store,
        [AGENTS["child_completion"]],
        default_model="bench/strict",
        deps_factory=lambda _: (postgres_dsn, schema, asyncio.Semaphore(4)),
    )
    state = BehaviorState(runtime, provider)
    await runtime.start()
    try:
        setup = await state.operation({"op": "behavior_setup", "scenario": "child_completion"})
        assert setup is not None
        if policy is not None:
            assert isinstance(provider.source, SyntheticProvider)
            provider.source.policy = policy
        await state.operation(
            {
                "op": "behavior_start",
                "name": "main",
                "sid": setup["sid"],
                "command": setup["command"],
                "input": "Delegate this fixture total check to one child.",
            }
        )
        await state.operation({"op": "behavior_wait", "name": "main", "gate": "parent_waiting"})
        await state.operation({"op": "behavior_wait", "name": "main", "gate": "child_finished"})
        final = await state.operation({"op": "behavior_wait", "name": "main", "gate": "parent_final"})
        assert final is not None
        events = await state._events(str(setup["sid"]))
        lifecycle = [
            event
            for event in events
            if isinstance(event, InputQueued) and event.input.startswith("[agent ") and " finished]" in event.input
        ]
        assert len(lifecycle) == 1
        assert final["turn_id"] == lifecycle[0].command_id
        assert any(isinstance(event, TurnCompleted) and event.turn_id == lifecycle[0].command_id for event in events)
        result = await state.operation({"op": "behavior_result", "name": "main"})
        assert result is not None and result["outcome"] == "completed"
        evidence = await state.operation(
            {
                "op": "behavior_evidence",
                "scenario": "child_completion",
                "sid": behavior_session_id("child_completion").hex,
            }
        )
        assert evidence is not None
        return [request.model_dump(mode="json") for request in provider.requests], evidence
    finally:
        await state.close()
        await runtime.aclose()
        await provider.aclose()
        await store.close()


async def test_child_completion_strict_request_replay(postgres_dsn: str, tmp_path: Path) -> None:
    schema = f"behavior_replay_{uuid4().hex[:8]}"
    model = "bench/strict"
    endpoint = "https://bench.invalid"
    limits = ModelLimits(context_window=32_000, max_output=4_096)
    identity = behavior_fixture_identity("child_completion")
    recordings = tmp_path / "recordings"
    source = SyntheticProvider(behavior_policy, limits=limits)
    source.state.tag = "strict"
    fresh = RecordedProvider(source, recordings, endpoint, limits, identity)
    try:
        await seed(postgres_dsn, schema, 1, [], model)
        await seed_behavior(postgres_dsn, schema)
        fresh_requests, fresh_evidence = await _strict_child_run(postgres_dsn, schema, fresh)
        drop_schema(postgres_dsn, schema)
        await seed(postgres_dsn, schema, 1, [], model)
        await seed_behavior(postgres_dsn, schema)
        replay = RecordedProvider(None, recordings, endpoint, limits, identity)
        replay_requests, replay_evidence = await _strict_child_run(postgres_dsn, schema, replay)
        assert replay_requests == fresh_requests
        assert replay_evidence == fresh_evidence
        assert replay_evidence["tool_calls"] == ["spawn"]
        assert replay_evidence["child_stats"] == {"count": 3, "total": 60}
        assert not (recordings / "misses").exists()
    finally:
        drop_schema(postgres_dsn, schema)


@pytest.mark.parametrize(("parent_text", "accepted"), [('"60"', True), ("61", False)])
async def test_child_parent_final_text(
    postgres_dsn: str,
    tmp_path: Path,
    parent_text: str,
    accepted: bool,
) -> None:
    schema = f"behavior_parent_text_{uuid4().hex[:8]}"
    model = "bench/strict"
    limits = ModelLimits(context_window=32_000, max_output=4_096)

    def policy(request: object, state: object) -> Sample:
        user = last_user(request)
        if user.startswith("[agent ") and " finished]" in user:
            return Sample(text=parent_text)
        return behavior_policy(request, state)

    source = SyntheticProvider(policy, limits=limits)
    source.state.tag = "parent-text"
    provider = RecordedProvider(
        source,
        tmp_path / "recordings",
        "https://bench.invalid",
        limits,
        behavior_fixture_identity("child_completion"),
    )
    try:
        await seed(postgres_dsn, schema, 1, [], model)
        await seed_behavior(postgres_dsn, schema)
        if accepted:
            _, evidence = await _strict_child_run(postgres_dsn, schema, provider, policy)
            assert evidence["parent_text"] == '"60"'
        else:
            with pytest.raises(ModelBehaviorError, match="child parent returned '61'"):
                await _strict_child_run(postgres_dsn, schema, provider, policy)
    finally:
        drop_schema(postgres_dsn, schema)


async def test_child_oracle_rejects_failed_stats_tool(
    postgres_dsn: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = f"behavior_child_error_{uuid4().hex[:8]}"
    model = "bench/strict"
    limits = ModelLimits(context_window=32_000, max_output=4_096)

    async def fail(ctx: object) -> None:
        raise RuntimeError("fixture read failed")

    monkeypatch.setattr(behavior_gated_fixture_stats, "fn", fail)
    source = SyntheticProvider(behavior_policy, limits=limits)
    source.state.tag = "failed"
    provider = RecordedProvider(
        source,
        tmp_path / "recordings",
        "https://bench.invalid",
        limits,
        behavior_fixture_identity("child_completion"),
    )
    try:
        await seed(postgres_dsn, schema, 1, [], model)
        await seed_behavior(postgres_dsn, schema)
        with pytest.raises(OracleError, match="did not read the fixture"):
            await _strict_child_run(postgres_dsn, schema, provider)
    finally:
        drop_schema(postgres_dsn, schema)


async def test_behavior_scenarios_and_oracles(postgres_dsn: str) -> None:
    schema = f"behavior_{uuid4().hex[:8]}"
    settings = {
        "mode": "baseline",
        "model": "bench/synthetic",
        "dsn": postgres_dsn,
        "schema": schema,
        "history_mode": "full",
        "suite": "behavioral",
    }
    workers: list[Worker] = []
    selected = list(SCENARIOS)
    report = {"samples": [], "scenarios": []}
    try:
        await seed(postgres_dsn, schema, 1, [], settings["model"])
        await seed_behavior(postgres_dsn, schema)
        workers.extend([Worker(settings), Worker(settings)])
        await asyncio.to_thread(
            run_behavior,
            report,
            workers,
            settings,
            SimpleNamespace(scenario=selected),
            measured,
            Worker,
        )
        assert [entry["name"] for entry in report["scenarios"]] == selected
        assert {entry["status"] for entry in report["scenarios"]} == {"passed"}, [
            entry for entry in report["scenarios"] if entry["status"] == "failed"
        ]
        assert report["scenarios"][selected.index("approved_write")]["evidence"]["effects"] == 1
        assert report["scenarios"][selected.index("denied_write")]["evidence"]["effects"] == 0
        assert report["scenarios"][selected.index("child_completion")]["evidence"]["child_result"] == "60"
        compacted = report["scenarios"][selected.index("compacted_recall")]["evidence"]
        assert compacted["initial"]["compactions"] >= 1
        assert compacted["initial"]["summary_requests"] >= 1
        assert compacted["initial"]["summary_has_marker"] is True
        async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                sql.SQL("UPDATE {}.behavior_audit SET value = 8 WHERE operation_key = %s").format(
                    sql.Identifier(schema)
                ),
                ("approved-write-v1",),
            )
        reply = await asyncio.to_thread(
            workers[0].call,
            "behavior_evidence",
            scenario="approved_write",
            sid=behavior_session_id("approved_write").hex,
        )
        assert "OracleError" in reply["error"]
        async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                sql.SQL("UPDATE {}.behavior_audit SET value = 7 WHERE operation_key = %s").format(
                    sql.Identifier(schema)
                ),
                ("approved-write-v1",),
            )
            await conn.execute(
                sql.SQL("DELETE FROM {}.behavior_audit WHERE operation_key = %s").format(sql.Identifier(schema)),
                ("approved-write-v1",),
            )
        reply = await asyncio.to_thread(
            workers[0].call,
            "behavior_evidence",
            scenario="approved_write",
            sid=behavior_session_id("approved_write").hex,
        )
        assert "OracleError" in reply["error"]
    finally:
        for worker in workers:
            await asyncio.to_thread(worker.stop)
        drop_schema(postgres_dsn, schema)
