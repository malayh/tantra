from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from stress.bench.__main__ import main, parser
from stress.bench.cpu import cases, inject_failure
from stress.bench.cpu_workload import CPUProvider, _metrics, fixture_payload, normalized_digest, split_payload
from stress.bench.worker import Metrics
from tantra.providers.base import SampleRequest


def test_cpu_payload_is_fixed_and_fragments_preserve_identity() -> None:
    outputs = [fixture_payload(count) for count in (64, 256, 1_024)]
    assert outputs[0] == outputs[1] == outputs[2] and len(outputs[0]) == 16_384
    for count in (64, 256, 1_024, 8_192):
        payload = fixture_payload(count)
        parts = split_payload(payload, count)
        assert len(parts) == len(set(parts)) == count
        assert "".join(parts) == payload
    with pytest.raises(ValueError):
        fixture_payload(0)


@pytest.mark.parametrize("kind", ["text", "reasoning", "tool", "progress"])
async def test_cpu_provider_preserves_exact_typed_fragments(kind: str) -> None:
    provider = CPUProvider()
    provider.configure({"kind": kind, "fragments": 64})
    streams = [item async for item in provider.stream(SampleRequest(model="bench/cpu"))]
    if kind in ("tool", "progress"):
        streams.extend([item async for item in provider.stream(SampleRequest(model="bench/cpu"))])
    assert streams[-1].type == "stream_end"
    emitted = [row["event"] for row in provider.emissions]
    expected = [item.model_dump(mode="json") for item in provider.expected]
    if kind == "progress":
        expected = [item for item in expected if item["type"] != "tool_progress"]
    assert emitted == expected
    assert all(row["emitted_at_ms"] > 0 for row in provider.emissions)


def test_cpu_digest_only_normalizes_generated_sample_ids() -> None:
    original = [
        {"seq": 1, "event": {"type": "sample_started", "sample_id": "a", "turn_id": "turn"}},
        {"seq": 2, "event": {"type": "tool_call_requested", "sample_id": "a", "args": {"sample_id": "literal"}}},
    ]
    changed = json.loads(json.dumps(original))
    changed[0]["event"]["sample_id"] = changed[1]["event"]["sample_id"] = "b"
    assert normalized_digest(original) == normalized_digest(changed)
    changed[1]["event"]["args"]["sample_id"] = "different literal"
    assert normalized_digest(original) != normalized_digest(changed)
    assert original[0]["event"]["sample_id"] == "a"


def test_cpu_metric_snapshot_preserves_counter_values() -> None:
    snapshot = _metrics(Metrics(sql_calls=10, notifications=Counter({"journal": 7, "writer": 2})))
    assert snapshot["notifications"] == {"journal": 7, "writer": 2}
    assert json.loads(json.dumps(snapshot))["sql_calls"] == 10


@pytest.mark.parametrize("kind", ["order", "duplicate", "drop", "premature"])
def test_cpu_oracles_fail_reports_and_exit_nonzero(kind: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    actual = [{"type": "text_delta", "text": "first"}, {"type": "text_delta", "text": "second"}]

    def fail(*_args):
        inject_failure(kind, actual, actual)

    monkeypatch.setattr("stress.bench.cpu.cpu_campaign", fail)
    output = tmp_path / kind
    assert main(["cpu", "--cpu-failure", kind, "--output", str(output)]) == 1
    report = json.loads((output / "report.json").read_text())
    assert report["error"].startswith("AssertionError:")
    assert "FAILED" in (output / "report.html").read_text()


def test_cpu_defaults_cover_required_matrix_without_changing_baseline_defaults() -> None:
    available = cases()
    assert sum(case["primary"] for case in available) == 9
    assert {case["kind"] for case in available} == {"text", "reasoning", "tool", "progress"}
    assert {case["readers"] for case in available} == {0, 1, 8}
    assert {case["history"] for case in available} == {0, 4_000, 100_000}
    assert any(case["children"] == 100 for case in available)
    assert any(case["slow_s"] > 0 for case in available)
    assert parser().parse_args(["cpu"]).samples == 5
    assert parser().parse_args(["baseline"]).sessions == 10_000


def test_cpu_comparison_rejects_changed_evidence_and_instrumentation() -> None:
    from copy import deepcopy

    from stress.bench.cpu import compare_cpu

    sample = {
        "label": "case",
        "trial": "0",
        "database": {"replay_digest": "same"},
        **dict.fromkeys(
            ("owner_cpu_ms", "remote_cpu_ms", "postgres_cpu_ms", "wal_bytes", "top_sql_calls", "nested_sql_calls"), 10
        ),
    }
    before = {"mode": "cpu", "bench_sha256": "bench", "cpu_instrumentation": {}, "samples": [sample]}
    after = deepcopy(before)
    after["samples"][0]["owner_cpu_ms"] = 5
    assert compare_cpu(before, after)[0]["owner_cpu_ms"]["percent"] == -50
    after["samples"][0]["database"]["replay_digest"] = "wrong"
    with pytest.raises(ValueError, match="replay digest"):
        compare_cpu(before, after)
    after = deepcopy(before)
    after["bench_sha256"] = "other"
    with pytest.raises(ValueError, match="bench_sha256"):
        compare_cpu(before, after)


@pytest.mark.parametrize("kind", ["text", "reasoning", "tool", "progress"])
def test_cpu_two_workers_read_committed_ordered_events(kind: str, postgres_dsn: str) -> None:
    import asyncio
    import sys
    from uuid import uuid4

    import psycopg

    from stress.bench.__main__ import Worker
    from stress.bench.cpu import call, database_audit
    from stress.bench.worker import seed
    from stress.conftest import drop_schema

    schema = f"cpu_{uuid4().hex[:12]}"
    settings = {"mode": "cpu", "suite": "smoke", "model": "bench/cpu", "dsn": postgres_dsn, "schema": schema}
    workers = []
    config = {"kind": kind, "fragments": 64, "readers": 1, "history": 40, "history_mode": "compacted"}
    try:
        asyncio.run(seed(postgres_dsn, schema, 1, [], "bench/cpu"))
        workers = [Worker(settings), Worker(settings)]
        owner, remote = workers
        sid = uuid4().hex
        prepared = call(owner, "cpu_prepare", sid=sid, config=config)
        call(remote, "cpu_observers", sid=sid, after=prepared["after"], config=config)
        for worker in workers:
            call(worker, "cpu_begin")
        call(owner, "cpu_start", sid=sid)
        call(owner, "cpu_gate", open=True)
        result = call(owner, "cpu_wait", sid=sid)
        readers = call(remote, "cpu_reader_wait", sid=sid)["readers"]
        windows = [call(worker, "cpu_end") for worker in workers]
        with psycopg.connect(postgres_dsn, autocommit=True) as conn:
            audit = database_audit(conn, schema, sid, prepared["after"], result)
        assert readers[0]["replay_digest"] == audit["replay_digest"]
        assert readers[0]["delta_digest"] == audit["delta_digest"]
        assert len(readers[0]["received_at_ms"]) == audit["delta_count"]
        assert len(readers[0]["probes"]) == 3
        assert windows[0]["cpu_ms"] > 0 and windows[1]["cpu_ms"] > 0
        assert windows[0]["turn_boundary_queries"] > 0
        assert all(call(worker, "cpu_release", sid=sid)["subscriptions"] == 0 for worker in workers)
    finally:
        for worker in workers:
            worker.stop(force=sys.exc_info()[0] is not None)
        drop_schema(postgres_dsn, schema)


async def test_live_commit_probe_uses_the_injected_oracle(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    import psycopg

    from stress.bench.cpu_workload import CPUState, assert_committed

    class Snapshot:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, *_args):
            return self

        async def fetchone(self):
            return (3,)

    async def connect(*_args, **_kwargs):
        return Snapshot()

    checked = []

    def check(observed, committed):
        checked.append((observed, committed))
        assert_committed(observed, committed)

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect)
    monkeypatch.setattr("stress.bench.cpu_workload.assert_committed", check)
    runtime = SimpleNamespace(store=SimpleNamespace(dsn="fixture", _sql=lambda value: value))
    state = CPUState(runtime, SimpleNamespace(source=CPUProvider()))
    with pytest.raises(AssertionError, match="before independent database commit"):
        await state._probe("session", 0, 5)
    assert checked == [(5, 3)]


def test_cpu_identity_includes_compose_configuration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from stress.bench.cpu import bench_identity

    directory = tmp_path / "stress/bench"
    directory.mkdir(parents=True)
    (directory / "cpu.py").write_text("code")
    (directory / "compose.yaml").write_text("durable")
    (directory / "cpu.compose.yaml").write_text("instrumented")
    monkeypatch.setattr("stress.bench.cpu.ROOT", tmp_path)
    before = bench_identity()
    (directory / "cpu.compose.yaml").write_text("different config")
    assert bench_identity() != before
