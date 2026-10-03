from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from stress.bench.__main__ import Database, Worker, baseline, compare, write_report
from stress.bench.providers import (
    Budget,
    BudgetExceeded,
    MeteredStream,
    MeteredTransport,
    RecordedProvider,
    request_key,
)
from stress.bench.worker import seed
from stress.conftest import drop_schema
from stress.invariants import check_log, check_pairs
from tantra import ModelLimits, OpenAICompatible, ProviderError, Sample, SampleRequest, SessionHeader
from tantra.events import Stamped, TextPart
from tantra.providers.base import AssistantMessage, ToolCall, UserMessage
from tantra.providers.fake import FakeProvider


def test_budget_is_atomic_persistent_and_counts_cached_input(tmp_path: Path) -> None:
    path = tmp_path / "campaign.sqlite3"
    budget = Budget(path, "campaign")

    def reserve(_: int) -> bool:
        try:
            Budget(path, "campaign").reserve(1_000_000)
            return True
        except BudgetExceeded:
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        accepted = list(executor.map(reserve, range(8)))
    assert sum(accepted) == 5
    assert Budget(path, "campaign").summary()["unknown_reserved_tokens"] == 5_000_000
    with budget.connect() as conn:
        request_id = conn.execute("SELECT id FROM requests LIMIT 1").fetchone()[0]
    budget.settle(
        request_id,
        {"prompt_tokens": 40, "completion_tokens": 10, "prompt_tokens_details": {"cached_tokens": 30}, "cost": 0.001},
    )
    summary = Budget(path, "campaign").summary()
    assert summary["reported_tokens"] == 50 and summary["cached_input_tokens"] == 30
    assert summary["unknown_reserved_tokens"] == 4_000_000 and summary["remaining_tokens"] == 999_950


def test_provider_overrun_persistently_blocks_campaign(tmp_path: Path) -> None:
    path = tmp_path / "campaign.sqlite3"
    budget = Budget(path, "campaign")
    request = budget.reserve(100)
    with pytest.raises(ValueError, match="campaign blocked"):
        budget.settle(request, {"prompt_tokens": 100, "completion_tokens": 1})
    with pytest.raises(BudgetExceeded):
        Budget(path, "campaign").reserve(1)


async def test_fragmented_sse_usage_and_unknown_reservations(tmp_path: Path) -> None:
    budget = Budget(tmp_path / "campaign.sqlite3", "campaign")
    request = budget.reserve(1_000)
    raw = b'data: {"id":"generation-a","usage":{"prompt_tokens":40,"completion_tokens":10}}\n\ndata: [DONE]\n\n'

    class Chunks(httpx.AsyncByteStream):
        async def __aiter__(self):
            for chunk in (raw[:7], raw[7:54], raw[54:]):
                yield chunk

    stream = MeteredStream(Chunks(), budget, request)
    assert b"".join([chunk async for chunk in stream]) == raw
    assert budget.summary()["reported_tokens"] == 50
    budget.reserve(1_000)
    assert Budget(budget.path, "campaign").summary()["unknown_reserved_tokens"] == 1_000


async def test_interim_usage_does_not_release_inflight_budget(tmp_path: Path) -> None:
    budget = Budget(tmp_path / "campaign.sqlite3", "campaign")
    request = budget.reserve(1_000)

    class Chunks(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"usage":{"prompt_tokens":20,"completion_tokens":0}}\n\n'
            yield b'data: {"usage":{"prompt_tokens":20,"completion_tokens":10}}\n\ndata: [DONE]\n\n'

    iterator = MeteredStream(Chunks(), budget, request).__aiter__()
    await anext(iterator)
    assert budget.summary()["unknown_reserved_tokens"] == 1_000
    await anext(iterator)
    assert budget.summary()["reported_tokens"] == 30
    await iterator.aclose()


async def test_live_transport_accounts_raw_usage_and_bypasses_existing_recording(tmp_path: Path) -> None:
    budget = Budget(tmp_path / "campaign.sqlite3", "campaign")
    transport = MeteredTransport(budget, 10_000)
    calls = []

    async def respond(request):
        calls.append(request)
        assert request.headers["X-OpenRouter-Cache"] == "false"
        common = {"id": "generation", "object": "chat.completion.chunk", "created": 1, "model": "test"}
        frames = [
            {
                **common,
                "choices": [{"index": 0, "delta": {"role": "assistant", "content": "60"}, "finish_reason": None}],
            },
            {**common, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            {
                **common,
                "choices": [],
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 2,
                    "total_tokens": 22,
                    "prompt_tokens_details": {"cached_tokens": 15},
                    "cost": 0.001,
                },
            },
        ]
        content = "".join(f"data: {json.dumps(frame)}\n\n" for frame in frames) + "data: [DONE]\n\n"
        return httpx.Response(
            200, headers={"Content-Type": "text/event-stream"}, stream=httpx.ByteStream(content.encode())
        )

    await transport.inner.aclose()
    transport.inner = httpx.MockTransport(respond)
    client = httpx.AsyncClient(transport=transport, headers={"X-OpenRouter-Cache": "false"})
    source = OpenAICompatible(base_url="https://test.invalid/api/v1", api_key="test-key", http_client=client)
    provider = RecordedProvider(
        source, tmp_path / "recordings", "test", ModelLimits(context_window=10_000, max_output=4096), budget=budget
    )
    req = SampleRequest(model="test", messages=[UserMessage(content="total")])
    try:
        for _ in range(2):
            result = [event async for event in provider.stream(req)]
            assert result[-1].text == "60"
        summary = budget.summary()
        assert len(calls) == 2 and summary["requests"] == 2
        assert summary["reported_tokens"] == 44 and summary["cached_input_tokens"] == 30
        assert summary["unknown_reserved_tokens"] == 0 and summary["reported_cost"] == 0.002
        recording = json.loads(next((tmp_path / "recordings").glob("*.json")).read_text())
        assert len(recording["usage_requests"]) == 1
        assert len(list((tmp_path / "recordings" / "archive").glob("*.json"))) == 2
        assert all(row["generation"] == "generation" and row["tokens"] == 22 for row in recording["usage_requests"])
    finally:
        await provider.aclose()


async def test_each_http_attempt_reserves_before_dispatch_and_failure_keeps_reservation(tmp_path: Path) -> None:
    budget = Budget(tmp_path / "campaign.sqlite3", "campaign")
    transport = MeteredTransport(budget, 5_000_000)
    calls = []

    async def fail(request):
        calls.append(request)
        raise httpx.ConnectError("injected lost connection", request=request)

    await transport.inner.aclose()
    transport.inner = httpx.MockTransport(fail)
    request = httpx.Request("POST", "https://test.invalid/api/v1/chat/completions")
    try:
        with pytest.raises(httpx.ConnectError):
            await transport.handle_async_request(request)
        with pytest.raises(BudgetExceeded):
            await transport.handle_async_request(request)
        assert len(calls) == 1
        assert Budget(budget.path, "campaign").summary()["unknown_reserved_tokens"] == 5_000_000
    finally:
        await transport.aclose()


async def test_exact_completed_recording_and_fail_closed_miss(tmp_path: Path) -> None:
    limits = ModelLimits(context_window=32_000, max_output=4_096)
    req = SampleRequest(model="test", messages=[UserMessage(content="hello")])
    source = FakeProvider([Sample(text="60")])
    live = RecordedProvider(source, tmp_path, "test-endpoint", limits)
    expected = [event.model_dump() async for event in live.stream(req)]
    replay = RecordedProvider(None, tmp_path, "test-endpoint", limits)
    actual = [event.model_dump() async for event in replay.stream(req)]
    assert actual == expected
    changed = req.model_copy(update={"model": "different"})
    assert request_key(req, "test-endpoint") != request_key(changed, "test-endpoint")
    with pytest.raises(ProviderError, match="recording miss"):
        _ = [event async for event in replay.stream(changed)]
    path = next(tmp_path.glob("*.json"))
    recording = json.loads(path.read_text())
    recording["events"].pop()
    path.write_text(json.dumps(recording))
    with pytest.raises(ProviderError, match="completed stream"):
        _ = [event async for event in replay.stream(req)]


async def test_oracles_reject_injected_gap_and_unanswered_tool() -> None:
    async def read(sid):
        yield Stamped(seq=2, event=TextPart(sample_id="sample", text="wrong"))

    async def header(sid):
        return SessionHeader(id=sid, agent="test", last_seq=2)

    with pytest.raises(AssertionError, match="not contiguous"):
        await check_log(SimpleNamespace(read=read, header=header), "session")
    malformed = SampleRequest(
        model="test",
        messages=[
            AssistantMessage(tool_calls=[ToolCall(id="unanswered", name="fixture_total", args="{}")]),
            UserMessage(content="next turn"),
        ],
    )
    with pytest.raises(AssertionError, match="orphans"):
        check_pairs([malformed])


def test_missing_docker_fails_and_html_escapes_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("stress.bench.__main__.shutil.which", lambda _: None)
    with pytest.raises(RuntimeError, match="refusing to skip"):
        with Database():
            pass
    report = {"mode": "baseline", "samples": [], "error": "<script>alert('bad')</script>"}
    write_report(report, tmp_path)
    document = (tmp_path / "report.html").read_text()
    assert "<script>" not in document and "&lt;script&gt;" in document and "FAILED" in document


def test_compare_rejects_incompatible_workload(tmp_path: Path) -> None:
    left, right = tmp_path / "before.json", tmp_path / "after.json"
    left.write_text(json.dumps({"workload": {"sessions": 10}}))
    right.write_text(json.dumps({"workload": {"sessions": 11}}))
    with pytest.raises(ValueError, match="different workload"):
        compare(left, right)


@pytest.mark.parametrize("history_mode", ["full", "compacted"])
async def test_two_process_baseline_and_recovery(postgres_dsn: str, history_mode: str) -> None:
    schema = f"bench_{uuid4().hex[:8]}"
    settings = {
        "mode": "baseline",
        "model": "bench/synthetic",
        "dsn": postgres_dsn,
        "schema": schema,
        "history_mode": history_mode,
    }
    workers = []
    try:
        ids = await seed(postgres_dsn, schema, 4, [40], settings["model"], compacted=history_mode == "compacted")
        workers.extend([Worker(settings), Worker(settings)])
        report = {"samples": []}
        await asyncio.to_thread(baseline, report, workers, settings, ids, SimpleNamespace(histories=[40], samples=1))
        assert any(sample["label"] == "40/recovery" and not sample["error"] for sample in report["samples"])
        assert len({sample["pid"] for sample in report["samples"]}) == 2
        assert all(sample["pool"]["pool_max"] == 4 for sample in report["samples"])
        assert all(
            {"observation_checks", "observation_ticks", "routed_wakeups", "dispatch_peak"}
            <= sample["coordinator_delta"].keys()
            for sample in report["samples"]
        )
        if history_mode == "compacted":
            assert (
                next(sample for sample in report["samples"] if sample["label"] == "40/context")["result"]["events"] < 40
            )
    finally:
        for worker in workers:
            await asyncio.to_thread(worker.stop)
        drop_schema(postgres_dsn, schema)
