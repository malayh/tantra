from __future__ import annotations

import asyncio
import json
import multiprocessing
from copy import deepcopy
from pathlib import Path

import httpx
import pytest

from stress.bench.providers import RECORDING_ATTEMPTS, Budget, MeteredTransport, RecordedProvider, request_key
from tantra import ModelLimits, ProviderError, Sample, SampleRequest
from tantra.providers.base import UserMessage
from tantra.providers.fake import FakeProvider


def test_idle_resource_oracle_rejects_extra_reads_and_capacity() -> None:
    from stress.bench.__main__ import verify_idle_resources

    sample = {
        "error": None,
        "journal_queries": 0,
        "journal_rows": 0,
        "result_queries": 0,
        "coordinator_delta": {"observation_ticks": 15, "observation_checks": 15, "routed_wakeups": 0},
        "pool": {"pool_size": 4},
        "coordinator": {"dispatch_peak": 4},
    }
    verify_idle_resources(sample)
    for path, value in (
        (("journal_queries",), 1),
        (("journal_rows",), 1),
        (("result_queries",), 1),
        (("coordinator_delta", "observation_ticks"), 0),
        (("coordinator_delta", "observation_checks"), 16),
        (("coordinator_delta", "routed_wakeups"), 1),
        (("pool", "pool_size"), 5),
        (("coordinator", "dispatch_peak"), 5),
    ):
        changed = deepcopy(sample)
        target = changed if len(path) == 1 else changed[path[0]]
        target[path[-1]] = value
        with pytest.raises(AssertionError):
            verify_idle_resources(changed)


def test_reconcile_requires_complete_matching_native_usage(tmp_path: Path) -> None:
    budget = Budget(tmp_path / "ledger.sqlite3", "campaign")
    ids = [budget.reserve(1000) for _ in range(4)]
    for index, identity in enumerate(ids):
        budget.generation(identity, f"gen-{index}")
    data = [
        {
            "id": "gen-0",
            "finish_reason": "stop",
            "native_tokens_prompt": 30,
            "native_tokens_completion": 12,
            "native_tokens_cached": 10,
            "native_tokens_reasoning": 8,
            "total_cost": 0.001,
        },
        {"id": "gen-1", "finish_reason": None, "native_tokens_prompt": 30, "native_tokens_completion": 12},
        {"id": "wrong", "finish_reason": "stop", "native_tokens_prompt": 30, "native_tokens_completion": 12},
        {"id": "gen-3", "finish_reason": "stop", "tokens_prompt": 30, "tokens_completion": 12},
    ]

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": data[int(request.url.params["id"].split("-")[-1])]})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        results = budget.reconcile(client, "https://test.invalid", limit=3)
    assert [r["status"] for r in results] == ["settled", "retained", "retained"]
    summary = budget.summary()
    assert summary["reported_tokens"] == 42
    assert summary["cached_input_tokens"] == 10 and summary["reasoning_tokens"] == 8
    assert summary["unknown_reserved_tokens"] == 3000
    assert not summary["cost_available"]


def test_campaign_cost_is_available_only_with_complete_numeric_costs(tmp_path: Path) -> None:
    budget = Budget(tmp_path / "ledger.sqlite3", "campaign")
    known = budget.reserve(1000)
    budget.settle(known, {"prompt_tokens": 12, "completion_tokens": 3, "cost": 0.001})
    assert budget.summary()["cost_available"]
    unknown = budget.reserve(1000)
    assert not budget.summary()["cost_available"]
    budget.settle(unknown, {"prompt_tokens": 12, "completion_tokens": 3, "cost": None})
    assert not budget.summary()["cost_available"]
    budget.settle(unknown, {"prompt_tokens": 12, "completion_tokens": 3, "cost": 0.001})
    assert budget.summary()["cost_available"]


async def test_inference_slots_are_held_until_stream_close_and_cancel_is_safe(tmp_path: Path) -> None:
    slots = multiprocessing.get_context("spawn").BoundedSemaphore(4)
    transport = MeteredTransport(Budget(tmp_path / "ledger.sqlite3", "campaign"), 1000, slots)
    entered = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal entered
        entered += 1
        return httpx.Response(200, stream=httpx.ByteStream(b"data: [DONE]\n\n"))

    await transport.inner.aclose()
    transport.inner = httpx.MockTransport(respond)
    request = httpx.Request("POST", "https://test.invalid/chat/completions")
    responses = [await transport.handle_async_request(request) for _ in range(4)]
    pending = asyncio.create_task(transport.handle_async_request(request))
    await asyncio.sleep(0.02)
    assert entered == 4 and not pending.done()
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await responses[0].aclose()
    replacement = await transport.handle_async_request(request)
    assert entered == 5
    for response in responses:
        await response.aclose()
    await replacement.aclose()
    for _ in range(4):
        assert slots.acquire(False)
    assert not slots.acquire(False)
    for _ in range(4):
        slots.release()
    await transport.aclose()


async def test_failed_reservation_and_response_cache_release_slots(tmp_path: Path) -> None:
    slots = multiprocessing.get_context("spawn").BoundedSemaphore(1)
    budget = Budget(tmp_path / "ledger.sqlite3", "campaign")
    transport = MeteredTransport(budget, 1000, slots)

    async def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"X-OpenRouter-Cache-Status": "HIT"}, stream=httpx.ByteStream(b""))

    await transport.inner.aclose()
    transport.inner = httpx.MockTransport(respond)
    with pytest.raises(ProviderError, match="response-cache hit"):
        await transport.handle_async_request(httpx.Request("POST", "https://test.invalid/chat/completions"))
    assert slots.acquire(False)
    slots.release()
    assert budget.summary()["unknown_reserved_tokens"] == 1000
    await transport.aclose()


async def test_recording_fixture_identity_and_provider_timing(tmp_path: Path) -> None:
    limits = ModelLimits(context_window=32000, max_output=4096)
    request = SampleRequest(model="test", messages=[UserMessage(content="total")])
    source = FakeProvider([Sample(text="60")])
    live = RecordedProvider(source, tmp_path, "test", limits, "fixture-a")
    expected = [e.model_dump() async for e in live.stream(request)]
    replay = RecordedProvider(None, tmp_path, "test", limits, "fixture-a")
    assert [e.model_dump() async for e in replay.stream(request)] == expected
    assert live.timings[0]["fresh"] and not replay.timings[0]["fresh"]
    assert live.timings[0]["provider_ms"] >= 0
    assert request_key(request, "test", "fixture-a") != request_key(request, "test", "fixture-b")
    changed = RecordedProvider(None, tmp_path, "test", limits, "fixture-b")
    with pytest.raises(ProviderError, match="recording miss"):
        _ = [e async for e in changed.stream(request)]
    path = next(tmp_path.glob("*.json"))
    recorded = json.loads(path.read_text())
    recorded["fixture_identity"] = "altered"
    path.write_text(json.dumps(recorded))
    with pytest.raises(ProviderError, match="fixture mismatch"):
        _ = [e async for e in replay.stream(request)]


async def test_paid_recording_requires_complete_authoritative_usage(tmp_path: Path) -> None:
    limits = ModelLimits(context_window=32000, max_output=4096)
    request = SampleRequest(model="test", messages=[UserMessage(content="total")])
    budget = Budget(tmp_path / "ledger.sqlite3", "campaign")
    key = request_key(request, "test", "fixture")

    class PaidFake:
        complete = False

        async def stream(self, req):
            identity = budget.reserve(1000, key)
            RECORDING_ATTEMPTS.get().append(identity)
            if self.complete:
                budget.generation(identity, "gen-1")
                budget.settle(identity, {"prompt_tokens": 12, "completion_tokens": 3})
            async for event in FakeProvider([Sample(text="60")]).stream(req):
                yield event

    source = PaidFake()
    provider = RecordedProvider(source, tmp_path, "test", limits, "fixture", budget)
    with pytest.raises(ProviderError, match="incomplete authoritative usage"):
        _ = [event async for event in provider.stream(request)]
    assert not list(tmp_path.glob("*.json"))
    assert budget.summary()["unknown_reserved_tokens"] == 1000
    source.complete = True
    expected = [event.model_dump() async for event in provider.stream(request)]
    assert budget.summary()["unknown_reserved_tokens"] == 1000
    replay = RecordedProvider(None, tmp_path, "test", limits, "fixture", require_usage=True)
    assert [event.model_dump() async for event in replay.stream(request)] == expected
    path = next(tmp_path.glob("*.json"))
    recording = json.loads(path.read_text())
    for rows in ([], [{**recording["usage_requests"][0], "tokens": None}]):
        recording["usage_requests"] = rows
        path.write_text(json.dumps(recording))
        with pytest.raises(ProviderError, match="authoritative usage"):
            _ = [event async for event in replay.stream(request)]


def _process_slots(pipe, slots, release, counter, path) -> None:
    async def run() -> None:
        transport = MeteredTransport(Budget(Path(path), "campaign"), 1000, slots)

        class CountedStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b"data: [DONE]\n\n"

            async def aclose(self):
                with counter.get_lock():
                    counter[0] -= 1

        async def respond(request):
            with counter.get_lock():
                counter[0] += 1
                counter[1] = max(counter[1], counter[0])
            pipe.send("entered")
            return httpx.Response(200, stream=CountedStream())

        await transport.inner.aclose()
        transport.inner = httpx.MockTransport(respond)

        async def attempt():
            response = await transport.handle_async_request(
                httpx.Request("POST", "https://test.invalid/chat/completions")
            )
            while not release.is_set():
                await asyncio.sleep(0.01)
            await response.aclose()

        await asyncio.gather(*(attempt() for _ in range(3)))
        await transport.aclose()
        pipe.send("done")

    asyncio.run(run())


def test_inference_limit_is_shared_across_worker_processes(tmp_path: Path) -> None:
    import time

    context = multiprocessing.get_context("spawn")
    slots = context.BoundedSemaphore(4)
    release = context.Event()
    counter = context.Array("i", [0, 0])
    children, parents = [], []
    try:
        for _ in range(2):
            parent, child = context.Pipe()
            process = context.Process(
                target=_process_slots, args=(child, slots, release, counter, str(tmp_path / "ledger.sqlite3"))
            )
            process.start()
            child.close()
            children.append(process)
            parents.append(parent)
        entered = 0
        deadline = time.monotonic() + 20
        while entered < 4 and time.monotonic() < deadline:
            for pipe in parents:
                if pipe.poll(0.01):
                    assert pipe.recv() == "entered"
                    entered += 1
        assert entered == 4 and counter[0] == 4
        assert not any(pipe.poll(0.05) for pipe in parents)
        release.set()
        for process in children:
            process.join(20)
            assert process.exitcode == 0
        assert counter[:] == [0, 4]
        assert Budget(tmp_path / "ledger.sqlite3", "campaign").summary()["requests"] == 6
    finally:
        release.set()
        for process in children:
            if process.is_alive():
                process.terminate()
                process.join(5)
        for pipe in parents:
            pipe.close()


def test_failed_scenario_fails_report_and_invalid_cli_does_not_run(tmp_path: Path) -> None:
    from stress.bench.__main__ import main, write_report

    report = {
        "mode": "live",
        "samples": [],
        "scenarios": [{"name": "sql_read", "status": "failed", "classification": "oracle", "error": "<wrong>"}],
    }
    write_report(report, tmp_path / "report")
    html = (tmp_path / "report" / "report.html").read_text()
    assert "FAILED" in html and "&lt;wrong&gt;" in html and "oracle" in html
    assert main(["baseline", "--faults", "--output", str(tmp_path / "invalid")]) == 1
    assert json.loads((tmp_path / "invalid" / "report.json").read_text())["error"].endswith(
        "--faults requires scale mode"
    )


def test_existing_campaign_ledger_keeps_reservations_on_upgrade(tmp_path: Path) -> None:
    import sqlite3

    path = tmp_path / "ledger.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE requests (id TEXT PRIMARY KEY, campaign TEXT NOT NULL, reserved INTEGER NOT NULL, "
            "tokens INTEGER, generation TEXT, usage TEXT, created REAL NOT NULL)"
        )
        connection.execute("INSERT INTO requests VALUES ('original', 'campaign', 1000, NULL, NULL, NULL, 1)")
    budget = Budget(path, "campaign")
    assert budget.summary()["unknown_reserved_tokens"] == 1000
    assert budget.summary()["remaining_tokens"] == 4_999_000
    request = budget.reserve(1000, "recording")
    assert budget.recording_requests("recording")[0]["id"] == request
    assert Budget(path, "campaign").summary()["unknown_reserved_tokens"] == 2000
