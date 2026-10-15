from __future__ import annotations

import argparse
import shutil
from types import SimpleNamespace
from uuid import uuid4

import pytest

from stress.bench import acp as acp_bench
from tantra.events import InputQueued, TextPart, TurnCompleted


def arguments(**overrides: object) -> argparse.Namespace:
    values = {
        "observers": 6,
        "active": 4,
        "slow_readers": 2,
        "slow_delay_ms": 2,
        "idle_seconds": 0.05,
        "timeout_seconds": 120,
        "inject_oracle_failure": False,
        "output": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.mark.parametrize(
    "values",
    [
        {"active": 0},
        {"active": 7},
        {"slow_readers": 5},
        {"slow_delay_ms": -1},
        {"idle_seconds": -1},
        {"timeout_seconds": 0},
    ],
)
def test_validate_args_rejects_invalid_scale(values: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        acp_bench.validate_args(arguments(**values))


async def test_journal_oracle_and_injected_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    root = uuid4()
    command = uuid4()
    events = [
        InputQueued(command_id=command.hex, input="input", submitted_by="acp-bench"),
        TextPart(sample_id=uuid4().hex, text="60"),
        TurnCompleted(turn_id=command.hex, stop_reason="stop"),
    ]

    class Store:
        async def setup(self) -> None:
            return None

        async def read(self, _root: str):
            for event in events:
                yield SimpleNamespace(event=event)

        async def close(self) -> None:
            return None

    monkeypatch.setattr(acp_bench, "PostgresStore", lambda *_args, **_kwargs: Store())
    result = await acp_bench.journal_oracle("dsn", "schema", {str(root): str(command)})
    assert result == {"verified": 1, "submitted_by": "acp-bench", "output": "60"}
    with pytest.raises(AssertionError, match="expected one input"):
        await acp_bench.journal_oracle("dsn", "schema", {str(root): str(command)}, True)


def test_main_returns_nonzero_and_writes_failed_report(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured = {}

    class Database:
        dsn = "dsn"
        metadata = {"fsync": "on", "synchronous_commit": "on"}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    def fail(*_args, **_kwargs):
        raise AssertionError("oracle rejected fixture")

    def write(report, _directory):
        captured.update(report)

    monkeypatch.setattr(acp_bench, "Database", Database)
    monkeypatch.setattr(acp_bench, "run_campaign", fail)
    monkeypatch.setattr(acp_bench, "write_report", write)
    code = acp_bench.main(["--output", str(tmp_path)])
    assert code == 1
    assert captured["complete"] is False
    assert "oracle rejected fixture" in captured["error"]


def test_small_real_acp_campaign(postgres_dsn: str) -> None:
    if shutil.which("node") is None or shutil.which("npm") is None:
        pytest.fail("Node.js and npm are required for the ACP WebSocket benchmark")
    report = {"mode": "acp", "samples": []}
    acp_bench.run_campaign(report, arguments(), postgres_dsn)
    assert report["complete"] is True
    assert report["oracle"]["verified"] == 4
    assert len(report["samples"]) == 2
    assert all(not worker["resources"]["watchers"] for worker in report["cleanup_workers"])
    assert all(not worker["resources"]["live_sockets"] for worker in report["cleanup_workers"])
