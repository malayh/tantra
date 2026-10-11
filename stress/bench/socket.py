from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import psycopg
from psycopg import sql

from stress.bench.__main__ import ARTIFACTS, ROOT, Database, percentiles, write_report
from stress.bench.worker import COORDINATOR_SETTINGS, INPUT, seed
from tantra import PostgresStore
from tantra.events import InputQueued, TextPart, TurnCompleted


class Process:
    def __init__(self, command: list[str]) -> None:
        self.process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def receive(self, timeout: float = 240) -> dict[str, Any]:
        assert self.process.stdout is not None
        ready, _, _ = select.select([self.process.stdout], [], [], timeout)
        if not ready:
            raise TimeoutError(f"process {self.process.pid} did not reply within {timeout} seconds")
        line = self.process.stdout.readline()
        if not line:
            error = self.process.stderr.read() if self.process.stderr is not None else ""
            raise RuntimeError(f"process {self.process.pid} exited {self.process.poll()}: {error.strip()}")
        return json.loads(line)

    def send(self, value: str) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write(f"{value}\n")
        self.process.stdin.flush()

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        if self.process.stdin is not None:
            self.process.stdin.close()
        if self.process.stdout is not None:
            self.process.stdout.close()
        if self.process.stderr is not None:
            self.process.stderr.close()


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def parser() -> argparse.ArgumentParser:
    made = argparse.ArgumentParser(description="Owned PostgreSQL real-WebSocket Tantra scale benchmark")
    made.add_argument("--observers", type=int, default=1_000)
    made.add_argument("--active", type=int, default=100)
    made.add_argument("--slow-readers", type=int, default=10)
    made.add_argument("--slow-delay-ms", type=int, default=10)
    made.add_argument("--idle-seconds", type=float, default=35)
    made.add_argument("--timeout-seconds", type=int, default=240)
    made.add_argument("--inject-oracle-failure", action="store_true")
    made.add_argument("--output", type=Path)
    return made


def validate_args(args: argparse.Namespace) -> None:
    if not 1 <= args.active <= args.observers:
        raise ValueError("require 1 <= active <= observers")
    if not 0 <= args.slow_readers <= args.active:
        raise ValueError("require 0 <= slow-readers <= active")
    if args.slow_delay_ms < 0 or args.idle_seconds < 0 or args.timeout_seconds < 1:
        raise ValueError("delays must be non-negative and timeout must be positive")


def request(base: str, path: str, method: str = "GET") -> dict[str, Any]:
    response = httpx.request(method, f"{base}{path}", timeout=30)
    response.raise_for_status()
    return response.json()


def sample(label: str, metrics: dict[str, Any], elapsed_ms: float) -> dict[str, Any]:
    return {
        "label": label,
        "error": None,
        "elapsed_ms": elapsed_ms,
        "cpu_ms": metrics["cpu_ms"],
        "sql_calls": metrics["sql_calls"],
        "fetched_rows": metrics["fetched_rows"],
        "rss_mb": metrics["rss_mb"],
        "peak_rss_mb": metrics["peak_rss_mb"],
        "loop_lag_ms": metrics["loop_lag_ms"],
    }


def verify_worker(metrics: dict[str, Any], *, require_control: bool = False) -> None:
    assert metrics["queue"]["max_frames"] <= 256
    assert metrics["queue"]["max_bytes"] <= 4 * 1024 * 1024
    assert metrics["pool"]["pool_size"] <= 4
    assert metrics["coordinator"]["dispatch_peak"] <= 4
    assert metrics["tool_connections"]["peak"] <= 4
    assert metrics["listen_connection"]
    if require_control:
        assert metrics["control_connection"]
    assert metrics["auth_sql_connections"] == 0


def verify_idle(metrics: dict[str, Any]) -> None:
    assert metrics["journal_queries"] == 0
    assert metrics["journal_rows"] == 0
    assert metrics["event_body_queries"] == 0
    assert metrics["event_body_rows"] == 0
    assert metrics["result_queries"] == 0
    verify_worker(metrics)


def verify_cleanup(metrics: dict[str, Any]) -> None:
    assert all(value == 0 for value in metrics["resources"].values()), metrics["resources"]
    assert metrics["coordinator"]["subscriptions"] == 0


async def journal_oracle(
    dsn: str, schema: str, commands: dict[str, str], inject_failure: bool = False
) -> dict[str, Any]:
    expected = dict(commands)
    if inject_failure:
        expected[next(iter(expected))] = str(uuid4())
    store = PostgresStore(dsn, schema=schema)
    verified = 0
    try:
        await store.setup()
        for root_id, command_id in expected.items():
            command = command_id.replace("-", "")
            events = [item.event async for item in store.read(root_id.replace("-", ""))]
            queued = [event for event in events if isinstance(event, InputQueued) and event.command_id == command]
            terminals = [event for event in events if isinstance(event, TurnCompleted) and event.turn_id == command]
            assert len(queued) == 1, f"expected one input for {root_id}/{command}"
            assert queued[0].submitted_by == "socket-bench"
            assert len(terminals) == 1, f"expected one completed turn for {root_id}/{command}"
            assert any(isinstance(event, TextPart) and event.text.strip() == "60" for event in events)
            verified += 1
    finally:
        await store.close()
    return {"verified": verified, "submitted_by": "socket-bench", "output": "60"}


def wait_cleanup(urls: list[str], timeout: float = 30) -> list[dict[str, Any]]:
    deadline = time.monotonic() + timeout
    while True:
        metrics = [request(url, "/control/metrics") for url in urls]
        if all(all(value == 0 for value in item["resources"].values()) for item in metrics):
            return metrics
        if time.monotonic() >= deadline:
            return metrics
        time.sleep(0.05)


def run_campaign(report: dict[str, Any], args: argparse.Namespace, dsn: str) -> None:
    schema = f"socket_bench_{uuid4().hex[:12]}"
    ports = [free_port(), free_port()]
    workers: list[Process] = []
    client: Process | None = None
    config_path: Path | None = None
    urls = [f"http://127.0.0.1:{port}" for port in ports]
    try:
        ids = asyncio.run(seed(dsn, schema, args.observers, [], "bench/synthetic"))
        root_ids = [str(UUID(hex=value)) for value in ids]
        for index, port in enumerate(ports):
            worker = Process([sys.executable, "-m", "stress.bench.socket_worker", dsn, schema, str(port), str(index)])
            ready = worker.receive()
            assert ready["ready"] and ready["port"] == port
            workers.append(worker)
        result = subprocess.run(
            ["npm", "run", "build", "--silent"],
            cwd=ROOT / "packages/tantra-client",
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode:
            raise RuntimeError(f"client build failed: {result.stderr.strip()}")
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as config:
            json.dump(
                {
                    "urls": [url.replace("http://", "ws://") for url in urls],
                    "rootIds": root_ids,
                    "active": args.active,
                    "slowReaders": args.slow_readers,
                    "slowDelayMs": args.slow_delay_ms,
                    "timeoutMs": args.timeout_seconds * 1_000,
                    "input": INPUT,
                },
                config,
            )
            config_path = Path(config.name)
        client = Process(["node", str(ROOT / "stress/bench/socket_client.mjs"), str(config_path)])
        ready = client.receive(args.timeout_seconds)
        assert ready["phase"] == "ready" and ready["observers"] == args.observers
        assert not ready["errors"], ready["errors"]
        for url in urls:
            request(url, "/control/reset", "POST")
        time.sleep(args.idle_seconds)
        idle = [request(url, "/control/metrics") for url in urls]
        for metrics in idle:
            verify_idle(metrics)
        for url in urls:
            request(url, "/control/reset", "POST")
        started = time.perf_counter()
        client.send("run")
        active = client.receive(args.timeout_seconds)
        active_elapsed = (time.perf_counter() - started) * 1_000
        assert active["phase"] == "active" and active["active"] == args.active
        assert active["receipts"] == args.active
        assert active["terminals"] == args.active
        assert active["slowProgress"]
        assert not active["errors"], active["errors"]
        assert active["metrics"]["maxIncomingFrames"] <= 256
        assert active["metrics"]["maxIncomingBytes"] <= 4 * 1024 * 1024
        assert active["metrics"]["maxBufferedAmount"] <= 4 * 1024 * 1024
        active_metrics = [request(url, "/control/metrics") for url in urls]
        for index, metrics in enumerate(active_metrics):
            writer_count = sum(1 for root_index in range(args.active) if 1 - root_index % 2 == index)
            verify_worker(metrics, require_control=writer_count > 0)
            if writer_count:
                assert 0 < metrics["auth"]["revalidate"] <= metrics["auth"]["bind"] * 5
            report["samples"].append(sample(f"socket-worker-{index}/active", metrics, active_elapsed))
        oracle = asyncio.run(journal_oracle(dsn, schema, active["commands"], args.inject_oracle_failure))
        client.send("close")
        closed = client.receive(args.timeout_seconds)
        assert closed["phase"] == "closed" and not closed["errors"], closed["errors"]
        cleanup = wait_cleanup(urls)
        for metrics in cleanup:
            verify_cleanup(metrics)
        report.update(
            {
                "complete": True,
                "workload": {
                    "mode": "socket",
                    "observers": args.observers,
                    "active": args.active,
                    "slow_readers": args.slow_readers,
                    "slow_delay_ms": args.slow_delay_ms,
                    "workers": 2,
                    "model": "bench/synthetic",
                    "runtime_data_connections_per_worker": 4,
                },
                "client": active,
                "idle_workers": idle,
                "active_workers": active_metrics,
                "cleanup_workers": cleanup,
                "oracle": oracle,
                "latency_ms": percentiles(active["deliveryLatencyMs"]),
                "scenarios": [
                    {
                        "name": "real_websocket_scale",
                        "status": "passed",
                        "classification": "transport",
                        "evidence": {
                            "observers": args.observers,
                            "active": args.active,
                            "slow_progress": active["slowProgress"],
                            "ordered": not active["errors"],
                        },
                    }
                ],
            }
        )
    finally:
        if client is not None:
            if client.process.poll() is None:
                try:
                    client.send("close")
                    client.receive(30)
                except Exception:
                    pass
            client.stop()
        for worker in reversed(workers):
            worker.stop()
        if config_path is not None:
            config_path.unlink(missing_ok=True)
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    directory = args.output or ARTIFACTS / datetime.now(UTC).strftime("socket-%Y%m%dT%H%M%S-%f")
    report: dict[str, Any] = {
        "mode": "socket",
        "samples": [],
        "created_at": datetime.now(UTC).isoformat(),
        "host": platform.node(),
        "python": platform.python_version(),
        "coordinator": COORDINATOR_SETTINGS,
    }
    code = 0
    try:
        validate_args(args)
        if shutil.which("node") is None or shutil.which("npm") is None:
            raise RuntimeError("Node.js and npm are required")
        report["runtime_sha256"] = hashlib.sha256(
            b"".join(path.read_bytes() for path in sorted((ROOT / "packages/tantra/src").rglob("*.py")))
        ).hexdigest()
        report["client_sha256"] = hashlib.sha256(
            b"".join(path.read_bytes() for path in sorted((ROOT / "packages/tantra-client/src").rglob("*.ts")))
        ).hexdigest()
        report["bench_sha256"] = hashlib.sha256(
            b"".join(
                path.read_bytes()
                for path in (
                    Path(__file__),
                    Path(__file__).with_name("socket_worker.py"),
                    Path(__file__).with_name("socket_client.mjs"),
                )
            )
        ).hexdigest()
        with Database() as database:
            report["database"] = database.metadata
            run_campaign(report, args, database.dsn)
    except BaseException as exc:
        report["complete"] = False
        report["error"] = f"{type(exc).__name__}: {exc}"
        report.setdefault("scenarios", []).append(
            {
                "name": "real_websocket_scale",
                "status": "failed",
                "classification": "oracle" if args.inject_oracle_failure else "transport",
                "error": report["error"],
            }
        )
        code = 1
    write_report(report, directory)
    print(f"report: {directory / 'report.json'}")
    if report.get("error"):
        print(report["error"], file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
