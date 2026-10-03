from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import multiprocessing
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4, uuid5

import httpx
import psycopg

from stress.bench.providers import OUTPUT_LIMIT, Budget
from stress.bench.worker import COORDINATOR_SETTINGS, seed, worker_main

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
COMPOSE = Path(__file__).with_name("compose.yaml")
ENDPOINT = "https://openrouter.ai/api/v1"
MODEL = "z-ai/glm-5.3-flash"


class Database(AbstractContextManager):
    def __init__(self) -> None:
        self.project = f"tantra-bench-{uuid4().hex[:12]}"
        self.dsn = ""
        self.metadata: dict[str, Any] = {}

    def command(self, *args: str) -> str:
        result = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE), "-p", self.project, *args],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode:
            raise RuntimeError(f"Docker Compose {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def __enter__(self) -> Database:
        if shutil.which("docker") is None:
            raise RuntimeError("Docker CLI is required; refusing to skip PostgreSQL verification")
        try:
            self.command("up", "-d", "--wait", "--wait-timeout", "90")
            endpoint = self.command("port", "db", "5432")
            port = int(endpoint.rsplit(":", 1)[1])
            self.dsn = f"postgresql://tantra_bench:tantra_bench@127.0.0.1:{port}/tantra_bench"
            with psycopg.connect(self.dsn, connect_timeout=5) as conn:
                row = conn.execute(
                    "SELECT version(), current_setting('fsync'), current_setting('synchronous_commit')"
                ).fetchone()
                if row[1:] != ("on", "on"):
                    raise RuntimeError("benchmark database requires fsync=on and synchronous_commit=on")
                self.metadata = {"version": row[0], "fsync": row[1], "synchronous_commit": row[2]}
            container = self.command("ps", "-q", "db")
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.Image}}", container],
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
            )
            self.metadata["image_id"] = result.stdout.strip()
            return self
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_args: Any) -> None:
        self.command("down", "-v", "--remove-orphans")


class Worker:
    def __init__(self, settings: dict[str, Any]) -> None:
        ctx = multiprocessing.get_context("spawn")
        self.pipe, child = ctx.Pipe()
        self.process = ctx.Process(target=worker_main, args=(child, settings))
        self.process.start()
        child.close()
        try:
            ready = self.receive()
            if not ready.get("ready"):
                raise RuntimeError(f"worker startup failed: {ready.get('error')}")
            self.pid = ready["pid"]
        except BaseException:
            self.stop(force=True)
            raise

    def receive(self) -> dict[str, Any]:
        if not self.pipe.poll(240):
            raise TimeoutError("worker did not reply within 240 seconds")
        try:
            return self.pipe.recv()
        except EOFError as exc:
            raise RuntimeError(f"worker exited with code {self.process.exitcode}") from exc

    def call(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        self.pipe.send({"op": operation, **kwargs})
        return self.receive()

    def stop(self, *, force: bool = False) -> None:
        timed_out = False
        if self.process.is_alive() and not force:
            try:
                self.pipe.send({"op": "close"})
                self.process.join(timeout=30)
            except (BrokenPipeError, EOFError):
                pass
        if self.process.is_alive():
            timed_out = not force
            self.process.terminate()
            self.process.join(timeout=10)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=10)
        self.pipe.close()
        if not force and (timed_out or self.process.exitcode):
            raise RuntimeError(f"worker failed graceful shutdown: exit={self.process.exitcode}, timeout={timed_out}")


def percentiles(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "p50": statistics.median(ordered),
        "p95": ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)],
        "max": ordered[-1],
    }


def summarize(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for sample in samples:
        groups.setdefault(sample["label"], []).append(sample)
    summaries = []
    for label, entries in groups.items():
        row = {"label": label, "samples": len(entries), "errors": sum(bool(item.get("error")) for item in entries)}
        for field in ("elapsed_ms", "cpu_ms", "sql_calls", "fetched_rows", "rss_mb", "peak_rss_mb"):
            row[field] = percentiles([entry[field] for entry in entries])
        row["loop_lag_ms"] = percentiles([value for entry in entries for value in entry["loop_lag_ms"]] or [0])
        summaries.append(row)
    return summaries


def write_report(report: dict[str, Any], directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    report["summary"] = summarize(report.get("samples", []))
    encoded = json.dumps(report, indent=2, default=str)
    (directory / "report.json").write_text(encoded)
    rows = "".join(
        f"<tr><td>{html.escape(row['label'])}</td><td>{row['samples']}</td><td>{row['errors']}</td>"
        f"<td>{row['elapsed_ms']['p50']:.2f}</td><td>{row['elapsed_ms']['p95']:.2f}</td>"
        f"<td>{row['sql_calls']['p50']:.0f}</td><td>{row['fetched_rows']['p50']:.0f}</td>"
        f"<td>{row['cpu_ms']['p50']:.2f}</td><td>{row['rss_mb']['max']:.1f}</td></tr>"
        for row in report["summary"]
    )
    changes = "".join(
        f"<tr><td>{html.escape(row['label'])}</td><td>{row['before_p95_ms']:.2f}</td>"
        f"<td>{row['after_p95_ms']:.2f}</td><td>{row['p95_change_ms']:+.2f}</td></tr>"
        for row in report.get("changes", [])
    )
    comparison = (
        (
            "<h2>Comparison</h2><div class='scroll'><table><thead><tr><th>Operation</th>"
            "<th>Before P95 ms</th><th>After P95 ms</th><th>Change ms</th></tr></thead>"
            f"<tbody>{changes}</tbody></table></div>"
        )
        if changes
        else ""
    )
    status = "FAILED" if report.get("error") or any(row["errors"] for row in report["summary"]) else "PASSED"
    document = (
        "<!doctype html><html lang='en'><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
        "<title>Tantra benchmark</title><style>body{font:16px system-ui;background:#111827;color:#e5e7eb;"
        "margin:32px}table{border-collapse:collapse;width:100%}td,th{padding:12px;text-align:left;"
        "border-bottom:1px solid #374151}pre{white-space:pre-wrap;overflow-wrap:anywhere}"
        ".scroll{overflow-x:auto}h1{font-size:26px}summary{cursor:pointer}</style>"
        f"<h1>Tantra bench — {html.escape(report['mode'])} — {status}</h1>"
        f"<p>{html.escape(report.get('error') or 'Runtime and database evidence recorded.')}</p>"
        "<p>SQL counts include background work during each measurement. Peak RSS is process lifetime. "
        "Fixture setup and verification are reported separately. Replay is not fresh inference.</p>"
        "<div class='scroll'><table><thead><tr><th>Operation</th><th>N</th><th>Errors</th>"
        "<th>Median ms</th><th>P95 ms</th><th>Median SQL</th><th>Median fetched rows</th>"
        f"<th>Median CPU ms</th><th>Max RSS MB</th></tr></thead><tbody>{rows}</tbody></table></div>"
        f"{comparison}<details><summary>Raw evidence and environment</summary>"
        f"<pre>{html.escape(encoded)}</pre></details></html>"
    )
    (directory / "report.html").write_text(document)


def measured(report: dict[str, Any], worker: Worker, label: str, operation: str, **kwargs: Any) -> dict[str, Any]:
    reply = worker.call(operation, **kwargs)
    reply["label"] = label
    reply["pid"] = worker.pid
    report["samples"].append(reply)
    if reply.get("error"):
        raise RuntimeError(f"{label}: {reply['error']}")
    print(f"{label}: {reply['elapsed_ms']:.1f} ms, SQL {reply['sql_calls']}, rows {reply['fetched_rows']}", flush=True)
    return reply["result"]


def baseline(
    report: dict[str, Any], workers: list[Worker], settings: dict[str, Any], ids: list[str], args: Any
) -> None:
    for index, history in enumerate(args.histories):
        sid = ids[index]
        a, b = workers
        prefix = str(history)
        for trial in range(args.samples):
            command = uuid5(UUID(hex=sid), f"baseline-{trial}").hex
            measured(report, a, f"{prefix}/claim", "claim", sid=sid)
            gate = measured(report, a, f"{prefix}/gate", "gate", open=False)
            measured(report, a, f"{prefix}/send", "send", sid=sid, command=command)
            measured(report, a, f"{prefix}/duplicate", "duplicate", sid=sid, command=command)
            measured(
                report,
                a,
                f"{prefix}/provider_ready",
                "wait_started",
                sid=sid,
                command=command,
                min_requests=gate["provider_requests"] + 1,
            )
            measured(report, b, f"{prefix}/replace_writer", "claim", sid=sid)
            measured(report, a, f"{prefix}/old_writer_denied", "replaced", sid=sid, command=command)
            measured(report, b, f"{prefix}/remote_duplicate", "duplicate", sid=sid, command=command)
            measured(report, a, f"{prefix}/gate", "gate", open=True)
            measured(report, b, f"{prefix}/complete", "complete", sid=sid, command=command)
            measured(report, b, f"{prefix}/verify", "verify", sid=sid, command=command, terminal="turn_completed")
            measured(report, a, f"{prefix}/context", "context", sid=sid)
            measured(report, b, f"{prefix}/playback", "playback", sid=sid)
            measured(report, a, f"{prefix}/release_replaced", "release", sid=sid)
            measured(report, b, f"{prefix}/release", "release", sid=sid)
        command = uuid5(UUID(hex=sid), "recovery").hex
        measured(report, a, f"{prefix}/claim_before_crash", "claim", sid=sid)
        gate = measured(report, a, f"{prefix}/gate", "gate", open=False)
        measured(report, a, f"{prefix}/send_before_crash", "send", sid=sid, command=command)
        measured(
            report,
            a,
            f"{prefix}/provider_ready",
            "wait_started",
            sid=sid,
            command=command,
            min_requests=gate["provider_requests"] + 1,
        )
        a.stop(force=True)
        time.sleep(COORDINATOR_SETTINGS["lease_ttl"] + 0.2)
        measured(report, b, f"{prefix}/recovery", "claim", sid=sid)
        measured(
            report, b, f"{prefix}/verify_recovery", "verify", sid=sid, command=command, terminal="turn_interrupted"
        )
        measured(report, b, f"{prefix}/release", "release", sid=sid)
        workers[0] = Worker(settings)


def scale(report: dict[str, Any], workers: list[Worker], ids: list[str], args: Any) -> None:
    roots = ids[len(args.histories) : len(args.histories) + args.observers]
    for index, worker in enumerate(workers):
        measured(report, worker, f"worker-{index}/subscribe", "observe", ids=roots[index::2])
        measured(report, worker, f"worker-{index}/idle", "idle", seconds=3)
    with ThreadPoolExecutor(max_workers=2) as executor:
        expected = {}
        futures = [
            executor.submit(worker.call, "drive", ids=roots[: args.active][1 - index :: 2])
            for index, worker in enumerate(workers)
        ]
        for index, future in enumerate(futures):
            reply = future.result()
            reply["label"] = f"worker-{index}/active"
            reply["pid"] = workers[index].pid
            report["samples"].append(reply)
            if reply.get("error"):
                raise RuntimeError(reply["error"])
            expected.update({turn["sid"]: turn["last_seq"] for turn in reply["result"]["turns"]})
    observed: dict[str, int] = {}
    for index, worker in enumerate(workers):
        result = measured(report, worker, f"worker-{index}/settle", "idle", seconds=3)
        assert not result["observer_errors"], result["observer_errors"]
        observed.update(result["observed"])
    assert set(roots[: args.active]).issubset(observed), "observer missed active root events"
    assert all(observed[sid] >= watermark for sid, watermark in expected.items()), "observer missed terminal events"
    for index, worker in enumerate(workers):
        stopped = measured(report, worker, f"worker-{index}/unsubscribe", "stop_observers")
        assert not stopped["observer_errors"], stopped["observer_errors"]
        assert stopped["watchers"] == stopped["subscriptions"] == 0, "observer resources were retained"
        measured(report, worker, f"worker-{index}/after_disconnect", "idle", seconds=3)
        sample = report["samples"][-1]
        assert sample["watchers"] == sample["coordinator"]["subscriptions"] == 0
        assert sample["coordinator_delta"]["observation_checks"] == 0


def provider_settings(args: Any) -> dict[str, Any]:
    settings = {
        "mode": args.mode,
        "model": args.model,
        "endpoint": ENDPOINT,
        "recordings": str(ARTIFACTS / "recordings"),
        "ledger": str(ARTIFACTS / "campaign.sqlite3"),
        "campaign": args.campaign,
    }
    if args.mode not in ("live", "replay"):
        settings["model"] = "bench/synthetic"
        return settings
    metadata_path = ARTIFACTS / "recordings" / f"model-{hashlib.sha256(args.model.encode()).hexdigest()}.json"
    if args.mode == "live":
        key = os.environ.get("OPENROUTER_API_KEY")
        if not key:
            raise RuntimeError("live requires OPENROUTER_API_KEY in the environment")
        with httpx.Client(timeout=30) as client:
            response = client.get(f"{ENDPOINT}/models", headers={"Authorization": f"Bearer {key}"})
            response.raise_for_status()
        model = next((item for item in response.json()["data"] if item["id"] == args.model), None)
        if model is None or type(model.get("context_length")) is not int or model["context_length"] < OUTPUT_LIMIT:
            raise RuntimeError("model has no verified context limit; refusing inference")
        settings["context_window"] = model["context_length"]
        settings["api_key"] = key
        budget = Budget(Path(settings["ledger"]), args.campaign)
        if budget.summary()["remaining_tokens"] < settings["context_window"] + OUTPUT_LIMIT:
            raise RuntimeError("campaign budget cannot cover one verified request reservation")
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps({"model": args.model, "context_window": settings["context_window"]}))
    else:
        if not metadata_path.is_file():
            raise RuntimeError("replay requires recorded model metadata; run live explicitly to record it")
        metadata = json.loads(metadata_path.read_text())
        if metadata["model"] != args.model:
            raise RuntimeError("recorded model identity differs")
        settings["context_window"] = metadata["context_window"]
    return settings


def compare(before: Path, after: Path) -> dict[str, Any]:
    left, right = json.loads(before.read_text()), json.loads(after.read_text())
    for key in ("workload", "host", "python", "coordinator"):
        if left.get(key) != right.get(key):
            raise ValueError(f"cannot compare different {key}")
    for key in ("version", "fsync", "synchronous_commit", "image_id"):
        if left.get("database", {}).get(key) != right.get("database", {}).get(key):
            raise ValueError(f"cannot compare different database {key}")
    if left.get("error") or right.get("error"):
        raise ValueError("cannot compare failed runs as a successful baseline")
    if not left.get("samples") or not right.get("samples"):
        raise ValueError("comparison requires measured samples")
    indexed = {row["label"]: row for row in summarize(left["samples"])}
    if set(indexed) != {row["label"] for row in summarize(right["samples"])}:
        raise ValueError("comparison has unmatched operations")
    changes = []
    for row in summarize(right["samples"]):
        previous = indexed.get(row["label"])
        if previous is None or row["errors"] or previous["errors"]:
            raise ValueError("comparison has unmatched or failed operations")
        changes.append(
            {
                "label": row["label"],
                "before_p95_ms": previous["elapsed_ms"]["p95"],
                "after_p95_ms": row["elapsed_ms"]["p95"],
                "p95_change_ms": row["elapsed_ms"]["p95"] - previous["elapsed_ms"]["p95"],
            }
        )
    return {"mode": "compare", "samples": [], "before": str(before), "after": str(after), "changes": changes}


def parser() -> argparse.ArgumentParser:
    made = argparse.ArgumentParser(description="Compose-backed Tantra Runtime bench; only live makes paid requests")
    made.add_argument("mode", choices=("baseline", "live", "replay", "scale", "compare"))
    made.add_argument("--sessions", type=int, default=10_000)
    made.add_argument("--histories", type=int, nargs="+", default=[4_000, 100_000])
    made.add_argument("--samples", type=int, default=5)
    made.add_argument("--history-mode", choices=("full", "compacted"), default="full")
    made.add_argument("--observers", type=int, default=1_000)
    made.add_argument("--active", type=int, default=100)
    made.add_argument("--campaign", default="journal-scaling")
    made.add_argument("--model", default=MODEL)
    made.add_argument("--output", type=Path)
    made.add_argument("--before", type=Path)
    made.add_argument("--after", type=Path)
    made.add_argument(
        "--stress", action="store_true", help="run existing stress tests against the owned Compose database"
    )
    return made


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    directory = args.output or ARTIFACTS / datetime.now(UTC).strftime("%Y%m%dT%H%M%S-%f")
    report: dict[str, Any] = {"mode": args.mode, "samples": [], "created_at": datetime.now(UTC).isoformat()}
    workers: list[Worker] = []
    try:
        if args.mode == "compare":
            if args.before is None or args.after is None:
                raise ValueError("compare requires --before and --after report.json paths")
            report = compare(args.before, args.after)
            for row in report["changes"]:
                print(
                    f"{row['label']}: P95 {row['before_p95_ms']:.2f} -> {row['after_p95_ms']:.2f} ms "
                    f"({row['p95_change_ms']:+.2f} ms)"
                )
            return 0
        if args.sessions < len(args.histories) or args.samples < 1 or min(args.histories) < 15:
            raise ValueError("require positive samples, history sizes >=15, and enough sessions")
        if args.mode == "scale" and not (1 <= args.active <= args.observers <= args.sessions - len(args.histories)):
            raise ValueError("scale requires 1 <= active <= observers <= sessions minus history roots")
        settings = provider_settings(args)
        settings["history_mode"] = args.history_mode
        live = args.mode in ("live", "replay")
        commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        report.update(
            {
                "commit": commit,
                "host": platform.node(),
                "python": platform.python_version(),
                "coordinator": COORDINATOR_SETTINGS,
                "runtime_sha256": hashlib.sha256(
                    b"".join(path.read_bytes() for path in sorted((ROOT / "packages/tantra/src").rglob("*.py")))
                ).hexdigest(),
                "workload": {
                    "mode": args.mode,
                    "sessions": 1 if live else args.sessions,
                    "histories": [] if live else args.histories,
                    "samples": 1 if live else args.samples,
                    "observers": args.observers if args.mode == "scale" else 0,
                    "active": args.active if args.mode == "scale" else 1,
                    "model": settings["model"],
                    "fixture_version": 2 if args.history_mode == "compacted" else 1,
                    "fixture_tool_connections": 4,
                },
            }
        )
        if args.history_mode == "compacted":
            report["workload"]["history_mode"] = "compacted"
        with Database() as database:
            report["database"] = database.metadata
            settings.update({"dsn": database.dsn, "schema": "bench"})
            started = time.perf_counter()
            live = args.mode in ("live", "replay")
            ids = asyncio.run(
                seed(
                    database.dsn,
                    "bench",
                    1 if live else args.sessions,
                    [] if live else args.histories,
                    settings["model"],
                    compacted=args.history_mode == "compacted",
                )
            )
            report["fixture_ms"] = (time.perf_counter() - started) * 1_000
            print(f"seeded {len(ids)} sessions in {report['fixture_ms']:.1f} ms", flush=True)
            try:
                for _ in range(2):
                    workers.append(Worker(settings))
                report["worker_pids"] = [worker.pid for worker in workers]
                if args.mode == "baseline":
                    baseline(report, workers, settings, ids, args)
                elif args.mode == "scale":
                    scale(report, workers, ids, args)
                else:
                    a, b = workers
                    sid, command = ids[0], uuid5(UUID(hex=ids[0]), "live-turn").hex
                    measured(report, a, "claim", "claim", sid=sid)
                    measured(report, a, "send", "send", sid=sid, command=command)
                    measured(report, b, "replace_writer", "claim", sid=sid)
                    measured(report, b, "complete", "complete", sid=sid, command=command)
                    measured(report, b, "verify", "verify", sid=sid, command=command, terminal="turn_completed")
                    measured(report, b, "playback", "playback", sid=sid)
                for index, worker in enumerate(workers):
                    measured(report, worker, f"worker-{index}/backlog", "backlog")
                if args.stress:
                    result = subprocess.run(
                        [sys.executable, "-m", "pytest", "stress", "-q", "-rs"],
                        cwd=ROOT,
                        env={**os.environ, "TANTRA_POSTGRES_DSN": database.dsn},
                        capture_output=True,
                        text=True,
                        timeout=1_200,
                    )
                    report["stress"] = {"exit_code": result.returncode, "output": result.stdout + result.stderr}
                    if result.returncode or "skipped" in result.stdout:
                        raise RuntimeError("required stress matrix failed or skipped; see report")
            finally:
                for worker in workers:
                    worker.stop()
                workers.clear()
        return 0
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr)
        return 1
    finally:
        for worker in workers:
            worker.stop(force=True)
        ledger = ARTIFACTS / "campaign.sqlite3"
        if ledger.exists():
            report["budget"] = Budget(ledger, args.campaign).summary()
        write_report(report, directory)
        print(f"report: {directory / 'report.html'}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
