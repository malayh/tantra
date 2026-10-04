from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg import sql

from stress.bench.cpu_workload import assert_committed, normalized_digest
from stress.bench.worker import COORDINATOR_SETTINGS, seed
from tantra.stores.postgres import _parse

ROOT = Path(__file__).resolve().parents[2]
DELTA_TYPES = {"text_delta", "reasoning_delta", "tool_call_delta", "tool_progress"}


def cases() -> list[dict[str, Any]]:
    defaults = {
        "kind": "text",
        "fragments": 64,
        "readers": 1,
        "pause_s": 0.0,
        "slow_s": 0.0,
        "history": 0,
        "history_mode": "full",
        "children": 0,
        "audit_all": False,
        "primary": False,
    }
    primary = [
        {"name": f"text-{count}-readers-{readers}", "fragments": count, "readers": readers, "primary": True}
        for count in (64, 256, 1_024)
        for readers in (0, 1, 8)
    ]
    extended = [
        {"name": "text-paced", "fragments": 256, "pause_s": 0.002},
        {"name": "text-8192", "fragments": 8_192},
        {"name": "reasoning-1024", "kind": "reasoning", "fragments": 1_024},
        {"name": "tool-1024", "kind": "tool", "fragments": 1_024},
        {"name": "progress-256", "kind": "progress", "fragments": 256},
        {"name": "slow-reader", "fragments": 1_024, "slow_s": 0.01},
        {"name": "wide-root", "fragments": 1_024, "children": 100},
        *[
            {"name": f"history-{size}-{mode}", "history": size, "history_mode": mode}
            for size in (4_000, 100_000)
            for mode in ("full", "compacted")
        ],
        {"name": "commit-audit", "audit_all": True},
    ]
    return [{**defaults, **case} for case in [*primary, *extended]]


def digest(values: Any) -> str:
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def assert_deltas(actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    if actual != expected:
        raise AssertionError("delta identity/order/count differs from deterministic provider fixture")


def inject_failure(kind: str, actual: list[dict[str, Any]], expected: list[dict[str, Any]]) -> None:
    mutated = list(actual)
    if kind == "premature":
        assert_committed(1, 0)
    elif kind == "order":
        mutated[0], mutated[1] = mutated[1], mutated[0]
    elif kind == "duplicate":
        mutated.insert(1, mutated[0])
    elif kind == "drop":
        del mutated[0]
    else:
        raise ValueError("unknown CPU oracle failure")
    assert_deltas(mutated, expected)


def bench_identity() -> str:
    paths = sorted((ROOT / "stress/bench").glob("*.py"))
    paths += [ROOT / "stress/bench/compose.yaml", ROOT / "stress/bench/cpu.compose.yaml"]
    return hashlib.sha256(b"".join(path.name.encode() + path.read_bytes() for path in paths)).hexdigest()


class PostgresMetrics:
    def __init__(self, database: Any) -> None:
        self.database = database
        self.conn = psycopg.connect(database.dsn, autocommit=True)
        self.conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        self.settings = dict(
            self.conn.execute(
                "SELECT name, setting FROM pg_settings WHERE name = ANY(%s) ORDER BY name",
                (
                    [
                        "max_connections",
                        "shared_buffers",
                        "fsync",
                        "synchronous_commit",
                        "full_page_writes",
                        "wal_compression",
                        "checkpoint_timeout",
                        "shared_preload_libraries",
                        "pg_stat_statements.track",
                        "pg_stat_statements.track_planning",
                    ],
                ),
            ).fetchall()
        )
        track, planning = self.conn.execute(
            "SELECT current_setting('pg_stat_statements.track'), current_setting('pg_stat_statements.track_planning')"
        ).fetchone()
        if (track, planning) != ("all", "off"):
            raise RuntimeError("CPU bench requires fixed nested-SQL instrumentation with planning disabled")

    def cpu_ms(self) -> float:
        raw = self.database.command("exec", "-T", "db", "cat", "/sys/fs/cgroup/cpu.stat")
        values = dict(line.split() for line in raw.splitlines())
        if "usage_usec" not in values:
            raise RuntimeError("CPU bench requires the database cgroup v2 CPU counter")
        return int(values["usage_usec"]) / 1_000

    def begin(self) -> tuple[float, str]:
        self.conn.execute("SELECT pg_stat_statements_reset()")
        lsn = str(self.conn.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0])
        return self.cpu_ms(), lsn

    def end(self, before: tuple[float, str]) -> dict[str, Any]:
        cpu = self.cpu_ms() - before[0]
        wal = int(
            self.conn.execute(
                "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), %s::pg_lsn)", (before[1],)
            ).fetchone()[0]
        )
        rows = self.conn.execute(
            "SELECT query, calls, total_exec_time, rows, wal_bytes::bigint, toplevel "
            "FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname=current_database()) "
            "ORDER BY calls DESC, query"
        ).fetchall()
        statements = [
            dict(zip(("query", "calls", "elapsed_ms", "rows", "wal_bytes", "toplevel"), row, strict=True))
            for row in rows
        ]
        return {
            "postgres_cpu_ms": cpu,
            "wal_bytes": wal,
            "attributed_wal_bytes": sum(row["wal_bytes"] for row in statements),
            "top_sql_calls": sum(row["calls"] for row in statements if row["toplevel"]),
            "nested_sql_calls": sum(row["calls"] for row in statements if not row["toplevel"]),
            "statements": statements,
        }

    def close(self) -> None:
        self.conn.close()


def call(worker: Any, operation: str, **kwargs: Any) -> dict[str, Any]:
    reply = worker.call(operation, **kwargs)
    if reply.get("error"):
        raise RuntimeError(f"{operation}: {reply['error']}")
    return reply["result"]


def database_audit(conn: Any, schema: str, sid: str, after: int, result: dict[str, Any]) -> dict[str, Any]:
    rows = conn.execute(
        sql.SQL("SELECT seq, stamped FROM {}.events WHERE session_id=%s AND seq>%s ORDER BY seq").format(
            sql.Identifier(schema)
        ),
        (sid, after),
    ).fetchall()
    items = [_parse(sid, raw) for _, raw in rows]
    if not items or [item.seq for item in items] != list(range(after + 1, after + len(items) + 1)):
        raise AssertionError("independent SQL found a journal gap")
    if any(seq != item.seq for (seq, _), item in zip(rows, items, strict=True)):
        raise AssertionError("journal envelope sequence differs from SQL sequence")
    events = [item.event.model_dump(mode="json") for item in items]
    actual = [event for event in events if event["type"] in DELTA_TYPES]
    assert_deltas(actual, result["expected_deltas"])
    if events[-1]["type"] != "turn_completed" or sum(event["type"] == "turn_completed" for event in events) != 1:
        raise AssertionError("fixture did not commit exactly one successful terminal")
    records = [{"seq": item.seq, "event": event} for item, event in zip(items, events, strict=True)]
    return {
        "rows": len(rows),
        "first_seq": items[0].seq,
        "last_seq": items[-1].seq,
        "delta_count": len(actual),
        "delta_digest": digest(actual),
        "replay_digest": normalized_digest(records),
        "deltas": actual,
    }


def run_case(
    workers: list[Any], monitor: PostgresMetrics, config: dict[str, Any], trial: str, *, failure: str | None = None
) -> dict[str, Any]:
    owner, remote = workers
    sid = uuid5(NAMESPACE_URL, f"tantra-bench/cpu-v1/{config['name']}/{trial}").hex
    command = uuid5(UUID(hex=sid), "cpu-turn").hex
    prepared = call(owner, "cpu_prepare", sid=sid, config=config)
    call(remote, "cpu_observers", sid=sid, after=prepared["after"], config=config, count=config["readers"])
    began = False
    released = False
    try:
        call(owner, "cpu_begin")
        call(remote, "cpu_begin")
        began = True
        pg_before = monitor.begin()
        started = time.perf_counter()
        call(owner, "cpu_start", sid=sid, command=command)
        call(owner, "cpu_gate", open=True)
        result = call(owner, "cpu_wait", sid=sid)
        readers = call(remote, "cpu_reader_wait", sid=sid)
        ends = [call(worker, "cpu_end") for worker in workers]
        began = False
        elapsed = (time.perf_counter() - started) * 1_000
        pg = monitor.end(pg_before)
        audit = database_audit(monitor.conn, "bench", sid, prepared["after"], result)
        if failure:
            inject_failure(failure, audit["deltas"], result["expected_deltas"])
        del audit["deltas"]
        if len(readers["readers"]) != config["readers"]:
            raise AssertionError("reader fan-out count differs from fixture")
        for reader in readers["readers"]:
            received = reader.pop("received_at_ms")
            emitted = [entry["emitted_at_ms"] for entry in result["emissions"]]
            delays = [seen - sent for seen, sent in zip(received, emitted, strict=True)]
            if any(delay < 0 for delay in delays):
                raise AssertionError("reader clock precedes provider emission clock")
            reader["first_ms"] = delays[0]
            reader["delivery_ms"] = {
                "min": min(delays),
                "p50": statistics.median(delays),
                "p95": sorted(delays)[max(0, (len(delays) * 95 + 99) // 100 - 1)],
                "max": max(delays),
            }
            for key in ("replay_digest", "delta_digest", "delta_count", "first_seq", "last_seq"):
                if reader[key] != audit[key]:
                    raise AssertionError(f"reader {key} differs from independent SQL journal")
        cleanup = [call(worker, "cpu_release", sid=sid) for worker in workers]
        released = True
        return {
            "label": config["name"],
            "trial": trial,
            "config": config,
            "elapsed_ms": elapsed,
            "cpu_ms": sum(end["cpu_ms"] for end in ends),
            "owner_cpu_ms": ends[0]["cpu_ms"],
            "remote_cpu_ms": ends[1]["cpu_ms"],
            "sql_calls": sum(end["sql_calls"] for end in ends),
            "fetched_rows": sum(end["fetched_rows"] for end in ends),
            "rss_mb": sum(end["rss_mb"] for end in ends),
            "peak_rss_mb": sum(end["peak_rss_mb"] for end in ends),
            "loop_lag_ms": [value for end in ends for value in end["loop_lag_ms"]],
            "python_cpu_ms_per_1000_deltas": sum(end["cpu_ms"] for end in ends) * 1_000 / audit["delta_count"],
            "postgres_cpu_ms_per_1000_deltas": pg["postgres_cpu_ms"] * 1_000 / audit["delta_count"],
            "workers": ends,
            "database": audit,
            "readers": readers["readers"],
            "provider": {
                key: value for key, value in result.items() if key not in ("expected_deltas", "text", "emissions")
            },
            "provider_emissions": [
                {"index": entry["index"], "emitted_at_ms": entry["emitted_at_ms"]} for entry in result["emissions"]
            ],
            "cleanup": cleanup,
            **pg,
        }
    finally:
        if began:
            for worker in workers:
                call(worker, "cpu_end")
        if not released:
            for worker in workers:
                call(worker, "cpu_release", sid=sid)


def cpu_campaign(report: dict[str, Any], args: Any, database_type: Any, worker_type: Any) -> None:
    selected = cases()
    if args.scenario:
        unknown = set(args.scenario) - {case["name"] for case in selected}
        if unknown:
            raise ValueError(f"unknown CPU scenarios: {sorted(unknown)}")
        selected = [case for case in selected if case["name"] in args.scenario]
    if args.samples < 1 or args.sessions < 1 or args.faults or args.inject_oracle_failure:
        raise ValueError("CPU mode requires positive samples/sessions; use --cpu-failure for oracle injection")
    settings = {"mode": "cpu", "model": "bench/cpu", "suite": "smoke"}
    report.update(
        {
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "host": platform.node(),
            "python": platform.python_version(),
            "coordinator": COORDINATOR_SETTINGS,
            "runtime_sha256": hashlib.sha256(
                b"".join(path.read_bytes() for path in sorted((ROOT / "packages/tantra/src").rglob("*.py")))
            ).hexdigest(),
            "bench_sha256": bench_identity(),
            "workload": {
                "mode": "cpu",
                "fixture_version": 1,
                "sessions": args.sessions,
                "primary_samples": args.samples,
                "extended_samples": 1,
                "cases": selected,
                "model": "bench/cpu",
                "event_hooks": False,
                "commit_probes": "first/middle/last; every delta in commit-audit",
            },
            "cpu_instrumentation": {
                "postgres": "cgroup v2 usage_usec, includes background work and counter probes",
                "sql": "pg_stat_statements.track=all; track_planning=off",
                "python": "per-worker process_time, no profiler",
                "measurement": "begin/end windows span control IPC and independent reader commit probes",
            },
            "baseline_eligible": args.samples >= 5 and not args.scenario and args.sessions >= 10_000,
        }
    )
    workers: list[Any] = []
    with database_type(cpu=True) as database:
        report["database"] = database.metadata
        settings.update({"dsn": database.dsn, "schema": "bench"})
        started = time.perf_counter()
        asyncio.run(seed(database.dsn, "bench", args.sessions, [], settings["model"]))
        report["fixture_ms"] = (time.perf_counter() - started) * 1_000
        monitor = PostgresMetrics(database)
        report["cpu_postgres_settings"] = monitor.settings
        report["complete"] = False
        try:
            workers.extend(worker_type(settings) for _ in range(2))
            report["worker_pids"] = [worker.pid for worker in workers]
            idle_cpu = monitor.cpu_ms()
            time.sleep(1.0)
            report["idle_postgres_cpu_ms_per_second"] = monitor.cpu_ms() - idle_cpu
            report["warmups"] = []
            for case in selected:
                warm = run_case(workers, monitor, case, "warmup")
                report["warmups"].append({"label": case["name"], "replay_digest": warm["database"]["replay_digest"]})
                print(f"cpu warmup {case['name']}: passed", flush=True)
            for trial in range(args.samples):
                order = selected[trial % len(selected) :] + selected[: trial % len(selected)]
                if trial % 2:
                    order = list(reversed(order))
                for case in order:
                    if trial and not case["primary"]:
                        continue
                    try:
                        sample = run_case(workers, monitor, case, str(trial), failure=args.cpu_failure)
                    except Exception as exc:
                        report.setdefault("scenarios", []).append(
                            {
                                "name": case["name"],
                                "status": "failed",
                                "classification": "correctness",
                                "error": str(exc),
                            }
                        )
                        raise
                    report["samples"].append(sample)
                    print(
                        f"cpu {case['name']} #{trial}: {sample['elapsed_ms']:.1f} ms; "
                        f"Python {sample['cpu_ms']:.1f} / PostgreSQL {sample['postgres_cpu_ms']:.1f} CPU ms",
                        flush=True,
                    )
            report["scenarios"] = [{"name": case["name"], "status": "passed"} for case in selected]
            report["cleanup_backlog"] = [call(worker, "backlog") for worker in workers]
        except BaseException as exc:
            report["error"] = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            for worker in workers:
                worker.stop(force=sys.exc_info()[0] is not None)
            monitor.close()
        if args.stress:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "packages/tantra/tests", "stress", "-q", "-rs"],
                cwd=ROOT,
                env={**os.environ, "TANTRA_POSTGRES_DSN": database.dsn},
                capture_output=True,
                text=True,
                timeout=1_800,
            )
            report["stress"] = {"exit_code": result.returncode, "output": result.stdout + result.stderr}
            if result.returncode or "skipped" in result.stdout:
                raise RuntimeError("required package/bench/stress checks failed or skipped; see report")
        report["complete"] = True


def cpu_summary(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for label in dict.fromkeys(sample["label"] for sample in samples):
        group = [sample for sample in samples if sample["label"] == label]
        row = {"label": label, "samples": len(group)}
        for key in (
            "owner_cpu_ms",
            "remote_cpu_ms",
            "postgres_cpu_ms",
            "wal_bytes",
            "top_sql_calls",
            "nested_sql_calls",
        ):
            values = [sample[key] for sample in group]
            row[key] = {"p50": statistics.median(values), "min": min(values), "max": max(values)}
        rows.append(row)
    return rows


def cpu_html(report: dict[str, Any]) -> str:
    if report.get("cpu_changes"):
        rows = "".join(
            f"<tr><td>{html.escape(row['label'])}</td>"
            f"<td>{row['owner_cpu_ms']['before']:.1f} → {row['owner_cpu_ms']['after']:.1f}</td>"
            f"<td>{row['remote_cpu_ms']['before']:.1f} → {row['remote_cpu_ms']['after']:.1f}</td>"
            f"<td>{row['postgres_cpu_ms']['before']:.1f} → {row['postgres_cpu_ms']['after']:.1f}</td></tr>"
            for row in report["cpu_changes"]
        )
        return (
            "<h2>CPU comparison</h2><div class='scroll'><table><thead><tr><th>Case</th>"
            "<th>Owner Python ms</th><th>Reader Python ms</th><th>PostgreSQL ms</th></tr></thead>"
            f"<tbody>{rows}</tbody></table></div>"
        )
    if not report.get("cpu_summary"):
        return ""
    rows = "".join(
        f"<tr><td>{html.escape(row['label'])}</td><td>{row['samples']}</td>"
        f"<td>{row['owner_cpu_ms']['p50']:.1f}</td><td>{row['remote_cpu_ms']['p50']:.1f}</td>"
        f"<td>{row['postgres_cpu_ms']['p50']:.1f}</td><td>{row['wal_bytes']['p50']:.0f}</td>"
        f"<td>{row['top_sql_calls']['p50']:.0f} / {row['nested_sql_calls']['p50']:.0f}</td></tr>"
        for row in report["cpu_summary"]
    )
    return (
        "<h2>Active-stream CPU</h2><p>Median CPU time per complete turn, in milliseconds. "
        "Extended cases have one measured trial; primary cases use the requested repetitions. "
        "PostgreSQL includes background work and counter probes. No paid inference or event hooks.</p>"
        "<div class='scroll'><table><thead><tr><th>Case</th><th>N</th><th>Owner Python</th>"
        "<th>Reader Python</th><th>PostgreSQL</th><th>WAL bytes</th><th>Top / nested SQL</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


def compare_cpu(left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("mode", "bench_sha256", "cpu_instrumentation", "cpu_postgres_settings"):
        if left.get(key) != right.get(key):
            raise ValueError(f"cannot compare different CPU {key}")
    indexed = [{(sample["label"], sample["trial"]): sample for sample in report["samples"]} for report in (left, right)]
    if set(indexed[0]) != set(indexed[1]) or any(
        len(index) != len(report["samples"]) for index, report in zip(indexed, (left, right), strict=True)
    ):
        raise ValueError("CPU comparison requires matching unique trial identities")
    for report in (left, right):
        if report.get("complete") is False:
            raise ValueError("cannot compare incomplete CPU campaign")
        if any(row["status"] != "passed" for row in report.get("scenarios", [])):
            raise ValueError("cannot compare failed CPU correctness evidence")
    for key, before in indexed[0].items():
        if before["database"]["replay_digest"] != indexed[1][key]["database"]["replay_digest"]:
            raise ValueError("CPU comparison replay digest differs")
    summaries = [{row["label"]: row for row in cpu_summary(report["samples"])} for report in (left, right)]
    changes = []
    for label, before in summaries[0].items():
        after = summaries[1][label]
        change: dict[str, Any] = {"label": label}
        for field in (
            "owner_cpu_ms",
            "remote_cpu_ms",
            "postgres_cpu_ms",
            "wal_bytes",
            "top_sql_calls",
            "nested_sql_calls",
        ):
            a, b = before[field]["p50"], after[field]["p50"]
            change[field] = {"before": a, "after": b, "percent": (b / a - 1) * 100 if a else None}
        changes.append(change)
    return changes
