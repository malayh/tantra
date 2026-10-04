from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid5

from psycopg import sql

from tantra import CleanupSelector, CommandTimeout, Runtime

CLEANUP_SCENARIOS = (
    "cleanup-dry-run",
    "cleanup-pagination",
    "cleanup-races",
    "cleanup-deletion-faults",
    "cleanup-long-history",
)


def _id(base: str, name: str) -> str:
    return uuid5(UUID(hex=base), f"cleanup-v1/{name}").hex


class CleanupState:
    def __init__(self, runtime: Runtime, agent: Any) -> None:
        self.runtime = runtime
        self.agent = agent
        self.original_select: Any = None
        self.original_begin: Any = None
        self.original_request: Any = None
        self.observers: dict[str, tuple[Any, asyncio.Task[Any], asyncio.Task[Any]]] = {}

    async def operation(self, request: dict[str, Any]) -> dict[str, Any] | None:
        op = request["op"]
        if op == "cleanup_seed":
            for item in request["roots"]:
                await self.runtime.create(
                    self.agent,
                    session_id=UUID(hex=item["sid"]),
                    metadata=item.get("metadata"),
                )
            return {"roots": [item["sid"] for item in request["roots"]]}
        if op == "cleanup_run":
            report = await self.runtime.cleanup(
                self._selector(request["selector"]),
                dry_run=request.get("dry_run", True),
                allow_active=request.get("allow_active", False),
                limit=request.get("limit", 100),
                after=request.get("after"),
            )
            return {
                "results": [
                    {
                        "root_id": result.root_id.hex,
                        "outcome": result.outcome,
                        "error_code": result.error_code,
                    }
                    for result in report.results
                ],
                "counts": report.counts,
                "next_after": report.next_after,
                "error_code": report.error_code,
            }
        if op == "cleanup_fingerprint":
            return {"tables": await self._fingerprint(request["roots"])}
        if op == "cleanup_history_counts":
            async with self.runtime.store._connection() as conn:
                cursor = await conn.execute(
                    self.runtime.store._sql(
                        "SELECT session_id, count(*) FROM {schema}.events"
                        " WHERE session_id = ANY(%s::text[]) GROUP BY session_id ORDER BY session_id"
                    ),
                    (request["roots"],),
                )
                return {"events": {sid: count for sid, count in await cursor.fetchall()}}
        if op == "cleanup_plan":
            return await self._plan(request["roots"])
        if op == "cleanup_verify_deleted":
            return await self._verify_deleted(request["roots"])
        if op == "cleanup_change_on":
            self.original_select = self.runtime.store.select_cleanup
            changed = False

            async def select(*args: Any, **kwargs: Any) -> Any:
                nonlocal changed
                candidates = await self.original_select(*args, **kwargs)
                if candidates and not changed:
                    changed = True
                    header = await self.runtime.store.header(candidates[0].root_id)
                    if header is None:
                        raise AssertionError("selected root disappeared before change injection")
                    await self.runtime.store.patch_header(
                        header.id,
                        metadata={**header.metadata, "cleanup_revision": "changed"},
                    )
                return candidates

            self.runtime.store.select_cleanup = select
            return {"armed": True}
        if op == "cleanup_change_off":
            if self.original_select is not None:
                self.runtime.store.select_cleanup = self.original_select
                self.original_select = None
            return {"armed": False}
        if op == "cleanup_rollback_on":
            self.original_begin = self.runtime._begin_deletion

            def rollback(*args: Any, **kwargs: Any) -> Any:
                self.original_begin(*args, **kwargs)
                raise RuntimeError("injected cleanup rollback")

            self.runtime._begin_deletion = rollback
            return {"armed": True}
        if op == "cleanup_rollback_off":
            if self.original_begin is not None:
                self.runtime._begin_deletion = self.original_begin
                self.original_begin = None
            return {"armed": False}
        if op == "cleanup_lost_reply_on":
            if self.runtime.coordinator is None:
                raise AssertionError("cleanup lost-reply fault requires a coordinator")
            self.original_request = self.runtime.coordinator.request
            lost = False

            async def request_command(envelope: Any) -> Any:
                nonlocal lost
                reply = await self.original_request(envelope)
                if envelope.operation == "delete" and not lost:
                    lost = True
                    raise CommandTimeout("injected cleanup committed reply loss")
                return reply

            self.runtime.coordinator.request = request_command
            return {"armed": True}
        if op == "cleanup_lost_reply_off":
            if self.original_request is not None:
                self.runtime.coordinator.request = self.original_request
                self.original_request = None
            return {"armed": False}
        if op == "cleanup_observe_start":
            sid = request["sid"]
            status = await self.runtime.status(UUID(hex=sid))
            stream = self.runtime.events(UUID(hex=sid), after=status.last_seq)
            event = asyncio.create_task(anext(stream))
            result = asyncio.create_task(self.runtime._wait_result(sid, UUID(hex=request["command"])))
            self.observers[sid] = (stream, event, result)
            await asyncio.sleep(0)
            return {"cursor": status.last_seq}
        if op == "cleanup_observe_finish":
            sid = request["sid"]
            stream, event, result = self.observers.pop(sid)
            outcomes = []
            for task in (event, result):
                try:
                    await asyncio.wait_for(asyncio.shield(task), 5)
                except Exception as exc:
                    outcomes.append(type(exc).__name__)
                else:
                    outcomes.append("returned")
            await stream.aclose()
            await self._wait_cleanup()
            return {
                "event": outcomes[0],
                "result": outcomes[1],
                "watchers": len(self.runtime._watchers),
                "subscriptions": len(self.runtime.coordinator._observations),
            }
        return None

    @staticmethod
    def _selector(raw: dict[str, Any]) -> CleanupSelector:
        cutoff = raw.get("inactive_before")
        return CleanupSelector(
            root_ids=None if raw.get("root_ids") is None else tuple(UUID(hex=sid) for sid in raw["root_ids"]),
            metadata=raw.get("metadata"),
            inactive_before=None if cutoff is None else datetime.fromisoformat(cutoff),
        )

    async def _fingerprint(self, roots: list[str]) -> dict[str, dict[str, Any]]:
        clauses = {
            "sessions": ("id = ANY(%s::text[])", roots),
            "events": ("session_id = ANY(%s::text[])", roots),
            "journal_index": ("actor_id = ANY(%s::text[])", roots),
            "coordinator_activity": ("root_id = ANY(%s::text[])", roots),
            "coordinator_roots": ("root_id = ANY(%s::text[])", roots),
            "coordinator_requests": ("root_id = ANY(%s::text[])", roots),
            "coordinator_changes": ("root_id = ANY(%s::text[])", roots),
            "deleted_sessions": ("root_id = ANY(%s::text[])", roots),
        }
        evidence = {}
        async with self.runtime.store._connection() as conn:
            for table, (where, params) in clauses.items():
                query = sql.SQL(
                    "SELECT count(*), md5(COALESCE(string_agg(ctid::text || ':' || xmin::text, ','"
                    " ORDER BY ctid::text), '')) FROM {}.{} WHERE " + where
                ).format(sql.Identifier(self.runtime.store.schema), sql.Identifier(table))
                row = await (await conn.execute(query, (params,))).fetchone()
                evidence[table] = {"rows": row[0], "version": row[1]}
        return evidence

    async def _plan(self, roots: list[str]) -> dict[str, Any]:
        class PlanConnection:
            def __init__(self, connection: Any) -> None:
                self.connection = connection
                self.plan: dict[str, Any] | None = None
                self.query = ""

            async def execute(self, query: Any, params: Any) -> Any:
                self.query = query.as_string(self.connection)
                explain = sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + query
                row = await (await self.connection.execute(explain, params)).fetchone()
                self.plan = row[0][0]["Plan"]
                return await self.connection.execute(query, params)

        async with self.runtime.store._connection() as conn:
            async with conn.transaction():
                await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                wrapped = PlanConnection(conn)
                candidates = await self.runtime.store._cleanup_rows(
                    wrapped,
                    root_ids=roots,
                    metadata={},
                    inactive_before=None,
                    after=None,
                    limit=len(roots),
                )
        if wrapped.plan is None:
            raise AssertionError("cleanup plan was not captured")
        plan = wrapped.plan
        nodes = []

        def visit(node: dict[str, Any]) -> None:
            nodes.append(
                {
                    "node": node["Node Type"],
                    "relation": node.get("Relation Name"),
                    "index": node.get("Index Name"),
                    "subplan": node.get("Subplan Name"),
                    "actual_rows": node.get("Actual Rows"),
                    "actual_loops": node.get("Actual Loops"),
                }
            )
            for child in node.get("Plans", ()):
                visit(child)

        visit(plan)
        root_node = next((node for node in nodes if node["subplan"] == "CTE roots"), None)
        tree_node = next((node for node in nodes if node["subplan"] == "CTE tree"), None)
        normalized = " ".join(wrapped.query.split()).lower()
        return {
            "query_sha256": hashlib.sha256(wrapped.query.encode()).hexdigest(),
            "query_features": {
                "revision_digest": "sha256" in normalized and "session_state" in normalized,
                "activity_cte": "activity as" in normalized and ".coordinator_activity" in normalized,
                "requests_cte": "requests as" in normalized and ".coordinator_requests" in normalized,
                "root_ids": "s.id = any" in normalized,
                "after": "s.created_at, s.id" in normalized,
                "metadata": "s.metadata @>" in normalized,
            },
            "candidate_roots": [candidate.root_id for candidate in candidates],
            "candidate_count": len(candidates),
            "root_rows": None if root_node is None else root_node["actual_rows"],
            "tree_rows": None if tree_node is None else tree_node["actual_rows"],
            "total_cost": plan["Total Cost"],
            "plan_rows": plan["Plan Rows"],
            "nodes": nodes,
        }

    async def _verify_deleted(self, roots: list[str]) -> dict[str, Any]:
        async with self.runtime.store._connection() as conn:
            marker_cursor = await conn.execute(
                self.runtime.store._sql(
                    "SELECT actor_id FROM {schema}.deleted_sessions WHERE root_id = ANY(%s::text[]) ORDER BY actor_id"
                ),
                (roots,),
            )
            actors = [row[0] for row in await marker_cursor.fetchall()]
            sessions = await (
                await conn.execute(
                    self.runtime.store._sql("SELECT count(*) FROM {schema}.sessions WHERE id = ANY(%s::text[])"),
                    (actors,),
                )
            ).fetchone()
            events = await (
                await conn.execute(
                    self.runtime.store._sql("SELECT count(*) FROM {schema}.events WHERE session_id = ANY(%s::text[])"),
                    (actors,),
                )
            ).fetchone()
            index = await (
                await conn.execute(
                    self.runtime.store._sql(
                        "SELECT count(*) FROM {schema}.journal_index WHERE actor_id = ANY(%s::text[])"
                    ),
                    (actors,),
                )
            ).fetchone()
            activity = await (
                await conn.execute(
                    self.runtime.store._sql(
                        "SELECT count(*) FROM {schema}.coordinator_activity WHERE root_id = ANY(%s::text[])"
                    ),
                    (roots,),
                )
            ).fetchone()
            receipt_cursor = await conn.execute(
                self.runtime.store._sql(
                    "SELECT envelope->>'operation', reply->'result'->>'deleted'"
                    " FROM {schema}.coordinator_requests WHERE root_id = ANY(%s::text[])"
                    " ORDER BY root_id, request_id"
                ),
                (roots,),
            )
            receipts = [list(row) for row in await receipt_cursor.fetchall()]
            change_cursor = await conn.execute(
                self.runtime.store._sql(
                    "SELECT kind, count(*) FROM {schema}.coordinator_changes"
                    " WHERE root_id = ANY(%s::text[]) GROUP BY kind ORDER BY kind"
                ),
                (roots,),
            )
            changes = {kind: count for kind, count in await change_cursor.fetchall()}
            per_root = {}
            for root in roots:
                marker_cursor = await conn.execute(
                    self.runtime.store._sql(
                        "SELECT actor_id FROM {schema}.deleted_sessions WHERE root_id = %s ORDER BY actor_id"
                    ),
                    (root,),
                )
                root_actors = [row[0] for row in await marker_cursor.fetchall()]
                live_cursor = await conn.execute(
                    self.runtime.store._sql(
                        "SELECT (SELECT count(*) FROM {schema}.sessions WHERE id = ANY(%s::text[])),"
                        " (SELECT count(*) FROM {schema}.events WHERE session_id = ANY(%s::text[])),"
                        " (SELECT count(*) FROM {schema}.journal_index WHERE actor_id = ANY(%s::text[])),"
                        " (SELECT count(*) FROM {schema}.coordinator_activity WHERE root_id = %s)"
                    ),
                    (root_actors, root_actors, root_actors, root),
                )
                live = await live_cursor.fetchone()
                root_receipts = await conn.execute(
                    self.runtime.store._sql(
                        "SELECT envelope->>'operation', reply->'result'->>'deleted'"
                        " FROM {schema}.coordinator_requests WHERE root_id = %s ORDER BY request_id"
                    ),
                    (root,),
                )
                per_root[root] = {
                    "markers": root_actors,
                    "live_content": sum(live),
                    "receipts": [list(row) for row in await root_receipts.fetchall()],
                }
        return {
            "markers": actors,
            "live_content": sessions[0] + events[0] + index[0] + activity[0],
            "receipts": receipts,
            "changes": changes,
            "per_root": per_root,
        }

    async def _wait_cleanup(self) -> None:
        async with asyncio.timeout(5):
            while self.runtime._watchers or self.runtime.coordinator._observations:
                await asyncio.sleep(0.01)

    async def close(self) -> None:
        if self.original_select is not None:
            self.runtime.store.select_cleanup = self.original_select
        if self.original_begin is not None:
            self.runtime._begin_deletion = self.original_begin
        if self.original_request is not None:
            self.runtime.coordinator.request = self.original_request
        for stream, event, result in self.observers.values():
            event.cancel()
            result.cancel()
            await asyncio.gather(event, result, return_exceptions=True)
            await stream.aclose()
        self.observers.clear()


def cleanup_scenarios(
    report: dict[str, Any],
    workers: list[Any],
    ids: list[str],
    call: Callable[..., dict[str, Any]],
) -> dict[str, Callable[[], dict[str, Any] | None]]:
    cutoff = (datetime.now(UTC) + timedelta(days=1)).isoformat()

    def assert_deleted(evidence: dict[str, Any], roots: list[str]) -> None:
        if evidence["live_content"] or len(evidence["markers"]) != len(roots):
            raise AssertionError(evidence)
        for root in roots:
            item = evidence["per_root"][root]
            if item["live_content"] or item["markers"] != [root] or item["receipts"] != [["delete", "true"]]:
                raise AssertionError({root: item})

    def seed(worker: Any, name: str, metadata: list[dict[str, Any]]) -> list[str]:
        roots = [_id(ids[0], f"{name}-{index}") for index in range(len(metadata))]
        call(
            worker,
            f"fault/{name}/seed",
            "cleanup_seed",
            roots=[{"sid": sid, "metadata": value} for sid, value in zip(roots, metadata, strict=True)],
        )
        return roots

    def dry_run() -> dict[str, Any]:
        a = workers[0]
        roots = seed(a, "cleanup-dry-run", [{"cleanup": "dry"}, {"cleanup": "dry"}])
        before = call(a, "fault/cleanup-dry-run/before", "cleanup_fingerprint", roots=roots)
        result = call(
            a,
            "fault/cleanup-dry-run/run",
            "cleanup_run",
            selector={"root_ids": roots, "metadata": {"cleanup": "dry"}},
            dry_run=True,
            limit=2,
        )
        sample = report["samples"][-1]
        after = call(a, "fault/cleanup-dry-run/after", "cleanup_fingerprint", roots=roots)
        if [item["outcome"] for item in result["results"]] != ["candidate", "candidate"]:
            raise AssertionError("dry run returned non-candidates")
        if before != after or sample["provider_requests"]:
            raise AssertionError("dry run mutated state or called the provider")
        return {"roots": roots, "counts": result["counts"], "fingerprint": before["tables"]}

    def pagination() -> dict[str, Any]:
        a, b = workers
        roots = seed(
            a,
            "cleanup-pagination",
            [
                {"cleanup": "page", "eligible": True, "nullable": None},
                {"cleanup": "page", "eligible": True, "nullable": None},
                {"cleanup": "page", "eligible": True, "nullable": None},
                {"cleanup": "page", "eligible": True, "nullable": None},
                {"cleanup": "page", "eligible": True},
            ],
        )
        selector = {
            "root_ids": roots,
            "metadata": {"cleanup": "page", "eligible": True, "nullable": None},
            "inactive_before": cutoff,
        }
        first = call(
            a,
            "fault/cleanup-pagination/page-1",
            "cleanup_run",
            selector=selector,
            dry_run=False,
            limit=2,
        )
        if first["next_after"] is None:
            raise AssertionError("first cleanup page had no continuation")
        second = call(
            b,
            "fault/cleanup-pagination/page-2",
            "cleanup_run",
            selector=selector,
            dry_run=False,
            limit=2,
            after=first["next_after"],
        )
        deleted = [item["root_id"] for page in (first, second) for item in page["results"]]
        if deleted != roots[:4] or any(
            item["outcome"] != "deleted" for page in (first, second) for item in page["results"]
        ):
            raise AssertionError({"expected": roots[:4], "deleted": deleted})
        if second["next_after"] is not None:
            raise AssertionError("final cleanup page retained a continuation")
        remaining = call(
            b,
            "fault/cleanup-pagination/filter",
            "cleanup_run",
            selector={"root_ids": roots, "metadata": {"cleanup": "page"}},
            dry_run=True,
        )
        if [item["root_id"] for item in remaining["results"]] != [roots[4]]:
            raise AssertionError("AND/null metadata filter selected the wrong roots")
        evidence = call(a, "fault/cleanup-pagination/verify", "cleanup_verify_deleted", roots=roots[:4])
        assert_deleted(evidence, roots[:4])
        return {"pages": [first["counts"], second["counts"]], "deleted": deleted}

    def races() -> dict[str, Any]:
        a, b = workers
        changed, running = seed(
            b,
            "cleanup-races",
            [{"cleanup": "changed"}, {"cleanup": "running"}],
        )
        call(a, "fault/cleanup-races/change-on", "cleanup_change_on")
        changed_result = call(
            a,
            "fault/cleanup-races/changed",
            "cleanup_run",
            selector={"root_ids": [changed], "metadata": {"cleanup": "changed"}},
            dry_run=False,
        )
        call(a, "fault/cleanup-races/change-off", "cleanup_change_off")
        if [item["outcome"] for item in changed_result["results"]] != ["changed"]:
            raise AssertionError(changed_result)
        command = _id(running, "running-command")
        call(b, "fault/cleanup-races/claim", "fault_claim", sid=running)
        call(b, "fault/cleanup-races/gate", "fault_gate", open=False)
        call(b, "fault/cleanup-races/send", "fault_send", sid=running, command=command, input="hold")
        call(b, "fault/cleanup-races/started", "fault_wait_started", sid=running, command=command)
        active = call(
            a,
            "fault/cleanup-races/active",
            "cleanup_run",
            selector={"root_ids": [running], "metadata": {"cleanup": "running"}},
            dry_run=False,
        )
        if [item["outcome"] for item in active["results"]] != ["active"]:
            raise AssertionError(active)
        call(b, "fault/cleanup-races/open", "fault_gate", open=True)
        call(b, "fault/cleanup-races/complete", "fault_prompt", sid=running, command=command, input="hold")
        call(b, "fault/cleanup-races/release", "fault_release", sid=running)
        return {"changed": changed_result["counts"], "active": active["counts"]}

    def deletion_faults() -> dict[str, Any]:
        a, b = workers
        remote, rollback, lost = seed(
            b,
            "cleanup-deletion-faults",
            [{"cleanup": "remote"}, {"cleanup": "rollback"}, {"cleanup": "lost"}],
        )
        command = _id(remote, "observer-result")
        call(b, "fault/cleanup-faults/remote-claim", "fault_claim", sid=remote)
        call(b, "fault/cleanup-faults/observe", "cleanup_observe_start", sid=remote, command=command)
        remote_result = call(
            a,
            "fault/cleanup-faults/remote-delete",
            "cleanup_run",
            selector={"root_ids": [remote]},
            dry_run=False,
        )
        observed = call(b, "fault/cleanup-faults/observe-finish", "cleanup_observe_finish", sid=remote)
        denied = call(
            b,
            "fault/cleanup-faults/writer-revoked",
            "fault_send",
            sid=remote,
            command=_id(remote, "late"),
            expect_error=True,
        )
        call(b, "fault/cleanup-faults/remote-release", "fault_release", sid=remote)
        if [item["outcome"] for item in remote_result["results"]] != ["deleted"]:
            raise AssertionError(remote_result)
        if observed != {"event": "SessionNotFound", "result": "SessionNotFound", "watchers": 0, "subscriptions": 0}:
            raise AssertionError(observed)
        if denied["accepted"]:
            raise AssertionError("deleted root retained its writer")
        call(a, "fault/cleanup-faults/rollback-on-a", "cleanup_rollback_on")
        call(b, "fault/cleanup-faults/rollback-on-b", "cleanup_rollback_on")
        rolled_back = call(
            a,
            "fault/cleanup-faults/rollback",
            "cleanup_run",
            selector={"root_ids": [rollback]},
            dry_run=False,
        )
        call(a, "fault/cleanup-faults/rollback-off-a", "cleanup_rollback_off")
        call(b, "fault/cleanup-faults/rollback-off-b", "cleanup_rollback_off")
        retained = call(b, "fault/cleanup-faults/rollback-retained", "cleanup_fingerprint", roots=[rollback])
        if rolled_back["results"][0]["outcome"] not in ("failed", "unknown"):
            raise AssertionError(rolled_back)
        if retained["tables"]["sessions"]["rows"] != 1:
            raise AssertionError("rollback did not retain the root")
        retried = call(
            a,
            "fault/cleanup-faults/rollback-retry",
            "cleanup_run",
            selector={"root_ids": [rollback]},
            dry_run=False,
        )
        if [item["outcome"] for item in retried["results"]] != ["deleted"]:
            raise AssertionError(retried)
        call(a, "fault/cleanup-faults/lost-on", "cleanup_lost_reply_on")
        unknown = call(
            a,
            "fault/cleanup-faults/lost",
            "cleanup_run",
            selector={"root_ids": [lost]},
            dry_run=False,
        )
        call(a, "fault/cleanup-faults/lost-off", "cleanup_lost_reply_off")
        if [item["outcome"] for item in unknown["results"]] != ["unknown"] or unknown["error_code"] != "CommandTimeout":
            raise AssertionError(unknown)
        retry = call(
            b,
            "fault/cleanup-faults/lost-retry",
            "cleanup_run",
            selector={"root_ids": [lost]},
            dry_run=False,
        )
        if retry["results"]:
            raise AssertionError("retry rediscovered an already deleted root")
        verified = call(a, "fault/cleanup-faults/verify", "cleanup_verify_deleted", roots=[remote, rollback, lost])
        assert_deleted(verified, [remote, rollback, lost])
        return {
            "remote": observed,
            "rollback": rolled_back["counts"],
            "lost_reply": unknown["counts"],
            "retry_results": retry["results"],
            "retained": verified,
        }

    def long_history() -> dict[str, Any]:
        a, b = workers
        roots = ids[:2]
        counts = call(b, "fault/cleanup-history/counts", "cleanup_history_counts", roots=roots)
        plan = call(b, "fault/cleanup-history/plan", "cleanup_plan", roots=roots)
        if not all(plan["query_features"].values()):
            raise AssertionError(plan["query_features"])
        if plan["candidate_roots"] != roots or plan["candidate_count"] != len(roots):
            raise AssertionError(plan)
        if plan["root_rows"] != len(roots) or plan["tree_rows"] != len(roots):
            raise AssertionError({"root_rows": plan["root_rows"], "tree_rows": plan["tree_rows"]})
        if any(node["relation"] == "events" for node in plan["nodes"]):
            raise AssertionError("production cleanup plan reached historical events")
        if len(ids) >= 10_000 and not any(node["index"] for node in plan["nodes"]):
            raise AssertionError("large cleanup plan used no indexes")
        preview = call(
            b,
            "fault/cleanup-history/preview",
            "cleanup_run",
            selector={"root_ids": roots},
            dry_run=True,
            limit=2,
        )
        preview_sample = report["samples"][-1]
        deleted = call(
            a,
            "fault/cleanup-history/delete",
            "cleanup_run",
            selector={"root_ids": roots},
            dry_run=False,
            limit=2,
        )
        delete_sample = report["samples"][-1]
        if [item["outcome"] for item in preview["results"]] != ["candidate", "candidate"]:
            raise AssertionError(preview)
        if [item["outcome"] for item in deleted["results"]] != ["deleted", "deleted"]:
            raise AssertionError(deleted)
        if preview_sample["event_body_queries"] or delete_sample["event_body_queries"]:
            raise AssertionError("cleanup read historical event bodies")
        verified = call(a, "fault/cleanup-history/verify", "cleanup_verify_deleted", roots=roots)
        assert_deleted(verified, roots)
        return {
            "history_events": counts["events"],
            "plan": plan,
            "preview": {
                "elapsed_ms": preview_sample["elapsed_ms"],
                "sql_calls": preview_sample["sql_calls"],
                "fetched_rows": preview_sample["fetched_rows"],
                "sql_ms": preview_sample["sql_ms"],
                "event_body_queries": preview_sample["event_body_queries"],
                "event_body_rows": preview_sample["event_body_rows"],
            },
            "delete": {
                "elapsed_ms": delete_sample["elapsed_ms"],
                "sql_calls": delete_sample["sql_calls"],
                "fetched_rows": delete_sample["fetched_rows"],
                "sql_ms": delete_sample["sql_ms"],
                "event_body_queries": delete_sample["event_body_queries"],
                "event_body_rows": delete_sample["event_body_rows"],
            },
            "retained": verified,
        }

    return {
        "cleanup-dry-run": dry_run,
        "cleanup-pagination": pagination,
        "cleanup-races": races,
        "cleanup-deletion-faults": deletion_faults,
        "cleanup-long-history": long_history,
    }
