from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import (
    Agent,
    CleanupReport,
    CleanupSelector,
    CommandTimeout,
    FakeProvider,
    MemoryStore,
    PostgresCoordinator,
    PostgresStore,
    Runtime,
    SessionNotFound,
    SQLiteStore,
    TantraError,
)
from tantra.events import InputQueued, SessionHeader
from tantra.stores.fs import FileSystemStore


class Bot(Agent):
    pass


@pytest.fixture(params=("memory", "sqlite"))
async def cleanup_runtime(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Runtime]:
    store = MemoryStore() if request.param == "memory" else SQLiteStore(tmp_path / "cleanup.db")
    await store.setup()
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m")
    try:
        yield runtime
    finally:
        await runtime.aclose()


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"metadata": {}},
        {"root_ids": "not-a-collection"},
        {"root_ids": ["not-a-uuid"]},
        {"metadata": {"nested": {"value": 1}}},
        {"metadata": {"nested": [1]}},
        {"metadata": {1: "value"}},
        {"metadata": {"number": float("nan")}},
        {"metadata": {"number": float("inf")}},
        {"inactive_before": datetime(2020, 1, 1)},
        {"inactive_before": "2020-01-01"},
    ],
)
def test_invalid_selector(kwargs: dict[str, Any]) -> None:
    with pytest.raises((TypeError, ValueError)):
        CleanupSelector(**kwargs)


def test_selector_freezes_inputs() -> None:
    root = uuid4()
    ids, metadata = [root, root], {"tenant": "one"}
    selector = CleanupSelector(root_ids=ids, metadata=metadata)
    ids.clear()
    metadata["tenant"] = "two"
    assert selector.root_ids == (root,)
    assert selector.metadata == {"tenant": "one"}
    with pytest.raises(TypeError):
        selector.metadata["tenant"] = "three"


@pytest.mark.parametrize(
    "kwargs", [{"limit": 0}, {"limit": 1001}, {"limit": True}, {"dry_run": 1}, {"allow_active": 1}]
)
async def test_argument_validation_before_selection(cleanup_runtime: Runtime, kwargs: dict[str, Any]) -> None:
    root = await cleanup_runtime.create(Bot)
    with pytest.raises((TypeError, ValueError)):
        await cleanup_runtime.cleanup(CleanupSelector(root_ids=[root]), **kwargs)
    assert await cleanup_runtime.store.header(root.hex) is not None


@pytest.mark.parametrize("after", ["", "invalid", "x" * 257, 5, ["invalid"]])
async def test_invalid_cursor(cleanup_runtime: Runtime, after: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        await cleanup_runtime.cleanup(CleanupSelector(root_ids=[]), after=after)


async def test_cursor_rejects_unknown_version(cleanup_runtime: Runtime) -> None:
    after = base64.urlsafe_b64encode(json.dumps([2, datetime.now(UTC).isoformat(), str(uuid4())]).encode()).decode()
    with pytest.raises(ValueError):
        await cleanup_runtime.cleanup(CleanupSelector(root_ids=[]), after=after)


async def test_dry_run_preserves_all_state_and_does_not_infer(cleanup_runtime: Runtime) -> None:
    runtime = cleanup_runtime
    root = await runtime.create(Bot, metadata={"tenant": "one"})
    other = await runtime.create(Bot, metadata={"tenant": "two"})
    child = uuid4()
    await runtime.store.create(SessionHeader(id=child.hex, parent_id=root.hex, agent="Bot", model="m"))
    before = [await runtime.store.header(sid.hex) for sid in (root, child, other)]
    report = await runtime.cleanup(CleanupSelector(metadata={"tenant": "one"}))
    assert [(result.root_id, result.outcome) for result in report.results] == [(root, "candidate")]
    assert report.counts["candidate"] == 1 and sum(report.counts.values()) == 1
    assert before == [await runtime.store.header(sid.hex) for sid in (root, child, other)]
    assert not runtime.active and not runtime._connections and not runtime.provider.requests
    assert not await runtime.store.is_deleted(root.hex)
    assert await runtime.cleanup(CleanupSelector(root_ids=[])) == CleanupReport(())


async def test_pages_survive_deletion_and_exact_reviewed_ids(cleanup_runtime: Runtime) -> None:
    runtime = cleanup_runtime
    roots = [await runtime.create(Bot, metadata={"tenant": "one"}) for _ in range(3)]
    unrelated = await runtime.create(Bot, metadata={"tenant": "two"})
    first = await runtime.cleanup(CleanupSelector(metadata={"tenant": "one"}), limit=1)
    assert first.results[0].root_id == roots[0] and first.next_after
    reviewed = await runtime.cleanup(
        CleanupSelector(root_ids=[first.results[0].root_id], metadata={"tenant": "one"}), dry_run=False
    )
    assert reviewed.counts["deleted"] == 1
    next_page = await runtime.cleanup(CleanupSelector(metadata={"tenant": "one"}), after=first.next_after, limit=1)
    assert next_page.results[0].root_id == roots[1] and next_page.next_after
    last = await runtime.cleanup(
        CleanupSelector(metadata={"tenant": "one"}), after=next_page.next_after, dry_run=False, limit=1
    )
    assert last.results[0].root_id == roots[2] and last.next_after is None
    assert await runtime.store.header(unrelated.hex) is not None
    assert (await runtime.cleanup(CleanupSelector(root_ids=[roots[0]]))).results == ()


async def test_pagination_includes_concurrent_creation_without_repeating_deleted_root(cleanup_runtime: Runtime) -> None:
    runtime = cleanup_runtime
    first = await runtime.create(Bot, metadata={"scope": "paging"})
    second = await runtime.create(Bot, metadata={"scope": "paging"})
    report = await runtime.cleanup(CleanupSelector(metadata={"scope": "paging"}), dry_run=False, limit=1)
    assert report.results[0].root_id == first and report.next_after
    added = await runtime.create(Bot, metadata={"scope": "paging"})
    following = await runtime.cleanup(
        CleanupSelector(metadata={"scope": "paging"}), dry_run=False, after=report.next_after
    )
    assert [result.root_id for result in following.results] == [second, added]
    assert following.counts["deleted"] == 2 and following.next_after is None


async def test_child_ids_rejected_before_any_mutation(cleanup_runtime: Runtime) -> None:
    runtime = cleanup_runtime
    root = await runtime.create(Bot)
    child = uuid4()
    await runtime.store.create(SessionHeader(id=child.hex, root_id=root.hex, parent_id=root.hex, agent="Bot"))
    with pytest.raises(TantraError, match="child"):
        await runtime.cleanup(CleanupSelector(root_ids=[root, child]), dry_run=False)
    assert await runtime.store.header(root.hex) is not None


async def test_changed_and_disappeared_candidates_skip_before_cancellation(
    cleanup_runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = cleanup_runtime
    changed = await runtime.create(Bot)
    absent = await runtime.create(Bot)
    original = runtime._accept_delete
    cancelled = []
    begin = runtime._begin_deletion

    def counted(root: str, *args: Any) -> Any:
        cancelled.append(root)
        return begin(root, *args)

    async def raced(root: UUID, **kwargs: Any) -> bool:
        if root == changed:
            await runtime.store.patch_header(root.hex, metadata={"changed": True})
        if root == absent:
            await original(root, allow_active=False)
        return await original(root, **kwargs)

    monkeypatch.setattr(runtime, "_accept_delete", raced)
    monkeypatch.setattr(runtime, "_begin_deletion", counted)
    report = await runtime.cleanup(CleanupSelector(root_ids=[changed, absent]), dry_run=False)
    assert [result.outcome for result in report.results] == ["changed", "absent"]
    assert changed.hex not in cancelled
    assert await runtime.store.header(changed.hex) is not None


async def test_active_skip_and_forced_cleanup_without_recovery(cleanup_runtime: Runtime) -> None:
    runtime = cleanup_runtime
    root = await runtime.create(Bot)
    await runtime.store.enqueue(root.hex, InputQueued(command_id=uuid4().hex, input="never run"))
    selector = CleanupSelector(root_ids=[root])
    assert (await runtime.cleanup(selector)).counts["active"] == 1
    assert (await runtime.cleanup(selector, dry_run=False)).counts["active"] == 1
    assert (await runtime.cleanup(selector, allow_active=True)).counts["candidate"] == 1
    assert (await runtime.cleanup(selector, dry_run=False, allow_active=True)).counts["deleted"] == 1
    assert not runtime.provider.requests


async def test_definitive_failures_do_not_undo_or_stop_batch(
    cleanup_runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = cleanup_runtime
    roots = [await runtime.create(Bot) for _ in range(3)]
    original = runtime._accept_delete

    async def fail(root: UUID, **kwargs: Any) -> bool:
        if root == roots[1]:
            raise ValueError("secret conversation content")
        return await original(root, **kwargs)

    monkeypatch.setattr(runtime, "_accept_delete", fail)
    report = await runtime.cleanup(CleanupSelector(root_ids=roots), dry_run=False)
    assert [result.outcome for result in report.results] == ["deleted", "failed", "deleted"]
    assert report.results[1].error_code == "ValueError" and "secret" not in repr(report)
    assert await runtime.store.header(roots[1].hex) is not None


async def test_unknown_outcome_stops_batch_and_cursor_retries_uncertain_root(
    cleanup_runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = cleanup_runtime
    roots = [await runtime.create(Bot) for _ in range(3)]
    original = runtime._accept_delete

    async def lose_reply(root: UUID, **kwargs: Any) -> bool:
        deleted = await original(root, **kwargs)
        if root == roots[1]:
            raise CommandTimeout("lost acknowledgment")
        return deleted

    monkeypatch.setattr(runtime, "_accept_delete", lose_reply)
    report = await runtime.cleanup(CleanupSelector(root_ids=roots), dry_run=False)
    assert [result.outcome for result in report.results] == ["deleted", "unknown"]
    assert report.next_after and report.error_code == "CommandTimeout"
    assert await runtime.store.header(roots[2].hex) is not None
    monkeypatch.setattr(runtime, "_accept_delete", original)
    assert await runtime.delete(roots[1]) is False
    resumed = await runtime.cleanup(CleanupSelector(root_ids=roots), dry_run=False, after=report.next_after)
    assert [(result.root_id, result.outcome) for result in resumed.results] == [(roots[2], "deleted")]


async def test_selection_infrastructure_failure_has_no_mutation(
    cleanup_runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def unavailable(*args: Any, **kwargs: Any) -> Any:
        raise OSError("database unavailable")

    root = await cleanup_runtime.create(Bot)
    monkeypatch.setattr(cleanup_runtime.store, "select_cleanup", unavailable)
    report = await cleanup_runtime.cleanup(CleanupSelector(root_ids=[root]), dry_run=False)
    assert not report.results and report.error_code == "OSError"
    assert await cleanup_runtime.store.header(root.hex) is not None


async def test_cancelled_batch_admits_no_more_roots_and_finishes_accepted_delete(
    cleanup_runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = cleanup_runtime
    roots = [await runtime.create(Bot) for _ in range(2)]
    original = runtime._accept_delete
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def gated(root: UUID, **kwargs: Any) -> bool:
        entered.set()
        await release.wait()
        result = await original(root, **kwargs)
        finished.set()
        return result

    monkeypatch.setattr(runtime, "_accept_delete", gated)
    task = asyncio.create_task(runtime.cleanup(CleanupSelector(root_ids=roots), dry_run=False))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release.set()
    await asyncio.wait_for(finished.wait(), 1)
    assert await runtime.store.header(roots[0].hex) is None
    assert await runtime.store.header(roots[1].hex) is not None


async def test_unsupported_cleanup_rejected_before_mutation(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    await store.setup()
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m")
    root = await runtime.create(Bot)
    try:
        with pytest.raises(NotImplementedError):
            await runtime.cleanup(CleanupSelector(root_ids=[root]), dry_run=False)
        assert await store.header(root.hex) is not None
    finally:
        await runtime.aclose()


async def test_postgres_remote_batch_and_readers(postgres_dsn: str, pg_schema: str) -> None:
    store = PostgresStore(postgres_dsn, schema=pg_schema)
    other_store = PostgresStore(postgres_dsn, schema=pg_schema)
    a = Runtime(FakeProvider([]), store, [Bot], default_model="m", coordinator=PostgresCoordinator(store))
    b = Runtime(FakeProvider([]), other_store, [Bot], default_model="m", coordinator=PostgresCoordinator(other_store))
    await a.start()
    await b.start()
    try:
        root = await a.create(Bot, metadata={"tenant": "one"})
        unrelated = await a.create(Bot, metadata={"tenant": "two"})
        async with a.connect(root, writable=True) as writer, b.connect(root) as reader:
            report = await b.cleanup(CleanupSelector(metadata={"tenant": "one"}), dry_run=False)
            assert report.counts["deleted"] == 1 and not report.error_code
            with pytest.raises(SessionNotFound):
                await asyncio.wait_for(anext(reader), 3)
            with pytest.raises(SessionNotFound):
                await writer.send("late", command_id=uuid4())
        assert await store.header(unrelated.hex) is not None
        assert not a.provider.requests and not b.provider.requests
        await asyncio.sleep(0.05)
        assert not a._watchers and not b._watchers
    finally:
        await a.aclose()
        await b.aclose()
        await store.close()
        await other_store.close()
