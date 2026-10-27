from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from acp_support import Scripted

from tantra import (
    Agent,
    CoordinatorUnavailable,
    FakeProvider,
    ModelChangeBusy,
    PostgresCoordinator,
    PostgresStore,
    Runtime,
    SessionHeader,
    SessionNotFound,
    TantraError,
    TreeChange,
)
from tantra.providers.base import ProviderEvent, SampleRequest, ToolCall
from tantra.providers.fake import Sample
from tantra.runtime import TREE_INTERVAL, _Tree
from tantra.stores import postgres

psycopg = pytest.importorskip("psycopg")
sql = pytest.importorskip("psycopg.sql")


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    subagents = [Worker]


class Bot(Agent):
    pass


class Plain(PostgresStore):
    pass


class PlainCoordinator(PostgresCoordinator):
    pass


class Held(Scripted):
    def __init__(self, release: asyncio.Event, **scripts: list[Sample]) -> None:
        super().__init__(**scripts)
        self.release = release

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        if req.model == "worker":
            await self.release.wait()
        async for event in self.providers[req.model].stream(req):
            yield event


def script(release: asyncio.Event) -> Held:
    spawn = ToolCall(id="s1", name="spawn", args=json.dumps({"agent_name": "worker", "input": "dig"}))
    return Held(
        release,
        root=[Sample(tool_calls=[spawn]), Sample(text="spawned"), Sample(text="noted")],
        worker=[Sample(text="found")],
    )


async def until(tree: AsyncIterator[TreeChange], seen: list[TreeChange], done: Callable[[TreeChange], bool]) -> None:
    async with asyncio.timeout(10):
        while not done(seen[-1]):
            seen.append(await anext(tree))


async def test_a_non_owner_watches_the_tree_without_reading_journals(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = asyncio.Event()
    instances = []
    for _ in range(2):
        store = PostgresStore(postgres_dsn, schema=pg_schema)
        coordinator = PostgresCoordinator(store, lease_ttl=2.0, request_timeout=5.0, catch_up_interval=0.02)
        runtime = Runtime(script(release), store, [Root], default_model="m", coordinator=coordinator)
        await runtime.start()
        instances.append((store, runtime))
    (owner_store, owner), (_, observer) = instances
    root_id = await owner.create(Root)
    stream = observer.events(root_id)
    tree = observer.watch_tree(root_id)
    try:
        await anext(stream)
        seen = [await anext(tree)]
        assert seen[0] == TreeChange(None, "root", tuple(await observer.tree_status(root_id)))
        assert root_id.hex in observer._known_roots and root_id.hex not in observer._ownerships
        await observer.patch_header(root_id, title="From observer")
        await until(tree, seen, lambda change: change.title == "From observer")
        await owner_store.patch_header(root_id.hex, title="From store")
        await until(tree, seen, lambda change: change.title == "From store")
        async with owner.connect(root_id, writable=True) as connection:
            await connection.send("go", command_id=uuid4())
            await until(tree, seen, lambda change: len(change.actors) == 2 and change.actors[1].state == "running")
            with pytest.raises(ModelChangeBusy):
                await observer.patch_header(root_id, model="other")
            release.set()

            def replied(change: TreeChange) -> bool:
                return (
                    len(change.actors) == 2
                    and change.actors[1].state == "idle"
                    and change.actors[0].state == "idle"
                    and change.actors[0].last_turn is not None
                    and not any(actor.active for actor in change.actors)
                )

            await until(tree, seen, replied)
            while any(not task.done() for task in owner.active.values()):
                await asyncio.sleep(0.02)
        child = seen[-1].actors[1]
        assert child.parent_id == root_id and child.name == "worker"
        queries: list[str] = []
        execute = postgres.psycopg.AsyncConnection.execute

        async def capture(connection: Any, query: Any, params: Any = None, **kwargs: Any) -> Any:
            rendered = query.as_string(connection) if hasattr(query, "as_string") else query
            if ".events" in rendered:
                queries.append(rendered)
            return await execute(connection, query, params, **kwargs)

        await asyncio.sleep(0.2)
        monkeypatch.setattr(postgres.psycopg.AsyncConnection, "execute", capture)
        pending = asyncio.ensure_future(anext(tree))
        await asyncio.sleep(0.5)
        monkeypatch.undo()
        assert queries == []
        assert await owner.delete(root_id, allow_active=True)
        with pytest.raises(SessionNotFound):
            await asyncio.wait_for(pending, 5)
    finally:
        release.set()
        await tree.aclose()
        await stream.aclose()
        for store, runtime in reversed(instances):
            await runtime.aclose()
            await store.close()
    states = [change.actors[1].state for change in seen if len(change.actors) == 2]
    assert states.index("running") < states.index("idle")
    assert all(left != right for left, right in zip(seen, seen[1:], strict=False))


async def test_an_expired_lease_turns_the_observed_tree_inactive(postgres_dsn: str, pg_schema: str) -> None:
    release = asyncio.Event()
    instances = []
    for _ in range(2):
        store = PostgresStore(postgres_dsn, schema=pg_schema)
        coordinator = PostgresCoordinator(store, lease_ttl=2.0, request_timeout=5.0, catch_up_interval=0.02)
        runtime = Runtime(script(release), store, [Root], default_model="m", coordinator=coordinator)
        await runtime.start()
        instances.append((store, runtime))
    (_, owner), (_, observer) = instances
    root_id = await owner.create(Root)
    tree = observer.watch_tree(root_id)
    pending: asyncio.Future[TreeChange] | None = None
    try:
        seen = [await anext(tree)]
        async with owner.connect(root_id, writable=True) as connection:
            await connection.send("go", command_id=uuid4())
            await until(
                tree,
                seen,
                lambda change: len(change.actors) == 2 and change.actors[1].active and not change.actors[0].active,
            )
        pending = asyncio.ensure_future(anext(tree))
        while (await asyncio.wait({pending}, timeout=0.3))[0]:
            seen.append(pending.result())
            pending = asyncio.ensure_future(anext(tree))
        assert seen[-1].actors[1].active
        renewal = owner._renewals[root_id.hex]
        renewal.cancel()
        await asyncio.gather(renewal, return_exceptions=True)
        async with await psycopg.AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.coordinator_roots SET expires_at=clock_timestamp()-interval '1 second' WHERE root_id=%s"
                ).format(sql.Identifier(pg_schema)),
                (root_id.hex,),
            )
        change = await asyncio.wait_for(pending, 3)
        assert not any(actor.active for actor in change.actors)
        assert change == TreeChange(None, "root", tuple(await observer.tree_status(root_id)))
        assert change.actors[1].state == "running"
    finally:
        release.set()
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await tree.aclose()
        for store, runtime in reversed(instances):
            await runtime.aclose()
            await store.close()


async def bot_runtime(
    postgres_dsn: str,
    pg_schema: str,
    store_type: type[PostgresStore] = PostgresStore,
    coordinator_type: type[PostgresCoordinator] = PostgresCoordinator,
    catch_up_interval: float = 0.02,
) -> Runtime:
    store = store_type(postgres_dsn, schema=pg_schema)
    coordinator = coordinator_type(store, lease_ttl=5.0, request_timeout=5.0, catch_up_interval=catch_up_interval)
    runtime = Runtime(FakeProvider([]), store, [Bot], default_model="m", coordinator=coordinator)
    await runtime.start()
    return runtime


def tree_counts(runtime: Runtime) -> dict[str, int]:
    counts = {"reads": 0, "pages": 0}
    tree_headers = runtime._tree_headers
    changes = runtime.coordinator._changes

    async def read(root: str) -> Any:
        counts["reads"] += 1
        return await tree_headers(root)

    async def page(root: str, after: int) -> Any:
        counts["pages"] += 1
        return await changes(root, after)

    runtime._tree_headers = read
    runtime.coordinator._changes = page
    return counts


async def publish(runtime: Runtime, root: str, kinds: list[str], title: str | None = None) -> list[tuple[int, str]]:
    store = runtime.store
    async with store._connection() as conn, conn.transaction():
        if title is not None:
            await conn.execute(
                store._sql(
                    "UPDATE {schema}.sessions SET header = jsonb_set(header, '{{title}}', to_jsonb(%s::text))"
                    " WHERE id = %s"
                ),
                (title, root),
            )
        cursor = await conn.execute(
            store._sql(
                "INSERT INTO {schema}.coordinator_changes (root_id, kind)"
                " SELECT %s, kind FROM unnest(%s::text[]) WITH ORDINALITY AS k(kind, n) ORDER BY n RETURNING id, kind"
            ),
            (root, kinds),
        )
        return sorted(await cursor.fetchall())


async def settle(runtime: Runtime, root: str, change_id: int) -> None:
    async with asyncio.timeout(5):
        while runtime._trees[root].change_id != change_id:
            await asyncio.sleep(0.01)
    await asyncio.sleep(TREE_INTERVAL * 2)


async def latest_change(runtime: Runtime, root: str) -> int:
    async with runtime.store._connection() as conn:
        cursor = await conn.execute(
            runtime.store._sql("SELECT coalesce(max(id), 0) FROM {schema}.coordinator_changes WHERE root_id = %s"),
            (root,),
        )
        return (await cursor.fetchone())[0]


async def test_journal_changes_neither_page_nor_reread_and_a_buried_header_bumps_once(
    postgres_dsn: str, pg_schema: str
) -> None:
    runtime = await bot_runtime(postgres_dsn, pg_schema)
    root_id = await runtime.create(Bot)
    root = root_id.hex
    counts = tree_counts(runtime)
    tree = runtime.watch_tree(root_id)
    pending: asyncio.Future[TreeChange] | None = None
    try:
        await anext(tree)
        pending = asyncio.ensure_future(anext(tree))
        await settle(runtime, root, await latest_change(runtime, root))
        reads = counts["reads"]
        journal = await publish(runtime, root, ["journal"] * 1500)
        await settle(runtime, root, journal[-1][0])
        assert counts == {"reads": reads, "pages": 0}
        buried = await publish(runtime, root, ["header"] + ["journal"] * 1500, title="Buried")
        assert buried[0][1] == "header" and {kind for _, kind in buried[1:]} == {"journal"}
        assert (await asyncio.wait_for(pending, 5)).title == "Buried"
        pending = asyncio.ensure_future(anext(tree))
        await settle(runtime, root, buried[-1][0])
        assert counts == {"reads": reads + 1, "pages": 0}
        assert runtime._trees[root].tree_change_id == buried[0][0]
        assert not pending.done()
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await tree.aclose()
        await runtime.aclose()
        await runtime.store.close()


async def test_dropped_changes_do_not_bump_and_a_later_header_change_does(postgres_dsn: str, pg_schema: str) -> None:
    runtime = await bot_runtime(postgres_dsn, pg_schema)
    root_id = await runtime.create(Bot)
    root = root_id.hex
    counts = tree_counts(runtime)
    tree = runtime.watch_tree(root_id)
    pending: asyncio.Future[TreeChange] | None = None
    try:
        await anext(tree)
        pending = asyncio.ensure_future(anext(tree))
        await settle(runtime, root, await latest_change(runtime, root))
        first = await publish(runtime, root, ["header"], title="One")
        assert (await asyncio.wait_for(pending, 5)).title == "One"
        pending = asyncio.ensure_future(anext(tree))
        await settle(runtime, root, first[-1][0])
        reads = counts["reads"]
        async with runtime.store._connection() as conn:
            await conn.execute(
                runtime.store._sql("DELETE FROM {schema}.coordinator_changes WHERE root_id = %s"), (root,)
            )
        async with asyncio.timeout(5):
            while runtime._observations[root].change_id != 0:
                await asyncio.sleep(0.01)
        await asyncio.sleep(TREE_INTERVAL * 2)
        assert counts == {"reads": reads, "pages": 0}
        journal = await publish(runtime, root, ["journal"])
        await settle(runtime, root, journal[-1][0])
        assert runtime._observations[root].tree_change_id == 0
        assert runtime._trees[root].tree_change_id == first[0][0]
        assert counts == {"reads": reads, "pages": 0}
        second = await publish(runtime, root, ["header"], title="Two")
        assert (await asyncio.wait_for(pending, 5)).title == "Two"
        pending = asyncio.ensure_future(anext(tree))
        await settle(runtime, root, second[-1][0])
        assert counts == {"reads": reads + 1, "pages": 0}
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await tree.aclose()
        await runtime.aclose()
        await runtime.store.close()


async def test_an_owner_change_rereads_the_tree_exactly_once(postgres_dsn: str, pg_schema: str) -> None:
    runtime = await bot_runtime(postgres_dsn, pg_schema)
    root_id = await runtime.create(Bot)
    root = root_id.hex
    other_store = PostgresStore(postgres_dsn, schema=pg_schema)
    other = PostgresCoordinator(other_store, lease_ttl=30.0, request_timeout=5.0)
    await other.start()
    counts = tree_counts(runtime)
    tree = runtime.watch_tree(root_id)
    pending: asyncio.Future[TreeChange] | None = None
    ownership = None
    try:
        await anext(tree)
        pending = asyncio.ensure_future(anext(tree))
        await settle(runtime, root, await latest_change(runtime, root))
        reads = counts["reads"]
        ownership = await other.acquire(root)
        assert ownership is not None
        await settle(runtime, root, await latest_change(runtime, root))
        assert runtime._observations[root].owner_instance == str(other.instance_id)
        assert counts == {"reads": reads + 1, "pages": 0}
        journal = await publish(runtime, root, ["journal"])
        await settle(runtime, root, journal[-1][0])
        assert counts == {"reads": reads + 1, "pages": 0}
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await tree.aclose()
        if ownership is not None:
            await other.release(ownership)
        await other.close()
        await other_store.close()
        await runtime.aclose()
        await runtime.store.close()


def tree_fixture(base: datetime) -> tuple[list[SessionHeader], list[SessionHeader]]:
    root = SessionHeader(id=uuid4().hex, agent="bot", model="m", created_at=base)
    root.root_id = root.id

    def child(parent: SessionHeader, seconds: int, *, depth: int, **fields: Any) -> SessionHeader:
        fields.setdefault("root_id", root.id)
        return SessionHeader(
            id=uuid4().hex,
            parent_id=parent.id,
            agent="bot",
            depth=depth,
            created_at=base + timedelta(seconds=seconds),
            **fields,
        )

    level1 = [child(root, index // 5, depth=1) for index in range(55)]
    legacy = child(root, 3, depth=0, root_id=None)
    foreign = child(root, 3, depth=1, root_id=uuid4().hex)
    first, second = level1[0], level1[1]
    level2 = [
        child(first, 200, depth=2),
        child(second, 150, depth=2),
        child(first, 100, depth=2),
        child(second, 100, depth=2),
        child(legacy, 100, depth=0, root_id=None),
    ]
    level3 = [child(level2[0], 300, depth=7), child(level2[4], 50, depth=0, root_id=None)]
    other = SessionHeader(id=uuid4().hex, agent="bot", model="m", created_at=base)
    other.root_id = other.id
    stray = child(other, 1, depth=1)
    stray_child = child(stray, 2, depth=2)
    tree = [root, *level1, legacy, foreign, *level2, *level3]
    return tree, [other, stray, stray_child]


def breadth_first(headers: list[SessionHeader]) -> list[str]:
    order = [headers[0].id]
    level = [headers[0].id]
    while level:
        ordered = sorted(headers, key=lambda header: (header.created_at, header.id))
        level = [header.id for header in ordered if header.parent_id in level]
        order.extend(level)
    return order


async def test_the_recursive_tree_read_matches_the_breadth_first_walk(postgres_dsn: str, pg_schema: str) -> None:
    fast = await bot_runtime(postgres_dsn, pg_schema, catch_up_interval=10)
    plain = await bot_runtime(postgres_dsn, pg_schema, Plain, PlainCoordinator, catch_up_interval=10)
    tree, outside = tree_fixture(datetime(2026, 1, 1, tzinfo=UTC))
    root = tree[0].id
    store = fast.store
    try:
        for header in tree + outside:
            await store.create(header)
        async with store._connection() as conn:
            await conn.execute(
                store._sql(
                    "UPDATE {schema}.sessions SET created_at = created_at - interval '1 hour' WHERE id = ANY(%s)"
                ),
                ([header.id for header in tree[10:20]],),
            )
            await conn.execute(
                store._sql(
                    "INSERT INTO {schema}.coordinator_roots (root_id, owner_instance, generation, expires_at)"
                    " VALUES (%s, 'instance', 1, clock_timestamp() + interval '1 hour')"
                ),
                (root,),
            )
            flags = {tree[0].id: True, tree[1].id: False, tree[2].id: True, tree[56].id: True, tree[-2].id: True}
            flags[outside[1].id] = True
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    store._sql(
                        "INSERT INTO {schema}.coordinator_activity (root_id, actor_id, active) VALUES (%s, %s, %s)"
                    ),
                    [(root, actor, active) for actor, active in flags.items()],
                )
        headers = await fast._tree_headers(root)
        assert [header.model_dump() for header in headers] == [
            header.model_dump() for header in await plain._tree_headers(root)
        ]
        assert [header.id for header in headers] == breadth_first(tree + outside)
        assert len(headers) == len(tree) and not {header.id for header in outside} & {header.id for header in headers}
        live = {actor for actor, active in flags.items() if active and actor != outside[1].id}
        for change in (
            None,
            "owner_instance = NULL",
            "owner_instance = 'instance', expires_at = clock_timestamp() - interval '1 second'",
            "DELETE",
        ):
            if change == "DELETE":
                async with store._connection() as conn:
                    await conn.execute(store._sql("DELETE FROM {schema}.coordinator_roots WHERE root_id = %s"), (root,))
            elif change is not None:
                async with store._connection() as conn:
                    await conn.execute(
                        store._sql(f"UPDATE {{schema}}.coordinator_roots SET {change} WHERE root_id = %s"), (root,)
                    )
            expected = live if change is None else set()
            statuses = await fast.tree_status(UUID(hex=root))
            assert statuses == await plain.tree_status(UUID(hex=root))
            assert {status.agent_id.hex for status in statuses if status.active} == expected
            assert await fast.coordinator._active_actors(root) == (live | {outside[1].id} if change is None else set())
            assert {actor for actor in flags if await plain.coordinator.active(root, actor)} == (
                live | {outside[1].id} if change is None else set()
            )
        for target, error in (
            (uuid4().hex, SessionNotFound),
            (tree[1].id, TantraError),
            (tree[56].id, TantraError),
            (tree[57].id, TantraError),
            (outside[1].id, TantraError),
        ):
            raised = []
            for runtime in (fast, plain):
                with pytest.raises(TantraError) as caught:
                    await runtime.tree_status(UUID(hex=target))
                raised.append((type(caught.value), str(caught.value)))
            assert raised[0] == raised[1] and raised[0][0] is error
            assert raised[0][1] == (target if error is SessionNotFound else f"session {target} is not a root")
    finally:
        for runtime in (fast, plain):
            await runtime.aclose()
            await runtime.store.close()
    with pytest.raises(CoordinatorUnavailable):
        await fast.coordinator._active_actors(root)
    with pytest.raises(CoordinatorUnavailable):
        await fast.coordinator.active(root, root)


@pytest.mark.parametrize("width", [2, 40])
async def test_a_tree_snapshot_costs_two_statements_at_any_size(
    postgres_dsn: str, pg_schema: str, width: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = await bot_runtime(postgres_dsn, pg_schema, catch_up_interval=10)
    store = runtime.store
    root = uuid4().hex
    try:
        await store.create(SessionHeader(id=root, root_id=root, agent="bot", model="m"))
        actors, parents = [root], [root]
        for depth in (1, 2):
            level = [
                SessionHeader(
                    id=uuid4().hex, root_id=root, parent_id=parents[index % len(parents)], agent="bot", depth=depth
                )
                for index in range(width)
            ]
            for header in level:
                await store.create(header)
            parents = [header.id for header in level]
            actors.extend(parents)
        async with store._connection() as conn:
            await conn.execute(
                store._sql(
                    "INSERT INTO {schema}.coordinator_roots (root_id, owner_instance, generation, expires_at)"
                    " VALUES (%s, 'instance', 1, clock_timestamp() + interval '1 hour')"
                ),
                (root,),
            )
            async with conn.cursor() as cursor:
                await cursor.executemany(
                    store._sql(
                        "INSERT INTO {schema}.coordinator_activity (root_id, actor_id, active) VALUES (%s, %s, %s)"
                    ),
                    [(root, actor, index % 2 == 0) for index, actor in enumerate(actors)],
                )
        task = asyncio.current_task()
        statements: list[str] = []
        execute = psycopg.AsyncConnection.execute

        async def capture(conn: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
            if asyncio.current_task() is task:
                statements.append(query if isinstance(query, str) else query.as_string(conn))
            return await execute(conn, query, *args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(psycopg.AsyncConnection, "execute", capture)
            snapshot = await runtime._tree_snapshot(root, _Tree())
        assert len(statements) == 2
        assert statements[0].startswith("WITH RECURSIVE tree") and statements[1].startswith("SELECT a.actor_id")
        assert {actor.agent_id.hex for actor in snapshot.actors} == set(actors) and len(actors) == 1 + 2 * width
        assert {actor.agent_id.hex for actor in snapshot.actors if actor.active} == set(actors[::2])
    finally:
        await runtime.aclose()
        await store.close()


async def test_tree_read_errors_propagate_and_activity_errors_are_retried(
    postgres_dsn: str, pg_schema: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = await bot_runtime(postgres_dsn, pg_schema)
    root_id = await runtime.create(Bot)
    failures = {"WITH RECURSIVE tree": 0, "SELECT a.actor_id": 0}
    budget = dict(failures)
    execute = psycopg.AsyncConnection.execute

    async def failing(conn: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        body = query if isinstance(query, str) else query.as_string(conn)
        for prefix, used in failures.items():
            if body.startswith(prefix) and used < budget[prefix]:
                failures[prefix] += 1
                raise psycopg.OperationalError(f"injected failure for {prefix}")
        return await execute(conn, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.AsyncConnection, "execute", failing)
    try:
        budget["WITH RECURSIVE tree"] = 1
        broken = runtime.watch_tree(root_id)
        with pytest.raises(psycopg.OperationalError, match="injected failure for WITH RECURSIVE tree"):
            await asyncio.wait_for(anext(broken), 5)
        await broken.aclose()
        budget["SELECT a.actor_id"] = 2
        retried = runtime.watch_tree(root_id)
        snapshot = await asyncio.wait_for(anext(retried), 5)
        await retried.aclose()
        assert failures == {"WITH RECURSIVE tree": 1, "SELECT a.actor_id": 2}
        assert snapshot == TreeChange(None, "m", tuple(await runtime.tree_status(root_id)))
    finally:
        monkeypatch.undo()
        await runtime.aclose()
        await runtime.store.close()


async def test_the_recursive_tree_read_stops_at_parent_cycles(postgres_dsn: str, pg_schema: str) -> None:
    separator = "&" if "?" in postgres_dsn else "?"
    bounded = f"{postgres_dsn}{separator}options=-c%20statement_timeout%3D2000"
    fast = await bot_runtime(bounded, pg_schema, catch_up_interval=10)
    plain = await bot_runtime(bounded, pg_schema, Plain, PlainCoordinator, catch_up_interval=10)
    root, child, grandchild, first, second, own = (uuid4().hex for _ in range(6))
    store = fast.store
    try:
        for sid, parent in (
            (root, None),
            (child, root),
            (grandchild, child),
            (first, second),
            (second, first),
            (own, own),
        ):
            await store.create(SessionHeader(id=sid, root_id=root, parent_id=parent, agent="bot", model="m"))
        for target, rows in (
            (root, [(0, root), (1, child), (2, grandchild)]),
            (first, [(0, first), (1, second)]),
            (own, [(0, own)]),
        ):
            found = await asyncio.wait_for(store._tree_rows(target), 10)
            assert sorted((depth, header["id"]) for header, _, depth in found) == rows
        assert [header.id for header in await fast._tree_headers(root)] == [root, child, grandchild]
        for target in (first, second, own):
            raised = []
            for runtime in (fast, plain):
                with pytest.raises(TantraError) as caught:
                    await asyncio.wait_for(runtime.tree_status(UUID(hex=target)), 10)
                raised.append((type(caught.value), str(caught.value)))
            assert raised[0] == raised[1] == (TantraError, f"session {target} is not a root")
    finally:
        for runtime in (fast, plain):
            await runtime.aclose()
            await runtime.store.close()
