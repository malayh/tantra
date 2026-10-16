from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import uuid4

import pytest
from acp_support import Scripted

from tantra import Agent, ModelChangeBusy, PostgresCoordinator, PostgresStore, Runtime, SessionNotFound, TreeChange
from tantra.providers.base import ProviderEvent, SampleRequest, ToolCall
from tantra.providers.fake import Sample
from tantra.stores import postgres

psycopg = pytest.importorskip("psycopg")


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    subagents = [Worker]


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
    finish = ToolCall(id="f1", name="finish", args=json.dumps({"result": "found"}))
    return Held(
        release,
        root=[Sample(tool_calls=[spawn]), Sample(text="spawned"), Sample(text="noted")],
        worker=[Sample(tool_calls=[finish])],
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

            def finished(change: TreeChange) -> bool:
                return (
                    len(change.actors) == 2
                    and change.actors[1].state == "finished"
                    and change.actors[0].state == "idle"
                    and change.actors[0].last_turn is not None
                    and not any(actor.active for actor in change.actors)
                )

            await until(tree, seen, finished)
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
    assert states.index("running") < states.index("finished")
    assert all(left != right for left, right in zip(seen, seen[1:], strict=False))
