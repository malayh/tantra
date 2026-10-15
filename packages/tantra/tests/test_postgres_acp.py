from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import pytest
from acp_support import Client, Scripted

from tantra import Agent, PostgresCoordinator, PostgresStore, Runtime
from tantra.acp import AcpAccess, AcpAuth, AcpServer
from tantra.acp.jsonrpc import RpcError
from tantra.events import TurnStarted
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall
from tantra.providers.fake import FAKE_LIMITS, Sample

psycopg = pytest.importorskip("psycopg")


class Bot(Agent):
    pass


class Worker(Agent):
    model = "worker"


class Root(Agent):
    model = "root"
    subagents = [Worker]


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        yield StreamEnd(text="echo")


async def scope(auth: AcpAuth, meta: Any) -> dict[str, str]:
    return {"user": "u"}


async def allow(access: AcpAccess) -> bool:
    return True


async def runtimes(
    dsn: str, schema: str, providers: list[Any], agent: type[Agent] = Bot
) -> list[tuple[PostgresStore, Runtime, AcpServer]]:
    created = []
    for provider in providers:
        store = PostgresStore(dsn, schema=schema)
        coordinator = PostgresCoordinator(store, lease_ttl=2.0, request_timeout=5.0, catch_up_interval=0.02)
        runtime = Runtime(provider, store, [agent], default_model="m", coordinator=coordinator)
        await runtime.start()
        server = AcpServer(runtime, agent=agent, scope=scope, policy=allow, models=["m", "n"], name="pg")
        created.append((store, runtime, server))
    return created


async def shutdown(instances: list[tuple[PostgresStore, Runtime, AcpServer]]) -> None:
    for store, runtime, _ in reversed(instances):
        await runtime.aclose()
        await store.close()


async def test_fencing_across_runtimes(postgres_dsn: str, pg_schema: str) -> None:
    instances = await runtimes(postgres_dsn, pg_schema, [EchoProvider(), EchoProvider()])
    try:
        async with Client(instances[0][2]) as first, Client(instances[1][2]) as second:
            session_id = await first.new()
            await first.prompt(session_id, "one")
            await first.idle(session_id)
            await second.resume(session_id)
            await second.prompt(session_id, "two")
            await first.idle(session_id, 2)
            with pytest.raises(RpcError) as fenced:
                await first.prompt(session_id, "three")
            await first.prompt(session_id, "three")
            await first.idle(session_id, 3)
            with pytest.raises(RpcError) as reclaimed:
                await second.prompt(session_id, "four")
        store = instances[0][0]
        inputs = [
            item.event.input async for item in store.read(UUID(session_id).hex) if isinstance(item.event, TurnStarted)
        ]
    finally:
        await shutdown(instances)
    assert fenced.value.data == reclaimed.value.data == {"tantra": {"code": "writer_required"}}
    assert inputs == ["one", "two", "three"]


async def test_a_prompt_sent_on_one_runtime_is_observed_on_the_other(postgres_dsn: str, pg_schema: str) -> None:
    instances = await runtimes(postgres_dsn, pg_schema, [EchoProvider(), EchoProvider()])
    try:
        async with Client(instances[0][2]) as writer, Client(instances[1][2]) as reader:
            session_id = await writer.new()
            await reader.resume(session_id)
            accepted = await writer.prompt(session_id, "hello")
            await reader.idle(session_id)
    finally:
        await shutdown(instances)
    [message] = reader.of(session_id, "user_message")
    assert message == {
        "sessionUpdate": "user_message",
        "messageId": accepted["messageId"],
        "content": [{"type": "text", "text": "hello"}],
    }
    assert [update["content"] for update in reader.of(session_id, "agent_message")] == [
        [{"type": "text", "text": "echo"}]
    ]
    assert reader.states(session_id)[-1] == {"state": "idle", "stopReason": "end_turn"}


async def test_child_states_are_mirrored_to_a_reader_on_another_runtime(postgres_dsn: str, pg_schema: str) -> None:
    def script() -> Scripted:
        spawn = ToolCall(id="s1", name="spawn", args=json.dumps({"agent_name": "worker", "input": "dig"}))
        return Scripted(
            root=[Sample(tool_calls=[spawn]), Sample(text="spawned"), Sample(text="noted")],
            worker=[Sample(text="dug")],
        )

    instances = await runtimes(postgres_dsn, pg_schema, [script(), script()], Root)
    try:
        async with Client(instances[0][2]) as writer, Client(instances[1][2]) as reader:
            session_id = await writer.new()
            await reader.resume(session_id)
            await writer.prompt(session_id, "go")
            await reader.idle(session_id, 2)

            def worker_idle() -> Any:
                return [
                    update
                    for update in reader.of(session_id, "subagent_update")
                    if update.get("state") == {"state": "idle", "stopReason": "end_turn"}
                ]

            await reader.until(worker_idle)
            [worker] = list(dict.fromkeys(update["sessionId"] for update in reader.of(session_id, "subagent_update")))
    finally:
        await shutdown(instances)
    assert reader.of(worker, "agent_message")[-1]["content"] == [{"type": "text", "text": "dug"}]
    assert reader.of(session_id, "subagent_update")[0]["title"] == "worker"


async def test_title_and_model_changes_reach_a_session_on_another_runtime(postgres_dsn: str, pg_schema: str) -> None:
    instances = await runtimes(postgres_dsn, pg_schema, [EchoProvider(), EchoProvider()])
    try:
        async with Client(instances[0][2]) as writer, Client(instances[1][2]) as reader:
            session_id = await writer.new()
            await reader.resume(session_id)
            await reader.until(lambda: reader.of(session_id, "state_update"))
            await instances[0][0].patch_header(UUID(session_id).hex, title="Renamed")
            await reader.until(
                lambda: [u for u in reader.of(session_id, "session_info_update") if u.get("title") == "Renamed"]
            )
            await writer.call(
                "session/set_config_option",
                {"sessionId": session_id, "configId": "model", "type": "id", "value": "n"},
            )
            update = await reader.until(lambda: reader.of(session_id, "config_option_update"))
    finally:
        await shutdown(instances)
    assert update[-1]["configOptions"][0]["currentValue"] == "n"
