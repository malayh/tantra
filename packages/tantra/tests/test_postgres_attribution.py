from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest

from tantra import (
    Agent,
    CommandReceipt,
    CommandTimeout,
    FreeText,
    FreeTextResponse,
    InvalidCommandReuse,
    PostgresCoordinator,
    PostgresStore,
    Runtime,
    Sample,
    tool,
)
from tantra.context import TurnContext
from tantra.events import (
    AskAnswered,
    AskRaised,
    CancellationRequested,
    InputQueued,
    SessionEvent,
    TurnCancelled,
    TurnCompleted,
    TurnStarted,
)
from tantra.hooks import Hook
from tantra.providers.base import ModelLimits, ProviderEvent, SampleRequest, StreamEnd, ToolCall, UserMessage
from tantra.providers.fake import FAKE_LIMITS, FakeProvider
from tantra.tools import Context

pytest.importorskip("psycopg")


class Bot(Agent):
    pass


class EchoProvider:
    def limits(self, model: str) -> ModelLimits:
        return FAKE_LIMITS

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        value = next(message.content for message in reversed(req.messages) if isinstance(message, UserMessage))
        yield StreamEnd(text=value)


class GateProvider(EchoProvider):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        self.started.set()
        await self.release.wait()
        async for event in super().stream(req):
            yield event


def coordinator(store: PostgresStore, *, lease_ttl: float = 0.6) -> PostgresCoordinator:
    return PostgresCoordinator(store, lease_ttl=lease_ttl, request_timeout=2.0, catch_up_interval=0.02)


async def events(store: PostgresStore, root_id: UUID) -> list[SessionEvent]:
    return [item.event async for item in store.read(root_id.hex)]


async def wait_for_event(
    store: PostgresStore,
    root_id: UUID,
    predicate: Any,
) -> SessionEvent:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        for event in await events(store, root_id):
            if predicate(event):
                return event
        await asyncio.sleep(0.01)
    raise AssertionError("event did not become durable")


async def close_runtime(runtime: Runtime | None, store: PostgresStore | None) -> None:
    if runtime is not None:
        await runtime.aclose()
    if store is not None:
        await store.close()


async def test_remote_prompt_persists_attribution_and_propagates_context(
    postgres_dsn: str,
    pg_schema: str,
) -> None:
    seen: list[tuple[str, str | None]] = []

    class Capture(Hook):
        async def before_turn(self, turn: TurnContext) -> None:
            seen.append((turn.input, turn.submitted_by))

    provider = GateProvider()
    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        provider,
        owner_store,
        [Bot],
        default_model="m",
        hooks=[Capture()],
        coordinator=coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    first_id = uuid4()
    prompt_id = uuid4()
    requester_store: PostgresStore | None = None
    requester: Runtime | None = None
    try:
        async with owner.connect(root_id, writable=True) as connection:
            await connection.send("hold", command_id=first_id, submitted_by="owner-user")
        await asyncio.wait_for(provider.started.wait(), 1)
        requester_store = PostgresStore(postgres_dsn, schema=pg_schema)
        requester = Runtime(
            EchoProvider(),
            requester_store,
            [Bot],
            default_model="m",
            coordinator=coordinator(requester_store),
        )
        await requester.start()
        async with requester.connect(root_id, writable=True) as connection:
            pending = asyncio.create_task(
                connection.prompt("queued", command_id=prompt_id, submitted_by="requester-user")
            )
            queued = await wait_for_event(
                requester_store,
                root_id,
                lambda event: isinstance(event, InputQueued) and event.command_id == prompt_id.hex,
            )
            assert isinstance(queued, InputQueued)
            assert queued.submitted_by == "requester-user"
            provider.release.set()
            result = await asyncio.wait_for(pending, 2)
            assert result.text == "queued"
            assert await connection.send(
                "queued",
                command_id=prompt_id,
                submitted_by="requester-user",
            ) == CommandReceipt(prompt_id, True)
            with pytest.raises(InvalidCommandReuse):
                await connection.send("queued", command_id=prompt_id, submitted_by="other-user")

        assert seen == [("hold", "owner-user"), ("queued", "requester-user")]
        queued_events = [
            event
            for event in await events(requester_store, root_id)
            if isinstance(event, InputQueued) and event.command_id == prompt_id.hex
        ]
        assert queued_events == [queued]
    finally:
        provider.release.set()
        await close_runtime(requester, requester_store)
        await close_runtime(owner, owner_store)


async def test_remote_answer_after_expiry_preserves_submitter_and_root_answerer(
    postgres_dsn: str,
    pg_schema: str,
) -> None:
    @tool
    async def interview(ctx: Context) -> str:
        response = await ctx.ask(FreeText(prompt="name?"))
        return response.text

    class Asker(Agent):
        tools = [interview]

    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        FakeProvider(
            [
                Sample(tool_calls=[ToolCall(id="ask", name="interview", args="{}")]),
                Sample(text="done"),
            ]
        ),
        owner_store,
        [Asker],
        default_model="m",
        coordinator=coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Asker)
    turn_id = uuid4()
    answer_id = uuid4()
    requester_store: PostgresStore | None = None
    requester: Runtime | None = None
    try:
        async with owner.connect(root_id, writable=True) as connection:
            await connection.send("go", command_id=turn_id, submitted_by="requester")
        raised = await wait_for_event(owner_store, root_id, lambda event: isinstance(event, AskRaised))
        assert isinstance(raised, AskRaised)
        requester_store = PostgresStore(postgres_dsn, schema=pg_schema)
        requester = Runtime(
            EchoProvider(),
            requester_store,
            [Asker],
            default_model="m",
            coordinator=coordinator(requester_store),
        )
        await requester.start()
        async with requester.connect(root_id, writable=True) as connection:
            assert await connection.answer(
                UUID(hex=raised.ask_id),
                FreeTextResponse(text="Malay"),
                command_id=answer_id,
                submitted_by="reviewer",
            ) == CommandReceipt(answer_id, False)
            result = await requester._wait_result(root_id.hex, turn_id)
            assert result.text == "done"
            assert await connection.answer(
                UUID(hex=raised.ask_id),
                FreeTextResponse(text="Malay"),
                command_id=answer_id,
                submitted_by="reviewer",
            ) == CommandReceipt(answer_id, True)
            with pytest.raises(InvalidCommandReuse):
                await connection.answer(
                    UUID(hex=raised.ask_id),
                    FreeTextResponse(text="Malay"),
                    command_id=answer_id,
                    submitted_by="other-reviewer",
                )

        answered = [event for event in await events(requester_store, root_id) if isinstance(event, AskAnswered)]
        assert len(answered) == 1
        assert answered[0].submitted_by == "reviewer"
        assert answered[0].answered_by == root_id.hex
    finally:
        await close_runtime(requester, requester_store)
        await close_runtime(owner, owner_store)


async def test_lost_cancel_reply_retries_frozen_targets_and_spares_later_work(
    postgres_dsn: str,
    pg_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = GateProvider()
    owner_store = PostgresStore(postgres_dsn, schema=pg_schema)
    owner = Runtime(
        provider,
        owner_store,
        [Bot],
        default_model="m",
        coordinator=coordinator(owner_store),
    )
    await owner.start()
    root_id = await owner.create(Bot)
    first_id = uuid4()
    later_id = uuid4()
    cancel_id = uuid4()
    requester_store: PostgresStore | None = None
    requester: Runtime | None = None
    try:
        async with owner.connect(root_id, writable=True) as connection:
            await connection.send("first", command_id=first_id)
        await asyncio.wait_for(provider.started.wait(), 1)
        requester_store = PostgresStore(postgres_dsn, schema=pg_schema)
        requester = Runtime(
            EchoProvider(),
            requester_store,
            [Bot],
            default_model="m",
            coordinator=coordinator(requester_store),
        )
        await requester.start()
        async with requester.connect(root_id, writable=True) as connection:
            assert requester.coordinator is not None
            original_request = requester.coordinator.request
            lost = False

            async def lose_committed_reply(envelope: Any) -> Any:
                nonlocal lost
                reply = await original_request(envelope)
                if envelope.operation == "cancel" and not lost:
                    lost = True
                    raise CommandTimeout("injected response loss")
                return reply

            monkeypatch.setattr(requester.coordinator, "request", lose_committed_reply)
            with pytest.raises(CommandTimeout):
                await connection.cancel(command_id=cancel_id, submitted_by="operator")
            monkeypatch.setattr(requester.coordinator, "request", original_request)
            cancellation = await wait_for_event(
                requester_store,
                root_id,
                lambda event: isinstance(event, CancellationRequested),
            )
            assert isinstance(cancellation, CancellationRequested)
            assert cancellation.submitted_by == "operator"
            assert cancellation.targets == {root_id.hex: [first_id.hex]}

            await connection.send("later", command_id=later_id, submitted_by="later-user")
            await wait_for_event(
                requester_store,
                root_id,
                lambda event: isinstance(event, TurnStarted) and event.turn_id == later_id.hex,
            )
            assert await connection.cancel(
                command_id=cancel_id,
                submitted_by="operator",
            ) == CommandReceipt(cancel_id, True)
            with pytest.raises(InvalidCommandReuse):
                await connection.cancel(command_id=cancel_id, submitted_by="other-operator")
            current = await events(requester_store, root_id)
            cancellations = [event for event in current if isinstance(event, CancellationRequested)]
            assert cancellations == [cancellation]
            assert sum(isinstance(event, TurnCancelled) and event.turn_id == first_id.hex for event in current) == 1
            assert not any(isinstance(event, TurnCancelled) and event.turn_id == later_id.hex for event in current)
            provider.release.set()
            completed = await wait_for_event(
                requester_store,
                root_id,
                lambda event: isinstance(event, TurnCompleted) and event.turn_id == later_id.hex,
            )
            assert isinstance(completed, TurnCompleted)
    finally:
        provider.release.set()
        await close_runtime(requester, requester_store)
        await close_runtime(owner, owner_store)


async def test_takeover_executes_accepted_unstarted_input_with_original_attribution(
    postgres_dsn: str,
    pg_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_store = PostgresStore(postgres_dsn, schema=pg_schema)
    old_coordinator = coordinator(old_store, lease_ttl=0.12)
    old = Runtime(EchoProvider(), old_store, [Bot], default_model="m", coordinator=old_coordinator)
    await old.start()
    root_id = await old.create(Bot)
    command_id = uuid4()
    original_activate = old._activate
    monkeypatch.setattr(old, "_activate", lambda agent_id, actor_root_id: None)
    replacement_store: PostgresStore | None = None
    replacement: Runtime | None = None
    seen: list[str | None] = []

    class Capture(Hook):
        async def before_turn(self, turn: TurnContext) -> None:
            seen.append(turn.submitted_by)

    try:
        async with old.connect(root_id, writable=True) as connection:
            assert await connection.send(
                "accepted",
                command_id=command_id,
                submitted_by="origin-user",
            ) == CommandReceipt(command_id, False)
        queued = await events(old_store, root_id)
        assert not any(isinstance(event, TurnStarted) for event in queued)
        await old_coordinator.close()
        await asyncio.sleep(0.15)
        replacement_store = PostgresStore(postgres_dsn, schema=pg_schema)
        replacement = Runtime(
            EchoProvider(),
            replacement_store,
            [Bot],
            default_model="m",
            hooks=[Capture()],
            coordinator=coordinator(replacement_store, lease_ttl=0.12),
        )
        await replacement.start()

        async with replacement.connect(root_id, writable=True) as connection:
            result = await connection.prompt(
                "accepted",
                command_id=command_id,
                submitted_by="origin-user",
            )

        assert result.text == "accepted"
        assert seen == ["origin-user"]
        recovered = await events(replacement_store, root_id)
        accepted = [
            event for event in recovered if isinstance(event, InputQueued) and event.command_id == command_id.hex
        ]
        assert len(accepted) == 1
        assert accepted[0].submitted_by == "origin-user"
    finally:
        monkeypatch.setattr(old, "_activate", original_activate)
        await close_runtime(replacement, replacement_store)
        await close_runtime(old, old_store)
