from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from sarathi.e2e import E2EPostgresCoordinator
from tantra import CommandEnvelope, CommandReply, CommandTimeout, PostgresCoordinator, SendPayload, WriterToken


class GateResponse:
    def __init__(self, drop: bool) -> None:
        self.drop = drop

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, bool]:
        return {"drop": self.drop}


class GateClient:
    def __init__(self, drops: list[bool], probes: list[dict]) -> None:
        self.drops = drops
        self.probes = probes

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args) -> None:
        return None

    async def post(self, url: str, *, json: dict) -> GateResponse:
        self.probes.append({"url": url, **json})
        return GateResponse(self.drops.pop(0))


async def test_reply_drop_happens_after_commit_and_same_uuid_retry_adds_one_input(monkeypatch) -> None:
    accepted = []
    probes = []
    drops = [True, False]

    async def committed_request(_coordinator, envelope):
        command_id = envelope.payload.command_id
        duplicate = command_id in accepted
        if not duplicate:
            accepted.append(command_id)
        return CommandReply(request_id=envelope.request_id, result={"duplicate": duplicate})

    monkeypatch.setattr(PostgresCoordinator, "request", committed_request)
    monkeypatch.setattr(
        "sarathi.e2e.httpx.AsyncClient",
        lambda **_kwargs: GateClient(drops, probes),
    )
    coordinator = object.__new__(E2EPostgresCoordinator)
    coordinator._e2e_gate_url = "http://gate:8090"
    root_id = uuid4().hex
    command_id = uuid4()
    token = WriterToken(root_id=root_id, connection_id=uuid4())

    def envelope() -> CommandEnvelope:
        return CommandEnvelope(
            request_id=uuid4(),
            root_id=root_id,
            operation="send",
            writer_token=token,
            payload=SendPayload(command_id=command_id, input="once"),
            deadline=datetime.now(UTC) + timedelta(seconds=2),
        )

    first = envelope()
    with pytest.raises(CommandTimeout) as raised:
        await coordinator.request(first)
    second = envelope()
    reply = await coordinator.request(second)

    assert raised.value.command_id == command_id
    assert reply.result == {"duplicate": True}
    assert accepted == [command_id]
    assert [probe["command_id"] for probe in probes] == [str(command_id), str(command_id)]
    assert probes[0]["request_id"] != probes[1]["request_id"]
