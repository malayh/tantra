from urllib.parse import quote

import httpx

from tantra import CommandEnvelope, CommandReply, CommandTimeout, PostgresCoordinator, PostgresStore
from tantra.tools import Tool


class E2EPostgresCoordinator(PostgresCoordinator):
    def __init__(self, store: PostgresStore, gate_url: str, *, lease_ttl: float = 60.0) -> None:
        super().__init__(store, lease_ttl=lease_ttl)
        self._e2e_gate_url = gate_url.rstrip("/")

    async def request(self, envelope: CommandEnvelope) -> CommandReply:
        reply = await super().request(envelope)
        command_id = getattr(envelope.payload, "command_id", None)
        if reply.status != "ok" or envelope.operation != "send" or command_id is None:
            return reply
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                response = await client.post(
                    f"{self._e2e_gate_url}/control/command-replies/consume",
                    json={
                        "request_id": str(envelope.request_id),
                        "command_id": str(command_id),
                        "operation": envelope.operation,
                    },
                )
                response.raise_for_status()
                body = response.json()
        except (httpx.HTTPError, TypeError, ValueError):
            return reply
        if isinstance(body, dict) and body.get("drop") is True:
            raise CommandTimeout(str(command_id), command_id=command_id)
        return reply


def e2e_gate(base_url: str) -> Tool:
    async def wait(name: str) -> str:
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.get(f"{base_url.rstrip('/')}/control/gates/{quote(name, safe='')}/wait")
            response.raise_for_status()
            return response.text

    return Tool(
        wait,
        name="e2e_gate",
        description="Wait at a named deterministic E2E gate until the test controller releases it.",
    )
