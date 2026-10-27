<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/malayh/tantra@main/assets/logo.svg" width="140" alt="tantra logo">
</p>

# tantra

**Agents that survive the crash.**

A durable actor runtime for Python agents. Every agent gets an inbox, a journal and one turn at a time. If a worker dies mid-turn, the journal survives and nothing runs twice; with PostgreSQL, another worker can take over its queue. Drop the connection and the client resumes from its cursor. Serve it to any ACP client.

[Website](https://malayh.github.io/tantra/) · [Documentation](https://malayh.github.io/tantra/docs/) · [Upgrade to 2.0](https://malayh.github.io/tantra/docs/guides/migration-2.0/)

Install name `tantra-harness`, import name `tantra`. A process-wide `Runtime` owns providers, stores, tools, hooks, permissions, skills, memory, compaction, telemetry, actor tasks, and writer connections.

## Install

| Command | Adds |
|---|---|
| `pip install tantra-harness` | Core runtime and shell tools |
| `pip install "tantra-harness[web]"` | Brave web search and web fetch |
| `pip install "tantra-harness[doc]"` | PDF and Word document reading |
| `pip install "tantra-harness[postgres]"` | PostgreSQL storage |
| `pip install "tantra-harness[telemetry]"` | OpenTelemetry tracing |
| `npm install tantra-acp-client` | TypeScript ACP client |

The unrelated PyPI project `tantra` installs the same import name. Do not install both projects in one environment.

## Basic usage

```python
import asyncio
from uuid import uuid4

from tantra import Agent, OpenAICompatible, Runtime, SQLiteStore
from tantra.extratools.web import web_search


class Researcher(Agent):
    model = "gpt-5"
    prompt = "Answer with current, sourced information."
    tools = [web_search(api_key=BRAVE_API_KEY)]


async def main() -> None:
    store = SQLiteStore("sessions.db")
    await store.setup()
    runtime = Runtime(
        OpenAICompatible("https://api.openai.com/v1", OPENAI_API_KEY),
        store,
        [Researcher],
    )
    root_id = await runtime.create(Researcher)
    async with runtime.connect(root_id, writable=True) as connection:
        result = await connection.prompt(
            "What changed in Python 3.13?",
            command_id=uuid4(),
        )
    print(result.text)
    await runtime.aclose()


asyncio.run(main())
```

`Connection.prompt()` accepts a durable command and waits for its terminal result. For streaming, call `send()` and iterate the connection. Reconnect with the last scalar `seq` as `after`; use `Runtime.events()` for a child journal. Command IDs are UUIDs and make acceptance idempotent.

Subagents are independent actors. Models use the injected `spawn`, `status`, `send`, and `cancel_agent` tools; no parent stream merges child events. Each completed child turn delivers its final message to the parent as a reply, and the child stays reusable. Human asks and approval-gated tools are root-only.

Compaction is opt-in with `Runtime(compactor=PruneThenSummarize())`. Configured runtimes budget the complete provider request, preserve system and tool definitions, and compact each actor independently.

For multi-process deployments, pair `PostgresStore` with the optional `PostgresCoordinator` for leased execution ownership, fenced writes, command forwarding, and takeover recovery. The default remains single-process.

## Serve it over ACP

`tantra.acp` serves a Runtime over an ACP v2–inspired protocol plus the `tantra` profile, on stdio or WebSocket. `AcpServer.websocket()` returns a raw ASGI endpoint; this example mounts it in Starlette and runs with `pip install starlette "uvicorn[standard]"` and `uvicorn app:app`. It reuses `Researcher` and `OPENAI_API_KEY` from above. `authenticate(asgi_scope)` stands for the application's own check of the WebSocket handshake; when `bind` raises `PermissionError`, the socket closes with 1008 before it is accepted.

```python
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.routing import WebSocketRoute

from tantra import OpenAICompatible, Runtime, SQLiteStore
from tantra.acp import AcpAccess, AcpAuth, AcpServer


async def bind(asgi_scope) -> AcpAuth:
    user = await authenticate(asgi_scope)
    if user is None:
        raise PermissionError
    return AcpAuth(principal=user.id, submitted_by=user.id)


async def scope(auth: AcpAuth, meta) -> dict[str, str]:
    return {"user": auth.principal}


async def policy(access: AcpAccess) -> bool:
    return access.root.metadata.get("user") == access.auth.principal


@asynccontextmanager
async def lifespan(app: Starlette):
    store = SQLiteStore("sessions.db")
    await store.setup()
    runtime = Runtime(
        OpenAICompatible("https://api.openai.com/v1", OPENAI_API_KEY),
        store,
        [Researcher],
    )
    await runtime.start()
    server = AcpServer(runtime, agent=Researcher, scope=scope, policy=policy, name="researcher")
    endpoint = server.websocket(bind=bind, allowed_origins={"https://app.example"})
    route = WebSocketRoute("/acp", endpoint)
    app.router.routes.append(route)
    try:
        yield
    finally:
        app.router.routes.remove(route)
        await endpoint.aclose()
        await runtime.aclose()


app = Starlette(lifespan=lifespan)
```

`tantra-acp-client` is the TypeScript client. It owns cursors, command IDs, writer state and reconnects:

```ts
import { TantraAcpClient } from "tantra-acp-client";

const client = new TantraAcpClient({
  url: async () => `wss://app.example/acp?ticket=${await ticket()}`,
  onUpdate: (sessionId, update) => reducer.apply(sessionId, update),
  onRequest: (request) => asks.show(request),
});

await client.connect();
const { sessionId } = await client.newSession();
const { messageId } = await client.prompt(sessionId, [{ type: "text", text: "What changed in Python 3.13?" }]);
```

The `tantra` profile adds what durable, shared sessions need:

- **Cursors.** Every journal-derived update carries `_meta.tantra.cursor`, and a reconnecting client resumes each session after its last applied cursor.
- **Command IDs.** A prompt's `commandId` becomes the Runtime command ID, so a retry is accepted once and answers the same `messageId`.
- **One fenced writer per root.** Many connections can read a session; prompts, cancels and asks go through one writer, and a new writer fences the old one.
- **Lazy child transcripts.** Subagent state arrives through a mirror; a client resumes a child read-only only when it needs the transcript.

See the [documentation](https://malayh.github.io/tantra/docs/), the [2.0 upgrade guide](https://malayh.github.io/tantra/docs/guides/migration-2.0/), the [ACP server reference](https://malayh.github.io/tantra/docs/reference/acp/), and the [coordinator setup guide](https://malayh.github.io/tantra/docs/guides/migration-1.2/).

## Reference app

`apps/sarathi` is the repository's interactive web application.

## License

Apache-2.0
