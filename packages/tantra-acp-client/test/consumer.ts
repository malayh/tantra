import {
  TantraAcpClient,
  TantraAcpError,
  cursorOf,
  type AgentRequest,
  type ClientState,
  type PromptPromise,
  type SessionUpdate,
} from "tantra-acp-client";

const client = new TantraAcpClient({
  url: async () => "wss://api.example/api/acp?ticket=fresh",
  cwd: "/work",
  onUpdate: async (sessionId: string, update: SessionUpdate) => {
    if (update.sessionUpdate === "agent_message_chunk") void update.content;
    const cursor: string | undefined = cursorOf(update);
    void [sessionId, cursor];
  },
  onRequest: (request: AgentRequest) => {
    if (request.method === "session/request_permission")
      request.respond({ outcome: { outcome: "selected", optionId: "allow" } });
    else request.respond({ action: "accept", content: { text: "yes" } });
  },
  onState: (state: ClientState) => {
    const session = state.sessions["12345678-1234-1234-1234-123456789abc"];
    void [session?.cursor, session?.writer, session?.paused];
  },
  onError: (error: TantraAcpError) => {
    void [error.code, error.commandId];
  },
});

async function main(): Promise<void> {
  await client.connect();
  const { sessionId } = await client.newSession();
  const prompt: PromptPromise = client.prompt(sessionId, [
    { type: "text", text: "hello" },
  ]);
  const commandId: string = prompt.commandId;
  const { messageId } = await prompt;
  void [commandId, messageId];
  await client.resume(sessionId, { mode: "write" });
  await client.claimWriter(sessionId);
  await client.cancel(sessionId);
  await client.setModel(sessionId, "gpt-5");
  const { sessions, nextCursor } = await client.listSessions();
  void [sessions, nextCursor];
  await client.close(sessionId);
  await client.delete(sessionId);
  await client.disconnect();
}

void main;
