import {
  TantraAcpClient,
  TantraAcpError,
  addRequest,
  apply,
  askOf,
  cursorOf,
  descendants,
  empty,
  pendingRequest,
  removeRequest,
  reset,
  treeRunning,
  type AgentRequest,
  type Ask,
  type ClientState,
  type Entry,
  type PromptPromise,
  type RosterEntry,
  type SessionUpdate,
  type SessionView,
  type Transcript,
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

function render(transcript: Transcript, root: string): void {
  const view: SessionView | undefined = transcript.sessions[root];
  for (const entry of view?.entries ?? []) {
    const shown: Entry = entry;
    if (shown.kind === "tool") void [shown.progress, shown.rawOutput];
    else if (shown.kind === "message") void [shown.sender, shown.finished];
  }
  const children: RosterEntry[] = descendants(transcript, root).flatMap(
    (id) => transcript.roster[id] ?? [],
  );
  const request = pendingRequest(transcript, root);
  if (request) {
    const ask: Ask = askOf(request);
    if (ask.kind === "approval") ask.respond(true);
    else if (ask.kind === "choice") ask.respond(ask.options[0] ?? "");
    else ask.respond("yes");
  }
  void [children, treeRunning(transcript, root), view?.state?.ask?.askId];
}

const track = (transcript: Transcript, request: AgentRequest): Transcript =>
  addRequest(transcript, request);
const transcript: Transcript = apply(empty(), "s", {
  sessionUpdate: "agent_message_chunk",
  messageId: "m",
  content: { type: "text", text: "hi" },
});
render(removeRequest(reset(transcript, "s"), "s", "a"), "s");
void track;
