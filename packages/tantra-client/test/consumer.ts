import {
  SessionClient,
  SessionClientError,
  type JsonValue,
  type Persistence,
  type PersistedState,
  type ReceiptFrame,
} from "tantra-session-client";

interface Input {
  message: string;
}
interface Event {
  kind: string;
}

const persistence: Persistence = {
  async load(): Promise<unknown> {
    return null;
  },
  async save(state: PersistedState): Promise<void> {
    void state.version;
  },
};

const client = new SessionClient<Input, Event>({
  serverScope: "https://api.example",
  rootId: "12345678-1234-1234-1234-123456789abc",
  identity: "user-1",
  url: async () =>
    "wss://api.example/sessions/12345678-1234-1234-1234-123456789abc/socket?ticket=fresh",
  onEvent: async (event) => {
    const body: Event | null = event.body;
    void body;
  },
  onReceipt: (receipt: ReceiptFrame) => {
    void receipt.command_id;
  },
  onNotice: (body: JsonValue) => {
    void body;
  },
  onError: (error: SessionClientError) => {
    void error.code;
  },
  persistence,
  initialCursors: { "12345678-1234-1234-1234-123456789abc": 0 },
});

const subscription = client.subscribe("12345678-1234-1234-1234-123456789abc");
void subscription.state;
const command = client.send({ message: "hello" });
const commandId: string = command.commandId;
void commandId;
