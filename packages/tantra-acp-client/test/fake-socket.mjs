import { setTimeout as delay } from "node:timers/promises";

import { TantraAcpClient } from "../dist/index.js";

export const until = async (predicate, label = "condition") => {
  for (let attempt = 0; attempt < 500; attempt++) {
    const value = await predicate();
    if (value) return value;
    await delay(2);
  }
  throw new Error(`timed out waiting for ${label}`);
};

export const settled = () => new Promise((resolve) => setImmediate(resolve));

export const cursored = (cursor, messageId = "m") => ({
  sessionUpdate: "agent_message",
  messageId,
  content: [{ type: "text", text: cursor }],
  _meta: { tantra: { cursor } },
});

const closeEvent = (code) => {
  const event = new Event("close");
  Object.defineProperty(event, "code", { value: code });
  return event;
};

export class FakeSocket extends EventTarget {
  readyState = 0;
  sent = [];
  closes = [];

  constructor(url, agent) {
    super();
    this.url = url;
    this.agent = agent;
  }

  send(text) {
    if (this.readyState !== 1) throw new Error("socket is not open");
    const frame = JSON.parse(text);
    this.sent.push(frame);
    this.agent.handle(this, frame);
  }

  close(code) {
    this.closes.push(code);
    if (this.readyState >= 2) return;
    this.readyState = 3;
    setImmediate(() => this.dispatchEvent(closeEvent(code ?? 1005)));
  }

  accept() {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  push(frame) {
    const data = typeof frame === "string" ? frame : JSON.stringify(frame);
    this.dispatchEvent(new MessageEvent("message", { data }));
  }

  drop(code = 1011) {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    this.dispatchEvent(closeEvent(code));
  }

  methods(method) {
    return this.sent.filter((frame) => frame.method === method);
  }
}

export const INITIALIZED = {
  protocolVersion: 2,
  capabilities: {
    session: { prompt: {}, mcp: {}, delete: {} },
    _meta: { tantra: { version: 1 } },
  },
  info: { name: "fake", version: "0" },
  authMethods: [],
};

export class FakeAgent {
  sockets = [];
  urls = [];
  handlers = {};
  refuse = 0;
  sessions = 0;

  factory = (url) => {
    const socket = new FakeSocket(url, this);
    this.sockets.push(socket);
    setImmediate(() => {
      if (this.refuse > 0) {
        this.refuse--;
        socket.drop(1006);
      } else socket.accept();
    });
    return socket;
  };

  get socket() {
    return this.sockets.at(-1);
  }

  sent(method) {
    return this.sockets.flatMap((socket) => socket.methods(method));
  }

  handle(socket, frame) {
    if (!frame.method) return;
    const handler = this.handlers[frame.method] ?? DEFAULTS[frame.method];
    handler?.call(this, socket, frame);
  }

  later(socket, frame) {
    setImmediate(() => socket.readyState === 1 && socket.push(frame));
  }

  reply(socket, request, result) {
    this.later(socket, { jsonrpc: "2.0", id: request.id, result });
  }

  fail(socket, request, code, message, data) {
    const error =
      data === undefined ? { code, message } : { code, message, data };
    this.later(socket, { jsonrpc: "2.0", id: request.id, error });
  }

  update(socket, sessionId, update) {
    this.later(socket, {
      jsonrpc: "2.0",
      method: "session/update",
      params: { sessionId, update },
    });
  }

  writer(socket, sessionId, reason) {
    this.update(socket, sessionId, {
      sessionUpdate: "_tantra_writer",
      writer: reason === "claimed",
      reason,
    });
  }

  client(options = {}) {
    return new TantraAcpClient({
      url: () => {
        this.urls.push(this.urls.length);
        return "ws://fake";
      },
      onUpdate: () => {},
      webSocketFactory: (url) => this.factory(url),
      timing: { reconnectBaseMs: 1, reconnectCapMs: 4 },
      ...options,
    });
  }
}

const DEFAULTS = {
  initialize(socket, frame) {
    this.reply(socket, frame, INITIALIZED);
  },
  "session/new"(socket, frame) {
    const sessionId = `00000000-0000-4000-8000-${String(++this.sessions).padStart(12, "0")}`;
    this.reply(socket, frame, { sessionId });
    this.writer(socket, sessionId, "claimed");
  },
  "session/resume"(socket, frame) {
    this.reply(socket, frame, {});
    if (frame.params._meta?.tantra?.mode === "write")
      this.writer(socket, frame.params.sessionId, "claimed");
  },
  "session/prompt"(socket, frame) {
    this.reply(socket, frame, {
      messageId: frame.params._meta.tantra.commandId,
    });
  },
  "_tantra/session/claim_writer"(socket, frame) {
    this.reply(socket, frame, {});
    this.writer(socket, frame.params.sessionId, "claimed");
  },
  "session/close"(socket, frame) {
    this.reply(socket, frame, {});
  },
  "session/delete"(socket, frame) {
    this.reply(socket, frame, {});
  },
  "session/list"(socket, frame) {
    this.reply(socket, frame, { sessions: [] });
  },
};
