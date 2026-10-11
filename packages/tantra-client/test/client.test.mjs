import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { SessionClient } from "../dist/index.js";

const root = "00000000-0000-0000-0000-000000000001";
const child = "00000000-0000-0000-0000-000000000002";
const ask = "00000000-0000-0000-0000-000000000003";
const until = async (predicate) => {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return;
    await delay(2);
  }
  throw new Error("condition timed out");
};
const gate = () => {
  let resolve;
  const promise = new Promise((yes) => {
    resolve = yes;
  });
  return { promise, resolve };
};

class Socket extends EventTarget {
  readyState = 0;
  protocol = "tantra.session.v1";
  bufferedAmount = 0;
  frames = [];
  listeners = 0;
  constructor(auto = true) {
    super();
    this.auto = auto;
  }
  addEventListener(...args) {
    this.listeners++;
    super.addEventListener(...args);
  }
  removeEventListener(...args) {
    this.listeners--;
    super.removeEventListener(...args);
  }
  open() {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }
  send(text) {
    const frame = JSON.parse(text);
    this.frames.push(frame);
    if (!this.auto) return;
    if (frame.type === "subscribe")
      this.ready(frame.subscription_id, frame.actor_id, frame.after, "read");
    if (frame.type === "acquire_writer") {
      const sub = this.frames.find(
        (item) =>
          item.type === "subscribe" &&
          item.subscription_id === frame.subscription_id,
      );
      this.ready(sub.subscription_id, sub.actor_id, sub.after, "write");
    }
    if (frame.type === "unsubscribe")
      this.frame({
        type: "unsubscribed",
        subscription_id: frame.subscription_id,
      });
  }
  close() {
    this.readyState = 3;
  }
  remoteClose(code = 1011) {
    this.readyState = 3;
    const event = new Event("close");
    Object.defineProperty(event, "code", { value: code });
    this.dispatchEvent(event);
  }
  frame(frame) {
    this.dispatchEvent(
      new MessageEvent("message", { data: JSON.stringify(frame) }),
    );
  }
  ready(id, actor, cursor, mode = "read") {
    this.frame({
      type: "ready",
      subscription_id: id,
      actor_id: actor,
      cursor,
      mode,
      active: false,
    });
  }
  receipt(command, duplicate = false) {
    this.frame({
      type: "receipt",
      subscription_id: command.subscription_id,
      command_id: command.command_id,
      operation: command.type,
      duplicate,
    });
  }
  event(sub, seq, body = null) {
    this.frame({
      type: "event",
      subscription_id: sub.subscription_id,
      actor_id: sub.actor_id,
      seq,
      body,
    });
  }
}

const fixture = async (options = {}, auto = true) => {
  const sockets = [];
  const errors = [];
  const events = [];
  const client = new SessionClient({
    serverScope: "test",
    rootId: root,
    identity: "alice",
    url: () => "ws://test",
    onEvent: (event) => {
      events.push(event);
    },
    onError: (error) => {
      errors.push(error);
    },
    webSocketFactory: () => {
      const socket = new Socket(auto);
      sockets.push(socket);
      queueMicrotask(() => socket.open());
      return socket;
    },
    timing: {
      receiptTimeoutMs: 15,
      retryDelaysMs: [3, 3, 3],
      reconnectBaseMs: 3,
      reconnectCapMs: 5,
      capacityTimeoutMs: 100,
    },
    ...options,
  });
  const sub = client.subscribe(root);
  await client.connect();
  if (auto) await sub.ready();
  return { client, sub, sockets, errors, events, socket: sockets[0] };
};
const commands = (socket) =>
  socket.frames.filter((frame) =>
    ["send", "answer", "cancel"].includes(frame.type),
  );

test("starts read-only; FIFO receipts and frozen command data", async () => {
  const f = await fixture();
  try {
    const input = { nested: { value: "original" } };
    const first = f.client.send(input);
    input.nested.value = "changed";
    const second = f.client.cancel();
    await delay(5);
    assert.equal(commands(f.socket).length, 0);
    await f.client.acquireWriter();
    await until(() => commands(f.socket).length === 1);
    const sent = commands(f.socket)[0];
    assert.equal(sent.input.nested.value, "original");
    f.socket.receipt({ ...sent, command_id: ask });
    f.socket.receipt({ ...sent, type: "answer" });
    await delay(3);
    assert.equal(f.client.state.outbox.length, 2);
    f.socket.receipt(sent);
    assert.equal((await first).command_id, first.commandId);
    await until(() => commands(f.socket).length === 2);
    f.socket.receipt(commands(f.socket)[1]);
    await second;
    assert.deepEqual(f.client.state.outbox, []);
  } finally {
    await f.client.disconnect();
  }
  assert.equal(f.socket.listeners, 0);
});

test("cursors follow successful async handlers; hidden events and callbacks are isolated", async () => {
  const held = gate();
  const seen = [];
  const f = await fixture({
    onEvent: async (event) => {
      seen.push(event);
      if (event.seq === 1) {
        event.body.nested.value = "mutated";
        await held.promise;
      }
    },
  });
  try {
    const subscription = f.socket.frames[0];
    f.socket.event(subscription, 1, { nested: { value: "original" } });
    f.socket.event(subscription, 2, null);
    await until(() => seen.length === 1);
    assert.equal(f.sub.state.cursor, 0);
    held.resolve();
    await until(() => f.sub.state.cursor === 2);
    assert.equal(seen[1].body, null);
    f.socket.event(subscription, 2, null);
    await delay(3);
    assert.equal(seen.length, 2);
    const state = f.client.state;
    state.subscriptions[0].cursor = 99;
    assert.equal(f.sub.state.cursor, 2);
  } finally {
    held.resolve();
    await f.client.disconnect();
  }
});

test("lost receipts retry exactly; duplicate receipt is acceptance", async () => {
  const f = await fixture();
  try {
    await f.client.acquireWriter();
    const result = f.client.answer(ask, { kind: "approval", allow: false });
    await until(() => commands(f.socket).length === 2);
    assert.deepEqual(commands(f.socket)[0], commands(f.socket)[1]);
    f.socket.receipt(commands(f.socket)[1], true);
    assert.equal((await result).duplicate, true);
  } finally {
    await f.client.disconnect();
  }
});

test("retry exhaustion holds FIFO and explicit retry preserves UUID", async () => {
  const f = await fixture();
  try {
    await f.client.acquireWriter();
    const first = f.client.send("one");
    const rejection = assert.rejects(
      first,
      (error) => error.code === "unknown_outcome" && error.uncertain,
    );
    const second = f.client.send("two");
    await rejection;
    assert.equal(commands(f.socket).length, 4);
    assert.equal(f.client.state.outbox[0].status, "manual");
    const retry = f.client.retry(first.commandId);
    await until(() => commands(f.socket).length === 5);
    assert.equal(commands(f.socket)[4].command_id, first.commandId);
    f.socket.receipt(commands(f.socket)[4], true);
    await retry;
    await until(() => commands(f.socket).length === 6);
    f.socket.receipt(commands(f.socket)[5]);
    await second;
  } finally {
    await f.client.disconnect();
  }
});

test("writer_required holds despite retryable false; explicit reclaim resumes", async () => {
  const f = await fixture();
  try {
    await f.client.acquireWriter();
    const result = f.client.send("held");
    await until(() => commands(f.socket).length === 1);
    const sent = commands(f.socket)[0];
    f.socket.frame({
      type: "error",
      subscription_id: sent.subscription_id,
      command_id: sent.command_id,
      code: "writer_required",
      message: "writer needed",
      retryable: false,
      uncertain: false,
    });
    await until(() => f.client.state.outbox[0].status === "held");
    await delay(25);
    assert.equal(commands(f.socket).length, 1);
    await f.client.acquireWriter();
    await until(() => commands(f.socket).length === 2);
    f.socket.receipt(commands(f.socket)[1]);
    await result;
  } finally {
    await f.client.disconnect();
  }
});

test("definitive command errors fail one head and release the next", async () => {
  const f = await fixture();
  try {
    await f.client.acquireWriter();
    const result = f.client.send("denied");
    const rejected = assert.rejects(
      result,
      (error) => error.code === "forbidden",
    );
    const next = f.client.cancel();
    await until(() => commands(f.socket).length === 1);
    const sent = commands(f.socket)[0];
    f.socket.frame({
      type: "error",
      subscription_id: sent.subscription_id,
      command_id: sent.command_id,
      code: "forbidden",
      message: "denied",
    });
    await rejected;
    await until(() => commands(f.socket).length === 2);
    f.socket.receipt(commands(f.socket)[1]);
    await next;
  } finally {
    await f.client.disconnect();
  }
});

test("reconnect waits for old handler and resumes with a fresh ID and read-only privilege", async () => {
  const held = gate();
  const f = await fixture({
    onEvent: async () => {
      await held.promise;
    },
  });
  try {
    await f.client.acquireWriter();
    const original = f.socket.frames[0];
    f.socket.event(original, 1, null);
    await delay(3);
    f.socket.remoteClose();
    await delay(12);
    assert.equal(f.sockets.length, 1);
    held.resolve();
    await until(() => f.sockets.length === 2 && f.sub.state.ready);
    const newSub = f.sockets[1].frames[0];
    assert.notEqual(newSub.subscription_id, original.subscription_id);
    assert.equal(newSub.after, 1);
    assert.equal(newSub.mode, "read");
    assert.equal(f.sub.state.writable, false);
    f.socket.event(original, 2, null);
    await delay(3);
    assert.equal(f.sub.state.cursor, 1);
  } finally {
    held.resolve();
    await f.client.disconnect();
  }
});

test("writer loss before a queued Ready cannot release commands", async () => {
  const f = await fixture();
  try {
    const result = f.client.send("held");
    const sub = f.socket.frames[0];
    f.socket.ready(sub.subscription_id, root, 0, "write");
    f.socket.frame({
      type: "writer_lost",
      subscription_id: sub.subscription_id,
    });
    f.socket.ready(sub.subscription_id, root, 0, "read");
    await delay(20);
    assert.equal(commands(f.socket).length, 0);
    await f.client.acquireWriter();
    await until(() => commands(f.socket).length === 1);
    f.socket.receipt(commands(f.socket)[0]);
    await result;
  } finally {
    await f.client.disconnect();
  }
});

test("one active actor; replacement waits for unsubscribe acknowledgment", async () => {
  const f = await fixture({}, false);
  try {
    const first = f.socket.frames[0];
    f.socket.ready(first.subscription_id, root, 0);
    await f.sub.ready();
    assert.throws(() => f.client.subscribe(root), /already subscribed/);
    const closed = f.sub.unsubscribe();
    await until(() =>
      f.socket.frames.some((frame) => frame.type === "unsubscribe"),
    );
    assert.throws(() => f.client.subscribe(root), /acknowledgment/);
    f.socket.frame({
      type: "unsubscribed",
      subscription_id: first.subscription_id,
    });
    await closed;
    const replacement = f.client.subscribe(root);
    await until(
      () =>
        f.socket.frames.filter((frame) => frame.type === "subscribe").length ===
        2,
    );
    const second = f.socket.frames.filter(
      (frame) => frame.type === "subscribe",
    )[1];
    assert.notEqual(first.subscription_id, second.subscription_id);
    f.socket.ready(second.subscription_id, root, 0);
    await replacement.ready();
  } finally {
    await f.client.disconnect();
  }
});

test("handler failure preserves cursor and requires manual recovery", async () => {
  let fail = true;
  const f = await fixture({
    onEvent: () => {
      if (fail) throw new Error("failure");
    },
  });
  try {
    f.socket.event(f.socket.frames[0], 1, null);
    await until(() => f.client.state.connection === "blocked");
    assert.equal(f.sub.state.cursor, 0);
    await delay(10);
    assert.equal(f.sockets.length, 1);
    fail = false;
    await f.client.connect();
    await f.sub.ready();
    f.sockets[1].event(f.sockets[1].frames[0], 1, null);
    await until(() => f.sub.state.cursor === 1);
  } finally {
    await f.client.disconnect();
  }
});

test("incoming capacity includes executing handler and overflow drains before reconnect", async () => {
  const held = gate();
  const f = await fixture({ onEvent: () => held.promise });
  try {
    const sub = f.socket.frames[0];
    f.socket.event(sub, 1, null);
    await delay(3);
    for (let seq = 2; seq <= 257; seq++) f.socket.event(sub, seq, null);
    await until(() => f.errors.some((error) => error.code === "overflow"));
    assert.equal(f.sockets.length, 1);
    held.resolve();
    await until(() => f.sockets.length === 2 && f.sub.state.ready);
    assert.equal(f.sockets[1].frames[0].after, 1);
  } finally {
    held.resolve();
    await f.client.disconnect();
  }
});

test("subscription/outbox bounds and strict answers fail before transmission", async () => {
  const f = await fixture();
  try {
    assert.throws(() =>
      f.client.answer(ask, { kind: "approval", allow: "yes" }),
    );
    assert.throws(() => f.client.send({ value: undefined }));
    assert.throws(() => f.client.send("x".repeat(256 * 1024)), /limit/);
    for (let i = 0; i < 64; i++) f.client.send(String(i));
    assert.throws(() => f.client.cancel(), /limit/);
    assert.equal(commands(f.socket).length, 0);
    for (let i = 2; i <= 8; i++)
      f.client.subscribe(
        `00000000-0000-0000-0000-${String(i).padStart(12, "0")}`,
      );
    assert.throws(
      () => f.client.subscribe("00000000-0000-0000-0000-000000000009"),
      /limit/,
    );
    assert.throws(
      () =>
        f.client.send("changed", {
          commandId: f.client.state.outbox[0].commandId,
        }),
      /different content/,
    );
  } finally {
    await f.client.disconnect();
  }
});

const stored = (overrides = {}) => ({
  version: 1,
  scope: { server: "test", root, identity: "alice" },
  actors: { [root]: 5 },
  commands: [],
  ...overrides,
});
test("persistence scope mismatch prevents dialing", async () => {
  for (const scope of [
    { server: "other", root, identity: "alice" },
    { server: "test", root: child, identity: "alice" },
    { server: "test", root, identity: "bob" },
  ]) {
    let dialed = false;
    const client = new SessionClient({
      serverScope: "test",
      rootId: root,
      identity: "alice",
      url: () => "ws://test",
      onEvent: () => {},
      webSocketFactory: () => {
        dialed = true;
        return new Socket();
      },
      persistence: {
        load: async () => stored({ scope }),
        save: async () => {},
      },
    });
    await assert.rejects(client.connect(), /persisted state/);
    assert.equal(dialed, false);
    await client.disconnect();
  }
});

test("saved cursors need explicitly restored application projection", async () => {
  for (const initialCursors of [undefined, { [root]: 5 }]) {
    const f = await fixture({
      initialCursors,
      persistence: { load: async () => stored(), save: async () => {} },
    });
    try {
      assert.equal(f.socket.frames[0].after, initialCursors ? 5 : 0);
    } finally {
      await f.client.disconnect();
    }
  }
});

test("persist before sending; save failure blocks commands and cursor advancement", async () => {
  let fail = false;
  const saves = [];
  const f = await fixture({
    persistence: {
      load: async () => null,
      save: async (state) => {
        if (fail) throw new Error("storage unavailable");
        saves.push(structuredClone(state));
      },
    },
  });
  try {
    await f.client.acquireWriter();
    const result = f.client.send("stored");
    await until(() => commands(f.socket).length === 1);
    assert.equal(saves.at(-1).commands[0].attempts, 1);
    f.socket.receipt(commands(f.socket)[0]);
    await result;
    fail = true;
    f.socket.event(f.socket.frames[0], 1, null);
    await until(() => f.client.state.connection === "blocked");
    assert.equal(f.sub.state.cursor, 0);
    assert(f.errors.some((error) => error.code === "storage"));
  } finally {
    await f.client.disconnect();
  }
});

test("restored attempts and payload survive; writer privilege never restores", async () => {
  const commandId = "00000000-0000-0000-0000-000000000004";
  const f = await fixture({
    persistence: {
      load: async () =>
        stored({
          commands: [
            {
              commandId,
              payload: { operation: "send", input: "frozen" },
              attempts: 3,
              manual: false,
            },
          ],
        }),
      save: async () => {},
    },
  });
  try {
    assert.equal(f.sub.state.writable, false);
    assert.equal(f.client.state.outbox[0].attempts, 3);
    assert.equal(commands(f.socket).length, 0);
    await f.client.acquireWriter();
    await until(() => commands(f.socket).length === 1);
    assert.equal(commands(f.socket)[0].input, "frozen");
    await until(() => f.client.state.outbox[0].status === "manual");
    assert.equal(commands(f.socket).length, 1);
    assert.equal(f.client.state.outbox[0].attempts, 4);
  } finally {
    await f.client.disconnect();
  }
});

test("terminal close codes stop reconnect; new socket generations stay isolated", async () => {
  for (const code of [1002, 1008, 1009, 4404]) {
    const f = await fixture();
    f.socket.remoteClose(code);
    await delay(10);
    assert.equal(f.client.state.connection, "blocked");
    assert.equal(f.sockets.length, 1);
    assert.equal(f.socket.listeners, 0);
    await f.client.disconnect();
  }
});

test("retired handles cannot unsubscribe or wait on a replacement", async () => {
  const f = await fixture();
  try {
    f.socket.event(f.socket.frames[0], 1, null);
    await until(() => f.sub.state.cursor === 1);
    await f.sub.unsubscribe();
    const replacement = f.client.subscribe(root);
    await replacement.ready();
    assert.equal(replacement.state.cursor, 1);
    await f.sub.unsubscribe();
    await assert.rejects(f.sub.ready(), /replaced/);
    assert.equal(replacement.state.ready, true);
    assert.equal(
      f.socket.frames.filter((frame) => frame.type === "unsubscribe").length,
      1,
    );
  } finally {
    await f.client.disconnect();
  }
});
