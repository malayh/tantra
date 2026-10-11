import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import { SessionClient } from "../dist/index.js";

const root = "10000000-0000-0000-0000-000000000001";
const child = "10000000-0000-0000-0000-000000000002";
const commandId = "10000000-0000-0000-0000-000000000003";

const until = async (predicate) => {
  for (let index = 0; index < 300; index += 1) {
    if (predicate()) return;
    await delay(2);
  }
  throw new Error("condition timed out");
};

const gate = () => {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

class EdgeSocket extends EventTarget {
  readyState = 0;
  protocol = "tantra.session.v1";
  bufferedAmount = 0;
  frames = [];

  constructor({ autoReady = true, autoAcquire = true, open = true } = {}) {
    super();
    this.autoReady = autoReady;
    this.autoAcquire = autoAcquire;
    this.shouldOpen = open;
  }

  start() {
    if (!this.shouldOpen) return;
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  send(text) {
    const frame = JSON.parse(text);
    this.frames.push(frame);
    if (frame.type === "subscribe" && this.autoReady)
      this.ready(frame.subscription_id, frame.actor_id, frame.after);
    if (frame.type === "acquire_writer" && this.autoAcquire) {
      const subscription = this.frames.find(
        (item) =>
          item.type === "subscribe" &&
          item.subscription_id === frame.subscription_id,
      );
      this.ready(
        subscription.subscription_id,
        subscription.actor_id,
        subscription.after,
        "write",
      );
    }
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

  data(value) {
    this.dispatchEvent(new MessageEvent("message", { data: value }));
  }

  frame(frame) {
    this.data(JSON.stringify(frame));
  }

  ready(subscriptionId, actorId, cursor, mode = "read") {
    this.frame({
      type: "ready",
      subscription_id: subscriptionId,
      actor_id: actorId,
      cursor,
      mode,
      active: false,
    });
  }

  event(subscription, seq, body = null) {
    this.frame({
      type: "event",
      subscription_id: subscription.subscription_id,
      actor_id: subscription.actor_id,
      seq,
      body,
    });
  }

  receipt(command) {
    this.frame({
      type: "receipt",
      subscription_id: command.subscription_id,
      command_id: command.command_id,
      operation: command.type,
      duplicate: false,
    });
  }
}

const commands = (socket) =>
  socket.frames.filter((frame) =>
    ["send", "answer", "cancel"].includes(frame.type),
  );

const setup = async ({
  options = {},
  socketOptions = {},
  subscribe = true,
} = {}) => {
  const sockets = [];
  const errors = [];
  const events = [];
  const notices = [];
  const client = new SessionClient({
    serverScope: "edge",
    rootId: root,
    identity: "alice",
    url: () => "ws://edge",
    onEvent: (event) => events.push(event),
    onNotice: (notice) => notices.push(notice),
    onError: (error) => errors.push(error),
    webSocketFactory: () => {
      const socket = new EdgeSocket(socketOptions);
      sockets.push(socket);
      queueMicrotask(() => socket.start());
      return socket;
    },
    timing: {
      receiptTimeoutMs: 20,
      retryDelaysMs: [4, 4, 4],
      reconnectBaseMs: 4,
      reconnectCapMs: 6,
      capacityTimeoutMs: 25,
    },
    ...options,
  });
  const subscription = subscribe ? client.subscribe(root) : undefined;
  await client.connect();
  if (subscription && socketOptions.autoReady !== false)
    await subscription.ready();
  return {
    client,
    subscription,
    sockets,
    socket: sockets[0],
    errors,
    events,
    notices,
  };
};

test("actors advance independently and notice/global error callbacks are delivered", async () => {
  const f = await setup({ socketOptions: { autoReady: false } });
  try {
    const childSubscription = f.client.subscribe(child, { after: 4 });
    await until(
      () =>
        f.socket.frames.filter((frame) => frame.type === "subscribe").length ===
        2,
    );
    const [rootFrame, childFrame] = f.socket.frames.filter(
      (frame) => frame.type === "subscribe",
    );
    f.socket.ready(rootFrame.subscription_id, root, 0);
    f.socket.ready(childFrame.subscription_id, child, 4);
    await Promise.all([f.subscription.ready(), childSubscription.ready()]);
    f.socket.event(childFrame, 5, { text: "child" });
    f.socket.frame({ type: "notice", body: { title: "updated" } });
    f.socket.frame({
      type: "error",
      code: "unavailable",
      message: "try later",
      subscription_id: null,
      command_id: null,
      retryable: true,
      uncertain: false,
    });
    await until(
      () =>
        childSubscription.state.cursor === 5 &&
        f.notices.length === 1 &&
        f.errors.length === 1,
    );
    assert.equal(f.subscription.state.cursor, 0);
    assert.equal(f.events[0].actor_id, child);
    assert.deepEqual(f.notices, [{ title: "updated" }]);
    assert.equal(f.errors[0].code, "unavailable");
  } finally {
    await f.client.disconnect();
  }
});

test("subscription denial can be replaced and writer denial can be retried", async () => {
  const f = await setup({
    socketOptions: { autoReady: false, autoAcquire: false },
  });
  try {
    const first = f.socket.frames.find((frame) => frame.type === "subscribe");
    const firstReady = f.subscription.ready();
    f.socket.frame({
      type: "error",
      code: "forbidden",
      message: "denied",
      subscription_id: first.subscription_id,
      command_id: null,
      retryable: false,
      uncertain: false,
    });
    await assert.rejects(firstReady, (error) => error.code === "forbidden");
    const replacement = f.client.subscribe(root);
    await until(
      () =>
        f.socket.frames.filter((frame) => frame.type === "subscribe").length ===
        2,
    );
    const second = f.socket.frames.filter(
      (frame) => frame.type === "subscribe",
    )[1];
    f.socket.ready(second.subscription_id, root, 0);
    await replacement.ready();
    const denied = f.client.acquireWriter();
    await until(
      () =>
        f.socket.frames.filter((frame) => frame.type === "acquire_writer")
          .length === 1,
    );
    f.socket.frame({
      type: "error",
      code: "forbidden",
      message: "denied",
      subscription_id: second.subscription_id,
      command_id: null,
      retryable: false,
      uncertain: false,
    });
    await assert.rejects(denied, (error) => error.code === "forbidden");
    const acquired = f.client.acquireWriter();
    await until(
      () =>
        f.socket.frames.filter((frame) => frame.type === "acquire_writer")
          .length === 2,
    );
    f.socket.ready(second.subscription_id, root, 0, "write");
    await acquired;
    assert.equal(replacement.state.writable, true);
  } finally {
    await f.client.disconnect();
  }
});

test("transport replacement ignores stale receipts and clears the old timeout", async () => {
  const f = await setup();
  try {
    await f.client.acquireWriter();
    const result = f.client.send("once", { commandId });
    await until(() => commands(f.socket).length === 1);
    const original = commands(f.socket)[0];
    f.socket.remoteClose();
    f.socket.receipt(original);
    await until(() => f.sockets.length === 2);
    await f.client.acquireWriter();
    await until(() => commands(f.sockets[1]).length === 1);
    assert.equal(f.client.state.outbox.length, 1);
    const retried = commands(f.sockets[1])[0];
    assert.equal(retried.command_id, original.command_id);
    f.sockets[1].receipt(retried);
    await result;
    await delay(30);
    assert.deepEqual(f.client.state.outbox, []);
  } finally {
    await f.client.disconnect();
  }
});

test("UTF-8 byte bounds reject oversized commands and server frames", async () => {
  const outgoing = await setup();
  try {
    assert.throws(() => outgoing.client.send("é".repeat(140_000)), /limit/);
    assert.equal(commands(outgoing.socket).length, 0);
  } finally {
    await outgoing.client.disconnect();
  }

  for (const data of [
    "{",
    JSON.stringify({ type: "notice", body: "é".repeat(524_288) }),
  ]) {
    const incoming = await setup();
    try {
      incoming.socket.data(data);
      await until(() => incoming.client.state.connection === "blocked");
      assert.equal(
        incoming.errors.at(-1).code,
        data === "{" ? "protocol" : "size",
      );
    } finally {
      await incoming.client.disconnect();
    }
  }
});

test("incoming queue byte accounting includes an executing notice", async () => {
  const held = gate();
  let entered = false;
  const f = await setup({
    options: {
      onNotice: async () => {
        entered = true;
        await held.promise;
      },
    },
  });
  try {
    const notice = {
      type: "notice",
      body: "é".repeat(450_000),
    };
    f.socket.frame(notice);
    await until(() => entered);
    for (let index = 0; index < 4; index += 1) f.socket.frame(notice);
    await until(() => f.errors.some((error) => error.code === "overflow"));
    assert.equal(f.sockets.length, 1);
    held.resolve();
    await until(() => f.sockets.length === 2);
  } finally {
    held.resolve();
    await f.client.disconnect();
  }
});

test("stalled socket buffers and stalled handshakes fail within the capacity deadline", async () => {
  const buffered = await setup();
  try {
    buffered.socket.bufferedAmount = 4 * 1024 * 1024;
    await assert.rejects(buffered.client.acquireWriter());
    await until(() =>
      buffered.errors.some((error) => error.code === "capacity"),
    );
    await until(() => buffered.sockets.length === 2);
  } finally {
    await buffered.client.disconnect();
  }

  const unopened = await setup({
    socketOptions: { open: false },
    subscribe: false,
  }).catch((error) => error);
  assert.equal(unopened.code, "transport");
});

test("disconnect invalidates a pending URL resolution", async () => {
  const pendingUrl = gate();
  let constructed = 0;
  const client = new SessionClient({
    serverScope: "edge",
    rootId: root,
    identity: "alice",
    url: () => pendingUrl.promise,
    onEvent: () => {},
    webSocketFactory: () => {
      constructed += 1;
      return new EdgeSocket();
    },
    timing: { capacityTimeoutMs: 100 },
  });
  client.subscribe(root);
  const connecting = client.connect();
  await delay(0);
  await client.disconnect();
  pendingUrl.resolve("ws://stale");
  await assert.rejects(connecting);
  await delay(0);
  assert.equal(constructed, 0);
});

const stored = (overrides = {}) => ({
  version: 1,
  scope: { server: "edge", root, identity: "alice" },
  actors: { [root]: 0 },
  commands: [],
  ...overrides,
});

test("malformed persistence never dials", async () => {
  const invalid = [
    stored({ actors: { [root]: Number.MAX_SAFE_INTEGER + 1 } }),
    stored({ extra: true }),
    stored({
      commands: [
        {
          commandId,
          payload: { operation: "cancel" },
          attempts: 1.5,
          manual: false,
        },
      ],
    }),
    stored({
      actors: { [child]: 0 },
      commands: [
        {
          commandId,
          payload: { operation: "cancel" },
          attempts: 1,
          manual: false,
        },
      ],
    }),
  ];
  for (const saved of invalid) {
    let dialed = false;
    const client = new SessionClient({
      serverScope: "edge",
      rootId: root,
      identity: "alice",
      url: () => "ws://edge",
      onEvent: () => {},
      persistence: { load: async () => saved, save: async () => {} },
      webSocketFactory: () => {
        dialed = true;
        const socket = new EdgeSocket();
        queueMicrotask(() => socket.start());
        return socket;
      },
    });
    await assert.rejects(client.connect(), (error) => error.code === "storage");
    assert.equal(dialed, false);
    await client.disconnect();
  }
});

test("queued commands persist before write and an initial save failure transmits nothing", async () => {
  const released = gate();
  const saves = [];
  const ordered = await setup({
    options: {
      persistence: {
        load: async () => null,
        save: async (state) => {
          saves.push(structuredClone(state));
          if (state.commands[0]?.attempts === 0) await released.promise;
        },
      },
    },
  });
  try {
    await ordered.client.acquireWriter();
    const result = ordered.client.send("persisted");
    await until(() => saves.some((state) => state.commands[0]?.attempts === 0));
    assert.equal(commands(ordered.socket).length, 0);
    released.resolve();
    await until(() => commands(ordered.socket).length === 1);
    assert.deepEqual(
      saves
        .filter((state) => state.commands.length)
        .slice(0, 2)
        .map((state) => state.commands[0].attempts),
      [0, 1],
    );
    ordered.socket.receipt(commands(ordered.socket)[0]);
    await result;
  } finally {
    released.resolve();
    await ordered.client.disconnect();
  }

  const failed = await setup({
    options: {
      persistence: {
        load: async () => null,
        save: async (state) => {
          if (state.commands.length) throw new Error("unavailable");
        },
      },
    },
  });
  try {
    await failed.client.acquireWriter();
    void failed.client.send("never sent");
    await until(() => failed.client.state.connection === "blocked");
    assert.equal(commands(failed.socket).length, 0);
    assert(failed.errors.some((error) => error.code === "storage"));
  } finally {
    await failed.client.disconnect();
  }
});

test("throwing state callbacks are contained without recursive failure", async () => {
  const errors = [];
  const client = new SessionClient({
    serverScope: "edge",
    rootId: root,
    identity: "alice",
    url: () => "ws://edge",
    onEvent: () => {},
    onState: () => {
      throw new Error("state callback failed");
    },
    onError: (error) => errors.push(error),
  });
  assert.doesNotThrow(() => client.subscribe(root));
  assert.equal(client.state.connection, "blocked");
  assert.deepEqual(
    errors.map((error) => error.code),
    ["application"],
  );
  await client.disconnect();
  assert.equal(errors.length, 2);
});

test("writer acquisition timeout retires ambiguous transport and requires fresh reclaim", async () => {
  const f = await setup({ socketOptions: { autoAcquire: false } });
  try {
    const result = f.client.send("held", { commandId });
    const original = f.socket.frames.find(
      (frame) => frame.type === "subscribe",
    );
    await assert.rejects(f.client.acquireWriter(), /timed out/);
    assert.equal(f.socket.readyState, 3);
    f.socket.ready(original.subscription_id, root, 0, "write");
    await until(() => f.sockets.length === 2 && f.subscription.state.ready);
    const replacement = f.sockets[1];
    assert.equal(f.subscription.state.writable, false);
    assert.equal(commands(f.socket).length, 0);
    assert.equal(commands(replacement).length, 0);
    replacement.autoAcquire = true;
    await f.client.acquireWriter();
    await until(() => commands(replacement).length === 1);
    assert.equal(commands(replacement)[0].command_id, commandId);
    replacement.receipt(commands(replacement)[0]);
    await result;
  } finally {
    await f.client.disconnect();
  }
});

test("idle desired actors persist only after loading and survive restart", async () => {
  let saved = stored({ actors: { [child]: 0 } });
  let loads = 0;
  let writes = 0;
  const persistence = {
    load: async () => {
      loads += 1;
      return structuredClone(saved);
    },
    save: async (state) => {
      assert(loads > 0);
      writes += 1;
      saved = structuredClone(state);
    },
  };
  const first = await setup({ options: { persistence } });
  try {
    assert.equal(writes, 1);
    assert.deepEqual(saved.actors, { [root]: 0, [child]: 0 });
    const idle = "10000000-0000-0000-0000-000000000004";
    first.client.subscribe(idle);
    await until(() => idle in saved.actors);
    await first.client.disconnect();
    const second = await setup({ options: { persistence }, subscribe: false });
    try {
      await until(() =>
        second.client.state.subscriptions.every((actor) => actor.ready),
      );
      assert.deepEqual(
        second.client.state.subscriptions.map((actor) => actor.actorId),
        [root, child, idle],
      );
      assert(
        second.client.state.subscriptions.every((actor) => !actor.writable),
      );
    } finally {
      await second.client.disconnect();
    }
  } finally {
    await first.client.disconnect();
  }
});

test("initial subscription persistence failure prevents dialing and permits manual retry", async () => {
  let fail = true;
  let dialed = 0;
  const client = new SessionClient({
    serverScope: "edge",
    rootId: root,
    identity: "alice",
    url: () => "ws://edge",
    onEvent: () => {},
    persistence: {
      load: async () => null,
      save: async () => {
        if (fail) throw new Error("unavailable");
      },
    },
    webSocketFactory: () => {
      dialed += 1;
      const socket = new EdgeSocket();
      queueMicrotask(() => socket.start());
      return socket;
    },
  });
  const subscription = client.subscribe(root);
  try {
    await assert.rejects(client.connect(), (error) => error.code === "storage");
    assert.equal(dialed, 0);
    assert.equal(client.state.connection, "blocked");
    fail = false;
    await client.connect();
    await subscription.ready();
    assert.equal(dialed, 1);
    assert.equal(subscription.state.writable, false);
  } finally {
    await client.disconnect();
  }
});
