import assert from "node:assert/strict";
import test from "node:test";

import { createChatStore } from "./state.ts";
import { canonicalId, ChatTransport } from "./transport.ts";

type Frame = Record<string, unknown>;

class FakeSocket {
  readyState = 0;
  protocol = "tantra.session.v1";
  bufferedAmount = 0;
  sent: Frame[] = [];
  private readonly listeners = new Map<string, Set<(event: unknown) => void>>();
  private readonly respond: (socket: FakeSocket, frame: Frame) => void;

  constructor(respond: (socket: FakeSocket, frame: Frame) => void) {
    this.respond = respond;
    queueMicrotask(() => {
      this.readyState = 1;
      this.emit("open", {});
    });
  }

  send(value: string) {
    const frame = JSON.parse(value) as Frame;
    this.sent.push(frame);
    this.respond(this, frame);
  }

  close() {
    this.readyState = 3;
    this.emit("close", { code: 1000 });
  }

  addEventListener(type: string, listener: (event: unknown) => void) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: unknown) => void) {
    this.listeners.get(type)?.delete(listener);
  }

  message(frame: Frame) {
    queueMicrotask(() => this.emit("message", { data: JSON.stringify(frame) }));
  }

  private emit(type: string, event: unknown) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

const root = "11111111111111111111111111111111";
const child = "22222222222222222222222222222222";
const command = "44444444-4444-4444-4444-444444444444";

const ready = (socket: FakeSocket, frame: Frame, mode: "read" | "write" = "read") =>
  socket.message({
    type: "ready",
    subscription_id: frame.subscription_id,
    actor_id: frame.actor_id,
    cursor: frame.after,
    mode,
    active: true,
  });

const standardResponder = (socket: FakeSocket, frame: Frame) => {
  if (frame.type === "subscribe") ready(socket, frame);
  if (frame.type === "unsubscribe") {
    socket.message({ type: "unsubscribed", subscription_id: frame.subscription_id });
  }
  if (frame.type === "acquire_writer") {
    const subscription = socket.sent.find(
      (item) => item.subscription_id === frame.subscription_id && item.type === "subscribe",
    )!;
    ready(socket, subscription, "write");
  }
  if (frame.type === "send" || frame.type === "answer" || frame.type === "cancel") {
    socket.message({
      type: "receipt",
      subscription_id: frame.subscription_id,
      command_id: frame.command_id,
      operation: frame.type,
      duplicate: false,
    });
  }
};

const event = (socket: FakeSocket, subscription: Frame, seq: number, body: Frame) =>
  socket.message({
    type: "event",
    subscription_id: subscription.subscription_id,
    actor_id: subscription.actor_id,
    seq,
    body,
  });

const setup = (respond = standardResponder, identity = "7") => {
  const store = createChatStore(root);
  let socket!: FakeSocket;
  const states: unknown[] = [];
  const transport = new ChatTransport({
    serverScope: "http://example.test",
    sessionId: root,
    identity,
    store,
    socketUrl: async () => "ws://example.test/socket?ticket=fresh",
    onNotice: () => {},
    onState: (state) => states.push(state),
    onError: (error) => assert.fail(error.message),
    webSocketFactory: () => {
      socket = new FakeSocket(respond);
      return socket as never;
    },
  });
  return { store, states, transport, socket: () => socket };
};

test("full reload replays from zero and transport IDs alone are canonical", async () => {
  const fixture = setup((socket, frame) => {
    if (frame.type === "subscribe") {
      socket.message({
        type: "event",
        subscription_id: frame.subscription_id,
        actor_id: frame.actor_id,
        seq: 1,
        body: { type: "input_queued", command_id: root, input: "hello" },
      });
      socket.message({
        type: "ready",
        subscription_id: frame.subscription_id,
        actor_id: frame.actor_id,
        cursor: 1,
        mode: "read",
        active: true,
      });
      return;
    }
    if (frame.type === "acquire_writer") {
      const subscription = socket.sent.find((item) => item.type === "subscribe")!;
      socket.message({
        type: "ready",
        subscription_id: subscription.subscription_id,
        actor_id: subscription.actor_id,
        cursor: 1,
        mode: "write",
        active: true,
      });
      return;
    }
    standardResponder(socket, frame);
  });

  await fixture.transport.connect();
  await fixture.transport.acquireWriter();
  await fixture.transport.send({ text: "next", attachments: [] }, command);

  const sent = fixture.socket().sent;
  assert.equal(sent[0].actor_id, canonicalId(root));
  assert.equal(sent[0].after, 0);
  assert.deepEqual(sent.at(-1), {
    type: "send",
    subscription_id: sent[0].subscription_id,
    command_id: command,
    input: { text: "next", attachments: [] },
  });
  assert.equal(fixture.store.getState().cursors[root], 1);
  assert.equal(fixture.store.getState().turns[0].input, "hello");
  await fixture.transport.disconnect();
});

test("inactive readiness reconciles only at replay boundaries", async () => {
  const replayTurn = "55555555555555555555555555555555";
  const liveTurn = "66666666666666666666666666666666";
  const replaySample = "77777777777777777777777777777777";
  const liveSample = "88888888888888888888888888888888";
  const fixture = setup((socket, frame) => {
    if (frame.type === "subscribe") {
      event(socket, frame, 1, { type: "input_queued", command_id: replayTurn, input: "replayed" });
      event(socket, frame, 2, { type: "turn_started", turn_id: replayTurn, input: "replayed" });
      event(socket, frame, 3, {
        type: "sample_started",
        turn_id: replayTurn,
        sample_id: replaySample,
        model: "test",
      });
      event(socket, frame, 4, { type: "reasoning_delta", text: "replay reason" });
      event(socket, frame, 5, { type: "reasoning_part", sample_id: replaySample, text: "replay reason" });
      event(socket, frame, 6, { type: "text_delta", text: "replay answer" });
      event(socket, frame, 7, { type: "text_part", sample_id: replaySample, text: "replay answer" });
      event(socket, frame, 8, { type: "sample_completed", sample_id: replaySample });
      event(socket, frame, 9, { type: "turn_completed", turn_id: replayTurn, stop_reason: "end" });
      socket.message({
        type: "ready",
        subscription_id: frame.subscription_id,
        actor_id: frame.actor_id,
        cursor: 9,
        mode: "read",
        active: false,
      });
      return;
    }
    if (frame.type === "acquire_writer") {
      const subscription = socket.sent.find((item) => item.type === "subscribe")!;
      socket.message({
        type: "ready",
        subscription_id: subscription.subscription_id,
        actor_id: subscription.actor_id,
        cursor: 9,
        mode: "write",
        active: false,
      });
      return;
    }
    standardResponder(socket, frame);
  });

  await fixture.transport.connect();
  await fixture.transport.acquireWriter();
  const socket = fixture.socket();
  const subscription = socket.sent.find((frame) => frame.type === "subscribe")!;
  event(socket, subscription, 10, { type: "input_queued", command_id: liveTurn, input: "live" });
  event(socket, subscription, 11, { type: "turn_started", turn_id: liveTurn, input: "live" });
  event(socket, subscription, 12, {
    type: "sample_started",
    turn_id: liveTurn,
    sample_id: liveSample,
    model: "test",
  });
  event(socket, subscription, 13, { type: "reasoning_delta", text: "live reason" });
  event(socket, subscription, 14, { type: "reasoning_part", sample_id: liveSample, text: "live reason" });
  event(socket, subscription, 15, { type: "text_delta", text: "live answer" });
  event(socket, subscription, 16, { type: "text_part", sample_id: liveSample, text: "live answer" });
  event(socket, subscription, 17, { type: "sample_completed", sample_id: liveSample });
  event(socket, subscription, 18, { type: "turn_completed", turn_id: liveTurn, stop_reason: "end" });
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.deepEqual(
    fixture.store
      .getState()
      .turns.map((turn) => [turn.status, turn.items.map((item) => [item.kind, item.content, item.final])]),
    [
      [
        "done",
        [
          ["thinking", "replay reason", true],
          ["text", "replay answer", true],
        ],
      ],
      [
        "done",
        [
          ["thinking", "live reason", true],
          ["text", "live answer", true],
        ],
      ],
    ],
  );
  assert.equal(fixture.store.getState().cursors[root], 18);
  await fixture.transport.disconnect();
});

test("later ready boundaries reconcile inactive work once", async () => {
  const interruptedTurn = "99999999999999999999999999999999";
  const liveTurn = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const liveSample = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
  const fixture = setup((socket, frame) => {
    if (frame.type === "subscribe") {
      socket.message({
        type: "ready",
        subscription_id: frame.subscription_id,
        actor_id: frame.actor_id,
        cursor: 0,
        mode: "read",
        active: false,
      });
      return;
    }
    if (frame.type === "acquire_writer") {
      const subscription = socket.sent.find((item) => item.type === "subscribe")!;
      socket.message({
        type: "ready",
        subscription_id: subscription.subscription_id,
        actor_id: subscription.actor_id,
        cursor: 2,
        mode: "write",
        active: false,
      });
      return;
    }
    standardResponder(socket, frame);
  });

  await fixture.transport.connect();
  const socket = fixture.socket();
  const subscription = socket.sent.find((frame) => frame.type === "subscribe")!;
  event(socket, subscription, 1, { type: "input_queued", command_id: interruptedTurn, input: "stale" });
  event(socket, subscription, 2, { type: "turn_started", turn_id: interruptedTurn, input: "stale" });
  await new Promise((resolve) => setTimeout(resolve, 10));
  await fixture.transport.acquireWriter();

  event(socket, subscription, 3, { type: "input_queued", command_id: liveTurn, input: "live" });
  event(socket, subscription, 4, { type: "turn_started", turn_id: liveTurn, input: "live" });
  event(socket, subscription, 5, {
    type: "sample_started",
    turn_id: liveTurn,
    sample_id: liveSample,
    model: "test",
  });
  event(socket, subscription, 6, { type: "text_delta", text: "kept" });
  event(socket, subscription, 7, { type: "text_part", sample_id: liveSample, text: "kept" });
  event(socket, subscription, 8, { type: "sample_completed", sample_id: liveSample });
  event(socket, subscription, 9, { type: "turn_completed", turn_id: liveTurn, stop_reason: "end" });
  await new Promise((resolve) => setTimeout(resolve, 10));

  const turns = fixture.store.getState().turns;
  assert.equal(turns[0].status, "interrupted");
  assert.equal(turns[1].status, "done");
  assert.deepEqual(
    turns[1].items.map((item) => [item.kind, item.content, item.final]),
    [["text", "kept", true]],
  );
  await fixture.transport.disconnect();
});

test("child projection watermarks survive the client retired-actor bound", async () => {
  const seenAfter = new Map<string, number[]>();
  const fixture = setup((socket, frame) => {
    if (frame.type === "subscribe") {
      const actorId = frame.actor_id as string;
      const after = frame.after as number;
      seenAfter.set(actorId, [...(seenAfter.get(actorId) ?? []), after]);
      if (actorId !== canonicalId(root) && after === 0) {
        socket.message({
          type: "event",
          subscription_id: frame.subscription_id,
          actor_id: actorId,
          seq: 1,
          body: { type: "input_queued", command_id: root, input: actorId },
        });
        socket.message({
          type: "ready",
          subscription_id: frame.subscription_id,
          actor_id: actorId,
          cursor: 1,
          mode: "read",
          active: true,
        });
      } else {
        ready(socket, frame);
      }
      return;
    }
    standardResponder(socket, frame);
  });

  await fixture.transport.connect();
  const children = Array.from({ length: 18 }, (_, index) => (index + 2).toString(16).padStart(32, "0"));
  for (const actorId of children) {
    await fixture.transport.openChild(actorId);
    await fixture.transport.closeChild();
    assert.equal(fixture.store.getState().children[actorId]?.ready, false);
  }
  await fixture.transport.openChild(children[0]);

  assert.deepEqual(seenAfter.get(canonicalId(children[0])), [0, 1]);
  assert.equal(fixture.store.getState().cursors[children[0]], 1);
  await fixture.transport.disconnect();
});

test("writer loss stays read-only until explicit reclaim", async () => {
  const fixture = setup();
  await fixture.transport.connect();
  await fixture.transport.acquireWriter();
  const socket = fixture.socket();
  const subscription = socket.sent.find((frame) => frame.type === "subscribe")!;
  const acquisitions = () => socket.sent.filter((frame) => frame.type === "acquire_writer").length;

  socket.message({ type: "writer_lost", subscription_id: subscription.subscription_id });
  socket.message({
    type: "ready",
    subscription_id: subscription.subscription_id,
    actor_id: subscription.actor_id,
    cursor: subscription.after,
    mode: "read",
    active: true,
  });
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(fixture.transport.state.subscriptions[0].writable, false);
  assert.equal(acquisitions(), 1);
  await fixture.transport.acquireWriter();
  assert.equal(acquisitions(), 2);
  await fixture.transport.disconnect();
});

test("typed answers resolve from receipts without journal acknowledgements", async () => {
  const fixture = setup();
  await fixture.transport.connect();
  await fixture.transport.acquireWriter();

  await fixture.transport.answer(child, { kind: "approval", allow: true });
  await fixture.transport.answer(child, { kind: "choice", selected: "blue" });
  await fixture.transport.answer(child, { kind: "free_text", text: "details" });

  assert.deepEqual(
    fixture
      .socket()
      .sent.filter((frame) => frame.type === "answer")
      .map((frame) => frame.response),
    [
      { kind: "approval", allow: true },
      { kind: "choice", selected: "blue" },
      { kind: "free_text", text: "details" },
    ],
  );
  await fixture.transport.disconnect();
});

test("rapid child replacement waits for unsubscribe and ignores the stale ready waiter", async () => {
  const nextChild = "33333333333333333333333333333333";
  const fixture = setup((socket, frame) => {
    if (frame.type === "subscribe" && frame.actor_id === canonicalId(root)) {
      ready(socket, frame);
      return;
    }
    if (frame.type === "subscribe" && frame.actor_id === canonicalId(nextChild)) {
      ready(socket, frame);
      return;
    }
    if (frame.type === "subscribe" && frame.actor_id === canonicalId(child)) return;
    standardResponder(socket, frame);
  });
  await fixture.transport.connect();

  const first = fixture.transport.openChild(child);
  await new Promise((resolve) => setTimeout(resolve, 0));
  const second = fixture.transport.openChild(nextChild);
  await Promise.all([first, second]);

  assert.equal(fixture.store.getState().openChildId, nextChild);
  assert.deepEqual(
    fixture
      .socket()
      .sent.filter((frame) => frame.type === "subscribe" || frame.type === "unsubscribe")
      .map((frame) => [frame.type, frame.actor_id]),
    [
      ["subscribe", canonicalId(root)],
      ["subscribe", canonicalId(child)],
      ["unsubscribe", undefined],
      ["subscribe", canonicalId(nextChild)],
    ],
  );
  await fixture.transport.disconnect();
});

test("identity replacement disconnects the old client without transferring its outbox", async () => {
  const old = setup((socket, frame) => {
    if (frame.type !== "send") standardResponder(socket, frame);
  }, "7");
  await old.transport.connect();
  await old.transport.acquireWriter();
  old.transport.send({ text: "old", attachments: [] }, command);
  assert.equal(old.transport.state.outbox.length, 1);
  await old.transport.disconnect();

  const replacement = setup(standardResponder, "8");
  await replacement.transport.connect();
  assert.deepEqual(replacement.transport.state.outbox, []);
  assert.equal(
    replacement.socket().sent.some((frame) => frame.type === "send"),
    false,
  );
  await replacement.transport.disconnect();
});
