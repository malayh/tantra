import assert from "node:assert/strict";
import test from "node:test";

import type { ActorStatusOut, EventFrame } from "../../generated/models/index.ts";
import {
  treeRunning,
  actorStatusLabel,
  composerDisabled,
  createChatStore,
  pendingAsk,
  subscriptionFrames,
} from "./state.ts";

const root = "11111111111111111111111111111111";
const child = "22222222222222222222222222222222";
const grandchild = "33333333333333333333333333333333";
const command = "44444444444444444444444444444444";

const event = (agentId: string, seq: number, value: EventFrame["event"]): EventFrame => ({
  type: "event",
  agent_id: agentId,
  seq,
  event: value,
});

const actor = (agentId: string, state: ActorStatusOut["state"], parentId: string | null = root): ActorStatusOut => ({
  agent_id: agentId,
  root_id: root,
  parent_id: parentId,
  agent: parentId === null ? "sarathi" : "researcher",
  state,
  active: state === "running",
  current_turn_id: state === "running" ? command : null,
  last_turn: null,
  last_seq: 0,
  updated_at: "2026-01-01T00:00:00Z",
});

test("root is the only default subscription and spawn remains an ordinary tool", () => {
  const store = createChatStore(root);
  const dispatch = store.getState().dispatch;
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "research" }));
  dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "research" }));
  dispatch(event(root, 3, { type: "sample_started", turn_id: command, sample_id: "sample", model: "m" }));
  dispatch(
    event(root, 4, {
      type: "tool_call_requested",
      turn_id: command,
      sample_id: "sample",
      call_id: "spawn",
      name: "spawn",
      args: { agent_name: "researcher", input: "look" },
    }),
  );
  dispatch(
    event(root, 5, {
      type: "child_created",
      child_id: child,
      agent: "researcher",
      turn_id: command,
      call_id: "spawn",
    }),
  );
  dispatch(
    event(root, 6, {
      type: "tool_call_completed",
      turn_id: command,
      call_id: "spawn",
      result: { child_id: child },
      is_error: false,
    }),
  );

  assert.deepEqual(subscriptionFrames(store.getState(), root), [
    { type: "subscribe", agent_id: root, after: 6, writable: true },
  ]);
  assert.deepEqual(store.getState().children, {});
  const item = store.getState().turns[0].items[0];
  assert.equal(item.kind, "tool");
  assert.equal(item.final, true);
  if (item.kind === "tool") assert.equal(item.name, "spawn");
});

test("child journals keep independent cursors, history, readiness, and deduplication", () => {
  const store = createChatStore(root);
  store.getState().dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "root" }));
  store.getState().setOpenChild(grandchild);
  store
    .getState()
    .dispatch(event(grandchild, 1, { type: "input_queued", command_id: command, input: "direct nested work" }));
  store
    .getState()
    .dispatch(event(grandchild, 2, { type: "turn_started", turn_id: command, input: "direct nested work" }));
  store
    .getState()
    .dispatch(event(grandchild, 3, { type: "sample_started", turn_id: command, sample_id: "nested", model: "m" }));
  store.getState().dispatch(event(grandchild, 4, { type: "text_delta", text: "deep" }));
  store.getState().dispatch(event(grandchild, 4, { type: "text_delta", text: " duplicate" }));
  store.getState().subscriptionReady({ type: "subscription_ready", agent_id: grandchild, seq: 4, active: true });

  assert.deepEqual(subscriptionFrames(store.getState(), root), [
    { type: "subscribe", agent_id: root, after: 1, writable: true },
    { type: "subscribe", agent_id: grandchild, after: 4, writable: false },
  ]);
  assert.equal(store.getState().turns[0].items.length, 0);
  assert.equal(store.getState().children[grandchild].turns[0].items[0].content, "deep");
  assert.equal(store.getState().children[grandchild].ready, true);

  const retained = store.getState().children[grandchild];
  store.getState().setOpenChild(null);
  assert.deepEqual(subscriptionFrames(store.getState(), root), [
    { type: "subscribe", agent_id: root, after: 1, writable: true },
  ]);
  assert.equal(store.getState().children[grandchild], retained);
  store.getState().setOpenChild(grandchild);
  assert.equal(subscriptionFrames(store.getState(), root)[1].after, 4);
});

test("switching the open child reconnects only root and the selected journal", () => {
  const store = createChatStore(root);
  store.getState().dispatch(event(child, 1, { type: "input_queued", command_id: command, input: "child" }));
  store.getState().dispatch(event(grandchild, 1, { type: "input_queued", command_id: command, input: "nested" }));
  store.getState().setOpenChild(child);
  store.getState().setOpenChild(grandchild);

  assert.deepEqual(subscriptionFrames(store.getState(), root), [
    { type: "subscribe", agent_id: root, after: 0, writable: true },
    { type: "subscribe", agent_id: grandchild, after: 1, writable: false },
  ]);
  assert.equal(store.getState().children[child].turns[0].input, "child");
});

test("lifecycle and finish inputs are hidden while queued human messages remain FIFO", () => {
  const store = createChatStore(root);
  const next = "55555555555555555555555555555555";
  const finish = "66666666666666666666666666666666";
  const lifecycle = "77777777777777777777777777777777";
  const dispatch = store.getState().dispatch;
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "first" }));
  dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "first" }));
  dispatch(event(root, 3, { type: "input_queued", command_id: next, input: "second" }));
  dispatch(event(root, 4, { type: "input_queued", command_id: lifecycle, input: `[agent ${child} turn ended] {}` }));
  dispatch(event(root, 5, { type: "input_queued", command_id: finish, input: `[agent ${child} finished] done` }));

  assert.deepEqual(
    store.getState().turns.map((turn) => [turn.input, turn.status, turn.synthetic]),
    [
      ["first", "running", false],
      ["second", "queued", false],
      [`[agent ${child} turn ended] {}`, "queued", true],
      [`[agent ${child} finished] done`, "queued", true],
    ],
  );
});

test("only root asks enter interactive state", () => {
  const store = createChatStore(root);
  const askId = "55555555555555555555555555555555";
  store.getState().dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "root" }));
  store.getState().dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "root" }));
  store
    .getState()
    .dispatch(event(root, 3, { type: "sample_started", turn_id: command, sample_id: "root-sample", model: "m" }));
  store.getState().dispatch(
    event(root, 4, {
      type: "ask_raised",
      ask_id: askId,
      call_id: "call",
      request: { kind: "approval", title: "Approve", body: "root" },
    }),
  );
  assert.deepEqual(pendingAsk(store.getState().turns), { askId });
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), false);

  store.getState().dispatch(
    event(root, 5, {
      type: "ask_answered",
      ask_id: askId,
      response: { kind: "approval", allow: true },
    }),
  );
  assert.equal(pendingAsk(store.getState().turns), null);
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), true);

  store.getState().dispatch(event(root, 6, { type: "turn_completed", turn_id: command, stop_reason: "completed" }));
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), false);

  store.getState().dispatch(event(child, 1, { type: "input_queued", command_id: command, input: "child" }));
  store.getState().dispatch(event(child, 2, { type: "turn_started", turn_id: command, input: "child" }));
  store
    .getState()
    .dispatch(event(child, 3, { type: "sample_started", turn_id: command, sample_id: "child-sample", model: "m" }));
  store.getState().dispatch(
    event(child, 4, {
      type: "ask_raised",
      ask_id: "66666666666666666666666666666666",
      call_id: "child-call",
      request: { kind: "approval", title: "Ignore", body: "child" },
    }),
  );

  assert.equal(pendingAsk(store.getState().turns), null);
  assert.equal(store.getState().children[child].turns[0].items.length, 0);
});

test("actor statuses drive running controls and complete labels", () => {
  const states: ActorStatusOut["state"][] = [
    "queued",
    "running",
    "awaiting_input",
    "idle",
    "finished",
    "failed",
    "cancelled",
    "interrupted",
  ];
  assert.deepEqual(states.map(actorStatusLabel), [
    "Queued",
    "Running",
    "Awaiting input",
    "Idle — awaiting parent",
    "Finished",
    "Failed",
    "Cancelled",
    "Interrupted",
  ]);
  assert.equal(composerDisabled(true, true, false, false), false);
  assert.equal(composerDisabled(true, true, false, true), true);
});

test("root journal drives busy state immediately and clears before the next poll", () => {
  const store = createChatStore(root);
  store.getState().setActors([actor(root, "idle", null)]);

  store.getState().dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "queued" }));
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), true);

  store.getState().dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "queued" }));
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), true);

  store.getState().setActors([actor(root, "running", null)]);
  store.getState().dispatch(event(root, 3, { type: "turn_completed", turn_id: command, stop_reason: "completed" }));
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), false);

  store.getState().setActors([actor(root, "running", null), actor(child, "queued")]);
  assert.equal(treeRunning(store.getState().turns, store.getState().actors), true);
});
