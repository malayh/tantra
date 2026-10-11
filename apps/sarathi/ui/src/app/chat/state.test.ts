import assert from "node:assert/strict";
import test from "node:test";

import type { ActorStatusOut } from "../../generated/models/index.ts";
import {
  bannerControls,
  treeRunning,
  actorStatusLabel,
  composerDisabled,
  createChatStore,
  isNearBottom,
  type ChatStore,
  pendingAsk,
  type EventFrame,
} from "./state.ts";

const root = "11111111111111111111111111111111";
const child = "22222222222222222222222222222222";
const grandchild = "33333333333333333333333333333333";
const command = "44444444444444444444444444444444";

const ready = (store: ChatStore, agentId = root) => store.getState().setSubscriptionReady(agentId, true);

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
  agent: parentId === null ? "sarathi" : "subagent",
  name: parentId === null ? "sarathi" : "Research scout",
  state,
  active: state === "running",
  current_turn_id: state === "running" ? command : null,
  last_turn: null,
  last_seq: 0,
  updated_at: "2026-01-01T00:00:00Z",
});

test("root is the only default subscription and spawn remains an ordinary tool", () => {
  const store = createChatStore(root);
  ready(store);
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
      args: { agent_name: "subagent", input: "look" },
    }),
  );
  dispatch(
    event(root, 5, {
      type: "child_created",
      child_id: child,
      agent: "subagent",
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

  assert.equal(store.getState().cursors[root], 6);
  assert.deepEqual(store.getState().children, {});
  const item = store.getState().turns[0].items[0];
  assert.equal(item.kind, "tool");
  assert.equal(item.final, true);
  if (item.kind === "tool") {
    assert.equal(item.name, "spawn");
    assert.deepEqual(item.child, { agent_id: child, agent: "subagent" });
  }
});

test("child journals keep independent cursors, history, readiness, and deduplication", () => {
  const store = createChatStore(root);
  ready(store);
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
  store.getState().setSubscriptionReady(grandchild, true);

  assert.deepEqual(store.getState().cursors, { [root]: 1, [grandchild]: 4 });
  assert.equal(store.getState().turns[0].items.length, 0);
  assert.equal(store.getState().children[grandchild].turns[0].items[0].content, "deep");
  assert.equal(store.getState().children[grandchild].ready, true);

  const retained = store.getState().children[grandchild];
  store.getState().setOpenChild(null);
  assert.equal(store.getState().children[grandchild], retained);
  store.getState().setOpenChild(grandchild);
  assert.equal(store.getState().cursors[grandchild], 4);
});

test("switching the open child reconnects only root and the selected journal", () => {
  const store = createChatStore(root);
  ready(store);
  store.getState().setOpenChild(child);
  ready(store, child);
  store.getState().dispatch(event(child, 1, { type: "input_queued", command_id: command, input: "child" }));
  store.getState().setOpenChild(grandchild);
  store.getState().dispatch(event(grandchild, 1, { type: "input_queued", command_id: command, input: "nested" }));
  ready(store, grandchild);

  assert.equal(store.getState().openChildId, grandchild);
  assert.equal(store.getState().cursors[grandchild], 1);
  assert.equal(store.getState().children[child].turns[0].input, "child");
});

test("lifecycle and finish inputs are hidden while queued human messages remain FIFO", () => {
  const store = createChatStore(root);
  ready(store);
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
  ready(store);
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
  const selectedAsk = pendingAsk(store.getState().turns);
  assert.equal(selectedAsk, askId);
  assert.equal(pendingAsk(store.getState().turns), selectedAsk);
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

  store.getState().setOpenChild(child);
  ready(store, child);
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

test("approval, choice, and free-text asks retain their request descriptors", () => {
  const store = createChatStore(root);
  ready(store);
  store.getState().dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "root" }));
  store.getState().dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "root" }));
  store
    .getState()
    .dispatch(event(root, 3, { type: "sample_started", turn_id: command, sample_id: "sample", model: "m" }));
  const requests = [
    { kind: "approval" as const, title: "Approve", body: "body" },
    { kind: "choice" as const, title: "Pick", options: ["one", "two"] },
    { kind: "free_text" as const, prompt: "Explain" },
  ];
  requests.forEach((request, index) => {
    store.getState().dispatch(
      event(root, index + 4, {
        type: "ask_raised",
        ask_id: (index + 5).toString().repeat(32),
        request,
      }),
    );
  });

  const retained = store
    .getState()
    .turns[0].items.filter((item) => item.kind === "ask")
    .map((item) => item.request);
  assert.deepEqual(retained, requests);
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
  assert.equal(composerDisabled(true, true, true, false), false);
  assert.equal(composerDisabled(true, true, false, false), true);
  assert.equal(composerDisabled(true, true, true, true), true);
  assert.equal(composerDisabled(true, true, true, false, true), true);
});

test("readiness remains separate from write authority", () => {
  const store = createChatStore(root);
  ready(store);
  assert.equal(store.getState().ready, true);
  assert.equal(composerDisabled(store.getState().ready, true, false, false), true);
});

test("retry and reclaim controls remain available together", () => {
  assert.deepEqual(bannerControls({ kind: "retry", message: "Unknown", commandId: "command" }, true), {
    reclaim: true,
    retryCommandId: "command",
  });
  assert.deepEqual(bannerControls(null, true), { reclaim: true, retryCommandId: null });
});

test("disconnect preserves transcript, cursors, and pending asks", () => {
  const store = createChatStore(root);
  const askId = "55555555555555555555555555555555";
  ready(store);
  store.getState().dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "queued" }));
  store.getState().dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "queued" }));
  store
    .getState()
    .dispatch(event(root, 3, { type: "sample_started", turn_id: command, sample_id: "sample", model: "m" }));
  store.getState().dispatch(
    event(root, 4, {
      type: "ask_raised",
      ask_id: askId,
      call_id: "call",
      request: { kind: "approval", title: "Approve", body: "root" },
    }),
  );

  store.getState().setSubscriptionReady(root, false);

  assert.equal(store.getState().ready, false);
  assert.equal(store.getState().cursors[root], 4);
  assert.equal(store.getState().turns[0].input, "queued");
  assert.equal(pendingAsk(store.getState().turns), askId);
});

test("root journal drives busy state immediately and clears before the next poll", () => {
  const store = createChatStore(root);
  ready(store);
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

test("replay and live reduction share cursor deduplication", () => {
  const buffered = createChatStore(root);
  const live = createChatStore(root);
  const frames = [
    event(root, 1, { type: "input_queued", command_id: command, input: "queued" }),
    event(root, 2, { type: "turn_started", turn_id: command, input: "queued" }),
    event(root, 2, { type: "turn_started", turn_id: command, input: "duplicate" }),
  ];

  for (const frame of frames) buffered.getState().dispatch(frame);
  ready(buffered);
  ready(live);
  for (const frame of frames) live.getState().dispatch(frame);

  assert.deepEqual(
    { cursors: buffered.getState().cursors, turns: buffered.getState().turns },
    { cursors: live.getState().cursors, turns: live.getState().turns },
  );
});

test("near-bottom threshold follows at 80px and stops above it", () => {
  assert.equal(isNearBottom(1_000, 900, 20), true);
  assert.equal(isNearBottom(1_000, 899, 20), false);
});
