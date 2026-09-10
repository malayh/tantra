import assert from "node:assert/strict";
import test from "node:test";

import type { EventFrame } from "../../generated/models/eventFrame.ts";
import { createChatStore, runningDescendants, subscriptionFrames } from "./state.ts";

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

test("reduces independent actor journals with cursors and nested children", () => {
  const store = createChatStore(root);
  const dispatch = store.getState().dispatch;
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "research" }));
  dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "research" }));
  dispatch(event(root, 3, { type: "sample_started", turn_id: command, sample_id: "s1", model: "m" }));
  dispatch(
    event(root, 4, {
      type: "tool_call_requested",
      turn_id: command,
      sample_id: "s1",
      call_id: "spawn",
      name: "spawn",
      args: { input: "look" },
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
      type: "input_queued",
      command_id: "55555555555555555555555555555555",
      input: `[agent ${child} finished] result`,
    }),
  );
  dispatch(event(root, 7, { type: "text_delta", text: "root answer" }));
  dispatch(event(root, 8, { type: "turn_completed", turn_id: command, stop_reason: "completed" }));
  dispatch(event(child, 1, { type: "sample_started", turn_id: "c", sample_id: "s2", model: "m" }));
  dispatch(
    event(child, 2, {
      type: "tool_call_requested",
      turn_id: "c",
      sample_id: "s2",
      call_id: "nested",
      name: "spawn",
      args: { input: "deeper" },
    }),
  );
  dispatch(
    event(child, 3, {
      type: "child_created",
      child_id: grandchild,
      agent: "researcher",
      turn_id: "c",
      call_id: "nested",
    }),
  );
  dispatch(event(grandchild, 1, { type: "sample_started", turn_id: "g", sample_id: "s3", model: "m" }));
  dispatch(event(grandchild, 2, { type: "text_delta", text: "deep" }));
  dispatch(event(grandchild, 2, { type: "text_delta", text: "duplicate" }));
  dispatch(event(grandchild, 3, { type: "agent_finished", result: "done" }));
  assert.equal(store.getState().active[grandchild], false);
  dispatch(
    event(child, 4, {
      type: "input_queued",
      command_id: "66666666666666666666666666666666",
      input: `[agent ${grandchild} finished] done`,
    }),
  );
  assert.equal(store.getState().active[grandchild], false);

  const turn = store.getState().turns[0];
  assert.equal(store.getState().turns[1].items.length, 0);
  assert.equal(store.getState().active[root], true);
  assert.equal(turn.items[1].content, "root answer");
  const researcher = turn.items[0];
  assert.equal(researcher.kind, "subagent");
  if (researcher.kind !== "subagent") return;
  const nested = researcher.items[0];
  assert.equal(nested.kind, "subagent");
  if (nested.kind !== "subagent") return;
  assert.equal(nested.items[0].content, "deep");
  assert.equal(nested.final, true);
  assert.equal(store.getState().cursors[grandchild], 3);
});

test("queues once, hides synthetic input, and interrupts stale replay", () => {
  const store = createChatStore(root);
  store
    .getState()
    .dispatch(
      event(root, 1, { type: "input_queued", command_id: command, input: `[agent ${child} finished]\nresult` }),
    );
  store.getState().dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "duplicate" }));
  assert.equal(store.getState().turns.length, 1);
  assert.equal(store.getState().turns[0].synthetic, true);
  assert.equal(store.getState().turns[0].status, "queued");

  store.getState().subscriptionReady({ type: "subscription_ready", agent_id: root, seq: 1, active: false });
  assert.equal(store.getState().turns[0].status, "interrupted");
  assert.equal(store.getState().ready, true);
});

test("reconnect subscribes every discovered actor at its own cursor", () => {
  const store = createChatStore(root);
  const dispatch = store.getState().dispatch;
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "research" }));
  dispatch(
    event(root, 2, {
      type: "child_created",
      child_id: child,
      agent: "researcher",
      turn_id: command,
      call_id: "spawn",
    }),
  );
  dispatch(
    event(child, 1, {
      type: "input_queued",
      command_id: "55555555555555555555555555555555",
      input: "look",
    }),
  );

  assert.deepEqual(subscriptionFrames(store.getState(), root), [
    { type: "subscribe", agent_id: root, after: 2, writable: true },
    { type: "subscribe", agent_id: child, after: 1, writable: false },
  ]);
});

test("tracks descendant queued running terminal and finish activity", () => {
  const store = createChatStore(root);
  const dispatch = store.getState().dispatch;
  const first = "55555555555555555555555555555555";
  const second = "66666666666666666666666666666666";
  const third = "77777777777777777777777777777777";
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "research" }));
  dispatch(
    event(root, 2, {
      type: "child_created",
      child_id: child,
      agent: "researcher",
      turn_id: command,
      call_id: "spawn",
    }),
  );
  const block = () => {
    const item = store.getState().turns[0].items[0];
    assert.equal(item.kind, "subagent");
    if (item.kind !== "subagent") throw new Error("missing child");
    return item;
  };

  dispatch(event(child, 1, { type: "input_queued", command_id: first, input: "first" }));
  assert.equal(store.getState().active[child], true);
  dispatch(event(root, 3, { type: "input_queued", command_id: second, input: `[agent ${child}] update` }));
  assert.equal(store.getState().active[child], true);
  dispatch(event(child, 2, { type: "turn_started", turn_id: first, input: "first" }));
  dispatch(event(child, 3, { type: "input_queued", command_id: second, input: "second" }));
  dispatch(event(child, 4, { type: "turn_completed", turn_id: first, stop_reason: "completed" }));
  assert.equal(store.getState().active[child], true);
  dispatch(event(child, 5, { type: "turn_started", turn_id: second, input: "second" }));
  dispatch(event(child, 6, { type: "turn_completed", turn_id: second, stop_reason: "completed" }));
  assert.equal(store.getState().active[child], false);
  dispatch(event(child, 7, { type: "input_queued", command_id: third, input: "third" }));
  assert.equal(store.getState().active[child], true);
  dispatch(event(child, 8, { type: "turn_failed", turn_id: third, error: "failed" }));
  assert.equal(store.getState().active[child], false);
  assert.equal(block().final, true);
  assert.equal(block().isError, true);
  const history = block().items;
  dispatch(event(child, 9, { type: "turn_started", turn_id: third, input: "third" }));
  assert.equal(store.getState().active[child], true);
  assert.equal(block().final, false);
  assert.equal(block().isError, false);
  assert.equal(block().result, undefined);
  assert.equal(block().items, history);
  dispatch(event(child, 10, { type: "turn_cancelled", turn_id: third, reason: "cancelled" }));
  assert.equal(store.getState().active[child], false);
  assert.equal(block().final, true);
  dispatch(event(child, 11, { type: "turn_started", turn_id: third, input: "third" }));
  assert.equal(store.getState().active[child], true);
  assert.equal(block().final, false);
  dispatch(event(child, 12, { type: "turn_interrupted", turn_id: third, reason: "stopped" }));
  assert.equal(store.getState().active[child], false);
  assert.equal(block().final, true);
  dispatch(event(child, 13, { type: "turn_started", turn_id: third, input: "third" }));
  assert.equal(store.getState().active[child], true);
  assert.equal(block().final, false);
  dispatch(event(child, 14, { type: "agent_finished", result: "done" }));
  assert.equal(store.getState().active[child], false);
  assert.equal(block().final, true);
  assert.equal(block().finished, true);
  dispatch(event(child, 15, { type: "input_queued", command_id: third, input: "again" }));
  assert.equal(store.getState().active[child], false);
  assert.equal(block().final, true);
  dispatch(
    event(root, 4, {
      type: "input_queued",
      command_id: third,
      input: `[agent ${child} finished] done`,
    }),
  );
  assert.equal(store.getState().active[child], false);
});

test("canonical actor ids stay synthetic and finish their child block", () => {
  const store = createChatStore(root);
  const dispatch = store.getState().dispatch;
  const canonical = "22222222-2222-2222-2222-222222222222";
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "research" }));
  dispatch(
    event(root, 2, {
      type: "child_created",
      child_id: canonical,
      agent: "researcher",
      turn_id: command,
      call_id: "spawn",
    }),
  );
  dispatch(
    event(root, 3, {
      type: "input_queued",
      command_id: "55555555555555555555555555555555",
      input: `[agent ${canonical} finished] result`,
    }),
  );
  dispatch(event(canonical, 1, { type: "agent_finished", result: "result" }));

  const childBlock = store.getState().turns[0].items[0];
  assert.equal(store.getState().turns[1].synthetic, true);
  assert.equal(childBlock.kind, "subagent");
  if (childBlock.kind !== "subagent") return;
  assert.equal(childBlock.final, true);
  assert.equal(childBlock.finished, true);
});

test("shows every running descendant and stops the latest human turn after tree cancellation", () => {
  const store = createChatStore(root);
  const dispatch = store.getState().dispatch;
  const childTurn = "55555555555555555555555555555555";
  const grandchildTurn = "66666666666666666666666666666666";
  dispatch(event(root, 1, { type: "input_queued", command_id: command, input: "research" }));
  dispatch(event(root, 2, { type: "turn_started", turn_id: command, input: "research" }));
  dispatch(
    event(root, 3, { type: "child_created", child_id: child, agent: "researcher", turn_id: command, call_id: "child" }),
  );
  dispatch(
    event(child, 1, {
      type: "child_created",
      child_id: grandchild,
      agent: "researcher",
      turn_id: childTurn,
      call_id: "grandchild",
    }),
  );
  dispatch(event(root, 4, { type: "turn_completed", turn_id: command, stop_reason: "completed" }));
  dispatch(event(child, 2, { type: "turn_started", turn_id: childTurn, input: "child" }));
  dispatch(event(grandchild, 1, { type: "turn_started", turn_id: grandchildTurn, input: "grandchild" }));

  assert.deepEqual(runningDescendants(store.getState().turns, store.getState().active), [
    { id: child, agent: "researcher" },
    { id: grandchild, agent: "researcher" },
  ]);

  dispatch(
    event(root, 5, {
      type: "cancellation_requested",
      command_id: "77777777777777777777777777777777",
      targets: { [child]: [childTurn], [grandchild]: [grandchildTurn] },
    }),
  );
  dispatch(event(child, 3, { type: "turn_cancelled", turn_id: childTurn, reason: "cancelled" }));
  assert.equal(store.getState().turns[0].status, "done");
  dispatch(event(grandchild, 2, { type: "turn_cancelled", turn_id: grandchildTurn, reason: "cancelled" }));
  assert.equal(store.getState().turns[0].status, "cancelled");
  assert.deepEqual(runningDescendants(store.getState().turns, store.getState().active), []);
});
