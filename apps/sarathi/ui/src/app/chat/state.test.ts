import assert from "node:assert/strict";
import test from "node:test";

import type { Emitted } from "@/generated/models";
import { createChatStore, pendingMessages, type SubagentItem } from "./state.ts";

const root = "root";
const child = "child";
const leaf = "leaf";

const frame = (sessionId: string, depth: number, event: Record<string, unknown>): Emitted =>
  ({ session_id: sessionId, depth, seq: 1, event }) as Emitted;

const rootStart = frame(root, 0, { type: "turn_started", turn_id: "turn", input: "start" });
const launchChild = [
  frame(root, 0, { type: "sample_started", turn_id: "turn", sample_id: "root-sample", model: "test" }),
  frame(root, 0, {
    type: "tool_call_requested",
    sample_id: "root-sample",
    call_id: "launch-child",
    name: "researcher",
    args: { task: "research" },
  }),
  frame(root, 0, {
    type: "child_session_spawned",
    call_id: "launch-child",
    child_session_id: child,
    agent: "researcher",
  }),
  frame(root, 0, {
    type: "tool_call_completed",
    call_id: "launch-child",
    result: { task_id: child, agent: "researcher" },
    is_error: false,
  }),
];

const task = (items: SubagentItem["items"], id: string): SubagentItem => {
  for (const item of items) {
    if (item.kind !== "subagent") continue;
    if (item.childSessionId === id) return item;
    try {
      return task(item.items, id);
    } catch {}
  }
  throw new Error(`missing task ${id}`);
};

test("routes nested tasks and keeps launch completion separate from task completion", () => {
  const store = createChatStore(root);
  const frames = [
    rootStart,
    ...launchChild,
    frame(child, 1, { type: "turn_started", turn_id: "child-turn", input: "research" }),
    frame(child, 1, { type: "sample_started", turn_id: "child-turn", sample_id: "child-sample", model: "test" }),
    frame(child, 1, {
      type: "tool_call_requested",
      sample_id: "child-sample",
      call_id: "launch-leaf",
      name: "investigator",
      args: { task: "verify" },
    }),
    frame(child, 1, {
      type: "child_session_spawned",
      call_id: "launch-leaf",
      child_session_id: leaf,
      agent: "investigator",
    }),
    frame(leaf, 2, { type: "turn_started", turn_id: "leaf-turn", input: "verify" }),
    frame(leaf, 2, {
      type: "agent_message_queued",
      message_id: "parent-message",
      sender_session_id: child,
      source: "parent",
      text: "focus here",
    }),
  ];
  for (const item of frames) store.getState().dispatch(item);

  const turn = store.getState().turns[0];
  const researcher = task(turn.items, child);
  const investigator = task(turn.items, leaf);
  assert.equal(turn.status, "running");
  assert.equal(researcher.launchComplete, true);
  assert.equal(researcher.final, false);
  assert.equal(researcher.state, "running");
  assert.equal(investigator.state, "running");
  assert.deepEqual(
    investigator.items.filter((item) => item.kind === "message").map((item) => item.content),
    ["focus here"],
  );
});

test("derives every task state and child completion never completes the root", () => {
  const store = createChatStore(root);
  for (const item of [rootStart, ...launchChild]) store.getState().dispatch(item);
  const dispatch = (event: Record<string, unknown>) => store.getState().dispatch(frame(child, 1, event));

  assert.equal(task(store.getState().turns[0].items, child).state, "queued");
  dispatch({ type: "turn_started", turn_id: "child-turn", input: "research" });
  assert.equal(task(store.getState().turns[0].items, child).state, "running");
  dispatch({ type: "tool_call_requested", sample_id: "s", call_id: "wait", name: "task_wait", args: {} });
  assert.equal(task(store.getState().turns[0].items, child).state, "waiting");
  dispatch({ type: "tool_call_completed", call_id: "wait", result: {}, is_error: false });
  dispatch({
    type: "ask_raised",
    ask_id: "ask",
    request: { kind: "free_text", prompt: "Need input" },
  });
  assert.equal(task(store.getState().turns[0].items, child).state, "awaiting_input");
  dispatch({ type: "ask_answered", ask_id: "ask", response: { kind: "free_text", text: "answer" } });
  assert.equal(task(store.getState().turns[0].items, child).state, "running");
  dispatch({ type: "turn_completed", turn_id: "child-turn", stop_reason: "completed" });
  assert.equal(task(store.getState().turns[0].items, child).state, "completed");
  assert.equal(store.getState().turns[0].status, "running");
  store.getState().dispatch(frame(root, 0, { type: "turn_completed", turn_id: "turn", stop_reason: "completed" }));
  assert.equal(store.getState().turns[0].status, "done");
});

test("deduplicates messages and notices and replaying frames twice is stable", () => {
  const store = createChatStore(root);
  const frames = [
    rootStart,
    ...launchChild,
    frame(root, 0, {
      type: "agent_message_queued",
      message_id: "user-message",
      sender_session_id: null,
      source: "user",
      text: "guidance",
    }),
    frame(root, 0, {
      type: "agent_message_queued",
      message_id: "child-message",
      sender_session_id: child,
      source: "child",
      text: "finding",
    }),
    frame(root, 0, {
      type: "task_notice_queued",
      notice_id: "notice",
      task_session_id: child,
      state: "killed",
      terminal_seq: 8,
    }),
    frame(child, 1, { type: "kill_requested", request_id: "kill", requested_by_session_id: root }),
  ];
  for (const item of frames) store.getState().dispatch(item);
  const once = structuredClone(store.getState().turns);
  for (const item of frames) store.getState().dispatch(item);

  assert.deepEqual(store.getState().turns, once);
  assert.deepEqual(store.getState().messageIds, ["user-message", "child-message"]);
  assert.deepEqual(store.getState().noticeIds, ["notice"]);
  assert.equal(task(store.getState().turns[0].items, child).state, "killed");
  const messages = store.getState().turns[0].items.filter((item) => item.kind === "message");
  assert.equal(messages.length, 2);
  assert.deepEqual(messages[0].attachments, []);
});

test("parses attachments from durable active user messages without exposing markers", () => {
  const store = createChatStore(root);
  store.getState().dispatch(rootStart);
  store.getState().dispatch(
    frame(root, 0, {
      type: "agent_message_queued",
      message_id: "attachment-message",
      sender_session_id: null,
      source: "user",
      text: "read it\n[attachment: note.pdf path=/uploads/note.pdf]",
    }),
  );
  const message = store.getState().turns[0].items.find((item) => item.kind === "message");
  assert.equal(message?.content, "read it");
  assert.deepEqual(message?.attachments, [{ name: "note.pdf", path: "/uploads/note.pdf" }]);
});

test("tracks a failed task and root task_wait separately", () => {
  const store = createChatStore(root);
  for (const item of [rootStart, ...launchChild]) store.getState().dispatch(item);
  store.getState().dispatch(frame(child, 1, { type: "turn_started", turn_id: "child-turn", input: "research" }));
  store.getState().dispatch(frame(child, 1, { type: "turn_failed", turn_id: "child-turn", error: "boom" }));
  assert.equal(task(store.getState().turns[0].items, child).state, "failed");
  store
    .getState()
    .dispatch(
      frame(root, 0, { type: "tool_call_requested", sample_id: "root-sample", call_id: "wait", name: "task_wait" }),
    );
  assert.equal(store.getState().turns[0].status, "waiting");
  store.getState().dispatch(frame(root, 0, { type: "tool_call_completed", call_id: "wait", result: {} }));
  assert.equal(store.getState().turns[0].status, "running");
});

test("only matching task_wait completion clears root and nested waiting", () => {
  const store = createChatStore(root);
  for (const item of [rootStart, ...launchChild]) store.getState().dispatch(item);
  store.getState().dispatch(frame(child, 1, { type: "turn_started", turn_id: "child-turn", input: "research" }));
  store
    .getState()
    .dispatch(
      frame(root, 0, { type: "tool_call_requested", sample_id: "root-sample", call_id: "status", name: "task_status" }),
    );
  store.getState().dispatch(
    frame(root, 0, {
      type: "tool_call_requested",
      sample_id: "root-sample",
      call_id: "root-wait",
      name: "task_wait",
    }),
  );
  store.getState().dispatch(frame(root, 0, { type: "tool_call_completed", call_id: "status", result: {} }));
  assert.equal(store.getState().turns[0].status, "waiting");
  assert.equal(store.getState().turns[0].waitCallId, "root-wait");

  store.getState().dispatch(
    frame(child, 1, {
      type: "tool_call_requested",
      sample_id: "child-sample",
      call_id: "messages",
      name: "task_messages",
    }),
  );
  store.getState().dispatch(
    frame(child, 1, {
      type: "tool_call_requested",
      sample_id: "child-sample",
      call_id: "child-wait",
      name: "task_wait",
    }),
  );
  store.getState().dispatch(frame(child, 1, { type: "tool_call_completed", call_id: "messages", result: {} }));
  assert.equal(task(store.getState().turns[0].items, child).state, "waiting");
  assert.equal(task(store.getState().turns[0].items, child).waitCallId, "child-wait");

  store.getState().dispatch(frame(child, 1, { type: "tool_call_completed", call_id: "child-wait", result: {} }));
  assert.equal(task(store.getState().turns[0].items, child).state, "running");
  assert.equal(task(store.getState().turns[0].items, child).waitCallId, null);
  store.getState().dispatch(frame(root, 0, { type: "tool_call_completed", call_id: "root-wait", result: {} }));
  assert.equal(store.getState().turns[0].status, "running");
  assert.equal(store.getState().turns[0].waitCallId, null);
});

test("retains pending delivery until matching acceptance and preserves rejected drafts", () => {
  const saved = new Map<string, string>();
  Object.defineProperty(globalThis, "sessionStorage", {
    configurable: true,
    value: {
      getItem: (key: string) => saved.get(key) ?? null,
      removeItem: (key: string) => saved.delete(key),
      setItem: (key: string, value: string) => saved.set(key, value),
    },
  });

  try {
    const pending = pendingMessages.create("keep me", [{ name: "note.pdf", path: "/note.pdf" }]);
    pendingMessages.set(root, pending);

    assert.match(pending.requestId, /^[0-9a-f-]{36}$/);
    assert.deepEqual(pendingMessages.get(root), pending);
    assert.equal(pendingMessages.accept(root, "wrong"), false);
    assert.deepEqual(pendingMessages.get(root), pending);

    const rejected = pendingMessages.reject(root, pending.requestId);
    assert.deepEqual(rejected, { ...pending, rejected: true });
    assert.deepEqual(pendingMessages.get(root), rejected);

    const retry = pendingMessages.create("edited", []);
    pendingMessages.set(root, retry);
    assert.equal(pendingMessages.accept(root, retry.requestId), true);
    assert.equal(pendingMessages.get(root), null);
  } finally {
    Reflect.deleteProperty(globalThis, "sessionStorage");
  }
});

test("drops malformed pending delivery storage", () => {
  Object.defineProperty(globalThis, "sessionStorage", {
    configurable: true,
    value: {
      getItem: () => "not json",
      removeItem: () => true,
      setItem: () => undefined,
    },
  });
  try {
    assert.equal(pendingMessages.get(root), null);
  } finally {
    Reflect.deleteProperty(globalThis, "sessionStorage");
  }
});
