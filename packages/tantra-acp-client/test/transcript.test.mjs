import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import {
  addRequest,
  apply,
  askOf,
  descendants,
  empty,
  pendingRequest,
  removeRequest,
  reset,
  treeRunning,
} from "../dist/index.js";

const golden = JSON.parse(
  readFileSync(new URL("./fixtures/session.json", import.meta.url), "utf8"),
);
const profile = (name) =>
  JSON.parse(
    readFileSync(
      new URL(
        `../../tantra/tests/fixtures/acp_profile/${name}.json`,
        import.meta.url,
      ),
      "utf8",
    ),
  ).params;
const ROOT = golden.live[0].sessionId;
const CHILD = golden.mirror[0].update.sessionId;
const UPSERTS = new Set([
  "user_message",
  "agent_message",
  "agent_thought",
  "tool_call_update",
  "session_message",
  "state_update",
  "subagent_update",
  "session_info_update",
  "config_option_update",
  "_tantra_queue_update",
  "_tantra_writer",
]);

const reduce = (frames, t = empty()) =>
  frames.reduce(
    (acc, { sessionId, update }) => apply(acc, sessionId, update),
    t,
  );
const transcriptOf = (t) => ({
  entries: t.sessions[ROOT].entries,
  state: t.sessions[ROOT].state,
  roster: t.roster,
});
const cursorOf = (update) => update._meta?.tantra?.cursor;
const clone = (value) => JSON.parse(JSON.stringify(value));
const messages = (t) =>
  t.sessions[ROOT].entries
    .filter((entry) => entry.kind === "message")
    .map(({ sender, recipient, text, reply }) => [
      sender,
      recipient,
      text,
      reply,
    ]);
const statuses = (t, id = ROOT) =>
  t.sessions[id].entries.filter((entry) => entry.kind === "status");
const requested = ({ method, params }) => {
  const results = [];
  const request = {
    method,
    sessionId: params.sessionId,
    params,
    signal: new AbortController().signal,
    respond: (result) => results.push(result),
  };
  return { request, results };
};

test("replay from start reduces to the live transcript", () => {
  const live = reduce(golden.live);
  assert.deepEqual(transcriptOf(reduce(golden.start)), transcriptOf(live));
  const { entries } = live.sessions[ROOT];
  assert.deepEqual(
    entries.map((entry) => entry.kind),
    [
      "user",
      "thought",
      "agent",
      "tool",
      "tool",
      "message",
      "agent",
      "message",
      "message",
      "agent",
      "message",
      "agent",
      "user",
      "agent",
      "status",
    ],
  );
  const [user, thought, agent, deploy, survey] = entries;
  assert.deepEqual(user.content, [{ type: "text", text: "Run the checks." }]);
  assert.deepEqual(
    [thought.text, thought.final, agent.text, agent.final],
    ["Plan the checks first.", true, "Checking the system.", true],
  );
  assert.deepEqual(
    [deploy.name, deploy.status, deploy.rawOutput, deploy.progress],
    ["deploy", "completed", "deployed", []],
  );
  assert.deepEqual(
    [survey.toolKind, survey.rawInput, survey.rawOutput, survey.progress],
    ["other", {}, "Ada:blue", ["Collecting answers"]],
  );
  assert.deepEqual(
    [entries.at(-2).text, entries.at(-2).final],
    ["Working ", false],
  );
  assert.deepEqual(entries.at(-1), {
    kind: "status",
    id: golden.live.at(-1).update._meta.tantra.cursor,
    stopReason: "cancelled",
  });
  assert.deepEqual(live.sessions[ROOT].state, {
    state: "idle",
    stopReason: "cancelled",
  });
  assert.deepEqual(live.roster, {
    [CHILD]: { parentId: ROOT, title: "scout", state: null },
  });
});

test("the live prefix plus a cursor replay equals the full live stream", () => {
  const cut = golden.live.findIndex(
    ({ update }) => cursorOf(update) === golden.after.cursor,
  );
  assert.ok(cut > 0 && cut < golden.live.length - 1);
  const prefix = reduce(golden.live.slice(0, cut + 1));
  assert.deepEqual(reduce(golden.after.updates, prefix), reduce(golden.live));
  const view = prefix.sessions[ROOT];
  assert.deepEqual(
    [view.writerReason, view.title, view.updatedAt, view.queue],
    ["claimed", null, golden.live[0].update.updatedAt, []],
  );
});

test("profile fixtures set the queue, writer reason and roster", () => {
  const queued = profile("queue_update");
  let t = apply(empty(), queued.sessionId, queued.update);
  assert.deepEqual(t.sessions[queued.sessionId].queue, queued.update.entries);
  for (const name of ["writer_claimed", "writer_replaced", "writer_lost"]) {
    const { sessionId, update } = profile(name);
    t = apply(t, sessionId, update);
    assert.equal(t.sessions[sessionId].writerReason, update.reason);
    assert.equal(apply(t, sessionId, update), t);
  }
  t = apply(t, queued.sessionId, { ...queued.update, entries: [] });
  assert.deepEqual(t.sessions[queued.sessionId].queue, []);
  const mirror = profile("mirror_subagent_update");
  t = apply(t, mirror.sessionId, mirror.update);
  const child = mirror.update.sessionId;
  assert.deepEqual(t.roster[child], {
    parentId: mirror.sessionId,
    title: "scout",
    state: { state: "running" },
  });
  const done = { state: "idle", stopReason: "end_turn" };
  t = apply(t, mirror.sessionId, {
    sessionUpdate: "subagent_update",
    sessionId: child,
    state: done,
  });
  assert.deepEqual(t.roster[child], {
    parentId: mirror.sessionId,
    title: "scout",
    state: done,
  });
});

test("a mirror state before the cursored announcement keeps its state", () => {
  const mirror = profile("mirror_subagent_update");
  const child = mirror.update.sessionId;
  let t = apply(empty(), mirror.sessionId, mirror.update);
  t = apply(t, mirror.sessionId, {
    sessionUpdate: "subagent_update",
    sessionId: child,
    title: "scout",
    _meta: { tantra: { cursor: "4.0" } },
  });
  assert.deepEqual(t.roster[child], {
    parentId: mirror.sessionId,
    title: "scout",
    state: { state: "running" },
  });
});

test("asks normalize all three kinds and answer in the server's shapes", () => {
  const [approval, text, choice] = golden.requests.map(requested);
  const asks = [approval, text, choice].map(({ request }) => askOf(request));
  assert.deepEqual(
    asks.map(({ kind, title, body, options }) => ({
      kind,
      title,
      body,
      options,
    })),
    [
      {
        kind: "approval",
        title: "Run deploy?",
        body: "{}",
        options: ["allow", "deny"],
      },
      { kind: "free_text", title: "Name?", body: null, options: [] },
      { kind: "choice", title: "Color?", body: null, options: ["red", "blue"] },
    ],
  );
  asks[0].respond(true);
  asks[0].respond(false);
  asks[1].respond("Ada");
  asks[2].respond("blue");
  assert.deepEqual(approval.results, [
    golden.requests[0].result,
    { outcome: { outcome: "selected", optionId: "deny" } },
  ]);
  assert.deepEqual(text.results, [golden.requests[1].result]);
  assert.deepEqual(choice.results, [golden.requests[2].result]);

  const askIds = golden.requests.map(({ params }) => params._meta.tantra.askId);
  assert.deepEqual(
    asks.map((ask) => ask.askId),
    askIds,
  );
  let t = empty();
  for (const { request } of [approval, text, choice])
    t = addRequest(t, request);
  assert.equal(addRequest(t, approval.request), t);
  assert.deepEqual(Object.keys(t.requests[ROOT]), askIds);
  assert.equal(pendingRequest(t, ROOT), approval.request);
  t = removeRequest(t, ROOT, askIds[0]);
  assert.equal(pendingRequest(t, ROOT), text.request);
  assert.equal(removeRequest(t, ROOT, askIds[0]), t);
  assert.equal(removeRequest(t, "unknown", askIds[1]), t);
  t = removeRequest(removeRequest(t, ROOT, askIds[1]), ROOT, askIds[2]);
  assert.deepEqual(t.requests, {});
  assert.equal(pendingRequest(t, ROOT), undefined);
});

test("subagent messages flow both ways and an idle child wakes again", () => {
  let t = empty();
  let position = 0;
  const seen = [];
  for (const { at, sessionId, update } of golden.mirror) {
    t = apply(reduce(golden.live.slice(position, at), t), sessionId, update);
    position = at;
    seen.push({
      child: t.roster[CHILD],
      messages: messages(t).length,
      running: treeRunning(t, ROOT),
    });
  }
  const scout = (state) => ({ parentId: ROOT, title: "scout", state });
  assert.deepEqual(seen, [
    { child: scout({ state: "running" }), messages: 1, running: true },
    {
      child: scout({ state: "idle", stopReason: "end_turn" }),
      messages: 2,
      running: true,
    },
    { child: scout({ state: "running" }), messages: 3, running: true },
    {
      child: scout({ state: "idle", stopReason: "end_turn" }),
      messages: 4,
      running: false,
    },
  ]);
  assert.deepEqual(messages(t), [
    [ROOT, CHILD, "Look around.", false],
    [CHILD, ROOT, "First pass done.", true],
    [ROOT, CHILD, "Look again.", false],
    [CHILD, ROOT, "The answer is 42.", true],
  ]);
  assert.deepEqual(descendants(t, ROOT), [CHILD]);
});

test("only cursored idle stops add a status entry", () => {
  const live = reduce(golden.live);
  const snapshot = {
    sessionUpdate: "state_update",
    ...live.sessions[ROOT].state,
  };
  const start = reduce(golden.start);
  assert.equal(apply(start, ROOT, snapshot), start);
  assert.equal(statuses(start).length, 1);
  const error = { code: -32603, message: "boom" };
  const failed = {
    sessionUpdate: "state_update",
    state: "idle",
    stopReason: "error",
    error,
  };
  let t = apply(empty(), "s", failed);
  assert.deepEqual(t.sessions.s.entries, []);
  assert.deepEqual(t.sessions.s.state, {
    state: "idle",
    stopReason: "error",
    error,
  });
  t = apply(t, "s", { ...failed, _meta: { tantra: { cursor: "4.0" } } });
  assert.deepEqual(statuses(t, "s"), [
    { kind: "status", id: "4.0", stopReason: "error", error },
  ]);
  const stops = [
    ["5.0", "_tantra_interrupted"],
    ["6.0", "end_turn"],
    ["7.0", "_tantra_finished"],
  ];
  for (const [cursor, stopReason] of stops)
    t = apply(t, "s", {
      sessionUpdate: "state_update",
      state: "idle",
      stopReason,
      _meta: { tantra: { cursor } },
    });
  assert.deepEqual(
    statuses(t, "s").map((entry) => entry.stopReason),
    ["error", "_tantra_interrupted"],
  );
  const asked = golden.live.find(
    ({ update }) => update._meta?.tantra?.ask?.state === "pending",
  ).update;
  assert.deepEqual(apply(t, "s", asked).sessions.s.state, {
    state: "requires_action",
    ask: asked._meta.tantra.ask,
  });
});

test("duplicates and ignored kinds keep the same reference", () => {
  let t = empty();
  for (const { sessionId, update } of golden.live) {
    t = apply(t, sessionId, update);
    if (UPSERTS.has(update.sessionUpdate))
      assert.equal(apply(t, sessionId, clone(update)), t, update.sessionUpdate);
  }
  for (const { sessionId, update } of golden.mirror) {
    t = apply(t, sessionId, update);
    assert.equal(apply(t, sessionId, clone(update)), t);
  }
  const ignored = [
    { sessionUpdate: "usage_update", used: 5, size: 10 },
    {
      sessionUpdate: "compaction_update",
      compactionId: "c",
      status: "completed",
    },
    {
      sessionUpdate: "compaction_summary_chunk",
      compactionId: "c",
      content: { type: "text", text: "brief" },
    },
    {
      sessionUpdate: "_tantra_notice",
      level: "warning",
      code: "writer_required",
      message: "Writer required",
    },
    { sessionUpdate: "_tantra_unknown" },
  ];
  for (const update of ignored) {
    assert.equal(apply(t, ROOT, update), t);
    assert.equal(apply(t, "elsewhere", update), t);
  }
  const done = statuses(t).at(-1);
  const ended = golden.live.findLast(
    ({ update }) => cursorOf(update) === done.id,
  );
  assert.equal(apply(t, ROOT, ended.update), t);
  const chunked = apply(t, ROOT, {
    sessionUpdate: "tool_call_content_chunk",
    toolCallId: t.sessions[ROOT].entries[3].id,
    content: { type: "content", content: { type: "text", text: "late" } },
  });
  assert.equal(chunked, t);
});

test("selectors walk the roster and the pending requests", () => {
  const grandchild = "00000000-0000-4000-8000-0000000000c2";
  const running = { state: "running" };
  const idle = { state: "idle", stopReason: "end_turn" };
  const child = (sessionId, state) => ({
    sessionUpdate: "subagent_update",
    sessionId,
    title: "worker",
    state,
  });
  let t = apply(empty(), ROOT, { sessionUpdate: "state_update", ...idle });
  t = apply(t, ROOT, child(CHILD, idle));
  t = apply(t, CHILD, child(grandchild, running));
  assert.deepEqual(descendants(t, ROOT), [CHILD, grandchild]);
  assert.deepEqual(descendants(t, CHILD), [grandchild]);
  assert.deepEqual(descendants(t, grandchild), []);
  assert.equal(treeRunning(t, ROOT), true);
  assert.equal(treeRunning(t, CHILD), true);
  t = apply(t, CHILD, child(grandchild, idle));
  assert.equal(treeRunning(t, ROOT), false);
  assert.equal(treeRunning(t, "unknown"), false);
  t = apply(t, ROOT, {
    sessionUpdate: "state_update",
    state: "requires_action",
  });
  assert.equal(treeRunning(t, ROOT), true);
  assert.equal(treeRunning(t, CHILD), false);

  const { request } = requested(golden.requests[0]);
  t = addRequest(t, request);
  assert.equal(pendingRequest(t, ROOT), request);
  assert.equal(pendingRequest(t, CHILD), undefined);
  const cleared = reset(t, ROOT);
  assert.equal(cleared.sessions[ROOT], undefined);
  assert.deepEqual(cleared.requests, {});
  assert.equal(cleared.roster, t.roster);
  assert.equal(reset(cleared, ROOT), cleared);
});

test("thought blocks are joined by a blank line while streaming and when final", () => {
  const block = (text) => ({ type: "text", text });
  const chunk = (text) => ({
    sessionUpdate: "agent_thought_chunk",
    messageId: "m",
    content: block(text),
  });
  const thought = (...texts) => ({
    sessionUpdate: "agent_thought",
    messageId: "m",
    content: texts.map(block),
  });
  let t = apply(empty(), "s", chunk("First."));
  t = apply(t, "s", thought("First."));
  t = apply(t, "s", chunk("Second."));
  const streaming = t.sessions.s.entries[0];
  assert.deepEqual(
    [streaming.text, streaming.final],
    ["First.\n\nSecond.", false],
  );
  t = apply(t, "s", thought("First.", "Second."));
  assert.deepEqual(t.sessions.s.entries, [
    { kind: "thought", id: "m", text: "First.\n\nSecond.", final: true },
  ]);
  t = apply(t, "s", {
    sessionUpdate: "agent_message",
    messageId: "m",
    content: [block("a"), block("b")],
  });
  assert.equal(t.sessions.s.entries[1].text, "ab");
});
