import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  TantraAcpError,
  apply,
  askOf,
  empty,
  type AgentRequest,
  type ContentBlock,
  type SessionUpdate,
  type StateBody,
  type Transcript,
} from "tantra-acp-client";

import {
  actorStatusLabel,
  attachmentsOf,
  composerDisabled,
  createChatStore,
  fromDroppedSession,
  isLoading,
  isNearBottom,
  isReady,
  stopOffered,
  textOf,
  type ChatStore,
} from "./state.ts";

type Frame = { sessionId: string; update: SessionUpdate };
type Asked = {
  method: AgentRequest["method"];
  params: AgentRequest["params"];
  result: { outcome?: { optionId: string }; content?: { text?: string; choice?: string } };
};
type Golden = { live: Frame[]; mirror: (Frame & { at: number })[]; requests: Asked[] };

const golden = JSON.parse(
  readFileSync(
    new URL("../../../../../../packages/tantra-acp-client/test/fixtures/session.json", import.meta.url),
    "utf8",
  ),
) as Golden;
const ROOT = golden.live[0].sessionId;
const CHILD = (golden.mirror[0].update as { sessionId: string }).sessionId;
const askIdOf = (asked: Asked) => (asked.params._meta as { tantra: { askId: string } }).tantra.askId;

const requested = (asked: Asked, signal = new AbortController().signal) => {
  const results: unknown[] = [];
  const request = {
    method: asked.method,
    sessionId: asked.params.sessionId,
    params: asked.params,
    signal,
    respond: (result: unknown) => results.push(result),
  } as AgentRequest;
  return { request, results };
};

const notifications = (store: ChatStore) => {
  const seen = { count: 0 };
  store.subscribe(() => seen.count++);
  return seen;
};

test("the store applies updates through the client's reducer and skips no-op updates", () => {
  const store = createChatStore();
  const seen = notifications(store);
  const [info, idle] = golden.live;
  store.getState().apply(info.sessionId, info.update);
  store.getState().apply(idle.sessionId, idle.update);
  const once = store.getState().transcript;
  assert.deepEqual(once, apply(apply(empty(), info.sessionId, info.update), idle.sessionId, idle.update));
  assert.equal(seen.count, 2);

  store.getState().apply(idle.sessionId, structuredClone(idle.update));
  store.getState().apply(ROOT, { sessionUpdate: "usage_update", used: 1, size: 2 });
  assert.equal(store.getState().transcript, once);
  assert.equal(seen.count, 2);
});

test("requests go through the reducer and an abort removes only that request", () => {
  const store = createChatStore();
  const askId = askIdOf(golden.requests[0]);
  const controller = new AbortController();
  const { request } = requested(golden.requests[0], controller.signal);

  store.getState().addRequest(request);
  assert.equal(store.getState().transcript.requests[ROOT][askId], request);
  controller.abort();
  assert.deepEqual(store.getState().transcript.requests, {});

  const aborted = new AbortController();
  aborted.abort();
  store.getState().addRequest(requested(golden.requests[0], aborted.signal).request);
  assert.deepEqual(store.getState().transcript.requests, {});

  const stale = new AbortController();
  const fresh = requested(golden.requests[0]).request;
  store.getState().addRequest(requested(golden.requests[0], stale.signal).request);
  store.getState().addRequest(fresh);
  stale.abort();
  assert.equal(store.getState().transcript.requests[ROOT][askId], fresh);
  store.getState().removeRequest(ROOT, askId);
  assert.deepEqual(store.getState().transcript.requests, {});
});

test("reset clears a session's view, requests and readiness but keeps the roster", () => {
  const store = createChatStore();
  for (const { sessionId, update } of [...golden.live, ...golden.mirror]) store.getState().apply(sessionId, update);
  store.getState().addRequest(requested(golden.requests[0]).request);
  store.getState().setReady(ROOT);
  const { roster } = store.getState().transcript;
  assert.equal(store.getState().ready[ROOT], true);

  store.getState().reset(ROOT);
  assert.equal(store.getState().transcript.sessions[ROOT], undefined);
  assert.deepEqual(store.getState().transcript.requests, {});
  assert.equal(store.getState().transcript.roster, roster);
  assert.equal(store.getState().ready[ROOT], false);
});

test("attachments and text come from user content blocks", () => {
  const content: ContentBlock[] = [
    { type: "text", text: "Read these" },
    { type: "resource_link", name: "a b#1.pdf", uri: "file:///data/uploads/7/0f_a%20b%231.pdf" },
    { type: "resource_link", name: "Ünïcode.docx", uri: "file:///data/uploads/7/1e_%C3%9Cn%C3%AFcode.docx" },
    { type: "image", data: "", mimeType: "image/png" },
  ];
  assert.deepEqual(attachmentsOf(content), [
    { name: "a b#1.pdf", path: "/data/uploads/7/0f_a b#1.pdf" },
    { name: "Ünïcode.docx", path: "/data/uploads/7/1e_Ünïcode.docx" },
  ]);
  assert.equal(textOf(content), "Read these");
  assert.deepEqual(attachmentsOf([{ type: "text", text: "" }]), []);
  assert.equal(textOf([{ type: "resource_link", name: "x", uri: "file:///x" }]), "");
});

test("actor status labels cover every state", () => {
  const states: (StateBody | null)[] = [
    null,
    { state: "running" },
    { state: "requires_action" },
    { state: "idle" },
    { state: "idle", stopReason: "end_turn" },
    { state: "idle", stopReason: "max_turn_requests" },
    { state: "idle", stopReason: "_tantra_finished" },
    { state: "idle", stopReason: "error", error: { code: -32603, message: "boom" } },
    { state: "idle", stopReason: "cancelled" },
    { state: "idle", stopReason: "_tantra_interrupted" },
  ];
  assert.deepEqual(states.map(actorStatusLabel), [
    "Queued",
    "Running",
    "Awaiting input",
    "Idle — awaiting parent",
    "Replied",
    "Idle — awaiting parent",
    "Finished",
    "Failed",
    "Cancelled",
    "Interrupted",
  ]);
});

test("the composer needs readiness, a connection, write control, no pending ask and a writable view", () => {
  assert.equal(composerDisabled(true, true, true, false), false);
  assert.equal(composerDisabled(false, true, true, false), true);
  assert.equal(composerDisabled(true, false, true, false), true);
  assert.equal(composerDisabled(true, true, false, false), true);
  assert.equal(composerDisabled(true, true, true, true), true);
  assert.equal(composerDisabled(true, true, true, false, true), true);
});

test("Stop is offered only to a writable running chat without a pending ask", () => {
  assert.equal(stopOffered(true, true, false), true);
  assert.equal(stopOffered(false, true, false), false);
  assert.equal(stopOffered(true, false, false), false);
  assert.equal(stopOffered(true, true, true), false);
  assert.equal(stopOffered(true, true, false, true), false);
});

test("a session is ready only while marked ready and open in the client", () => {
  const store = createChatStore();
  store.getState().setReady(ROOT);
  assert.equal(isReady(store.getState(), ROOT), false);
  store
    .getState()
    .setClient({ connection: "open", sessions: { [ROOT]: { writer: true, cursor: null, paused: false } } });
  assert.equal(isReady(store.getState(), ROOT), true);
  store.getState().setClient({ connection: "open", sessions: {} });
  assert.equal(isReady(store.getState(), ROOT), false);
  store
    .getState()
    .setClient({ connection: "open", sessions: { [ROOT]: { writer: false, cursor: "3.0", paused: false } } });
  store.getState().reset(ROOT);
  assert.equal(isReady(store.getState(), ROOT), false);
});

test("a chat dropped while open shows its banner instead of the loader", () => {
  const store = createChatStore();
  store.getState().reset(ROOT);
  assert.equal(isLoading(store.getState(), ROOT), true);
  store
    .getState()
    .setClient({ connection: "open", sessions: { [ROOT]: { writer: true, cursor: "3.0", paused: false } } });
  store.getState().setReady(ROOT);
  assert.equal(isLoading(store.getState(), ROOT), false);
  store.getState().setClient({ connection: "open", sessions: {} });
  store.getState().setBanner("This chat no longer exists.");
  assert.equal(isReady(store.getState(), ROOT), false);
  assert.equal(isLoading(store.getState(), ROOT), false);
  store.getState().setBanner(null);
  assert.equal(isLoading(store.getState(), ROOT), true);
});

test("uncertain and closed prompts are moot once their session was dropped", () => {
  const store = createChatStore();
  const uncertain = new TantraAcpError("uncertain", "Prompt outcome unknown");
  const closed = new TantraAcpError("closed", "Session closed");
  const fenced = new TantraAcpError("writer_required", "Writer required");
  store
    .getState()
    .setClient({ connection: "open", sessions: { [ROOT]: { writer: true, cursor: null, paused: false } } });
  assert.equal(fromDroppedSession(store.getState(), ROOT, uncertain), false);
  assert.equal(fromDroppedSession(store.getState(), ROOT, closed), false);
  store.getState().setClient({ connection: "open", sessions: {} });
  assert.equal(fromDroppedSession(store.getState(), ROOT, uncertain), true);
  assert.equal(fromDroppedSession(store.getState(), ROOT, closed), true);
  assert.equal(fromDroppedSession(store.getState(), ROOT, fenced), false);
  assert.equal(fromDroppedSession(store.getState(), ROOT, new Error("x")), false);
});

test("near-bottom threshold follows at 80px and stops above it", () => {
  assert.equal(isNearBottom(1_000, 900, 20), true);
  assert.equal(isNearBottom(1_000, 899, 20), false);
});

test("the golden session reduces end to end through the store", () => {
  const store = createChatStore();
  const asks = new Map(golden.requests.map((asked) => [askIdOf(asked), asked]));
  const frames = golden.live.flatMap((frame, index) => [...golden.mirror.filter(({ at }) => at === index), frame]);
  frames.push(...golden.mirror.filter(({ at }) => at >= golden.live.length));
  const answered: string[] = [];

  for (const { sessionId, update } of frames) {
    store.getState().apply(sessionId, update);
    const marker = (update._meta?.tantra as { ask?: { askId: string; state: string } } | undefined)?.ask;
    if (update.sessionUpdate !== "state_update" || marker?.state !== "pending") continue;
    const asked = asks.get(marker.askId)!;
    const { request, results } = requested(asked);
    store.getState().addRequest(request);
    const view = store.getState().transcript.sessions[ROOT];
    assert.equal(composerDisabled(true, true, true, view.state?.state === "requires_action"), true);

    const ask = askOf(store.getState().transcript.requests[ROOT][marker.askId]);
    if (ask.kind === "approval") ask.respond(asked.result.outcome?.optionId === "allow");
    else if (ask.kind === "choice") ask.respond(asked.result.content!.choice!);
    else ask.respond(asked.result.content!.text!);
    store.getState().removeRequest(ROOT, ask.askId);
    assert.deepEqual(results, [asked.result]);
    answered.push(ask.kind);
  }

  const expected = frames.reduce<Transcript>((t, { sessionId, update }) => apply(t, sessionId, update), empty());
  const { transcript } = store.getState();
  assert.deepEqual(transcript, expected);
  assert.deepEqual(answered, ["approval", "free_text", "choice"]);
  assert.deepEqual(transcript.requests, {});

  const view = transcript.sessions[ROOT];
  const [user] = view.entries;
  assert.equal(user.kind, "user");
  if (user.kind === "user") {
    assert.equal(textOf(user.content), "Run the checks.");
    assert.deepEqual(attachmentsOf(user.content), []);
  }
  assert.deepEqual(
    view.entries.filter((entry) => entry.kind === "message").map((entry) => entry.kind === "message" && entry.reply),
    [false, true, false, true, false],
  );
  assert.equal(actorStatusLabel(transcript.roster[CHILD].state), "Replied");
  assert.equal(actorStatusLabel(view.state), "Cancelled");
  assert.equal(view.entries.at(-1)?.kind, "status");
  assert.deepEqual([view.writerReason, view.queue], ["claimed", []]);
});
