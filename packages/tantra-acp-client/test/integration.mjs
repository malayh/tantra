import assert from "node:assert/strict";

import { TantraAcpClient, cursorOf } from "../dist/index.js";

const [baseUrl] = process.argv.slice(2);
assert.ok(baseUrl);

const ENTITIES = new Set([
  "user_message",
  "agent_message",
  "agent_thought",
  "session_message",
  "tool_call_update",
  "subagent_update",
]);
const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const waitFor = async (predicate, label, timeout = 15_000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await predicate();
    if (result) return result;
    await sleep(10);
  }
  throw new Error(`timed out waiting for ${label}`);
};
const request = async (path, body) => {
  const response = await fetch(`${baseUrl}${path}`, {
    method: body === undefined ? "GET" : "POST",
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  assert.equal(response.status, 200);
  return response.json();
};
const journal = (actor) => request(`/control/journal?actor=${actor}`);
const hex = (uuid) => uuid.replaceAll("-", "");
const text = (value) => [{ type: "text", text: value }];
const completed = async (actor, commandId) =>
  (await journal(actor)).some(
    (event) =>
      event.type === "turn_completed" && event.turn_id === hex(commandId),
  );

const reduce = (updates, sessionId) => {
  const entities = {};
  let state = null;
  for (const { sessionId: id, update } of updates) {
    if (id !== sessionId) continue;
    const { _meta, sessionUpdate, ...body } = update;
    void _meta;
    if (sessionUpdate === "state_update") state = body;
    const kind = sessionUpdate.replace(/_chunk$/, "");
    if (!ENTITIES.has(kind)) continue;
    const key = `${kind}:${body.messageId ?? body.toolCallId ?? body.sessionId}`;
    const entity = (entities[key] ??= {});
    if (sessionUpdate.endsWith("_chunk"))
      entity.text = (entity.text ?? "") + body.content.text;
    else if (Array.isArray(body.content))
      Object.assign(entity, body, {
        text: body.content.map((block) => block.text).join(""),
      });
    else Object.assign(entity, body);
  }
  return { entities, state };
};

const participant = (worker, token) => {
  const updates = [];
  const errors = [];
  const states = [];
  const requests = [];
  const client = new TantraAcpClient({
    url: () =>
      `${baseUrl.replace("http://", "ws://")}/${worker}?token=${token}`,
    cwd: "/work",
    onUpdate: (sessionId, update) => {
      updates.push({ sessionId, update });
    },
    onRequest: (incoming) => {
      requests.push(incoming);
      if (incoming.method === "session/request_permission")
        incoming.respond({
          outcome: { outcome: "selected", optionId: "allow" },
        });
    },
    onState: (state) => states.push(state),
    onError: (error) => errors.push(error),
    timing: { reconnectBaseMs: 50, reconnectCapMs: 200 },
  });
  const of = (sessionId, kind) =>
    updates
      .filter(
        (entry) =>
          entry.sessionId === sessionId &&
          (kind === undefined || entry.update.sessionUpdate === kind),
      )
      .map((entry) => entry.update);
  const state = () => states.at(-1);
  return { client, updates, errors, states, requests, of, state };
};

const settledIdle = (party, sessionId) => {
  const last = party.of(sessionId, "state_update").at(-1);
  return last?.state === "idle" && last.stopReason === "end_turn";
};

const a = participant("one", "a");
const b = participant("two", "b");

try {
  await a.client.connect();
  const { sessionId } = await a.client.newSession();
  assert.equal(a.state().sessions[sessionId].writer, true);
  const first = a.client.prompt(sessionId, text("hello"));
  assert.deepEqual(await first, { messageId: first.commandId });
  await waitFor(() => settledIdle(a, sessionId), "first turn");

  await b.client.connect();
  assert.deepEqual(await b.client.resume(sessionId), {});
  await waitFor(
    () =>
      b.of(sessionId, "state_update").some((update) => !cursorOf(update)) &&
      settledIdle(b, sessionId),
    "resume snapshot",
  );
  const cursored = (party) =>
    party.of(sessionId).filter((update) => cursorOf(update));
  assert.deepEqual(cursored(b), cursored(a));
  assert.deepEqual(reduce(b.updates, sessionId), reduce(a.updates, sessionId));
  assert.ok(
    reduce(a.updates, sessionId).entities[`user_message:${first.commandId}`],
  );
  assert.equal(b.state().sessions[sessionId].writer, false);

  await request("/control/drop", {});
  const mark = a.states.length;
  const lost = a.client.prompt(sessionId, text("again"));
  await waitFor(
    () =>
      a.states.slice(mark).some((state) => state.connection === "connecting"),
    "dropped response",
  );
  await waitFor(() => a.state().connection === "open", "reconnect");
  assert.equal(a.state().sessions[sessionId].writer, false);
  assert.equal(a.state().sessions[sessionId].paused, true);
  const attempts = async () =>
    (await request("/control/frames"))
      .flat()
      .filter(
        ({ from, frame }) =>
          from === "client" &&
          frame.method === "session/prompt" &&
          frame.params._meta.tantra.commandId === lost.commandId,
      );
  await sleep(100);
  assert.equal((await attempts()).length, 1);
  assert.deepEqual(await a.client.claimWriter(sessionId), {});
  assert.deepEqual(await lost, { messageId: lost.commandId });
  assert.equal(a.state().sessions[sessionId].paused, false);
  const queued = (await journal(sessionId)).filter(
    (event) =>
      event.type === "input_queued" && event.command_id === hex(lost.commandId),
  );
  assert.equal(queued.length, 1);
  assert.equal(queued[0].submitted_by, "user-a");
  const [original, retry] = await attempts();
  assert.deepEqual(retry.frame.params, original.frame.params);
  await waitFor(() => completed(sessionId, lost.commandId), "retried turn");

  await b.client.claimWriter(sessionId);
  assert.equal(b.state().sessions[sessionId].writer, true);
  await waitFor(
    () => a.state().sessions[sessionId].writer === false,
    "A replaced",
  );
  assert.deepEqual(a.of(sessionId, "_tantra_writer").at(-1), {
    sessionUpdate: "_tantra_writer",
    writer: false,
    reason: "replaced",
  });
  const fenced = a.client.prompt(sessionId, text("fenced"));
  await assert.rejects(fenced, {
    code: "writer_required",
    commandId: fenced.commandId,
  });
  await a.client.claimWriter(sessionId);
  await waitFor(
    () => b.state().sessions[sessionId].writer === false,
    "B replaced",
  );
  assert.equal(b.of(sessionId, "_tantra_writer").at(-1).reason, "replaced");

  const approval = a.client.prompt(sessionId, text("deploy"));
  await approval;
  await waitFor(
    () => completed(sessionId, approval.commandId),
    "approved turn",
  );
  assert.deepEqual(
    a.requests.map((incoming) => incoming.method),
    ["session/request_permission"],
  );
  assert.equal(a.requests[0].sessionId, sessionId);
  assert.deepEqual(b.requests, []);
  assert.deepEqual((await request("/control/state")).invoked, ["prod"]);

  const spawn = a.client.prompt(sessionId, text("spawn"));
  await spawn;
  const announced = await waitFor(
    () =>
      a
        .of(sessionId, "subagent_update")
        .find(
          (update) => update.state?.state === "running" && !cursorOf(update),
        ),
    "subagent mirror",
  );
  const child = announced.sessionId;
  assert.equal(announced.title, "worker");
  assert.deepEqual(a.of(child), []);
  assert.deepEqual(await a.client.resume(child), {});
  await waitFor(
    () => a.of(child, "session_message").length > 0,
    "child transcript",
  );
  const inbound = a.of(child, "session_message");
  assert.ok(inbound.every((update) => cursorOf(update)));
  assert.ok(a.state().sessions[child].cursor);
  await waitFor(
    () => a.of(child, "state_update").some((update) => !cursorOf(update)),
    "child snapshot",
  );
  assert.deepEqual(await a.client.close(child), {});
  const seen = a.of(child).length;
  assert.equal(a.state().sessions[child], undefined);
  await request("/control/release", {});
  await waitFor(
    async () =>
      (await journal(child)).some((event) => event.type === "turn_completed"),
    "child finished after close",
  );
  await waitFor(
    () =>
      a
        .of(sessionId, "subagent_update")
        .some(
          (update) =>
            update.sessionId === child &&
            update.state?.state === "idle" &&
            !cursorOf(update),
        ),
    "child idle in the mirror",
  );
  await waitFor(async () => {
    const events = await journal(sessionId);
    const started = events.filter((event) => event.type === "turn_started");
    const ended = events.filter((event) => event.type === "turn_completed");
    return (
      started.some((event) => event.input.startsWith("[agent ")) &&
      started.length === ended.length
    );
  }, "root lifecycle turn");
  assert.equal(a.of(child).length, seen);

  const listed = await b.client.listSessions();
  assert.ok(
    listed.sessions.some(
      (session) => session.sessionId === sessionId && session.cwd === "/work",
    ),
  );
  await assert.rejects(b.client.setModel(sessionId, "other"), {
    code: "invalid_params",
  });

  const cancelId = crypto.randomUUID();
  await a.client.cancel(sessionId, { commandId: cancelId });
  await waitFor(
    async () =>
      (await journal(sessionId)).some(
        (event) =>
          event.type === "cancellation_requested" &&
          event.command_id === hex(cancelId),
      ),
    "cancel recorded",
  );
  assert.deepEqual(await a.client.delete(sessionId), {});
  assert.equal(a.state().sessions[sessionId], undefined);
  const remaining = await b.client.listSessions();
  assert.ok(
    remaining.sessions.every((session) => session.sessionId !== sessionId),
  );
  assert.deepEqual(a.errors, []);
  assert.deepEqual(b.errors, []);
} finally {
  await Promise.allSettled([a.client.disconnect(), b.client.disconnect()]);
}
