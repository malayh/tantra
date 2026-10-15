import assert from "node:assert/strict";
import { test } from "node:test";

import { FakeAgent, cursored, settled, until } from "./fake-socket.mjs";

const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const CHILD = "00000000-0000-4000-8000-0000000000c1";
const GRANDCHILD = "00000000-0000-4000-8000-0000000000c2";
const text = (value) => [{ type: "text", text: value }];

const tracked = (agent, options = {}) => {
  const states = [];
  const errors = [];
  const updates = [];
  const client = agent.client({
    onUpdate: (sessionId, update) => {
      updates.push({ sessionId, update });
    },
    onState: (state) => states.push(state),
    onError: (error) => errors.push(error),
    ...options,
  });
  const state = () => states.at(-1);
  return { client, states, errors, updates, state };
};

const opened = async (options = {}) => {
  const agent = new FakeAgent();
  const fixture = tracked(agent, options);
  await fixture.client.connect();
  const { sessionId } = await fixture.client.newSession();
  await until(() => fixture.state().sessions[sessionId]?.writer);
  return { agent, sessionId, ...fixture };
};

const reconnected = (fixture, count) =>
  until(
    () =>
      fixture.agent.sockets.length >= count &&
      fixture.state().connection === "open",
    "reconnect",
  );

test("the cursor advances only after onUpdate resolves", async () => {
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  const seen = [];
  const { agent, sessionId, state } = await opened({
    onUpdate: async (id, update) => {
      seen.push(update);
      if (update._meta) await gate;
    },
  });
  agent.update(agent.socket, sessionId, cursored("1.0"));
  await until(() => seen.some((update) => update._meta));
  await settled();
  assert.equal(state().sessions[sessionId].cursor, null);
  release();
  await until(() => state().sessions[sessionId].cursor === "1.0");
});

test("a throwing handler fails the client and keeps the applied cursor", async () => {
  const { agent, client, sessionId, state, errors } = await opened({
    onUpdate: (id, update) => {
      if (update._meta?.tantra?.cursor === "2.0")
        throw new Error("reducer broke");
    },
  });
  agent.update(agent.socket, sessionId, cursored("1.0"));
  agent.update(agent.socket, sessionId, cursored("2.0"));
  await until(() => state().connection === "failed");
  assert.equal(state().sessions[sessionId].cursor, "1.0");
  assert.equal(errors.at(-1).code, "handler_failed");
  assert.equal(errors.at(-1).cause.message, "reducer broke");
  assert.deepEqual(agent.sockets[0].closes, [1000]);
  await client.connect();
  const [resume] = agent.sockets[1].methods("session/resume");
  assert.deepEqual(resume.params, {
    sessionId,
    cwd: "/",
    replayFrom: { type: "_tantra_after", cursor: "1.0" },
  });
});

test("resume resolves only after the replayed updates are handled", async () => {
  const agent = new FakeAgent();
  const handled = [];
  const { client } = tracked(agent, {
    onUpdate: async (sessionId, update) => {
      await settled();
      handled.push(update._meta.tantra.cursor);
    },
  });
  agent.handlers["session/resume"] = function (socket, frame) {
    for (const cursor of ["1.0", "1.1", "2.0"])
      this.update(socket, frame.params.sessionId, cursored(cursor));
    this.reply(socket, frame, {});
  };
  await client.connect();
  await client.resume(CHILD);
  assert.deepEqual(handled, ["1.0", "1.1", "2.0"]);
});

test("replayed updates at or before the applied cursor are skipped", async () => {
  const { agent, client, sessionId, updates } = await opened();
  agent.update(agent.socket, sessionId, cursored("2.1"));
  await until(() => updates.some(({ update }) => update._meta));
  agent.handlers["session/resume"] = function (socket, frame) {
    for (const cursor of ["2.0", "2.1", "3.0"])
      this.update(socket, frame.params.sessionId, cursored(cursor));
    this.reply(socket, frame, {});
  };
  await client.resume(sessionId);
  const cursors = updates
    .filter(({ update }) => update._meta)
    .map(({ update }) => update._meta.tantra.cursor);
  assert.deepEqual(cursors, ["2.1", "3.0"]);
});

test("command ids are generated synchronously or taken from the caller", async () => {
  const { agent, client, sessionId } = await opened();
  const generated = client.prompt(sessionId, text("one"));
  assert.match(generated.commandId, UUID);
  const chosen = "3d9e5b7a-1f2c-4d6e-8a9b-7c5d3e1f2a4b";
  const explicit = client.prompt(sessionId, text("two"), { commandId: chosen });
  assert.equal(explicit.commandId, chosen);
  assert.deepEqual(await generated, { messageId: generated.commandId });
  assert.deepEqual(await explicit, { messageId: chosen });
  const ids = agent
    .sent("session/prompt")
    .map((frame) => frame.params._meta.tantra.commandId);
  assert.deepEqual(ids, [generated.commandId, chosen]);
});

test("a prompt lost with the connection is held until the writer is reclaimed", async () => {
  const fixture = await opened();
  const { agent, client, sessionId, state } = fixture;
  agent.handlers["session/prompt"] = function (socket) {
    delete this.handlers["session/prompt"];
    setImmediate(() => socket.drop(1011));
  };
  const lost = client.prompt(sessionId, text("lost"));
  await reconnected(fixture, 2);
  const queued = client.prompt(sessionId, text("next"));
  const last = client.prompt(sessionId, text("last"));
  const [resume] = agent.sockets[1].methods("session/resume");
  assert.equal(resume.params._meta, undefined);
  assert.equal(state().sessions[sessionId].writer, false);
  await settled();
  assert.deepEqual(agent.sockets[1].methods("session/prompt"), []);
  await client.claimWriter(sessionId);
  assert.deepEqual(await lost, { messageId: lost.commandId });
  await Promise.all([queued, last]);
  const retried = agent.sockets[1].methods("session/prompt");
  assert.deepEqual(
    retried.map((frame) => frame.params._meta.tantra.commandId),
    [lost.commandId, queued.commandId, last.commandId],
  );
  assert.deepEqual(
    retried[0].params.prompt,
    agent.sockets[0].methods("session/prompt")[0].params.prompt,
  );
});

test("a reader's prompt is rejected with writer_required and nothing is held", async () => {
  const agent = new FakeAgent();
  const { client } = tracked(agent);
  agent.handlers["session/prompt"] = function (socket, frame) {
    this.fail(socket, frame, -32603, "Writer required", {
      tantra: { code: "writer_required" },
    });
  };
  await client.connect();
  await client.resume(CHILD);
  const first = client.prompt(CHILD, text("one"));
  const second = client.prompt(CHILD, text("two"));
  await assert.rejects(first, {
    code: "writer_required",
    rpcCode: -32603,
    sessionId: CHILD,
    commandId: first.commandId,
  });
  await assert.rejects(second, {
    code: "writer_required",
    commandId: second.commandId,
  });
  assert.equal(agent.sent("session/prompt").length, 2);
});

test("unavailable rejects with its command id and no retry", async () => {
  const { agent, client, sessionId } = await opened();
  agent.handlers["session/prompt"] = function (socket, frame) {
    this.fail(socket, frame, -32603, "Service unavailable", {
      tantra: { code: "unavailable" },
    });
  };
  const prompt = client.prompt(sessionId, text("one"));
  await assert.rejects(prompt, {
    code: "unavailable",
    commandId: prompt.commandId,
  });
  assert.equal(agent.sent("session/prompt").length, 1);
});

for (const [name, stop] of [
  ["close", (client, sessionId) => client.close(sessionId)],
  ["delete", (client, sessionId) => client.delete(sessionId)],
  ["disconnect", (client) => client.disconnect()],
]) {
  test(`${name} rejects sent prompts as uncertain and queued ones as closed`, async () => {
    const { agent, client, sessionId, state } = await opened();
    agent.handlers["session/prompt"] = () => {};
    const sent = client.prompt(sessionId, text("sent"));
    const queued = client.prompt(sessionId, text("queued"));
    await until(() => agent.sent("session/prompt").length === 1);
    await stop(client, sessionId);
    await assert.rejects(sent, {
      code: "uncertain",
      sessionId,
      commandId: sent.commandId,
    });
    await assert.rejects(queued, {
      code: "closed",
      sessionId,
      commandId: queued.commandId,
    });
    assert.equal(agent.sent("session/prompt").length, 1);
    if (name !== "disconnect")
      assert.equal(state().sessions[sessionId], undefined);
  });
}

test("failing rejects the outbox as uncertain and closed", async () => {
  const { agent, client, sessionId } = await opened({
    onUpdate: (id, update) => {
      if (update.sessionUpdate === "_tantra_notice") throw new Error("broken");
    },
  });
  agent.handlers["session/prompt"] = () => {};
  const sent = client.prompt(sessionId, text("sent"));
  const queued = client.prompt(sessionId, text("queued"));
  await until(() => agent.sent("session/prompt").length === 1);
  agent.update(agent.socket, sessionId, {
    sessionUpdate: "_tantra_notice",
    level: "warning",
    code: "x",
    message: "x",
  });
  await assert.rejects(sent, { code: "uncertain", commandId: sent.commandId });
  await assert.rejects(queued, { code: "closed", commandId: queued.commandId });
});

test("url is called on every dial and backoff stays within bounds", async (t) => {
  const timer = t.mock.method(globalThis, "setTimeout");
  const agent = new FakeAgent();
  agent.refuse = Infinity;
  const { client, state, errors } = tracked(agent, {
    timing: { reconnectBaseMs: 2, reconnectCapMs: 8 },
  });
  await assert.rejects(client.connect(), { code: "dial_failed" });
  assert.equal(agent.urls.length, 10);
  assert.equal(agent.sockets.length, 10);
  assert.equal(state().connection, "failed");
  assert.equal(errors.at(-1).code, "dial_failed");
  const delays = timer.mock.calls.map((call) => call.arguments[1]);
  assert.equal(delays.length, 9);
  delays.forEach((delay, index) => {
    const ceiling = Math.min(8, 2 * 2 ** index);
    assert.ok(
      delay >= ceiling / 2 && delay <= ceiling,
      `${delay} outside ${ceiling}`,
    );
  });
});

test("reaching open resets the dial count", async () => {
  const agent = new FakeAgent();
  const { client, state } = tracked(agent);
  agent.refuse = 9;
  await client.connect();
  agent.refuse = 9;
  agent.socket.drop(1012);
  await until(
    () => agent.sockets.length === 20 && state().connection === "open",
    "recovery",
  );
});

test("a server that closes during every resume fails after 10 dials", async () => {
  const fixture = await opened();
  const { agent, client, sessionId, state, errors } = fixture;
  agent.handlers["session/prompt"] = () => {};
  const held = client.prompt(sessionId, text("held"));
  await until(() => agent.sent("session/prompt").length === 1);
  agent.handlers["session/resume"] = (socket) =>
    setImmediate(() => socket.drop(1011));
  agent.socket.drop(1011);
  try {
    await until(() => state().connection === "failed", "failed");
  } finally {
    await client.disconnect();
  }
  await assert.rejects(held, { code: "uncertain", commandId: held.commandId });
  assert.equal(agent.sockets.length, 1 + 10);
  assert.equal(errors.at(-1).code, "dial_failed");
});

test("a non-404 resume error on reconnect redials instead of dropping", async () => {
  const fixture = await opened();
  const { agent, sessionId, state, errors } = fixture;
  let resumes = 0;
  agent.handlers["session/resume"] = function (socket, frame) {
    if (++resumes === 1)
      this.fail(socket, frame, -32602, "Too many open sessions");
    else this.reply(socket, frame, {});
  };
  agent.socket.drop(1011);
  await reconnected(fixture, 3);
  assert.equal(resumes, 2);
  assert.ok(state().sessions[sessionId]);
  assert.deepEqual(errors, []);
  assert.deepEqual(agent.sockets[1].closes, [1000]);
});

test("reconnect waits before its first dial and backs off within bounds", async (t) => {
  const fixture = await opened();
  const { agent } = fixture;
  const timer = t.mock.method(globalThis, "setTimeout");
  agent.refuse = 3;
  agent.socket.drop(1011);
  await reconnected(fixture, 5);
  const delays = timer.mock.calls.map((call) => call.arguments[1]);
  assert.equal(delays.length, 4);
  delays.forEach((delay, index) => {
    const ceiling = Math.min(4, 1 * 2 ** index);
    assert.ok(
      delay >= ceiling / 2 && delay <= ceiling,
      `${delay} outside ${ceiling}`,
    );
  });
});

test("reconnect resumes open sessions in order, read-only, from their cursors", async () => {
  const fixture = await opened();
  const { agent, client, sessionId: root } = fixture;
  agent.update(agent.socket, root, cursored("3.1"));
  agent.update(agent.socket, root, {
    sessionUpdate: "subagent_update",
    sessionId: CHILD,
    title: "child",
  });
  await until(() => fixture.state().sessions[root].cursor === "3.1");
  await client.resume(CHILD);
  const { sessionId: other } = await client.newSession();
  agent.socket.drop(1011);
  await reconnected(fixture, 2);
  const resumes = agent.sockets[1]
    .methods("session/resume")
    .map((frame) => frame.params);
  assert.deepEqual(resumes, [
    {
      sessionId: root,
      cwd: "/",
      replayFrom: { type: "_tantra_after", cursor: "3.1" },
    },
    { sessionId: CHILD, cwd: "/", replayFrom: { type: "start" } },
    { sessionId: other, cwd: "/", replayFrom: { type: "start" } },
  ]);
  assert.deepEqual(
    Object.values(fixture.state().sessions).map((session) => session.writer),
    [false, false, false],
  );
});

test("a session lost on reconnect is dropped and reported", async () => {
  const fixture = await opened();
  const { agent, client, sessionId: lost } = fixture;
  const { sessionId: kept } = await client.newSession();
  agent.handlers["session/resume"] = function (socket, frame) {
    if (frame.params.sessionId === lost)
      this.fail(socket, frame, -32002, "Session not found");
    else this.reply(socket, frame, {});
  };
  agent.socket.drop(1011);
  await reconnected(fixture, 2);
  assert.deepEqual(Object.keys(fixture.state().sessions), [kept]);
  const [error] = fixture.errors;
  assert.equal(error.code, "not_found");
  assert.equal(error.sessionId, lost);
  assert.equal(error.rpcCode, -32002);
});

test("an agent without the profile fails without reconnecting", async () => {
  const agent = new FakeAgent();
  agent.handlers.initialize = function (socket, frame) {
    this.reply(socket, frame, {
      protocolVersion: 2,
      capabilities: {},
      info: { name: "v2", version: "0" },
    });
  };
  const { client, state, errors } = tracked(agent);
  await assert.rejects(client.connect(), { code: "profile_unsupported" });
  await settled();
  assert.equal(agent.sockets.length, 1);
  assert.equal(state().connection, "failed");
  assert.deepEqual(
    errors.map((error) => error.code),
    ["profile_unsupported"],
  );
});

test("writer state follows replaced, lost, and claims", async () => {
  const { agent, client, sessionId, state } = await opened();
  agent.writer(agent.socket, sessionId, "replaced");
  await until(() => state().sessions[sessionId].writer === false);
  await client.claimWriter(sessionId);
  assert.equal(state().sessions[sessionId].writer, true);
  agent.writer(agent.socket, sessionId, "lost");
  await until(() => state().sessions[sessionId].writer === false);
  await client.resume(sessionId, { mode: "write" });
  assert.equal(state().sessions[sessionId].writer, true);
  await client.resume(sessionId);
  assert.equal(state().sessions[sessionId].writer, false);
});

test("closing or re-resuming a root drops its known descendants", async () => {
  const { agent, client, sessionId: root, state } = await opened();
  agent.update(agent.socket, root, {
    sessionUpdate: "subagent_update",
    sessionId: CHILD,
    title: "child",
  });
  agent.update(agent.socket, CHILD, {
    sessionUpdate: "subagent_update",
    sessionId: GRANDCHILD,
    title: "grand",
  });
  await client.resume(CHILD);
  await client.resume(GRANDCHILD);
  assert.deepEqual(Object.keys(state().sessions), [root, CHILD, GRANDCHILD]);
  await client.resume(GRANDCHILD);
  assert.deepEqual(Object.keys(state().sessions), [root, CHILD, GRANDCHILD]);
  await client.resume(root);
  assert.deepEqual(Object.keys(state().sessions), [root]);
  await client.resume(CHILD);
  await client.close(CHILD);
  assert.deepEqual(Object.keys(state().sessions), [root]);
  await client.resume(CHILD);
  await client.close(root);
  assert.deepEqual(state().sessions, {});
});

const answers = (agent, id) =>
  agent.socket.sent.filter((frame) => frame.id === id && !frame.method);
const answer = (agent, id) => answers(agent, id)[0];

const permission = (sessionId, id) => ({
  jsonrpc: "2.0",
  id,
  method: "session/request_permission",
  params: {
    sessionId,
    title: "deploy",
    options: [
      { optionId: "allow", name: "Allow", kind: "allow_once" },
      { optionId: "deny", name: "Deny", kind: "reject_once" },
    ],
  },
});

test("permission requests are answered through respond", async () => {
  const requests = [];
  const { agent, sessionId } = await opened({
    onRequest: (request) => {
      requests.push(request);
      request.respond({ outcome: { outcome: "selected", optionId: "allow" } });
    },
  });
  agent.later(agent.socket, permission(sessionId, 7));
  const reply = await until(() => answer(agent, 7));
  assert.deepEqual(reply, {
    jsonrpc: "2.0",
    id: 7,
    result: { outcome: { outcome: "selected", optionId: "allow" } },
  });
  assert.equal(requests[0].method, "session/request_permission");
  assert.equal(requests[0].sessionId, sessionId);
  requests[0].respond({ outcome: { outcome: "cancelled" } });
  await settled();
  assert.equal(answers(agent, 7).length, 1);
});

test("cancel_request aborts the request and replies -32800", async () => {
  const requests = [];
  const { agent, sessionId } = await opened({
    onRequest: (request) => requests.push(request),
  });
  agent.later(agent.socket, permission(sessionId, 1));
  await until(() => requests.length);
  agent.later(agent.socket, {
    jsonrpc: "2.0",
    method: "$/cancel_request",
    params: { requestId: 1 },
  });
  await until(() => requests[0].signal.aborted);
  const reply = await until(() => answer(agent, 1));
  assert.equal(reply.error.code, -32800);
  requests[0].respond({ outcome: { outcome: "selected", optionId: "allow" } });
  assert.equal(answers(agent, 1).length, 1);
});

test("disconnect aborts pending requests and a late respond does nothing", async () => {
  const requests = [];
  const { agent, client, sessionId } = await opened({
    onRequest: (request) => requests.push(request),
  });
  agent.later(agent.socket, permission(sessionId, 2));
  await until(() => requests.length);
  await client.disconnect();
  assert.equal(requests[0].signal.aborted, true);
  requests[0].respond({ outcome: { outcome: "selected", optionId: "allow" } });
  assert.equal(answers(agent, 2).length, 0);
});

test("unknown request methods are answered with -32601", async () => {
  const { agent } = await opened();
  agent.later(agent.socket, {
    jsonrpc: "2.0",
    id: 9,
    method: "fs/read_text_file",
    params: {},
  });
  const reply = await until(() => answer(agent, 9));
  assert.equal(reply.error.code, -32601);
});

test("frames over 1 MiB are rejected locally", async () => {
  const { agent, client, sessionId } = await opened();
  const huge = client.prompt(sessionId, text("x".repeat(1024 * 1024)));
  await assert.rejects(huge, { code: "too_large", commandId: huge.commandId });
  const wide = client.prompt(sessionId, text("\u00e9".repeat(600 * 1024)));
  await assert.rejects(wide, { code: "too_large", commandId: wide.commandId });
  await assert.rejects(client.resume("x".repeat(1024 * 1024)), {
    code: "too_large",
  });
  await settled();
  assert.deepEqual(agent.sent("session/prompt"), []);
  assert.equal(agent.sent("session/resume").length, 0);
});

test("calls need an open connection", async () => {
  const agent = new FakeAgent();
  const { client } = tracked(agent);
  await assert.rejects(client.newSession(), { code: "closed" });
  await assert.rejects(client.cancel(CHILD), { code: "closed" });
  await assert.rejects(client.prompt(CHILD, text("x")), { code: "closed" });
  await client.connect();
  await assert.rejects(client.cancel(CHILD), { code: "closed" });
});

test("session methods send their wire frames", async () => {
  const agent = new FakeAgent();
  agent.handlers["session/set_config_option"] = function (socket, frame) {
    this.reply(socket, frame, {
      configOptions: [
        {
          configId: "model",
          name: "Model",
          type: "select",
          currentValue: frame.params.value,
          options: [{ value: "n", name: "n" }],
        },
      ],
    });
  };
  const { client } = tracked(agent, { cwd: "/work" });
  await client.connect();
  const { sessionId } = await client.newSession();
  assert.deepEqual(agent.sent("session/new")[0].params, { cwd: "/work" });
  await client.listSessions();
  await client.listSessions("abc");
  assert.deepEqual(
    agent.sent("session/list").map((frame) => frame.params),
    [{}, { cursor: "abc" }],
  );
  const options = await client.setModel(sessionId, "n");
  assert.equal(options[0].currentValue, "n");
  assert.deepEqual(agent.sent("session/set_config_option")[0].params, {
    sessionId,
    configId: "model",
    type: "id",
    value: "n",
  });
  await client.resume(sessionId, { mode: "write" });
  assert.deepEqual(agent.sent("session/resume")[0].params, {
    sessionId,
    cwd: "/work",
    replayFrom: { type: "start" },
    _meta: { tantra: { mode: "write" } },
  });
  await client.cancel(sessionId);
  assert.match(
    agent.sent("session/cancel")[0].params._meta.tantra.commandId,
    UUID,
  );
});

test("a claimed update or a write resume releases held prompts", async () => {
  const fixture = await opened();
  const { agent, client, sessionId } = fixture;
  agent.socket.drop(1011);
  await reconnected(fixture, 2);
  const held = client.prompt(sessionId, text("held"));
  await settled();
  assert.deepEqual(agent.sockets[1].methods("session/prompt"), []);
  agent.writer(agent.socket, sessionId, "claimed");
  assert.deepEqual(await held, { messageId: held.commandId });
  agent.socket.drop(1011);
  await reconnected(fixture, 3);
  const again = client.prompt(sessionId, text("again"));
  await settled();
  assert.deepEqual(agent.sockets[2].methods("session/prompt"), []);
  await client.resume(sessionId, { mode: "write" });
  assert.deepEqual(await again, { messageId: again.commandId });
});

test("elicitation requests are answered through respond", async () => {
  const { agent, sessionId } = await opened({
    onRequest: (request) => {
      assert.equal(request.method, "elicitation/create");
      request.respond({ action: "accept", content: { text: "yes" } });
    },
  });
  agent.later(agent.socket, {
    jsonrpc: "2.0",
    id: 5,
    method: "elicitation/create",
    params: {
      sessionId,
      mode: "form",
      message: "Why?",
      requestedSchema: { type: "object" },
    },
  });
  const reply = await until(() => answer(agent, 5));
  assert.deepEqual(reply.result, {
    action: "accept",
    content: { text: "yes" },
  });
});

test("a failed first resume rejects prompts queued on it", async () => {
  const agent = new FakeAgent();
  const { client, state } = tracked(agent);
  await client.connect();
  agent.handlers["session/prompt"] = () => {};
  agent.handlers["session/resume"] = function (socket, frame) {
    setImmediate(() => this.fail(socket, frame, -32002, "Session not found"));
  };
  const resumed = client.resume(CHILD);
  const sent = client.prompt(CHILD, text("a"));
  const queued = client.prompt(CHILD, text("b"));
  await assert.rejects(resumed, { code: "not_found", rpcCode: -32002 });
  await assert.rejects(sent, { code: "uncertain", commandId: sent.commandId });
  await assert.rejects(queued, { code: "closed", commandId: queued.commandId });
  assert.deepEqual(state().sessions, {});
});

test("a failed re-resume closes the session on both sides", async () => {
  const { agent, client, sessionId: root, state } = await opened();
  agent.update(agent.socket, root, {
    sessionUpdate: "subagent_update",
    sessionId: CHILD,
    title: "child",
  });
  await client.resume(CHILD);
  agent.handlers["session/prompt"] = () => {};
  const sent = client.prompt(root, text("a"));
  await until(() => agent.sent("session/prompt").length === 1);
  agent.handlers["session/resume"] = function (socket, frame) {
    this.fail(socket, frame, -32002, "Session not found");
  };
  await assert.rejects(client.resume(root, { mode: "write" }), {
    code: "not_found",
  });
  await assert.rejects(sent, { code: "uncertain", commandId: sent.commandId });
  assert.deepEqual(state().sessions, {});
  await until(() => agent.sent("session/close").length === 1);
  assert.deepEqual(agent.sent("session/close")[0].params, { sessionId: root });
});

test("onRequest is not awaited and a failing handler answers -32603", async () => {
  const requests = [];
  const { agent, sessionId, state, errors, updates } = await opened({
    onRequest: (request) => {
      requests.push(request);
      if (request.params.title === "broken") throw new Error("dialog broke");
      return new Promise(() => {});
    },
  });
  agent.later(agent.socket, permission(sessionId, 11));
  agent.update(agent.socket, sessionId, cursored("4.0"));
  await until(() => state().sessions[sessionId].cursor === "4.0");
  assert.ok(updates.some(({ update }) => update._meta));
  agent.later(agent.socket, {
    jsonrpc: "2.0",
    method: "$/cancel_request",
    params: { requestId: 11 },
  });
  await until(() => requests[0].signal.aborted);
  assert.equal(answer(agent, 11).error.code, -32800);
  agent.later(agent.socket, {
    ...permission(sessionId, 12),
    params: { ...permission(sessionId, 12).params, title: "broken" },
  });
  const failed = await until(() => answer(agent, 12));
  assert.equal(failed.error.code, -32603);
  assert.equal(errors.at(-1).code, "handler_failed");
  assert.equal(errors.at(-1).sessionId, sessionId);
  assert.equal(state().connection, "open");
});

test("a wedged handler does not block the next connection", async () => {
  const { agent, client, sessionId, state } = await opened({
    onUpdate: (id, update) =>
      update._meta?.tantra?.cursor === "9.0"
        ? new Promise(() => {})
        : undefined,
  });
  agent.update(agent.socket, sessionId, cursored("9.0"));
  await settled();
  await client.disconnect();
  await client.connect();
  assert.equal(state().connection, "open");
  assert.equal(agent.sockets[1].methods("session/resume").length, 1);
});

test("a retried prompt that gets writer_required is held again", async () => {
  const fixture = await opened();
  const { agent, client, sessionId, state } = fixture;
  let prompts = 0;
  agent.handlers["session/prompt"] = function (socket, frame) {
    prompts++;
    if (prompts === 1) setImmediate(() => socket.drop(1011));
    else if (prompts === 2)
      this.fail(socket, frame, -32603, "Writer required", {
        tantra: { code: "writer_required" },
      });
    else
      this.reply(socket, frame, {
        messageId: frame.params._meta.tantra.commandId,
      });
  };
  const lost = client.prompt(sessionId, text("lost"));
  await reconnected(fixture, 2);
  assert.equal(state().sessions[sessionId].paused, true);
  await client.claimWriter(sessionId);
  await until(() => prompts === 2 && state().sessions[sessionId].paused);
  assert.equal(state().sessions[sessionId].writer, false);
  await settled();
  assert.equal(prompts, 2);
  await client.claimWriter(sessionId);
  assert.deepEqual(await lost, { messageId: lost.commandId });
  assert.equal(state().sessions[sessionId].paused, false);
});

test("a reader at drop time is not held after reconnect", async () => {
  const agent = new FakeAgent();
  const fixture = { agent, ...tracked(agent) };
  const { client, state } = fixture;
  await client.connect();
  await client.resume(CHILD);
  agent.socket.drop(1011);
  await reconnected(fixture, 2);
  assert.equal(state().sessions[CHILD].paused, false);
  agent.handlers["session/prompt"] = function (socket, frame) {
    this.fail(socket, frame, -32603, "Writer required", {
      tantra: { code: "writer_required" },
    });
  };
  const prompt = client.prompt(CHILD, text("read"));
  await assert.rejects(prompt, {
    code: "writer_required",
    commandId: prompt.commandId,
  });
  assert.equal(agent.sockets[1].methods("session/prompt").length, 1);
});

test("without onRequest a permission request answers -32601", async () => {
  const { agent, sessionId } = await opened();
  agent.later(agent.socket, permission(sessionId, 13));
  const reply = await until(() => answer(agent, 13));
  assert.equal(reply.error.code, -32601);
});

test("dropping a root prunes its learned descendants", async () => {
  const { agent, client, sessionId: root } = await opened();
  agent.update(agent.socket, root, {
    sessionUpdate: "subagent_update",
    sessionId: CHILD,
    title: "child",
  });
  agent.update(agent.socket, CHILD, {
    sessionUpdate: "subagent_update",
    sessionId: GRANDCHILD,
    title: "grand",
  });
  await client.resume(CHILD);
  assert.equal(client.parents.size, 2);
  await client.close(root);
  assert.equal(client.parents.size, 0);
});
