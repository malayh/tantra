import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { SessionClient } from "../../../packages/tantra-client/dist/index.js";

const require = createRequire(new URL("../ui/package.json", import.meta.url));
const WebSocket = require("next/dist/compiled/ws");
const api = process.env.SARATHI_E2E_API ?? "http://localhost:18000";
const backendA = process.env.SARATHI_E2E_A ?? "http://localhost:18001";
const backendB = process.env.SARATHI_E2E_B ?? "http://localhost:18002";
const gate = process.env.SARATHI_E2E_GATE ?? "http://localhost:18090";
const origin = process.env.SARATHI_E2E_ORIGIN ?? "http://localhost:13001";
const clients = [];
const outcomes = [];
const roots = [];
const canonical = (id) =>
  id.includes("-")
    ? id
    : `${id.slice(0, 8)}-${id.slice(8, 12)}-${id.slice(12, 16)}-${id.slice(16, 20)}-${id.slice(20)}`;
const hex = (id) => id.replaceAll("-", "");
const wait = async (test, label, timeout = 30000) => {
  const end = Date.now() + timeout;
  while (!(await test())) {
    if (Date.now() >= end) throw new Error(`Timed out: ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};
let token;
const request = async (base, path, options = {}) => {
  const response = await fetch(`${base}${path}`, {
    ...options,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...options.headers,
    },
  });
  const body = await response.json();
  assert.ok(response.ok, `${path}: ${response.status} ${JSON.stringify(body)}`);
  return body;
};
const post = (base, path, body) =>
  request(base, path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
const makeRoot = async () => {
  const made = await post(backendA, "/api/sessions", {});
  roots.push(made.id);
  return made.id;
};
const socket = async (base, root, mode = "write") => {
  const issue = () =>
    post(base, `/api/sessions/${root}/socket-ticket`, { mode });
  const first = await issue();
  const events = [];
  const receipts = [];
  const notices = [];
  const errors = [];
  const client = new SessionClient({
    serverScope: api,
    rootId: canonical(root),
    identity: first.user_id,
    url: async () =>
      `${base.replace(/^http/, "ws")}/api/ws/sessions/${root}?ticket=${encodeURIComponent((await issue()).ticket)}`,
    webSocketFactory: (url, protocol) =>
      new WebSocket(url, protocol, { origin }),
    onEvent: (frame) => events.push(frame),
    onReceipt: (receipt) => receipts.push(receipt),
    onNotice: (notice) => notices.push(notice),
    onError: (error) =>
      errors.push({ code: error.code, uncertain: error.uncertain }),
    timing: { receiptTimeoutMs: 1200, retryDelaysMs: [100, 200, 400] },
  });
  clients.push(client);
  const subscription = client.subscribe(canonical(root));
  await client.connect();
  await subscription.ready();
  return { client, subscription, events, receipts, notices, errors };
};
const terminal = (connection, command) =>
  connection.events.find(
    (frame) =>
      frame.body?.turn_id === hex(command) &&
      [
        "turn_completed",
        "turn_cancelled",
        "turn_failed",
        "turn_interrupted",
      ].includes(frame.body.type),
  );
const turn = async (connection, input) => {
  const sent = connection.client.send({ text: input, attachments: [] });
  await sent;
  await wait(
    () => terminal(connection, sent.commandId),
    `turn ${sent.commandId}`,
  );
  assert.equal(
    terminal(connection, sent.commandId).body.type,
    "turn_completed",
  );
  return sent.commandId;
};
const pendingAsk = (connection, command) => {
  const index = connection.events.findIndex(
    (frame) =>
      frame.body?.type === "turn_started" &&
      frame.body.turn_id === hex(command),
  );
  if (index < 0) return undefined;
  return connection.events
    .slice(index + 1)
    .find((frame) => frame.body?.type === "ask_raised")?.body;
};
const scenario = async (name, run) => {
  const start = Date.now();
  try {
    const evidence = await run();
    outcomes.push({
      name,
      passed: true,
      milliseconds: Date.now() - start,
      evidence,
    });
    console.log(`PASS ${name}`);
  } catch (error) {
    outcomes.push({
      name,
      passed: false,
      milliseconds: Date.now() - start,
      error: String(error),
    });
    throw error;
  }
};

try {
  const account = await post(api, "/api/auth/signup", {
    email: `017-p3-${Date.now()}@example.test`,
    password: "disposable-017-p3-password",
  });
  token = account.access_token;
  let root;
  let first;
  let second;
  await scenario("streaming-title-writer-replacement", async () => {
    root = await makeRoot();
    first = await socket(backendA, root);
    await first.client.acquireWriter();
    const command = await turn(first, "Streaming fixture");
    assert.ok(
      first.events.some((frame) => frame.body?.type === "reasoning_delta"),
    );
    assert.ok(first.events.some((frame) => frame.body?.type === "text_delta"));
    await wait(
      () => first.notices.some((notice) => notice.title),
      "title notice",
    );
    second = await socket(backendB, root);
    await second.client.acquireWriter();
    await wait(
      () => !first.subscription.state.writable,
      "first writer downgraded",
    );
    assert.equal(first.client.state.connection, "open");
    const next = await turn(second, "Second writer");
    await wait(() => terminal(first, next), "old writer continues observing");
    await first.client.acquireWriter();
    await wait(
      () => !second.subscription.state.writable,
      "second writer downgraded",
    );
    return {
      root,
      commands: [command, next],
      title: first.notices.find((notice) => notice.title).title,
    };
  });
  await scenario("readonly-and-full-replay", async () => {
    const viewer = await socket(backendB, root, "read");
    await assert.rejects(
      viewer.client.acquireWriter(),
      (error) => error.code === "forbidden",
    );
    assert.ok(first.subscription.state.writable);
    const original = first.events.filter(
      (frame) => frame.actor_id === canonical(root),
    );
    assert.deepEqual(
      viewer.events.map((frame) => [frame.seq, frame.body]),
      original.map((frame) => [frame.seq, frame.body]),
    );
    await viewer.client.disconnect();
    return { root, replayed: original.length };
  });
  await scenario("approval-and-denial", async () => {
    for (const allow of [true, false]) {
      const command = first.client.send({
        text: `[gate:ask=${allow ? "approved" : "denied"}]`,
        attachments: [],
      });
      await command;
      await wait(
        () => pendingAsk(first, command.commandId),
        "approval descriptor",
      );
      const ask = pendingAsk(first, command.commandId);
      assert.equal(ask.request.extra.permission, "memory_write");
      assert.equal(
        ask.request.extra.arguments.title,
        `Gate ${allow ? "approved" : "denied"}`,
      );
      await first.client.answer(canonical(ask.ask_id), {
        kind: "approval",
        allow,
      });
      await wait(() => terminal(first, command.commandId), "answered turn");
    }
    const memories = await request(api, "/api/memory");
    assert.ok(memories.some((item) => item.title === "Gate approved"));
    assert.ok(!memories.some((item) => item.title === "Gate denied"));
    return { root, memoryTitles: memories.map((item) => item.title) };
  });
  await scenario("typed-choice-and-free-text", async () => {
    for (const [name, response] of [
      ["choice", { kind: "choice", selected: "Green" }],
      ["free-text", { kind: "free_text", text: "Canary rollout" }],
    ]) {
      const command = first.client.send({
        text: `[gate:tool=${name}]`,
        attachments: [],
      });
      await command;
      await wait(() => pendingAsk(first, command.commandId), `${name} ask`);
      const ask = pendingAsk(first, command.commandId);
      assert.equal(ask.request.kind, response.kind);
      await first.client.answer(canonical(ask.ask_id), response);
      await wait(() => terminal(first, command.commandId), `${name} turn`);
      assert.ok(
        first.events.some(
          (frame) =>
            frame.body?.type === "ask_answered" &&
            frame.body.ask_id === ask.ask_id &&
            JSON.stringify(frame.body.response) === JSON.stringify(response),
        ),
      );
    }
    return { root };
  });
  await scenario("cancel-and-later-work", async () => {
    await post(gate, "/control/gates/io/close", {});
    const command = first.client.send({
      text: "[gate:tool=io]",
      attachments: [],
    });
    await command;
    await wait(
      () =>
        first.events.some(
          (frame) =>
            frame.body?.type === "tool_call_started" &&
            frame.seq >
              first.events.find(
                (frame) => frame.body?.turn_id === hex(command.commandId),
              )?.seq,
        ),
      "blocked tool start",
    );
    await first.client.cancel();
    await wait(
      () => terminal(first, command.commandId),
      "cancellation terminal",
    );
    assert.equal(
      terminal(first, command.commandId).body.type,
      "turn_cancelled",
    );
    await post(gate, "/control/gates/io/open", {});
    const later = await turn(first, "Work after Stop");
    return { root, cancelled: command.commandId, later };
  });
  await scenario("child-completion-and-lazy-history", async () => {
    await post(gate, "/control/gates/worker/close", {});
    const command = await turn(first, "[gate:child=worker]");
    const child = first.events.find(
      (frame) => frame.body?.type === "child_created",
    ).body.child_id;
    const subscription = first.client.subscribe(canonical(child));
    await subscription.ready();
    await post(gate, "/control/gates/worker/open", {});
    await wait(
      () =>
        first.events.some(
          (frame) =>
            frame.actor_id === canonical(child) &&
            frame.body?.type === "agent_finished",
        ),
      "child finish",
    );
    const childEvents = first.events.filter(
      (frame) => frame.actor_id === canonical(child),
    );
    assert.ok(childEvents.length > 2);
    await subscription.unsubscribe();
    const replay = await socket(backendB, root, "read");
    const restored = replay.client.subscribe(canonical(child));
    await restored.ready();
    assert.deepEqual(
      replay.events
        .filter((frame) => frame.actor_id === canonical(child))
        .map((frame) => [frame.seq, frame.body]),
      childEvents.map((frame) => [frame.seq, frame.body]),
    );
    await replay.client.disconnect();
    return { root, command, child, replayed: childEvents.length };
  });
  await scenario("document-and-skill", async () => {
    const fixture = await readFile(
      new URL("fixtures/sample.pdf", import.meta.url),
    );
    const form = new FormData();
    form.append(
      "file",
      new Blob([fixture], { type: "application/pdf" }),
      "sample.pdf",
    );
    const attachment = await request(backendA, "/api/uploads", {
      method: "POST",
      body: form,
    });
    await second.client.acquireWriter();
    const sent = second.client.send({
      text: "Read the attached document",
      attachments: [attachment],
    });
    await sent;
    await wait(
      () => terminal(second, sent.commandId),
      "cross-backend document read",
    );
    const completed = second.events.find(
      (frame) =>
        frame.body?.type === "tool_call_completed" &&
        String(frame.body.result).includes("Quellmarsh"),
    );
    assert.ok(completed, "read_doc recovered fixture facts");
    await turn(second, "[gate:tool=skill]");
    assert.ok(
      second.events.some(
        (frame) =>
          frame.body?.type === "tool_call_requested" &&
          frame.body.name === "skill",
      ),
    );
    await wait(
      async () =>
        (await request(api, `/api/sessions/${root}/actors`)).every(
          (actor) => !actor.active,
        ),
      "tree idle after child notification",
    );
    await request(backendA, `/api/sessions/${root}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: "e2e-other" }),
    });
    await wait(
      () => second.notices.some((notice) => notice.model === "e2e-other"),
      "cross-backend model notice",
    );
    const later = await turn(second, "New model fixture");
    return {
      root,
      commands: [sent.commandId, later],
      attachmentName: attachment.name,
      model: "e2e-other",
    };
  });
  await scenario("lost-committed-receipt-and-fifo", async () => {
    const target = await makeRoot();
    const owner = await socket(backendA, target);
    await owner.client.acquireWriter();
    await post(gate, "/control/gates/model/close", {});
    const held = owner.client.send({
      text: "[gate:provider=model]",
      attachments: [],
    });
    await held;
    await wait(
      () => owner.events.some((frame) => frame.body?.type === "sample_started"),
      "owner started",
    );
    const remote = await socket(backendB, target);
    await remote.client.acquireWriter();
    await post(gate, "/control/command-replies/drop-next-send", {});
    const queued = remote.client.send({
      text: "Queued after held turn",
      attachments: [],
    });
    const receipt = await queued;
    assert.equal(receipt.duplicate, true);
    await post(gate, "/control/gates/model/open", {});
    await wait(() => terminal(remote, queued.commandId), "queued completion");
    assert.equal(
      remote.events.filter(
        (frame) =>
          frame.body?.type === "input_queued" &&
          frame.body.command_id === hex(queued.commandId),
      ).length,
      1,
    );
    const state = await request(gate, "/control/state");
    assert.ok(
      state.command_reply.drops.some(
        (drop) => drop.command_id === queued.commandId,
      ),
    );
    return {
      root: target,
      commands: [held.commandId, queued.commandId],
      duplicate: receipt.duplicate,
    };
  });
} catch (error) {
  process.exitCode = 1;
  console.error(String(error));
} finally {
  await Promise.allSettled(clients.map((client) => client.disconnect()));
  await post(gate, "/control/reset", {}).catch(() => {});
  const report = {
    passed:
      process.exitCode !== 1 &&
      outcomes.length === 8 &&
      outcomes.every((outcome) => outcome.passed),
    outcomes,
    roots,
  };
  await mkdir(new URL("reports/017-p3/", import.meta.url), { recursive: true });
  await writeFile(
    new URL("reports/017-p3/client-behavior.json", import.meta.url),
    JSON.stringify(report, null, 2),
  );
}
