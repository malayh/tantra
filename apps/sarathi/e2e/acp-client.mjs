import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import {
  TantraAcpClient,
  addRequest,
  apply,
  askOf,
  descendants,
  empty,
  removeRequest,
} from "../../../packages/tantra-acp-client/dist/index.js";

const require = createRequire(new URL("../ui/package.json", import.meta.url));
const WebSocket = require("next/dist/compiled/ws");
const run = promisify(execFile);
const api = process.env.SARATHI_E2E_API ?? "http://localhost:18000";
const backendA = process.env.SARATHI_E2E_A ?? "http://localhost:18001";
const backendB = process.env.SARATHI_E2E_B ?? "http://localhost:18002";
const gate = process.env.SARATHI_E2E_GATE ?? "http://localhost:18090";
const origin = process.env.SARATHI_E2E_ORIGIN ?? "http://localhost:13001";
const otherModel = process.env.SARATHI_E2E_MODEL ?? "e2e-other";
const leaseMs = (Number(process.env.E2E_COORDINATOR_LEASE_TTL ?? 6) + 1) * 1000;
const compose = (
  process.env.SARATHI_E2E_COMPOSE ??
  "docker compose -p tantra011e2e -f docker-compose.yaml -f docker-compose.e2e.yaml"
)
  .split(/\s+/)
  .filter(Boolean);
const SCENARIOS = 13;
const clients = [];
const outcomes = [];
const roots = [];
let token;
let ownerStopped = false;

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const wait = async (test, label, timeout = 30000) => {
  const end = Date.now() + timeout;
  while (!(await test())) {
    if (Date.now() >= end) throw new Error(`Timed out: ${label}`);
    await sleep(20);
  }
};
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
const dockerCompose = (...args) =>
  run(compose[0], [...compose.slice(1), ...args], {
    cwd: fileURLToPath(new URL("..", import.meta.url)),
  });
const healthy = async (base) => {
  try {
    return (await fetch(`${base}/api/health`)).ok;
  } catch {
    return false;
  }
};
const retrying = async (operation, code, label) => {
  const end = Date.now() + 30000;
  for (;;) {
    try {
      return await operation();
    } catch (error) {
      if (Date.now() >= end || error.code !== code) throw error;
      console.log(`retrying ${label} after ${code}`);
      await sleep(500);
    }
  }
};
const gateState = () => request(gate, "/control/state");
const waiting = async (name) => {
  await wait(
    async () => ((await gateState()).gates[name]?.waiters ?? 0) > 0,
    `gate ${name} waiter`,
  );
};

const connect = async (base, mode = "write") => {
  const peer = {
    route: { base },
    updates: [],
    requests: [],
    errors: [],
    state: { connection: "closed", sessions: {} },
    transcript: empty(),
  };
  peer.client = new TantraAcpClient({
    url: async () => {
      const { ticket } = await post(peer.route.base, "/api/acp/ticket", {
        mode,
      });
      return `${peer.route.base.replace(/^http/, "ws")}/api/acp?ticket=${encodeURIComponent(ticket)}`;
    },
    webSocketFactory: (url) => {
      const socket = new WebSocket(url, { origin });
      socket.on("error", () => {});
      return socket;
    },
    timing: { reconnectBaseMs: 200, reconnectCapMs: 2000 },
    onUpdate: (sessionId, update) => {
      peer.updates.push({ sessionId, update });
      peer.transcript = apply(peer.transcript, sessionId, update);
    },
    onRequest: (incoming) => {
      peer.requests.push(incoming);
      peer.transcript = addRequest(peer.transcript, incoming);
      incoming.signal.addEventListener("abort", () => {
        peer.transcript = removeRequest(
          peer.transcript,
          incoming.sessionId,
          askOf(incoming).askId,
        );
      });
    },
    onState: (state) => {
      peer.state = state;
    },
    onError: (error) =>
      peer.errors.push({ code: error.code, message: error.message }),
  });
  clients.push(peer.client);
  await peer.client.connect();
  return peer;
};
const view = (peer, sessionId) => peer.transcript.sessions[sessionId];
const writer = (peer, sessionId) =>
  peer.state.sessions[sessionId]?.writer === true;
const text = (value) => [{ type: "text", text: value }];
const idle = (update) =>
  update.sessionUpdate === "state_update" &&
  update.state === "idle" &&
  update._meta?.tantra?.cursor !== undefined;
const position = (peer, sessionId, predicate, from = 0) => {
  const index = peer.updates
    .slice(from)
    .findIndex(
      (frame) => frame.sessionId === sessionId && predicate(frame.update),
    );
  return index < 0 ? -1 : index + from;
};
const startedAt = (peer, sessionId, commandId) =>
  position(
    peer,
    sessionId,
    (update) =>
      update.sessionUpdate === "user_message" && update.messageId === commandId,
  );
const endedAt = (peer, sessionId, commandId) => {
  const start = startedAt(peer, sessionId, commandId);
  return start < 0 ? -1 : position(peer, sessionId, idle, start + 1);
};
const terminal = (peer, sessionId, commandId) =>
  peer.updates[endedAt(peer, sessionId, commandId)]?.update;
const settle = async (peer, sessionId, commandId, stopReason = "end_turn") => {
  await wait(
    () => terminal(peer, sessionId, commandId),
    `terminal ${commandId}`,
  );
  assert.equal(terminal(peer, sessionId, commandId).stopReason, stopReason);
};
const send = async (peer, sessionId, blocks) => {
  const sent = peer.client.prompt(sessionId, blocks);
  const { messageId } = await sent;
  assert.equal(messageId, sent.commandId);
  return sent.commandId;
};
const turn = async (peer, sessionId, input) => {
  const commandId = await send(peer, sessionId, text(input));
  await settle(peer, sessionId, commandId);
  return commandId;
};
const entriesAfter = (peer, sessionId, commandId) => {
  const entries = view(peer, sessionId)?.entries ?? [];
  const index = entries.findIndex(
    (entry) => entry.kind === "user" && entry.id === commandId,
  );
  return index < 0 ? [] : entries.slice(index + 1);
};
const toolAfter = (peer, sessionId, commandId, name) =>
  entriesAfter(peer, sessionId, commandId).find(
    (entry) => entry.kind === "tool" && entry.name === name,
  );
const nextRequest = async (peer, sessionId, from, label) => {
  await wait(
    () =>
      peer.requests
        .slice(from)
        .some((item) => item.sessionId === sessionId && !item.signal.aborted),
    label,
  );
  return peer.requests
    .slice(from)
    .find((item) => item.sessionId === sessionId && !item.signal.aborted);
};
const snapshot = (peer, sessionId) => ({
  entries: view(peer, sessionId)?.entries,
  state: view(peer, sessionId)?.state,
});
const listAll = async (peer) => {
  const found = [];
  let cursor;
  do {
    const page = await peer.client.listSessions(cursor);
    found.push(...page.sessions);
    cursor = page.nextCursor;
  } while (cursor !== undefined);
  return found;
};
const scenario = async (name, body) => {
  const start = Date.now();
  try {
    const evidence = await body();
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
    email: `018-p5-${Date.now()}@example.test`,
    password: "disposable-018-p5-password",
  });
  token = account.access_token;
  const first = await connect(backendA);
  const second = await connect(backendB);
  const observer = await connect(backendB, "read");
  let root;

  await scenario("observing-across-backends", async () => {
    ({ sessionId: root } = await first.client.newSession());
    roots.push(root);
    assert.ok(writer(first, root));
    await observer.client.resume(root, { mode: "read" });
    const command = await turn(first, root, "Streaming fixture");
    await settle(observer, root, command);
    const kinds = new Set(
      first.updates.map(({ update }) => update.sessionUpdate),
    );
    assert.ok(kinds.has("agent_thought_chunk"));
    assert.ok(kinds.has("agent_message_chunk"));
    await wait(
      () => view(first, root).title && view(observer, root).title,
      "live titles on both backends",
    );
    assert.deepEqual(snapshot(observer, root), snapshot(first, root));
    assert.equal(view(observer, root).title, view(first, root).title);
    await assert.rejects(
      observer.client.claimWriter(root),
      (error) => error.code === "not_found",
    );
    await assert.rejects(
      observer.client.prompt(root, text("Read-only prompt")),
      (error) => error.code === "not_found",
    );
    const late = await connect(backendB, "read");
    await late.client.resume(root);
    assert.deepEqual(snapshot(late, root), snapshot(first, root));
    await wait(
      () => view(late, root)?.title === view(first, root).title,
      "replayed title",
    );
    await late.client.disconnect();
    return {
      root,
      command,
      title: view(first, root).title,
      entries: view(first, root).entries.length,
    };
  });

  await scenario("fencing-and-reclaim", async () => {
    await second.client.resume(root, { mode: "write" });
    assert.ok(writer(second, root));
    await wait(
      () =>
        !writer(first, root) && view(first, root).writerReason === "replaced",
      "first writer replaced",
    );
    assert.equal(first.state.connection, "open");
    await assert.rejects(
      first.client.prompt(root, text("Fenced prompt")),
      (error) => error.code === "writer_required",
    );
    const next = await turn(second, root, "Second writer");
    await settle(first, root, next);
    await first.client.claimWriter(root);
    assert.ok(writer(first, root));
    await wait(
      () =>
        !writer(second, root) && view(second, root).writerReason === "replaced",
      "second writer replaced",
    );
    return { root, command: next };
  });

  await scenario("answer-after-takeover", async () => {
    const before = first.requests.length;
    const command = await send(first, root, text("[gate:ask=approved]"));
    const original = await nextRequest(first, root, before, "approval");
    const ask = askOf(original);
    assert.equal(ask.kind, "approval");
    assert.equal(ask.title, "Run memory_write?");
    assert.ok(ask.body.includes("Gate approved"));
    const taken = second.requests.length;
    await second.client.claimWriter(root);
    await wait(() => original.signal.aborted, "old writer's request withdrawn");
    const moved = await nextRequest(second, root, taken, "reissued approval");
    assert.equal(askOf(moved).askId, ask.askId);
    askOf(moved).respond(true);
    await settle(second, root, command);
    await settle(first, root, command);
    const memories = await request(api, "/api/memory");
    assert.ok(memories.some((item) => item.title === "Gate approved"));
    return { root, command, askId: ask.askId };
  });

  await scenario("approval-denial", async () => {
    const before = second.requests.length;
    const command = await send(second, root, text("[gate:ask=denied]"));
    const ask = askOf(await nextRequest(second, root, before, "denial"));
    assert.equal(ask.kind, "approval");
    ask.respond(false);
    await settle(second, root, command);
    const memories = await request(api, "/api/memory");
    assert.ok(memories.some((item) => item.title === "Gate approved"));
    assert.ok(!memories.some((item) => item.title === "Gate denied"));
    return { root, command, memoryTitles: memories.map((item) => item.title) };
  });

  await scenario("typed-choice-and-free-text", async () => {
    const answered = {};
    for (const [name, kind, value] of [
      ["choice", "choice", "Green"],
      ["free-text", "free_text", "Canary rollout"],
    ]) {
      const before = second.requests.length;
      const command = await send(second, root, text(`[gate:tool=${name}]`));
      const ask = askOf(await nextRequest(second, root, before, `${name} ask`));
      assert.equal(ask.kind, kind);
      if (kind === "choice") assert.deepEqual(ask.options, ["Blue", "Green"]);
      ask.respond(value);
      await settle(second, root, command);
      const tool = toolAfter(second, root, command, "e2e_gate");
      assert.equal(tool.status, "completed");
      assert.ok(JSON.stringify(tool.rawOutput).includes(value));
      answered[name] = command;
    }
    return { root, commands: answered };
  });

  await scenario("stop-then-prompt", async () => {
    await post(gate, "/control/gates/io/close", {});
    const command = await send(second, root, text("[gate:tool=io]"));
    await wait(
      () =>
        toolAfter(second, root, command, "e2e_gate")?.status === "in_progress",
      "blocked tool start",
    );
    await second.client.cancel(root);
    await settle(second, root, command, "cancelled");
    assert.ok(
      entriesAfter(second, root, command).some(
        (entry) => entry.kind === "status" && entry.stopReason === "cancelled",
      ),
    );
    await post(gate, "/control/gates/io/open", {});
    const later = await turn(second, root, "Work after Stop");
    await settle(first, root, later);
    return { root, cancelled: command, later };
  });

  await scenario("fifo-order", async () => {
    await post(gate, "/control/gates/model/close", {});
    const held = await send(second, root, text("[gate:provider=model] FIFO 1"));
    await waiting("model");
    const queued = [
      await send(second, root, text("FIFO 2")),
      await send(second, root, text("FIFO 3")),
    ];
    const listed = (peer) =>
      peer.updates.some(
        ({ sessionId, update }) =>
          sessionId === root &&
          update.sessionUpdate === "_tantra_queue_update" &&
          JSON.stringify(update.entries.map((entry) => entry.commandId)) ===
            JSON.stringify(queued),
      );
    await wait(() => listed(second) && listed(first), "queue on both writers");
    await post(gate, "/control/gates/model/open", {});
    await settle(second, root, queued[1]);
    const order = view(second, root)
      .entries.filter(
        (entry) =>
          entry.kind === "user" && [held, ...queued].includes(entry.id),
      )
      .map((entry) => entry.id);
    assert.deepEqual(order, [held, ...queued]);
    assert.ok(endedAt(second, root, held) < startedAt(second, root, queued[0]));
    assert.ok(
      endedAt(second, root, queued[0]) < startedAt(second, root, queued[1]),
    );
    assert.deepEqual(view(second, root).queue, []);
    return { root, commands: [held, ...queued] };
  });

  await scenario("title-and-model", async () => {
    const options = (peer) => view(peer, root)?.configOptions?.[0];
    await post(gate, "/control/gates/model/close", {});
    const busy = await send(second, root, text("[gate:provider=model] busy"));
    await waiting("model");
    await assert.rejects(
      second.client.setModel(root, otherModel),
      (error) => error.code === "model_busy",
    );
    await post(gate, "/control/gates/model/open", {});
    await settle(second, root, busy);
    const configOptions = await retrying(
      () => second.client.setModel(root, otherModel),
      "model_busy",
      "setModel",
    );
    assert.equal(configOptions[0].currentValue, otherModel);
    second.transcript = apply(second.transcript, root, {
      sessionUpdate: "config_option_update",
      configOptions,
    });
    await wait(
      () =>
        options(first)?.currentValue === otherModel &&
        options(observer)?.currentValue === otherModel,
      "model update on other viewers",
    );
    const later = await turn(second, root, "New model fixture");
    const { requests } = await gateState();
    assert.ok(requests.some((item) => item.model === otherModel));
    const title = view(first, root).title;
    assert.ok(title);
    assert.equal(view(observer, root).title, title);
    assert.equal(view(second, root).title, title);
    assert.ok(
      observer.updates.some(
        ({ sessionId, update }) =>
          sessionId === root &&
          update.sessionUpdate === "session_info_update" &&
          update.title === title,
      ),
    );
    return { root, busy, later, model: otherModel, title };
  });

  await scenario("child-and-lazy-history", async () => {
    await post(gate, "/control/gates/worker/close", {});
    const command = await turn(second, root, "[gate:child=worker]");
    const spawned = second.updates.find(
      ({ sessionId, update }) =>
        sessionId === root &&
        update.sessionUpdate === "subagent_update" &&
        update.title === "Gate worker",
    );
    assert.ok(spawned, "child announced");
    const child = spawned.update.sessionId;
    await second.client.resume(child);
    await wait(
      () =>
        view(second, child)?.entries.some((entry) => entry.kind === "message"),
      "child input replayed",
    );
    await post(gate, "/control/gates/worker/open", {});
    await wait(
      () =>
        view(second, child)?.entries.some(
          (entry) => entry.kind === "agent" && entry.final,
        ),
      "child reply written",
    );
    await wait(() => {
      const woke = position(
        second,
        root,
        (update) =>
          update.sessionUpdate === "session_message" &&
          update.senderSessionId === child &&
          update._meta?.tantra?.reply === true,
      );
      return woke >= 0 && position(second, root, idle, woke + 1) >= 0;
    }, "root woken by the child's reply");
    await wait(
      () => second.transcript.roster[child]?.state?.stopReason === "end_turn",
      "child mirrored as replied",
    );
    assert.equal(second.transcript.roster[child].title, "Gate worker");
    assert.deepEqual(descendants(second.transcript, root), [child]);
    const replay = await connect(backendA, "read");
    await replay.client.resume(root);
    await replay.client.resume(child);
    await wait(
      () =>
        view(replay, child)?.entries.length ===
        view(second, child).entries.length,
      "lazy child history",
    );
    assert.deepEqual(view(replay, child).entries, view(second, child).entries);
    await replay.client.disconnect();
    await second.client.close(child);
    assert.equal(second.state.sessions[child], undefined);
    return {
      root,
      command,
      child,
      replayed: view(second, child).entries.length,
    };
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
    const attachment = await request(backendB, "/api/uploads", {
      method: "POST",
      body: form,
    });
    const uri = `file://${attachment.path.split("/").map(encodeURIComponent).join("/")}`;
    const command = await send(second, root, [
      { type: "text", text: "Read the attached document" },
      { type: "resource_link", name: attachment.name, uri },
    ]);
    await settle(second, root, command);
    const user = view(second, root).entries.find(
      (entry) => entry.kind === "user" && entry.id === command,
    );
    assert.deepEqual(user.content, [
      { type: "text", text: "Read the attached document" },
      { type: "resource_link", name: "sample.pdf", uri },
    ]);
    const read = toolAfter(second, root, command, "read_doc");
    assert.ok(JSON.stringify(read.rawOutput).includes("Quellmarsh"));
    const skill = await turn(second, root, "[gate:tool=skill]");
    assert.ok(toolAfter(second, root, skill, "skill"));
    return {
      root,
      commands: [command, skill],
      attachmentName: attachment.name,
    };
  });

  await scenario("lost-prompt-response", async () => {
    const { sessionId: target } = await first.client.newSession();
    roots.push(target);
    await post(gate, "/control/gates/model/close", {});
    const held = await send(first, target, text("[gate:provider=model] held"));
    await waiting("model");
    await second.client.resume(target, { mode: "write" });
    await post(gate, "/control/command-replies/drop-next-send", {});
    const queued = second.client.prompt(target, text("Queued after held turn"));
    const { messageId } = await queued;
    assert.equal(messageId, queued.commandId);
    const { command_reply } = await gateState();
    assert.ok(
      command_reply.drops.some((drop) => drop.command_id === queued.commandId),
    );
    await post(gate, "/control/gates/model/open", {});
    await settle(second, target, queued.commandId);
    const replay = await connect(backendA, "read");
    await replay.client.resume(target);
    const accepted = replay.updates.filter(
      ({ update }) =>
        update.sessionUpdate === "user_message" &&
        update.messageId === queued.commandId,
    );
    assert.equal(accepted.length, 1);
    assert.ok(
      second.updates.every(
        ({ update }) =>
          update.sessionUpdate !== "_tantra_queue_update" ||
          update.entries.filter((entry) => entry.commandId === queued.commandId)
            .length <= 1,
      ),
    );
    await replay.client.disconnect();
    return {
      root: target,
      commands: [held, queued.commandId],
      messageId,
      drops: command_reply.drops.length,
    };
  });

  await scenario("delete", async () => {
    const { sessionId: doomed } = await first.client.newSession();
    roots.push(doomed);
    const command = await turn(first, doomed, "Delete me");
    await observer.client.resume(doomed);
    await first.client.delete(doomed);
    assert.equal(first.state.sessions[doomed], undefined);
    const listed = await listAll(observer);
    assert.ok(!listed.some((item) => item.sessionId === doomed));
    assert.ok(listed.some((item) => item.sessionId === root));
    await assert.rejects(
      observer.client.resume(doomed),
      (error) => error.code === "not_found",
    );
    await assert.rejects(
      first.client.prompt(doomed, text("After delete")),
      (error) => error.code === "closed",
    );
    return { root: doomed, command, listed: listed.length };
  });

  await scenario("owner-death", async () => {
    const { sessionId: target } = await first.client.newSession();
    roots.push(target);
    await observer.client.resume(target);
    await post(gate, "/control/gates/model/close", {});
    const held = await send(first, target, text("[gate:provider=model] owner"));
    await waiting("model");
    first.route.base = backendB;
    ownerStopped = true;
    await dockerCompose("kill", "backend_a");
    await sleep(leaseMs);
    await wait(
      () =>
        first.state.connection === "open" &&
        first.state.sessions[target]?.paused === true,
      "writer resumed read-only through B",
      60000,
    );
    assert.ok(!writer(first, target));
    await retrying(
      () => first.client.claimWriter(target),
      "unavailable",
      "claimWriter",
    );
    const interrupted = (peer) =>
      view(peer, target)?.entries.some(
        (entry) =>
          entry.kind === "status" && entry.stopReason === "_tantra_interrupted",
      );
    await wait(
      () => interrupted(first) && interrupted(observer),
      "abandoned turn interrupted",
      30000,
    );
    await post(gate, "/control/gates/model/open", {});
    const later = await turn(first, target, "After owner death");
    await settle(observer, target, later);
    assert.deepEqual(snapshot(observer, target), snapshot(first, target));
    await dockerCompose("start", "backend_a");
    await wait(() => healthy(backendA), "backend A restored", 120000);
    ownerStopped = false;
    return { root: target, commands: [held, later] };
  });
} catch (error) {
  process.exitCode = 1;
  console.error(String(error));
} finally {
  await Promise.allSettled(clients.map((client) => client.disconnect()));
  if (ownerStopped)
    await dockerCompose("start", "backend_a").catch((error) =>
      console.error(`Could not restart backend_a: ${error}`),
    );
  await post(gate, "/control/reset", {}).catch(() => {});
  const report = {
    passed:
      process.exitCode !== 1 &&
      outcomes.length === SCENARIOS &&
      outcomes.every((outcome) => outcome.passed),
    outcomes,
    roots,
  };
  await mkdir(new URL("reports/018-p5/", import.meta.url), { recursive: true });
  await writeFile(
    new URL("reports/018-p5/client-behavior.json", import.meta.url),
    JSON.stringify(report, null, 2),
  );
}
