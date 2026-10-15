import fs from "node:fs";
import readline from "node:readline";

import {
  TantraAcpClient,
  cursorOf,
  isAfter,
} from "../../packages/tantra-acp-client/dist/index.js";

const config = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const clients = [];
const observers = [];
const writers = [];
const errors = [];
const receipts = [];
const commands = {};
const progress = new Map();
const terminals = new Map();
const activeStarted = new Map();
const deliveryLatencyMs = [];
const slowRoots = new Set(config.rootIds.slice(0, config.slowReaders));
const metrics = {
  maxIncomingFrameBytes: 0,
  maxBufferedAmount: 0,
  maxBacklog: 0,
  reconnects: 0,
  dials: 0,
};

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const sample = (sockets) => {
  for (const socket of sockets)
    metrics.maxBufferedAmount = Math.max(
      metrics.maxBufferedAmount,
      socket.bufferedAmount,
    );
};
const waitFor = async (predicate, label, timeout = config.timeoutMs) => {
  const deadline = performance.now() + timeout;
  while (performance.now() < deadline) {
    if (predicate()) return;
    await sleep(10);
  }
  throw new Error(`timed out waiting for ${label}`);
};
const batches = async (items, width, action) => {
  for (let offset = 0; offset < items.length; offset += width)
    await Promise.all(items.slice(offset, offset + width).map(action));
};
const watch = (socket, key, received) => {
  let previous;
  socket.addEventListener("message", (event) => {
    if (typeof event.data !== "string") return;
    metrics.maxIncomingFrameBytes = Math.max(
      metrics.maxIncomingFrameBytes,
      Buffer.byteLength(event.data),
    );
    const message = JSON.parse(event.data);
    if (message.method !== "session/update") return;
    received();
    const cursor = cursorOf(message.params.update);
    if (cursor === undefined) return;
    if (previous !== undefined && !isAfter(cursor, previous))
      errors.push(`cursor order for ${key}: ${cursor} after ${previous}`);
    previous = cursor;
  });
};
const observe = (rootId, update) => {
  const command = commands[rootId];
  if (command === undefined) return;
  const step = progress.get(rootId);
  if (update.sessionUpdate === "user_message" && update.messageId === command)
    progress.set(rootId, "prompted");
  else if (
    step === "prompted" &&
    update.sessionUpdate === "agent_message" &&
    update.content.some(
      (block) => block.type === "text" && block.text.trim() === "60",
    )
  )
    progress.set(rootId, "answered");
  else if (
    step !== undefined &&
    update.sessionUpdate === "state_update" &&
    update.state === "idle"
  ) {
    progress.delete(rootId);
    if (step !== "answered" || update.stopReason !== "end_turn") {
      errors.push(
        `unexpected terminal for ${rootId}: ${step} ${update.stopReason}`,
      );
      return;
    }
    terminals.set(rootId, command);
    const started = activeStarted.get(rootId);
    if (started !== undefined)
      deliveryLatencyMs.push(performance.now() - started);
  }
};
const makeClient = (rootId, worker, slow, observer) => {
  const sockets = [];
  const key = `${observer ? "observer" : "writer"}:${rootId}`;
  let opened = false;
  let backlog = 0;
  const client = new TantraAcpClient({
    url: () => `${config.urls[worker]}/acp?token=acp-bench`,
    webSocketFactory: (url) => {
      const socket = new WebSocket(url);
      metrics.dials += 1;
      sockets.push(socket);
      watch(socket, key, () => {
        backlog += 1;
        metrics.maxBacklog = Math.max(metrics.maxBacklog, backlog);
      });
      return socket;
    },
    onUpdate: async (_sessionId, update) => {
      sample(sockets);
      if (slow) await sleep(config.slowDelayMs);
      if (observer) observe(rootId, update);
      backlog -= 1;
    },
    onState: (state) => {
      sample(sockets);
      if (state.connection === "open") opened = true;
      else if (state.connection === "connecting" && opened)
        metrics.reconnects += 1;
    },
    onError: (error) => errors.push(`${key} ${error.code}: ${error.message}`),
    timing: { reconnectBaseMs: 100, reconnectCapMs: 1_000 },
  });
  clients.push(client);
  return client;
};
const emit = (value) => process.stdout.write(`${JSON.stringify(value)}\n`);

const start = async () => {
  await batches(config.rootIds, 50, async (rootId) => {
    const index = config.rootIds.indexOf(rootId);
    const worker = index % 2;
    const client = makeClient(rootId, worker, slowRoots.has(rootId), true);
    await client.connect();
    await client.resume(rootId, { mode: "read" });
    observers.push({ rootId, client, worker });
  });
  emit({ phase: "ready", observers: observers.length, errors, metrics });
};

const run = async () => {
  const active = config.rootIds.slice(0, config.active);
  for (const rootId of active) commands[rootId] = crypto.randomUUID();
  await batches(active, 25, async (rootId) => {
    const observer = observers.find((item) => item.rootId === rootId);
    const worker = 1 - observer.worker;
    const client = makeClient(rootId, worker, false, false);
    await client.connect();
    await client.resume(rootId, { mode: "write" });
    writers.push({ rootId, client, worker });
  });
  const started = performance.now();
  await Promise.all(
    writers.map(async ({ rootId, client }) => {
      activeStarted.set(rootId, performance.now());
      const receipt = await client.prompt(
        rootId,
        [{ type: "text", text: config.input }],
        { commandId: commands[rootId] },
      );
      receipts.push(receipt);
      if (receipt.messageId !== commands[rootId])
        errors.push(`incorrect messageId for ${rootId}`);
    }),
  );
  await waitFor(
    () => active.every((rootId) => terminals.get(rootId) === commands[rootId]),
    "observer terminal delivery",
  );
  const slowProgress = [...slowRoots].every(
    (rootId) => terminals.get(rootId) === commands[rootId],
  );
  emit({
    phase: "active",
    active: active.length,
    commands,
    receipts: receipts.length,
    terminals: terminals.size,
    elapsedMs: performance.now() - started,
    deliveryLatencyMs,
    slowProgress,
    errors,
    metrics,
  });
};

const close = async () => {
  await batches(clients, 100, (client) => client.disconnect());
  emit({ phase: "closed", clients: clients.length, errors, metrics });
};

await start();
const lines = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});
for await (const line of lines) {
  if (line === "run") await run();
  if (line === "close") {
    await close();
    break;
  }
}
lines.close();
