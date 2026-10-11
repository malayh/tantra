import fs from "node:fs";
import readline from "node:readline";

import { SessionClient } from "../../packages/tantra-client/dist/index.js";

const config = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const clients = [];
const observers = [];
const writers = [];
const errors = [];
const receipts = [];
const terminals = new Map();
const sequences = new Map();
const activeStarted = new Map();
const deliveryLatencyMs = [];
const slowRoots = new Set(config.rootIds.slice(0, config.slowReaders));
const metrics = {
  maxIncomingFrames: 0,
  maxIncomingBytes: 0,
  maxBufferedAmount: 0,
  maxOutbox: 0,
};

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const hex = (value) => value.replaceAll("-", "");
const sample = (client, sockets) => {
  metrics.maxIncomingFrames = Math.max(
    metrics.maxIncomingFrames,
    client.incomingFrames ?? 0,
  );
  metrics.maxIncomingBytes = Math.max(
    metrics.maxIncomingBytes,
    client.incomingBytes ?? 0,
  );
  metrics.maxOutbox = Math.max(metrics.maxOutbox, client.state.outbox.length);
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
const makeClient = (rootId, worker, slow, observer) => {
  const sockets = [];
  let client;
  client = new SessionClient({
    serverScope: `socket-bench-${worker}`,
    rootId,
    identity: "socket-bench",
    url: () => `${config.urls[worker]}/${rootId}?token=socket-bench`,
    webSocketFactory: (url, protocol) => {
      const socket = new WebSocket(url, protocol);
      sockets.push(socket);
      return socket;
    },
    onEvent: async (frame) => {
      sample(client, sockets);
      const sequenceKey = `${observer ? "observer" : "writer"}:${rootId}`;
      const previous = sequences.get(sequenceKey) ?? 0;
      if (frame.seq !== previous + 1)
        errors.push(
          `sequence gap for ${sequenceKey}: ${frame.seq} after ${previous}`,
        );
      sequences.set(sequenceKey, frame.seq);
      if (slow) await sleep(config.slowDelayMs);
      const command = frame.body?.turn_id;
      if (
        observer &&
        frame.body?.type === "turn_completed" &&
        typeof command === "string"
      ) {
        terminals.set(rootId, command);
        const started = activeStarted.get(rootId);
        if (started !== undefined)
          deliveryLatencyMs.push(performance.now() - started);
      }
    },
    onReceipt: async (receipt) => receipts.push(receipt),
    onState: () => sample(client, sockets),
    onError: (error) => errors.push(`${error.code}: ${error.message}`),
    timing: {
      receiptTimeoutMs: 30_000,
      retryDelaysMs: [100, 250, 500],
      reconnectBaseMs: 100,
      reconnectCapMs: 1_000,
      capacityTimeoutMs: 30_000,
    },
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
    const subscription = client.subscribe(rootId);
    await subscription.ready();
    observers.push({ rootId, client, subscription, worker });
  });
  emit({ phase: "ready", observers: observers.length, errors, metrics });
};

const run = async () => {
  const active = config.rootIds.slice(0, config.active);
  const commands = Object.fromEntries(
    active.map((rootId) => [rootId, crypto.randomUUID()]),
  );
  await batches(active, 25, async (rootId) => {
    const observer = observers.find((item) => item.rootId === rootId);
    const worker = 1 - observer.worker;
    const client = makeClient(rootId, worker, false, false);
    await client.connect();
    const subscription = client.subscribe(rootId);
    await subscription.ready();
    await client.acquireWriter();
    writers.push({ rootId, client, subscription, worker });
  });
  const started = performance.now();
  await Promise.all(
    writers.map(async ({ rootId, client }) => {
      activeStarted.set(rootId, performance.now());
      const receipt = await client.send(config.input, {
        commandId: commands[rootId],
      });
      if (receipt.command_id !== commands[rootId] || receipt.duplicate)
        errors.push(`incorrect receipt for ${rootId}`);
    }),
  );
  await waitFor(
    () =>
      active.every((rootId) => terminals.get(rootId) === hex(commands[rootId])),
    "observer terminal delivery",
  );
  const slowProgress = [...slowRoots].every(
    (rootId) => terminals.get(rootId) === hex(commands[rootId]),
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
