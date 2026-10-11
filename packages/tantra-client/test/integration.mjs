import assert from "node:assert/strict";

import { SessionClient } from "../dist/index.js";

const [baseUrl, rootId, childId] = process.argv.slice(2);
assert.ok(baseUrl && rootId && childId);

const sleep = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));
const waitFor = async (predicate, label, timeout = 10_000) => {
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
    headers:
      body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  assert.equal(response.status, 200);
  return response.json();
};
const uuidHex = (value) => value.replaceAll("-", "");
const canonicalUuid = (value) =>
  value.includes("-")
    ? value
    : `${value.slice(0, 8)}-${value.slice(8, 12)}-${value.slice(12, 16)}-${value.slice(16, 20)}-${value.slice(20)}`;
const timing = {
  receiptTimeoutMs: 200,
  retryDelaysMs: [20, 30, 40],
  reconnectBaseMs: 20,
  reconnectCapMs: 100,
  capacityTimeoutMs: 500,
};
const makeClient = (worker, events, receipts, errors) =>
  new SessionClient({
    serverScope: "integration",
    rootId,
    identity: "integration-user",
    url: () => `${baseUrl.replace("http://", "ws://")}/${worker}?token=test`,
    onEvent: async (frame) => events.push(frame),
    onReceipt: async (frame) => receipts.push(frame),
    onError: (error) => errors.push(error),
    timing,
  });

const oneEvents = [];
const oneReceipts = [];
const oneErrors = [];
const twoEvents = [];
const twoReceipts = [];
const twoErrors = [];
const one = makeClient("one", oneEvents, oneReceipts, oneErrors);
const two = makeClient("two", twoEvents, twoReceipts, twoErrors);

try {
  await one.connect();
  const rootOne = one.subscribe(rootId);
  const childOne = one.subscribe(childId);
  await Promise.all([rootOne.ready(), childOne.ready()]);
  assert.ok(
    oneEvents.some(
      (frame) => frame.actor_id === rootId && frame.body?.text === "root-seed",
    ),
  );
  assert.ok(
    oneEvents.some(
      (frame) =>
        frame.actor_id === childId && frame.body?.text === "child-seed",
    ),
  );
  assert.equal(childOne.state.writable, false);
  await one.acquireWriter();
  assert.equal(rootOne.state.writable, true);

  await two.connect();
  const rootTwo = two.subscribe(rootId);
  await rootTwo.ready();
  await two.acquireWriter();
  await waitFor(
    () => rootOne.state.ready && !rootOne.state.writable,
    "first writer downgrade",
  );
  await one.acquireWriter();
  await waitFor(
    () => rootTwo.state.ready && !rootTwo.state.writable,
    "second writer downgrade",
  );
  await two.acquireWriter();
  await waitFor(
    () => rootOne.state.ready && !rootOne.state.writable,
    "explicit remote reclaim",
  );

  const approvalCommand = crypto.randomUUID();
  const approvalReceipt = await two.send("approval", {
    commandId: approvalCommand,
  });
  assert.equal(approvalReceipt.duplicate, false);
  const raised = await waitFor(
    () => twoEvents.find((frame) => frame.body?.type === "ask_raised"),
    "durable approval ask",
  );
  assert.deepEqual(raised.body.request.extra, {
    permission: "deploy",
    arguments: { target: "prod" },
  });
  await request("/control/lose", { worker: 2, operation: "answer" });
  const answerCommand = crypto.randomUUID();
  const answerReceipt = await two.answer(
    canonicalUuid(raised.body.ask_id),
    { kind: "approval", allow: true },
    { commandId: answerCommand },
  );
  assert.equal(answerReceipt.duplicate, true);
  await waitFor(
    () => twoEvents.some((frame) => frame.body?.type === "turn_completed"),
    "approved turn",
  );

  const blockedCommand = crypto.randomUUID();
  const blockedReceipt = await two.send("blocked", {
    commandId: blockedCommand,
  });
  assert.equal(blockedReceipt.duplicate, false);
  await waitFor(
    async () => (await request("/control/state")).blocked.some(Boolean),
    "blocked provider",
  );
  const replayAfter = rootTwo.state.cursor;
  await two.disconnect();
  assert.equal(two.state.connection, "closed");
  await request("/control/release", {});
  await waitFor(async () => {
    const journal = await request("/control/journal");
    return journal[rootId].some(
      (event) =>
        event.type === "turn_completed" &&
        event.turn_id === uuidHex(blockedCommand),
    );
  }, "accepted work after disconnect");
  const replayStart = twoEvents.length;
  await two.connect();
  await rootTwo.ready();
  assert.equal(rootTwo.state.writable, false);
  assert.ok(rootTwo.state.cursor > replayAfter);
  assert.ok(
    twoEvents
      .slice(replayStart)
      .some(
        (frame) =>
          frame.body?.type === "turn_completed" &&
          frame.body.turn_id === uuidHex(blockedCommand),
      ),
  );

  const journal = await request("/control/journal");
  const rootEvents = journal[rootId];
  const approvalInputs = rootEvents.filter(
    (event) =>
      event.type === "input_queued" &&
      event.command_id === uuidHex(approvalCommand),
  );
  const blockedInputs = rootEvents.filter(
    (event) =>
      event.type === "input_queued" &&
      event.command_id === uuidHex(blockedCommand),
  );
  const answers = rootEvents.filter(
    (event) =>
      event.type === "ask_answered" &&
      event.command_id === uuidHex(answerCommand),
  );
  assert.equal(approvalInputs.length, 1);
  assert.equal(blockedInputs.length, 1);
  assert.equal(answers.length, 1);
  assert.equal(approvalInputs[0].input, "approval");
  assert.equal(blockedInputs[0].input, "blocked");
  assert.equal(approvalInputs[0].submitted_by, "integration-user");
  assert.equal(blockedInputs[0].submitted_by, "integration-user");
  assert.equal(answers[0].submitted_by, "integration-user");
  const state = await request("/control/state");
  assert.deepEqual(state.invoked, ["prod"]);
  assert.equal(state.approvals, 2);
  assert.ok(
    twoReceipts.some(
      (receipt) => receipt.command_id === answerCommand && receipt.duplicate,
    ),
  );

  assert.deepEqual(await request("/control/delete", {}), { deleted: true });
  await waitFor(
    () =>
      oneErrors.some((error) => error.code === "deleted") &&
      twoErrors.some((error) => error.code === "deleted"),
    "deletion closure",
  );
  await waitFor(async () => {
    const cleaned = await request("/control/state");
    return cleaned.workers.every((worker) =>
      Object.values(worker).every((value) => value === 0),
    );
  }, "socket and subscription cleanup");
} finally {
  await Promise.allSettled([one.disconnect(), two.disconnect()]);
}
