import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { test } from "node:test";

import { FakeAgent, until } from "./fake-socket.mjs";

const directory = new URL(
  "../../tantra/tests/fixtures/acp_profile/",
  import.meta.url,
);
const fixtures = Object.fromEntries(
  readdirSync(directory)
    .filter((name) => name.endsWith(".json"))
    .map((name) => [
      name.slice(0, -5),
      JSON.parse(readFileSync(new URL(name, directory), "utf8")),
    ]),
);
const ROOT = fixtures.prompt_request.params.sessionId;
const CHILD = fixtures.resume_child_request.params.sessionId;
const withoutId = ({ id, ...frame }) => {
  void id;
  return frame;
};

const harness = async (options = {}) => {
  const agent = new FakeAgent();
  const updates = [];
  const states = [];
  const client = agent.client({
    cwd: "/work",
    onUpdate: (sessionId, update) => {
      updates.push({ sessionId, update });
    },
    onState: (state) => states.push(state),
    ...options,
  });
  await client.connect();
  await client.resume(ROOT);
  return { agent, client, updates, state: () => states.at(-1) };
};

const push = (agent, fixture) => agent.later(agent.socket, fixture);

test("outbound frames match the profile fixtures", async () => {
  const { agent, client, state } = await harness();
  const [initialize] = agent.sent("initialize");
  const { elicitation, ...capabilities } = initialize.params.capabilities;
  assert.deepEqual(elicitation, { form: {} });
  assert.deepEqual(
    withoutId({
      ...initialize,
      params: { ...initialize.params, capabilities },
    }),
    withoutId(fixtures.initialize_request),
  );

  const commandId = fixtures.prompt_request.params._meta.tantra.commandId;
  await client.prompt(ROOT, fixtures.prompt_request.params.prompt, {
    commandId,
  });
  assert.deepEqual(
    withoutId(agent.sent("session/prompt")[0]),
    withoutId(fixtures.prompt_request),
  );

  await client.cancel(ROOT, {
    commandId: fixtures.cancel_notification.params._meta.tantra.commandId,
  });
  assert.deepEqual(
    agent.sent("session/cancel")[0],
    fixtures.cancel_notification,
  );

  await client.claimWriter(ROOT);
  assert.deepEqual(
    withoutId(agent.sent("_tantra/session/claim_writer")[0]),
    withoutId(fixtures.claim_writer_request),
  );

  push(agent, fixtures.cursor_update);
  await until(() => state().sessions[ROOT].cursor === "12.3");
  await client.resume(ROOT);
  assert.deepEqual(
    withoutId(agent.sent("session/resume").at(-1)),
    withoutId(fixtures.resume_after_request),
  );

  await client.resume(CHILD);
  assert.deepEqual(
    withoutId(agent.sent("session/resume").at(-1)),
    withoutId(fixtures.resume_child_request),
  );
});

test("writer fixtures drive the writer flag", async () => {
  const { agent, state } = await harness();
  assert.equal(state().sessions[ROOT].writer, false);
  push(agent, fixtures.writer_claimed);
  await until(() => state().sessions[ROOT].writer === true);
  push(agent, fixtures.writer_replaced);
  await until(() => state().sessions[ROOT].writer === false);
  push(agent, fixtures.writer_claimed);
  await until(() => state().sessions[ROOT].writer === true);
  push(agent, fixtures.writer_lost);
  await until(() => state().sessions[ROOT].writer === false);
});

test("cursor, queue, notice, and mirror fixtures reach onUpdate", async () => {
  const { agent, client, updates, state } = await harness();
  for (const name of [
    "queue_update",
    "notice_writer_required",
    "notice_command_conflict",
    "mirror_subagent_update",
  ])
    push(agent, fixtures[name]);
  push(agent, fixtures.cursor_update);
  await until(() => state().sessions[ROOT].cursor === "12.3");
  assert.deepEqual(
    updates.map(({ sessionId, update }) => ({ sessionId, update })).slice(-5),
    [
      "queue_update",
      "notice_writer_required",
      "notice_command_conflict",
      "mirror_subagent_update",
      "cursor_update",
    ].map((name) => fixtures[name].params),
  );
  await client.resume(CHILD);
  await client.close(ROOT);
  assert.deepEqual(state().sessions, {});
});

test("response fixtures resolve and error fixtures map to codes", async () => {
  const agent = new FakeAgent();
  const responses = {
    initialize: fixtures.initialize_response,
    "_tantra/session/claim_writer": fixtures.claim_writer_response,
  };
  for (const [method, fixture] of Object.entries(responses))
    agent.handlers[method] = function (socket, frame) {
      this.reply(socket, frame, fixture.result);
    };
  const prompts = [
    fixtures.prompt_response,
    fixtures.prompt_command_conflict,
    fixtures.prompt_writer_required,
  ];
  agent.handlers["session/prompt"] = function (socket, frame) {
    const { id, ...body } = prompts.shift();
    void id;
    this.later(socket, { ...body, id: frame.id });
  };
  const client = agent.client({ cwd: "/work" });
  await client.connect();
  await client.resume(ROOT);
  assert.deepEqual(await client.claimWriter(ROOT), {});
  const commandId = fixtures.prompt_response.result.messageId;
  assert.deepEqual(
    await client.prompt(ROOT, [{ type: "text", text: "a" }], { commandId }),
    {
      messageId: commandId,
    },
  );
  await assert.rejects(client.prompt(ROOT, [{ type: "text", text: "b" }]), {
    code: "command_conflict",
    rpcCode: -32602,
    sessionId: ROOT,
  });
  await assert.rejects(client.prompt(ROOT, [{ type: "text", text: "c" }]), {
    code: "writer_required",
    rpcCode: -32603,
  });
});
