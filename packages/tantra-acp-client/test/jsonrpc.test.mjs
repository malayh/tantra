import assert from "node:assert/strict";
import { test } from "node:test";

import { Peer, RpcError } from "../dist/jsonrpc.js";

const peer = (handlers = {}) => {
  const sent = [];
  const instance = new Peer((frame) => sent.push(JSON.parse(frame)), {
    notifications: handlers.notifications ?? {},
    requests: handlers.requests ?? {},
  });
  return { peer: instance, sent };
};

const frame = (message) => JSON.stringify({ jsonrpc: "2.0", ...message });

test("requests get increasing ids and settle before they resolve", async () => {
  const { peer: instance, sent } = peer();
  const order = [];
  const first = instance.request("a", { x: 1 }, (result) =>
    order.push(["settle", result]),
  );
  const second = instance.request("b", undefined);
  assert.deepEqual(sent, [
    { jsonrpc: "2.0", id: 0, method: "a", params: { x: 1 } },
    { jsonrpc: "2.0", id: 1, method: "b" },
  ]);
  void first.then((result) => order.push(["resolve", result]));
  await instance.receive(frame({ id: 0, result: { ok: true } }));
  await first;
  assert.deepEqual(order, [
    ["settle", { ok: true }],
    ["resolve", { ok: true }],
  ]);
  await instance.receive(
    frame({
      id: 1,
      error: { code: -32602, message: "Invalid params", data: { a: 1 } },
    }),
  );
  await assert.rejects(second, (error) => {
    assert.ok(error instanceof RpcError);
    assert.equal(error.code, -32602);
    assert.equal(error.message, "Invalid params");
    assert.deepEqual(error.data, { a: 1 });
    return true;
  });
});

test("a write failure throws synchronously and leaves nothing pending", async () => {
  const instance = new Peer(
    () => {
      throw new Error("closed");
    },
    { notifications: {}, requests: {} },
  );
  assert.throws(() => instance.request("a", {}), /closed/);
  assert.throws(() => instance.notify("b", {}), /closed/);
});

test("malformed frames get -32700 and unknown requests -32601", async () => {
  const { peer: instance, sent } = peer();
  await instance.receive("{not json");
  await instance.receive(frame({ id: 4, method: "toString", params: {} }));
  await instance.receive(frame({ method: "unknown/notification", params: {} }));
  await instance.receive(frame({ id: 99, result: {} }));
  assert.deepEqual(sent, [
    {
      jsonrpc: "2.0",
      id: null,
      error: { code: -32700, message: "Parse error" },
    },
    {
      jsonrpc: "2.0",
      id: 4,
      error: { code: -32601, message: "Method not found" },
    },
  ]);
});

test("notifications and requests are awaited in order", async () => {
  const seen = [];
  const { peer: instance, sent } = peer({
    notifications: {
      "session/update": async (params) => {
        await new Promise((resolve) => setImmediate(resolve));
        seen.push(params);
      },
    },
    requests: {
      ask: (params, reply) => {
        seen.push(params);
        reply.result({ answered: params.n });
        reply.result({ answered: "again" });
      },
    },
  });
  await instance.receive(frame({ method: "session/update", params: { n: 1 } }));
  await instance.receive(frame({ id: "r", method: "ask", params: { n: 2 } }));
  assert.deepEqual(seen, [{ n: 1 }, { n: 2 }]);
  assert.deepEqual(sent, [
    { jsonrpc: "2.0", id: "r", result: { answered: 2 } },
  ]);
});

test("cancel_request aborts an incoming request and answers -32800", async () => {
  let reply;
  const { peer: instance, sent } = peer({
    requests: {
      ask: (params, handle) => {
        reply = handle;
      },
    },
  });
  await instance.receive(frame({ id: 3, method: "ask", params: {} }));
  await instance.receive(
    frame({ method: "$/cancel_request", params: { requestId: 8 } }),
  );
  assert.equal(reply.signal.aborted, false);
  await instance.receive(
    frame({ method: "$/cancel_request", params: { requestId: 3 } }),
  );
  assert.equal(reply.signal.aborted, true);
  reply.result({});
  assert.deepEqual(sent, [
    {
      jsonrpc: "2.0",
      id: 3,
      error: { code: -32800, message: "Request cancelled" },
    },
  ]);
});

test("close rejects pending requests and aborts incoming ones", async () => {
  let reply;
  const { peer: instance, sent } = peer({
    requests: {
      ask: (params, handle) => {
        reply = handle;
      },
    },
  });
  const pending = instance.request("a", {});
  await instance.receive(frame({ id: 0, method: "ask", params: {} }));
  const reason = new Error("gone");
  instance.close(reason);
  await assert.rejects(pending, reason);
  assert.equal(reply.signal.aborted, true);
  reply.result({});
  reply.error(-32603);
  assert.equal(sent.length, 1);
});
