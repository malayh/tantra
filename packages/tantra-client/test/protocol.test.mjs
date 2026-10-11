import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  canonicalUuid,
  cursor,
  jsonBytes,
  jsonSnapshot,
  parseServerFrame,
  validateAnswer,
} from "../dist/protocol.js";

const fixtures = JSON.parse(
  await readFile(new URL("./fixtures/protocol.json", import.meta.url), "utf8"),
);
const subscriptionId = "12345678-1234-1234-1234-123456789abc";

test("shared server frames parse exactly", () => {
  for (const frame of fixtures.server_frames) {
    assert.deepEqual(parseServerFrame(JSON.stringify(frame)), frame);
  }
});

test("shared typed answers validate exactly", () => {
  for (const answer of fixtures.answers)
    assert.deepEqual(validateAnswer(answer), answer);
});

test("server frames reject malformed JSON, unknown types, missing fields, and extra fields", () => {
  assert.throws(() => parseServerFrame("{"), TypeError);
  assert.throws(() => parseServerFrame("null"), TypeError);
  assert.throws(() => parseServerFrame('{"type":"other"}'), TypeError);
  assert.throws(() => parseServerFrame('{"type":"notice"}'), TypeError);
  for (const frame of fixtures.server_frames) {
    assert.throws(
      () => parseServerFrame(JSON.stringify({ ...frame, extra: true })),
      TypeError,
    );
  }
});

test("server frames reject duplicate JSON keys at every depth and after escape decoding", () => {
  for (const text of [
    '{"type":"notice","body":null,"body":true}',
    '{"type":"notice","body":{"key":1,"key":2}}',
    '{"type":"notice","body":[{"key":1,"k\\u0065y":2}]}',
    '{"type":"notice","body":{"\\u006eame":1,"name":2}}',
  ]) {
    assert.throws(() => parseServerFrame(text), /duplicate JSON key/);
  }
  const distinctObjects = {
    type: "notice",
    body: [{ key: 1 }, { key: 2 }],
  };
  assert.deepEqual(
    parseServerFrame(JSON.stringify(distinctObjects)),
    distinctObjects,
  );
});

test("server frames reject coercion, unsafe cursors, invalid modes, and invalid operations", () => {
  const event = fixtures.server_frames[0];
  const ready = fixtures.server_frames[2];
  const receipt = fixtures.server_frames[3];
  for (const seq of [true, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(
      () => parseServerFrame(JSON.stringify({ ...event, seq })),
      TypeError,
    );
  }
  assert.throws(
    () => parseServerFrame(JSON.stringify({ ...ready, active: 1 })),
    TypeError,
  );
  assert.throws(
    () => parseServerFrame(JSON.stringify({ ...ready, mode: "owner" })),
    TypeError,
  );
  assert.throws(
    () => parseServerFrame(JSON.stringify({ ...receipt, duplicate: 0 })),
    TypeError,
  );
  assert.throws(
    () =>
      parseServerFrame(JSON.stringify({ ...receipt, operation: "subscribe" })),
    TypeError,
  );
});

test("server frames reject noncanonical UUIDs and nonfinite parsed numbers", () => {
  const event = fixtures.server_frames[0];
  for (const subscription_id of [
    "12345678123412341234123456789abc",
    "12345678-1234-1234-1234-123456789ABC",
    `{${subscriptionId}}`,
    `${subscriptionId}0`,
  ]) {
    assert.throws(
      () => parseServerFrame(JSON.stringify({ ...event, subscription_id })),
      TypeError,
    );
  }
  assert.throws(
    () => parseServerFrame(`{"type":"notice","body":1e999}`),
    TypeError,
  );
});

test("error defaults and nullable IDs match the Python model", () => {
  assert.deepEqual(
    parseServerFrame('{"type":"error","code":"busy","message":"wait"}'),
    {
      type: "error",
      code: "busy",
      message: "wait",
      subscription_id: null,
      command_id: null,
      retryable: false,
      uncertain: false,
    },
  );
  assert.throws(
    () =>
      parseServerFrame(
        '{"type":"error","code":"busy","message":"wait","subscription_id":false}',
      ),
    TypeError,
  );
});

test("typed answers reject coercion, unknown kinds, missing fields, and extra fields", () => {
  for (const answer of [
    { kind: "approval", allow: 1 },
    { kind: "approval", allow: true, title: "forged" },
    { kind: "choice", selected: true },
    { kind: "free_text" },
    { kind: "other", text: "no" },
  ]) {
    assert.throws(() => validateAnswer(answer), TypeError);
  }
});

test("jsonSnapshot clones finite JSON values and shared references", () => {
  const child = { text: "hello" };
  const input = JSON.parse(
    '{"left":null,"right":null,"values":[null,true,3.5],"__proto__":{"safe":true}}',
  );
  input.left = child;
  input.right = child;
  const snapshot = jsonSnapshot(input);
  assert.deepEqual(snapshot, input);
  assert.notEqual(snapshot, input);
  assert.notEqual(snapshot.left, child);
  assert.notEqual(snapshot.left, snapshot.right);
  assert.equal(Object.getPrototypeOf(snapshot), Object.prototype);
  assert.deepEqual(snapshot.__proto__, { safe: true });
});

test("jsonSnapshot rejects values serialization would alter", () => {
  const cycle = {};
  cycle.self = cycle;
  const accessor = {};
  Object.defineProperty(accessor, "value", { enumerable: true, get: () => 1 });
  const hidden = {};
  Object.defineProperty(hidden, "value", { value: 1 });
  const sparse = Array(1);
  const extraArray = [1];
  extraArray.extra = true;
  for (const value of [
    undefined,
    1n,
    Symbol("x"),
    () => null,
    NaN,
    Infinity,
    new Date(),
    Object.create(null),
    cycle,
    accessor,
    hidden,
    sparse,
    extraArray,
    { missing: undefined },
  ]) {
    assert.throws(() => jsonSnapshot(value), TypeError);
  }
});

test("UUID, cursor, and UTF-8 byte helpers are strict", () => {
  assert.equal(canonicalUuid(subscriptionId), subscriptionId);
  assert.equal(cursor(0), 0);
  assert.throws(() => canonicalUuid(1), TypeError);
  assert.throws(() => cursor(true), TypeError);
  assert.equal(
    jsonBytes({ text: "é" }).byteLength,
    Buffer.byteLength('{"text":"é"}'),
  );
});
