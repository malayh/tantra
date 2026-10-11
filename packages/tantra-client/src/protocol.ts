export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type AskResponse =
  | { kind: "approval"; allow: boolean }
  | { kind: "choice"; selected: string }
  | { kind: "free_text"; text: string };

export type CommandOperation = "send" | "answer" | "cancel";

export type CommandPayload =
  | { readonly operation: "send"; readonly input: JsonValue }
  | {
      readonly operation: "answer";
      readonly askId: string;
      readonly response: AskResponse;
    }
  | { readonly operation: "cancel" };

export type EventFrame = {
  type: "event";
  subscription_id: string;
  actor_id: string;
  seq: number;
  body: JsonValue;
};

export type ReadyFrame = {
  type: "ready";
  subscription_id: string;
  actor_id: string;
  cursor: number;
  mode: "read" | "write";
  active: boolean;
};

export type ReceiptFrame = {
  type: "receipt";
  subscription_id: string;
  command_id: string;
  operation: CommandOperation;
  duplicate: boolean;
};

export type WriterLostFrame = {
  type: "writer_lost";
  subscription_id: string;
};

export type ErrorFrame = {
  type: "error";
  code: string;
  message: string;
  subscription_id: string | null;
  command_id: string | null;
  retryable: boolean;
  uncertain: boolean;
};

export type NoticeFrame = {
  type: "notice";
  body: JsonValue;
};

export type UnsubscribedFrame = {
  type: "unsubscribed";
  subscription_id: string;
};

export type ServerFrame =
  | EventFrame
  | ReadyFrame
  | ReceiptFrame
  | WriterLostFrame
  | ErrorFrame
  | NoticeFrame
  | UnsubscribedFrame;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function fail(message: string): never {
  throw new TypeError(message);
}

function cloneJson(value: unknown, ancestors: Set<object>): JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return value;
  if (typeof value === "number")
    return Number.isFinite(value) ? value : fail("JSON numbers must be finite");
  if (typeof value !== "object") return fail("value is not JSON");
  if (ancestors.has(value)) return fail("JSON values cannot contain cycles");

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Object.getPrototypeOf(value) !== Array.prototype)
        return fail("JSON arrays must be plain");
      const keys = Reflect.ownKeys(value);
      if (
        keys.length !== value.length + 1 ||
        keys.some((key) => key !== "length" && typeof key !== "string")
      ) {
        return fail("JSON arrays must be dense and have no extra properties");
      }
      const result: JsonValue[] = [];
      for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(
          value,
          String(index),
        );
        if (
          descriptor === undefined ||
          !("value" in descriptor) ||
          !descriptor.enumerable
        ) {
          return fail("JSON arrays must contain plain data elements");
        }
        result.push(cloneJson(descriptor.value, ancestors));
      }
      return result;
    }

    if (Object.getPrototypeOf(value) !== Object.prototype)
      return fail("JSON objects must be plain");
    const result: { [key: string]: JsonValue } = {};
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string")
        return fail("JSON objects cannot have symbol keys");
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (
        descriptor === undefined ||
        !("value" in descriptor) ||
        !descriptor.enumerable
      ) {
        return fail("JSON objects must contain enumerable data properties");
      }
      Object.defineProperty(result, key, {
        value: cloneJson(descriptor.value, ancestors),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    return result;
  } finally {
    ancestors.delete(value);
  }
}

export function jsonSnapshot<T extends JsonValue>(value: T): T;
export function jsonSnapshot(value: unknown): JsonValue;
export function jsonSnapshot(value: unknown): JsonValue {
  return cloneJson(value, new Set());
}

export function canonicalUuid(value: unknown): string {
  return typeof value === "string" && UUID_PATTERN.test(value)
    ? value
    : fail("value must be a canonical UUID");
}

export function cursor(value: unknown): number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
    ? value
    : fail("cursor must be a nonnegative safe integer");
}

function record(value: JsonValue, name: string): { [key: string]: JsonValue } {
  if (value === null || Array.isArray(value) || typeof value !== "object")
    return fail(`${name} must be an object`);
  return value;
}

function exactFields(
  value: { [key: string]: JsonValue },
  required: string[],
  optional: string[] = [],
): void {
  const allowed = new Set([...required, ...optional]);
  const keys = Object.keys(value);
  if (
    required.some((key) => !Object.hasOwn(value, key)) ||
    keys.some((key) => !allowed.has(key))
  ) {
    fail("frame fields do not match its type");
  }
}

function field(value: { [key: string]: JsonValue }, name: string): JsonValue {
  const result = value[name];
  return result === undefined ? fail(`${name} is required`) : result;
}

function stringField(value: JsonValue, name: string): string {
  return typeof value === "string" ? value : fail(`${name} must be a string`);
}

function booleanField(value: JsonValue, name: string): boolean {
  return typeof value === "boolean" ? value : fail(`${name} must be a boolean`);
}

export function validateAnswer(value: unknown): AskResponse {
  const answer = record(jsonSnapshot(value), "response");
  if (answer.kind === "approval") {
    exactFields(answer, ["kind", "allow"]);
    return {
      kind: "approval",
      allow: booleanField(field(answer, "allow"), "response allow"),
    };
  }
  if (answer.kind === "choice") {
    exactFields(answer, ["kind", "selected"]);
    return {
      kind: "choice",
      selected: stringField(field(answer, "selected"), "response selected"),
    };
  }
  if (answer.kind === "free_text") {
    exactFields(answer, ["kind", "text"]);
    return {
      kind: "free_text",
      text: stringField(field(answer, "text"), "response text"),
    };
  }
  return fail("response kind is invalid");
}

function rejectDuplicateKeys(text: string): void {
  type Container =
    | { kind: "object"; keys: Set<string>; expectKey: boolean }
    | { kind: "array" };
  const stack: Container[] = [];
  for (const match of text.matchAll(/"(?:\\[\s\S]|[^"\\])*"|[{}[\],:]/g)) {
    const token = match[0];
    if (token === "{")
      stack.push({ kind: "object", keys: new Set(), expectKey: true });
    else if (token === "[") stack.push({ kind: "array" });
    else if (token === "}" || token === "]") stack.pop();
    else if (token === ",") {
      const container = stack.at(-1);
      if (container?.kind === "object") container.expectKey = true;
    } else if (token[0] === '"') {
      const container = stack.at(-1);
      if (container?.kind !== "object" || !container.expectKey) continue;
      const key = JSON.parse(token) as string;
      if (container.keys.has(key)) fail(`duplicate JSON key: ${key}`);
      container.keys.add(key);
      container.expectKey = false;
    }
  }
}

export function parseServerFrame(text: string): ServerFrame {
  if (typeof text !== "string") return fail("server frame must be JSON text");
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return fail("server frame is not valid JSON");
  }
  rejectDuplicateKeys(text);
  const frame = record(jsonSnapshot(parsed), "server frame");
  const type = stringField(field(frame, "type"), "frame type");

  if (type === "event") {
    exactFields(frame, ["type", "subscription_id", "actor_id", "seq", "body"]);
    return {
      type,
      subscription_id: canonicalUuid(field(frame, "subscription_id")),
      actor_id: canonicalUuid(field(frame, "actor_id")),
      seq: cursor(field(frame, "seq")),
      body: field(frame, "body"),
    };
  }
  if (type === "ready") {
    exactFields(frame, [
      "type",
      "subscription_id",
      "actor_id",
      "cursor",
      "mode",
      "active",
    ]);
    const mode = frame.mode;
    if (mode !== "read" && mode !== "write")
      return fail("ready mode is invalid");
    return {
      type,
      subscription_id: canonicalUuid(field(frame, "subscription_id")),
      actor_id: canonicalUuid(field(frame, "actor_id")),
      cursor: cursor(field(frame, "cursor")),
      mode,
      active: booleanField(field(frame, "active"), "ready active"),
    };
  }
  if (type === "receipt") {
    exactFields(frame, [
      "type",
      "subscription_id",
      "command_id",
      "operation",
      "duplicate",
    ]);
    const operation = frame.operation;
    if (
      operation !== "send" &&
      operation !== "answer" &&
      operation !== "cancel"
    ) {
      return fail("receipt operation is invalid");
    }
    return {
      type,
      subscription_id: canonicalUuid(field(frame, "subscription_id")),
      command_id: canonicalUuid(field(frame, "command_id")),
      operation,
      duplicate: booleanField(field(frame, "duplicate"), "receipt duplicate"),
    };
  }
  if (type === "writer_lost") {
    exactFields(frame, ["type", "subscription_id"]);
    return {
      type,
      subscription_id: canonicalUuid(field(frame, "subscription_id")),
    };
  }
  if (type === "error") {
    exactFields(
      frame,
      ["type", "code", "message"],
      ["subscription_id", "command_id", "retryable", "uncertain"],
    );
    const subscriptionId = frame.subscription_id;
    const commandId = frame.command_id;
    return {
      type,
      code: stringField(field(frame, "code"), "error code"),
      message: stringField(field(frame, "message"), "error message"),
      subscription_id:
        subscriptionId === undefined || subscriptionId === null
          ? null
          : canonicalUuid(subscriptionId),
      command_id:
        commandId === undefined || commandId === null
          ? null
          : canonicalUuid(commandId),
      retryable:
        frame.retryable === undefined
          ? false
          : booleanField(frame.retryable, "error retryable"),
      uncertain:
        frame.uncertain === undefined
          ? false
          : booleanField(frame.uncertain, "error uncertain"),
    };
  }
  if (type === "notice") {
    exactFields(frame, ["type", "body"]);
    return { type, body: field(frame, "body") };
  }
  if (type === "unsubscribed") {
    exactFields(frame, ["type", "subscription_id"]);
    return {
      type,
      subscription_id: canonicalUuid(field(frame, "subscription_id")),
    };
  }
  return fail("server frame type is invalid");
}

export function jsonBytes(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(jsonSnapshot(value)));
}
