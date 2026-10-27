import type { AgentRequest } from "./client.js";
import {
  cursorOf,
  type ConfigOption,
  type ContentBlock,
  type JsonObject,
  type JsonValue,
  type MessageChunk,
  type QueueEntry,
  type SessionMessage,
  type SessionUpdate,
  type StateBody,
  type StateUpdate,
  type SubagentUpdate,
  type ToolCallContentChunk,
  type ToolCallUpdate,
  type WriterUpdate,
} from "./protocol.js";

export interface UserEntry {
  kind: "user";
  id: string;
  content: ContentBlock[];
}

export interface TextEntry {
  kind: "agent" | "thought";
  id: string;
  text: string;
  final: boolean;
}

export interface ToolEntry {
  kind: "tool";
  id: string;
  name?: string;
  title?: string;
  toolKind?: string;
  status?: string;
  content?: JsonObject[];
  rawInput?: JsonValue;
  rawOutput?: JsonValue;
  progress: string[];
}

export interface MessageEntry {
  kind: "message";
  id: string;
  sender: string | null;
  recipient: string | null;
  text: string;
  finished: boolean;
  reply: boolean;
}

export interface StatusEntry {
  kind: "status";
  id: string;
  stopReason: string;
  error?: NonNullable<StateBody["error"]>;
}

export type Entry =
  UserEntry | TextEntry | ToolEntry | MessageEntry | StatusEntry;

export interface AskMarker {
  askId: string;
  state: "pending" | "answered";
}

export interface ViewState extends StateBody {
  ask?: AskMarker;
}

export interface SessionView {
  entries: Entry[];
  state: ViewState | null;
  title: string | null;
  updatedAt: string | null;
  configOptions: ConfigOption[];
  queue: QueueEntry[];
  writerReason: WriterUpdate["reason"] | null;
}

export interface RosterEntry {
  parentId: string;
  title: string | null;
  state: StateBody | null;
}

export interface Transcript {
  sessions: Record<string, SessionView>;
  roster: Record<string, RosterEntry>;
  requests: Record<string, Record<string, AgentRequest>>;
}

interface AskBase {
  askId: string;
  title: string;
  body: string | null;
  options: string[];
}

export interface ApprovalAsk extends AskBase {
  kind: "approval";
  respond(allow: boolean): void;
}

export interface ChoiceAsk extends AskBase {
  kind: "choice";
  respond(choice: string): void;
}

export interface FreeTextAsk extends AskBase {
  kind: "free_text";
  respond(text: string): void;
}

export type Ask = ApprovalAsk | ChoiceAsk | FreeTextAsk;

type Patch<T> = { [K in keyof T]?: T[K] | undefined };

const STOPS: ReadonlySet<string> = new Set([
  "cancelled",
  "_tantra_interrupted",
  "error",
]);

const blank = (): SessionView => ({
  entries: [],
  state: null,
  title: null,
  updatedAt: null,
  configOptions: [],
  queue: [],
  writerReason: null,
});

const object = (value: JsonValue | undefined): JsonObject | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? value
    : undefined;

const tantraOf = (meta: JsonObject | undefined): JsonObject | undefined =>
  object(meta?.["tantra"]);

const textOf = (blocks: ContentBlock[], separator = ""): string =>
  blocks
    .flatMap((block) =>
      block.type === "text" && typeof block["text"] === "string"
        ? [block["text"]]
        : [],
    )
    .join(separator);

function same(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (typeof left !== "object" || typeof right !== "object") return false;
  if (left === null || right === null) return false;
  if (Array.isArray(left) !== Array.isArray(right)) return false;
  const a = left as Record<string, unknown>;
  const b = right as Record<string, unknown>;
  const keys = Object.keys(a);
  return (
    keys.length === Object.keys(b).length &&
    keys.every((key) => key in b && same(a[key], b[key]))
  );
}

function patch<T extends object>(base: T, changes: Patch<T>): T {
  const changed = Object.entries(changes).filter(
    ([key, value]) => value !== undefined && !same(base[key as keyof T], value),
  );
  return changed.length === 0
    ? base
    : { ...base, ...Object.fromEntries(changed) };
}

const locate = (view: SessionView, kind: Entry["kind"], id: string) =>
  view.entries.find((entry) => entry.kind === kind && entry.id === id);

function upsert(view: SessionView, entry: Entry): SessionView {
  const index = view.entries.findIndex(
    (item) => item.kind === entry.kind && item.id === entry.id,
  );
  if (index < 0) return { ...view, entries: [...view.entries, entry] };
  if (same(view.entries[index], entry)) return view;
  return {
    ...view,
    entries: view.entries.map((item, at) => (at === index ? entry : item)),
  };
}

function chunk(view: SessionView, update: MessageChunk): SessionView {
  const text = textOf([update.content]);
  if (!text) return view;
  const kind =
    update.sessionUpdate === "agent_message_chunk" ? "agent" : "thought";
  const previous = locate(view, kind, update.messageId) as
    TextEntry | undefined;
  const joint = kind === "thought" && previous?.final ? "\n\n" : "";
  return upsert(view, {
    kind,
    id: update.messageId,
    text: (previous?.text ?? "") + joint + text,
    final: false,
  });
}

function tool(view: SessionView, update: ToolCallUpdate): SessionView {
  const previous = locate(view, "tool", update.toolCallId) as
    ToolEntry | undefined;
  const base: ToolEntry = previous ?? {
    kind: "tool",
    id: update.toolCallId,
    progress: [],
  };
  return upsert(
    view,
    patch<ToolEntry>(base, {
      name: update.name,
      title: update.title,
      toolKind: update.kind,
      status: update.status,
      content: update.content,
      rawInput: update.rawInput,
      rawOutput: update.rawOutput,
    }),
  );
}

function progress(
  view: SessionView,
  update: ToolCallContentChunk,
): SessionView {
  const text = textOf([update.content.content]);
  const previous = locate(view, "tool", update.toolCallId) as
    ToolEntry | undefined;
  const status = previous?.status;
  if (!text || status === "completed" || status === "failed") return view;
  const base: ToolEntry = previous ?? {
    kind: "tool",
    id: update.toolCallId,
    progress: [],
  };
  return upsert(view, { ...base, progress: [...base.progress, text] });
}

const message = (update: SessionMessage): MessageEntry => ({
  kind: "message",
  id: update.messageId,
  sender: update.senderSessionId ?? null,
  recipient: update.recipientSessionId ?? null,
  text: textOf(update.content),
  finished: tantraOf(update._meta)?.["finished"] === true,
  reply: tantraOf(update._meta)?.["reply"] === true,
});

function state(view: SessionView, update: StateUpdate): SessionView {
  const next: ViewState = { state: update.state };
  const { stopReason, error } = update;
  const ask = object(tantraOf(update._meta)?.["ask"]);
  if (stopReason !== undefined) next.stopReason = stopReason;
  if (error !== undefined) next.error = error;
  if (ask) next.ask = ask as unknown as AskMarker;
  const changed = same(view.state, next) ? view : { ...view, state: next };
  const cursor = cursorOf(update);
  if (cursor === undefined || update.state !== "idle") return changed;
  if (stopReason === undefined || !STOPS.has(stopReason)) return changed;
  const status: StatusEntry = { kind: "status", id: cursor, stopReason };
  if (error !== undefined) status.error = error;
  return upsert(changed, status);
}

function reduce(view: SessionView, update: SessionUpdate): SessionView {
  switch (update.sessionUpdate) {
    case "user_message":
      return upsert(view, {
        kind: "user",
        id: update.messageId,
        content: update.content,
      });
    case "agent_message":
      return upsert(view, {
        kind: "agent",
        id: update.messageId,
        text: textOf(update.content),
        final: true,
      });
    case "agent_thought":
      return upsert(view, {
        kind: "thought",
        id: update.messageId,
        text: textOf(update.content, "\n\n"),
        final: true,
      });
    case "agent_message_chunk":
    case "agent_thought_chunk":
      return chunk(view, update);
    case "tool_call_update":
      return tool(view, update);
    case "tool_call_content_chunk":
      return progress(view, update);
    case "session_message":
      return upsert(view, message(update));
    case "state_update":
      return state(view, update);
    case "session_info_update":
      return patch(view, { title: update.title, updatedAt: update.updatedAt });
    case "config_option_update":
      return patch(view, { configOptions: update.configOptions });
    case "_tantra_queue_update":
      return patch(view, { queue: update.entries });
    case "_tantra_writer":
      return patch(view, { writerReason: update.reason });
    default:
      return view;
  }
}

function subagent(
  t: Transcript,
  parentId: string,
  update: SubagentUpdate,
): Transcript {
  const previous = t.roster[update.sessionId];
  const next: RosterEntry = {
    parentId,
    title: update.title ?? previous?.title ?? null,
    state: update.state ?? previous?.state ?? null,
  };
  if (same(previous, next)) return t;
  return { ...t, roster: { ...t.roster, [update.sessionId]: next } };
}

export function empty(): Transcript {
  return { sessions: {}, roster: {}, requests: {} };
}

export function apply(
  t: Transcript,
  sessionId: string,
  update: SessionUpdate,
): Transcript {
  if (update.sessionUpdate === "subagent_update")
    return subagent(t, sessionId, update);
  const view = t.sessions[sessionId] ?? blank();
  const next = reduce(view, update);
  if (next === view) return t;
  return { ...t, sessions: { ...t.sessions, [sessionId]: next } };
}

export function reset(t: Transcript, sessionId: string): Transcript {
  if (!(sessionId in t.sessions) && !(sessionId in t.requests)) return t;
  const { [sessionId]: view, ...sessions } = t.sessions;
  const { [sessionId]: pending, ...requests } = t.requests;
  void [view, pending];
  return { ...t, sessions, requests };
}

const askIdOf = (request: AgentRequest): string | undefined => {
  const askId = tantraOf(request.params._meta)?.["askId"];
  return typeof askId === "string" ? askId : undefined;
};

export function addRequest(t: Transcript, request: AgentRequest): Transcript {
  const askId = askIdOf(request);
  if (askId === undefined) return t;
  const pending = t.requests[request.sessionId] ?? {};
  if (pending[askId] === request) return t;
  return {
    ...t,
    requests: {
      ...t.requests,
      [request.sessionId]: { ...pending, [askId]: request },
    },
  };
}

export function removeRequest(
  t: Transcript,
  sessionId: string,
  askId: string,
): Transcript {
  const pending = t.requests[sessionId];
  if (!pending || !(askId in pending)) return t;
  const { [askId]: removed, ...rest } = pending;
  const { [sessionId]: previous, ...requests } = t.requests;
  void [removed, previous];
  if (Object.keys(rest).length > 0) requests[sessionId] = rest;
  return { ...t, requests };
}

export function askOf(request: AgentRequest): Ask {
  const askId = askIdOf(request) ?? "";
  if (request.method === "session/request_permission") {
    const { params } = request;
    return {
      kind: "approval",
      askId,
      title: params.title,
      body: params.description ?? null,
      options: params.options.map((option) => option.optionId),
      respond: (allow) =>
        request.respond({
          outcome: { outcome: "selected", optionId: allow ? "allow" : "deny" },
        }),
    };
  }
  const { params } = request;
  const choice = object(
    object(params.requestedSchema["properties"])?.["choice"],
  );
  if (choice) {
    const values = choice["enum"];
    return {
      kind: "choice",
      askId,
      title: params.message,
      body: null,
      options: Array.isArray(values)
        ? values.filter((value) => typeof value === "string")
        : [],
      respond: (selected) =>
        request.respond({ action: "accept", content: { choice: selected } }),
    };
  }
  return {
    kind: "free_text",
    askId,
    title: params.message,
    body: null,
    options: [],
    respond: (text) => request.respond({ action: "accept", content: { text } }),
  };
}

const busy = (state: StateBody | null | undefined): boolean =>
  !!state && state.state !== "idle";

export function descendants(t: Transcript, root: string): string[] {
  const found: string[] = [];
  for (let index = -1; index < found.length; index++) {
    const parent = index < 0 ? root : found[index];
    for (const [id, entry] of Object.entries(t.roster))
      if (entry.parentId === parent && id !== root && !found.includes(id))
        found.push(id);
  }
  return found;
}

export function treeRunning(t: Transcript, root: string): boolean {
  return (
    busy(t.sessions[root]?.state) ||
    descendants(t, root).some((id) => busy(t.roster[id]?.state))
  );
}

export function pendingRequest(
  t: Transcript,
  sessionId: string,
): AgentRequest | undefined {
  return Object.values(t.requests[sessionId] ?? {})[0];
}
