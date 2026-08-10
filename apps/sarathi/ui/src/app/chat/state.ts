import { createStore } from "zustand/vanilla";

import type {
  AskRaised,
  AskResponseFrame,
  Attachment,
  BusyFrame,
  CancelFrame,
  Emitted,
  ReplayDoneFrame,
  ServerErrorFrame,
  TitleUpdatedFrame,
  UserMessageFrame,
} from "@/generated/models";

export type ServerFrame = Emitted | ReplayDoneFrame | BusyFrame | TitleUpdatedFrame | ServerErrorFrame;

export type ClientFrame = UserMessageFrame | AskResponseFrame | CancelFrame;

export type SessionEvent = Emitted["event"];

export type TextKind = "thinking" | "text";

export type TurnStatus = "running" | "done" | "failed" | "cancelled";

type ItemBase = {
  sampleId: string;
  content: string;
  final: boolean;
};

export type TextItem = ItemBase & { kind: TextKind };

export type ToolItem = ItemBase & {
  kind: "tool";
  callId: string;
  name: string;
  args: Record<string, unknown>;
  result?: unknown;
  isError: boolean;
  progress: string[];
};

export type SubagentItem = ItemBase & {
  kind: "subagent";
  callId: string;
  childSessionId: string;
  agent: string;
  args: Record<string, unknown>;
  items: TranscriptItem[];
  childSampleId: string | null;
  result?: unknown;
  isError: boolean;
};

export type AskItem = ItemBase & {
  kind: "ask";
  callId: string | null;
  askId: string;
  title: string;
  body: string;
  requestKind: string;
  status: "pending" | "answered";
  allow?: boolean;
};

export type TranscriptItem = TextItem | ToolItem | SubagentItem | AskItem;

export type Turn = {
  id: string;
  input: string;
  attachments: Attachment[];
  items: TranscriptItem[];
  status: TurnStatus;
  error?: string;
};

export type Banner = { kind: "busy" | "error"; message: string };

export type PendingMessage = { text: string; attachments: Attachment[] };

export type ChatState = {
  turns: Turn[];
  ready: boolean;
  banner: Banner | null;
  sampleId: string | null;
  dispatch: (frame: Emitted) => void;
  reset: () => void;
  setReady: (ready: boolean) => void;
  setBanner: (banner: Banner | null) => void;
};

export type ChatStore = ReturnType<typeof createChatStore>;

const ATTACHMENT_LINE = /^\[attachment: (.+)\]$/;
const PATH_MARKER = " path=";

const parseInput = (input: string): { text: string; attachments: Attachment[] } => {
  const lines = input.split("\n");
  const attachments: Attachment[] = [];

  while (lines.length > 0) {
    const match = ATTACHMENT_LINE.exec(lines[lines.length - 1].trim());
    if (match === null) break;
    const cut = match[1].lastIndexOf(PATH_MARKER);
    if (cut === -1) break;
    attachments.unshift({ name: match[1].slice(0, cut), path: match[1].slice(cut + PATH_MARKER.length) });
    lines.pop();
  }

  return { text: lines.join("\n"), attachments };
};

const mapLast = (turns: Turn[], update: (turn: Turn) => Turn): Turn[] =>
  turns.length === 0 ? turns : [...turns.slice(0, -1), update(turns[turns.length - 1])];

const mapTurn = (turns: Turn[], turnId: string, update: (turn: Turn) => Turn): Turn[] =>
  turns.some((turn) => turn.id === turnId)
    ? turns.map((turn) => (turn.id === turnId ? update(turn) : turn))
    : mapLast(turns, update);

const finalizeItems = (items: TranscriptItem[]): TranscriptItem[] =>
  items.map((item) =>
    item.kind === "subagent" ? { ...item, final: true, items: finalizeItems(item.items) } : { ...item, final: true },
  );

const appendDelta = (items: TranscriptItem[], kind: TextKind, sampleId: string, text: string): TranscriptItem[] => {
  const index = items.findIndex((item) => item.kind === kind && item.sampleId === sampleId && !item.final);
  if (index === -1) return [...items, { kind, sampleId, content: text, final: false }];

  const target = items[index] as TextItem;
  return items.map((item, position) => (position === index ? { ...target, content: target.content + text } : item));
};

const applyPart = (items: TranscriptItem[], kind: TextKind, sampleId: string, text: string): TranscriptItem[] => {
  const index = items.findIndex((item) => item.kind === kind && item.sampleId === sampleId);
  if (index === -1) return [...items, { kind, sampleId, content: text, final: true }];

  const target = items[index] as TextItem;
  return items.map((item, position) => (position === index ? { ...target, content: text, final: true } : item));
};

const addProgress = (items: TranscriptItem[], callId: string, message: string): TranscriptItem[] => {
  const index = items.findIndex(
    (item) => item.kind === "tool" && item.callId === callId && !item.progress.includes(message),
  );
  if (index === -1) return items;

  const target = items[index] as ToolItem;
  return items.map((item, position) =>
    position === index ? { ...target, progress: [...target.progress, message] } : item,
  );
};

const completeCall = (items: TranscriptItem[], callId: string, result: unknown, isError: boolean): TranscriptItem[] => {
  const index = items.findIndex((item) => (item.kind === "tool" || item.kind === "subagent") && item.callId === callId);
  if (index === -1) return items;

  return items.map((item, position) => (position === index ? { ...item, result, isError, final: true } : item));
};

const askText = (request: AskRaised["request"]): { title: string; body: string } => {
  if (request.kind === "free_text") return { title: request.prompt, body: "" };
  if (request.kind === "choice") return { title: request.title, body: (request.options ?? []).join("\n") };
  if (request.kind === "approval") return { title: request.title, body: request.body ?? "" };
  return { title: "", body: "" };
};

const findAsk = (items: TranscriptItem[], match: (item: AskItem) => boolean): AskItem | null => {
  for (const item of items) {
    if (item.kind === "ask" && match(item)) return item;
    if (item.kind === "subagent") {
      const nested = findAsk(item.items, match);
      if (nested !== null) return nested;
    }
  }
  return null;
};

const answerAsk = (items: TranscriptItem[], askId: string, allow: boolean | undefined): TranscriptItem[] => {
  let changed = false;

  const next = items.map((item): TranscriptItem => {
    if (item.kind === "ask" && item.askId === askId && item.status === "pending") {
      changed = true;
      return { ...item, status: "answered", allow };
    }
    if (item.kind === "subagent") {
      const nested = answerAsk(item.items, askId, allow);
      if (nested !== item.items) {
        changed = true;
        return { ...item, items: nested };
      }
    }
    return item;
  });

  return changed ? next : items;
};

export const pendingAsk = (turns: Turn[]): { askId: string } | null => {
  const last = turns[turns.length - 1];
  if (last === undefined) return null;

  const found = findAsk(last.items, (item) => item.status === "pending");
  return found === null ? null : { askId: found.askId };
};

type ItemScope = { items: TranscriptItem[]; sampleId: string | null };

const reduceItems = (scope: ItemScope, event: SessionEvent): ItemScope => {
  const openSample = scope.sampleId ?? "";

  switch (event.type) {
    case "sample_started":
      return {
        sampleId: event.sample_id,
        items: scope.items.filter((item) => item.final || item.kind === "tool" || item.kind === "subagent"),
      };
    case "reasoning_delta":
      return { ...scope, items: appendDelta(scope.items, "thinking", openSample, event.text) };
    case "text_delta":
      return { ...scope, items: appendDelta(scope.items, "text", openSample, event.text) };
    case "reasoning_part":
      return { ...scope, items: applyPart(scope.items, "thinking", event.sample_id, event.text) };
    case "text_part":
      return { ...scope, items: applyPart(scope.items, "text", event.sample_id, event.text) };
    case "sample_completed":
      return {
        ...scope,
        items: scope.items.map((item) =>
          (item.kind === "thinking" || item.kind === "text") && item.sampleId === event.sample_id
            ? { ...item, final: true }
            : item,
        ),
      };
    case "tool_call_requested":
      return {
        ...scope,
        items: [
          ...scope.items,
          {
            kind: "tool",
            callId: event.call_id,
            name: event.name,
            args: event.args ?? {},
            isError: false,
            progress: [],
            sampleId: event.sample_id,
            content: "",
            final: false,
          },
        ],
      };
    case "tool_progress":
      return { ...scope, items: addProgress(scope.items, event.call_id, event.message) };
    case "tool_call_completed":
      return { ...scope, items: completeCall(scope.items, event.call_id, event.result, event.is_error ?? false) };
    case "ask_raised": {
      if (findAsk(scope.items, (item) => item.askId === event.ask_id) !== null) return scope;
      const { title, body } = askText(event.request);
      return {
        ...scope,
        items: [
          ...scope.items,
          {
            kind: "ask",
            askId: event.ask_id,
            callId: event.call_id ?? null,
            title,
            body,
            requestKind: event.request.kind ?? "approval",
            status: "pending",
            sampleId: openSample,
            content: "",
            final: true,
          },
        ],
      };
    }
    case "ask_answered": {
      const allow = event.response.kind === "approval" ? event.response.allow : undefined;
      return { ...scope, items: answerAsk(scope.items, event.ask_id, allow) };
    }
    case "child_session_spawned": {
      const index = scope.items.findIndex((item) => item.kind === "tool" && item.callId === event.call_id);
      const requested = index === -1 ? null : (scope.items[index] as ToolItem);
      const spawned: SubagentItem = {
        kind: "subagent",
        callId: event.call_id,
        childSessionId: event.child_session_id,
        agent: event.agent,
        args: requested?.args ?? {},
        items: [],
        childSampleId: null,
        isError: false,
        sampleId: requested?.sampleId ?? openSample,
        content: "",
        final: false,
      };
      return {
        ...scope,
        items:
          index === -1
            ? [...scope.items, spawned]
            : scope.items.map((item, position) => (position === index ? spawned : item)),
      };
    }
    default:
      return scope;
  }
};

const routeChild = (items: TranscriptItem[], childSessionId: string, event: SessionEvent): TranscriptItem[] => {
  let changed = false;

  const next = items.map((item) => {
    if (item.kind !== "subagent") return item;

    if (item.childSessionId === childSessionId) {
      const scope = reduceItems({ items: item.items, sampleId: item.childSampleId }, event);
      if (scope.items === item.items && scope.sampleId === item.childSampleId) return item;
      changed = true;
      return { ...item, items: scope.items, childSampleId: scope.sampleId };
    }

    const nested = routeChild(item.items, childSessionId, event);
    if (nested === item.items) return item;
    changed = true;
    return { ...item, items: nested };
  });

  return changed ? next : items;
};

const reduce = (state: ChatState, frame: Emitted, sessionId: string): Partial<ChatState> => {
  const event = frame.event;
  const last = state.turns[state.turns.length - 1];

  if (frame.session_id !== sessionId) {
    if (last === undefined) return {};
    const items = routeChild(last.items, frame.session_id, event);
    return items === last.items ? {} : { turns: mapLast(state.turns, (turn) => ({ ...turn, items })) };
  }

  switch (event.type) {
    case "turn_started": {
      if (last?.id === event.turn_id) return {};
      const { text, attachments } = parseInput(event.input);
      const turn: Turn = { id: event.turn_id, input: text, attachments, items: [], status: "running" };
      return { turns: [...state.turns, turn], sampleId: null };
    }
    case "turn_completed":
      return {
        turns: mapTurn(state.turns, event.turn_id, (turn) => ({
          ...turn,
          status: event.stop_reason === "cancelled" ? "cancelled" : "done",
          items: finalizeItems(turn.items),
        })),
        sampleId: null,
      };
    case "turn_failed":
      return {
        turns: mapTurn(state.turns, event.turn_id, (turn) => ({
          ...turn,
          status: "failed",
          error: event.error,
          items: finalizeItems(turn.items),
        })),
        sampleId: null,
      };
    default: {
      if (last === undefined) return {};
      const scope = reduceItems({ items: last.items, sampleId: state.sampleId }, event);
      if (scope.items === last.items && scope.sampleId === state.sampleId) return {};
      return {
        turns: mapLast(state.turns, (turn) => ({ ...turn, items: scope.items })),
        sampleId: scope.sampleId,
      };
    }
  }
};

export const createChatStore = (sessionId: string) =>
  createStore<ChatState>((set) => ({
    turns: [],
    ready: false,
    banner: null,
    sampleId: null,
    dispatch: (frame) => set((state) => reduce(state, frame, sessionId)),
    reset: () => set({ turns: [], banner: null, sampleId: null }),
    setReady: (ready) => set({ ready }),
    setBanner: (banner) => set({ banner }),
  }));

let pending: ({ sessionId: string } & PendingMessage) | null = null;

export const pendingFirstMessage = {
  set: (sessionId: string, text: string, attachments: Attachment[]) => {
    pending = { sessionId, text, attachments };
  },
  take: (sessionId: string): PendingMessage | null => {
    if (pending?.sessionId !== sessionId) return null;
    const { text, attachments } = pending;
    pending = null;
    return { text, attachments };
  },
};
