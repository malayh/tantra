import { createStore } from "zustand/vanilla";

import type {
  AskRaised,
  AskResponseFrame,
  Attachment,
  BusyFrame,
  CancelFrame,
  Emitted,
  MessageAcceptedFrame,
  ReplayDoneFrame,
  ServerErrorFrame,
  TitleUpdatedFrame,
  UserMessageFrame,
} from "@/generated/models";

export type ServerFrame =
  Emitted | ReplayDoneFrame | BusyFrame | TitleUpdatedFrame | MessageAcceptedFrame | ServerErrorFrame;

export type ClientFrame = UserMessageFrame | AskResponseFrame | CancelFrame;

export type SessionEvent = Emitted["event"];

export type TextKind = "thinking" | "text";

export type TurnStatus = "running" | "waiting" | "done" | "failed" | "cancelled";

export type TaskState = "queued" | "running" | "waiting" | "awaiting_input" | "completed" | "failed" | "killed";

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
  state: TaskState;
  waitCallId: string | null;
  launchComplete: boolean;
  result?: unknown;
  isError: boolean;
};

export type MessageItem = ItemBase & {
  kind: "message";
  messageId: string;
  senderSessionId: string | null;
  source: "user" | "parent" | "child";
  attachments: Attachment[];
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

export type TranscriptItem = TextItem | ToolItem | SubagentItem | MessageItem | AskItem;

export type Turn = {
  id: string;
  input: string;
  attachments: Attachment[];
  items: TranscriptItem[];
  status: TurnStatus;
  waitCallId: string | null;
  error?: string;
};

export type Banner = { kind: "busy" | "error"; message: string };

export type PendingMessage = {
  requestId: string;
  text: string;
  attachments: Attachment[];
  rejected?: boolean;
};

export type ChatState = {
  turns: Turn[];
  ready: boolean;
  banner: Banner | null;
  sampleId: string | null;
  messageIds: string[];
  noticeIds: string[];
  dispatch: (frame: Emitted) => void;
  finishReplay: (frames: Emitted[]) => void;
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

  return items.map((item, position) => {
    if (position !== index) return item;
    if (item.kind === "subagent") return { ...item, result, isError, launchComplete: true };
    return { ...item, result, isError, final: true };
  });
};

const taskStateForEvent = (
  state: TaskState,
  waitCallId: string | null,
  event: SessionEvent,
): { state: TaskState; waitCallId: string | null } => {
  if (event.type === "kill_requested") return { state: "killed", waitCallId: null };
  if (event.type === "turn_failed") return { state: "failed", waitCallId: null };
  if (event.type === "turn_completed") {
    return { state: event.stop_reason === "killed" ? "killed" : "completed", waitCallId: null };
  }
  if (event.type === "sample_started" || event.type === "turn_started" || event.type === "ask_answered") {
    return { state: "running", waitCallId: null };
  }
  if (event.type === "ask_raised") return { state: "awaiting_input", waitCallId: null };
  if (event.type === "tool_call_requested" && event.name === "task_wait") {
    return { state: "waiting", waitCallId: event.call_id };
  }
  if (event.type === "tool_call_completed" && event.call_id === waitCallId) {
    return { state: "running", waitCallId: null };
  }
  return { state, waitCallId };
};

const setTaskState = (items: TranscriptItem[], taskId: string, state: TaskState): TranscriptItem[] => {
  let changed = false;
  const next = items.map((item): TranscriptItem => {
    if (item.kind !== "subagent") return item;
    if (item.childSessionId === taskId) {
      if (item.state === state) return item;
      changed = true;
      return {
        ...item,
        state,
        waitCallId: state === "completed" || state === "failed" || state === "killed" ? null : item.waitCallId,
        final: state === "completed" || state === "failed" || state === "killed",
      };
    }
    const nested = setTaskState(item.items, taskId, state);
    if (nested === item.items) return item;
    changed = true;
    return { ...item, items: nested };
  });
  return changed ? next : items;
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
  const found = last.items.find((item): item is AskItem => item.kind === "ask" && item.status === "pending");
  return found === undefined ? null : { askId: found.askId };
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
      if (
        scope.items.some((item) => (item.kind === "tool" || item.kind === "subagent") && item.callId === event.call_id)
      ) {
        return scope;
      }
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
    case "agent_message_queued": {
      const parsed = event.source === "user" ? parseInput(event.text) : { text: event.text, attachments: [] };
      return {
        ...scope,
        items: [
          ...scope.items,
          {
            kind: "message",
            messageId: event.message_id,
            senderSessionId: event.sender_session_id,
            source: event.source,
            attachments: parsed.attachments,
            sampleId: "",
            content: parsed.text,
            final: true,
          },
        ],
      };
    }
    case "task_notice_queued":
      return { ...scope, items: setTaskState(scope.items, event.task_session_id, event.state) };
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
      if (
        scope.items.some(
          (item) =>
            item.kind === "subagent" &&
            (item.callId === event.call_id || item.childSessionId === event.child_session_id),
        )
      ) {
        return scope;
      }
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
        state: "queued",
        waitCallId: null,
        launchComplete: false,
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
      const task = taskStateForEvent(item.state, item.waitCallId, event);
      const final = task.state === "completed" || task.state === "failed" || task.state === "killed";
      if (
        scope.items === item.items &&
        scope.sampleId === item.childSampleId &&
        task.state === item.state &&
        task.waitCallId === item.waitCallId &&
        final === item.final
      ) {
        return item;
      }
      changed = true;
      return { ...item, items: scope.items, childSampleId: scope.sampleId, ...task, final };
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
  if (event.type === "agent_message_queued" && state.messageIds.includes(event.message_id)) return {};
  if (event.type === "task_notice_queued" && state.noticeIds.includes(event.notice_id)) return {};
  const seen =
    event.type === "agent_message_queued"
      ? { messageIds: [...state.messageIds, event.message_id] }
      : event.type === "task_notice_queued"
        ? { noticeIds: [...state.noticeIds, event.notice_id] }
        : {};

  if (frame.session_id !== sessionId) {
    if (last === undefined) return seen;
    const items = routeChild(last.items, frame.session_id, event);
    return items === last.items ? seen : { ...seen, turns: mapLast(state.turns, (turn) => ({ ...turn, items })) };
  }

  switch (event.type) {
    case "turn_started": {
      if (last?.id === event.turn_id) return seen;
      const { text, attachments } = parseInput(event.input);
      const turn: Turn = {
        id: event.turn_id,
        input: text,
        attachments,
        items: [],
        status: "running",
        waitCallId: null,
      };
      return { ...seen, turns: [...state.turns, turn], sampleId: null };
    }
    case "turn_completed":
      return {
        ...seen,
        turns: mapTurn(state.turns, event.turn_id, (turn) => ({
          ...turn,
          status: event.stop_reason === "cancelled" ? "cancelled" : "done",
          waitCallId: null,
          items: finalizeItems(turn.items),
        })),
        sampleId: null,
      };
    case "turn_failed":
      return {
        ...seen,
        turns: mapTurn(state.turns, event.turn_id, (turn) => ({
          ...turn,
          status: "failed",
          waitCallId: null,
          error: event.error,
          items: finalizeItems(turn.items),
        })),
        sampleId: null,
      };
    default: {
      if (last === undefined) return seen;
      const scope = reduceItems({ items: last.items, sampleId: state.sampleId }, event);
      const waiting =
        event.type === "tool_call_requested" && event.name === "task_wait"
          ? { status: "waiting" as const, waitCallId: event.call_id }
          : event.type === "tool_call_completed" && event.call_id === last.waitCallId
            ? { status: "running" as const, waitCallId: null }
            : event.type === "sample_started"
              ? { status: "running" as const, waitCallId: null }
              : { status: last.status, waitCallId: last.waitCallId };
      if (
        scope.items === last.items &&
        scope.sampleId === state.sampleId &&
        waiting.status === last.status &&
        waiting.waitCallId === last.waitCallId
      ) {
        return seen;
      }
      return {
        ...seen,
        turns: mapLast(state.turns, (turn) => ({ ...turn, items: scope.items, ...waiting })),
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
    messageIds: [],
    noticeIds: [],
    dispatch: (frame) => set((state) => reduce(state, frame, sessionId)),
    finishReplay: (frames) =>
      set((state) => ({
        ...frames.reduce((next, frame) => ({ ...next, ...reduce(next, frame, sessionId) }), state),
        ready: true,
      })),
    reset: () => set({ turns: [], banner: null, sampleId: null, messageIds: [], noticeIds: [] }),
    setReady: (ready) => set({ ready }),
    setBanner: (banner) => set({ banner }),
  }));

const pendingKey = (sessionId: string) => `sarathi:pending-message:${sessionId}`;

const storage = (): Storage | null => (typeof sessionStorage === "undefined" ? null : sessionStorage);

export const pendingMessages = {
  create: (text: string, attachments: Attachment[]): PendingMessage => ({
    requestId: crypto.randomUUID(),
    text,
    attachments,
  }),
  set: (sessionId: string, message: PendingMessage) => {
    storage()?.setItem(pendingKey(sessionId), JSON.stringify(message));
  },
  get: (sessionId: string): PendingMessage | null => {
    const saved = storage()?.getItem(pendingKey(sessionId));
    if (saved === null || saved === undefined) return null;
    try {
      return JSON.parse(saved) as PendingMessage;
    } catch {
      storage()?.removeItem(pendingKey(sessionId));
      return null;
    }
  },
  accept: (sessionId: string, requestId: string): boolean => {
    const pending = pendingMessages.get(sessionId);
    if (pending?.requestId !== requestId) return false;
    storage()?.removeItem(pendingKey(sessionId));
    return true;
  },
  reject: (sessionId: string, requestId: string): PendingMessage | null => {
    const pending = pendingMessages.get(sessionId);
    if (pending?.requestId !== requestId) return null;
    const rejected = { ...pending, rejected: true };
    pendingMessages.set(sessionId, rejected);
    return rejected;
  },
};
