import { createStore } from "zustand/vanilla";

import type {
  AskExpiredFrame,
  AskRaised,
  AskResponseFrame,
  Attachment,
  CancelFrame,
  EventFrame,
  ServerErrorFrame,
  SubscribeFrame,
  SubscriptionReadyFrame,
  TitleUpdatedFrame,
  UnsubscribeFrame,
  UserMessageFrame,
} from "@/generated/models";

export type ServerFrame = EventFrame | SubscriptionReadyFrame | AskExpiredFrame | TitleUpdatedFrame | ServerErrorFrame;

export type ClientFrame = SubscribeFrame | UnsubscribeFrame | UserMessageFrame | AskResponseFrame | CancelFrame;

export type SessionEvent = EventFrame["event"];

export type TextKind = "thinking" | "text";

export type TurnStatus = "queued" | "running" | "done" | "failed" | "cancelled" | "interrupted";

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
  finished: boolean;
};

export type AskItem = ItemBase & {
  kind: "ask";
  callId: string | null;
  askId: string;
  title: string;
  body: string;
  requestKind: string;
  status: "pending" | "answered" | "expired";
  allow?: boolean;
};

export type TranscriptItem = TextItem | ToolItem | SubagentItem | AskItem;

export type Turn = {
  id: string;
  input: string;
  attachments: Attachment[];
  items: TranscriptItem[];
  status: TurnStatus;
  synthetic: boolean;
  error?: string;
};

export type Banner = { kind: "error" | "writer"; message: string };

export type PendingMessage = { text: string; attachments: Attachment[] };

export type ChatState = {
  turns: Turn[];
  ready: boolean;
  banner: Banner | null;
  sampleId: string | null;
  cursors: Record<string, number>;
  active: Record<string, boolean>;
  work: Record<string, string[]>;
  finishedActors: Record<string, boolean>;
  dispatch: (frame: EventFrame) => void;
  subscriptionReady: (frame: SubscriptionReadyFrame) => void;
  expireAsk: (frame: AskExpiredFrame) => void;
  setReady: (ready: boolean) => void;
  setBanner: (banner: Banner | null) => void;
};

export type ChatStore = ReturnType<typeof createChatStore>;

const ATTACHMENT_LINE = /^\[attachment: (.+)\]$/;
const PATH_MARKER = " path=";
const SYNTHETIC_INPUT = /^\[agent ([0-9a-f]{32})(?: finished)?\]/;
const FINISHED_INPUT = /^\[agent ([0-9a-f]{32}) finished\]/;

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

const mapTurn = (turns: Turn[], turnId: string, update: (turn: Turn) => Turn): Turn[] =>
  turns.map((turn) => (turn.id === turnId ? update(turn) : turn));

const childActors = (items: TranscriptItem[], actors: Set<string>) => {
  for (const item of items) {
    if (item.kind !== "subagent") continue;
    actors.add(item.childSessionId);
    childActors(item.items, actors);
  }
};

export const subscriptionFrames = (state: ChatState, rootId: string): SubscribeFrame[] => {
  const actors = new Set([rootId]);
  for (const turn of state.turns) childActors(turn.items, actors);
  return [...actors].map((agentId) => ({
    type: "subscribe",
    agent_id: agentId,
    after: state.cursors[agentId] ?? 0,
    writable: agentId === rootId,
  }));
};

const withWork = (
  state: ChatState,
  agentId: string,
  event: SessionEvent,
): { active: Record<string, boolean>; work: Record<string, string[]> } => {
  const current = state.work[agentId] ?? [];
  let next = current;
  if (state.finishedActors[agentId] && (event.type === "input_queued" || event.type === "turn_started")) {
    return { active: state.active, work: state.work };
  }
  if (event.type === "input_queued") {
    next = current.includes(event.command_id) ? current : [...current, event.command_id];
  } else if (event.type === "turn_started") {
    next = current.includes(event.turn_id) ? current : [...current, event.turn_id];
  } else if (
    event.type === "turn_completed" ||
    event.type === "turn_failed" ||
    event.type === "turn_cancelled" ||
    event.type === "turn_interrupted"
  ) {
    next = current.filter((turnId) => turnId !== event.turn_id);
  } else if (event.type === "agent_finished") {
    next = [];
  } else {
    return { active: state.active, work: state.work };
  }
  return {
    active: { ...state.active, [agentId]: next.length > 0 },
    work: { ...state.work, [agentId]: next },
  };
};

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

const updateAsks = (
  items: TranscriptItem[],
  askId: string | null,
  status: AskItem["status"],
  allow?: boolean,
): TranscriptItem[] => {
  let changed = false;
  const next = items.map((item): TranscriptItem => {
    if (item.kind === "ask" && item.status === "pending" && (askId === null || item.askId === askId)) {
      changed = true;
      return { ...item, status, allow };
    }
    if (item.kind === "subagent") {
      const nested = updateAsks(item.items, askId, status, allow);
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
  for (let index = turns.length - 1; index >= 0; index -= 1) {
    const found = findAsk(turns[index].items, (item) => item.status === "pending");
    if (found !== null) return { askId: found.askId };
  }
  return null;
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
      if (scope.items.some((item) => item.kind === "tool" && item.callId === event.call_id)) return scope;
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
      return { ...scope, items: updateAsks(scope.items, event.ask_id, "answered", allow) };
    }
    case "child_created": {
      const index = scope.items.findIndex((item) => item.kind === "tool" && item.callId === event.call_id);
      const requested = index === -1 ? null : (scope.items[index] as ToolItem);
      const spawned: SubagentItem = {
        kind: "subagent",
        callId: event.call_id,
        childSessionId: event.child_id,
        agent: event.agent,
        args: requested?.args ?? {},
        items: [],
        childSampleId: null,
        isError: false,
        finished: false,
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

const routeChild = (items: TranscriptItem[], childId: string, event: SessionEvent): TranscriptItem[] => {
  let changed = false;
  const next = items.map((item): TranscriptItem => {
    if (item.kind !== "subagent") return item;
    if (item.childSessionId === childId) {
      if (event.type === "agent_finished") {
        changed = true;
        return {
          ...item,
          result: event.result,
          isError: false,
          final: true,
          finished: true,
          items: finalizeItems(item.items),
        };
      }
      if (!item.finished && (event.type === "input_queued" || event.type === "turn_started")) {
        changed = true;
        return {
          ...item,
          result: undefined,
          isError: false,
          final: false,
          childSampleId: null,
        };
      }
      if (event.type === "turn_failed" || event.type === "turn_interrupted" || event.type === "turn_cancelled") {
        if (item.final) return item;
        changed = true;
        return {
          ...item,
          result: event.type === "turn_failed" ? event.error : event.reason,
          isError: event.type !== "turn_cancelled",
          final: true,
          items: finalizeItems(updateAsks(item.items, null, "expired")),
        };
      }
      const scope = reduceItems({ items: item.items, sampleId: item.childSampleId }, event);
      if (scope.items === item.items && scope.sampleId === item.childSampleId) return item;
      changed = true;
      return { ...item, items: scope.items, childSampleId: scope.sampleId };
    }
    const nested = routeChild(item.items, childId, event);
    if (nested === item.items) return item;
    changed = true;
    return { ...item, items: nested };
  });
  return changed ? next : items;
};

const interruptActor = (state: ChatState, agentId: string, rootId: string): Turn[] => {
  if (agentId === rootId) {
    return state.turns.map((turn) =>
      turn.status === "queued" || turn.status === "running"
        ? { ...turn, status: "interrupted", items: finalizeItems(updateAsks(turn.items, null, "expired")) }
        : { ...turn, items: updateAsks(turn.items, null, "expired") },
    );
  }
  return state.turns.map((turn) => ({
    ...turn,
    items: routeChild(turn.items, agentId, { type: "turn_interrupted", turn_id: "", reason: "interrupted" }),
  }));
};

export const reduceEventFrame = (state: ChatState, frame: EventFrame, rootId: string): Partial<ChatState> => {
  if (frame.seq <= (state.cursors[frame.agent_id] ?? 0)) return {};
  const event = frame.event;
  const lifecycle = withWork(state, frame.agent_id, event);
  const finishedAgent = event.type === "input_queued" ? FINISHED_INPUT.exec(event.input)?.[1] : undefined;
  const active = finishedAgent === undefined ? lifecycle.active : { ...lifecycle.active, [finishedAgent]: false };
  const work = finishedAgent === undefined ? lifecycle.work : { ...lifecycle.work, [finishedAgent]: [] };
  const finishedActors =
    event.type === "agent_finished"
      ? { ...state.finishedActors, [frame.agent_id]: true }
      : finishedAgent === undefined
        ? state.finishedActors
        : { ...state.finishedActors, [finishedAgent]: true };
  const base = {
    cursors: { ...state.cursors, [frame.agent_id]: frame.seq },
    active,
    work,
    finishedActors,
  };
  if (frame.agent_id !== rootId) {
    const ignoredRestart =
      state.finishedActors[frame.agent_id] && (event.type === "input_queued" || event.type === "turn_started");
    return {
      ...base,
      turns: ignoredRestart
        ? state.turns
        : state.turns.map((turn) => ({ ...turn, items: routeChild(turn.items, frame.agent_id, event) })),
    };
  }

  switch (event.type) {
    case "input_queued": {
      if (state.turns.some((turn) => turn.id === event.command_id)) return base;
      const { text, attachments } = parseInput(event.input);
      return {
        ...base,
        turns: [
          ...state.turns,
          {
            id: event.command_id,
            input: text,
            attachments,
            items: [],
            status: "queued",
            synthetic: SYNTHETIC_INPUT.test(text),
          },
        ],
        active: { ...active, [rootId]: true },
        sampleId: state.turns.some((turn) => turn.status === "running") ? state.sampleId : null,
      };
    }
    case "turn_started": {
      const found = state.turns.some((turn) => turn.id === event.turn_id);
      const parsed = parseInput(event.input);
      return {
        ...base,
        turns: found
          ? mapTurn(state.turns, event.turn_id, (turn) => ({ ...turn, status: "running" }))
          : [
              ...state.turns,
              {
                id: event.turn_id,
                input: parsed.text,
                attachments: parsed.attachments,
                items: [],
                status: "running",
                synthetic: SYNTHETIC_INPUT.test(parsed.text),
              },
            ],
        active: { ...active, [rootId]: true },
        sampleId: null,
      };
    }
    case "turn_completed": {
      const turns = mapTurn(state.turns, event.turn_id, (turn) => ({
        ...turn,
        status: event.stop_reason === "cancelled" ? "cancelled" : "done",
        items: finalizeItems(turn.items),
      }));
      return {
        ...base,
        turns,
        active: {
          ...active,
          [rootId]: (work[rootId]?.length ?? 0) > 0,
        },
        sampleId: null,
      };
    }
    case "turn_failed": {
      const turns = mapTurn(state.turns, event.turn_id, (turn) => ({
        ...turn,
        status: "failed",
        error: event.error,
        items: finalizeItems(updateAsks(turn.items, null, "expired")),
      }));
      return {
        ...base,
        turns,
        active: {
          ...active,
          [rootId]: (work[rootId]?.length ?? 0) > 0,
        },
        sampleId: null,
      };
    }
    case "turn_cancelled": {
      const turns = mapTurn(state.turns, event.turn_id, (turn) => ({
        ...turn,
        status: "cancelled",
        error: event.reason,
        items: finalizeItems(updateAsks(turn.items, null, "expired")),
      }));
      return {
        ...base,
        turns,
        active: {
          ...active,
          [rootId]: (work[rootId]?.length ?? 0) > 0,
        },
        sampleId: null,
      };
    }
    case "turn_interrupted": {
      const turns = mapTurn(state.turns, event.turn_id, (turn) => ({
        ...turn,
        status: "interrupted",
        error: event.reason,
        items: finalizeItems(updateAsks(turn.items, null, "expired")),
      }));
      return {
        ...base,
        turns,
        active: {
          ...active,
          [rootId]: (work[rootId]?.length ?? 0) > 0,
        },
        sampleId: null,
      };
    }
    default: {
      const running = state.turns.findLastIndex((turn) => turn.status === "running");
      const index = running === -1 ? state.turns.findLastIndex((turn) => turn.status === "queued") : running;
      if (index === -1) return base;
      const turn = state.turns[index];
      const scope = reduceItems({ items: turn.items, sampleId: state.sampleId }, event);
      if (scope.items === turn.items && scope.sampleId === state.sampleId) return base;
      return {
        ...base,
        turns: state.turns.map((item, position) => (position === index ? { ...item, items: scope.items } : item)),
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
    cursors: {},
    active: {},
    work: {},
    finishedActors: {},
    dispatch: (frame) => set((state) => reduceEventFrame(state, frame, sessionId)),
    subscriptionReady: (frame) =>
      set((state) => ({
        ready: frame.agent_id === sessionId ? true : state.ready,
        active: {
          ...state.active,
          [frame.agent_id]: frame.active && !state.finishedActors[frame.agent_id],
        },
        work: frame.active ? state.work : { ...state.work, [frame.agent_id]: [] },
        turns: frame.active ? state.turns : interruptActor(state, frame.agent_id, sessionId),
      })),
    expireAsk: (frame) =>
      set((state) => ({
        turns: state.turns.map((turn) => ({ ...turn, items: updateAsks(turn.items, frame.ask_id, "expired") })),
        banner: { kind: "error", message: frame.message },
      })),
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
