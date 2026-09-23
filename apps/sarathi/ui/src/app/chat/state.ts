import { createStore } from "zustand/vanilla";

import type {
  ActorStatusOut,
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

export type TranscriptItem = TextItem | ToolItem | AskItem;

export type Turn = {
  id: string;
  input: string;
  attachments: Attachment[];
  items: TranscriptItem[];
  status: TurnStatus;
  synthetic: boolean;
  error?: string;
};

export type JournalState = {
  turns: Turn[];
  sampleId: string | null;
  ready: boolean;
};

export type Banner = { kind: "error" | "writer"; message: string };

export type PendingMessage = { text: string; attachments: Attachment[] };

export type ChatState = JournalState & {
  banner: Banner | null;
  cursors: Record<string, number>;
  children: Record<string, JournalState>;
  actors: ActorStatusOut[];
  openChildId: string | null;
  dispatch: (frame: EventFrame) => void;
  subscriptionReady: (frame: SubscriptionReadyFrame) => void;
  expireAsk: (frame: AskExpiredFrame) => void;
  setReady: (ready: boolean) => void;
  setBanner: (banner: Banner | null) => void;
  setActors: (actors: ActorStatusOut[]) => void;
  setOpenChild: (agentId: string | null) => void;
};

export type ChatStore = ReturnType<typeof createChatStore>;

const ATTACHMENT_LINE = /^\[attachment: (.+)\]$/;
const PATH_MARKER = " path=";
const ACTOR_ID = "([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";
const SYNTHETIC_INPUT = new RegExp(`^\\[agent ${ACTOR_ID}(?: finished| turn ended)?\\]`);
const EMPTY_JOURNAL: JournalState = { turns: [], sampleId: null, ready: false };

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

const finalizeItems = (items: TranscriptItem[]): TranscriptItem[] => items.map((item) => ({ ...item, final: true }));

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
  const index = items.findIndex((item) => item.kind === "tool" && item.callId === callId);
  if (index === -1) return items;
  return items.map((item, position) => (position === index ? { ...item, result, isError, final: true } : item));
};

const askText = (request: AskRaised["request"]): { title: string; body: string } => {
  if (request.kind === "free_text") return { title: request.prompt, body: "" };
  if (request.kind === "choice") return { title: request.title, body: (request.options ?? []).join("\n") };
  if (request.kind === "approval") return { title: request.title, body: request.body ?? "" };
  return { title: "", body: "" };
};

const updateAsks = (
  items: TranscriptItem[],
  askId: string | null,
  status: AskItem["status"],
  allow?: boolean,
): TranscriptItem[] =>
  items.map((item) =>
    item.kind === "ask" && item.status === "pending" && (askId === null || item.askId === askId)
      ? { ...item, status, allow }
      : item,
  );

export const pendingAsk = (turns: Turn[]): { askId: string } | null => {
  for (let turnIndex = turns.length - 1; turnIndex >= 0; turnIndex -= 1) {
    const found = turns[turnIndex].items.find((item) => item.kind === "ask" && item.status === "pending");
    if (found?.kind === "ask") return { askId: found.askId };
  }
  return null;
};

type ItemScope = { items: TranscriptItem[]; sampleId: string | null };

const reduceItems = (scope: ItemScope, event: SessionEvent, allowAsk: boolean): ItemScope => {
  const openSample = scope.sampleId ?? "";

  switch (event.type) {
    case "sample_started":
      return {
        sampleId: event.sample_id,
        items: scope.items.filter((item) => item.final || item.kind === "tool"),
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
      if (!allowAsk || scope.items.some((item) => item.kind === "ask" && item.askId === event.ask_id)) return scope;
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
    default:
      return scope;
  }
};

const interruptJournal = (journal: JournalState): JournalState => ({
  ...journal,
  turns: journal.turns.map((turn) =>
    turn.status === "queued" || turn.status === "running"
      ? { ...turn, status: "interrupted", items: finalizeItems(updateAsks(turn.items, null, "expired")) }
      : turn,
  ),
});

const reduceJournal = (
  journal: JournalState,
  event: SessionEvent,
  syntheticInputs: boolean,
  allowAsk: boolean,
): JournalState => {
  switch (event.type) {
    case "input_queued": {
      if (journal.turns.some((turn) => turn.id === event.command_id)) return journal;
      const { text, attachments } = parseInput(event.input);
      return {
        ...journal,
        turns: [
          ...journal.turns,
          {
            id: event.command_id,
            input: text,
            attachments,
            items: [],
            status: "queued",
            synthetic: syntheticInputs && SYNTHETIC_INPUT.test(text),
          },
        ],
        sampleId: journal.turns.some((turn) => turn.status === "running") ? journal.sampleId : null,
      };
    }
    case "turn_started": {
      const parsed = parseInput(event.input);
      const found = journal.turns.some((turn) => turn.id === event.turn_id);
      return {
        ...journal,
        turns: found
          ? mapTurn(journal.turns, event.turn_id, (turn) => ({ ...turn, status: "running" }))
          : [
              ...journal.turns,
              {
                id: event.turn_id,
                input: parsed.text,
                attachments: parsed.attachments,
                items: [],
                status: "running",
                synthetic: syntheticInputs && SYNTHETIC_INPUT.test(parsed.text),
              },
            ],
        sampleId: null,
      };
    }
    case "turn_completed":
      return {
        ...journal,
        turns: mapTurn(journal.turns, event.turn_id, (turn) => ({
          ...turn,
          status: event.stop_reason === "cancelled" ? "cancelled" : "done",
          items: finalizeItems(turn.items),
        })),
        sampleId: null,
      };
    case "turn_failed":
      return {
        ...journal,
        turns: mapTurn(journal.turns, event.turn_id, (turn) => ({
          ...turn,
          status: "failed",
          error: event.error,
          items: finalizeItems(updateAsks(turn.items, null, "expired")),
        })),
        sampleId: null,
      };
    case "turn_cancelled":
      return {
        ...journal,
        turns: mapTurn(journal.turns, event.turn_id, (turn) => ({
          ...turn,
          status: "cancelled",
          error: event.reason,
          items: finalizeItems(updateAsks(turn.items, null, "expired")),
        })),
        sampleId: null,
      };
    case "turn_interrupted":
      return {
        ...journal,
        turns: mapTurn(journal.turns, event.turn_id, (turn) => ({
          ...turn,
          status: "interrupted",
          error: event.reason,
          items: finalizeItems(updateAsks(turn.items, null, "expired")),
        })),
        sampleId: null,
      };
    default: {
      const running = journal.turns.findLastIndex((turn) => turn.status === "running");
      const index = running === -1 ? journal.turns.findLastIndex((turn) => turn.status === "queued") : running;
      if (index === -1) return journal;
      const turn = journal.turns[index];
      const scope = reduceItems({ items: turn.items, sampleId: journal.sampleId }, event, allowAsk);
      if (scope.items === turn.items && scope.sampleId === journal.sampleId) return journal;
      return {
        ...journal,
        turns: journal.turns.map((item, position) => (position === index ? { ...item, items: scope.items } : item)),
        sampleId: scope.sampleId,
      };
    }
  }
};

export const actorStatusLabel = (state: ActorStatusOut["state"]): string =>
  ({
    queued: "Queued",
    running: "Running",
    awaiting_input: "Awaiting input",
    idle: "Idle — awaiting parent",
    finished: "Finished",
    failed: "Failed",
    cancelled: "Cancelled",
    interrupted: "Interrupted",
  })[state];

export const composerDisabled = (
  ready: boolean,
  connected: boolean,
  writerLost: boolean,
  askPending: boolean,
): boolean => !ready || !connected || writerLost || askPending;

export const treeRunning = (turns: Turn[], actors: ActorStatusOut[]): boolean =>
  (pendingAsk(turns) === null && turns.some((turn) => turn.status === "queued" || turn.status === "running")) ||
  actors.some((actor) => actor.parent_id !== null && (actor.state === "queued" || actor.state === "running"));

export const subscriptionFrames = (state: ChatState, rootId: string): SubscribeFrame[] => {
  const ids = state.openChildId === null ? [rootId] : [rootId, state.openChildId];
  return ids.map((agentId) => ({
    type: "subscribe",
    agent_id: agentId,
    after: state.cursors[agentId] ?? 0,
    writable: agentId === rootId,
  }));
};

export const reduceEventFrame = (state: ChatState, frame: EventFrame, rootId: string): Partial<ChatState> => {
  if (frame.seq <= (state.cursors[frame.agent_id] ?? 0)) return {};
  const cursors = { ...state.cursors, [frame.agent_id]: frame.seq };
  if (frame.agent_id === rootId) {
    const reduced = reduceJournal(state, frame.event, true, true);
    return { ...reduced, cursors };
  }
  const current = state.children[frame.agent_id] ?? EMPTY_JOURNAL;
  return {
    cursors,
    children: {
      ...state.children,
      [frame.agent_id]: reduceJournal(current, frame.event, false, false),
    },
  };
};

export const createChatStore = (sessionId: string) =>
  createStore<ChatState>((set) => ({
    ...EMPTY_JOURNAL,
    banner: null,
    cursors: {},
    children: {},
    actors: [],
    openChildId: null,
    dispatch: (frame) => set((state) => reduceEventFrame(state, frame, sessionId)),
    subscriptionReady: (frame) =>
      set((state) => {
        if (frame.agent_id === sessionId) {
          const journal = frame.active ? state : interruptJournal(state);
          return { ...journal, ready: true };
        }
        const journal = state.children[frame.agent_id] ?? EMPTY_JOURNAL;
        return {
          children: {
            ...state.children,
            [frame.agent_id]: { ...journal, ready: true },
          },
        };
      }),
    expireAsk: (frame) =>
      set((state) => ({
        turns: state.turns.map((turn) => ({ ...turn, items: updateAsks(turn.items, frame.ask_id, "expired") })),
        banner: { kind: "error", message: frame.message },
      })),
    setReady: (ready) => set({ ready }),
    setBanner: (banner) => set({ banner }),
    setActors: (actors) => set({ actors }),
    setOpenChild: (openChildId) => set({ openChildId }),
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
