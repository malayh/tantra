import { createStore } from "zustand/vanilla";

import type {
  AskResponseFrame,
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

export type ItemKind = "thinking" | "text";

export type TurnStatus = "running" | "done" | "failed" | "cancelled";

export type TranscriptItem = {
  kind: ItemKind;
  sampleId: string;
  content: string;
  final: boolean;
};

export type Turn = {
  id: string;
  input: string;
  items: TranscriptItem[];
  status: TurnStatus;
  error?: string;
};

export type Banner = { kind: "busy" | "error"; message: string };

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

const ATTACHMENT_LINE = /^\[attachment: .+\]$/;

const stripAttachments = (input: string): string => {
  const lines = input.split("\n");
  while (lines.length > 0 && ATTACHMENT_LINE.test(lines[lines.length - 1].trim())) lines.pop();
  return lines.join("\n");
};

const mapLast = (turns: Turn[], update: (turn: Turn) => Turn): Turn[] =>
  turns.length === 0 ? turns : [...turns.slice(0, -1), update(turns[turns.length - 1])];

const mapTurn = (turns: Turn[], turnId: string, update: (turn: Turn) => Turn): Turn[] =>
  turns.some((turn) => turn.id === turnId)
    ? turns.map((turn) => (turn.id === turnId ? update(turn) : turn))
    : mapLast(turns, update);

const finalizeItems = (turn: Turn): TranscriptItem[] => turn.items.map((item) => ({ ...item, final: true }));

const appendDelta = (turns: Turn[], kind: ItemKind, sampleId: string, text: string): Turn[] =>
  mapLast(turns, (turn) => {
    const index = turn.items.findIndex((item) => item.kind === kind && item.sampleId === sampleId && !item.final);
    if (index === -1) return { ...turn, items: [...turn.items, { kind, sampleId, content: text, final: false }] };

    const items = [...turn.items];
    items[index] = { ...items[index], content: items[index].content + text };
    return { ...turn, items };
  });

const applyPart = (turns: Turn[], kind: ItemKind, sampleId: string, text: string): Turn[] =>
  mapLast(turns, (turn) => {
    const index = turn.items.findIndex((item) => item.kind === kind && item.sampleId === sampleId);
    if (index === -1) return { ...turn, items: [...turn.items, { kind, sampleId, content: text, final: true }] };

    const items = [...turn.items];
    items[index] = { ...items[index], content: text, final: true };
    return { ...turn, items };
  });

const reduce = (state: ChatState, frame: Emitted, sessionId: string): Partial<ChatState> => {
  if (frame.session_id !== sessionId) return {};

  const event = frame.event;
  const openSample = state.sampleId ?? "";

  switch (event.type) {
    case "turn_started": {
      if (state.turns[state.turns.length - 1]?.id === event.turn_id) return {};
      const turn: Turn = { id: event.turn_id, input: stripAttachments(event.input), items: [], status: "running" };
      return { turns: [...state.turns, turn], sampleId: null };
    }
    case "sample_started":
      return {
        sampleId: event.sample_id,
        turns: mapLast(state.turns, (turn) => ({ ...turn, items: turn.items.filter((item) => item.final) })),
      };
    case "reasoning_delta":
      return { turns: appendDelta(state.turns, "thinking", openSample, event.text) };
    case "text_delta":
      return { turns: appendDelta(state.turns, "text", openSample, event.text) };
    case "reasoning_part":
      return { turns: applyPart(state.turns, "thinking", event.sample_id, event.text) };
    case "text_part":
      return { turns: applyPart(state.turns, "text", event.sample_id, event.text) };
    case "sample_completed":
      return {
        turns: mapLast(state.turns, (turn) => ({
          ...turn,
          items: turn.items.map((item) => (item.sampleId === event.sample_id ? { ...item, final: true } : item)),
        })),
      };
    case "turn_completed":
      return {
        turns: mapTurn(state.turns, event.turn_id, (turn) => ({
          ...turn,
          status: event.stop_reason === "cancelled" ? "cancelled" : "done",
          items: finalizeItems(turn),
        })),
        sampleId: null,
      };
    case "turn_failed":
      return {
        turns: mapTurn(state.turns, event.turn_id, (turn) => ({
          ...turn,
          status: "failed",
          error: event.error,
          items: finalizeItems(turn),
        })),
        sampleId: null,
      };
    default:
      return {};
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

let pending: { sessionId: string; text: string } | null = null;

export const pendingFirstMessage = {
  set: (sessionId: string, text: string) => {
    pending = { sessionId, text };
  },
  get: (sessionId: string): string | null => (pending?.sessionId === sessionId ? pending.text : null),
  take: (sessionId: string): string | null => {
    if (pending?.sessionId !== sessionId) return null;
    const { text } = pending;
    pending = null;
    return text;
  },
};
