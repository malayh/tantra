import { createStore } from "zustand/vanilla";
import {
  addRequest,
  apply,
  askOf,
  empty,
  removeRequest,
  reset,
  TantraAcpError,
  type AgentRequest,
  type ClientState,
  type ContentBlock,
  type SessionUpdate,
  type StateBody,
  type Transcript,
} from "tantra-acp-client";

import type { Attachment } from "@/generated/models";

export type ChatState = {
  transcript: Transcript;
  client: ClientState;
  banner: string | null;
  ready: Record<string, boolean>;
  openChildId: string | null;
  apply: (sessionId: string, update: SessionUpdate) => void;
  reset: (sessionId: string) => void;
  addRequest: (request: AgentRequest) => void;
  removeRequest: (sessionId: string, askId: string) => void;
  setClient: (client: ClientState) => void;
  setBanner: (banner: string | null) => void;
  setReady: (sessionId: string) => void;
  setOpenChild: (sessionId: string | null) => void;
};

export type ChatStore = ReturnType<typeof createChatStore>;

export const createChatStore = () =>
  createStore<ChatState>((set, get) => {
    const update = (next: (transcript: Transcript) => Transcript) =>
      set((state) => {
        const transcript = next(state.transcript);
        return transcript === state.transcript ? state : { transcript };
      });

    return {
      transcript: empty(),
      client: { connection: "closed", sessions: {} },
      banner: null,
      ready: {},
      openChildId: null,
      apply: (sessionId, value) => update((transcript) => apply(transcript, sessionId, value)),
      reset: (sessionId) =>
        set((state) => ({
          transcript: reset(state.transcript, sessionId),
          ready: { ...state.ready, [sessionId]: false },
        })),
      addRequest: (request) => {
        if (request.signal.aborted) return;
        const { askId } = askOf(request);
        update((transcript) => addRequest(transcript, request));
        request.signal.addEventListener("abort", () => {
          if (get().transcript.requests[request.sessionId]?.[askId] === request)
            get().removeRequest(request.sessionId, askId);
        });
      },
      removeRequest: (sessionId, askId) => update((transcript) => removeRequest(transcript, sessionId, askId)),
      setClient: (client) => set({ client }),
      setBanner: (banner) => set({ banner }),
      setReady: (sessionId) => set((state) => ({ ready: { ...state.ready, [sessionId]: true } })),
      setOpenChild: (openChildId) => set({ openChildId }),
    };
  });

export const textOf = (blocks: ContentBlock[]): string =>
  blocks.map((block) => (block.type === "text" && typeof block.text === "string" ? block.text : "")).join("");

export const attachmentsOf = (blocks: ContentBlock[]): Attachment[] =>
  blocks.flatMap((block) =>
    block.type === "resource_link" && typeof block.name === "string" && typeof block.uri === "string"
      ? [{ name: block.name, path: decodeURIComponent(new URL(block.uri).pathname) }]
      : [],
  );

const STOP_LABELS: Record<string, string> = {
  end_turn: "Replied",
  _tantra_finished: "Finished",
  error: "Failed",
  cancelled: "Cancelled",
  _tantra_interrupted: "Interrupted",
};

export const actorStatusLabel = (state: StateBody | null): string => {
  if (state === null) return "Queued";
  if (state.state === "running") return "Running";
  if (state.state === "requires_action") return "Awaiting input";
  return STOP_LABELS[state.stopReason ?? ""] ?? "Idle — awaiting parent";
};

export const isReady = (state: ChatState, sessionId: string): boolean =>
  state.ready[sessionId] === true && sessionId in state.client.sessions;

export const isLoading = (state: ChatState, sessionId: string): boolean =>
  !isReady(state, sessionId) && state.banner === null;

export const fromDroppedSession = (state: ChatState, sessionId: string, error: unknown): boolean =>
  error instanceof TantraAcpError &&
  (error.code === "uncertain" || error.code === "closed") &&
  !(sessionId in state.client.sessions);

export const isNearBottom = (scrollHeight: number, scrollTop: number, clientHeight: number): boolean =>
  scrollHeight - scrollTop - clientHeight <= 80;

export const composerDisabled = (
  ready: boolean,
  connected: boolean,
  writable: boolean,
  askPending: boolean,
  readonly = false,
): boolean => !ready || !connected || !writable || askPending || readonly;

export const stopOffered = (running: boolean, writable: boolean, askPending: boolean, readonly = false): boolean =>
  running && writable && !askPending && !readonly;
