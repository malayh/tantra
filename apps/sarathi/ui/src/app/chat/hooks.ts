"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { env } from "next-runtime-env";
import useWebSocket, { ReadyState } from "react-use-websocket";
import { toast } from "sonner";
import { useStore } from "zustand";

import { getListSessionsQueryKey } from "@/generated/api/sessions/sessions";
import type { Emitted } from "@/generated/models";
import { getToken } from "@/lib/apiClient";
import {
  type ChatStore,
  type ClientFrame,
  pendingAsk,
  type PendingMessage,
  pendingMessages,
  type ServerFrame,
} from "./state";

const RECONNECT_ATTEMPTS = 30;
const RECONNECT_INTERVAL = 2000;
const POLICY_VIOLATION = 1008;
const INTERNAL_ERROR = 1011;

const route = (
  store: ChatStore,
  data: string,
  onTitle: () => void,
  onAccepted: (requestId: string) => void,
  onRejected: (requestId: string, message: string) => void,
  replay: Emitted[],
) => {
  const frame = JSON.parse(data) as ServerFrame;
  const state = store.getState();

  if ("event" in frame) {
    if (state.ready) state.dispatch(frame);
    else replay.push(frame);
    return;
  }
  if (frame.type === "replay_done") {
    state.finishReplay(replay.splice(0));
    return;
  }
  if (frame.type === "title_updated") {
    onTitle();
    return;
  }
  if (frame.type === "message_accepted") {
    onAccepted(frame.request_id);
    return;
  }
  if (frame.type === "busy") {
    const message = `Another turn is running — retry in ${Math.ceil(frame.retry_in)}s.`;
    state.setBanner({ kind: "busy", message });
    toast.info(message);
    return;
  }
  if (frame.type === "server_error") {
    state.setBanner({ kind: "error", message: frame.message });
    if (frame.request_id) onRejected(frame.request_id, frame.message);
  }
};

export const useChatSocket = (sessionId: string, store: ChatStore) => {
  const [authorized, setAuthorized] = useState(false);
  const queryClient = useQueryClient();
  const [pending, setPending] = useState<PendingMessage | null>(null);
  const resolver = useRef<{
    requestId: string;
    resolve: () => void;
    reject: (error: Error) => void;
  } | null>(null);
  const replay = useRef<Emitted[]>([]);

  useEffect(() => {
    setPending(pendingMessages.get(sessionId));
  }, [sessionId]);

  const accept = useCallback(
    (requestId: string) => {
      if (!pendingMessages.accept(sessionId, requestId)) return;
      setPending(null);
      if (resolver.current?.requestId === requestId) {
        resolver.current.resolve();
        resolver.current = null;
      }
    },
    [sessionId],
  );

  const reject = useCallback(
    (requestId: string, message: string) => {
      const rejected = pendingMessages.reject(sessionId, requestId);
      if (rejected === null) return;
      setPending(rejected);
      if (resolver.current?.requestId === requestId) {
        resolver.current.reject(new Error(message));
        resolver.current = null;
      }
    },
    [sessionId],
  );

  useEffect(() => {
    let active = true;
    void getToken().then((token) => {
      if (!active) return;
      if (token) setAuthorized(true);
      else window.location.href = "/login";
    });
    return () => {
      active = false;
    };
  }, []);

  const getSocketUrl = useCallback(async () => {
    const base = (env("NEXT_PUBLIC_API_URL") ?? window.location.origin).replace(/^http/, "ws");
    const token = await getToken();
    return `${base}/api/ws/sessions/${sessionId}?token=${encodeURIComponent(token ?? "")}`;
  }, [sessionId]);

  const { sendJsonMessage, readyState } = useWebSocket(
    getSocketUrl,
    {
      shouldReconnect: (event) => event.code !== POLICY_VIOLATION && event.code !== INTERNAL_ERROR,
      reconnectAttempts: RECONNECT_ATTEMPTS,
      reconnectInterval: RECONNECT_INTERVAL,
      onOpen: () => {
        replay.current = [];
        store.getState().reset();
        store.getState().setReady(false);
      },
      onMessage: (message) => {
        route(
          store,
          message.data as string,
          () => queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() }),
          accept,
          reject,
          replay.current,
        );
      },
    },
    authorized,
  );

  const ready = useStore(store, (state) => state.ready);
  const running = useStore(store, (state) => {
    const status = state.turns[state.turns.length - 1]?.status;
    return status === "running" || status === "waiting";
  });
  const askId = useStore(store, (state) => pendingAsk(state.turns)?.askId ?? null);

  const sendFrame = useCallback(
    (frame: ClientFrame) => {
      if (frame.type === "user_message") store.getState().setBanner(null);
      sendJsonMessage(frame, false);
    },
    [sendJsonMessage, store],
  );

  const sendMessage = useCallback(
    (text: string, attachments: PendingMessage["attachments"]): Promise<void> => {
      const message = pendingMessages.create(text, attachments);
      pendingMessages.set(sessionId, message);
      setPending(message);
      store.getState().setBanner(null);
      return new Promise((resolve, rejectDelivery) => {
        resolver.current = { requestId: message.requestId, resolve, reject: rejectDelivery };
        sendJsonMessage({ type: "user_message", text, attachments, request_id: message.requestId }, false);
      });
    },
    [sendJsonMessage, sessionId, store],
  );

  useEffect(() => {
    if (!ready) return;
    const message = pendingMessages.get(sessionId);
    setPending(message);
    if (message && !message.rejected) {
      sendJsonMessage(
        { type: "user_message", text: message.text, attachments: message.attachments, request_id: message.requestId },
        false,
      );
    }
  }, [ready, sendJsonMessage, sessionId]);

  return {
    sendFrame,
    sendMessage,
    connected: readyState === ReadyState.OPEN,
    ready,
    running,
    pendingAsk: askId,
    pendingMessage: pending,
    awaitingAcceptance: pending !== null && !pending.rejected,
  };
};
