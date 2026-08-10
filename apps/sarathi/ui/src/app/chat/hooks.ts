"use client";

import { useCallback, useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { env } from "next-runtime-env";
import useWebSocket, { ReadyState } from "react-use-websocket";
import { toast } from "sonner";
import { useStore } from "zustand";

import { getListSessionsQueryKey } from "@/generated/api/sessions/sessions";
import { getToken } from "@/lib/apiClient";
import { type ChatStore, type ClientFrame, pendingAsk, pendingFirstMessage, type ServerFrame } from "./state";

const RECONNECT_ATTEMPTS = 30;
const RECONNECT_INTERVAL = 2000;
const POLICY_VIOLATION = 1008;

const route = (store: ChatStore, data: string, onTitle: () => void) => {
  const frame = JSON.parse(data) as ServerFrame;
  const state = store.getState();

  if ("event" in frame) {
    state.dispatch(frame);
    return;
  }
  if (frame.type === "replay_done") {
    state.setReady(true);
    return;
  }
  if (frame.type === "title_updated") {
    onTitle();
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
  }
};

export const useChatSocket = (sessionId: string, store: ChatStore) => {
  const [authorized, setAuthorized] = useState(false);
  const queryClient = useQueryClient();

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
      shouldReconnect: (event) => event.code !== POLICY_VIOLATION,
      reconnectAttempts: RECONNECT_ATTEMPTS,
      reconnectInterval: RECONNECT_INTERVAL,
      onOpen: () => {
        store.getState().reset();
        store.getState().setReady(false);
      },
      onMessage: (message) =>
        route(store, message.data as string, () =>
          queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() }),
        ),
    },
    authorized,
  );

  const ready = useStore(store, (state) => state.ready);
  const running = useStore(store, (state) => state.turns[state.turns.length - 1]?.status === "running");
  const askId = useStore(store, (state) => pendingAsk(state.turns)?.askId ?? null);

  const sendFrame = useCallback(
    (frame: ClientFrame) => {
      if (frame.type === "user_message") store.getState().setBanner(null);
      sendJsonMessage(frame);
    },
    [sendJsonMessage, store],
  );

  useEffect(() => {
    if (!ready) return;
    const message = pendingFirstMessage.take(sessionId);
    if (message) sendFrame({ type: "user_message", text: message.text, attachments: message.attachments });
  }, [ready, sessionId, sendFrame]);

  return { sendFrame, connected: readyState === ReadyState.OPEN, ready, running, pendingAsk: askId };
};
