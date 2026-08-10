"use client";

import { useCallback, useEffect, useState } from "react";
import { env } from "next-runtime-env";
import useWebSocket, { ReadyState } from "react-use-websocket";
import { useStore } from "zustand";

import { getToken } from "@/lib/apiClient";
import { type ChatStore, type ClientFrame, pendingFirstMessage, type ServerFrame } from "./state";

const RECONNECT_ATTEMPTS = 30;
const RECONNECT_INTERVAL = 2000;
const POLICY_VIOLATION = 1008;

const route = (store: ChatStore, data: string) => {
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
  if (frame.type === "busy") {
    state.setBanner({ kind: "busy", message: `Another turn is running — retry in ${Math.ceil(frame.retry_in)}s.` });
    return;
  }
  if (frame.type === "server_error") {
    state.setBanner({ kind: "error", message: frame.message });
  }
};

export const useChatSocket = (sessionId: string, store: ChatStore) => {
  const [authorized, setAuthorized] = useState(false);

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
      onMessage: (message) => route(store, message.data as string),
    },
    authorized,
  );

  const ready = useStore(store, (state) => state.ready);
  const running = useStore(store, (state) => state.turns[state.turns.length - 1]?.status === "running");

  const sendFrame = useCallback(
    (frame: ClientFrame) => {
      if (frame.type === "user_message") store.getState().setBanner(null);
      sendJsonMessage(frame);
    },
    [sendJsonMessage, store],
  );

  useEffect(() => {
    if (!ready) return;
    const text = pendingFirstMessage.take(sessionId);
    if (text) sendFrame({ type: "user_message", text, attachments: [] });
  }, [ready, sessionId, sendFrame]);

  return { sendFrame, connected: readyState === ReadyState.OPEN, ready, running };
};
