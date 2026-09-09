"use client";

import { useCallback, useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { env } from "next-runtime-env";
import useWebSocket, { ReadyState } from "react-use-websocket";
import { useStore } from "zustand";

import { getListSessionsQueryKey } from "@/generated/api/sessions/sessions";
import { getToken } from "@/lib/apiClient";
import {
  type ChatStore,
  type ClientFrame,
  pendingAsk,
  pendingFirstMessage,
  type ServerFrame,
  subscriptionFrames,
} from "./state";

const RECONNECT_ATTEMPTS = 30;
const RECONNECT_INTERVAL = 2000;
const POLICY_VIOLATION = 1008;
const WRITER_REPLACED = 4009;

export const commandId = () => crypto.randomUUID().replaceAll("-", "");

const route = (store: ChatStore, data: string, onTitle: () => void, subscribe: (agentId: string) => void) => {
  const frame = JSON.parse(data) as ServerFrame;
  const state = store.getState();

  if (frame.type === "event") {
    state.dispatch(frame);
    if (frame.event.type === "child_created") subscribe(frame.event.child_id);
    return;
  }
  if (frame.type === "subscription_ready") {
    state.subscriptionReady(frame);
    return;
  }
  if (frame.type === "ask_expired") {
    state.expireAsk(frame);
    return;
  }
  if (frame.type === "title_updated") {
    onTitle();
    return;
  }
  if (frame.type === "server_error") state.setBanner({ kind: "error", message: frame.message });
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
      shouldReconnect: (event) => event.code !== POLICY_VIOLATION && event.code !== WRITER_REPLACED,
      reconnectAttempts: RECONNECT_ATTEMPTS,
      reconnectInterval: RECONNECT_INTERVAL,
      onOpen: (event) => {
        store.getState().setReady(false);
        const socket = event.currentTarget as WebSocket;
        for (const frame of subscriptionFrames(store.getState(), sessionId)) socket.send(JSON.stringify(frame));
      },
      onClose: (event) => {
        store.getState().setReady(false);
        if (event.code === WRITER_REPLACED) {
          store.getState().setBanner({
            kind: "writer",
            message: "This chat is open for writing in another tab. Reload to reclaim control.",
          });
        }
      },
      onMessage: (message) =>
        route(
          store,
          message.data as string,
          () => queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() }),
          (agentId) =>
            sendJsonMessage({
              type: "subscribe",
              agent_id: agentId,
              after: store.getState().cursors[agentId] ?? 0,
              writable: false,
            }),
        ),
    },
    authorized,
  );

  const ready = useStore(store, (state) => state.ready);
  const running = useStore(store, (state) => Object.values(state.active).some(Boolean));
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
    if (message) {
      sendFrame({
        type: "user_message",
        command_id: commandId(),
        text: message.text,
        attachments: message.attachments,
      });
    }
  }, [ready, sessionId, sendFrame]);

  return { sendFrame, connected: readyState === ReadyState.OPEN, ready, running, pendingAsk: askId };
};
