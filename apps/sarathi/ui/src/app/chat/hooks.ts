"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { env } from "next-runtime-env";
import useWebSocket, { ReadyState } from "react-use-websocket";
import { useStore } from "zustand";

import { getListSessionsQueryKey } from "@/generated/api/sessions/sessions";
import { getToken } from "@/lib/apiClient";
import {
  treeRunning,
  type ChatStore,
  type ClientFrame,
  type CommandFrame,
  pendingAsk,
  pendingFirstMessage,
  type ServerFrame,
  subscriptionFrames,
} from "./state";

const RECONNECT_ATTEMPTS = 30;
const RECONNECT_INTERVAL = 2000;
const POLICY_VIOLATION = 1008;
const WRITER_REPLACED = 4009;
const COMMAND_RETRY_ATTEMPTS = 3;
const COMMAND_RETRY_INTERVAL = 2000;

export const commandId = () => crypto.randomUUID().replaceAll("-", "");

const isCommandFrame = (frame: ClientFrame): frame is CommandFrame => "command_id" in frame;

const route = (store: ChatStore, data: string, onTitle: () => void) => {
  const frame = JSON.parse(data) as ServerFrame;
  const state = store.getState();

  if (frame.type === "event") {
    state.dispatch(frame);
    return;
  }
  if (frame.type === "subscription_ready") {
    state.subscriptionReady(frame);
    return;
  }
  if (frame.type === "ask_expired") {
    state.expireAsk(frame);
    if (frame.command_id) state.failCommand(frame.command_id, frame.message);
    return;
  }
  if (frame.type === "title_updated" || frame.type === "header_updated") {
    onTitle();
    return;
  }
  if (frame.type === "server_error") {
    if (frame.command_id && !frame.retryable) state.failCommand(frame.command_id, frame.message);
    else if (!frame.command_id) state.setBanner({ kind: "error", message: frame.message });
  }
};

export const useChatSocket = (sessionId: string, store: ChatStore, readonly = false) => {
  const [authorized, setAuthorized] = useState(false);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
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
    const params = new URLSearchParams({ token: token ?? "" });
    if (readonly) params.set("view", "readonly");
    return `${base}/api/ws/sessions/${sessionId}?${params}`;
  }, [readonly, sessionId]);

  const { sendJsonMessage, readyState } = useWebSocket(
    getSocketUrl,
    {
      shouldReconnect: (event) => event.code !== POLICY_VIOLATION && event.code !== WRITER_REPLACED,
      reconnectAttempts: RECONNECT_ATTEMPTS,
      reconnectInterval: RECONNECT_INTERVAL,
      onOpen: (event) => {
        store.getState().setReady(false);
        const socket = event.currentTarget as WebSocket;
        for (const frame of subscriptionFrames(store.getState(), sessionId, !readonly)) {
          socket.send(JSON.stringify(frame));
        }
      },
      onClose: (event) => {
        store.getState().setReady(false);
        if (event.code === WRITER_REPLACED) store.getState().loseWriter();
      },
      onMessage: (message) =>
        route(store, message.data as string, () =>
          queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() }),
        ),
    },
    authorized,
  );

  const ready = useStore(store, (state) => state.ready);
  const connected = readyState === ReadyState.OPEN;
  const running = useStore(store, (state) => treeRunning(state.turns, state.actors));
  const askId = useStore(store, (state) => pendingAsk(state.turns)?.askId ?? null);
  const writerLost = useStore(store, (state) => state.writerLost);
  const pendingCommand = useStore(store, (state) => state.outbox[0]);

  const sendFrame = useCallback(
    (frame: ClientFrame) => {
      if (readonly) return;
      if (frame.type === "user_message") store.getState().setBanner(null);
      if (isCommandFrame(frame)) store.getState().enqueueCommand(frame);
      else sendJsonMessage(frame);
    },
    [readonly, sendJsonMessage, store],
  );

  useEffect(() => {
    if (retryTimer.current !== null) clearTimeout(retryTimer.current);
    retryTimer.current = null;
    if (!connected || !ready || readonly || writerLost || pendingCommand?.status !== "pending") return;
    retryTimer.current = setTimeout(
      () => {
        const current = store.getState().outbox[0];
        if (current?.frame.command_id !== pendingCommand.frame.command_id || current.status !== "pending") return;
        if (current.attempts >= COMMAND_RETRY_ATTEMPTS) {
          store.getState().requireRetry(current.frame.command_id);
          return;
        }
        sendJsonMessage(current.frame);
        store.getState().markCommandSent(current.frame.command_id);
      },
      pendingCommand.attempts === 0 ? 0 : COMMAND_RETRY_INTERVAL,
    );
    return () => {
      if (retryTimer.current !== null) clearTimeout(retryTimer.current);
      retryTimer.current = null;
    };
  }, [connected, pendingCommand, readonly, ready, sendJsonMessage, store, writerLost]);

  const openChild = useCallback(
    (agentId: string) => {
      const state = store.getState();
      if (state.openChildId === agentId) return;
      if (state.openChildId !== null && readyState === ReadyState.OPEN) {
        sendJsonMessage({ type: "unsubscribe", agent_id: state.openChildId });
      }
      state.setOpenChild(agentId);
      if (readyState === ReadyState.OPEN) {
        sendJsonMessage({
          type: "subscribe",
          agent_id: agentId,
          after: state.cursors[agentId] ?? 0,
          writable: false,
        });
      }
    },
    [readyState, sendJsonMessage, store],
  );

  const closeChild = useCallback(() => {
    const state = store.getState();
    if (state.openChildId !== null && readyState === ReadyState.OPEN) {
      sendJsonMessage({ type: "unsubscribe", agent_id: state.openChildId });
    }
    state.setOpenChild(null);
  }, [readyState, sendJsonMessage, store]);

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

  return {
    sendFrame,
    openChild,
    closeChild,
    connected,
    ready,
    running,
    pendingAsk: askId,
    retryCommand: store.getState().retryCommand,
    writerLost,
  };
};
