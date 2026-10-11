"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import axios from "axios";
import { env } from "next-runtime-env";
import { useSession } from "next-auth/react";
import { useStore } from "zustand";
import { SessionClientError, type AskResponse, type ClientState, type CommandPromise } from "tantra-session-client";

import { me } from "@/generated/api/auth/auth";
import { createSessionSocketTicket, getListSessionsQueryKey } from "@/generated/api/sessions/sessions";
import type { HeaderNotice } from "@/generated/models";
import { clearToken } from "@/lib/apiClient";
import { treeRunning, type Banner, type ChatStore, pendingAsk, pendingFirstMessage } from "./state";
import { canonicalId, ChatTransport } from "./transport";

const EMPTY_CLIENT: ClientState = { connection: "idle", subscriptions: [], outbox: [] };

const isHeaderNotice = (notice: unknown): notice is HeaderNotice =>
  notice !== null &&
  !Array.isArray(notice) &&
  typeof notice === "object" &&
  "type" in notice &&
  notice.type === "header_updated";

export const clientErrorBanner = (error: SessionClientError): Banner => {
  if (error.code === "unknown_outcome") {
    return {
      kind: "retry",
      message: "The command may already be durably accepted or may still be in flight. Retry with the same command ID.",
      ...(error.commandId ? { commandId: error.commandId } : {}),
    };
  }
  if (error.code === "writer_required") {
    return { kind: "writer", message: "Write control was lost. Reclaim control to continue." };
  }
  if (error.code === "deleted") return { kind: "error", message: "This chat no longer exists." };
  if (error.code === "forbidden") return { kind: "error", message: "You do not have access to this chat." };
  if (error.code === "authentication") return { kind: "error", message: "The connection was rejected." };
  if (error.uncertain) {
    return {
      kind: "error",
      message: "The command outcome is uncertain. Reconnect before taking further action.",
      ...(error.commandId ? { commandId: error.commandId } : {}),
    };
  }
  if (error.code === "transport" || error.code === "reconnect_exhausted") {
    return { kind: "error", message: "The chat connection is unavailable. Your readable history is preserved." };
  }
  return { kind: "error", message: error.message };
};

export const useChatSocket = (sessionId: string, store: ChatStore, readonly = false) => {
  const queryClient = useQueryClient();
  const { data: authSession, status } = useSession();
  const transportRef = useRef<ChatTransport | null>(null);
  const initialAcquisitionStarted = useRef(false);
  const [clientState, setClientState] = useState<ClientState>(EMPTY_CLIENT);
  const [acquiring, setAcquiring] = useState(false);
  const ready = useStore(store, (state) => state.ready);
  const running = useStore(store, (state) => treeRunning(state.turns, state.actors));
  const ask = useStore(store, (state) => pendingAsk(state.turns));
  const root = clientState.subscriptions.find((item) => item.actorId === canonicalId(sessionId));
  const connected = clientState.connection === "open";
  const writable = root?.ready === true && root.writable;

  const report = useCallback(
    (error: unknown) => {
      const failure =
        error instanceof SessionClientError
          ? error
          : new SessionClientError("application", error instanceof Error ? error.message : "Chat operation failed");
      store.getState().setBanner(clientErrorBanner(failure));
    },
    [store],
  );

  const track = useCallback(
    (command: CommandPromise) => {
      void command.catch(report);
    },
    [report],
  );

  const sendPending = useCallback(() => {
    const pending = pendingFirstMessage.peek(sessionId);
    const transport = transportRef.current;
    if (!pending || !transport) return;
    try {
      const command = transport.send(pending);
      pendingFirstMessage.clear(sessionId);
      track(command);
    } catch (error) {
      report(error);
    }
  }, [report, sessionId, track]);

  useEffect(() => {
    if (status === "loading") return;
    if (status === "unauthenticated") {
      window.location.href = "/login";
      return;
    }

    let active = true;
    let transport: ChatTransport | null = null;
    clearToken();
    initialAcquisitionStarted.current = false;

    void (async () => {
      const user = await me();
      if (!active) return;
      const serverScope = env("NEXT_PUBLIC_API_URL") ?? window.location.origin;
      const identity = String(user.id);
      transport = new ChatTransport({
        serverScope,
        sessionId,
        identity,
        store,
        socketUrl: async () => {
          try {
            const issued = await createSessionSocketTicket(sessionId, { mode: readonly ? "read" : "write" });
            if (issued.user_id !== identity) throw new SessionClientError("identity", "Authenticated identity changed");
            return `${serverScope.replace(/^http/, "ws")}/api/ws/sessions/${sessionId}?ticket=${encodeURIComponent(issued.ticket)}`;
          } catch (error) {
            if (axios.isAxiosError(error)) {
              if (error.response?.status === 401) {
                window.location.href = "/login";
                throw new SessionClientError("ticket_authentication", "Your login expired. Sign in again.");
              }
              if (error.response?.status === 403)
                throw new SessionClientError("forbidden", "You do not have access to this chat.");
              if (error.response?.status === 404)
                throw new SessionClientError("deleted", "This chat no longer exists.");
              throw new SessionClientError("ticket", "Could not issue a socket ticket.");
            }
            throw error;
          }
        },
        onNotice: (notice) => {
          if (isHeaderNotice(notice)) {
            void queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() });
          }
        },
        onState: (next) => {
          if (!active) return;
          setClientState(next);
          const rootState = next.subscriptions.find((item) => item.actorId === canonicalId(sessionId));
          if (readonly || initialAcquisitionStarted.current || rootState?.ready !== true) return;
          initialAcquisitionStarted.current = true;
          setAcquiring(true);
          void transport!
            .acquireWriter()
            .then(() => {
              if (active) sendPending();
            })
            .catch(report)
            .finally(() => {
              if (active) setAcquiring(false);
            });
        },
        onError: (error) => {
          if (active) report(error);
        },
      });
      transportRef.current = transport;
      await transport.connect();
    })().catch((error) => {
      if (active) report(error);
    });

    return () => {
      active = false;
      clearToken();
      if (transportRef.current === transport) transportRef.current = null;
      if (transport) void transport.disconnect();
    };
  }, [authSession?.accessToken, queryClient, readonly, report, sendPending, sessionId, status, store]);

  const send = useCallback(
    (text: string, attachments: Parameters<ChatTransport["send"]>[0]["attachments"]) => {
      if (!transportRef.current || !writable) return;
      store.getState().setBanner(null);
      try {
        track(transportRef.current.send({ text, attachments }));
      } catch (error) {
        report(error);
      }
    },
    [report, store, track, writable],
  );

  const answer = useCallback(
    (askId: string, response: AskResponse) => {
      if (!transportRef.current || !writable) return;
      try {
        const command = transportRef.current.answer(askId, response);
        void command.catch((error) => {
          if (error instanceof SessionClientError && error.code === "ask_expired") {
            store.getState().expireAsk(askId, error.message);
          } else {
            report(error);
          }
        });
      } catch (error) {
        report(error);
      }
    },
    [report, store, writable],
  );

  const cancel = useCallback(() => {
    if (!transportRef.current || !writable) return;
    try {
      track(transportRef.current.cancel());
    } catch (error) {
      report(error);
    }
  }, [report, track, writable]);

  const retryCommand = useCallback(
    (commandId: string) => {
      if (!transportRef.current) return;
      store.getState().setBanner(null);
      try {
        track(transportRef.current.retry(commandId));
      } catch (error) {
        report(error);
      }
    },
    [report, store, track],
  );

  const reclaim = useCallback(() => {
    const transport = transportRef.current;
    if (!transport || readonly || acquiring) return;
    store.getState().setBanner(null);
    setAcquiring(true);
    void transport
      .acquireWriter()
      .then(sendPending)
      .catch(report)
      .finally(() => setAcquiring(false));
  }, [acquiring, readonly, report, sendPending, store]);

  const openChild = useCallback(
    (agentId: string) => {
      void transportRef.current?.openChild(agentId).catch(report);
    },
    [report],
  );

  const closeChild = useCallback(() => {
    void transportRef.current?.closeChild().catch(report);
  }, [report]);

  const reclaimRequired = !readonly && ready && initialAcquisitionStarted.current && !writable && !acquiring;

  return {
    send,
    answer,
    cancel,
    openChild,
    closeChild,
    connected,
    ready,
    writable,
    acquiring,
    reclaimRequired,
    reclaim,
    running,
    pendingAsk: ask,
    retryCommand,
  };
};
