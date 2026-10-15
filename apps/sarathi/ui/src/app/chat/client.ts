import { createContext, useContext } from "react";
import { isAxiosError } from "axios";
import { env } from "next-runtime-env";
import { TantraAcpClient, TantraAcpError, type ContentBlock, type Mode } from "tantra-acp-client";

import { issueAcpTicket } from "@/generated/api/acp/acp";
import type { Attachment } from "@/generated/models";
import type { ChatStore } from "./state";

export type Chat = { client: TantraAcpClient; store: ChatStore };

export const SESSIONS_KEY = ["acp-sessions"];

const HEX_ID = /^[0-9a-f]{32}$/;

export const canonicalId = (id: string): string =>
  HEX_ID.test(id) ? `${id.slice(0, 8)}-${id.slice(8, 12)}-${id.slice(12, 16)}-${id.slice(16, 20)}-${id.slice(20)}` : id;

export const blocks = (text: string, attachments: Attachment[]): ContentBlock[] => [
  { type: "text", text },
  ...attachments.map((attachment) => ({
    type: "resource_link",
    name: attachment.name,
    uri: `file://${attachment.path.split("/").map(encodeURIComponent).join("/")}`,
  })),
];

const MESSAGES: Partial<Record<TantraAcpError["code"], string>> = {
  not_found: "This chat no longer exists.",
  writer_required: "Write control was lost. Reclaim control to continue.",
  uncertain: "The message may not have been sent. Check the transcript before sending it again.",
  closed: "The chat connection closed before the request finished.",
  unavailable: "The chat service is unavailable. Try again shortly.",
  model_busy: "The model cannot change while the chat is busy.",
  too_large: "The message is too large.",
  invalid_params: "The request was rejected.",
  dial_failed: "The chat connection is unavailable. Your readable history is preserved.",
  profile_unsupported: "The chat server is not compatible with this app.",
};

export const errorText = (error: unknown): string =>
  (error instanceof TantraAcpError ? MESSAGES[error.code] : undefined) ?? "The chat request failed.";

export const createChatClient = (store: ChatStore, onSessionInfo: () => void) =>
  new TantraAcpClient({
    url: async () => {
      try {
        const { ticket } = await issueAcpTicket({ mode: "write" });
        const base = env("NEXT_PUBLIC_API_URL") ?? window.location.origin;
        return `${base.replace(/^http/, "ws")}/api/acp?ticket=${encodeURIComponent(ticket)}`;
      } catch (error) {
        if (isAxiosError(error) && error.response?.status === 401) window.location.href = "/login";
        throw error;
      }
    },
    onUpdate: (sessionId, update) => {
      store.getState().apply(sessionId, update);
      if (update.sessionUpdate === "session_info_update") onSessionInfo();
    },
    onRequest: (request) => store.getState().addRequest(request),
    onState: (state) => store.getState().setClient(state),
    onError: (error) => store.getState().setBanner(errorText(error)),
  });

export const openSession = async ({ client, store }: Chat, sessionId: string, mode: Mode): Promise<void> => {
  if (sessionId in store.getState().client.sessions) {
    store.getState().setReady(sessionId);
    return;
  }
  store.getState().reset(sessionId);
  const { configOptions } = await client.resume(sessionId, { mode });
  if (configOptions) store.getState().apply(sessionId, { sessionUpdate: "config_option_update", configOptions });
  store.getState().setReady(sessionId);
};

export const newChat = async ({ client, store }: Chat): Promise<string> => {
  const { sessionId, configOptions } = await client.newSession();
  if (configOptions) store.getState().apply(sessionId, { sessionUpdate: "config_option_update", configOptions });
  return sessionId;
};

export const ChatContext = createContext<Chat | null>(null);

export const useChat = (): Chat => {
  const chat = useContext(ChatContext);
  if (chat === null) throw new Error("useChat must be used inside the chat layout");
  return chat;
};
