"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useParams, useRouter, useSearchParams } from "next/navigation";
import { TantraAcpError, treeRunning } from "tantra-acp-client";
import { useStore } from "zustand";

import type { Attachment } from "@/generated/models";
import { cn } from "@/lib/utils";
import { blocks, canonicalId, errorText, openSession, useChat } from "../client";
import { ActorStrip, ChildDrawer } from "../components/actors";
import { Composer } from "../components/composer";
import { ModelPicker } from "../components/model-picker";
import { QueueStrip } from "../components/queue-strip";
import { Sidebar } from "../components/sidebar";
import { Transcript } from "../components/transcript";
import { composerDisabled, fromDroppedSession, isReady, stopOffered } from "../state";

const reclaimText = (paused: boolean, reason: string | null): string => {
  if (paused) return "The connection was interrupted. Unsent messages are held until you reclaim control.";
  if (reason === "replaced") return "Another tab took control of this chat. Reclaim control to continue.";
  return "Write control was lost. Reclaim control to continue.";
};

export default function SessionPage() {
  const params = useParams<{ sessionId: string }>();
  const router = useRouter();
  const readonly = useSearchParams().get("view") === "readonly";
  const chat = useChat();
  const { client, store } = chat;
  const sessionId = canonicalId(params.sessionId);
  const redirect = sessionId !== params.sessionId;
  const connected = useStore(store, (state) => state.client.connection === "open");
  const writer = useStore(store, (state) => state.client.sessions[sessionId]?.writer ?? false);
  const paused = useStore(store, (state) => state.client.sessions[sessionId]?.paused ?? false);
  const ready = useStore(store, (state) => isReady(state, sessionId));
  const title = useStore(store, (state) => state.transcript.sessions[sessionId]?.title ?? null);
  const reason = useStore(store, (state) => state.transcript.sessions[sessionId]?.writerReason ?? null);
  const askPending = useStore(
    store,
    (state) => state.transcript.sessions[sessionId]?.state?.state === "requires_action",
  );
  const running = useStore(store, (state) => treeRunning(state.transcript, sessionId));
  const banner = useStore(store, (state) => state.banner);
  const openChildId = useStore(store, (state) => state.openChildId);
  const [claiming, setClaiming] = useState(false);
  const shown = useRef<object | null>(null);
  const reclaim = !readonly && ready && connected && !writer;

  const reporter = useCallback(
    (quiet: boolean) => {
      const token = shown.current;
      return (error: unknown) => {
        if (token === null || shown.current !== token) return;
        if (quiet && error instanceof TantraAcpError && error.code === "closed") return;
        if (fromDroppedSession(store.getState(), sessionId, error)) return;
        store.getState().setBanner(errorText(error));
      };
    },
    [sessionId, store],
  );

  useEffect(() => {
    if (redirect) router.replace(`/chat/${sessionId}${readonly ? "?view=readonly" : ""}`);
  }, [readonly, redirect, router, sessionId]);

  useEffect(() => {
    if (openChildId === null || !connected) return;
    openSession(chat, openChildId, "read").catch(reporter(true));
    return () => {
      void client.close(openChildId).catch(() => {});
    };
  }, [chat, client, connected, openChildId, reporter]);

  useEffect(() => {
    if (redirect) return;
    shown.current = {};
    store.getState().setBanner(null);
    return () => {
      shown.current = null;
      store.getState().setBanner(null);
      store.getState().setOpenChild(null);
      void client.close(sessionId).catch(() => {});
    };
  }, [client, readonly, redirect, sessionId, store]);

  useEffect(() => {
    if (redirect || !connected) return;
    openSession(chat, sessionId, readonly ? "read" : "write").catch(reporter(true));
  }, [chat, connected, readonly, redirect, reporter, sessionId]);

  const send = (text: string, attachments: Attachment[]) => {
    store.getState().setBanner(null);
    client.prompt(sessionId, blocks(text, attachments)).catch(reporter(false));
  };

  const stop = () => {
    client.cancel(sessionId).catch(reporter(false));
  };

  const claim = () => {
    store.getState().setBanner(null);
    setClaiming(true);
    client
      .claimWriter(sessionId)
      .catch(reporter(false))
      .finally(() => setClaiming(false));
  };

  const openChild = (actorId: string) => store.getState().setOpenChild(actorId);

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex min-w-0 flex-1 flex-col">
        <header className="border-border flex items-center gap-2 border-b px-6 py-3">
          <h1 className="truncate text-sm font-medium">{title ?? "New chat"}</h1>
          <ModelPicker sessionId={sessionId} disabled={readonly || !ready || !writer || running || askPending} />
          {readonly && <span className="text-muted-foreground text-xs">Read-only</span>}
          <span
            title={connected ? "Connected" : "Disconnected"}
            className={cn("ml-auto size-2 rounded-full", connected ? "bg-green-500" : "bg-muted-foreground")}
          />
        </header>

        <Transcript
          sessionId={sessionId}
          readonly={readonly}
          onOpen={openChild}
          emptyText="Send a message to get started"
        />

        {(banner !== null || reclaim) && (
          <div className="border-border text-destructive mx-auto flex w-full max-w-3xl items-center gap-2 border-t px-6 py-2 text-xs">
            <span>{banner ?? reclaimText(paused, reason)}</span>
            {reclaim && (
              <button type="button" className="underline" disabled={claiming} onClick={claim}>
                Reclaim control
              </button>
            )}
          </div>
        )}

        <div className="mx-auto w-full max-w-3xl px-6 pb-6">
          {ready && <ActorStrip rootId={sessionId} selected={openChildId} onOpen={openChild} />}
          {ready && <QueueStrip sessionId={sessionId} />}
          <Composer
            key={sessionId}
            disabled={composerDisabled(ready, connected, writer, askPending, readonly)}
            running={stopOffered(running, writer, askPending, readonly)}
            onSend={send}
            onStop={stop}
          />
        </div>
      </main>
      <ChildDrawer
        rootId={sessionId}
        childId={openChildId}
        onSelect={openChild}
        onClose={() => store.getState().setOpenChild(null)}
      />
    </div>
  );
}
