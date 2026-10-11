"use client";

import { useEffect, useMemo } from "react";
import { useParams, useSearchParams } from "next/navigation";
import { useStore } from "zustand";

import { useListActors, useListSessions } from "@/generated/api/sessions/sessions";
import { cn } from "@/lib/utils";
import { ActorStrip, ChildDrawer } from "../components/actors";
import { Composer } from "../components/composer";
import { ModelPicker } from "../components/model-picker";
import { Sidebar } from "../components/sidebar";
import { Transcript } from "../components/transcript";
import { useChatSocket } from "../hooks";
import { bannerControls, composerDisabled, createChatStore } from "../state";

export default function SessionPage() {
  const { sessionId } = useParams<{ sessionId: string }>();
  const readonly = useSearchParams().get("view") === "readonly";
  const store = useMemo(() => createChatStore(sessionId), [sessionId]);
  const {
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
    pendingAsk,
    retryCommand,
  } = useChatSocket(sessionId, store, readonly);
  const banner = useStore(store, (state) => state.banner);
  const actors = useStore(store, (state) => state.actors);
  const openChildId = useStore(store, (state) => state.openChildId);
  const childJournal = useStore(store, (state) =>
    state.openChildId === null ? undefined : state.children[state.openChildId],
  );
  const { data: sessions } = useListSessions({ query: { refetchInterval: 2000 } });
  const { data: polledActors } = useListActors(sessionId, { query: { refetchInterval: 2000 } });
  const current = sessions?.find((item) => item.id === sessionId);
  const openActor = actors.find((actor) => actor.agent_id === openChildId);
  const controls = bannerControls(banner, reclaimRequired);

  useEffect(() => {
    if (polledActors) store.getState().setActors(polledActors);
  }, [polledActors, store]);

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex min-w-0 flex-1 flex-col">
        <header className="border-border flex items-center gap-2 border-b px-6 py-3">
          <h1 className="truncate text-sm font-medium">{current?.title ?? "New chat"}</h1>
          <ModelPicker
            sessionId={sessionId}
            model={current?.model}
            disabled={readonly || !ready || !writable || running || pendingAsk !== null}
          />
          {readonly && <span className="text-muted-foreground text-xs">Read-only</span>}
          <span
            title={connected ? "Connected" : "Disconnected"}
            className={cn("ml-auto size-2 rounded-full", connected ? "bg-green-500" : "bg-muted-foreground")}
          />
        </header>

        <Transcript store={store} onAskResponse={readonly || !writable ? undefined : answer} onOpen={openChild} />

        {(banner !== null || controls.reclaim) && (
          <div className="border-border text-destructive mx-auto flex w-full max-w-3xl items-center gap-2 border-t px-6 py-2 text-xs">
            <span>{banner?.message ?? "Write control was lost. Reclaim control to continue."}</span>
            {controls.reclaim && (
              <button type="button" className="underline" disabled={acquiring} onClick={reclaim}>
                Reclaim control
              </button>
            )}
            {controls.retryCommandId && (
              <button
                type="button"
                className="ml-auto underline"
                onClick={() => retryCommand(controls.retryCommandId!)}
              >
                Retry
              </button>
            )}
          </div>
        )}

        <div className="mx-auto w-full max-w-3xl px-6 pb-6">
          <ActorStrip actors={actors} selected={openChildId} onOpen={openChild} />
          <Composer
            key={sessionId}
            disabled={composerDisabled(ready, connected, writable, pendingAsk !== null, readonly)}
            running={running && writable && !readonly}
            onSend={send}
            onStop={cancel}
          />
        </div>
      </main>
      <ChildDrawer actor={openActor} actors={actors} journal={childJournal} onSelect={openChild} onClose={closeChild} />
    </div>
  );
}
