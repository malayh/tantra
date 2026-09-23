"use client";

import { useEffect, useMemo } from "react";
import { useParams } from "next/navigation";
import { useStore } from "zustand";

import { useListActors, useListSessions } from "@/generated/api/sessions/sessions";
import { cn } from "@/lib/utils";
import { ActorStrip, ChildDrawer } from "../components/actors";
import { Composer } from "../components/composer";
import { ModelPicker } from "../components/model-picker";
import { Sidebar } from "../components/sidebar";
import { Transcript } from "../components/transcript";
import { commandId, useChatSocket } from "../hooks";
import { composerDisabled, createChatStore } from "../state";

export default function SessionPage() {
  const { sessionId } = useParams<{ sessionId: string }>();
  const store = useMemo(() => createChatStore(sessionId), [sessionId]);
  const { sendFrame, openChild, closeChild, connected, ready, running, pendingAsk } = useChatSocket(sessionId, store);
  const banner = useStore(store, (state) => state.banner);
  const actors = useStore(store, (state) => state.actors);
  const openChildId = useStore(store, (state) => state.openChildId);
  const childJournal = useStore(store, (state) =>
    state.openChildId === null ? undefined : state.children[state.openChildId],
  );
  const { data: sessions } = useListSessions();
  const { data: polledActors } = useListActors(sessionId, { query: { refetchInterval: 2000 } });
  const current = sessions?.find((item) => item.id === sessionId);
  const openActor = actors.find((actor) => actor.agent_id === openChildId);

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
            disabled={!ready || running || pendingAsk !== null}
          />
          <span
            title={connected ? "Connected" : "Disconnected"}
            className={cn("ml-auto size-2 rounded-full", connected ? "bg-green-500" : "bg-muted-foreground")}
          />
        </header>

        <Transcript
          store={store}
          onAskResponse={(askId, response) =>
            sendFrame({ type: "ask_response", command_id: commandId(), ask_id: askId, response })
          }
        />

        {banner !== null && (
          <div className="border-border text-destructive mx-auto flex w-full max-w-3xl items-center gap-2 border-t px-6 py-2 text-xs">
            <span>{banner.message}</span>
            {banner.kind === "writer" && (
              <button type="button" className="ml-auto underline" onClick={() => window.location.reload()}>
                Reload
              </button>
            )}
          </div>
        )}

        <div className="mx-auto w-full max-w-3xl px-6 pb-6">
          <ActorStrip actors={actors} selected={openChildId} onOpen={openChild} />
          <Composer
            key={sessionId}
            disabled={composerDisabled(ready, connected, banner?.kind === "writer", pendingAsk !== null)}
            running={running}
            onSend={(text, attachments) =>
              sendFrame({ type: "user_message", command_id: commandId(), text, attachments })
            }
            onStop={() => sendFrame({ type: "cancel", command_id: commandId() })}
          />
        </div>
      </main>
      <ChildDrawer actor={openActor} actors={actors} journal={childJournal} onSelect={openChild} onClose={closeChild} />
    </div>
  );
}
