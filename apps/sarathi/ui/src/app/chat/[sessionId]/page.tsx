"use client";

import { useMemo } from "react";
import { useParams } from "next/navigation";
import { useStore } from "zustand";

import { useListSessions } from "@/generated/api/sessions/sessions";
import { cn } from "@/lib/utils";
import { Composer } from "../components/composer";
import { ModelPicker } from "../components/model-picker";
import { Sidebar } from "../components/sidebar";
import { Transcript } from "../components/transcript";
import { commandId, useChatSocket } from "../hooks";
import { createChatStore } from "../state";

export default function SessionPage() {
  const { sessionId } = useParams<{ sessionId: string }>();
  const store = useMemo(() => createChatStore(sessionId), [sessionId]);
  const { sendFrame, connected, ready, running, pendingAsk } = useChatSocket(sessionId, store);
  const banner = useStore(store, (state) => state.banner);
  const { data: sessions } = useListSessions();
  const current = sessions?.find((item) => item.id === sessionId);

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex min-w-0 flex-1 flex-col">
        <header className="border-border flex items-center gap-2 border-b px-6 py-3">
          <h1 className="truncate text-sm font-medium">{current?.title ?? "New chat"}</h1>
          <ModelPicker sessionId={sessionId} model={current?.model} disabled={running} />
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
          <Composer
            key={sessionId}
            disabled={!ready || running}
            running={running}
            askPending={pendingAsk !== null}
            onSend={(text, attachments) =>
              sendFrame({ type: "user_message", command_id: commandId(), text, attachments })
            }
            onStop={() => sendFrame({ type: "cancel", command_id: commandId() })}
          />
        </div>
      </main>
    </div>
  );
}
