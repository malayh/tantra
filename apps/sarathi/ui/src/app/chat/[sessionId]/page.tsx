"use client";

import { useMemo } from "react";
import { useParams } from "next/navigation";
import { useStore } from "zustand";

import { cn } from "@/lib/utils";
import { Composer } from "../components/composer";
import { Sidebar } from "../components/sidebar";
import { Transcript } from "../components/transcript";
import { useChatSocket } from "../hooks";
import { createChatStore } from "../state";

export default function SessionPage() {
  const { sessionId } = useParams<{ sessionId: string }>();
  const store = useMemo(() => createChatStore(sessionId), [sessionId]);
  const { sendFrame, connected, ready, running, pendingAsk } = useChatSocket(sessionId, store);
  const banner = useStore(store, (state) => state.banner);

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex min-w-0 flex-1 flex-col">
        <header className="border-border flex items-center gap-2 border-b px-6 py-3">
          <h1 className="truncate text-sm font-medium">Chat</h1>
          <span
            title={connected ? "Connected" : "Disconnected"}
            className={cn("ml-auto size-2 rounded-full", connected ? "bg-green-500" : "bg-muted-foreground")}
          />
        </header>

        <Transcript
          store={store}
          onAskResponse={(askId, response) => sendFrame({ type: "ask_response", ask_id: askId, response })}
        />

        {banner && (
          <div
            className={cn(
              "border-border mx-auto w-full max-w-3xl border-t px-6 py-2 text-xs",
              banner.kind === "error" ? "text-destructive" : "text-muted-foreground",
            )}
          >
            {banner.message}
          </div>
        )}

        <div className="mx-auto w-full max-w-3xl px-6 pb-6">
          <Composer
            key={sessionId}
            disabled={!ready || running}
            running={running}
            askPending={pendingAsk !== null}
            onSend={(text, attachments) => sendFrame({ type: "user_message", text, attachments })}
            onStop={() => sendFrame({ type: "cancel" })}
          />
        </div>
      </main>
    </div>
  );
}
