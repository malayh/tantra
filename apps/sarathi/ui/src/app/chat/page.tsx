"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { useStore } from "zustand";

import type { Attachment } from "@/generated/models";
import { blocks, errorText, newChat, SESSIONS_KEY, useChat } from "./client";
import { Composer } from "./components/composer";
import { Sidebar } from "./components/sidebar";

export default function ChatPage() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const chat = useChat();
  const connected = useStore(chat.store, (state) => state.client.connection === "open");
  const [pending, setPending] = useState(false);

  const onSend = async (text: string, attachments: Attachment[]) => {
    setPending(true);
    const sessionId = await newChat(chat).catch((error) => {
      toast.error(errorText(error));
      setPending(false);
      return null;
    });
    if (sessionId === null) return;
    void queryClient.invalidateQueries({ queryKey: SESSIONS_KEY });
    await chat.client.prompt(sessionId, blocks(text, attachments)).catch((error) => toast.error(errorText(error)));
    router.push(`/chat/${sessionId}`);
  };

  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex min-w-0 flex-1 flex-col">
        <div className="flex flex-1 flex-col items-center justify-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">Start a conversation</h1>
          <p className="text-muted-foreground text-sm">Ask Sarathi anything.</p>
        </div>
        <div className="mx-auto w-full max-w-3xl px-6 pb-6">
          <Composer disabled={pending || !connected} onSend={onSend} />
        </div>
      </main>
    </div>
  );
}
