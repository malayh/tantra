"use client";

import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";
import { useSession } from "next-auth/react";

import { ChatContext, createChatClient, SESSIONS_KEY } from "./client";
import { createChatStore } from "./state";

function ChatProvider({ children }: { children: React.ReactNode }) {
  const queryClient = useQueryClient();
  const [chat] = useState(() => {
    const store = createChatStore();
    const client = createChatClient(store, () => void queryClient.invalidateQueries({ queryKey: SESSIONS_KEY }));
    return { client, store };
  });

  useEffect(() => {
    void chat.client.connect().catch(() => {});
    return () => {
      void chat.client.disconnect();
    };
  }, [chat]);

  return <ChatContext value={chat}>{children}</ChatContext>;
}

export default function ChatLayout({ children }: { children: React.ReactNode }) {
  const { status } = useSession();

  useEffect(() => {
    if (status === "unauthenticated") window.location.href = "/login";
  }, [status]);

  if (status !== "authenticated") {
    return (
      <div className="flex h-screen items-center justify-center">
        <Loader2 className="text-muted-foreground size-4 animate-spin" />
      </div>
    );
  }

  return <ChatProvider>{children}</ChatProvider>;
}
