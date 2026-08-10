"use client";

import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";

import { getListSessionsQueryKey, useCreateSession } from "@/generated/api/sessions/sessions";
import type { Attachment } from "@/generated/models";
import { Composer } from "./components/composer";
import { Sidebar } from "./components/sidebar";
import { pendingFirstMessage } from "./state";

export default function ChatPage() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const createSession = useCreateSession();

  const onSend = async (text: string, attachments: Attachment[]) => {
    const created = await createSession.mutateAsync({ data: {} });
    pendingFirstMessage.set(created.id, text, attachments);
    queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() });
    router.push(`/chat/${created.id}`);
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
          <Composer disabled={createSession.isPending} onSend={onSend} />
        </div>
      </main>
    </div>
  );
}
