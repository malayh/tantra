"use client";

import { useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { getListSessionsQueryKey, useCreateSession } from "@/generated/api/sessions/sessions";
import type { Attachment } from "@/generated/models";
import { errorMessage } from "@/lib/errors";
import { Composer } from "./components/composer";
import { Sidebar } from "./components/sidebar";
import { pendingMessages } from "./state";

export default function ChatPage() {
  const router = useRouter();
  const queryClient = useQueryClient();
  const createSession = useCreateSession({
    mutation: {
      onError: (failure) => toast.error(errorMessage(failure, "Could not start a new chat.")),
    },
  });

  const onSend = async (text: string, attachments: Attachment[]) => {
    const created = await createSession.mutateAsync({ data: {} });
    const pending = pendingMessages.create(text, attachments);
    pendingMessages.set(created.id, pending);
    void queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() });
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
