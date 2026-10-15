"use client";

import { Clock } from "lucide-react";
import { useStore } from "zustand";

import { useChat } from "../client";
import { attachmentsOf, textOf } from "../state";

export function QueueStrip({ sessionId }: { sessionId: string }) {
  const queue = useStore(useChat().store, (state) => state.transcript.sessions[sessionId]?.queue);
  if (!queue || queue.length === 0) return null;

  return (
    <div className="flex flex-col gap-1 pb-2" aria-label="Queued messages">
      {queue.map((entry) => (
        <div key={entry.commandId} className="text-muted-foreground flex min-w-0 items-center gap-1.5 text-xs">
          <Clock className="size-3 shrink-0" />
          <span className="truncate">
            {textOf(entry.content) ||
              attachmentsOf(entry.content)
                .map((attachment) => attachment.name)
                .join(", ")}
          </span>
        </div>
      ))}
    </div>
  );
}
