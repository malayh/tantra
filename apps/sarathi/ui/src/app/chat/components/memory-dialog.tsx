"use client";

import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Brain, Loader2, Trash2 } from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { getListMemoryQueryKey, useDeleteMemory, useListMemory } from "@/generated/api/memory/memory";
import { errorMessage } from "@/lib/errors";

const shortDate = (iso: string): string =>
  new Date(iso).toLocaleDateString(undefined, { month: "short", day: "numeric" });

export function MemoryDialog() {
  const [open, setOpen] = useState(false);
  const queryClient = useQueryClient();
  const { data: memories, isPending, error } = useListMemory({ query: { enabled: open } });

  useEffect(() => {
    if (error) toast.error(errorMessage(error, "Could not load memories."));
  }, [error]);

  const remove = useDeleteMemory({
    mutation: {
      onSuccess: () => queryClient.invalidateQueries({ queryKey: getListMemoryQueryKey() }),
      onError: (failure) => toast.error(errorMessage(failure, "Could not delete the memory.")),
    },
  });

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="ghost" size="icon-sm" aria-label="Memory" title="Memory">
          <Brain />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Memory</DialogTitle>
          <DialogDescription>What Sarathi remembers about you.</DialogDescription>
        </DialogHeader>

        <div className="flex max-h-[60vh] flex-col gap-3 overflow-y-auto">
          {isPending && <Loader2 className="text-muted-foreground mx-auto size-4 animate-spin" />}
          {error && <p className="text-muted-foreground text-sm">Failed to load memories</p>}
          {!isPending && memories?.length === 0 && <p className="text-muted-foreground text-sm">No memories yet</p>}
          {memories?.map((memory) => (
            <div key={memory.id} className="border-border flex items-start gap-2 rounded-lg border p-3">
              <div className="min-w-0 flex-1">
                <p className="text-muted-foreground text-xs uppercase">{memory.kind}</p>
                <p className="text-sm font-medium">{memory.title}</p>
                <p className="text-muted-foreground line-clamp-2 text-sm">{memory.body}</p>
              </div>
              <span className="text-muted-foreground shrink-0 text-xs">{shortDate(memory.created_at)}</span>
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label={`Delete ${memory.title}`}
                disabled={remove.isPending && remove.variables.memoryId === memory.id}
                onClick={() => remove.mutate({ memoryId: memory.id })}
              >
                <Trash2 />
              </Button>
            </div>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
}
