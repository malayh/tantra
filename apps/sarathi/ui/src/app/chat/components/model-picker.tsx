"use client";

import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useListModels } from "@/generated/api/meta/meta";
import { getListSessionsQueryKey, usePatchSession } from "@/generated/api/sessions/sessions";
import { errorMessage } from "@/lib/errors";

export function ModelPicker({
  sessionId,
  model,
  running,
}: {
  sessionId: string;
  model: string | null | undefined;
  running: boolean;
}) {
  const queryClient = useQueryClient();
  const { data: models } = useListModels();

  const patch = usePatchSession({
    mutation: {
      onSuccess: () => queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() }),
      onError: (failure) => toast.error(errorMessage(failure, "Could not switch the model.")),
    },
  });

  return (
    <Select
      value={model ?? undefined}
      disabled={running || patch.isPending}
      onValueChange={(value) => patch.mutate({ sessionId, data: { model: value } })}
    >
      <SelectTrigger
        size="sm"
        aria-label="Model"
        className="text-muted-foreground hover:bg-accent hover:text-accent-foreground border-0 text-xs dark:bg-transparent dark:hover:bg-accent"
      >
        <SelectValue placeholder="Model" />
      </SelectTrigger>
      <SelectContent>
        {models?.map((name) => (
          <SelectItem key={name} value={name} className="text-xs">
            {name}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
