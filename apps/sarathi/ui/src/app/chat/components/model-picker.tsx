"use client";

import { useMutation } from "@tanstack/react-query";
import { toast } from "sonner";
import { TantraAcpError } from "tantra-acp-client";
import { useStore } from "zustand";

import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { errorText, useChat } from "../client";

export function ModelPicker({ sessionId, disabled = false }: { sessionId: string; disabled?: boolean }) {
  const { client, store } = useChat();
  const option = useStore(store, (state) =>
    state.transcript.sessions[sessionId]?.configOptions.find((item) => item.configId === "model"),
  );
  const setModel = useMutation({
    mutationFn: (model: string) => client.setModel(sessionId, model),
    onSuccess: (configOptions) =>
      store.getState().apply(sessionId, { sessionUpdate: "config_option_update", configOptions }),
    onError: (error) =>
      toast.error(
        error instanceof TantraAcpError && error.code === "model_busy"
          ? errorText(error)
          : "Could not switch the model.",
      ),
  });

  return (
    <Select value={option?.currentValue} disabled={disabled || setModel.isPending} onValueChange={setModel.mutate}>
      <SelectTrigger
        size="sm"
        aria-label="Model"
        className="text-muted-foreground hover:bg-accent hover:text-accent-foreground border-0 text-xs dark:bg-transparent dark:hover:bg-accent"
      >
        <SelectValue placeholder="Model" />
      </SelectTrigger>
      <SelectContent>
        {option?.options.map((item) => (
          <SelectItem key={item.value} value={item.value} className="text-xs">
            {item.name}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
