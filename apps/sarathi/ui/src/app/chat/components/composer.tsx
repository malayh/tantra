"use client";

import { useState } from "react";
import { Send, Square } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";

type ComposerProps = {
  disabled: boolean;
  running?: boolean;
  onSend: (text: string) => void;
  onStop?: () => void;
};

export function Composer({ disabled, running = false, onSend, onStop }: ComposerProps) {
  const [text, setText] = useState("");

  const submit = () => {
    const trimmed = text.trim();
    if (disabled || trimmed.length === 0) return;
    onSend(trimmed);
    setText("");
  };

  return (
    <div className="border-border bg-card flex items-end gap-2 rounded-xl border p-2">
      <Textarea
        value={text}
        disabled={disabled}
        placeholder="Send a message…"
        rows={1}
        className="max-h-48 min-h-9 resize-none border-0 bg-transparent focus-visible:ring-0 disabled:bg-transparent dark:bg-transparent dark:disabled:bg-transparent"
        onChange={(event) => setText(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            submit();
          }
        }}
      />
      {running ? (
        <Button variant="outline" size="icon" aria-label="Stop" onClick={onStop}>
          <Square />
        </Button>
      ) : (
        <Button size="icon" aria-label="Send" disabled={disabled || text.trim().length === 0} onClick={submit}>
          <Send />
        </Button>
      )}
    </div>
  );
}
