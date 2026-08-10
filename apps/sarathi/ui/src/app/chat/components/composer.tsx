"use client";

import { useRef, useState } from "react";
import { FileText, Loader2, Paperclip, Send, Square, X } from "lucide-react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { useCreateUpload } from "@/generated/api/uploads/uploads";
import type { Attachment } from "@/generated/models";
import { errorMessage } from "@/lib/errors";

type ComposerProps = {
  disabled: boolean;
  running?: boolean;
  askPending?: boolean;
  onSend: (text: string, attachments: Attachment[]) => void;
  onStop?: () => void;
};

export function Composer({ disabled, running = false, askPending = false, onSend, onStop }: ComposerProps) {
  const [text, setText] = useState("");
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const fileRef = useRef<HTMLInputElement>(null);
  const upload = useCreateUpload();

  const blocked = disabled || upload.isPending;
  const nothingToSend = text.trim().length === 0 && attachments.length === 0;

  const submit = () => {
    if (blocked || nothingToSend) return;
    onSend(text.trim(), attachments);
    setText("");
    setAttachments([]);
  };

  const pick = async (file: File) => {
    try {
      const created = await upload.mutateAsync({ data: { file } });
      setAttachments((current) => [...current, created]);
    } catch (error) {
      toast.error(errorMessage(error, "Upload failed."));
    }
  };

  return (
    <div className="border-border bg-card flex flex-col gap-2 rounded-xl border p-2">
      {attachments.length > 0 && (
        <div className="flex flex-wrap gap-1.5 px-1 pt-1">
          {attachments.map((attachment) => (
            <span key={attachment.path} className="bg-muted flex items-center gap-1 rounded-md px-2 py-0.5 text-xs">
              <FileText className="size-3 shrink-0" />
              {attachment.name}
              <button
                type="button"
                aria-label={`Remove ${attachment.name}`}
                className="text-muted-foreground hover:text-foreground"
                onClick={() => setAttachments((current) => current.filter((item) => item.path !== attachment.path))}
              >
                <X className="size-3" />
              </button>
            </span>
          ))}
        </div>
      )}

      <div className="flex items-end gap-2">
        <input
          ref={fileRef}
          type="file"
          accept=".pdf,.docx"
          className="hidden"
          onChange={(event) => {
            const file = event.target.files?.[0];
            event.target.value = "";
            if (file) void pick(file);
          }}
        />
        <Button
          variant="ghost"
          size="icon"
          aria-label="Attach file"
          disabled={blocked}
          onClick={() => fileRef.current?.click()}
        >
          {upload.isPending ? <Loader2 className="animate-spin" /> : <Paperclip />}
        </Button>
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
        {running && !askPending ? (
          <Button variant="outline" size="icon" aria-label="Stop" onClick={onStop}>
            <Square />
          </Button>
        ) : (
          <Button size="icon" aria-label="Send" disabled={blocked || nothingToSend} onClick={submit}>
            <Send />
          </Button>
        )}
      </div>
    </div>
  );
}
