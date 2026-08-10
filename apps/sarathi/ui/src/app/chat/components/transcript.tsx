"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronRight } from "lucide-react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useStore } from "zustand";

import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";
import type { ChatStore, TranscriptItem, Turn } from "../state";

const MARKDOWN_CLASS = [
  "text-sm leading-relaxed [&>*+*]:mt-3",
  "[&_a]:underline [&_a]:underline-offset-4",
  "[&_code]:bg-muted [&_code]:rounded [&_code]:px-1 [&_code]:py-0.5 [&_code]:text-xs",
  "[&_pre]:bg-muted [&_pre]:overflow-x-auto [&_pre]:rounded-lg [&_pre]:p-3",
  "[&_pre_code]:bg-transparent [&_pre_code]:p-0",
  "[&_h1]:text-base [&_h1]:font-semibold [&_h2]:text-sm [&_h2]:font-semibold [&_h3]:text-sm [&_h3]:font-semibold",
  "[&_ul]:list-disc [&_ul]:pl-5 [&_ol]:list-decimal [&_ol]:pl-5 [&_li]:my-1",
  "[&_blockquote]:border-border [&_blockquote]:text-muted-foreground [&_blockquote]:border-l-2 [&_blockquote]:pl-3",
  "[&_table]:w-full [&_th]:border-border [&_td]:border-border [&_th]:border [&_td]:border [&_th]:px-2 [&_td]:px-2",
].join(" ");

function ThinkingBlock({ item }: { item: TranscriptItem }) {
  const [open, setOpen] = useState(!item.final);

  useEffect(() => {
    setOpen(!item.final);
  }, [item.final]);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger
        className={cn(
          "text-muted-foreground hover:text-foreground flex items-center gap-1 text-xs",
          !item.final && "animate-pulse",
        )}
      >
        <ChevronRight className={cn("size-3 transition-transform", open && "rotate-90")} />
        Thinking
      </CollapsibleTrigger>
      <CollapsibleContent>
        <p className="text-muted-foreground border-border mt-2 border-l pl-3 text-xs whitespace-pre-wrap">
          {item.content}
        </p>
      </CollapsibleContent>
    </Collapsible>
  );
}

function TextBlock({ item }: { item: TranscriptItem }) {
  return (
    <div className={MARKDOWN_CLASS}>
      <Markdown remarkPlugins={[remarkGfm]}>{item.content}</Markdown>
      {!item.final && <span className="bg-foreground ml-0.5 inline-block h-4 w-1.5 animate-pulse align-text-bottom" />}
    </div>
  );
}

function TurnBlock({ turn }: { turn: Turn }) {
  const empty = turn.items.every((item) => item.content.length === 0);

  return (
    <div className="flex flex-col gap-3">
      {turn.input.length > 0 && (
        <div className="flex justify-end">
          <div className="bg-secondary text-secondary-foreground max-w-[80%] rounded-2xl px-4 py-2 text-sm whitespace-pre-wrap">
            {turn.input}
          </div>
        </div>
      )}

      {turn.items.map((item, index) => (
        <div key={`${item.kind}-${item.sampleId}-${index}`}>
          {item.kind === "thinking" ? <ThinkingBlock item={item} /> : <TextBlock item={item} />}
        </div>
      ))}

      {turn.status === "running" && empty && <p className="text-muted-foreground animate-pulse text-sm">Thinking…</p>}
      {turn.status === "cancelled" && <p className="text-muted-foreground text-xs">Stopped.</p>}
      {turn.status === "failed" && <p className="text-destructive text-sm">{turn.error ?? "The turn failed."}</p>}
    </div>
  );
}

export function Transcript({ store }: { store: ChatStore }) {
  const turns = useStore(store, (state) => state.turns);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const element = scrollRef.current;
    if (element) element.scrollTop = element.scrollHeight;
  }, [turns]);

  return (
    <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto">
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-8 px-6 py-8">
        {turns.map((turn) => (
          <TurnBlock key={turn.id} turn={turn} />
        ))}
      </div>
    </div>
  );
}
