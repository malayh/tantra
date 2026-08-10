"use client";

import { useEffect, useRef, useState } from "react";
import { Bot, ChevronRight, FileText, Globe, Loader2, Search, Wrench } from "lucide-react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { useStore } from "zustand";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";
import type { AskItem, Banner, ChatStore, SubagentItem, TextItem, ToolItem, TranscriptItem, Turn } from "../state";

type AskResponder = (askId: string, response: string) => void;

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

const TOOL_ICONS = { web_search: Search, web_fetch: Globe, read_doc: FileText } as const;

const RESULT_LIMIT = 4000;
const SUMMARY_LIMIT = 80;

const stringify = (value: unknown): string =>
  typeof value === "string" ? value : (JSON.stringify(value, null, 2) ?? "");

const formatResult = (result: unknown): string => {
  const text = stringify(result);
  return text.length > RESULT_LIMIT ? `${text.slice(0, RESULT_LIMIT)}\n… truncated` : text;
};

const summarizeArgs = (args: Record<string, unknown>): string => {
  const values = Object.values(args);
  if (values.length === 0) return "";
  const text = typeof values[0] === "string" ? values[0] : (JSON.stringify(values[0]) ?? "");
  return text.length > SUMMARY_LIMIT ? `${text.slice(0, SUMMARY_LIMIT)}…` : text;
};

const formatBody = (body: string): string => {
  try {
    return JSON.stringify(JSON.parse(body), null, 2);
  } catch {
    return body;
  }
};

const itemKey = (item: TranscriptItem, index: number) => `${item.kind}-${item.sampleId}-${index}`;

function ThinkingBlock({ item }: { item: TextItem }) {
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

function TextBlock({ item }: { item: TextItem }) {
  return (
    <div className={MARKDOWN_CLASS}>
      <Markdown remarkPlugins={[remarkGfm]}>{item.content}</Markdown>
      {!item.final && <span className="bg-foreground ml-0.5 inline-block h-4 w-1.5 animate-pulse align-text-bottom" />}
    </div>
  );
}

function ToolChip({ item }: { item: ToolItem }) {
  const [open, setOpen] = useState(!item.final);

  useEffect(() => {
    setOpen(!item.final);
  }, [item.final]);

  const Icon = TOOL_ICONS[item.name as keyof typeof TOOL_ICONS] ?? Wrench;
  const summary = summarizeArgs(item.args);
  const result = formatResult(item.result);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger
        className={cn(
          "text-muted-foreground hover:text-foreground flex w-full items-center gap-1.5 text-xs",
          item.isError && "text-destructive",
        )}
      >
        <ChevronRight className={cn("size-3 shrink-0 transition-transform", open && "rotate-90")} />
        <Icon className="size-3 shrink-0" />
        <span className="font-medium">{item.name}</span>
        {summary.length > 0 && <span className="truncate">{summary}</span>}
        {!item.final && <Loader2 className="size-3 shrink-0 animate-spin" />}
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="border-border mt-2 flex flex-col gap-2 border-l pl-3">
          {item.progress.map((line, index) => (
            <p key={`${index}-${line}`} className="text-muted-foreground text-xs">
              {line}
            </p>
          ))}
          {result.length > 0 && <pre className="bg-muted overflow-x-auto rounded-lg p-3 text-xs">{result}</pre>}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

function AskCard({
  item,
  banner,
  onAskResponse,
}: {
  item: AskItem;
  banner: Banner | null;
  onAskResponse: AskResponder;
}) {
  const [sent, setSent] = useState(false);

  useEffect(() => {
    if (banner !== null) setSent(false);
  }, [banner]);

  const respond = (response: string) => {
    setSent(true);
    onAskResponse(item.askId, response);
  };

  const body = formatBody(item.body);

  return (
    <Card size="sm" className="max-w-md">
      <CardHeader>
        <CardTitle>{item.title}</CardTitle>
      </CardHeader>
      {body.length > 0 && (
        <CardContent>
          <pre className="bg-muted overflow-x-auto rounded-lg p-3 text-xs whitespace-pre-wrap">{body}</pre>
        </CardContent>
      )}
      <CardFooter className="gap-2">
        {item.status === "pending" ? (
          <>
            <Button size="sm" disabled={sent} onClick={() => respond("allow")}>
              Approve
            </Button>
            <Button size="sm" variant="destructive" disabled={sent} onClick={() => respond("deny")}>
              Deny
            </Button>
          </>
        ) : (
          <p className="text-muted-foreground text-xs">{item.allow === false ? "Denied" : "Approved"}</p>
        )}
      </CardFooter>
    </Card>
  );
}

function SubagentBlock({
  item,
  banner,
  onAskResponse,
}: {
  item: SubagentItem;
  banner: Banner | null;
  onAskResponse: AskResponder;
}) {
  const [open, setOpen] = useState(!item.final);

  useEffect(() => {
    setOpen(!item.final);
  }, [item.final]);

  const summary = summarizeArgs(item.args);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger
        className={cn(
          "text-muted-foreground hover:text-foreground flex w-full items-center gap-1.5 text-xs",
          item.isError && "text-destructive",
        )}
      >
        <ChevronRight className={cn("size-3 shrink-0 transition-transform", open && "rotate-90")} />
        <Bot className="size-3 shrink-0" />
        <span className="font-medium">{item.agent}</span>
        {summary.length > 0 && <span className="truncate">{summary}</span>}
        {!item.final && <Loader2 className="size-3 shrink-0 animate-spin" />}
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="border-border mt-2 flex flex-col gap-3 border-l pl-3">
          {item.items.map((nested, index) => (
            <Item key={itemKey(nested, index)} item={nested} banner={banner} onAskResponse={onAskResponse} />
          ))}
          {item.isError && <p className="text-destructive text-xs">{formatResult(item.result)}</p>}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

function Item({
  item,
  banner,
  onAskResponse,
}: {
  item: TranscriptItem;
  banner: Banner | null;
  onAskResponse: AskResponder;
}) {
  switch (item.kind) {
    case "thinking":
      return <ThinkingBlock item={item} />;
    case "text":
      return <TextBlock item={item} />;
    case "tool":
      return <ToolChip item={item} />;
    case "subagent":
      return <SubagentBlock item={item} banner={banner} onAskResponse={onAskResponse} />;
    case "ask":
      return <AskCard item={item} banner={banner} onAskResponse={onAskResponse} />;
  }
}

function TurnBlock({
  turn,
  banner,
  onAskResponse,
}: {
  turn: Turn;
  banner: Banner | null;
  onAskResponse: AskResponder;
}) {
  const empty = turn.items.every(
    (item) => item.kind !== "tool" && item.kind !== "subagent" && item.kind !== "ask" && item.content.length === 0,
  );

  return (
    <div className="flex flex-col gap-3">
      {(turn.input.length > 0 || turn.attachments.length > 0) && (
        <div className="flex justify-end">
          <div className="bg-secondary text-secondary-foreground flex max-w-[80%] flex-col gap-2 rounded-2xl px-4 py-2 text-sm">
            {turn.attachments.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {turn.attachments.map((attachment) => (
                  <span
                    key={attachment.path}
                    className="bg-muted flex items-center gap-1 rounded-md px-2 py-0.5 text-xs"
                  >
                    <FileText className="size-3 shrink-0" />
                    {attachment.name}
                  </span>
                ))}
              </div>
            )}
            {turn.input.length > 0 && <span className="whitespace-pre-wrap">{turn.input}</span>}
          </div>
        </div>
      )}

      {turn.items.map((item, index) => (
        <div key={itemKey(item, index)}>
          <Item item={item} banner={banner} onAskResponse={onAskResponse} />
        </div>
      ))}

      {turn.status === "running" && empty && <p className="text-muted-foreground animate-pulse text-sm">Thinking…</p>}
      {turn.status === "cancelled" && <p className="text-muted-foreground text-xs">Stopped.</p>}
      {turn.status === "failed" && <p className="text-destructive text-sm">{turn.error ?? "The turn failed."}</p>}
    </div>
  );
}

export function Transcript({ store, onAskResponse }: { store: ChatStore; onAskResponse: AskResponder }) {
  const turns = useStore(store, (state) => state.turns);
  const banner = useStore(store, (state) => state.banner);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const element = scrollRef.current;
    if (element) element.scrollTop = element.scrollHeight;
  }, [turns]);

  return (
    <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto">
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-8 px-6 py-8">
        {turns.map((turn) => (
          <TurnBlock key={turn.id} turn={turn} banner={banner} onAskResponse={onAskResponse} />
        ))}
      </div>
    </div>
  );
}
