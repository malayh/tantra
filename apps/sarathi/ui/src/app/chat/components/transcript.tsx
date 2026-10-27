"use client";

import { useEffect, useRef, useState } from "react";
import { Bot, ChevronRight, FileText, Globe, Loader2, Search, Wrench } from "lucide-react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import {
  askOf,
  type AgentRequest,
  type Entry,
  type MessageEntry,
  type RosterEntry,
  type StatusEntry,
  type ToolEntry,
  type UserEntry,
} from "tantra-acp-client";
import { useStore } from "zustand";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { useChat } from "../client";
import { actorStatusLabel, attachmentsOf, isLoading, isNearBottom, isReady, textOf } from "../state";

type Opener = (actorId: string) => void;

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

const summarizeArgs = (args: unknown): string => {
  const values = typeof args === "object" && args !== null && !Array.isArray(args) ? Object.values(args) : [];
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

function ThinkingBlock({ text, final }: { text: string; final: boolean }) {
  const [open, setOpen] = useState(!final);

  useEffect(() => {
    setOpen(!final);
  }, [final]);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger
        className={cn(
          "text-muted-foreground hover:text-foreground flex items-center gap-1 text-xs",
          !final && "animate-pulse",
        )}
      >
        <ChevronRight className={cn("size-3 transition-transform", open && "rotate-90")} />
        Thinking
      </CollapsibleTrigger>
      <CollapsibleContent>
        <p className="text-muted-foreground border-border mt-2 border-l pl-3 text-xs whitespace-pre-wrap">{text}</p>
      </CollapsibleContent>
    </Collapsible>
  );
}

function TextBlock({ text, final }: { text: string; final: boolean }) {
  return (
    <div className={MARKDOWN_CLASS}>
      <Markdown remarkPlugins={[remarkGfm]}>{text}</Markdown>
      {!final && <span className="bg-foreground ml-0.5 inline-block h-4 w-1.5 animate-pulse align-text-bottom" />}
    </div>
  );
}

function ChildButton({ actorId, actor, onOpen }: { actorId: string; actor: RosterEntry; onOpen: Opener }) {
  return (
    <Button variant="outline" size="sm" className="max-w-full" onClick={() => onOpen(actorId)}>
      <Bot />
      <span className="truncate">{actor.title ?? "Subagent"}</span>
      <span className="text-muted-foreground truncate font-normal">{actorStatusLabel(actor.state)}</span>
      {actor.state?.state !== "idle" && <Loader2 className="animate-spin" />}
    </Button>
  );
}

function ToolChip({ entry, live }: { entry: ToolEntry; live: boolean }) {
  const final = entry.status === "completed" || entry.status === "failed" || !live;
  const [open, setOpen] = useState(!final);

  useEffect(() => {
    setOpen(!final);
  }, [final]);

  const name = entry.name ?? entry.title ?? "tool";
  const Icon = TOOL_ICONS[name as keyof typeof TOOL_ICONS] ?? Wrench;
  const summary = summarizeArgs(entry.rawInput);
  const result = formatResult(entry.rawOutput);

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger
        className={cn(
          "text-muted-foreground hover:text-foreground flex w-full items-center gap-1.5 text-xs",
          entry.status === "failed" && "text-destructive",
        )}
      >
        <ChevronRight className={cn("size-3 shrink-0 transition-transform", open && "rotate-90")} />
        <Icon className="size-3 shrink-0" />
        <span className="font-medium">{name}</span>
        {summary.length > 0 && <span className="truncate">{summary}</span>}
        {!final && <Loader2 className="size-3 shrink-0 animate-spin" />}
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="border-border mt-2 flex flex-col gap-2 border-l pl-3">
          {entry.progress.map((line, index) => (
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

function MessageChip({
  entry,
  sessionId,
  roster,
  onOpen,
}: {
  entry: MessageEntry;
  sessionId: string;
  roster: Record<string, RosterEntry>;
  onOpen?: Opener;
}) {
  const [open, setOpen] = useState(false);
  const outgoing = entry.sender === sessionId;
  const peer = outgoing ? entry.recipient : entry.sender;
  const actor = peer === null ? undefined : roster[peer];

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="flex flex-wrap items-center gap-2">
        {peer !== null && actor && onOpen ? (
          <ChildButton actorId={peer} actor={actor} onOpen={onOpen} />
        ) : (
          <span className="text-muted-foreground flex items-center gap-1 text-xs">
            <Bot className="size-3" />
            Parent
          </span>
        )}
        <CollapsibleTrigger className="text-muted-foreground hover:text-foreground flex items-center gap-1 text-xs">
          <ChevronRight className={cn("size-3 transition-transform", open && "rotate-90")} />
          {outgoing ? "Sent" : entry.finished ? "Finished" : entry.reply ? "Replied" : "Received"}
        </CollapsibleTrigger>
      </div>
      <CollapsibleContent>
        <p className="text-muted-foreground border-border mt-2 border-l pl-3 text-xs whitespace-pre-wrap">
          {entry.text}
        </p>
      </CollapsibleContent>
    </Collapsible>
  );
}

function StatusLine({ entry }: { entry: StatusEntry }) {
  if (entry.stopReason === "cancelled") return <p className="text-muted-foreground text-xs">Stopped.</p>;
  if (entry.stopReason === "_tantra_interrupted") return <p className="text-muted-foreground text-xs">Interrupted.</p>;
  return (
    <div className="border-destructive/50 bg-destructive/10 text-destructive rounded-md border px-3 py-2 text-sm">
      {entry.error?.message ?? "The turn failed."}
    </div>
  );
}

function UserMessage({ entry }: { entry: UserEntry }) {
  const text = textOf(entry.content);
  const attachments = attachmentsOf(entry.content);
  if (text.length === 0 && attachments.length === 0) return null;

  return (
    <div className="flex justify-end">
      <div className="bg-secondary text-secondary-foreground flex max-w-[80%] flex-col gap-2 rounded-2xl px-4 py-2 text-sm">
        {attachments.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {attachments.map((attachment) => (
              <span key={attachment.path} className="bg-muted flex items-center gap-1 rounded-md px-2 py-0.5 text-xs">
                <FileText className="size-3 shrink-0" />
                {attachment.name}
              </span>
            ))}
          </div>
        )}
        {text.length > 0 && <span className="whitespace-pre-wrap">{text}</span>}
      </div>
    </div>
  );
}

function AskCard({ request, onAnswer }: { request: AgentRequest; onAnswer: () => void }) {
  const [text, setText] = useState("");
  const ask = askOf(request);
  const body = ask.body === null ? "" : formatBody(ask.body);

  const answer = (respond: () => void) => {
    respond();
    onAnswer();
  };

  return (
    <Card size="sm" className="max-w-md">
      <CardHeader>
        <CardTitle>{ask.title}</CardTitle>
      </CardHeader>
      {body.length > 0 && (
        <CardContent>
          <pre className="bg-muted overflow-x-auto rounded-lg p-3 text-xs whitespace-pre-wrap">{body}</pre>
        </CardContent>
      )}
      <CardFooter className="gap-2">
        {ask.kind === "approval" ? (
          <>
            <Button size="sm" onClick={() => answer(() => ask.respond(true))}>
              Approve
            </Button>
            <Button size="sm" variant="destructive" onClick={() => answer(() => ask.respond(false))}>
              Deny
            </Button>
          </>
        ) : ask.kind === "choice" ? (
          ask.options.map((option) => (
            <Button key={option} size="sm" variant="outline" onClick={() => answer(() => ask.respond(option))}>
              {option}
            </Button>
          ))
        ) : (
          <>
            <Input value={text} onChange={(event) => setText(event.target.value)} />
            <Button
              size="sm"
              disabled={text.trim().length === 0}
              onClick={() => answer(() => ask.respond(text.trim()))}
            >
              Submit
            </Button>
          </>
        )}
      </CardFooter>
    </Card>
  );
}

function EntryView({
  entry,
  live,
  sessionId,
  roster,
  onOpen,
}: {
  entry: Entry;
  live: boolean;
  sessionId: string;
  roster: Record<string, RosterEntry>;
  onOpen?: Opener;
}) {
  switch (entry.kind) {
    case "user":
      return <UserMessage entry={entry} />;
    case "thought":
      return entry.text.length > 0 && <ThinkingBlock text={entry.text} final={entry.final || !live} />;
    case "agent":
      return entry.text.length > 0 && <TextBlock text={entry.text} final={entry.final || !live} />;
    case "tool":
      return <ToolChip entry={entry} live={live} />;
    case "message":
      return <MessageChip entry={entry} sessionId={sessionId} roster={roster} onOpen={onOpen} />;
    case "status":
      return <StatusLine entry={entry} />;
  }
}

export function Transcript({
  sessionId,
  readonly = false,
  onOpen,
  emptyText,
}: {
  sessionId: string;
  readonly?: boolean;
  onOpen?: Opener;
  emptyText: string;
}) {
  const { store } = useChat();
  const view = useStore(store, (state) => state.transcript.sessions[sessionId]);
  const requests = useStore(store, (state) => state.transcript.requests[sessionId]);
  const roster = useStore(store, (state) => state.transcript.roster);
  const ready = useStore(store, (state) => isReady(state, sessionId));
  const loading = useStore(store, (state) => isLoading(state, sessionId));
  const writer = useStore(store, (state) => state.client.sessions[sessionId]?.writer ?? false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const followingRef = useRef(true);
  const entries = ready ? (view?.entries ?? []) : [];
  const live = !!view?.state && view.state.state !== "idle";
  const thinking = view?.state?.state === "running" && entries.at(-1)?.kind === "user";
  const awaiting = ready && (readonly || !writer) && view?.state?.ask?.state === "pending";

  useEffect(() => {
    const element = scrollRef.current;
    if (element && followingRef.current) element.scrollTop = element.scrollHeight;
  }, [view, requests]);

  return (
    <div
      ref={scrollRef}
      className="min-h-0 flex-1 overflow-y-auto"
      onScroll={(event) => {
        const element = event.currentTarget;
        followingRef.current = isNearBottom(element.scrollHeight, element.scrollTop, element.clientHeight);
      }}
    >
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-3 px-6 py-8">
        {ready && entries.length === 0 && (
          <p className="text-muted-foreground py-16 text-center text-sm">{emptyText}</p>
        )}
        {loading && <Loader2 className="text-muted-foreground mx-auto my-16 size-4 animate-spin" />}
        {entries.map((entry, index) => (
          <div key={`${entry.kind}-${entry.id}`} className={cn(entry.kind === "user" && index > 0 && "mt-5")}>
            <EntryView entry={entry} live={live} sessionId={sessionId} roster={roster} onOpen={onOpen} />
          </div>
        ))}
        {thinking && <p className="text-muted-foreground animate-pulse text-sm">Thinking…</p>}
        {Object.entries(ready ? (requests ?? {}) : {}).map(([askId, request]) => (
          <AskCard key={askId} request={request} onAnswer={() => store.getState().removeRequest(sessionId, askId)} />
        ))}
        {awaiting && <p className="text-muted-foreground text-xs">Awaiting input</p>}
      </div>
    </div>
  );
}
