"use client";

import { Bot, Loader2 } from "lucide-react";
import { descendants, type RosterEntry } from "tantra-acp-client";
import { useStore } from "zustand";

import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useChat } from "../client";
import { actorStatusLabel } from "../state";
import { Transcript } from "./transcript";

const isRunning = (actor: RosterEntry) => actor.state?.state !== "idle";

function ActorButton({
  actorId,
  actor,
  selected,
  onOpen,
}: {
  actorId: string;
  actor: RosterEntry;
  selected: boolean;
  onOpen: (actorId: string) => void;
}) {
  return (
    <Button
      variant={selected ? "secondary" : "outline"}
      size="sm"
      className="max-w-full"
      onClick={() => onOpen(actorId)}
    >
      <Bot />
      <span className="truncate">{actor.title ?? "Subagent"}</span>
      <span className="text-muted-foreground truncate font-normal">{actorStatusLabel(actor.state)}</span>
      {isRunning(actor) && <Loader2 className="animate-spin" />}
    </Button>
  );
}

export function ActorStrip({
  rootId,
  selected,
  onOpen,
}: {
  rootId: string;
  selected: string | null;
  onOpen: (actorId: string) => void;
}) {
  const transcript = useStore(useChat().store, (state) => state.transcript);
  const running = descendants(transcript, rootId).filter((id) => isRunning(transcript.roster[id]));
  if (running.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-2 pb-2" aria-label="Subagents">
      {running.map((id) => (
        <ActorButton key={id} actorId={id} actor={transcript.roster[id]} selected={id === selected} onOpen={onOpen} />
      ))}
    </div>
  );
}

export function ChildDrawer({
  rootId,
  childId,
  onSelect,
  onClose,
}: {
  rootId: string;
  childId: string | null;
  onSelect: (actorId: string) => void;
  onClose: () => void;
}) {
  const transcript = useStore(useChat().store, (state) => state.transcript);
  const tree = descendants(transcript, rootId);
  const actor = childId === null ? undefined : transcript.roster[childId];

  return (
    <Dialog open={childId !== null && actor !== undefined} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="!top-0 !right-0 !bottom-0 !left-auto flex !h-dvh !w-full !max-w-xl !translate-x-0 !translate-y-0 flex-col gap-0 !rounded-none p-0 sm:!max-w-xl">
        {childId !== null && actor && (
          <>
            <DialogHeader className="border-border gap-3 border-b p-4 pr-12">
              <div>
                <DialogTitle>{actor.title ?? "Subagent"}</DialogTitle>
                <DialogDescription>{actorStatusLabel(actor.state)}</DialogDescription>
              </div>
              {tree.length > 1 && (
                <div className="flex gap-2 overflow-x-auto pb-1">
                  {tree.map((id) => (
                    <ActorButton
                      key={id}
                      actorId={id}
                      actor={transcript.roster[id]}
                      selected={id === childId}
                      onOpen={onSelect}
                    />
                  ))}
                </div>
              )}
            </DialogHeader>
            <Transcript sessionId={childId} readonly onOpen={onSelect} emptyText="No journal entries yet" />
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}
