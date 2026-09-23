"use client";

import { Bot, Loader2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import type { ActorStatusOut } from "@/generated/models";
import { actorStatusLabel, type JournalState } from "../state";
import { JournalTranscript } from "./transcript";

const isRunning = (actor: ActorStatusOut) => actor.state === "queued" || actor.state === "running";

function ActorButton({
  actor,
  selected,
  onOpen,
}: {
  actor: ActorStatusOut;
  selected: boolean;
  onOpen: (agentId: string) => void;
}) {
  return (
    <Button
      variant={selected ? "secondary" : "outline"}
      size="sm"
      className="max-w-full"
      onClick={() => onOpen(actor.agent_id)}
    >
      <Bot />
      <span className="truncate">{actor.name}</span>
      <span className="text-muted-foreground truncate font-normal">{actorStatusLabel(actor.state)}</span>
      {isRunning(actor) && <Loader2 className="animate-spin" />}
    </Button>
  );
}

export function ActorStrip({
  actors,
  selected,
  onOpen,
}: {
  actors: ActorStatusOut[];
  selected: string | null;
  onOpen: (agentId: string) => void;
}) {
  const descendants = actors.filter((actor) => actor.parent_id !== null && isRunning(actor));
  if (descendants.length === 0) return null;

  return (
    <div className="flex flex-wrap gap-2 pb-2" aria-label="Subagents">
      {descendants.map((actor) => (
        <ActorButton key={actor.agent_id} actor={actor} selected={actor.agent_id === selected} onOpen={onOpen} />
      ))}
    </div>
  );
}

export function ChildDrawer({
  actor,
  actors,
  journal,
  onSelect,
  onClose,
}: {
  actor: ActorStatusOut | undefined;
  actors: ActorStatusOut[];
  journal: JournalState | undefined;
  onSelect: (agentId: string) => void;
  onClose: () => void;
}) {
  const descendants = actors.filter((item) => item.parent_id !== null);

  return (
    <Dialog open={actor !== undefined} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="!top-0 !right-0 !bottom-0 !left-auto flex !h-dvh !w-full !max-w-xl !translate-x-0 !translate-y-0 flex-col gap-0 !rounded-none p-0 sm:!max-w-xl">
        {actor && (
          <>
            <DialogHeader className="border-border gap-3 border-b p-4 pr-12">
              <div>
                <DialogTitle>{actor.name}</DialogTitle>
                <DialogDescription>{actorStatusLabel(actor.state)}</DialogDescription>
              </div>
              {descendants.length > 1 && (
                <div className="flex gap-2 overflow-x-auto pb-1">
                  {descendants.map((item) => (
                    <ActorButton
                      key={item.agent_id}
                      actor={item}
                      selected={item.agent_id === actor.agent_id}
                      onOpen={onSelect}
                    />
                  ))}
                </div>
              )}
            </DialogHeader>
            <JournalTranscript
              turns={journal?.turns ?? []}
              ready={journal?.ready ?? false}
              actors={actors}
              onOpen={onSelect}
              emptyText="No journal entries yet"
            />
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}
