"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useQueryClient } from "@tanstack/react-query";
import { LogOut, Plus } from "lucide-react";
import { signOut, useSession } from "next-auth/react";

import { Button } from "@/components/ui/button";
import { getListSessionsQueryKey, useCreateSession, useListSessions } from "@/generated/api/sessions/sessions";
import { clearToken } from "@/lib/apiClient";
import { cn } from "@/lib/utils";
import { MemoryDialog } from "./memory-dialog";

const shortTime = (iso: string): string => {
  const minutes = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 60000));
  if (minutes < 1) return "now";
  if (minutes < 60) return `${minutes}m`;
  if (minutes < 1440) return `${Math.round(minutes / 60)}h`;
  return `${Math.round(minutes / 1440)}d`;
};

export function Sidebar() {
  const router = useRouter();
  const pathname = usePathname();
  const queryClient = useQueryClient();
  const { data: sessions } = useListSessions();
  const { data: session } = useSession();

  const createSession = useCreateSession({
    mutation: {
      onSuccess: (created) => {
        queryClient.invalidateQueries({ queryKey: getListSessionsQueryKey() });
        router.push(`/chat/${created.id}`);
      },
    },
  });

  const logout = async () => {
    clearToken();
    await signOut({ callbackUrl: "/login" });
  };

  return (
    <aside className="border-border bg-sidebar flex h-screen w-64 shrink-0 flex-col border-r">
      <div className="p-3">
        <Button
          className="w-full justify-start"
          variant="outline"
          disabled={createSession.isPending}
          onClick={() => createSession.mutate({ data: {} })}
        >
          <Plus />
          New chat
        </Button>
      </div>

      <nav className="flex-1 overflow-y-auto px-2 pb-2">
        {sessions?.map((item) => (
          <Link
            key={item.id}
            href={`/chat/${item.id}`}
            className={cn(
              "hover:bg-accent hover:text-accent-foreground flex items-center gap-2 rounded-lg px-2 py-2 text-sm",
              pathname === `/chat/${item.id}` && "bg-accent text-accent-foreground",
            )}
          >
            <span className="min-w-0 flex-1 truncate">{item.title ?? "New chat"}</span>
            <span className="text-muted-foreground shrink-0 text-xs">{shortTime(item.updated_at)}</span>
          </Link>
        ))}
      </nav>

      <div className="border-border flex items-center gap-2 border-t p-3">
        <span className="text-muted-foreground min-w-0 flex-1 truncate text-xs">{session?.user?.email}</span>
        <MemoryDialog />
        <Button variant="ghost" size="icon-sm" aria-label="Log out" onClick={logout}>
          <LogOut />
        </Button>
      </div>
    </aside>
  );
}
