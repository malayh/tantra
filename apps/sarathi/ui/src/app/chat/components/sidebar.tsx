"use client";

import { useEffect } from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { LogOut, Moon, Plus, Sun } from "lucide-react";
import { signOut, useSession } from "next-auth/react";
import { useTheme } from "next-themes";
import { toast } from "sonner";
import type { SessionInfo } from "tantra-acp-client";
import { useStore } from "zustand";

import { Button } from "@/components/ui/button";
import { clearToken } from "@/lib/apiClient";
import { cn } from "@/lib/utils";
import { errorText, newChat, SESSIONS_KEY, useChat } from "../client";
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
  const chat = useChat();
  const connected = useStore(chat.store, (state) => state.client.connection === "open");
  const { data: sessions, error } = useQuery({
    queryKey: SESSIONS_KEY,
    enabled: connected,
    queryFn: async () => {
      const found: SessionInfo[] = [];
      let cursor: string | undefined;
      do {
        const page = await chat.client.listSessions(cursor);
        found.push(...page.sessions);
        cursor = page.nextCursor;
      } while (cursor !== undefined);
      return found;
    },
  });
  const { data: session } = useSession();
  const { resolvedTheme, setTheme } = useTheme();

  useEffect(() => {
    if (error) toast.error("Could not load sessions.");
  }, [error]);

  const createSession = useMutation({
    mutationFn: () => newChat(chat),
    onSuccess: (sessionId) => {
      queryClient.invalidateQueries({ queryKey: SESSIONS_KEY });
      router.push(`/chat/${sessionId}`);
    },
    onError: (failure) => toast.error(errorText(failure)),
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
          disabled={createSession.isPending || !connected}
          onClick={() => createSession.mutate()}
        >
          <Plus />
          New chat
        </Button>
      </div>

      <nav className="flex-1 overflow-y-auto px-2 pb-2">
        {sessions?.length === 0 && <p className="text-muted-foreground px-2 py-2 text-sm">No sessions yet</p>}
        {sessions?.map((item) => (
          <Link
            key={item.sessionId}
            href={`/chat/${item.sessionId}`}
            className={cn(
              "hover:bg-accent hover:text-accent-foreground flex items-center gap-2 rounded-lg px-2 py-2 text-sm",
              pathname === `/chat/${item.sessionId}` && "bg-accent text-accent-foreground",
            )}
          >
            <span className="min-w-0 flex-1 truncate">{item.title ?? "New chat"}</span>
            {item.updatedAt && (
              <span className="text-muted-foreground shrink-0 text-xs">{shortTime(item.updatedAt)}</span>
            )}
          </Link>
        ))}
      </nav>

      <div className="border-border flex items-center gap-2 border-t p-3">
        <span className="text-muted-foreground min-w-0 flex-1 truncate text-xs">{session?.user?.email}</span>
        <MemoryDialog />
        <Button
          variant="ghost"
          size="icon-sm"
          aria-label="Toggle theme"
          title="Toggle theme"
          onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
        >
          <Sun className="dark:hidden" />
          <Moon className="hidden dark:block" />
        </Button>
        <Button variant="ghost" size="icon-sm" aria-label="Log out" onClick={logout}>
          <LogOut />
        </Button>
      </div>
    </aside>
  );
}
