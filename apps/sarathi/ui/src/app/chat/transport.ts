import {
  SessionClient,
  SessionClientError,
  type AskResponse,
  type ClientState,
  type CommandPromise,
  type JsonValue,
  type SocketLike,
  type Subscription,
} from "tantra-session-client";

import type { SessionInput } from "@/generated/models";
import type { ChatStore, EventFrame, SessionEvent } from "./state";

const HEX_ID = /^[0-9a-f]{32}$/;
const UUID_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const canonicalId = (id: string): string => {
  if (!HEX_ID.test(id)) throw new TypeError("Expected a lowercase 32-character ID");
  return `${id.slice(0, 8)}-${id.slice(8, 12)}-${id.slice(12, 16)}-${id.slice(16, 20)}-${id.slice(20)}`;
};

export const journalId = (id: string): string => {
  if (!UUID_ID.test(id)) throw new TypeError("Expected a canonical UUID");
  return id.replaceAll("-", "");
};

export type ChatTransportOptions = {
  serverScope: string;
  sessionId: string;
  identity: string;
  store: ChatStore;
  socketUrl: () => Promise<string>;
  onNotice: (notice: JsonValue) => void | Promise<void>;
  onState: (state: ClientState) => void;
  onError: (error: SessionClientError) => void;
  webSocketFactory?: (url: string, protocol: string) => SocketLike;
};

export class ChatTransport {
  readonly client: SessionClient<SessionInput, SessionEvent>;
  private readonly options: ChatTransportOptions;
  private readonly root: Subscription;
  private readonly readyActors = new Set<string>();
  private child: Subscription | null = null;
  private childGeneration = 0;
  private childSwap: Promise<void> = Promise.resolve();
  private connection: ClientState["connection"] = "idle";

  constructor(options: ChatTransportOptions) {
    this.options = options;
    const cursors = Object.fromEntries(
      Object.entries(options.store.getState().cursors).map(([id, cursor]) => [canonicalId(id), cursor]),
    );
    this.client = new SessionClient<SessionInput, SessionEvent>({
      serverScope: options.serverScope,
      rootId: canonicalId(options.sessionId),
      identity: options.identity,
      url: options.socketUrl,
      initialCursors: cursors,
      onEvent: (frame) => {
        const agentId = journalId(frame.actor_id);
        if (frame.body === null) {
          options.store.getState().advanceCursor(agentId, frame.seq);
          return;
        }
        options.store.getState().dispatch({
          type: "event",
          agent_id: agentId,
          seq: frame.seq,
          event: frame.body,
        } as EventFrame);
      },
      onNotice: options.onNotice,
      onState: (state) => {
        if (state.connection !== "open" && this.connection === "open") {
          for (const actorId of this.readyActors) options.store.getState().setSubscriptionReady(actorId, false);
          this.readyActors.clear();
        }
        const subscribedActors = new Set(state.subscriptions.map((subscription) => journalId(subscription.actorId)));
        for (const actorId of this.readyActors) {
          if (!subscribedActors.has(actorId)) {
            this.readyActors.delete(actorId);
            options.store.getState().setSubscriptionReady(actorId, false);
          }
        }
        for (const subscription of state.subscriptions) {
          const actorId = journalId(subscription.actorId);
          if (!subscription.ready) this.readyActors.delete(actorId);
          if (subscription.ready && !this.readyActors.has(actorId)) {
            this.readyActors.add(actorId);
            options.store.getState().setSubscriptionReady(actorId, true, subscription.active);
          }
        }
        this.connection = state.connection;
        options.onState(state);
      },
      onError: options.onError,
      ...(options.webSocketFactory ? { webSocketFactory: options.webSocketFactory } : {}),
    });
    this.root = this.client.subscribe(canonicalId(options.sessionId), {
      after: options.store.getState().cursors[options.sessionId] ?? 0,
    });
  }

  get state(): ClientState {
    return this.client.state;
  }

  async connect(): Promise<void> {
    await this.client.connect();
    await this.root.ready();
  }

  async disconnect(): Promise<void> {
    ++this.childGeneration;
    await this.childSwap;
    await this.client.disconnect();
  }

  acquireWriter(): Promise<void> {
    return this.client.acquireWriter();
  }

  send(input: SessionInput, commandId = crypto.randomUUID()): CommandPromise {
    return this.client.send(input, { commandId });
  }

  answer(askId: string, response: AskResponse, commandId = crypto.randomUUID()): CommandPromise {
    return this.client.answer(canonicalId(askId), response, { commandId });
  }

  cancel(commandId = crypto.randomUUID()): CommandPromise {
    return this.client.cancel({ commandId });
  }

  retry(commandId: string): CommandPromise {
    return this.client.retry(commandId);
  }

  async openChild(agentId: string): Promise<void> {
    const generation = ++this.childGeneration;
    const swap = this.childSwap.then(async () => {
      if (this.child) await this.child.unsubscribe();
      this.child = null;
      if (generation !== this.childGeneration) return null;
      this.options.store.getState().setOpenChild(agentId);
      const next = this.client.subscribe(canonicalId(agentId), {
        after: this.options.store.getState().cursors[agentId] ?? 0,
      });
      this.child = next;
      return next;
    });
    this.childSwap = swap.then(
      () => {},
      () => {},
    );
    const next = await swap;
    try {
      await next?.ready();
    } catch (error) {
      if (generation === this.childGeneration) throw error;
    }
  }

  async closeChild(): Promise<void> {
    const generation = ++this.childGeneration;
    const swap = this.childSwap.then(async () => {
      if (this.child) await this.child.unsubscribe();
      this.child = null;
      if (generation === this.childGeneration) this.options.store.getState().setOpenChild(null);
    });
    this.childSwap = swap.catch(() => {});
    await swap;
  }
}
