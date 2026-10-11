import {
  canonicalUuid,
  cursor,
  jsonBytes,
  jsonSnapshot,
  parseServerFrame,
  validateAnswer,
  type AskResponse,
  type CommandPayload,
  type JsonValue,
  type ServerFrame,
} from "./protocol.js";

export type EventFrame<T = JsonValue> = Omit<
  Extract<ServerFrame, { type: "event" }>,
  "body"
> & { body: T | null };
export type ReceiptFrame = Extract<ServerFrame, { type: "receipt" }>;
export type SocketLike = Pick<
  WebSocket,
  | "readyState"
  | "protocol"
  | "bufferedAmount"
  | "send"
  | "close"
  | "addEventListener"
  | "removeEventListener"
>;
export interface SubscriptionState {
  actorId: string;
  cursor: number;
  ready: boolean;
  writable: boolean;
  active: boolean;
}
export interface CommandState {
  commandId: string;
  attempts: number;
  status: "queued" | "waiting" | "retry" | "held" | "manual";
}
export interface ClientState {
  connection: "idle" | "connecting" | "open" | "closed" | "blocked";
  subscriptions: SubscriptionState[];
  outbox: CommandState[];
}
export interface PersistedState {
  version: 1;
  scope: { server: string; root: string; identity: string };
  actors: Record<string, number>;
  commands: {
    commandId: string;
    payload: CommandPayload;
    attempts: number;
    manual: boolean;
  }[];
}
export interface Persistence {
  load(): Promise<unknown>;
  save(state: PersistedState): Promise<void>;
}
export interface ClientTiming {
  receiptTimeoutMs?: number;
  retryDelaysMs?: [number, number, number];
  reconnectBaseMs?: number;
  reconnectCapMs?: number;
  capacityTimeoutMs?: number;
}
export interface SessionClientOptions<I, E> {
  serverScope: string;
  rootId: string;
  identity: string;
  url: () => string | Promise<string>;
  onEvent: (event: EventFrame<E>) => void | Promise<void>;
  onNotice?: (body: JsonValue) => void | Promise<void>;
  onReceipt?: (receipt: ReceiptFrame) => void | Promise<void>;
  onState?: (state: ClientState) => void;
  onError?: (error: SessionClientError) => void;
  persistence?: Persistence;
  initialCursors?: Record<string, number>;
  webSocketFactory?: (url: string, protocol: string) => SocketLike;
  timing?: ClientTiming;
}

export class SessionClientError extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly commandId?: string,
    public readonly uncertain = false,
    public readonly retryable = false,
  ) {
    super(message);
    this.name = "SessionClientError";
  }
}

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
};
const deferred = <T>(): Deferred<T> => {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  void promise.catch(() => {});
  return { promise, resolve, reject };
};
export type CommandPromise = Promise<ReceiptFrame> & {
  readonly commandId: string;
};

type Actor = SubscriptionState & {
  id: string | null;
  desired: boolean;
  lost: boolean;
  closing?: Deferred<void> | undefined;
  acquiring?: Deferred<void> | undefined;
  waiters: Deferred<void>[];
};
type Pending = CommandState & {
  payload: CommandPayload;
  bytes: number;
  reply?: Deferred<ReceiptFrame> | undefined;
  sentId?: string;
};
type Incoming = { frame: ServerFrame; generation: number; bytes: number };

export class Subscription {
  constructor(
    readonly actorId: string,
    private readonly getState: () => SubscriptionState,
    private readonly getReady: () => Promise<void>,
    private readonly close: () => Promise<void>,
  ) {}
  get state(): SubscriptionState {
    return this.getState();
  }
  ready(): Promise<void> {
    return this.getReady();
  }
  unsubscribe(): Promise<void> {
    return this.close();
  }
}

export class SessionClient<I = JsonValue, E = JsonValue> {
  private readonly options: SessionClientOptions<I, E>;
  private readonly scope: PersistedState["scope"];
  private readonly initialCursors: Record<string, number>;
  private readonly timing: Required<ClientTiming>;
  private readonly actors = new Map<string, Actor>();
  private readonly outbox: Pending[] = [];
  private readonly incoming: Incoming[] = [];
  private incomingFrames = 0;
  private incomingBytes = 0;
  private socket: SocketLike | null = null;
  private detach: (() => void) | undefined;
  private opening: Deferred<void> | undefined;
  private generation = 0;
  private wanted = false;
  private phase: ClientState["connection"] = "idle";
  private dialTask: Promise<void> | undefined;
  private draining: Promise<void> | undefined;
  private pumping = false;
  private notifying = false;
  private loaded = false;
  private loading: Promise<void> | undefined;
  private storageFailed = false;
  private saving: Promise<void> = Promise.resolve();
  private receiptTimer: ReturnType<typeof setTimeout> | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  private reconnectFailures = 0;
  private readonly sleepers = new Set<{
    timer: ReturnType<typeof setTimeout>;
    reject: (error: unknown) => void;
  }>();

  constructor(options: SessionClientOptions<I, E>) {
    if (
      typeof options.serverScope !== "string" ||
      !options.serverScope ||
      typeof options.identity !== "string" ||
      !options.identity ||
      typeof options.url !== "function" ||
      typeof options.onEvent !== "function"
    ) {
      throw new TypeError(
        "Server scope, authenticated identity, URL resolver, and event handler are required",
      );
    }
    this.scope = {
      server: options.serverScope,
      root: canonicalUuid(options.rootId),
      identity: options.identity,
    };
    this.options = { ...options };
    this.initialCursors = {};
    for (const [id, value] of Object.entries(options.initialCursors ?? {})) {
      this.initialCursors[canonicalUuid(id)] = cursor(value);
    }
    this.timing = {
      receiptTimeoutMs: 10_000,
      retryDelaysMs: [1_000, 2_000, 4_000],
      reconnectBaseMs: 1_000,
      reconnectCapMs: 30_000,
      capacityTimeoutMs: 10_000,
      ...options.timing,
    };
    if (
      this.timing.retryDelaysMs.length !== 3 ||
      [
        ...this.timing.retryDelaysMs,
        this.timing.receiptTimeoutMs,
        this.timing.reconnectBaseMs,
        this.timing.reconnectCapMs,
        this.timing.capacityTimeoutMs,
      ].some((value) => !Number.isFinite(value) || value <= 0)
    ) {
      throw new TypeError(
        "Timing values must be finite and positive; exactly three retry delays are required",
      );
    }
    this.timing.retryDelaysMs = [...this.timing.retryDelaysMs];
  }

  get state(): ClientState {
    return {
      connection: this.phase,
      subscriptions: [...this.actors.values()]
        .filter((actor) => actor.desired)
        .map((actor) => this.actorSnapshot(actor)),
      outbox: this.outbox.map(({ commandId, attempts, status }) => ({
        commandId,
        attempts,
        status,
      })),
    };
  }

  async connect(): Promise<void> {
    if (this.socket?.readyState === 1) return;
    if (this.dialTask) return this.dialTask;
    this.wanted = true;
    this.storageFailed = false;
    this.reconnectFailures = 0;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    await this.load();
    try {
      await this.save();
    } catch (error) {
      this.fault(error);
      throw error;
    }
    await this.draining;
    if (!this.wanted) return;
    return this.startDial();
  }

  async disconnect(): Promise<void> {
    this.wanted = false;
    this.phase = "closed";
    this.retire(new SessionClientError("disconnected", "Disconnected"));
    await this.draining;
    this.changed();
  }

  subscribe(actorId: string, options: { after?: number } = {}): Subscription {
    actorId = canonicalUuid(actorId);
    if (this.actors.get(actorId)?.desired)
      throw new SessionClientError(
        "subscription_exists",
        "Actor is already subscribed",
      );
    if (this.actors.get(actorId)?.closing)
      throw new SessionClientError(
        "subscription_closing",
        "Wait for unsubscribe acknowledgment",
      );
    if ([...this.actors.values()].filter((actor) => actor.desired).length >= 8)
      throw new SessionClientError("capacity", "Subscription limit exceeded");
    const actor: Actor = {
      actorId,
      cursor: cursor(
        options.after ??
          this.actors.get(actorId)?.cursor ??
          this.initialCursors[actorId] ??
          0,
      ),
      ready: false,
      writable: false,
      active: false,
      id: null,
      desired: true,
      lost: false,
      waiters: [],
    };
    this.actors.set(actorId, actor);
    if (this.loaded) void this.save().catch((error) => this.fault(error));
    if (this.socket?.readyState === 1)
      void this.openActor(actor).catch((error) => this.fault(error));
    this.changed();
    return new Subscription(
      actorId,
      () => this.actorSnapshot(actor),
      () =>
        this.actors.get(actorId) === actor
          ? this.subscriptionReady(actorId)
          : Promise.reject(
              new SessionClientError("unsubscribed", "Subscription replaced"),
            ),
      () =>
        this.actors.get(actorId) === actor
          ? this.unsubscribe(actorId)
          : Promise.resolve(),
    );
  }

  async unsubscribe(actorId: string): Promise<void> {
    const actor = this.actor(actorId);
    if (actor.actorId === this.scope.root && this.outbox.length)
      throw new SessionClientError(
        "pending_commands",
        "Settle the root outbox before unsubscribing; disconnect preserves pending commands",
      );
    if (actor.closing) return actor.closing.promise;
    actor.desired = false;
    actor.ready = actor.writable = false;
    const error = new SessionClientError("unsubscribed", "Subscription closed");
    this.rejectActor(actor, error);
    if (!actor.id || this.socket?.readyState !== 1) {
      await this.draining;
      this.finishActor(actor);
      await this.save();
      this.changed();
      return;
    }
    const closing = deferred<void>();
    actor.closing = closing;
    try {
      await this.write({ type: "unsubscribe", subscription_id: actor.id });
      await this.deadline(closing.promise);
      await this.save();
    } catch (error) {
      if (this.actors.get(actor.actorId) === actor) this.finishActor(actor);
      if (error instanceof SessionClientError && error.code === "storage")
        this.fault(error);
      else if (this.socket)
        this.drop(
          new SessionClientError(
            "transport",
            "Unsubscribe acknowledgment unavailable",
          ),
          true,
        );
      throw error;
    }
    this.changed();
  }

  subscriptionState(actorId: string): SubscriptionState {
    return this.actorSnapshot(this.actor(actorId));
  }
  subscriptionReady(actorId: string): Promise<void> {
    const actor = this.actor(actorId);
    if (!actor.desired)
      return Promise.reject(
        new SessionClientError("unsubscribed", "Subscription closed"),
      );
    if (actor.ready) return Promise.resolve();
    const waiter = deferred<void>();
    actor.waiters.push(waiter);
    return waiter.promise;
  }

  async acquireWriter(): Promise<void> {
    const actor = this.actor(this.scope.root);
    await this.subscriptionReady(actor.actorId);
    if (actor.writable) return;
    if (actor.acquiring) return actor.acquiring.promise;
    const generation = this.generation;
    const acquiring = deferred<void>();
    actor.acquiring = acquiring;
    actor.ready = false;
    actor.lost = false;
    this.changed();
    try {
      await this.write({ type: "acquire_writer", subscription_id: actor.id! });
      await this.deadline(acquiring.promise);
    } catch (error) {
      if (generation === this.generation && actor.acquiring === acquiring)
        this.drop(
          new SessionClientError(
            "transport",
            "Writer acquisition outcome unknown; reclaim after reconnect",
            undefined,
            true,
            true,
          ),
          true,
        );
      throw error;
    }
  }

  send(input: I, options: { commandId?: string } = {}): CommandPromise {
    return this.enqueue(
      { operation: "send", input: jsonSnapshot(input) },
      options.commandId,
    );
  }
  answer(
    askId: string,
    response: AskResponse,
    options: { commandId?: string } = {},
  ): CommandPromise {
    return this.enqueue(
      {
        operation: "answer",
        askId: canonicalUuid(askId),
        response: validateAnswer(response),
      },
      options.commandId,
    );
  }
  cancel(options: { commandId?: string } = {}): CommandPromise {
    return this.enqueue({ operation: "cancel" }, options.commandId);
  }
  retry(commandId: string): CommandPromise {
    const pending = this.outbox.find(
      (item) => item.commandId === canonicalUuid(commandId),
    );
    if (!pending || pending.status !== "manual")
      throw new SessionClientError(
        "invalid_retry",
        "Only a paused command can be retried",
      );
    pending.attempts = 0;
    pending.status = "queued";
    pending.reply = deferred<ReceiptFrame>();
    void this.save().catch((error) => this.fault(error));
    this.pump();
    this.changed();
    return this.commandPromise(pending);
  }

  private enqueue(payload: CommandPayload, id?: string): CommandPromise {
    if (this.options.persistence && !this.loaded)
      throw new SessionClientError(
        "storage",
        "Connect and load persistence before enqueueing commands",
      );
    if (!this.actors.get(this.scope.root)?.desired)
      throw new SessionClientError(
        "subscription_required",
        "Subscribe to the root before sending",
      );
    const commandId = canonicalUuid(id ?? crypto.randomUUID());
    payload = jsonSnapshot(
      payload as unknown as JsonValue,
    ) as unknown as CommandPayload;
    const existing = this.outbox.find((item) => item.commandId === commandId);
    if (existing) {
      if (existing.status === "manual")
        throw new SessionClientError(
          "unknown_outcome",
          "Retry this paused command explicitly",
          commandId,
          true,
        );
      if (JSON.stringify(existing.payload) !== JSON.stringify(payload))
        throw new SessionClientError(
          "command_conflict",
          "Command ID has different content",
          commandId,
        );
      if (!existing.reply) existing.reply = deferred<ReceiptFrame>();
      return this.commandPromise(existing);
    }
    const bytes = jsonBytes(
      this.commandFrame(commandId, payload, this.scope.root),
    ).byteLength;
    if (
      bytes > 256 * 1024 ||
      this.outbox.length >= 64 ||
      this.outbox.reduce((sum, item) => sum + item.bytes, bytes) > 1024 * 1024
    ) {
      throw new SessionClientError(
        "capacity",
        "Command/outbox limit exceeded",
        commandId,
      );
    }
    const pending: Pending = {
      commandId,
      payload,
      bytes,
      attempts: 0,
      status: "queued",
      reply: deferred<ReceiptFrame>(),
    };
    this.outbox.push(pending);
    void this.save().catch((error) => this.fault(error));
    this.pump();
    this.changed();
    return this.commandPromise(pending);
  }

  private commandPromise(pending: Pending): CommandPromise {
    const promise = pending.reply!.promise as CommandPromise;
    if (!("commandId" in promise))
      Object.defineProperty(promise, "commandId", { value: pending.commandId });
    return promise;
  }
  private commandFrame(
    id: string,
    payload: CommandPayload,
    subscriptionId: string,
  ): JsonValue {
    const base = {
      type: payload.operation,
      subscription_id: subscriptionId,
      command_id: id,
    };
    return payload.operation === "send"
      ? { ...base, input: payload.input }
      : payload.operation === "answer"
        ? { ...base, ask_id: payload.askId, response: payload.response }
        : base;
  }
  private actor(id: string): Actor {
    const actor = this.actors.get(canonicalUuid(id));
    if (!actor)
      throw new SessionClientError(
        "subscription_required",
        "Actor is not subscribed",
      );
    return actor;
  }
  private actorSnapshot({
    actorId,
    cursor,
    ready,
    writable,
    active,
  }: Actor): SubscriptionState {
    return { actorId, cursor, ready, writable, active };
  }
  private changed(): void {
    if (this.notifying) return;
    this.notifying = true;
    try {
      this.options.onState?.(this.state);
    } catch {
      this.fault(
        new SessionClientError("application", "State callback failed"),
      );
    } finally {
      this.notifying = false;
    }
  }
  private report(error: SessionClientError): void {
    try {
      this.options.onError?.(
        new SessionClientError(
          error.code,
          error.message,
          error.commandId,
          error.uncertain,
          error.retryable,
        ),
      );
    } catch {}
  }

  private startDial(): Promise<void> {
    if (this.dialTask) return this.dialTask;
    const task = this.dial();
    this.dialTask = task;
    void task
      .finally(() => {
        if (this.dialTask === task) this.dialTask = undefined;
      })
      .catch(() => {});
    return task;
  }
  private async dial(): Promise<void> {
    const generation = ++this.generation;
    this.phase = "connecting";
    this.changed();
    try {
      const url = await this.deadline(Promise.resolve(this.options.url()));
      if (!this.wanted || generation !== this.generation) return;
      const socket =
        this.options.webSocketFactory?.(url, "tantra.session.v1") ??
        new WebSocket(url, "tantra.session.v1");
      this.socket = socket;
      const opening = deferred<void>();
      this.opening = opening;
      const onOpen = () => {
        if (generation !== this.generation) return;
        if (socket.protocol !== "tantra.session.v1") {
          this.fault(
            new SessionClientError(
              "protocol",
              "Incorrect WebSocket subprotocol",
            ),
          );
          return;
        }
        this.phase = "open";
        this.reconnectFailures = 0;
        this.opening = undefined;
        opening.resolve();
        for (const actor of this.actors.values())
          if (actor.desired)
            void this.openActor(actor).catch((error) => this.fault(error));
        this.changed();
      };
      const onMessage = (event: Event) => {
        if (generation !== this.generation) return;
        try {
          const data: unknown = (event as MessageEvent).data;
          if (typeof data !== "string")
            throw new SessionClientError("protocol", "JSON text required");
          const bytes = new TextEncoder().encode(data).byteLength;
          if (bytes > 1024 * 1024)
            throw new SessionClientError("size", "Server frame too large");
          const frame = parseServerFrame(data);
          if (frame.type === "writer_lost") {
            const actor = [...this.actors.values()].find(
              (item) => item.id === frame.subscription_id,
            );
            if (actor) {
              actor.writable = actor.ready = false;
              actor.lost = true;
              this.hold();
            }
          }
          if (
            this.incomingFrames >= 256 ||
            this.incomingBytes + bytes > 4 * 1024 * 1024
          ) {
            this.drop(
              new SessionClientError(
                "overflow",
                "Incoming queue exceeded capacity",
              ),
              true,
            );
            return;
          }
          this.incoming.push({ frame, generation, bytes });
          this.incomingFrames++;
          this.incomingBytes += bytes;
          this.drain();
        } catch (error) {
          this.fault(
            error instanceof SessionClientError
              ? error
              : new SessionClientError("protocol", "Invalid server frame"),
          );
        }
      };
      const onClose = (event: Event) => {
        if (generation !== this.generation) return;
        const code = (event as CloseEvent).code;
        const terminal = [1002, 1008, 1009, 4404].includes(code);
        this.drop(
          new SessionClientError(
            code === 4404
              ? "deleted"
              : code === 1008
                ? "authentication"
                : terminal
                  ? "protocol"
                  : "transport",
            code === 4404 ? "Session deleted" : "WebSocket closed",
            undefined,
            false,
            !terminal,
          ),
          !terminal,
        );
      };
      const onError = () => {};
      socket.addEventListener("open", onOpen);
      socket.addEventListener("message", onMessage);
      socket.addEventListener("close", onClose);
      socket.addEventListener("error", onError);
      this.detach = () => {
        socket.removeEventListener("open", onOpen);
        socket.removeEventListener("message", onMessage);
        socket.removeEventListener("close", onClose);
        socket.removeEventListener("error", onError);
      };
      await this.deadline(opening.promise);
    } catch (error) {
      if (generation === this.generation)
        this.drop(
          new SessionClientError(
            "transport",
            "Connection unavailable",
            undefined,
            false,
            true,
          ),
          true,
        );
      throw error;
    }
  }
  private async openActor(actor: Actor): Promise<void> {
    if (!actor.desired || actor.id) return;
    actor.id = crypto.randomUUID();
    actor.ready = actor.writable = false;
    await this.write({
      type: "subscribe",
      subscription_id: actor.id,
      actor_id: actor.actorId,
      after: actor.cursor,
      mode: "read",
    });
  }
  private finishActor(actor: Actor): void {
    actor.id = null;
    actor.desired = actor.ready = actor.writable = false;
    actor.closing?.resolve();
    actor.closing = undefined;
    while (this.actors.size > 16) {
      const retired = [...this.actors.values()].find(
        (item) => !item.desired && item !== actor,
      );
      if (!retired) break;
      this.actors.delete(retired.actorId);
    }
  }
  private rejectActor(actor: Actor, error: SessionClientError): void {
    actor.waiters.splice(0).forEach((waiter) => waiter.reject(error));
    actor.acquiring?.reject(error);
    actor.acquiring = undefined;
  }
  private hold(): void {
    clearTimeout(this.receiptTimer);
    clearTimeout(this.retryTimer);
    this.receiptTimer = this.retryTimer = undefined;
    const head = this.outbox[0];
    if (head && head.status !== "manual") head.status = "held";
  }
  private retire(error: SessionClientError): void {
    this.generation++;
    this.detach?.();
    this.detach = undefined;
    const socket = this.socket;
    this.socket = null;
    if (socket && socket.readyState < 2) socket.close();
    this.opening?.reject(error);
    this.opening = undefined;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = undefined;
    this.hold();
    for (const sleeper of this.sleepers) {
      clearTimeout(sleeper.timer);
      sleeper.reject(error);
    }
    this.sleepers.clear();
    for (const actor of this.actors.values()) {
      actor.id = null;
      actor.ready = actor.writable = false;
      actor.lost = true;
      this.rejectActor(actor, error);
      if (actor.closing) {
        actor.closing.resolve();
        this.finishActor(actor);
      }
    }
    for (const item of this.incoming.splice(0)) {
      this.incomingFrames--;
      this.incomingBytes -= item.bytes;
    }
  }
  private drop(error: SessionClientError, reconnect: boolean): void {
    this.phase = reconnect ? "closed" : "blocked";
    this.retire(error);
    this.report(error);
    this.changed();
    if (reconnect && this.wanted) this.scheduleReconnect();
  }
  private fault(error: unknown): void {
    const failure =
      error instanceof SessionClientError
        ? error
        : new SessionClientError("application", "Application callback failed");
    if (failure.code === "storage") this.storageFailed = true;
    this.drop(failure, false);
  }
  private scheduleReconnect(): void {
    if (++this.reconnectFailures > 10) {
      this.phase = "blocked";
      this.report(
        new SessionClientError(
          "reconnect_exhausted",
          "Reconnect attempts exhausted",
        ),
      );
      this.changed();
      return;
    }
    const delay =
      Math.min(
        this.timing.reconnectCapMs,
        this.timing.reconnectBaseMs * 2 ** (this.reconnectFailures - 1),
      ) *
      (0.75 + Math.random() * 0.25);
    const generation = this.generation;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.wanted || generation !== this.generation) return;
      void (async () => {
        await this.draining;
        if (this.wanted && generation === this.generation)
          await this.startDial();
      })().catch(() => {});
    }, delay);
  }
  private async write(frame: JsonValue): Promise<void> {
    const socket = this.socket;
    const generation = this.generation;
    if (!socket || socket.readyState !== 1)
      throw new SessionClientError("transport", "Not connected");
    const encoded = JSON.stringify(frame);
    const bytes = new TextEncoder().encode(encoded).byteLength;
    if (bytes > 256 * 1024)
      throw new SessionClientError("size", "Client frame too large");
    const deadline = performance.now() + this.timing.capacityTimeoutMs;
    while (socket.bufferedAmount + bytes > 4 * 1024 * 1024) {
      if (performance.now() >= deadline) {
        this.drop(
          new SessionClientError("capacity", "Socket send stalled"),
          true,
        );
        throw new SessionClientError("transport", "Socket send stalled");
      }
      await this.sleep(10);
      if (generation !== this.generation || socket.readyState !== 1)
        throw new SessionClientError("transport", "Connection changed");
    }
    if (generation !== this.generation)
      throw new SessionClientError("transport", "Connection changed");
    socket.send(encoded);
  }
  private async deadline<T>(promise: Promise<T>): Promise<T> {
    const timeout = deferred<T>();
    const sleeper = {
      timer: setTimeout(
        () =>
          timeout.reject(
            new SessionClientError("transport", "Operation timed out"),
          ),
        this.timing.capacityTimeoutMs,
      ),
      reject: timeout.reject,
    };
    this.sleepers.add(sleeper);
    try {
      return await Promise.race([promise, timeout.promise]);
    } finally {
      clearTimeout(sleeper.timer);
      this.sleepers.delete(sleeper);
    }
  }
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const sleeper = {
        timer: setTimeout(() => {
          this.sleepers.delete(sleeper);
          resolve();
        }, ms),
        reject,
      };
      this.sleepers.add(sleeper);
    });
  }

  private drain(): void {
    if (this.draining) return;
    const task = Promise.resolve().then(async () => {
      while (this.incoming.length) {
        const item = this.incoming.shift()!;
        try {
          if (item.generation === this.generation)
            await this.handle(item.frame);
        } catch (error) {
          this.fault(error);
        } finally {
          this.incomingFrames--;
          this.incomingBytes -= item.bytes;
        }
      }
    });
    this.draining = task;
    void task
      .finally(() => {
        if (this.draining === task) this.draining = undefined;
        if (this.incoming.length) this.drain();
      })
      .catch(() => {});
  }
  private async handle(frame: ServerFrame): Promise<void> {
    if (frame.type === "error" && !frame.subscription_id) {
      this.report(
        new SessionClientError(
          frame.code,
          frame.message,
          frame.command_id ?? undefined,
          frame.uncertain,
          frame.retryable,
        ),
      );
      return;
    }
    if (frame.type === "notice") {
      await this.options.onNotice?.(jsonSnapshot(frame.body));
      return;
    }
    const actor = frame.subscription_id
      ? [...this.actors.values()].find(
          (item) => item.id === frame.subscription_id,
        )
      : undefined;
    if (!actor) return;
    if (frame.type === "event") {
      if (frame.actor_id !== actor.actorId)
        throw new SessionClientError("protocol", "Event actor mismatch");
      if (!actor.desired || frame.seq <= actor.cursor) return;
      if (frame.seq !== actor.cursor + 1) {
        this.drop(new SessionClientError("gap", "Replay sequence gap"), true);
        return;
      }
      await this.options.onEvent(
        jsonSnapshot(frame as unknown as JsonValue) as EventFrame<E>,
      );
      const previous = actor.cursor;
      actor.cursor = frame.seq;
      try {
        await this.save();
      } catch (error) {
        actor.cursor = previous;
        throw error;
      }
      this.changed();
    } else if (frame.type === "ready") {
      if (!actor.desired) return;
      if (
        frame.actor_id !== actor.actorId ||
        frame.cursor !== actor.cursor ||
        (frame.mode === "write" && actor.actorId !== this.scope.root)
      )
        throw new SessionClientError("protocol", "Invalid replay readiness");
      actor.ready = true;
      actor.active = frame.active;
      actor.writable = frame.mode === "write" && !actor.lost;
      actor.waiters.splice(0).forEach((waiter) => waiter.resolve());
      if (actor.writable) {
        actor.acquiring?.resolve();
        actor.acquiring = undefined;
        const head = this.outbox[0];
        if (head?.status === "held") head.status = "queued";
        this.pump();
      }
      this.changed();
    } else if (frame.type === "writer_lost") {
      actor.ready = actor.writable = false;
      actor.lost = true;
      this.rejectActor(
        actor,
        new SessionClientError(
          "writer_required",
          "Writer replaced; reclaim explicitly",
        ),
      );
      this.hold();
      this.changed();
    } else if (frame.type === "unsubscribed") {
      if (actor.closing) {
        this.finishActor(actor);
      }
    } else if (frame.type === "receipt") {
      const head = this.outbox[0];
      if (
        !head ||
        frame.command_id !== head.commandId ||
        frame.operation !== head.payload.operation ||
        frame.subscription_id !== head.sentId
      )
        return;
      clearTimeout(this.receiptTimer);
      clearTimeout(this.retryTimer);
      this.outbox.shift();
      head.reply?.resolve(
        jsonSnapshot(frame as unknown as JsonValue) as ReceiptFrame,
      );
      await this.save();
      await this.options.onReceipt?.(
        jsonSnapshot(frame as unknown as JsonValue) as ReceiptFrame,
      );
      this.changed();
      this.pump();
    } else if (frame.type === "error") {
      const error = new SessionClientError(
        frame.code,
        frame.message,
        frame.command_id ?? undefined,
        frame.uncertain,
        frame.retryable,
      );
      const head = this.outbox[0];
      if (
        frame.command_id &&
        head?.commandId === frame.command_id &&
        frame.subscription_id === head.sentId
      ) {
        if (frame.code === "writer_required") {
          actor.writable = false;
          actor.lost = true;
          this.hold();
        } else if (frame.retryable || frame.uncertain) this.scheduleRetry(head);
        else {
          clearTimeout(this.receiptTimer);
          clearTimeout(this.retryTimer);
          this.outbox.shift();
          head.reply?.reject(error);
          await this.save();
          this.pump();
        }
      } else if (!frame.command_id) {
        const acquiring = Boolean(actor.acquiring);
        this.rejectActor(actor, error);
        actor.writable = false;
        if (actor.closing || !acquiring) this.finishActor(actor);
        else actor.ready = true;
      }
      this.report(error);
      this.changed();
    }
  }
  private pump(): void {
    if (this.pumping) return;
    const actor = this.actors.get(this.scope.root);
    const head = this.outbox[0];
    if (
      !head ||
      head.status !== "queued" ||
      !actor?.desired ||
      !actor.ready ||
      !actor.writable ||
      !actor.id ||
      this.socket?.readyState !== 1 ||
      this.storageFailed
    )
      return;
    if (head.attempts >= 4) {
      this.pause(head);
      return;
    }
    this.pumping = true;
    const generation = this.generation;
    const subscriptionId = actor.id;
    void (async () => {
      head.attempts++;
      head.status = "waiting";
      await this.save();
      if (
        generation !== this.generation ||
        this.outbox[0] !== head ||
        !actor.writable
      ) {
        head.status = "held";
        return;
      }
      head.sentId = subscriptionId;
      this.receiptTimer = setTimeout(
        () => this.scheduleRetry(head),
        this.timing.receiptTimeoutMs,
      );
      await this.write(
        this.commandFrame(head.commandId, head.payload, subscriptionId),
      );
      this.changed();
    })()
      .catch((error) => {
        if (error instanceof SessionClientError && error.code === "storage")
          this.fault(error);
        else if (generation === this.generation)
          this.drop(
            new SessionClientError(
              "transport",
              "Command delivery uncertain",
              head.commandId,
              true,
              true,
            ),
            true,
          );
      })
      .finally(() => {
        this.pumping = false;
        this.pump();
      });
  }
  private scheduleRetry(head: Pending): void {
    if (this.outbox[0] !== head || head.status === "manual") return;
    clearTimeout(this.receiptTimer);
    clearTimeout(this.retryTimer);
    if (head.attempts >= 4) {
      this.pause(head);
      return;
    }
    head.status = "retry";
    this.retryTimer = setTimeout(
      () => {
        if (this.outbox[0] === head && head.status === "retry") {
          head.status = "queued";
          this.pump();
          this.changed();
        }
      },
      this.timing.retryDelaysMs[Math.max(0, head.attempts - 1)]!,
    );
    this.changed();
  }
  private pause(head: Pending): void {
    head.status = "manual";
    const error = new SessionClientError(
      "unknown_outcome",
      "Automatic retries exhausted; retry the same command explicitly",
      head.commandId,
      true,
    );
    head.reply?.reject(error);
    head.reply = undefined;
    this.report(error);
    void this.save().catch((failure) => this.fault(failure));
    this.changed();
  }

  private snapshot(): PersistedState {
    return {
      version: 1,
      scope: { ...this.scope },
      actors: Object.fromEntries(
        [...this.actors.values()]
          .filter((actor) => actor.desired)
          .map((actor) => [actor.actorId, actor.cursor]),
      ),
      commands: this.outbox.map(({ commandId, payload, attempts, status }) => ({
        commandId,
        payload: jsonSnapshot(
          payload as unknown as JsonValue,
        ) as unknown as CommandPayload,
        attempts,
        manual: status === "manual",
      })),
    };
  }
  private async save(): Promise<void> {
    if (!this.options.persistence) return;
    const snapshot = this.snapshot();
    const save = this.saving.then(async () => {
      if (this.storageFailed)
        throw new SessionClientError(
          "storage",
          "Persistence requires manual recovery",
        );
      try {
        await this.options.persistence!.save(snapshot);
      } catch {
        this.storageFailed = true;
        throw new SessionClientError("storage", "Persistence save failed");
      }
    });
    this.saving = save.catch(() => {});
    await save;
  }
  private async load(): Promise<void> {
    if (this.loaded) return;
    if (this.loading) return this.loading;
    const task = (async () => {
      if (this.options.persistence) {
        const saved = await this.options.persistence.load();
        if (saved !== null && saved !== undefined) this.restore(saved);
      }
      this.loaded = true;
    })();
    this.loading = task;
    try {
      await task;
    } catch {
      this.phase = "blocked";
      this.report(
        new SessionClientError(
          "storage",
          "Invalid or unavailable persisted state",
        ),
      );
      throw new SessionClientError(
        "storage",
        "Invalid or unavailable persisted state",
      );
    } finally {
      this.loading = undefined;
    }
  }
  private restore(value: unknown): void {
    const saved = jsonSnapshot(value) as unknown as PersistedState;
    if (
      saved.version !== 1 ||
      !saved.scope ||
      saved.scope.server !== this.scope.server ||
      saved.scope.root !== this.scope.root ||
      saved.scope.identity !== this.scope.identity ||
      Object.keys(saved).sort().join() !== "actors,commands,scope,version" ||
      Object.keys(saved.scope).sort().join() !== "identity,root,server" ||
      !saved.actors ||
      Array.isArray(saved.actors) ||
      !Array.isArray(saved.commands) ||
      Object.keys(saved.actors).length > 8 ||
      saved.commands.length > 64
    )
      throw new TypeError("Invalid persistence scope/state");
    const pending: Pending[] = [];
    let bytes = 0;
    for (const item of saved.commands) {
      const id = canonicalUuid(item.commandId);
      if (
        Object.keys(item).sort().join() !==
          "attempts,commandId,manual,payload" ||
        typeof item.manual !== "boolean" ||
        !Number.isInteger(item.attempts) ||
        item.attempts < 0 ||
        item.attempts > 4 ||
        pending.some((other) => other.commandId === id)
      )
        throw new TypeError("Invalid persisted command");
      const payload = item.payload;
      const keys = Object.keys(payload).sort().join();
      if (payload.operation === "send" && keys === "input,operation")
        jsonSnapshot(payload.input);
      else if (
        payload.operation === "answer" &&
        keys === "askId,operation,response"
      ) {
        canonicalUuid(payload.askId);
        validateAnswer(payload.response);
      } else if (!(payload.operation === "cancel" && keys === "operation"))
        throw new TypeError("Invalid persisted payload");
      const size = jsonBytes(
        this.commandFrame(id, payload, this.scope.root),
      ).byteLength;
      bytes += size;
      if (size > 256 * 1024 || bytes > 1024 * 1024)
        throw new TypeError("Persisted outbox too large");
      pending.push({
        commandId: id,
        payload,
        bytes: size,
        attempts: item.attempts,
        status: item.manual || item.attempts >= 4 ? "manual" : "held",
      });
    }
    const actors = Object.entries(saved.actors).map(([id, position]) => ({
      id: canonicalUuid(id),
      position: cursor(position),
    }));
    if (pending.length && !actors.some((actor) => actor.id === this.scope.root))
      throw new TypeError("Persisted outbox has no root subscription");
    if (this.outbox.length)
      throw new TypeError("Load persistence before enqueueing commands");
    for (const { id } of actors)
      if (!this.actors.get(id)?.desired) this.subscribe(id);
    this.outbox.push(...pending);
    this.changed();
  }
}
