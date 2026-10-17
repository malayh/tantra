import {
  INTERNAL_ERROR,
  METHOD_NOT_FOUND,
  Peer,
  RpcError,
  type Reply,
} from "./jsonrpc.js";
import {
  MAX_FRAME,
  PROFILE_VERSION,
  PROTOCOL_VERSION,
  cursorOf,
  errorCode,
  isAfter,
  type ConfigOption,
  type ContentBlock,
  type ElicitationParams,
  type ElicitationResult,
  type EmptyResult,
  type InitializeResult,
  type JsonObject,
  type ListSessionsResult,
  type Mode,
  type NewSessionResult,
  type PermissionResult,
  type PromptResult,
  type RequestPermissionParams,
  type ResumeSessionResult,
  type ServerErrorCode,
  type SessionNotification,
  type SessionUpdate,
} from "./protocol.js";

export const VERSION = "2.0.1";

export type ErrorCode =
  | ServerErrorCode
  | "uncertain"
  | "closed"
  | "too_large"
  | "profile_unsupported"
  | "handler_failed"
  | "dial_failed";

export interface ErrorDetails {
  rpcCode?: number;
  sessionId?: string;
  commandId?: string;
  cause?: unknown;
}

export class TantraAcpError extends Error {
  declare readonly rpcCode?: number;
  declare readonly sessionId?: string;
  declare readonly commandId?: string;

  constructor(
    readonly code: ErrorCode,
    message: string,
    details: ErrorDetails = {},
  ) {
    const { cause, ...rest } = details;
    super(message, cause === undefined ? undefined : { cause });
    this.name = "TantraAcpError";
    Object.assign(this, rest);
  }
}

export type ConnectionState = "closed" | "connecting" | "open" | "failed";

export interface SessionState {
  writer: boolean;
  cursor: string | null;
  paused: boolean;
}

export interface ClientState {
  connection: ConnectionState;
  sessions: Record<string, SessionState>;
}

export type SocketLike = Pick<
  WebSocket,
  "readyState" | "send" | "close" | "addEventListener"
>;

export interface ClientTiming {
  reconnectBaseMs?: number;
  reconnectCapMs?: number;
  reconnectStableMs?: number;
}

interface RequestBase {
  sessionId: string;
  signal: AbortSignal;
}

export interface PermissionRequest extends RequestBase {
  method: "session/request_permission";
  params: RequestPermissionParams;
  respond(result: PermissionResult): void;
}

export interface ElicitationRequest extends RequestBase {
  method: "elicitation/create";
  params: ElicitationParams;
  respond(result: ElicitationResult): void;
}

export type AgentRequest = PermissionRequest | ElicitationRequest;

export interface TantraAcpClientOptions {
  url: () => string | Promise<string>;
  cwd?: string;
  onUpdate: (sessionId: string, update: SessionUpdate) => void | Promise<void>;
  onRequest?: (request: AgentRequest) => void | Promise<void>;
  onState?: (state: ClientState) => void;
  onError?: (error: TantraAcpError) => void;
  webSocketFactory?: (url: string) => SocketLike;
  timing?: ClientTiming;
}

export type PromptPromise = Promise<PromptResult> & {
  readonly commandId: string;
};

interface Entry {
  commandId: string;
  params: JsonObject;
  sent: boolean;
  resolve(result: PromptResult): void;
  reject(error: TantraAcpError): void;
}

interface Session {
  writer: boolean;
  cursor: string | null;
  paused: boolean;
  busy: boolean;
  outbox: Entry[];
}

const MAX_DIALS = 10;
const encoder = new TextEncoder();
const bytes = (text: string): number => encoder.encode(text).byteLength;
const closed = (message = "Not connected", details: ErrorDetails = {}) =>
  new TantraAcpError("closed", message, details);
const isClosed = (error: unknown): boolean =>
  error instanceof TantraAcpError && error.code === "closed";
const session = (writer: boolean): Session => ({
  writer,
  cursor: null,
  paused: false,
  busy: false,
  outbox: [],
});

export class TantraAcpClient {
  private readonly options: TantraAcpClientOptions;
  private readonly cwd: string;
  private readonly baseMs: number;
  private readonly capMs: number;
  private readonly stableMs: number;
  private readonly sessions = new Map<string, Session>();
  private readonly parents = new Map<string, string>();
  private readonly pendingResumes = new Map<string, number>();
  private readonly deferredClose = new Set<string>();
  private connection: ConnectionState = "closed";
  private socket: SocketLike | null = null;
  private peer: Peer | null = null;
  private chain: Promise<void> = Promise.resolve();
  private generation = 0;
  private epoch = 0;
  private loop: Promise<void> | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private wake: ((error: unknown) => void) | undefined;
  private halt: TantraAcpError = closed("Disconnected");
  private failures = 0;
  private waits = -1;
  private openedAt = 0;

  constructor(options: TantraAcpClientOptions) {
    this.options = options;
    this.cwd = options.cwd ?? "/";
    this.baseMs = options.timing?.reconnectBaseMs ?? 1_000;
    this.capMs = options.timing?.reconnectCapMs ?? 30_000;
    this.stableMs = options.timing?.reconnectStableMs ?? 10_000;
  }

  connect(): Promise<void> {
    if (this.connection === "open") return Promise.resolve();
    return this.loop ?? this.start(false);
  }

  async disconnect(): Promise<void> {
    this.shutdown(closed("Disconnected"));
    this.setConnection("closed");
  }

  newSession(): Promise<NewSessionResult> {
    return this.invoke<NewSessionResult>(
      "session/new",
      { cwd: this.cwd },
      (result) => {
        this.sessions.set(result.sessionId, session(true));
        this.emit();
      },
    );
  }

  listSessions(cursor?: string): Promise<ListSessionsResult> {
    return this.invoke("session/list", cursor === undefined ? {} : { cursor });
  }

  resume(
    sessionId: string,
    options: { mode?: Mode; after?: string } = {},
  ): Promise<ResumeSessionResult> {
    if (this.connection !== "open") return Promise.reject(closed());
    const existing = this.sessions.has(sessionId);
    if (!existing && options.after !== undefined)
      this.sessions.set(sessionId, {
        ...session(false),
        cursor: options.after,
      });
    const opening = this.open(sessionId, options.mode ?? "read", true);
    const target = this.sessions.get(sessionId);
    const generation = this.generation;
    this.pendingResumes.set(
      sessionId,
      (this.pendingResumes.get(sessionId) ?? 0) + 1,
    );
    return opening.then(
      (result) => {
        this.resumed(sessionId, generation);
        if (this.sessions.get(sessionId) !== target)
          throw closed("Session closed", { sessionId });
        return result;
      },
      (error: unknown) => {
        this.resumed(sessionId, generation);
        if (this.sessions.get(sessionId) === target) {
          if (existing) this.detach(sessionId);
          else {
            this.drop(sessionId, false);
            if (this.pendingResumes.has(sessionId))
              this.deferredClose.add(sessionId);
          }
        }
        throw error;
      },
    );
  }

  close(sessionId: string): Promise<EmptyResult> {
    return this.detach(sessionId);
  }

  prompt(
    sessionId: string,
    blocks: ContentBlock[],
    options: { commandId?: string } = {},
  ): PromptPromise {
    const commandId = options.commandId ?? crypto.randomUUID();
    let resolve!: (result: PromptResult) => void;
    let reject!: (error: TantraAcpError) => void;
    const promise = new Promise<PromptResult>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    void promise.catch(() => {});
    const params = JSON.parse(
      JSON.stringify({
        sessionId,
        prompt: blocks,
        _meta: { tantra: { commandId } },
      }),
    ) as JsonObject;
    const frame = JSON.stringify({
      jsonrpc: "2.0",
      id: Number.MAX_SAFE_INTEGER,
      method: "session/prompt",
      params,
    });
    const target = this.sessions.get(sessionId);
    const details = { sessionId, commandId };
    if (!target || this.connection === "closed" || this.connection === "failed")
      reject(closed("Session is not open", details));
    else if (bytes(frame) > MAX_FRAME)
      reject(new TantraAcpError("too_large", "Prompt exceeds 1 MiB", details));
    else {
      target.outbox.push({ commandId, params, sent: false, resolve, reject });
      this.pump(sessionId);
    }
    return Object.assign(promise, { commandId });
  }

  async cancel(
    sessionId: string,
    options: { commandId?: string } = {},
  ): Promise<void> {
    const peer = this.peer;
    if (this.connection !== "open" || !peer || !this.sessions.has(sessionId))
      throw closed("Session is not open", { sessionId });
    const commandId = options.commandId ?? crypto.randomUUID();
    peer.notify("session/cancel", {
      sessionId,
      _meta: { tantra: { commandId } },
    });
  }

  claimWriter(sessionId: string): Promise<EmptyResult> {
    return this.invoke("_tantra/session/claim_writer", { sessionId }, () =>
      this.claimed(sessionId),
    );
  }

  async setModel(sessionId: string, model: string): Promise<ConfigOption[]> {
    const result = await this.invoke<{ configOptions: ConfigOption[] }>(
      "session/set_config_option",
      { sessionId, configId: "model", type: "id", value: model },
    );
    return result.configOptions;
  }

  delete(sessionId: string): Promise<EmptyResult> {
    return this.invoke("session/delete", { sessionId }, () =>
      this.drop(sessionId, true),
    );
  }

  private start(reconnecting: boolean): Promise<void> {
    const loop = this.run(this.epoch, reconnecting);
    const clear = () => {
      if (this.loop === loop) this.loop = null;
    };
    this.loop = loop;
    loop.then(clear, clear);
    return loop;
  }

  private async run(epoch: number, reconnecting: boolean): Promise<void> {
    this.setConnection("connecting");
    if (!reconnecting) {
      this.failures = 0;
      this.waits = -1;
    }
    for (;;) {
      try {
        if (this.waits >= 0) await this.backoff(this.waits);
        this.waits++;
        await this.dial(epoch);
        await this.resumeAll();
        this.current(epoch);
        if (!this.peer) throw closed("Connection lost");
        this.openedAt = globalThis.performance.now();
        this.setConnection("open");
        for (const sessionId of [...this.sessions.keys()]) this.pump(sessionId);
        return;
      } catch (error) {
        if (epoch !== this.epoch) throw this.halt;
        this.retire(closed("Connection lost"));
        if (++this.failures === MAX_DIALS)
          throw this.fail(
            new TantraAcpError("dial_failed", "Could not connect", {
              cause: error,
            }),
          );
      }
    }
  }

  private backoff(waits: number): Promise<void> {
    const ceiling = Math.min(this.capMs, this.baseMs * 2 ** waits);
    const delay = ceiling * (0.5 + Math.random() * 0.5);
    return new Promise((resolve, reject) => {
      this.wake = reject;
      this.timer = setTimeout(() => {
        this.wake = undefined;
        resolve();
      }, delay);
    });
  }

  private current(epoch: number): void {
    if (epoch !== this.epoch) throw closed("Disconnected");
  }

  private async dial(epoch: number): Promise<void> {
    const url = await this.options.url();
    this.current(epoch);
    await this.attach(url);
    this.current(epoch);
    const result = await this.rpc<InitializeResult>("initialize", {
      protocolVersion: PROTOCOL_VERSION,
      info: { name: "tantra-acp-client", version: VERSION },
      capabilities: {
        elicitation: { form: {} },
        _meta: { tantra: { version: PROFILE_VERSION } },
      },
    });
    this.current(epoch);
    if (result?.capabilities?._meta?.tantra?.version !== PROFILE_VERSION)
      throw this.fail(
        new TantraAcpError(
          "profile_unsupported",
          "Agent does not support the tantra profile",
        ),
      );
  }

  private attach(url: string): Promise<void> {
    const generation = ++this.generation;
    const socket = this.options.webSocketFactory?.(url) ?? new WebSocket(url);
    const peer = new Peer((frame) => this.write(socket, frame), {
      notifications: {
        "session/update": (params) =>
          this.update(params as SessionNotification),
      },
      requests: {
        "session/request_permission": (params, reply) =>
          this.request("session/request_permission", params, reply),
        "elicitation/create": (params, reply) =>
          this.request("elicitation/create", params, reply),
      },
    });
    this.socket = socket;
    this.peer = peer;
    return new Promise((resolve, reject) => {
      let opened = false;
      socket.addEventListener("open", () => {
        opened = true;
        resolve();
      });
      socket.addEventListener("message", (event) => {
        const data: unknown = event.data;
        if (typeof data === "string")
          this.enqueue(generation, () => peer.receive(data));
      });
      socket.addEventListener("close", () => {
        if (opened) this.enqueue(generation, () => this.lost());
        else reject(new TantraAcpError("dial_failed", "WebSocket closed"));
      });
    });
  }

  private async resumeAll(): Promise<void> {
    const resumed: [string, Session][] = [];
    for (const [sessionId, entry] of [...this.sessions]) {
      if (this.sessions.get(sessionId) !== entry) continue;
      try {
        await this.open(sessionId, "read", false);
        resumed.push([sessionId, entry]);
      } catch (error) {
        if (!(error instanceof TantraAcpError) || error.rpcCode !== -32002)
          throw error;
        this.drop(sessionId, true);
        this.report(error);
      }
    }
    for (const [sessionId, entry] of resumed)
      if (this.sessions.get(sessionId) !== entry)
        this.rpc("session/close", { sessionId }).catch(() => {});
  }

  private open(
    sessionId: string,
    mode: Mode,
    replace: boolean,
  ): Promise<ResumeSessionResult> {
    const existing = this.sessions.get(sessionId);
    const target = existing ?? session(false);
    this.sessions.set(sessionId, target);
    const params: JsonObject = {
      sessionId,
      cwd: this.cwd,
      replayFrom:
        target.cursor === null
          ? { type: "start" }
          : { type: "_tantra_after", cursor: target.cursor },
    };
    if (mode === "write") params["_meta"] = { tantra: { mode } };
    return this.rpc<ResumeSessionResult>("session/resume", params, () => {
      if (this.sessions.get(sessionId) !== target) return;
      if (replace && !this.parents.has(sessionId))
        for (const child of this.descendants(sessionId))
          this.drop(child, false);
      target.writer = mode === "write";
      if (target.writer) target.paused = false;
      this.emit();
      this.pump(sessionId);
    });
  }

  private enqueue(generation: number, task: () => unknown): void {
    this.chain = this.chain.then(async () => {
      if (generation !== this.generation) return;
      try {
        await task();
      } catch (error) {
        if (generation === this.generation)
          this.fail(
            new TantraAcpError("handler_failed", "Handler failed", {
              cause: error,
            }),
          );
      }
    });
  }

  private async update({ sessionId, update }: SessionNotification) {
    const target = this.sessions.get(sessionId);
    const cursor = cursorOf(update);
    if (
      cursor !== undefined &&
      target?.cursor &&
      !isAfter(cursor, target.cursor)
    )
      return;
    if (update.sessionUpdate === "subagent_update")
      this.parents.set(update.sessionId, sessionId);
    if (target && update.sessionUpdate === "_tantra_writer") {
      if (update.writer) this.claimed(sessionId);
      else {
        target.writer = false;
        this.emit();
      }
    }
    if (
      target &&
      update.sessionUpdate === "_tantra_notice" &&
      update.code === "session_deleted"
    ) {
      this.drop(sessionId, true);
      this.report(
        new TantraAcpError("not_found", "Session deleted", { sessionId }),
      );
    }
    await this.options.onUpdate(sessionId, update);
    if (
      target &&
      cursor !== undefined &&
      this.sessions.get(sessionId) === target
    ) {
      target.cursor = cursor;
      this.emit();
    }
  }

  private request(
    method: AgentRequest["method"],
    params: unknown,
    reply: Reply,
  ): void {
    const handler = this.options.onRequest;
    const sessionId = (params as { sessionId: string }).sessionId;
    const refuse = (code: number) => {
      try {
        reply.error(code);
      } catch {}
    };
    if (!handler) {
      refuse(METHOD_NOT_FOUND);
      return;
    }
    const failed = (error: unknown) => {
      this.report(
        new TantraAcpError("handler_failed", "Request handler failed", {
          sessionId,
          cause: error,
        }),
      );
      refuse(INTERNAL_ERROR);
    };
    const respond = (result: unknown) => {
      if (reply.signal.aborted) return;
      try {
        reply.result(result);
      } catch (error) {
        if (!isClosed(error)) throw error;
      }
    };
    try {
      const request = { method, sessionId, params, signal: reply.signal };
      Promise.resolve(handler({ ...request, respond } as AgentRequest)).catch(
        failed,
      );
    } catch (error) {
      failed(error);
    }
  }

  private claimed(sessionId: string): void {
    const target = this.sessions.get(sessionId);
    if (!target) return;
    target.writer = true;
    target.paused = false;
    this.emit();
    this.pump(sessionId);
  }

  private pump(sessionId: string): void {
    const target = this.sessions.get(sessionId);
    const entry = target?.outbox[0];
    const peer = this.peer;
    if (!target || !entry || !peer || target.busy || target.paused) return;
    if (this.connection !== "open") return;
    const details = { sessionId, commandId: entry.commandId };
    const retry = entry.sent;
    const settle = (finish: () => void) => {
      if (target.outbox[0] === entry) {
        target.outbox.shift();
        finish();
      }
      target.busy = false;
      this.pump(sessionId);
    };
    target.busy = true;
    try {
      peer
        .request("session/prompt", entry.params, (result) =>
          settle(() => entry.resolve(result as PromptResult)),
        )
        .catch((error: unknown) => {
          const failure = this.failure(error, details);
          if (retry && failure.code === "writer_required") this.hold(target);
          else if (isClosed(failure)) target.busy = false;
          else settle(() => entry.reject(failure));
        });
      entry.sent = true;
    } catch {
      target.busy = false;
    }
  }

  private hold(target: Session): void {
    target.busy = false;
    target.writer = false;
    target.paused = true;
    this.emit();
  }

  private failure(error: unknown, details: ErrorDetails): TantraAcpError {
    if (error instanceof TantraAcpError) return error;
    if (error instanceof RpcError)
      return new TantraAcpError(
        errorCode(error.code, error.data),
        error.message,
        { ...details, rpcCode: error.code },
      );
    return new TantraAcpError("internal", "Request failed", {
      ...details,
      cause: error,
    });
  }

  private rpc<T>(
    method: string,
    params: JsonObject,
    settle?: (result: T) => void,
  ): Promise<T> {
    const peer = this.peer;
    if (!peer) return Promise.reject(closed());
    const sessionId = params["sessionId"];
    const details = typeof sessionId === "string" ? { sessionId } : {};
    try {
      return peer
        .request(method, params, settle as (result: unknown) => void)
        .catch((error: unknown) => {
          throw this.failure(error, details);
        }) as Promise<T>;
    } catch (error) {
      return Promise.reject(this.failure(error, details));
    }
  }

  private invoke<T>(
    method: string,
    params: JsonObject,
    settle?: (result: T) => void,
  ): Promise<T> {
    if (this.connection !== "open") return Promise.reject(closed());
    return this.rpc(method, params, settle);
  }

  private write(socket: SocketLike, frame: string): void {
    if (bytes(frame) > MAX_FRAME)
      throw new TantraAcpError("too_large", "Frame exceeds 1 MiB");
    if (socket !== this.socket || socket.readyState !== 1) throw closed();
    socket.send(frame);
  }

  private lost(): void {
    for (const target of this.sessions.values()) {
      if (target.writer) target.paused = true;
      target.writer = false;
    }
    const reason = closed("Connection lost");
    this.retire(reason);
    if (this.connection === "open") {
      if (globalThis.performance.now() - this.openedAt >= this.stableMs) {
        this.failures = 0;
        this.waits = 0;
      } else if (++this.failures === MAX_DIALS) {
        this.fail(
          new TantraAcpError("dial_failed", "Could not connect", {
            cause: reason,
          }),
        );
        return;
      }
      void this.start(true).catch(() => {});
    }
    this.emit();
  }

  private retire(reason: TantraAcpError): void {
    this.generation++;
    const socket = this.socket;
    const peer = this.peer;
    this.socket = null;
    this.peer = null;
    this.chain = Promise.resolve();
    this.pendingResumes.clear();
    this.deferredClose.clear();
    peer?.close(reason);
    if (socket && socket.readyState < 2) socket.close(1000);
  }

  private shutdown(reason: TantraAcpError, halt = reason): void {
    this.halt = halt;
    this.epoch++;
    this.loop = null;
    clearTimeout(this.timer);
    this.wake?.(reason);
    this.wake = undefined;
    for (const [sessionId, target] of this.sessions) {
      target.writer = false;
      target.paused = false;
      this.reject(sessionId, target);
    }
    this.retire(reason);
  }

  private fail(error: TantraAcpError): TantraAcpError {
    this.shutdown(closed("Connection failed"), error);
    this.setConnection("failed");
    this.report(error);
    return error;
  }

  private detach(sessionId: string): Promise<EmptyResult> {
    this.drop(sessionId, !this.parents.has(sessionId));
    if (this.connection !== "open") return Promise.resolve({});
    if (this.pendingResumes.has(sessionId)) {
      this.deferredClose.add(sessionId);
      return Promise.resolve({});
    }
    const closing = this.rpc<EmptyResult>("session/close", { sessionId });
    closing.catch(() => {});
    return closing;
  }

  private resumed(sessionId: string, generation: number): void {
    if (generation !== this.generation) return;
    const pending = (this.pendingResumes.get(sessionId) ?? 1) - 1;
    if (pending > 0) {
      this.pendingResumes.set(sessionId, pending);
      return;
    }
    this.pendingResumes.delete(sessionId);
    if (this.deferredClose.delete(sessionId) && !this.sessions.has(sessionId))
      this.rpc("session/close", { sessionId }).catch(() => {});
  }

  private drop(sessionId: string, descendants: boolean): void {
    const tree = descendants ? this.under(sessionId, this.parents.keys()) : [];
    for (const id of [sessionId, ...tree]) {
      const target = this.sessions.get(id);
      if (!target) continue;
      this.sessions.delete(id);
      this.reject(id, target);
    }
    for (const id of tree) this.parents.delete(id);
    this.emit();
  }

  private descendants(rootId: string): string[] {
    return this.under(rootId, this.sessions.keys());
  }

  private under(rootId: string, ids: Iterable<string>): string[] {
    return [...ids].filter((id) => {
      for (let up = this.parents.get(id); up; up = this.parents.get(up))
        if (up === rootId) return true;
      return false;
    });
  }

  private reject(sessionId: string, target: Session): void {
    for (const entry of target.outbox.splice(0)) {
      const details = { sessionId, commandId: entry.commandId };
      entry.reject(
        entry.sent
          ? new TantraAcpError("uncertain", "Prompt outcome unknown", details)
          : closed("Session closed", details),
      );
    }
  }

  private setConnection(state: ConnectionState): void {
    if (this.connection === state) return;
    this.connection = state;
    this.emit();
  }

  private emit(): void {
    const sessions: Record<string, SessionState> = {};
    for (const [id, target] of this.sessions)
      sessions[id] = {
        writer: target.writer,
        cursor: target.cursor,
        paused: target.paused,
      };
    try {
      this.options.onState?.({ connection: this.connection, sessions });
    } catch (error) {
      this.report(
        new TantraAcpError("handler_failed", "State handler failed", {
          cause: error,
        }),
      );
    }
  }

  private report(error: TantraAcpError): void {
    try {
      this.options.onError?.(error);
    } catch {}
  }
}
