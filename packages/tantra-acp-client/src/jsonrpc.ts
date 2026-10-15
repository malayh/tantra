export const PARSE_ERROR = -32700;
export const METHOD_NOT_FOUND = -32601;
export const INTERNAL_ERROR = -32603;
export const REQUEST_CANCELLED = -32800;
export const CANCEL_REQUEST = "$/cancel_request";

const MESSAGES: Record<number, string> = {
  [PARSE_ERROR]: "Parse error",
  [METHOD_NOT_FOUND]: "Method not found",
  [INTERNAL_ERROR]: "Internal error",
  [REQUEST_CANCELLED]: "Request cancelled",
};

export class RpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
    readonly data?: unknown,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

export interface Reply {
  readonly signal: AbortSignal;
  result(value: unknown): void;
  error(code: number, message?: string): void;
}

export interface Handlers {
  notifications: Record<string, (params: unknown) => void | Promise<void>>;
  requests: Record<
    string,
    (params: unknown, reply: Reply) => void | Promise<void>
  >;
}

interface Pending {
  resolve(value: unknown): void;
  reject(error: unknown): void;
  settle: ((result: unknown) => void) | undefined;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export class Peer {
  private nextId = 0;
  private readonly pending = new Map<number, Pending>();
  private readonly incoming = new Map<unknown, AbortController>();

  constructor(
    private readonly write: (frame: string) => void,
    private readonly handlers: Handlers,
  ) {}

  request(
    method: string,
    params: unknown,
    settle?: (result: unknown) => void,
  ): Promise<unknown> {
    const id = this.nextId++;
    this.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, settle });
    });
  }

  notify(method: string, params: unknown): void {
    this.write(JSON.stringify({ jsonrpc: "2.0", method, params }));
  }

  async receive(text: string): Promise<void> {
    let message: unknown;
    try {
      message = JSON.parse(text);
    } catch {
      this.reply(null, { error: this.error(PARSE_ERROR) });
      return;
    }
    if (!isObject(message)) return;
    const { method, params } = message;
    if (typeof method !== "string") {
      this.resolve(message);
      return;
    }
    if (!("id" in message)) {
      if (method === CANCEL_REQUEST) this.cancel(params);
      else if (Object.hasOwn(this.handlers.notifications, method))
        await this.handlers.notifications[method]!(params);
      return;
    }
    const id = message["id"];
    if (!Object.hasOwn(this.handlers.requests, method)) {
      this.reply(id, { error: this.error(METHOD_NOT_FOUND) });
      return;
    }
    const controller = new AbortController();
    this.incoming.set(id, controller);
    await this.handlers.requests[method]!(params, {
      signal: controller.signal,
      result: (value) => this.answer(id, controller, { result: value }),
      error: (code, text) =>
        this.answer(id, controller, { error: this.error(code, text) }),
    });
  }

  close(error: unknown): void {
    const pending = [...this.pending.values()];
    const incoming = [...this.incoming.values()];
    this.pending.clear();
    this.incoming.clear();
    for (const item of pending) item.reject(error);
    for (const controller of incoming) controller.abort();
  }

  private error(code: number, message?: string): object {
    return { code, message: message ?? MESSAGES[code] ?? "Error" };
  }

  private answer(id: unknown, controller: AbortController, body: object) {
    if (this.incoming.get(id) !== controller) return;
    this.write(JSON.stringify({ jsonrpc: "2.0", id, ...body }));
    this.incoming.delete(id);
  }

  private reply(id: unknown, body: object): void {
    try {
      this.write(JSON.stringify({ jsonrpc: "2.0", id, ...body }));
    } catch {}
  }

  private cancel(params: unknown): void {
    const id = isObject(params) ? params["requestId"] : undefined;
    const controller = this.incoming.get(id);
    if (controller === undefined) return;
    this.incoming.delete(id);
    controller.abort();
    this.reply(id, { error: this.error(REQUEST_CANCELLED) });
  }

  private resolve(message: Record<string, unknown>): void {
    const id = message["id"];
    const pending = typeof id === "number" ? this.pending.get(id) : undefined;
    if (pending === undefined) return;
    this.pending.delete(id as number);
    if ("error" in message) {
      const error = isObject(message["error"]) ? message["error"] : {};
      const code = error["code"];
      const text = error["message"];
      pending.reject(
        new RpcError(
          typeof code === "number" ? code : INTERNAL_ERROR,
          typeof text === "string" ? text : MESSAGES[INTERNAL_ERROR]!,
          error["data"],
        ),
      );
      return;
    }
    pending.settle?.(message["result"]);
    pending.resolve(message["result"]);
  }
}
