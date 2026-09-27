/** Minimal flattened-session Chrome DevTools Protocol client over a Workers
 * WebSocket. One connection per remote browser; page targets are reached by
 * their flattened `sessionId`. */

export interface CdpEvent {
  method: string;
  params: Record<string, unknown>;
  sessionId?: string;
}

interface Pending {
  resolve(value: Record<string, unknown>): void;
  reject(error: Error): void;
  timer: ReturnType<typeof setTimeout>;
  method: string;
}

export interface SocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "close", listener: (event: { code?: number }) => void): void;
  addEventListener(type: "error", listener: () => void): void;
}

export class CdpError extends Error {
  readonly method: string;
  constructor(method: string, message: string) {
    super(`${method}: ${message}`);
    this.method = method;
    this.name = "CdpError";
  }
}

export class Cdp {
  private nextId = 0;
  private readonly pending = new Map<number, Pending>();
  closed = false;
  private readonly ws: SocketLike;
  private readonly handlers: {
    onEvent?: (event: CdpEvent) => void;
    onClose?: (why: string) => void;
  };
  constructor(
    ws: SocketLike,
    handlers: {
      onEvent?: (event: CdpEvent) => void;
      onClose?: (why: string) => void;
    } = {},
  ) {
    this.ws = ws;
    this.handlers = handlers;
    const decoder = new TextDecoder();
    ws.addEventListener("message", (event) => {
      const text =
        typeof event.data === "string"
          ? event.data
          : decoder.decode(event.data as ArrayBuffer);
      let message: {
        id?: number;
        result?: Record<string, unknown>;
        error?: { message?: string };
        method?: string;
        params?: Record<string, unknown>;
        sessionId?: string;
      };
      try {
        message = JSON.parse(text);
      } catch {
        return;
      }
      if (message.id !== undefined) {
        const waiting = this.pending.get(message.id);
        if (!waiting) return;
        this.pending.delete(message.id);
        clearTimeout(waiting.timer);
        if (message.error)
          waiting.reject(
            new CdpError(waiting.method, message.error.message ?? "failed"),
          );
        else waiting.resolve(message.result ?? {});
        return;
      }
      if (message.method)
        this.handlers.onEvent?.({
          method: message.method,
          params: message.params ?? {},
          sessionId: message.sessionId,
        });
    });
    const done = (why: string) => {
      if (this.closed) return;
      this.closed = true;
      for (const waiting of this.pending.values()) {
        clearTimeout(waiting.timer);
        waiting.reject(
          new CdpError(waiting.method, `browser connection closed (${why})`),
        );
      }
      this.pending.clear();
      this.handlers.onClose?.(why);
    };
    ws.addEventListener("close", (event) => done(`close ${event.code ?? ""}`));
    ws.addEventListener("error", () => done("error"));
  }

  send<T = Record<string, unknown>>(
    method: string,
    params: Record<string, unknown> = {},
    sessionId?: string,
    timeoutMs = 15_000,
  ): Promise<T> {
    if (this.closed)
      return Promise.reject(new CdpError(method, "browser connection closed"));
    const id = ++this.nextId;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new CdpError(method, "timed out"));
      }, timeoutMs);
      this.pending.set(id, {
        resolve: resolve as (value: Record<string, unknown>) => void,
        reject,
        timer,
        method,
      });
      const message: Record<string, unknown> = { id, method, params };
      if (sessionId) message.sessionId = sessionId;
      try {
        this.ws.send(JSON.stringify(message));
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(
          new CdpError(
            method,
            error instanceof Error ? error.message : "send failed",
          ),
        );
      }
    });
  }

  close(): void {
    try {
      this.ws.close(1000, "done");
    } catch {}
  }
}
