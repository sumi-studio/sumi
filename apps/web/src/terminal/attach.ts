import type { TerminalScope } from "./api";
import {
  decodeBase64,
  encodeBase64,
  parseTerminalSession,
  type TerminalInputReceipt,
  type TerminalSession,
} from "./model";

/**
 * Live attach to one shared terminal session over `/terminal/ws`.
 *
 * The socket is a viewer: opening it replays retained output from an
 * absolute cursor, and closing it NEVER closes the session. Reconnects
 * resume from the furthest cursor actually rendered, so output is
 * neither lost nor duplicated. Bytes are only sent while the socket is
 * open — a send that cannot be placed is reported to the caller, not
 * silently buffered.
 */

export type TerminalAttachState = "connecting" | "open" | "closed";

export interface TerminalEndedInfo {
  status: string;
  reason: string;
  exitCode: number | null;
  exitSignal: string | null;
}

export interface TerminalAttachEvents {
  onSession(session: TerminalSession): void;
  onOutput(base: number, data: Uint8Array): void;
  /** Retained output is missing; the stream skipped [base, to). */
  onGap(base: number, to: number | null): void;
  onInputAck(ack: TerminalInputReceipt): void;
  onEnded(info: TerminalEndedInfo): void;
  onServerError(code: string, message: string): void;
  onConnection(
    state: TerminalAttachState,
    info?: { willRetry?: boolean; reason?: string },
  ): void;
}

/** Minimal socket surface so tests can drive the attach deterministically. */
export interface TerminalSocketLike {
  readonly readyState: number;
  onopen: (() => void) | null;
  onclose: ((event: { code: number; reason?: string }) => void) | null;
  onerror: (() => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  send(payload: string): void;
  close(code?: number, reason?: string): void;
}

const SOCKET_OPEN = 1;
const CLOSE_POLICY_VIOLATION = 1008;

const ENV = (
  import.meta as ImportMeta & { env?: Record<string, string | undefined> }
).env;

/** Exported so the URL contract is regression-testable. */
export function resolveTerminalWsURL({
  apiBaseURL,
  authMode,
  scope,
  sessionId,
  cursor,
  pageOrigin,
}: {
  apiBaseURL?: string;
  authMode?: string;
  scope: TerminalScope;
  sessionId: string;
  cursor: number;
  pageOrigin?: string;
}): URL {
  if (!pageOrigin) throw new Error("terminal page origin is unavailable");
  const pageURL = new URL(pageOrigin);
  const configuredBase = apiBaseURL?.trim();
  const apiURL = configuredBase ? new URL(configuredBase, pageURL) : pageURL;
  if (
    apiURL.pathname !== "/" ||
    apiURL.search ||
    apiURL.hash ||
    apiURL.username ||
    apiURL.password
  ) {
    throw new Error("terminal API base URL must contain only an origin");
  }
  // Session-cookie authentication and the WebSocket upgrade are one
  // same-origin browser contract. A cross-origin fixture is permitted only
  // in explicit preissued mode, which bypasses browser session auth.
  if (apiURL.origin !== pageURL.origin && authMode !== "preissued") {
    throw new Error(
      "cross-origin terminal is unavailable outside preissued E2E mode",
    );
  }
  const url = new URL("/terminal/ws", apiURL);
  url.searchParams.set("installation_id", scope.installationId);
  url.searchParams.set("authority_epoch", scope.authorityEpoch);
  url.searchParams.set("session_id", sessionId);
  if (cursor > 0) url.searchParams.set("cursor", String(cursor));
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url;
}

const RECONNECT_DELAYS_MS = [400, 800, 1600, 3200, 6400, 10_000] as const;

export interface TerminalAttachOptions {
  scope: TerminalScope;
  sessionId: string;
  events: TerminalAttachEvents;
  socketFactory?: (url: string) => TerminalSocketLike;
  maxReconnectAttempts?: number;
}

export class TerminalAttach {
  private readonly options: TerminalAttachOptions;
  private socket: TerminalSocketLike | null = null;
  /** Furthest absolute output offset handed to the UI. */
  private nextCursor = 0;
  private attempts = 0;
  private ended = false;
  private detached = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: TerminalAttachOptions) {
    this.options = options;
  }

  get cursor(): number {
    return this.nextCursor;
  }

  get isEnded(): boolean {
    return this.ended;
  }

  open(): void {
    this.detached = false;
    this.connect();
  }

  /** Viewer detach — intentionally does not end the session. */
  detach(): void {
    this.detached = true;
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    const socket = this.socket;
    this.socket = null;
    socket?.close(1000, "viewer detached");
  }

  private connect(): void {
    const { scope, sessionId, socketFactory, events } = this.options;
    events.onConnection("connecting");
    let url: URL;
    try {
      url = resolveTerminalWsURL({
        apiBaseURL: ENV?.VITE_API_BASE_URL,
        authMode: ENV?.VITE_SUMI_AUTH_MODE,
        scope,
        sessionId,
        cursor: this.nextCursor,
        pageOrigin: globalThis.location?.origin,
      });
    } catch {
      events.onConnection("closed", {
        willRetry: false,
        reason: "socket_error",
      });
      return;
    }
    let socket: TerminalSocketLike;
    try {
      socket = socketFactory
        ? socketFactory(url.toString())
        : (new WebSocket(url) as unknown as TerminalSocketLike);
    } catch {
      events.onConnection("closed", { reason: "socket_error" });
      this.scheduleReconnect();
      return;
    }
    this.socket = socket;

    socket.onopen = () => {
      this.attempts = 0;
      events.onConnection("open");
    };
    socket.onmessage = (event) => this.handleMessage(event.data);
    socket.onerror = () => {
      // onclose follows; nothing meaningful to report here.
    };
    socket.onclose = (event) => {
      if (this.socket === socket) this.socket = null;
      if (this.ended || this.detached) {
        events.onConnection("closed");
        return;
      }
      if (event.code === CLOSE_POLICY_VIOLATION) {
        // Authorization (installation/authority) was rejected; retrying
        // the same scope cannot succeed until the binding is refreshed.
        events.onConnection("closed", {
          willRetry: false,
          reason: "authorization",
        });
        return;
      }
      this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    const { events, maxReconnectAttempts } = this.options;
    if (this.ended || this.detached) return;
    if (this.attempts >= (maxReconnectAttempts ?? Number.POSITIVE_INFINITY)) {
      events.onConnection("closed", { willRetry: false, reason: "retries" });
      return;
    }
    const index = Math.min(this.attempts, RECONNECT_DELAYS_MS.length - 1);
    const delay = RECONNECT_DELAYS_MS[index];
    this.attempts += 1;
    events.onConnection("closed", { willRetry: true });
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  private handleMessage(raw: unknown): void {
    if (typeof raw !== "string") return;
    let frame: Record<string, unknown>;
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed !== "object" || parsed === null) return;
      frame = parsed as Record<string, unknown>;
    } catch {
      return;
    }
    const { events } = this.options;
    try {
      switch (frame.type) {
        case "session": {
          events.onSession(parseTerminalSession(frame.session));
          break;
        }
        case "output": {
          const base = Number(frame.base);
          const data = decodeBase64(String(frame.data ?? ""));
          if (!Number.isSafeInteger(base) || base < 0) return;
          // Overlap after a cursor-resume is trimmed so nothing renders twice.
          const overlap = Math.max(0, this.nextCursor - base);
          const visible =
            overlap >= data.length ? null : data.subarray(overlap);
          const end = base + data.length;
          if (visible && visible.length > 0)
            events.onOutput(base + overlap, visible);
          if (end > this.nextCursor) this.nextCursor = end;
          break;
        }
        case "gap": {
          const base = Number(frame.base);
          const to = Number.isSafeInteger(frame.to) ? Number(frame.to) : null;
          if (!Number.isSafeInteger(base)) return;
          events.onGap(base, to);
          if (to !== null && to > this.nextCursor) this.nextCursor = to;
          break;
        }
        case "input_ack": {
          events.onInputAck({
            inputId: String(frame.input_id ?? ""),
            seq: Number.isSafeInteger(frame.seq) ? Number(frame.seq) : 0,
            status: String(frame.status ?? "unknown"),
          } satisfies TerminalInputReceipt);
          break;
        }
        case "ended": {
          this.ended = true;
          events.onEnded({
            status: String(frame.status ?? "ended"),
            reason: String(frame.reason ?? ""),
            exitCode: Number.isSafeInteger(frame.exit_code)
              ? Number(frame.exit_code)
              : null,
            exitSignal:
              typeof frame.exit_signal === "string" ? frame.exit_signal : null,
          });
          break;
        }
        case "error": {
          events.onServerError(
            String(frame.code ?? "error"),
            String(frame.message ?? ""),
          );
          break;
        }
        default:
          break;
      }
    } catch {
      // A malformed frame must not wedge the socket; skip it.
    }
  }

  /** Returns false when the socket is not open — the caller must surface it. */
  private send(frame: Record<string, unknown>): boolean {
    const socket = this.socket;
    if (!socket || socket.readyState !== SOCKET_OPEN || this.ended) {
      return false;
    }
    socket.send(JSON.stringify(frame));
    return true;
  }

  sendStdin(text: string): boolean {
    return this.send({
      type: "stdin",
      data: encodeBase64(new TextEncoder().encode(text)),
    });
  }

  sendStdinBytes(bytes: Uint8Array): boolean {
    return this.send({ type: "stdin", data: encodeBase64(bytes) });
  }

  sendResize(cols: number, rows: number): boolean {
    if (
      !Number.isSafeInteger(cols) ||
      !Number.isSafeInteger(rows) ||
      cols <= 0 ||
      rows <= 0
    ) {
      return false;
    }
    return this.send({ type: "resize", cols, rows });
  }

  sendSignal(signal: string): boolean {
    return this.send({ type: "signal", signal });
  }

  sendEof(): boolean {
    return this.send({ type: "eof" });
  }

  setControl(hold: boolean): boolean {
    return this.send({ type: "control", hold });
  }

  /** Asks the session to end. Distinct from detach(); this is explicit. */
  requestClose(): boolean {
    return this.send({ type: "close" });
  }
}
