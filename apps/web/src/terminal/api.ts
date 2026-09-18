import {
  parseTerminalInputReceipt,
  parseTerminalRead,
  parseTerminalSession,
  type TerminalInputReceipt,
  type TerminalReadResult,
  type TerminalSession,
} from "./model";

const REQUEST_TIMEOUT_MS = 15_000;

/**
 * The exact installation identity every terminal route re-authorizes
 * against (`installation_id` + `authority_epoch` query parameters,
 * verified against the browser session cookie by the API).
 */
export interface TerminalScope {
  installationId: string;
  authorityEpoch: string;
}

export type TerminalErrorCode =
  | "unavailable"
  | "auth"
  | "forbidden"
  | "invalid_scope"
  | "not_found"
  | "ended"
  | "not_live"
  | "control_held"
  | "capacity"
  | "bad_request"
  | "request_failed";

export class TerminalAPIError extends Error {
  readonly code: TerminalErrorCode;
  readonly status: number;

  constructor(code: TerminalErrorCode, status: number) {
    super(code);
    this.name = "TerminalAPIError";
    this.code = code;
    this.status = status;
  }
}

/**
 * No HTTP response arrived, so the mutation may have committed anyway.
 * Callers must reconcile by re-reading state, never by blind retry.
 */
export class TerminalAPIUncertainError extends Error {
  constructor(cause: unknown) {
    super("terminal_request_outcome_uncertain", { cause });
    this.name = "TerminalAPIUncertainError";
  }
}

// The API answers failures with plain-text http.Error bodies; the text
// is the only error detail it publishes, so it is mapped here.
const BODY_ERROR_CODES: Record<string, TerminalErrorCode> = {
  "terminal unavailable": "unavailable",
  "authorization unavailable": "unavailable",
  "terminal backend unavailable": "unavailable",
  "invalid session": "auth",
  "missing session": "auth",
  "origin not allowed": "forbidden",
  "not authorized": "forbidden",
  invalid_scope: "invalid_scope",
  "terminal session not found": "not_found",
  "terminal session has ended": "ended",
  "terminal session is not live": "not_live",
  "terminal control is held": "control_held",
  "too many live terminal sessions": "capacity",
};

function statusFallback(status: number): TerminalErrorCode {
  if (status === 401) return "auth";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 409) return "not_live";
  if (status === 429) return "capacity";
  if (status >= 500) return "unavailable";
  return "request_failed";
}

export type TerminalInputKind = "stdin" | "resize" | "signal" | "eof";

export type TerminalInputPayload =
  | { kind: "stdin"; data: string }
  | { kind: "resize"; cols: number; rows: number }
  | { kind: "signal"; signal: string }
  | { kind: "eof" };

/** Signals the backend admits (terminalSignalAllowlist in agentstate). */
export const TERMINAL_SIGNALS = [
  "INT",
  "TERM",
  "HUP",
  "QUIT",
  "KILL",
  "TSTP",
  "USR1",
  "USR2",
] as const;

type Fetcher = typeof fetch;

/** Same-origin browser-session client for the shared terminal routes. */
export class TerminalApiClient {
  private readonly scope: TerminalScope;
  private readonly fetcher: Fetcher;

  constructor(scope: TerminalScope, fetcher?: Fetcher) {
    this.scope = scope;
    this.fetcher = fetcher ?? globalThis.fetch.bind(globalThis);
  }

  private scoped(path: string, params: Record<string, string> = {}): string {
    const search = new URLSearchParams({
      installation_id: this.scope.installationId,
      authority_epoch: this.scope.authorityEpoch,
      ...params,
    });
    return `${path}?${search.toString()}`;
  }

  async listSessions(): Promise<TerminalSession[]> {
    const body = asRecord(await this.request(this.scoped("/terminal/list")));
    const sessions = Array.isArray(body.sessions) ? body.sessions : [];
    return sessions.map(parseTerminalSession);
  }

  async openSession(name: string): Promise<TerminalSession> {
    const body = asRecord(
      await this.request(this.scoped("/terminal/open"), {
        method: "POST",
        body: { name },
      }),
    );
    return parseTerminalSession(body.session);
  }

  async getSession(sessionId: string): Promise<TerminalSession> {
    const body = asRecord(
      await this.request(
        this.scoped("/terminal/session", { session_id: sessionId }),
      ),
    );
    return parseTerminalSession(body.session);
  }

  async readOutput(
    sessionId: string,
    cursor: number,
    limit?: number,
  ): Promise<TerminalReadResult> {
    const params: Record<string, string> = { session_id: sessionId };
    if (cursor > 0) params.cursor = String(cursor);
    if (limit !== undefined) params.limit = String(limit);
    const body = asRecord(
      await this.request(this.scoped("/terminal/read", params)),
    );
    return parseTerminalRead(body);
  }

  async submitInput(
    sessionId: string,
    payload: TerminalInputPayload,
  ): Promise<TerminalInputReceipt> {
    const { kind, ...rest } = payload;
    const body = asRecord(
      await this.request(this.scoped("/terminal/input"), {
        method: "POST",
        body: { session_id: sessionId, kind, ...rest },
      }),
    );
    return parseTerminalInputReceipt(body.input);
  }

  async setControl(sessionId: string, hold: boolean): Promise<TerminalSession> {
    const body = asRecord(
      await this.request(this.scoped("/terminal/control"), {
        method: "POST",
        body: { session_id: sessionId, hold },
      }),
    );
    return parseTerminalSession(body.session);
  }

  async closeSession(sessionId: string): Promise<TerminalSession> {
    const body = asRecord(
      await this.request(this.scoped("/terminal/close"), {
        method: "POST",
        body: { session_id: sessionId },
      }),
    );
    return parseTerminalSession(body.session);
  }

  private async request(
    path: string,
    options: { method?: string; body?: unknown } = {},
  ): Promise<unknown> {
    let response: Response;
    try {
      response = await this.fetcher(path, {
        method: options.method ?? "GET",
        credentials: "include",
        cache: "no-store",
        headers: {
          Accept: "application/json",
          ...(options.body === undefined
            ? {}
            : { "Content-Type": "application/json" }),
        },
        body:
          options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (error) {
      throw new TerminalAPIUncertainError(error);
    }
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      const code =
        BODY_ERROR_CODES[text.trim()] ?? statusFallback(response.status);
      throw new TerminalAPIError(code, response.status);
    }
    return response.json() as Promise<unknown>;
  }
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("invalid terminal response");
  }
  return value as Record<string, unknown>;
}
