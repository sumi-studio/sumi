import { fetchCSRFToken } from "../auth/session-client";

export const CHATGPT_MODEL = "gpt-6-astra";
export const CHATGPT_EFFORTS = [
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
] as const;
export type ChatGPTEffort = (typeof CHATGPT_EFFORTS)[number];

export interface ChatGPTConnection {
  connected: boolean;
  connectionId?: string;
  accountId?: string;
  expiresAt?: string;
  model: string;
  effort: ChatGPTEffort | "";
  reconnectRequired: boolean;
  activation: "next_start";
}

export interface ChatGPTLogin {
  loginId: string;
  verificationUrl: string;
  userCode: string;
  expiresAt: string;
  intervalMs: number;
  status: "pending" | "completed" | "failed" | "expired" | "cancelled";
  error?: string;
  connection?: ChatGPTConnection;
}

export interface ModelConnectionsAPI {
  status(signal: AbortSignal): Promise<ChatGPTConnection>;
  startLogin(signal: AbortSignal): Promise<ChatGPTLogin>;
  loginStatus(id: string, signal: AbortSignal): Promise<ChatGPTLogin>;
  cancelLogin(id: string, signal: AbortSignal): Promise<void>;
  disconnect(signal: AbortSignal): Promise<void>;
  selectModel(
    connectionId: string,
    effort: ChatGPTEffort,
    signal: AbortSignal,
  ): Promise<ChatGPTConnection>;
}

const BASE = "/api/model-connections/chatgpt";

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("接続状態を確認できませんでした。");
  }
  return value as Record<string, unknown>;
}

function text(value: unknown): string {
  if (typeof value !== "string" || !value)
    throw new Error("接続状態を確認できませんでした。");
  return value;
}

function connection(value: unknown): ChatGPTConnection {
  const row = record(value);
  if (
    typeof row.connected !== "boolean" ||
    typeof row.reconnectRequired !== "boolean" ||
    row.activation !== "next_start" ||
    typeof row.model !== "string" ||
    (row.effort !== "" &&
      !CHATGPT_EFFORTS.includes(row.effort as ChatGPTEffort))
  ) {
    throw new Error("接続状態を確認できませんでした。");
  }
  return {
    connected: row.connected,
    ...(row.connected ? { connectionId: text(row.connectionId) } : {}),
    ...(typeof row.accountId === "string" && row.accountId
      ? { accountId: row.accountId }
      : {}),
    ...(typeof row.expiresAt === "string" && row.expiresAt
      ? { expiresAt: row.expiresAt }
      : {}),
    model: row.model,
    effort: row.effort as ChatGPTConnection["effort"],
    reconnectRequired: row.reconnectRequired,
    activation: "next_start",
  };
}

function login(value: unknown): ChatGPTLogin {
  const row = record(value);
  const url = new URL(text(row.verificationUrl));
  if (
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    !Number.isFinite(Date.parse(text(row.expiresAt))) ||
    typeof row.intervalMs !== "number" ||
    !Number.isFinite(row.intervalMs) ||
    row.intervalMs <= 0 ||
    !["pending", "completed", "failed", "expired", "cancelled"].includes(
      String(row.status),
    )
  ) {
    throw new Error("ログイン情報を確認できませんでした。");
  }
  return {
    loginId: text(row.loginId),
    verificationUrl: url.href,
    userCode: text(row.userCode),
    expiresAt: row.expiresAt as string,
    intervalMs: row.intervalMs,
    status: row.status as ChatGPTLogin["status"],
    ...(typeof row.error === "string" ? { error: row.error } : {}),
    ...(row.connection === undefined
      ? {}
      : { connection: connection(row.connection) }),
  };
}

export function createModelConnectionsAPI(
  fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
): ModelConnectionsAPI {
  async function request(
    path: string,
    signal: AbortSignal,
    method = "GET",
    body?: unknown,
  ): Promise<unknown> {
    let response: Response;
    try {
      const csrfToken =
        method === "GET"
          ? undefined
          : await fetchCSRFToken({ fetcher, signal });
      signal.throwIfAborted();
      response = await fetcher(`${BASE}${path}`, {
        method,
        credentials: "include",
        cache: "no-store",
        headers: {
          Accept: "application/json",
          ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.any([signal, AbortSignal.timeout(15_000)]),
      });
    } catch {
      throw new Error("通信できませんでした。接続状態を再確認してください。");
    }
    if (!response.ok) {
      let message =
        "接続の変更を確認できませんでした。状態を再確認してください。";
      try {
        const failure = record(await response.json());
        if (typeof failure.error === "object" && failure.error !== null) {
          const detail = record(failure.error);
          if (typeof detail.message === "string" && detail.message)
            message = detail.message;
        }
      } catch {
        /* HTTP failure remains authoritative. */
      }
      throw new Error(message);
    }
    if (response.status === 204) return null;
    try {
      return await response.json();
    } catch {
      throw new Error("接続状態の応答を確認できませんでした。");
    }
  }
  return {
    status: async (signal) => connection(await request("", signal)),
    startLogin: async (signal) =>
      login(await request("/login", signal, "POST")),
    loginStatus: async (id, signal) =>
      login(await request(`/login/${encodeURIComponent(id)}`, signal)),
    cancelLogin: async (id, signal) => {
      await request(`/login/${encodeURIComponent(id)}`, signal, "DELETE");
    },
    disconnect: async (signal) => {
      await request("", signal, "DELETE");
    },
    selectModel: async (connectionId, effort, signal) =>
      connection(
        await request("/model", signal, "PUT", {
          connectionId,
          model: CHATGPT_MODEL,
          effort,
        }),
      ),
  };
}
