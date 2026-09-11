import type { FeedbackDiagnostics } from "./diagnostics";
export interface Participant {
  kind: "human" | "personality_agent";
  human_id?: string;
  personality_agent_id?: string;
}
export interface Author {
  participant: Participant;
  display_name: string;
}
export interface Message {
  id: string;
  author: Author;
  body: string;
  created_at: string;
  revision: number;
}
export interface Thread {
  id: string;
  title: string;
  body: string;
  status: "open" | "resolved";
  author: Author;
  created_at: string;
  updated_at: string;
  revision: number;
  diagnostics?: FeedbackDiagnostics;
  latest_message?: Message | null;
  unread: boolean;
}
export interface Activity {
  id: string;
  author: Author;
  status: "open" | "resolved";
  created_at: string;
  revision: number;
}
export interface Bootstrap {
  recipient_name: string;
  available: boolean;
  participant: Participant;
  is_recipient: boolean;
  installed: boolean;
  enabled: boolean;
  installation_id?: string;
}
export interface ThreadPage {
  threads: Thread[];
  next_cursor?: string | null;
}
export interface Detail {
  thread: Thread;
  messages: Message[];
  activities?: Activity[];
  next_cursor?: string | null;
}
export type Filter = "all" | "open" | "resolved";
export interface FeedbackClient {
  bootstrap(): Promise<Bootstrap>;
  list(status: Filter, cursor?: string): Promise<ThreadPage>;
  open(id: string, cursor?: string): Promise<Detail>;
  create(
    title: string,
    body: string,
    requestId: string,
    diagnostics?: FeedbackDiagnostics,
  ): Promise<Thread>;
  reply(id: string, body: string, requestId: string): Promise<Message>;
  status(
    id: string,
    status: Thread["status"],
    revision: number,
  ): Promise<Thread>;
  read(id: string, revision: number): Promise<void>;
}
export class FeedbackError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string) {
    super(code);
    this.status = status;
    this.code = code;
  }
}
async function request<T>(
  path: string,
  method = "GET",
  body?: unknown,
): Promise<T> {
  const response = await fetch(`/feedback/${path}`, {
    method,
    credentials: "same-origin",
    signal: AbortSignal.timeout(20_000),
    ...(body === undefined
      ? {}
      : {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    throw new FeedbackError(
      response.status,
      typeof error.error === "string" ? error.error : "request_failed",
    );
  }
  return response.status === 204 ? (undefined as T) : response.json();
}
const threadPath = (id: string) => `threads/${encodeURIComponent(id)}`;
export const feedbackClient: FeedbackClient = {
  bootstrap: () => request("bootstrap"),
  list: (status, cursor) =>
    request(
      `threads?${new URLSearchParams({ status, ...(cursor ? { cursor } : {}) })}`,
    ),
  open: (id, cursor) =>
    request(
      `${threadPath(id)}${cursor ? `?${new URLSearchParams({ cursor })}` : ""}`,
    ),
  create: (title, body, request_id, diagnostics) =>
    request("threads", "POST", { title, body, request_id, diagnostics }),
  reply: (id, body, request_id) =>
    request(`${threadPath(id)}/messages`, "POST", { body, request_id }),
  status: (id, status, revision) =>
    request(threadPath(id), "PATCH", { status, revision }),
  read: (id, revision) =>
    request(`${threadPath(id)}/read`, "PUT", { revision }),
};
export function errorMessage(error: unknown): string {
  if (error instanceof FeedbackError) {
    if (error.status === 401)
      return "ログイン状態を確認してください。入力内容は残っています。";
    if (error.status === 409)
      return "会話が更新されています。最新の状態を確認して、もう一度お試しください。";
    if (error.status === 403 || error.status === 404)
      return "この会話を開く権限がないか、見つかりませんでした。";
    if (error.status === 503)
      return "送信先の準備ができていません。時間をおいてお試しください。";
  }
  return "接続できませんでした。入力内容は残っています。もう一度お試しください。";
}
