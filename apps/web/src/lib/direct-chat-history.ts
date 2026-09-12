import type { BrowserEventEnvelope } from "@sumi/api-client";
import {
  type DirectChatInstallationBinding,
  parseDirectChatServerFrame,
  resolveDirectChatURL,
} from "./direct-chat-socket";

export type DurableHistoryEvent = Extract<
  BrowserEventEnvelope,
  { seq: number }
>;
export interface DirectChatIndexEntry {
  id: string;
  seq: number;
  title: string;
  preview?: string;
}
export interface DirectChatHistoryPage {
  events: DurableHistoryEvent[];
  context: DurableHistoryEvent[];
  latestSeq: number;
  beforeSeq: number | null;
  hasMore: boolean;
  index: DirectChatIndexEntry[];
  activeRun: DurableHistoryEvent | null;
  pendingApprovals: DurableHistoryEvent[];
  commandDispositions?: DurableHistoryEvent[];
}
export interface DirectChatHistoryQuery {
  beforeSeq?: number;
  aroundMessageId?: string;
  commandIds?: string[];
  includeIndex?: boolean;
}
export interface DirectChatHistoryAPI {
  page(
    binding: DirectChatInstallationBinding,
    query: DirectChatHistoryQuery,
    signal: AbortSignal,
  ): Promise<DirectChatHistoryPage>;
}
const failure = () => new Error("会話の履歴を読み込めませんでした。");
function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || !value || Array.isArray(value))
    throw failure();
  return value as Record<string, unknown>;
}
function sequence(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw failure();
  return value as number;
}
function events(value: unknown, head: number): DurableHistoryEvent[] {
  if (!Array.isArray(value)) throw failure();
  return value.map((item) => {
    const row = record(item);
    const seq = sequence(row.seq);
    if (seq === 0 || seq > head) throw failure();
    const parsed = parseDirectChatServerFrame(
      { type: "event", envelope: row },
      seq - 1,
    );
    if (parsed?.type !== "event" || !("seq" in parsed.envelope))
      throw failure();
    return parsed.envelope as DurableHistoryEvent;
  });
}
export function parseDirectChatHistoryPage(
  value: unknown,
): DirectChatHistoryPage {
  const row = record(value);
  const latestSeq = sequence(row.latest_seq);
  if (typeof row.has_more !== "boolean" || !Array.isArray(row.index))
    throw failure();
  const beforeSeq = row.before_seq === null ? null : sequence(row.before_seq);
  if (beforeSeq !== null && (beforeSeq === 0 || beforeSeq > latestSeq))
    throw failure();
  if (row.has_more && beforeSeq === null) throw failure();
  const index = row.index.map((item) => {
    const entry = record(item);
    if (
      typeof entry.id !== "string" ||
      !entry.id ||
      typeof entry.title !== "string" ||
      ("preview" in entry && typeof entry.preview !== "string")
    )
      throw failure();
    const seq = sequence(entry.seq);
    if (seq === 0 || seq > latestSeq) throw failure();
    return {
      id: entry.id,
      seq,
      title: entry.title,
      ...(typeof entry.preview === "string" ? { preview: entry.preview } : {}),
    };
  });
  const activeRun =
    row.active_run === null ? null : events([row.active_run], latestSeq)[0];
  if (activeRun && activeRun.event.type !== "agent_start") throw failure();
  const pendingApprovals = events(row.pending_approvals, latestSeq);
  if (
    pendingApprovals.some((event) => event.event.type !== "approval_requested")
  )
    throw failure();
  const commandDispositions = events(row.command_dispositions, latestSeq);
  if (
    commandDispositions.some(
      (event) => event.event.type !== "command_disposition",
    )
  )
    throw failure();
  return {
    commandDispositions,
    events: events(row.events, latestSeq),
    context: events(row.context, latestSeq),
    latestSeq,
    beforeSeq,
    hasMore: row.has_more,
    index,
    activeRun,
    pendingApprovals,
  };
}
export function createDirectChatHistoryAPI(): DirectChatHistoryAPI {
  return {
    async page(binding, query, signal) {
      const env = (
        import.meta as ImportMeta & { env?: Record<string, string | undefined> }
      ).env;
      const url = resolveDirectChatURL({
        apiBaseURL: env?.VITE_API_BASE_URL,
        authMode: env?.VITE_SUMI_AUTH_MODE,
        ...binding,
        pageOrigin: globalThis.location?.origin,
      });
      url.protocol = url.protocol === "wss:" ? "https:" : "http:";
      url.pathname = "/direct-chat/history";
      url.searchParams.set("limit", "100");
      if (query.beforeSeq !== undefined)
        url.searchParams.set("before_seq", String(query.beforeSeq));
      if (query.aroundMessageId)
        url.searchParams.set("around_message_id", query.aroundMessageId);
      for (const id of query.commandIds ?? [])
        url.searchParams.append("command_id", id);
      if (query.includeIndex === false)
        url.searchParams.set("include_index", "false");
      const response = await fetch(url, {
        credentials: "include",
        signal,
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      if (!response.ok) throw failure();
      return parseDirectChatHistoryPage(await response.json());
    },
  };
}
