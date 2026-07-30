import type { BrowserEventEnvelope } from "@sumi/api-client";

export type DirectChatCommand =
  | { type: "abort" }
  | { type: "user_message"; text: string; attachments: [] }
  | {
      type: "approval_decision";
      request_id: string;
      decision:
        | { type: "approve_once" }
        | { type: "approve_always"; rule: Record<string, unknown> }
        | { type: "deny" };
    };

export type DirectChatConnectionState = "connecting" | "connected" | "closed";
export type DirectChatReadyState = "unknown" | "ready" | "not_ready";

export type DirectChatEventFrame = {
  type: "event";
  envelope: BrowserEventEnvelope;
};
export type DirectChatStatusFrame = {
  type: "direct_chat_status";
  status: "ready" | "unavailable";
};
export type DirectChatAcceptedFrame = {
  type: "command_accepted";
  idempotency_key: string;
  command_id: string;
  seq: number;
};
export type DirectChatRejectedFrame = {
  type: "command_rejected";
  idempotency_key: string;
  reject_reason: string;
};
export type DirectChatServerFrame =
  | DirectChatEventFrame
  | DirectChatStatusFrame
  | DirectChatAcceptedFrame
  | DirectChatRejectedFrame;

type Listener = (frame: DirectChatServerFrame) => void;
type ConnectionListener = (state: DirectChatConnectionState) => void;
type ReadyListener = (state: DirectChatReadyState) => void;

const InitialReconnectDelay = 500;
const MaxReconnectDelay = 30000;
const RejectReasons = new Set([
  "unknown_command",
  "schema_violation",
  "attachments_not_empty",
  "oversized",
  "not_allowed",
  "idempotency_conflict",
  "unavailable",
]);
const UUIDPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DurableEventTypes = new Set([
  "agent_start",
  "agent_end",
  "turn_start",
  "turn_end",
  "message_start",
  "message_end",
  "tool_execution_start",
  "tool_execution_end",
  "approval_requested",
  "approval_resolved",
  "steered",
  "memory_maintenance",
  "retry_scheduled",
]);
const VolatileEventTypes = new Set([
  "message_update",
  "tool_execution_update",
  "error",
]);
const StreamStartTypes = new Set([
  "text_start",
  "thinking_start",
  "tool_call_start",
  "reasoning_summary_start",
]);
const StreamDeltaTypes = new Set([
  "text_delta",
  "thinking_delta",
  "tool_call_delta",
  "reasoning_summary_delta",
]);
const StreamEndTypes = new Set([
  "text_end",
  "thinking_end",
  "reasoning_summary_end",
]);
const StopReasons = new Set(["stop", "length", "tool_use", "error", "aborted"]);
const APIProtocols = new Set([
  "open_ai_chat_completions",
  "open_ai_responses",
  "anthropic_messages",
]);
const ToolArgumentErrors = new Set([
  "invalid_json",
  "non_object",
  "schema_violation",
  "incomplete_response",
  "too_large",
]);
const AuditOutcomes = new Set(["allow", "deny"]);
const RiskLevels = new Set(["low", "medium", "high", "critical"]);
const UserAuthorizations = new Set(["unknown", "low", "medium", "high"]);

function reconnectDelay(attempt: number): number {
  const exponential = InitialReconnectDelay * 2 ** attempt;
  const capped = Math.min(exponential, MaxReconnectDelay);
  return Math.min(capped + Math.random() * 200, MaxReconnectDelay);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, keys: string[]): boolean {
  return Object.keys(value).every((key) => keys.includes(key));
}

function hasRequiredAndOnlyKeys(
  value: Record<string, unknown>,
  required: string[],
  allowed = required,
): boolean {
  return hasOnlyKeys(value, allowed) && required.every((key) => key in value);
}

// Opaque AnyJSON is faithfully preserved. Its keys are data, not browser
// routing fields; only its recursively nested number range is constrained.
function isSafeAnyJSON(value: unknown): boolean {
  const pending = [value];
  const seen = new WeakSet<object>();
  while (pending.length > 0) {
    const current = pending.pop();
    if (
      current === null ||
      typeof current === "string" ||
      typeof current === "boolean"
    )
      continue;
    if (typeof current === "number") {
      if (
        !Number.isFinite(current) ||
        Math.abs(current) > Number.MAX_SAFE_INTEGER
      )
        return false;
      continue;
    }
    if (typeof current !== "object" || seen.has(current)) return false;
    seen.add(current);
    if (Array.isArray(current)) {
      for (const nested of current) pending.push(nested);
      continue;
    }
    if (!isRecord(current)) return false;
    for (const [key, nested] of Object.entries(current)) {
      if (typeof key !== "string") return false;
      pending.push(nested);
    }
  }
  return true;
}

function isSafeSequence(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isStringOrNull(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isUUID(value: unknown): value is string {
  return (
    typeof value === "string" && value.length === 36 && UUIDPattern.test(value)
  );
}

function isDateTime(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(?:Z|[+-](\d{2}):(\d{2}))$/.exec(
      value,
    );
  if (!match || match[0] !== value) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[8] === undefined ? 0 : Number(match[8]);
  const offsetMinute = match[9] === undefined ? 0 : Number(match[9]);
  if (
    month < 1 ||
    month > 12 ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    return false;
  }
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [
    31,
    leapYear ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ];
  return day >= 1 && day <= daysInMonth[month - 1];
}

function isUserContent(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  if (value.type === "text") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "text"]) &&
      typeof value.text === "string"
    );
  }
  if (value.type === "image") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "data", "mime_type"]) &&
      typeof value.data === "string" &&
      typeof value.mime_type === "string"
    );
  }
  return false;
}

function isToolCall(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(value, ["id", "name", "arguments"]) &&
    typeof value.id === "string" &&
    typeof value.name === "string" &&
    isRecord(value.arguments) &&
    isSafeAnyJSON(value.arguments)
  );
}

function isRejectedToolCall(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(value, ["id", "name", "error"]) &&
    typeof value.id === "string" &&
    typeof value.name === "string" &&
    typeof value.error === "string" &&
    ToolArgumentErrors.has(value.error)
  );
}

function isAssistantContent(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  if (value.type === "text") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "text", "wire_item_index"]) &&
      typeof value.text === "string" &&
      isSafeSequence(value.wire_item_index)
    );
  }
  if (value.type === "thinking") {
    return (
      hasRequiredAndOnlyKeys(value, [
        "type",
        "thinking",
        "signature_field",
        "wire_item_index",
      ]) &&
      typeof value.thinking === "string" &&
      typeof value.signature_field === "string" &&
      isSafeSequence(value.wire_item_index)
    );
  }
  if (value.type === "tool_call") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "tool_call", "wire_item_index"]) &&
      isToolCall(value.tool_call) &&
      isSafeSequence(value.wire_item_index)
    );
  }
  if (value.type === "rejected_tool_call") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "rejected", "wire_item_index"]) &&
      isRejectedToolCall(value.rejected) &&
      isSafeSequence(value.wire_item_index)
    );
  }
  return false;
}

function isProviderOrigin(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(value, [
      "provider_instance_id",
      "protocol",
      "model",
    ]) &&
    typeof value.provider_instance_id === "string" &&
    typeof value.protocol === "string" &&
    APIProtocols.has(value.protocol) &&
    typeof value.model === "string"
  );
}

function isUsage(value: unknown): boolean {
  const keys = [
    "input",
    "output",
    "cache_read",
    "cache_write",
    "reasoning",
    "total_tokens",
  ];
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(value, keys) &&
    keys.every((key) => isSafeSequence(value[key]))
  );
}

function isPublicMessage(value: unknown): boolean {
  if (!isRecord(value) || typeof value.role !== "string") return false;
  if (value.role === "user") {
    return (
      hasRequiredAndOnlyKeys(value, ["role", "content", "timestamp"]) &&
      Array.isArray(value.content) &&
      value.content.every(isUserContent) &&
      isDateTime(value.timestamp)
    );
  }
  if (value.role === "assistant") {
    return (
      hasRequiredAndOnlyKeys(value, [
        "role",
        "content",
        "model",
        "provider",
        "origin",
        "usage",
        "stop_reason",
        "error_message",
        "provider_code",
        "interrupted",
        "timestamp",
      ]) &&
      Array.isArray(value.content) &&
      value.content.every(isAssistantContent) &&
      typeof value.model === "string" &&
      typeof value.provider === "string" &&
      isProviderOrigin(value.origin) &&
      isUsage(value.usage) &&
      typeof value.stop_reason === "string" &&
      StopReasons.has(value.stop_reason) &&
      isStringOrNull(value.error_message) &&
      isStringOrNull(value.provider_code) &&
      typeof value.interrupted === "boolean" &&
      isDateTime(value.timestamp)
    );
  }
  if (value.role === "tool_result") {
    return (
      hasRequiredAndOnlyKeys(value, [
        "role",
        "tool_call_id",
        "tool_name",
        "content",
        "details",
        "is_error",
        "timestamp",
      ]) &&
      typeof value.tool_call_id === "string" &&
      typeof value.tool_name === "string" &&
      Array.isArray(value.content) &&
      value.content.every(isUserContent) &&
      isSafeAnyJSON(value.details) &&
      typeof value.is_error === "boolean" &&
      isDateTime(value.timestamp)
    );
  }
  return false;
}

function isToolResultPayload(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(value, [
      "tool_call_id",
      "tool_name",
      "content",
      "details",
      "is_error",
      "timestamp",
    ]) &&
    typeof value.tool_call_id === "string" &&
    typeof value.tool_name === "string" &&
    Array.isArray(value.content) &&
    value.content.every(isUserContent) &&
    isSafeAnyJSON(value.details) &&
    typeof value.is_error === "boolean" &&
    isDateTime(value.timestamp)
  );
}

function isPublicStreamEvent(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  if (StreamStartTypes.has(value.type)) {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "content_index"]) &&
      isSafeSequence(value.content_index)
    );
  }
  if (StreamDeltaTypes.has(value.type)) {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "content_index", "delta"]) &&
      isSafeSequence(value.content_index) &&
      typeof value.delta === "string"
    );
  }
  if (StreamEndTypes.has(value.type)) {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "content_index", "content"]) &&
      isSafeSequence(value.content_index) &&
      typeof value.content === "string"
    );
  }
  if (value.type === "tool_call_preview") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "content_index", "preview"]) &&
      isSafeSequence(value.content_index) &&
      isSafeAnyJSON(value.preview)
    );
  }
  if (value.type === "tool_call_end") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "content_index", "tool_call"]) &&
      isSafeSequence(value.content_index) &&
      isToolCall(value.tool_call)
    );
  }
  if (value.type === "tool_call_rejected") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "content_index", "rejected"]) &&
      isSafeSequence(value.content_index) &&
      isRejectedToolCall(value.rejected)
    );
  }
  return false;
}

function isReviewProjection(value: unknown): boolean {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, ["reviewable", "insufficient_evidence"])
  )
    return false;
  const hasReviewable = "reviewable" in value;
  const hasInsufficient = "insufficient_evidence" in value;
  if (hasReviewable === hasInsufficient) return false;
  if (hasReviewable) return isSafeAnyJSON(value.reviewable);
  return (
    isRecord(value.insufficient_evidence) &&
    hasRequiredAndOnlyKeys(value.insufficient_evidence, ["reason"]) &&
    typeof value.insufficient_evidence.reason === "string"
  );
}

function isAuditDecision(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(value, [
      "outcome",
      "risk",
      "authorization",
      "rationale",
    ]) &&
    typeof value.outcome === "string" &&
    AuditOutcomes.has(value.outcome) &&
    typeof value.risk === "string" &&
    RiskLevels.has(value.risk) &&
    typeof value.authorization === "string" &&
    UserAuthorizations.has(value.authorization) &&
    typeof value.rationale === "string"
  );
}

function isApprovalRequest(value: unknown): boolean {
  return (
    isRecord(value) &&
    hasRequiredAndOnlyKeys(
      value,
      ["id", "tool_call_id", "tool_name", "action", "args_summary"],
      [
        "id",
        "tool_call_id",
        "tool_name",
        "action",
        "args_summary",
        "reason",
        "audit",
      ],
    ) &&
    typeof value.id === "string" &&
    typeof value.tool_call_id === "string" &&
    typeof value.tool_name === "string" &&
    isReviewProjection(value.action) &&
    isSafeAnyJSON(value.args_summary) &&
    (!("reason" in value) || isStringOrNull(value.reason)) &&
    (!("audit" in value) ||
      value.audit === null ||
      isAuditDecision(value.audit))
  );
}

function isApprovalDecision(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  if (value.type === "approve_once" || value.type === "deny") {
    return hasRequiredAndOnlyKeys(value, ["type"]);
  }
  return (
    value.type === "approve_always" &&
    hasRequiredAndOnlyKeys(value, ["type", "rule"]) &&
    isRecord(value.rule) &&
    isSafeAnyJSON(value.rule)
  );
}

function isApprovalResolution(value: unknown): boolean {
  return (
    value === "cancelled" ||
    (isRecord(value) &&
      hasRequiredAndOnlyKeys(value, ["decision"]) &&
      isApprovalDecision(value.decision))
  );
}

/**
 * The browser may submit content and an idempotency key only. Target selection
 * and provenance are server-authenticated direct-chat concerns, so this guard
 * rejects any caller that tries to smuggle those fields through a command.
 */
export function isDirectChatCommand(
  value: unknown,
): value is DirectChatCommand {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  if (value.type === "abort") return hasOnlyKeys(value, ["type"]);
  if (value.type === "user_message") {
    return (
      hasOnlyKeys(value, ["type", "text", "attachments"]) &&
      typeof value.text === "string" &&
      Array.isArray(value.attachments) &&
      value.attachments.length === 0
    );
  }
  if (value.type !== "approval_decision") return false;
  if (
    !hasOnlyKeys(value, ["type", "request_id", "decision"]) ||
    typeof value.request_id !== "string" ||
    !isApprovalDecision(value.decision)
  ) {
    return false;
  }
  return true;
}

function isSafeEventForUI(
  value: unknown,
): value is Record<string, unknown> & { type: string } {
  if (!isRecord(value) || typeof value.type !== "string" || !value.type)
    return false;
  if (
    value.type === "agent_start" ||
    value.type === "agent_end" ||
    value.type === "turn_start"
  ) {
    return hasRequiredAndOnlyKeys(value, ["type"]);
  }
  if (value.type === "turn_end") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "message", "tool_results"]) &&
      (value.message === null || isPublicMessage(value.message)) &&
      Array.isArray(value.tool_results) &&
      value.tool_results.every(isToolResultPayload)
    );
  }
  if (value.type === "message_start" || value.type === "message_end") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "message_id", "message"]) &&
      isUUID(value.message_id) &&
      isPublicMessage(value.message)
    );
  }
  if (value.type === "message_update") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "message_id", "event"]) &&
      isUUID(value.message_id) &&
      isPublicStreamEvent(value.event)
    );
  }
  if (value.type === "tool_execution_start") {
    return (
      hasRequiredAndOnlyKeys(value, [
        "type",
        "tool_call_id",
        "tool_name",
        "args",
      ]) &&
      typeof value.tool_call_id === "string" &&
      typeof value.tool_name === "string" &&
      isRecord(value.args) &&
      isSafeAnyJSON(value.args)
    );
  }
  if (value.type === "tool_execution_update") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "tool_call_id", "partial"]) &&
      typeof value.tool_call_id === "string" &&
      isSafeAnyJSON(value.partial)
    );
  }
  if (value.type === "tool_execution_end") {
    return (
      hasRequiredAndOnlyKeys(value, [
        "type",
        "tool_call_id",
        "result",
        "is_error",
      ]) &&
      typeof value.tool_call_id === "string" &&
      isSafeAnyJSON(value.result) &&
      typeof value.is_error === "boolean"
    );
  }
  if (value.type === "steered") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "mode"]) &&
      (value.mode === "hard" || value.mode === "soft")
    );
  }
  if (value.type === "approval_requested") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "request"]) &&
      isApprovalRequest(value.request)
    );
  }
  if (value.type === "approval_resolved") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "request_id", "resolution"]) &&
      typeof value.request_id === "string" &&
      isApprovalResolution(value.resolution)
    );
  }
  if (value.type === "memory_maintenance") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "kind"]) &&
      typeof value.kind === "string"
    );
  }
  if (value.type === "retry_scheduled") {
    return (
      hasRequiredAndOnlyKeys(value, [
        "type",
        "attempt",
        "delay_ms",
        "retry_at",
        "error_message",
      ]) &&
      isSafeSequence(value.attempt) &&
      isSafeSequence(value.delay_ms) &&
      isDateTime(value.retry_at) &&
      typeof value.error_message === "string"
    );
  }
  if (value.type === "error") {
    return (
      hasRequiredAndOnlyKeys(value, ["type", "message"]) &&
      typeof value.message === "string"
    );
  }
  return false;
}

/** Parses only the target-free public direct-chat wire shape. */
export function parseDirectChatServerFrame(
  value: unknown,
  lastEventSeq: number,
): DirectChatServerFrame | undefined {
  if (!isRecord(value)) return undefined;
  if (
    value.type === "direct_chat_status" &&
    (value.status === "ready" || value.status === "unavailable") &&
    hasOnlyKeys(value, ["type", "status"])
  ) {
    return value as DirectChatStatusFrame;
  }
  if (
    value.type === "event" &&
    isRecord(value.envelope) &&
    hasOnlyKeys(value, ["type", "envelope"])
  ) {
    const envelope = value.envelope;
    if (
      !hasOnlyKeys(envelope, ["seq", "event"]) ||
      !isSafeEventForUI(envelope.event)
    ) {
      return undefined;
    }
    const eventType = envelope.event.type;
    if (DurableEventTypes.has(eventType)) {
      if (!isSafeSequence(envelope.seq) || envelope.seq !== lastEventSeq + 1)
        return undefined;
    } else if (VolatileEventTypes.has(eventType)) {
      if ("seq" in envelope) return undefined;
    } else {
      return undefined;
    }
    return value as DirectChatEventFrame;
  }
  if (
    value.type === "command_accepted" &&
    typeof value.idempotency_key === "string" &&
    value.idempotency_key.length > 0 &&
    isUUID(value.command_id) &&
    isSafeSequence(value.seq) &&
    hasOnlyKeys(value, ["type", "idempotency_key", "command_id", "seq"])
  ) {
    return value as DirectChatAcceptedFrame;
  }
  if (
    value.type === "command_rejected" &&
    typeof value.reject_reason === "string" &&
    RejectReasons.has(value.reject_reason) &&
    hasOnlyKeys(value, ["type", "idempotency_key", "reject_reason"]) &&
    typeof value.idempotency_key === "string" &&
    value.idempotency_key.length > 0
  ) {
    return value as DirectChatRejectedFrame;
  }
  return undefined;
}

// One screen owns one always-on browser socket. Its target and provenance are
// derived from the signed HttpOnly session by the API, never from browser JSON.
export class DirectChatSocket {
  private socket?: WebSocket;
  private retry?: ReturnType<typeof setTimeout>;
  private reconnectAttempt = 0;
  private lastEventSeq = 0;
  private admissionReady = false;
  private readonly pending = new Map<string, DirectChatCommand>();
  private readonly listeners = new Set<Listener>();
  private readonly connectionListeners = new Set<ConnectionListener>();
  private readonly readyListeners = new Set<ReadyListener>();

  connect() {
    if (this.socket) {
      const { readyState } = this.socket;
      if (
        readyState === WebSocket.CONNECTING ||
        readyState === WebSocket.OPEN ||
        readyState === WebSocket.CLOSING
      )
        return;
      this.socket = undefined;
    }
    this.clearRetry();
    this.setConnectionState("connecting");
    const env = (
      import.meta as ImportMeta & { env?: Record<string, string | undefined> }
    ).env;
    const base = env?.VITE_API_BASE_URL ?? globalThis.location?.origin;
    if (!base) throw new Error("direct chat API base URL is unavailable");
    const url = new URL("/direct-chat/ws", base);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(url);
    this.socket = socket;
    socket.onopen = () => {
      if (this.socket !== socket) return;
      this.reconnectAttempt = 0;
      this.setConnectionState("connected");
      this.admissionReady = false;
      this.setReadyState("unknown");
      this.sendRaw({ type: "hello", last_event_seq: this.lastEventSeq });
    };
    socket.onerror = () => {};
    socket.onmessage = (message) => {
      if (
        this.socket !== socket ||
        socket.readyState !== WebSocket.OPEN ||
        typeof message.data !== "string"
      ) {
        socket.close();
        return;
      }
      let raw: unknown;
      try {
        raw = JSON.parse(message.data);
      } catch {
        socket.close();
        return;
      }
      const frame = parseDirectChatServerFrame(raw, this.lastEventSeq);
      if (!frame) {
        socket.close();
        return;
      }
      if (frame.type === "event" && "seq" in frame.envelope) {
        this.lastEventSeq = frame.envelope.seq;
      }
      if (frame.type === "direct_chat_status") {
        this.admissionReady = frame.status === "ready";
        this.setReadyState(frame.status === "ready" ? "ready" : "not_ready");
        if (this.admissionReady) this.flushPending();
      }
      if (frame.type === "command_accepted")
        this.pending.delete(frame.idempotency_key);
      if (frame.type === "command_rejected")
        this.pending.delete(frame.idempotency_key);
      for (const listener of this.listeners) listener(frame);
    };
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.socket = undefined;
      this.admissionReady = false;
      this.setConnectionState("closed");
      this.setReadyState("unknown");
      this.scheduleReconnect();
    };
  }

  sendCommand(command: unknown, idempotencyKey = crypto.randomUUID()): boolean {
    if (
      !isDirectChatCommand(command) ||
      !idempotencyKey ||
      idempotencyKey.length > 1024
    )
      return false;
    this.pending.set(idempotencyKey, command);
    this.flushPending();
    return true;
  }

  pendingIdempotencyKeys(): string[] {
    return [...this.pending.keys()];
  }
  onFrame(listener: Listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  onConnection(listener: ConnectionListener) {
    this.connectionListeners.add(listener);
    return () => this.connectionListeners.delete(listener);
  }
  onReady(listener: ReadyListener) {
    this.readyListeners.add(listener);
    return () => this.readyListeners.delete(listener);
  }

  close() {
    this.clearRetry();
    const socket = this.socket;
    this.socket = undefined;
    this.admissionReady = false;
    socket?.close();
  }

  private flushPending() {
    if (!this.admissionReady || this.socket?.readyState !== WebSocket.OPEN)
      return;
    for (const [idempotency_key, command] of this.pending) {
      this.sendRaw({ type: "command", idempotency_key, command });
    }
  }
  private sendRaw(frame: unknown) {
    if (this.socket?.readyState !== WebSocket.OPEN)
      throw new Error("direct chat websocket is not connected");
    this.socket.send(JSON.stringify(frame));
  }
  private scheduleReconnect() {
    if (this.retry !== undefined) return;
    this.retry = setTimeout(
      () => this.connect(),
      reconnectDelay(this.reconnectAttempt++),
    );
  }
  private setConnectionState(state: DirectChatConnectionState) {
    for (const listener of this.connectionListeners) listener(state);
  }
  private setReadyState(state: DirectChatReadyState) {
    for (const listener of this.readyListeners) listener(state);
  }
  private clearRetry() {
    if (this.retry !== undefined) clearTimeout(this.retry);
    this.retry = undefined;
  }
}
