import type {
  DirectChatStatusFrame as APIClientDirectChatStatusFrame,
  BrowserEventEnvelope,
  CommandDispositionEvent,
} from "@sumi/api-client";
import { secureRandomUUID } from "./random-uuid";

export interface DirectChatInstallationBinding {
  installationId: string;
  authorityEpoch: string;
}

export type DirectChatCommand =
  | { type: "abort" }
  | { type: "user_message"; text: string; attachments: [] }
  | {
      type: "approval_decision";
      request_id: string;
      decision: { type: "approve_once" } | { type: "deny_once" };
    };

/**
 * The API accepts the upgrade and closes with this code/reason pair when an
 * authorized session could not get an agent runtime started. It is the only
 * channel a page can read a cause on, and it mirrors
 * `DirectChatRuntimeUnavailableCloseCode` in
 * `apps/api/internal/agentevents/browser_ws.go`.
 */
export const DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE = 4001;
export const DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_REASON = "runtime_not_ready";

export type DirectChatConnectionState = "connecting" | "connected" | "closed";
// Only a server-stated unavailable reason can describe lifecycle progress. A
// 4001 close is the distinct, attributed failure to start.
export type DirectChatReadyState =
  | "unknown"
  | "ready"
  | "rehydrating"
  | "stopped"
  | "unavailable"
  | "not_ready";

export type DirectChatEventFrame = {
  type: "event";
  envelope: BrowserEventEnvelope;
};
// This status frame is generated from the public agent-events contract. In
// particular, `reason` is prohibited for ready and required for unavailable.
export type DirectChatStatusFrame = APIClientDirectChatStatusFrame;
export type DirectChatAcceptedFrame = {
  type: "command_accepted";
  idempotency_key: string;
  command_id: string;
  seq: number;
  disposition?: CommandDispositionEvent;
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
const DurableCommandRejectReasons = new Set([
  "unknown_command",
  "schema_violation",
  "attachments_not_empty",
  "oversized",
  "not_allowed",
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
  "command_disposition",
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
type DirectChatUnavailableReason = Extract<
  DirectChatStatusFrame,
  { status: "unavailable" }
>["reason"];
const DirectChatUnavailableReasons = new Set<DirectChatUnavailableReason>([
  "rehydrating",
  "stopped",
  "unavailable",
]);

function isDirectChatUnavailableReason(
  value: unknown,
): value is DirectChatUnavailableReason {
  return (
    typeof value === "string" &&
    DirectChatUnavailableReasons.has(value as DirectChatUnavailableReason)
  );
}

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
    hasRequiredAndOnlyKeys(value, ["id", "name", "route", "arguments"]) &&
    typeof value.id === "string" &&
    typeof value.name === "string" &&
    (value.route === "normal" || value.route === "elevated") &&
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
  if (value.type === "approve_once" || value.type === "deny_once") {
    return hasRequiredAndOnlyKeys(value, ["type"]);
  }
  return false;
}

function isApprovalResolution(value: unknown): boolean {
  return (
    value === "cancelled" ||
    (isRecord(value) &&
      hasRequiredAndOnlyKeys(value, ["decision"]) &&
      isApprovalDecision(value.decision)) ||
    (isRecord(value) &&
      hasRequiredAndOnlyKeys(value, ["rejected"]) &&
      isRecord(value.rejected) &&
      hasRequiredAndOnlyKeys(value.rejected, ["decision"]) &&
      isRecord(value.rejected.decision) &&
      hasRequiredAndOnlyKeys(value.rejected.decision, ["type"]) &&
      value.rejected.decision.type === "approve_once")
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
  if (value.type === "command_disposition") {
    const commonShape =
      isUUID(value.command_id) &&
      isSafeSequence(value.command_seq) &&
      (value.status === "applied" ||
        value.status === "superseded" ||
        value.status === "rejected");
    if (!commonShape) return false;
    if (value.status === "rejected") {
      return (
        hasRequiredAndOnlyKeys(
          value,
          ["type", "command_id", "command_seq", "status", "reject_reason"],
          ["type", "command_id", "command_seq", "status", "reject_reason"],
        ) &&
        typeof value.reject_reason === "string" &&
        DurableCommandRejectReasons.has(value.reject_reason)
      );
    }
    return hasRequiredAndOnlyKeys(value, [
      "type",
      "command_id",
      "command_seq",
      "status",
    ]);
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
  if (value.type === "direct_chat_status") {
    if (
      value.status === "ready" &&
      hasRequiredAndOnlyKeys(value, ["type", "status"])
    ) {
      return value as DirectChatStatusFrame;
    }
    if (
      value.status === "unavailable" &&
      hasRequiredAndOnlyKeys(value, ["type", "status", "reason"]) &&
      isDirectChatUnavailableReason(value.reason)
    ) {
      return value as DirectChatStatusFrame;
    }
    return undefined;
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
    value.idempotency_key.length <= 1024 &&
    isUUID(value.command_id) &&
    isSafeSequence(value.seq) &&
    hasOnlyKeys(value, [
      "type",
      "idempotency_key",
      "command_id",
      "seq",
      "disposition",
    ]) &&
    (!("disposition" in value) ||
      (isSafeEventForUI(value.disposition) &&
        value.disposition.type === "command_disposition" &&
        value.disposition.command_id === value.command_id &&
        value.disposition.command_seq === value.seq))
  ) {
    return value as DirectChatAcceptedFrame;
  }
  if (
    value.type === "command_rejected" &&
    typeof value.reject_reason === "string" &&
    RejectReasons.has(value.reject_reason) &&
    hasOnlyKeys(value, ["type", "idempotency_key", "reject_reason"]) &&
    typeof value.idempotency_key === "string" &&
    value.idempotency_key.length > 0 &&
    value.idempotency_key.length <= 1024
  ) {
    return value as DirectChatRejectedFrame;
  }
  return undefined;
}

export function resolveDirectChatURL({
  apiBaseURL,
  authMode,
  installationId,
  authorityEpoch,
  pageOrigin,
}: {
  apiBaseURL?: string;
  authMode?: string;
  installationId: string;
  authorityEpoch: string;
  pageOrigin?: string;
}): URL {
  if (!pageOrigin) throw new Error("direct chat page origin is unavailable");
  if (!installationId.trim()) {
    throw new Error("direct chat installation is unavailable");
  }
  if (!/^[1-9][0-9]*$/.test(authorityEpoch)) {
    throw new Error("direct chat authority epoch is unavailable");
  }

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
    throw new Error("direct chat API base URL must contain only an origin");
  }
  // Authentication cookies and the WebSocket upgrade are one same-origin
  // browser contract. A cross-origin fixture is permitted only when the
  // explicit preissued mode bypasses browser session authentication.
  if (apiURL.origin !== pageURL.origin && authMode !== "preissued") {
    throw new Error(
      "cross-origin direct chat is unavailable outside preissued E2E mode",
    );
  }

  const url = new URL("/direct-chat/ws", apiURL);
  url.searchParams.set("installation_id", installationId);
  url.searchParams.set("authority_epoch", authorityEpoch);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url;
}

// One screen owns one always-on browser socket. Its target and provenance are
// derived from the signed HttpOnly session by the API, never from browser JSON.
export class DirectChatSocket {
  private socket?: WebSocket;
  private retry?: ReturnType<typeof setTimeout>;
  private reconnectAttempt = 0;
  private lastEventSeq = 0;
  private admissionReady = false;
  private installationId?: string;
  private authorityEpoch?: string;
  private readonly pending = new Map<string, DirectChatCommand>();
  private readonly listeners = new Set<Listener>();
  private readonly connectionListeners = new Set<ConnectionListener>();
  private readonly readyListeners = new Set<ReadyListener>();

  /**
   * Binds the socket to one exact app installation. Changing the binding is an
   * authority transition: unaccepted transport retries are fenced, while the
   * durable event cursor remains valid for the same Human's private chat log.
   */
  bindInstallation(binding: DirectChatInstallationBinding) {
    const installationId = binding.installationId.trim();
    const authorityEpoch = binding.authorityEpoch.trim();
    if (!installationId || !/^[1-9][0-9]*$/.test(authorityEpoch))
      throw new Error("direct chat installation binding is unavailable");
    if (
      this.installationId === installationId &&
      this.authorityEpoch === authorityEpoch
    )
      return;
    this.close();
    this.installationId = installationId;
    this.authorityEpoch = authorityEpoch;
    this.reconnectAttempt = 0;
    this.pending.clear();
    this.setConnectionState("closed");
    this.setReadyState("unknown");
  }

  /**
   * Ends one installation's transport authority epoch without discarding the
   * Human's durable event cursor. A later enable of the same installation id
   * must bind and connect as a fresh epoch, and must not replay commands that
   * were never accepted before suspension.
   */
  suspendInstallation() {
    this.close();
    this.installationId = undefined;
    this.authorityEpoch = undefined;
    this.reconnectAttempt = 0;
    this.pending.clear();
    this.setConnectionState("closed");
    this.setReadyState("unknown");
  }

  connect() {
    if (!this.installationId || !this.authorityEpoch) {
      this.setConnectionState("closed");
      this.setReadyState("unknown");
      return;
    }
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
    const url = resolveDirectChatURL({
      apiBaseURL: env?.VITE_API_BASE_URL,
      authMode: env?.VITE_SUMI_AUTH_MODE,
      installationId: this.installationId,
      authorityEpoch: this.authorityEpoch,
      pageOrigin: globalThis.location?.origin,
    });
    const socket = new WebSocket(url);
    this.socket = socket;
    socket.onopen = () => {
      if (this.socket !== socket) return;
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
        this.setReadyState(
          frame.status === "ready"
            ? "ready"
            : frame.reason === "rehydrating"
              ? "rehydrating"
              : frame.reason === "stopped"
                ? "stopped"
                : "unavailable",
        );
        if (this.admissionReady) {
          // An accepted upgrade can still immediately report a failed lazy
          // runtime spawn. Only an explicit ready frame proves this connection
          // is usable enough to restart the reconnect backoff.
          this.reconnectAttempt = 0;
          this.flushPending();
        }
      }
      if (frame.type === "command_accepted")
        this.pending.delete(frame.idempotency_key);
      if (frame.type === "command_rejected")
        this.pending.delete(frame.idempotency_key);
      for (const listener of this.listeners) listener(frame);
    };
    socket.onclose = (event) => {
      if (this.socket !== socket) return;
      this.socket = undefined;
      this.admissionReady = false;
      this.setConnectionState("closed");
      // Only a cause the server states is a cause. A page cannot read the HTTP
      // status of a refused upgrade, so a logged-out session, a disallowed
      // origin, an offline network, and a DNS or TLS failure are all the same
      // unattributable close here and stay "unknown".
      this.setReadyState(
        event?.code === DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_CODE &&
          event.reason === DIRECT_CHAT_RUNTIME_UNAVAILABLE_CLOSE_REASON
          ? "not_ready"
          : "unknown",
      );
      this.scheduleReconnect();
    };
  }

  sendCommand(command: unknown, idempotencyKey = secureRandomUUID()): boolean {
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

  /**
   * Drops every piece of authority-scoped transport state. A normal close
   * preserves replay and retry state for reconnect; an identity transition
   * must not.
   */
  resetAuthority() {
    this.close();
    this.reconnectAttempt = 0;
    this.lastEventSeq = 0;
    this.pending.clear();
    this.installationId = undefined;
    this.authorityEpoch = undefined;
    this.setConnectionState("closed");
    this.setReadyState("unknown");
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
