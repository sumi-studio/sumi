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
  envelope: { seq?: number; event: Record<string, unknown> };
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
]);
const UUIDPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const InternalIdentityOrProvenanceFields = new Set([
  "personality_agent_id",
  "conversation_id",
  "agent_id",
  "tenant_id",
  "user_id",
  "actor",
  "actor_id",
  "source",
  "source_surface",
  "workspace_id",
  "resource_id",
  "correlation_id",
  "causation_id",
  "provenance",
]);
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

// Public projections must never carry routing identity or authenticated
// provenance at their structural boundary. This deliberately does not recurse:
// tool args, review projections, and summaries are legitimate AnyJSON payloads.
function hasInternalIdentityOrProvenance(value: Record<string, unknown>): boolean {
  return Object.keys(value).some((key) => InternalIdentityOrProvenanceFields.has(key));
}

function isSafeSequence(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

/**
 * The browser may submit content and an idempotency key only. Target selection
 * and provenance are server-authenticated direct-chat concerns, so this guard
 * rejects any caller that tries to smuggle those fields through a command.
 */
export function isDirectChatCommand(value: unknown): value is DirectChatCommand {
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
    !isRecord(value.decision) ||
    typeof value.decision.type !== "string"
  ) {
    return false;
  }
  if (value.decision.type === "approve_once" || value.decision.type === "deny") {
    return hasOnlyKeys(value.decision, ["type"]);
  }
  return (
    value.decision.type === "approve_always" &&
    hasOnlyKeys(value.decision, ["type", "rule"]) &&
    isRecord(value.decision.rule)
  );
}

function isSafeMessageContent(value: unknown): boolean {
  return isRecord(value) && typeof value.type === "string" &&
    (value.type !== "text" || typeof value.text === "string");
}

function isSafeMessage(value: unknown): boolean {
  return isRecord(value) && !hasInternalIdentityOrProvenance(value) && typeof value.role === "string" &&
    Array.isArray(value.content) && value.content.every(isSafeMessageContent);
}

function isSafeEventForUI(
  value: unknown,
): value is Record<string, unknown> & { type: string } {
  if (!isRecord(value) || hasInternalIdentityOrProvenance(value) || typeof value.type !== "string" || !value.type) return false;
  if (value.type === "message_start" || value.type === "message_end") {
    return typeof value.message_id === "string" && isSafeMessage(value.message);
  }
  if (value.type === "message_update") {
    return typeof value.message_id === "string" && isRecord(value.event) &&
      typeof value.event.type === "string" &&
      (value.event.type !== "text_delta" || typeof value.event.delta === "string");
  }
  if (value.type === "tool_execution_start") {
    return typeof value.tool_call_id === "string" && typeof value.tool_name === "string";
  }
  if (value.type === "tool_execution_end") return typeof value.tool_call_id === "string";
  if (value.type === "steered") return typeof value.mode === "string";
  if (value.type === "approval_requested") {
    return isRecord(value.request) && !hasInternalIdentityOrProvenance(value.request) &&
      typeof value.request.id === "string";
  }
  if (value.type === "approval_resolved") return typeof value.request_id === "string";
  return true;
}

/** Parses only the target-free public direct-chat wire shape. */
export function parseDirectChatServerFrame(
  value: unknown,
  lastEventSeq: number,
): DirectChatServerFrame | undefined {
  if (!isRecord(value)) return undefined;
  if (value.type === "direct_chat_status" && (value.status === "ready" || value.status === "unavailable") &&
    hasOnlyKeys(value, ["type", "status"])) {
    return value as DirectChatStatusFrame;
  }
  if (value.type === "event" && isRecord(value.envelope) &&
    hasOnlyKeys(value, ["type", "envelope"])) {
    const envelope = value.envelope;
    if (!hasOnlyKeys(envelope, ["seq", "event"]) || !isSafeEventForUI(envelope.event)) {
      return undefined;
    }
    const eventType = envelope.event.type;
    if (DurableEventTypes.has(eventType)) {
      if (!isSafeSequence(envelope.seq) || envelope.seq !== lastEventSeq + 1) return undefined;
    } else if (VolatileEventTypes.has(eventType)) {
      if ("seq" in envelope) return undefined;
    } else {
      return undefined;
    }
    return value as DirectChatEventFrame;
  }
  if (value.type === "command_accepted" && typeof value.idempotency_key === "string" &&
    value.idempotency_key.length > 0 && typeof value.command_id === "string" && UUIDPattern.test(value.command_id) &&
    isSafeSequence(value.seq) && hasOnlyKeys(value, ["type", "idempotency_key", "command_id", "seq"])) {
    return value as DirectChatAcceptedFrame;
  }
  if (value.type === "command_rejected" && typeof value.reject_reason === "string" &&
    RejectReasons.has(value.reject_reason) &&
    hasOnlyKeys(value, ["type", "idempotency_key", "reject_reason"]) &&
    typeof value.idempotency_key === "string" && value.idempotency_key.length > 0) {
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
  private readonly pending = new Map<string, DirectChatCommand>();
  private readonly listeners = new Set<Listener>();
  private readonly connectionListeners = new Set<ConnectionListener>();
  private readonly readyListeners = new Set<ReadyListener>();

  connect() {
    if (this.socket) {
      const { readyState } = this.socket;
      if (readyState === WebSocket.CONNECTING || readyState === WebSocket.OPEN || readyState === WebSocket.CLOSING) return;
      this.socket = undefined;
    }
    this.clearRetry();
    this.setConnectionState("connecting");
    const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
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
      this.setReadyState("unknown");
      this.sendRaw({ type: "hello", last_event_seq: this.lastEventSeq });
      this.flushPending();
    };
    socket.onerror = () => {};
    socket.onmessage = (message) => {
      if (this.socket !== socket || socket.readyState !== WebSocket.OPEN || typeof message.data !== "string") {
        socket.close();
        return;
      }
      let raw: unknown;
      try { raw = JSON.parse(message.data); } catch { socket.close(); return; }
      const frame = parseDirectChatServerFrame(raw, this.lastEventSeq);
      if (!frame) { socket.close(); return; }
      if (frame.type === "event" && typeof frame.envelope.seq === "number") this.lastEventSeq = frame.envelope.seq;
      if (frame.type === "direct_chat_status") this.setReadyState(frame.status === "ready" ? "ready" : "not_ready");
      if (frame.type === "command_accepted") this.pending.delete(frame.idempotency_key);
      if (frame.type === "command_rejected") this.pending.delete(frame.idempotency_key);
      for (const listener of this.listeners) listener(frame);
    };
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.socket = undefined;
      this.setConnectionState("closed");
      this.setReadyState("unknown");
      this.scheduleReconnect();
    };
  }

  sendCommand(command: unknown, idempotencyKey = crypto.randomUUID()): boolean {
    if (!isDirectChatCommand(command) || !idempotencyKey || idempotencyKey.length > 1024) return false;
    this.pending.set(idempotencyKey, command);
    this.flushPending();
    return true;
  }

  pendingIdempotencyKeys(): string[] { return [...this.pending.keys()]; }
  onFrame(listener: Listener) { this.listeners.add(listener); return () => this.listeners.delete(listener); }
  onConnection(listener: ConnectionListener) { this.connectionListeners.add(listener); return () => this.connectionListeners.delete(listener); }
  onReady(listener: ReadyListener) { this.readyListeners.add(listener); return () => this.readyListeners.delete(listener); }

  close() {
    this.clearRetry();
    const socket = this.socket;
    this.socket = undefined;
    socket?.close();
  }

  private flushPending() {
    if (this.socket?.readyState !== WebSocket.OPEN) return;
    for (const [idempotency_key, command] of this.pending) {
      this.sendRaw({ type: "command", idempotency_key, command });
    }
  }
  private sendRaw(frame: unknown) {
    if (this.socket?.readyState !== WebSocket.OPEN) throw new Error("direct chat websocket is not connected");
    this.socket.send(JSON.stringify(frame));
  }
  private scheduleReconnect() {
    if (this.retry !== undefined) return;
    this.retry = setTimeout(() => this.connect(), reconnectDelay(this.reconnectAttempt++));
  }
  private setConnectionState(state: DirectChatConnectionState) { for (const listener of this.connectionListeners) listener(state); }
  private setReadyState(state: DirectChatReadyState) { for (const listener of this.readyListeners) listener(state); }
  private clearRetry() { if (this.retry !== undefined) clearTimeout(this.retry); this.retry = undefined; }
}
