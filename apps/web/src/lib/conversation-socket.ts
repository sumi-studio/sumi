import type {
  BrowserClientFrame,
  BrowserServerFrame,
  Command,
  Envelope,
} from "@sumi/api-client";

export type ConnectionState = "connecting" | "open" | "closed";

type Listener = (frame: BrowserServerFrame) => void;
type StateListener = (state: ConnectionState) => void;

const InitialReconnectDelay = 500;
const MaxReconnectDelay = 30000;
const UUIDPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const RejectReasons = new Set([
  "unknown_command",
  "schema_violation",
  "attachments_not_empty",
  "oversized",
  "not_allowed",
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
  const jitter = Math.random() * 200;
  return Math.min(capped + jitter, MaxReconnectDelay);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSafeSequence(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function isCommand(value: unknown): boolean {
  if (!isRecord(value)) return false;
  if (value.type === "abort") return true;
  if (value.type === "user_message") {
    return (
      typeof value.text === "string" &&
      Array.isArray(value.attachments) &&
      value.attachments.length === 0
    );
  }
  if (value.type !== "approval_decision") return false;
  if (typeof value.request_id !== "string" || !isRecord(value.decision)) {
    return false;
  }
  return (
    value.decision.type === "approve_once" ||
    (value.decision.type === "approve_always" &&
      isRecord(value.decision.rule)) ||
    value.decision.type === "deny"
  );
}

function isSafeMessageContent(value: unknown): boolean {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  return value.type !== "text" || typeof value.text === "string";
}

// This validates every field consumed by the UI and every field that can
// advance the durable reconnect cursor. The server remains authoritative for
// full schema validation.
function isSafeEventForUI(
  value: unknown,
): value is Record<string, unknown> & { type: string } {
  if (!isRecord(value) || typeof value.type !== "string" || !value.type) {
    return false;
  }
  if (value.type === "message_update") {
    return (
      typeof value.message_id === "string" &&
      isRecord(value.event) &&
      typeof value.event.type === "string" &&
      (value.event.type !== "text_delta" ||
        typeof value.event.delta === "string")
    );
  }
  if (value.type === "message_end") {
    return (
      typeof value.message_id === "string" &&
      isRecord(value.message) &&
      typeof value.message.role === "string" &&
      Array.isArray(value.message.content) &&
      value.message.content.every(isSafeMessageContent)
    );
  }
  if (value.type === "tool_execution_start") {
    return (
      typeof value.tool_call_id === "string" &&
      typeof value.tool_name === "string"
    );
  }
  if (value.type === "tool_execution_end") {
    return typeof value.tool_call_id === "string";
  }
  if (value.type === "steered") {
    return typeof value.mode === "string";
  }
  if (value.type === "approval_requested") {
    return isRecord(value.request) && typeof value.request.id === "string";
  }
  if (value.type === "approval_resolved") {
    return typeof value.request_id === "string";
  }
  return true;
}

export function parseBrowserServerFrame(
  value: unknown,
  conversationID: string,
  lastEventSeq: number,
): BrowserServerFrame | undefined {
  if (!isRecord(value)) return undefined;

  if (value.type === "event") {
    if (!isRecord(value.envelope)) return undefined;
    const envelope = value.envelope;
    if (
      envelope.conversation_id !== conversationID ||
      !isSafeEventForUI(envelope.event)
    ) {
      return undefined;
    }
    const eventType = envelope.event.type;
    if (DurableEventTypes.has(eventType)) {
      if (
        !("seq" in envelope) ||
        !isSafeSequence(envelope.seq) ||
        envelope.seq !== lastEventSeq + 1
      ) {
        return undefined;
      }
    } else if (VolatileEventTypes.has(eventType)) {
      if ("seq" in envelope) return undefined;
    } else {
      return undefined;
    }
    return value as unknown as BrowserServerFrame;
  }

  if (value.type === "command_accepted") {
    if (!isRecord(value.envelope)) return undefined;
    const envelope = value.envelope;
    if (
      !isSafeSequence(envelope.seq) ||
      typeof envelope.command_id !== "string" ||
      !UUIDPattern.test(envelope.command_id) ||
      !isCommand(envelope.command)
    ) {
      return undefined;
    }
    return value as unknown as BrowserServerFrame;
  }

  if (
    value.type === "command_rejected" &&
    typeof value.reject_reason === "string" &&
    RejectReasons.has(value.reject_reason)
  ) {
    return value as unknown as BrowserServerFrame;
  }

  return undefined;
}

// One screen owns one always-on browser WebSocket. Authentication is the
// HttpOnly session cookie attached by the browser; no agent credential is ever
// accepted or stored here.
export class ConversationSocket {
  private readonly conversationID: string;
  private socket?: WebSocket;
  private retry?: number;
  private reconnectAttempt = 0;
  private lastEventSeq = 0;
  private readonly listeners = new Set<Listener>();
  private readonly stateListeners = new Set<StateListener>();

  constructor(conversationID: string) {
    this.conversationID = conversationID;
  }

  connect() {
    if (this.socket) {
      const { readyState } = this.socket;
      if (
        readyState === WebSocket.CONNECTING ||
        readyState === WebSocket.OPEN
      ) {
        return;
      }
      if (readyState === WebSocket.CLOSING) {
        // onclose will schedule the reconnect when the socket is clean.
        return;
      }
      // CLOSED: drop the stale reference and reconnect below.
      this.socket = undefined;
    }

    this.clearRetry();
    this.setState("connecting");

    const base = import.meta.env.VITE_API_BASE_URL ?? window.location.origin;
    const url = new URL(
      `/conversations/${encodeURIComponent(this.conversationID)}/ws`,
      base,
    );
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(url);
    this.socket = socket;

    socket.onopen = () => {
      this.reconnectAttempt = 0;
      this.setState("open");
      this.send({ type: "hello", last_event_seq: this.lastEventSeq });
    };
    socket.onerror = () => {
      // Browsers always follow an error with an onclose; the close path owns
      // state cleanup and reconnect scheduling.
    };
    socket.onmessage = (message) => {
      if (this.socket !== socket || socket.readyState !== WebSocket.OPEN)
        return;
      if (typeof message.data !== "string") {
        // Binary frames are not part of the browser wire contract; close and
        // let the reconnect path replay from the last consumed durable cursor.
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
      const frame = parseBrowserServerFrame(
        raw,
        this.conversationID,
        this.lastEventSeq,
      );
      if (!frame) {
        socket.close();
        return;
      }
      if (
        frame.type === "event" &&
        "seq" in frame.envelope &&
        typeof frame.envelope.seq === "number"
      ) {
        this.lastEventSeq = frame.envelope.seq;
      }
      for (const listener of this.listeners) listener(frame);
    };
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.socket = undefined;
      this.setState("closed");
      this.scheduleReconnect();
    };
  }

  sendCommand(command: Command, idempotencyKey = crypto.randomUUID()): boolean {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      return false;
    }
    this.send({ type: "command", idempotency_key: idempotencyKey, command });
    return true;
  }

  onFrame(listener: Listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  onState(listener: StateListener) {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  close() {
    this.clearRetry();
    const socket = this.socket;
    this.socket = undefined;
    socket?.close();
  }

  private send(frame: BrowserClientFrame) {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      throw new Error("conversation websocket is not connected");
    }
    this.socket.send(JSON.stringify(frame));
  }

  private scheduleReconnect() {
    if (this.retry !== undefined) return;
    const delay = reconnectDelay(this.reconnectAttempt++);
    this.retry = window.setTimeout(() => this.connect(), delay);
  }

  private setState(state: ConnectionState) {
    for (const listener of this.stateListeners) listener(state);
  }

  private clearRetry() {
    if (this.retry !== undefined) window.clearTimeout(this.retry);
    this.retry = undefined;
  }
}

export function eventType(envelope: Envelope): string {
  return (envelope.event as { type?: string }).type ?? "unknown";
}
