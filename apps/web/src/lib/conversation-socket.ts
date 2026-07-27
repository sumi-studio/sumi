import type {
  BrowserClientFrame,
  BrowserServerFrame,
  Command,
  Envelope,
} from "@sumi/api-client";

export type ConnectionState = "connecting" | "open" | "closed";

type Listener = (frame: BrowserServerFrame) => void;
type StateListener = (state: ConnectionState) => void;

// One screen owns one always-on browser WebSocket. Authentication is the
// HttpOnly session cookie attached by the browser; no agent credential is ever
// accepted or stored here.
export class ConversationSocket {
  private readonly conversationID: string;
  private socket?: WebSocket;
  private retry?: number;
  private lastEventSeq = 0;
  private readonly listeners = new Set<Listener>();
  private readonly stateListeners = new Set<StateListener>();

  constructor(conversationID: string) {
    this.conversationID = conversationID;
  }

  connect() {
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
      this.setState("open");
      this.send({ type: "hello", last_event_seq: this.lastEventSeq });
    };
    socket.onmessage = (message) => {
      const frame = JSON.parse(String(message.data)) as BrowserServerFrame;
      if (frame.type === "event" && "seq" in frame.envelope) {
        this.lastEventSeq = frame.envelope.seq;
      }
      for (const listener of this.listeners) listener(frame);
    };
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.setState("closed");
      this.retry = window.setTimeout(() => this.connect(), 500);
    };
  }

  sendCommand(command: Command, idempotencyKey = crypto.randomUUID()) {
    this.send({ type: "command", idempotency_key: idempotencyKey, command });
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
    this.socket?.close();
    this.socket = undefined;
  }

  private send(frame: BrowserClientFrame) {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      throw new Error("conversation websocket is not connected");
    }
    this.socket.send(JSON.stringify(frame));
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
