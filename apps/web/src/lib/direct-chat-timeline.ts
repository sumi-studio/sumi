import type { DirectChatEventFrame, DirectChatServerFrame } from "./direct-chat-socket";

export type FeedItem = {
  id: string;
  kind: "user" | "assistant" | "tool" | "steer" | "approval" | "system";
  text: string;
  requestID?: string;
};

function eventType(frame: DirectChatEventFrame): string {
  return String(frame.envelope.event.type);
}

function messageText(message: Record<string, unknown>): string {
  if (!Array.isArray(message.content)) return "";
  return message.content
    .filter((content): content is Record<string, unknown> => typeof content === "object" && content !== null && !Array.isArray(content))
    .filter((content) => content.type === "text" && typeof content.text === "string")
    .map((content) => String(content.text))
    .join("");
}

/** Keeps durable replay idempotent while allowing short-lived stream previews. */
export class DirectChatTimeline {
  private feed: FeedItem[] = [];
  private readonly completedMessages = new Set<string>();
  private readonly seenDurableEvents = new Set<number>();
  private streamingMessageID?: string;
  private streamingText = "";

  items(): FeedItem[] { return this.feed; }

  apply(frame: DirectChatServerFrame): FeedItem[] {
    if (frame.type === "command_rejected") {
      this.appendOnce(`reject-${frame.idempotency_key}`, { id: `reject-${frame.idempotency_key}`, kind: "system", text: `Command rejected: ${frame.reject_reason}` });
      return this.feed;
    }
    if (frame.type !== "event") return this.feed;
    if (typeof frame.envelope.seq === "number") {
      if (this.seenDurableEvents.has(frame.envelope.seq)) return this.feed;
      this.seenDurableEvents.add(frame.envelope.seq);
    }
    const event = frame.envelope.event;
    const type = eventType(frame);
    if (type === "message_update") {
      const messageID = String(event.message_id);
      if (this.completedMessages.has(messageID)) return this.feed;
      const stream = event.event as Record<string, unknown>;
      if (stream.type === "text_delta" && typeof stream.delta === "string") {
        if (this.streamingMessageID !== messageID) { this.streamingMessageID = messageID; this.streamingText = ""; }
        this.streamingText += stream.delta;
        this.replace({ id: `stream-${messageID}`, kind: "assistant", text: this.streamingText });
      }
      return this.feed;
    }
    if (type === "message_start" || type === "message_end") {
      const messageID = String(event.message_id);
      const message = event.message as Record<string, unknown>;
      const role = message.role === "user" ? "user" : message.role === "assistant" ? "assistant" : undefined;
      const text = messageText(message);
      if (role && text) this.replace({ id: `message-${messageID}`, kind: role, text });
      if (type === "message_end") {
        this.completedMessages.add(messageID);
        this.remove(`stream-${messageID}`);
        if (this.streamingMessageID === messageID) { this.streamingMessageID = undefined; this.streamingText = ""; }
      }
      return this.feed;
    }
    if (type === "tool_execution_start" || type === "tool_execution_end") {
      const callID = String(event.tool_call_id);
      this.replace({ id: `tool-${callID}`, kind: "tool", text: type === "tool_execution_start" ? `Tool started: ${String(event.tool_name)}` : `Tool finished: ${callID}` });
    } else if (type === "steered") {
      this.appendOnce(`event-${frame.envelope.seq}`, { id: `event-${frame.envelope.seq}`, kind: "steer", text: String(event.mode) });
    } else if (type === "approval_requested") {
      const request = event.request as Record<string, unknown>;
      const requestID = String(request.id);
      this.replace({ id: `approval-${requestID}`, kind: "approval", requestID, text: JSON.stringify(request.action ?? request) });
    } else if (type === "approval_resolved") {
      this.remove(`approval-${String(event.request_id)}`);
    }
    return this.feed;
  }

  private appendOnce(id: string, item: FeedItem) { if (!this.feed.some((entry) => entry.id === id)) this.feed = [...this.feed, item]; }
  private replace(item: FeedItem) { this.feed = [...this.feed.filter((entry) => entry.id !== item.id), item]; }
  private remove(id: string) { this.feed = this.feed.filter((entry) => entry.id !== id); }
}
