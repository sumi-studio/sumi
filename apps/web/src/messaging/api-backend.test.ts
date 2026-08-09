// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend } from "./api-backend";
import type { ServerEvent } from "./model";

const channel = { kind: "channel", channelId: "channel-1" } as const;
const bootstrap = {
  self: { kind: "human", human_id: "human-1" },
  workspaces: [{ workspace_id: "workspace-1", name: "Sumi" }],
  channels: [
    {
      channel_id: "channel-1",
      workspace_id: "workspace-1",
      name: "general",
      topic: "",
      visibility: "public",
    },
  ],
  dms: [],
  members: [
    {
      participant: { kind: "human", human_id: "human-1" },
      display_name: "Yohaku",
    },
  ],
  read_markers: [{ place: channelWire(), last_read_seq: 0 }],
  unread_summaries: [
    {
      place: channelWire(),
      latest_seq: 0,
      unread_count: 0,
      mention_count: 0,
    },
  ],
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("ApiMessagingBackend", () => {
  it("uses the browser session REST surface for bootstrap, history, send, and read", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path.includes("/messages?") && init?.method === "GET") {
          return json({ messages: [messageWire(1, "hello")] });
        }
        if (path.endsWith("/messages") && init?.method === "POST") {
          return json({ message_id: "message-2", seq: 2 }, 201);
        }
        if (path.endsWith("/read-through") && init?.method === "PUT") {
          return new Response(null, { status: 204 });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend();

    const snapshot = await backend.bootstrap();
    expect(snapshot.channels[0]?.name).toBe("general");
    expect(snapshot.self).toEqual({ kind: "human", humanId: "human-1" });
    expect(await backend.fetchMessages(channel, { limit: 50 })).toHaveLength(1);
    await expect(
      backend.sendMessage({
        place: channel,
        content: "sent",
        urgency: "normal",
        replyTo: null,
        clientNonce: "nonce-1",
      }),
    ).resolves.toEqual({ messageId: "message-2", seq: 2 });
    await backend.markRead(channel, 2);

    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/channel-1/read-through",
      expect.objectContaining({
        method: "PUT",
        credentials: "include",
        body: JSON.stringify({ seq: 2 }),
      }),
    );
  });

  it("opens one messaging socket, sends cursors, and projects message_created", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json(bootstrap)),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend();
    await backend.bootstrap();
    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event), {
      sinceByPlace: { "channel:channel-1": 4 },
    });
    const socket = FakeWebSocket.instances[0];
    expect(socket).toBeDefined();
    socket?.open();
    expect(JSON.parse(socket?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4 },
    });
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        place_id: "channel-1",
        message: messageWire(5, "live"),
      },
    });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "message_created",
      message: { seq: 5, content: "live", place: channel },
    });
  });

  it("returns the canonical REST reaction result and projects WS updates", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path.endsWith("/reactions") && init?.method === "POST") {
          return json({
            message: messageWire(1, "hello", [
              {
                emoji: "👍",
                participants: [{ kind: "human", human_id: "human-1" }],
              },
            ]),
            reacted: true,
          });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend();
    expect(backend.capabilities.reactions).toBe(true);
    await backend.bootstrap();

    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event), {
      sinceByPlace: { "channel:channel-1": 4 },
    });

    const canonical = await backend.toggleReaction(channel, "message-1", "👍");
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/channel-1/messages/message-1/reactions",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ emoji: "👍" }),
      }),
    );
    expect(canonical).toEqual({
      messageId: "message-1",
      reactions: [
        {
          emoji: "👍",
          participants: [{ kind: "human", humanId: "human-1" }],
        },
      ],
    });
    // REST and WS have different ordering semantics. The store coordinates
    // the canonical ACK with live events, so the backend must not disguise
    // the ACK as another live event.
    expect(events).toEqual([]);

    vi.useFakeTimers();
    try {
      const socket = FakeWebSocket.instances[0];
      socket?.open();
      socket?.message({
        type: "event",
        event: {
          type: "reaction_updated",
          place_id: "channel-1",
          reaction: {
            message_id: "message-1",
            reactions: [
              {
                emoji: "👍",
                participants: [{ kind: "human", human_id: "human-1" }],
              },
            ],
          },
        },
      });
      // The event is a reaction-only patch: it carries no content, so it can
      // never roll back an edit that raced it.
      expect(events).toEqual([
        {
          type: "reaction_updated",
          place: channel,
          messageId: "message-1",
          reactions: [
            {
              emoji: "👍",
              participants: [{ kind: "human", humanId: "human-1" }],
            },
          ],
        },
      ]);
      events.length = 0;
      // Clearing the last reaction arrives as an empty set, not an omission.
      socket?.message({
        type: "event",
        event: {
          type: "reaction_updated",
          place_id: "channel-1",
          reaction: { message_id: "message-1", reactions: [] },
        },
      });
      expect(events).toEqual([
        {
          type: "reaction_updated",
          place: channel,
          messageId: "message-1",
          reactions: [],
        },
      ]);
      events.length = 0;
      // A reaction to an old message must not rewind the replay cursor: the
      // reconnect hello still asks for everything after seq 4.
      socket?.close();
      vi.advanceTimersByTime(300);
      const reconnected = FakeWebSocket.instances[0];
      expect(reconnected).not.toBe(socket);
      reconnected?.open();
      expect(JSON.parse(reconnected?.sent[0] ?? "{}")).toEqual({
        type: "hello",
        cursors: { "channel-1": 4 },
      });
      // caught_up is surfaced so the subscriber can re-read its loaded window:
      // reactions below the cursor are never replayed by catch-up.
      reconnected?.message({
        type: "caught_up",
        place_id: "channel-1",
        latest_seq: 9,
      });
      expect(events).toEqual([{ type: "caught_up", place: channel }]);
    } finally {
      vi.useRealTimers();
    }
  });
});

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 3;
  static instances: FakeWebSocket[] = [];
  readyState = FakeWebSocket.CONNECTING;
  readonly sent: string[] = [];
  readonly url: string | URL;

  constructor(url: string | URL) {
    super();
    this.url = url;
    FakeWebSocket.instances = [this];
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
  }

  message(value: unknown): void {
    this.dispatchEvent(
      new MessageEvent("message", { data: JSON.stringify(value) }),
    );
  }
}

function channelWire() {
  return { kind: "channel", channel_id: "channel-1" };
}

function messageWire(
  seq: number,
  content: string,
  reactions: { emoji: string; participants: unknown[] }[] = [],
) {
  return {
    message_id: `message-${seq}`,
    place: channelWire(),
    seq,
    author: { kind: "human", human_id: "human-1" },
    content,
    mentions: [],
    urgency: "normal",
    reactions,
    reply_to: null,
    client_nonce: `nonce-${seq}`,
    created_at: "2026-08-01T10:00:00Z",
    edited_at: null,
    deleted: false,
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
