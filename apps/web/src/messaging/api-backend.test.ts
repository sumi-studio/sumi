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
  statuses: [
    {
      participant: { kind: "human", human_id: "human-2" },
      status: "busy",
      note: "取り込み中",
      expires_at: null,
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
  // 相手のmarkerにremind_atは載らない。自分のものだけが予定を持つ。
  reply_later_markers: [
    replyLaterWire("marker-1", "human-2"),
    replyLaterWire("marker-2", "human-1", "2026-08-01T11:00:00Z"),
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

  it("toggles reactions over REST and projects reaction_updated", async () => {
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

    await backend.toggleReaction(channel, "message-1", "👍");
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/channel-1/messages/message-1/reactions",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ emoji: "👍" }),
      }),
    );

    vi.useFakeTimers();
    try {
      const events: ServerEvent[] = [];
      backend.subscribe((event) => events.push(event), {
        sinceByPlace: { "channel:channel-1": 4 },
      });
      const socket = FakeWebSocket.instances[0];
      socket?.open();
      socket?.message({
        type: "event",
        event: {
          type: "reaction_updated",
          place_id: "channel-1",
          message: messageWire(1, "hello", [
            {
              emoji: "👍",
              participants: [{ kind: "human", human_id: "human-1" }],
            },
          ]),
        },
      });
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        type: "reaction_updated",
        message: {
          seq: 1,
          reactions: [
            {
              emoji: "👍",
              participants: [{ kind: "human", humanId: "human-1" }],
            },
          ],
        },
      });
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
    } finally {
      vi.useRealTimers();
    }
  });

  it("declares and projects self-declared attention state", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path === "/messaging/status" && init?.method === "PUT") {
          return json({
            participant: { kind: "human", human_id: "human-1" },
            status: "busy",
            note: "取り込み中",
            expires_at: null,
          });
        }
        if (path.endsWith("/reply-later") && init?.method === "POST") {
          return json(
            {
              marker: replyLaterWire(
                "marker-3",
                "human-1",
                "2026-08-01T11:00:00Z",
              ),
              created: true,
            },
            201,
          );
        }
        if (path.endsWith("/resolve") && init?.method === "POST") {
          return json({
            marker: replyLaterWire("marker-3", "human-1"),
          });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend();
    expect(backend.capabilities.status).toBe(true);
    expect(backend.capabilities.replyLater).toBe(true);

    // Bootstrap は現在の status と、開いていないplaceのmarkerまで運ぶ。
    const snapshot = await backend.bootstrap();
    expect(snapshot.statuses).toEqual([
      {
        participant: { kind: "human", humanId: "human-2" },
        status: "busy",
        note: "取り込み中",
        expiresAt: null,
      },
    ]);
    expect(snapshot.replyLaterMarkers.map((m) => m.remindAt)).toEqual([
      null,
      Date.parse("2026-08-01T11:00:00Z"),
    ]);

    await backend.setStatus("busy", "取り込み中");
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/status",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ status: "busy", note: "取り込み中" }),
      }),
    );

    const remindAt = Date.parse("2026-08-01T11:00:00Z");
    await backend.createReplyLater(channel, "message-1", remindAt);
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/channel-1/messages/message-1/reply-later",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ remind_at: "2026-08-01T11:00:00.000Z" }),
      }),
    );
    await backend.resolveReplyLater("marker-3");
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/reply-later/marker-3/resolve",
      expect.objectContaining({ method: "POST" }),
    );

    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event));
    const socket = FakeWebSocket.instances[0];
    socket?.open();
    socket?.message({
      type: "event",
      event: {
        type: "status_updated",
        status: {
          participant: { kind: "human", human_id: "human-2" },
          status: "away",
          note: "",
          expires_at: "2026-08-01T12:00:00Z",
        },
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "reply_later_created",
        place_id: "channel-1",
        marker: replyLaterWire("marker-4", "human-2"),
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "reply_later_resolved",
        place_id: "channel-1",
        marker_id: "marker-4",
      },
    });
    expect(events).toEqual([
      {
        type: "status_updated",
        status: {
          participant: { kind: "human", humanId: "human-2" },
          status: "away",
          note: "",
          expiresAt: Date.parse("2026-08-01T12:00:00Z"),
        },
      },
      {
        type: "reply_later_created",
        marker: {
          markerId: "marker-4",
          participant: { kind: "human", humanId: "human-2" },
          place: channel,
          messageId: "message-1",
          note: "後で返信します",
          // 相手の約束にリマインドの時刻は付いてこない。
          remindAt: null,
          resolved: false,
        },
      },
      { type: "reply_later_resolved", markerId: "marker-4" },
    ]);
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

function replyLaterWire(markerId: string, humanId: string, remindAt?: string) {
  return {
    marker_id: markerId,
    participant: { kind: "human", human_id: humanId },
    place: channelWire(),
    message_id: "message-1",
    note: "後で返信します",
    ...(remindAt === undefined ? {} : { remind_at: remindAt }),
    resolved: false,
  };
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
