// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend } from "./api-backend";
import type { ServerEvent } from "./model";
import {
  expectScopedMessagingPath,
  MESSAGING_SCOPE,
  scopedMessagingTestPath,
} from "./scope.test-support";

const channel = { kind: "channel", channelId: "channel-1" } as const;
const bootstrap = {
  self: { kind: "human", human_id: "human-1" },
  workspaces: [{ workspace_id: "workspace-1", name: "Sumi" }],
  channels: [
    {
      channel_id: "channel-1",
      workspace_id: "workspace-1",
      revision: 1,
      name: "general",
      topic: "",
      visibility: "public",
      voice: false,
    },
  ],
  dms: [],
  statuses: [],
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
  // 相手のmarkerにremind_atは載らない。自分のものだけが予定を持つ。
  reply_later_markers: [
    replyLaterWire("marker-1", "human-2"),
    replyLaterWire("marker-2", "human-1", "2026-08-01T11:00:00Z"),
  ],
  notification_setting: {
    owner: { kind: "human", human_id: "human-1" },
    defaults: { level: "mentions" },
    per_place: [{ place: channelWire(), level: "mute" }],
    keywords: ["デプロイ"],
  },
};

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("ApiMessagingBackend", () => {
  it("uses the browser session REST surface for bootstrap, history, send, and read", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path.includes("/messages?") && init?.method === "GET") {
          return json({ messages: [messageWire(1, "hello")] });
        }
        if (path.endsWith("/messages") && init?.method === "POST") {
          return json(
            {
              client_nonce: "nonce-1",
              message_id: "message-2",
              seq: 2,
              created: true,
            },
            201,
          );
        }
        if (path.endsWith("/read-through") && init?.method === "PUT") {
          return new Response(null, { status: 204 });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

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
        attachments: [],
      }),
    ).resolves.toEqual({
      clientNonce: "nonce-1",
      messageId: "message-2",
      seq: 2,
      created: true,
    });
    await backend.markRead(channel, 2);

    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/places/channel-1/read-through"),
      expect.objectContaining({
        method: "PUT",
        credentials: "include",
        body: JSON.stringify({ seq: 2 }),
      }),
    );
  });

  it("requests the scoped bounded search projection", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = expectScopedMessagingPath(input);
      if (path.startsWith("/messaging/search?")) {
        return json({
          results: [
            {
              message_id: "message-7",
              place: channelWire(),
              seq: 7,
              author: { kind: "human", human_id: "human-1" },
              snippet: "明日の予定です",
              created_at: "2026-08-01T11:00:00Z",
            },
          ],
        });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(
      backend.searchMessages("予定", { place: channel, limit: 2 }),
    ).resolves.toEqual([
      expect.objectContaining({
        messageId: "message-7",
        seq: 7,
        snippet: "明日の予定です",
      }),
    ]);
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath(
        "/messaging/search?q=%E4%BA%88%E5%AE%9A&place_id=channel-1&limit=2",
      ),
      expect.any(Object),
    );
  });

  it("opens one messaging socket, sends cursors, and projects message_created", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json(bootstrap)),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
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
        const path = expectScopedMessagingPath(input);
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
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    expect(backend.capabilities.reactions).toBe(true);
    await backend.bootstrap();

    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event), {
      sinceByPlace: { "channel:channel-1": 4 },
    });

    const canonical = await backend.toggleReaction(
      channel,
      "message-1",
      "👍",
      "reaction-nonce-1",
    );
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath(
        "/messaging/places/channel-1/messages/message-1/reactions",
      ),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ emoji: "👍", client_nonce: "reaction-nonce-1" }),
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

  it("creates channels, dms, and group dms and edits topics over REST", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path === "/messaging/channels" && init?.method === "POST") {
          return json(channelSummaryWire("開発の相談", true), 201);
        }
        if (path === "/messaging/dms" && init?.method === "POST") {
          return json({
            dm_id: "dm-1",
            kind: "dm",
            participants: [
              { kind: "human", human_id: "human-1" },
              { kind: "human", human_id: "human-2" },
            ],
          });
        }
        if (path === "/messaging/group-dms" && init?.method === "POST") {
          return json(
            {
              dm_id: "group-dm-1",
              kind: "group_dm",
              participants: [
                { kind: "human", human_id: "human-1" },
                { kind: "human", human_id: "human-2" },
                { kind: "personality_agent", personality_agent_id: "agent-1" },
              ],
            },
            201,
          );
        }
        if (
          path === "/messaging/places/channel-2" &&
          init?.method === "PATCH"
        ) {
          return json(channelSummaryWire("新しいトピック"));
        }
        if (
          path === "/messaging/places/channel-2/duplicate" &&
          init?.method === "POST"
        ) {
          return json(channelSummaryWire("開発の相談"), 201);
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();

    await expect(
      backend.createChannel("workspace-1", "dev", "開発の相談", true),
    ).resolves.toEqual({
      channelId: "channel-2",
      workspaceId: "workspace-1",
      revision: 1,
      name: "dev",
      topic: "開発の相談",
      visibility: "public",
      voice: true,
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/channels"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          workspace_id: "workspace-1",
          name: "dev",
          topic: "開発の相談",
          voice: true,
        }),
      }),
    );

    await expect(
      backend.ensureDM({ kind: "human", humanId: "human-2" }),
    ).resolves.toMatchObject({ dmId: "dm-1", kind: "dm" });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/dms"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          participant: { kind: "human", human_id: "human-2" },
        }),
      }),
    );

    await expect(
      backend.createGroupDM([
        { kind: "human", humanId: "human-2" },
        { kind: "personality_agent", personalityAgentId: "agent-1" },
      ]),
    ).resolves.toMatchObject({ dmId: "group-dm-1", kind: "group_dm" });

    // 省いた項目はwireにも載せない。トピックだけの編集で名前を巻き込まない。
    await expect(
      backend.updateChannel("channel-2", { topic: "新しいトピック" }),
    ).resolves.toMatchObject({ topic: "新しいトピック" });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/places/channel-2"),
      expect.objectContaining({
        method: "PATCH",
        body: JSON.stringify({ topic: "新しいトピック" }),
      }),
    );

    // 複製の名前はサーバーが決める。クライアントは「〜 のコピー」を組み立てない。
    await expect(backend.duplicateChannel("channel-2")).resolves.toMatchObject({
      channelId: "channel-2",
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/places/channel-2/duplicate"),
      expect.objectContaining({ method: "POST", body: JSON.stringify({}) }),
    );
  });

  it("projects place_created and place_updated from the socket", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json(bootstrap)),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();
    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event));
    const socket = FakeWebSocket.instances[0];
    socket?.open();
    socket?.message({
      type: "event",
      event: {
        type: "place_created",
        place_id: "channel-2",
        channel: channelSummaryWire(""),
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "place_created",
        place_id: "dm-9",
        dm: {
          dm_id: "dm-9",
          kind: "dm",
          participants: [
            { kind: "human", human_id: "human-1" },
            { kind: "human", human_id: "human-2" },
          ],
        },
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "place_updated",
        place_id: "channel-2",
        channel: channelSummaryWire("更新後"),
      },
    });
    expect(events).toMatchObject([
      { type: "place_created", channel: { channelId: "channel-2" } },
      { type: "place_created", dm: { dmId: "dm-9", kind: "dm" } },
      { type: "place_updated", channel: { topic: "更新後" } },
    ]);
  });

  it("replays live-learned places after reconnecting", async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json(bootstrap)),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();
    backend.subscribe(() => {}, {
      sinceByPlace: { "channel:channel-1": 4 },
    });
    const firstSocket = FakeWebSocket.instances[0];
    firstSocket?.open();
    firstSocket?.message({
      type: "event",
      event: {
        type: "place_created",
        place_id: "channel-2",
        channel: channelSummaryWire(""),
      },
    });
    firstSocket?.message({
      type: "event",
      event: {
        type: "place_created",
        place_id: "dm-9",
        dm: {
          dm_id: "dm-9",
          kind: "dm",
          participants: [
            { kind: "human", human_id: "human-1" },
            { kind: "human", human_id: "human-2" },
          ],
        },
      },
    });

    firstSocket?.close();
    await vi.advanceTimersByTimeAsync(250);
    const reconnectSocket = FakeWebSocket.instances[0];
    expect(reconnectSocket).not.toBe(firstSocket);
    reconnectSocket?.open();

    expect(JSON.parse(reconnectSocket?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4, "channel-2": 0, "dm-9": 0 },
    });
  });

  it("declares and projects self-declared attention state", async () => {
    const presenceBootstrap = {
      ...bootstrap,
      statuses: [
        {
          participant: { kind: "human", human_id: "human-2" },
          status: "busy",
          note: "取り込み中",
          expires_at: null,
        },
      ],
      reply_later_markers: [
        replyLaterWire("marker-1", "human-2"),
        replyLaterWire("marker-2", "human-1", "2026-08-01T11:00:00Z"),
      ],
    };
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(presenceBootstrap);
        if (path === "/messaging/status" && init?.method === "PUT") {
          return json({
            participant: { kind: "human", human_id: "human-1" },
            status: "busy",
            note: "取り込み中",
            expires_at: "2026-08-01T11:00:00Z",
            base_status: "available",
            base_note: "",
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
            marker: {
              ...replyLaterWire("marker-3", "human-1"),
              resolved: true,
            },
          });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    const snapshot = await backend.bootstrap();
    expect(snapshot.statuses).toEqual([
      {
        participant: { kind: "human", humanId: "human-2" },
        status: "busy",
        note: "取り込み中",
        expiresAt: null,
        baseStatus: null,
        baseNote: "",
      },
    ]);
    expect(snapshot.replyLaterMarkers.map((marker) => marker.remindAt)).toEqual(
      [null, Date.parse("2026-08-01T11:00:00Z")],
    );

    // 再接続後の再同期は、bootstrapと同じ現在値をもう一度読み直す。
    await expect(backend.fetchPresence()).resolves.toEqual({
      statuses: snapshot.statuses,
      replyLaterMarkers: snapshot.replyLaterMarkers,
    });

    // mutationはserverが確定した値を返す。呼び出し側はecho待ちにならない。
    // 期限付きの申告は、戻る先までserverが確定して返す。
    const until = Date.parse("2026-08-01T11:00:00Z");
    await expect(
      backend.setStatus("busy", "取り込み中", until),
    ).resolves.toEqual({
      participant: { kind: "human", humanId: "human-1" },
      status: "busy",
      note: "取り込み中",
      expiresAt: until,
      baseStatus: "available",
      baseNote: "",
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/status"),
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          status: "busy",
          note: "取り込み中",
          expires_at: "2026-08-01T11:00:00.000Z",
        }),
      }),
    );

    const remindAt = Date.parse("2026-08-01T11:00:00Z");
    await expect(
      backend.createReplyLater(channel, "message-1", remindAt),
    ).resolves.toMatchObject({
      markerId: "marker-3",
      remindAt,
      resolved: false,
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath(
        "/messaging/places/channel-1/messages/message-1/reply-later",
      ),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ remind_at: "2026-08-01T11:00:00.000Z" }),
      }),
    );
    await expect(backend.resolveReplyLater("marker-3")).resolves.toMatchObject({
      markerId: "marker-3",
      resolved: true,
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/reply-later/marker-3/resolve"),
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
    expect(events).toMatchObject([
      { type: "status_updated", status: { status: "away" } },
      { type: "reply_later_created", marker: { markerId: "marker-4" } },
      { type: "reply_later_resolved", markerId: "marker-4" },
    ]);
  });

  it("carries the receiver's notification setting and the per-recipient notify", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (
          path === "/messaging/notification-settings" &&
          init?.method === "PUT"
        ) {
          return json({
            owner: { kind: "human", human_id: "human-1" },
            defaults: { level: "all" },
            per_place: [{ place: channelWire(), level: "mentions" }],
            keywords: ["リリース"],
          });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    expect(backend.capabilities.notifications).toBe(true);

    const snapshot = await backend.bootstrap();
    expect(snapshot.notificationSetting).toEqual({
      owner: { kind: "human", humanId: "human-1" },
      defaults: { level: "mentions" },
      perPlace: [{ place: channel, level: "mute" }],
      keywords: ["デプロイ"],
    });

    await backend.setNotificationSetting({
      defaults: { level: "all" },
      perPlace: [{ place: channel, level: "mentions" }],
      keywords: ["リリース"],
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/notification-settings"),
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          defaults: { level: "all" },
          per_place: [
            {
              place: { kind: "channel", channel_id: "channel-1" },
              level: "mentions",
            },
          ],
          keywords: ["リリース"],
        }),
      }),
    );

    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event));
    const socket = FakeWebSocket.instances[0];
    socket?.open();
    // 呼ばれた人のwireにだけ notify が載る。
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        place_id: "channel-1",
        message: messageWire(6, "@yohaku 例の件"),
        notify: { reason: "mention" },
      },
    });
    // 呼ばれていない人には無い。欠損ではなく「呼んでいない」という答え。
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        place_id: "channel-1",
        message: messageWire(7, "ふつうの発言"),
      },
    });
    expect(events).toMatchObject([
      { type: "message_created", notify: { reason: "mention" } },
      { type: "message_created", notify: null },
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

function channelSummaryWire(topic: string, voice = false) {
  return {
    channel_id: "channel-2",
    workspace_id: "workspace-1",
    revision: 1,
    name: "dev",
    topic,
    visibility: "public",
    voice,
  };
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
