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
  it("returns the existing thread carried by a thread_exists conflict", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (
          path === "/messaging/places/channel-1/threads" &&
          init?.method === "POST"
        ) {
          return json(
            { error: "thread_exists", thread: threadSummaryWire("thread-1") },
            409,
          );
        }
        throw new Error(`unexpected request ${path}`);
      }),
    );
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(
      backend.createThread(channel, "すでにある枝", "message-1", "new-nonce"),
    ).resolves.toMatchObject({
      threadId: "thread-1",
      parentPlace: channel,
    });
  });

  it("rejects zero message revisions before they can become a CAS base", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({ messages: [{ ...messageWire(1, "invalid"), revision: 0 }] }),
      ),
    );
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(backend.fetchMessages(channel)).rejects.toThrow(
      "invalid messaging revision",
    );
  });

  it("rejects attachment wires that omit required spoiler or alt declarations", async () => {
    for (const missing of ["spoiler", "alt"] as const) {
      const attachment: Record<string, unknown> = {
        attachment_id: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        filename: "shot.png",
        mime: "image/png",
        size_bytes: 3,
        sha256: "ab",
        position: 0,
        spoiler: false,
        alt: "",
      };
      delete attachment[missing];
      vi.stubGlobal(
        "fetch",
        vi.fn(async () => json({ attachment, created: true }, 201)),
      );
      const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
      await expect(
        backend.uploadAttachment({
          place: channel,
          clientNonce: `missing-${missing}`,
          filename: "shot.png",
          contentType: "image/png",
          body: new Blob(["png"]),
        }),
      ).rejects.toThrow("invalid messaging response");
    }
  });

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

  it("keeps the server current message on an edit conflict", async () => {
    const current = {
      ...messageWire(1, "サーバで確定した本文"),
      edited_at: "2026-08-18T12:00:00Z",
      revision: 2,
    };
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = expectScopedMessagingPath(input);
      if (path.endsWith("/messages/message-1")) {
        return json({ error: "edit_conflict", message: current }, 409);
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(
      backend.editMessage(channel, "message-1", "古い書きかけ", 1),
    ).rejects.toMatchObject({
      code: "edit_conflict",
      status: 409,
      currentMessage: expect.objectContaining({
        content: "サーバで確定した本文",
        revision: 2,
      }),
    });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/places/channel-1/messages/message-1"),
      expect.objectContaining({
        method: "PATCH",
        body: JSON.stringify({ content: "古い書きかけ", revision: 1 }),
      }),
    );
  });

  it("keeps the terminal tombstone on a deleted edit target", async () => {
    const deleted = {
      ...messageWire(1, ""),
      deleted: true,
      revision: 2,
    };
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = expectScopedMessagingPath(input);
      if (path.endsWith("/messages/message-1")) {
        return json({ error: "message_deleted", message: deleted }, 409);
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(
      backend.editMessage(channel, "message-1", "古い書きかけ", 1),
    ).rejects.toMatchObject({
      code: "message_deleted",
      status: 409,
      responseMessage: expect.objectContaining({
        deleted: true,
        revision: 2,
      }),
    });
  });

  it("returns the committed message from a successful edit", async () => {
    const committed = {
      ...messageWire(1, "サーバで確定した本文"),
      edited_at: "2026-08-18T12:00:00Z",
      revision: 2,
    };
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = expectScopedMessagingPath(input);
      if (path.endsWith("/messages/message-1")) {
        return json({ message: committed });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(
      backend.editMessage(channel, "message-1", "サーバで確定した本文", 1),
    ).resolves.toMatchObject({
      messageId: "message-1",
      content: "サーバで確定した本文",
      revision: 2,
    });
  });

  it("returns the revisioned tombstone from DELETE", async () => {
    const deleted = {
      ...messageWire(1, ""),
      deleted: true,
      revision: 2,
    };
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path.endsWith("/messages/message-1") && init?.method === "DELETE") {
          return json({ message: deleted });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);

    await expect(
      backend.deleteMessage(channel, "message-1"),
    ).resolves.toMatchObject({
      deleted: true,
      revision: 2,
    });
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

  it("routes a reaction for an unjoined thread immediately after its first message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({
          ...bootstrap,
          // threads is intentionally empty: bootstrap projects joined threads
          // only, and this thread has no pre-existing unread summary either.
          threads: [],
        }),
      ),
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
        type: "message_created",
        message: messageWire(
          1,
          "最初の返信",
          [],
          threadPlaceWire("thread-unjoined"),
        ),
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "reaction_updated",
        place_id: "thread-unjoined",
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

    expect(events).toMatchObject([
      {
        type: "message_created",
        message: { place: { kind: "thread", threadId: "thread-unjoined" } },
      },
      {
        type: "reaction_updated",
        place: { kind: "thread", threadId: "thread-unjoined" },
        messageId: "message-1",
      },
    ]);
  });

  it("routes a reaction for an unjoined thread named only by an unread summary", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({
          ...bootstrap,
          threads: [],
          unread_summaries: [
            ...bootstrap.unread_summaries,
            {
              place: threadPlaceWire("thread-summary-only"),
              latest_seq: 1,
              unread_count: 1,
              mention_count: 0,
            },
          ],
        }),
      ),
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
        type: "reaction_updated",
        place_id: "thread-summary-only",
        reaction: { message_id: "message-1", reactions: [] },
      },
    });

    expect(events).toEqual([
      {
        type: "reaction_updated",
        place: { kind: "thread", threadId: "thread-summary-only" },
        messageId: "message-1",
        reactions: [],
      },
    ]);
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
      backend.createChannel(
        "workspace-1",
        "dev",
        "開発の相談",
        true,
        "create-channel-gesture",
      ),
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
          client_nonce: "create-channel-gesture",
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
      backend.createGroupDM(
        [
          { kind: "personality_agent", personalityAgentId: "agent-1" },
          { kind: "human", humanId: "human-2" },
        ],
        "create-group-gesture",
      ),
    ).resolves.toMatchObject({ dmId: "group-dm-1", kind: "group_dm" });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/group-dms"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          participants: [
            { kind: "human", human_id: "human-2" },
            {
              kind: "personality_agent",
              personality_agent_id: "agent-1",
            },
          ],
          client_nonce: "create-group-gesture",
        }),
      }),
    );

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
    await expect(
      backend.duplicateChannel("channel-2", "duplicate-channel-gesture"),
    ).resolves.toMatchObject({ channelId: "channel-2" });
    expect(fetchMock).toHaveBeenCalledWith(
      scopedMessagingTestPath("/messaging/places/channel-2/duplicate"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          client_nonce: "duplicate-channel-gesture",
        }),
      }),
    );
  });

  it("retries an ambiguous committed place creation once with the same nonce", async () => {
    const creationBodies: string[] = [];
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path === "/messaging/channels" && init?.method === "POST") {
          creationBodies.push(String(init.body));
          if (creationBodies.length === 1) {
            // The server committed, but no response reached the browser.
            throw new TypeError("response lost after commit");
          }
          return json(channelSummaryWire("reconciled"), 200);
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();

    await expect(
      backend.createChannel(
        "workspace-1",
        "incident",
        "reconciled",
        false,
        "stable-logical-gesture",
      ),
    ).resolves.toMatchObject({ channelId: "channel-2" });
    expect(creationBodies).toEqual([
      JSON.stringify({
        workspace_id: "workspace-1",
        name: "incident",
        topic: "reconciled",
        voice: false,
        client_nonce: "stable-logical-gesture",
      }),
      JSON.stringify({
        workspace_id: "workspace-1",
        name: "incident",
        topic: "reconciled",
        voice: false,
        client_nonce: "stable-logical-gesture",
      }),
    ]);
  });

  it.each([
    502, 503, 408, 429,
  ])("reconciles an ambiguous HTTP %i once with the same request", async (status) => {
    const bodies: string[] = [];
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path === "/messaging/group-dms" && init?.method === "POST") {
          bodies.push(String(init.body));
          if (bodies.length === 1) {
            return json({ error: "intermediary_response_lost" }, status);
          }
          return json(
            {
              dm_id: "group-dm-reconciled",
              kind: "group_dm",
              participants: [
                { kind: "human", human_id: "human-1" },
                { kind: "human", human_id: "human-2" },
                {
                  kind: "personality_agent",
                  personality_agent_id: "agent-1",
                },
              ],
            },
            200,
          );
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();

    await expect(
      backend.createGroupDM(
        [
          { kind: "human", humanId: "human-2" },
          { kind: "personality_agent", personalityAgentId: "agent-1" },
        ],
        "ambiguous-http-status",
      ),
    ).resolves.toMatchObject({ dmId: "group-dm-reconciled" });
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toBe(bodies[0]);
  });

  it("stops after one reconciliation when both responses are ambiguous", async () => {
    const bodies: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path === "/messaging/channels" && init?.method === "POST") {
          bodies.push(String(init.body));
          return json({ error: "upstream_response_unknown" }, 503);
        }
        throw new Error(`unexpected request ${path}`);
      }),
    );
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();

    await expect(
      backend.createChannel(
        "workspace-1",
        "bounded",
        "",
        false,
        "bounded-reconciliation",
      ),
    ).rejects.toMatchObject({ status: 503 });
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toBe(bodies[0]);
  });

  it.each([
    400, 403, 404, 409,
  ])("does not retry a definitive pre-mutation HTTP %i rejection", async (status) => {
    let attempts = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (
          path === "/messaging/places/channel-1/duplicate" &&
          init?.method === "POST"
        ) {
          attempts += 1;
          return json({ error: "definitive_rejection" }, status);
        }
        throw new Error(`unexpected request ${path}`);
      }),
    );
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();

    await expect(
      backend.duplicateChannel("channel-1", "definitive-http-status"),
    ).rejects.toMatchObject({ status });
    expect(attempts).toBe(1);
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

  it("keeps live-learned places out of the handshake until they are held", async () => {
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

    // 履歴を持っていない場所にreplayさせるものは無い。その未読はreconnect後の
    // bootstrap snapshotが直すので、握手はstoreが宣言した場所のままでよい。
    expect(JSON.parse(reconnectSocket?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4 },
    });
  });

  it("declares and projects self-declared attention state", async () => {
    const presenceBootstrap = {
      ...bootstrap,
      statuses: [
        {
          participant: { kind: "human", human_id: "human-2" },
          revision: 5,
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
            revision: 6,
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
        revision: 5,
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
      clearedStatuses: [],
      replyLaterMarkers: snapshot.replyLaterMarkers,
    });

    // mutationはserverが確定した値を返す。呼び出し側はecho待ちにならない。
    // 期限付きの申告は、戻る先までserverが確定して返す。
    const until = Date.parse("2026-08-01T11:00:00Z");
    await expect(
      backend.setStatus("busy", "取り込み中", until),
    ).resolves.toEqual({
      participant: { kind: "human", humanId: "human-1" },
      revision: 6,
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
          revision: 7,
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

  it("keeps announced and listed threads out of the handshake", async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path === "/messaging/places/channel-1/threads") {
          return json({
            threads: [
              threadSummaryWire("thread-listed-1"),
              threadSummaryWire("thread-listed-2"),
            ],
          });
        }
        throw new Error(`unexpected request ${path}`);
      }),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();
    backend.subscribe(() => {}, { sinceByPlace: { "channel:channel-1": 4 } });
    const socket = FakeWebSocket.instances[0];
    socket?.open();

    // 一覧で見えたthreadも、親channelへ告知されただけのthreadも、自分の
    // 台帳ではない。cursorにすると次のhandshakeが上限で撥ねられる。
    expect(await backend.fetchThreads(channel)).toHaveLength(2);
    socket?.message({
      type: "event",
      event: {
        type: "place_created",
        place_id: "thread-announced",
        thread: threadSummaryWire("thread-announced"),
      },
    });

    socket?.close();
    await vi.advanceTimersByTimeAsync(250);
    const reconnected = FakeWebSocket.instances[0];
    reconnected?.open();
    expect(JSON.parse(reconnected?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4 },
    });
  });

  it("drops a visited place's cursor when it is closed, and keeps a held one", async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json(bootstrap)),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();
    // storeが宣言するのは履歴を持っている場所——参加しているthreadも例外では
    // なく、開いて読み込んだからここに居る。
    backend.subscribe(() => {}, {
      sinceByPlace: { "channel:channel-1": 4, "thread:thread-held": 7 },
    });
    const socket = FakeWebSocket.instances[0];
    socket?.open();

    // 開いている間だけ購読するthreadはcursorも借り物で、閉じれば返す。
    backend.openPlace({ kind: "thread", threadId: "thread-visiting" }, 3);
    backend.openPlace(null);

    socket?.close();
    await vi.advanceTimersByTimeAsync(250);
    const reconnected = FakeWebSocket.instances[0];
    reconnected?.open();
    expect(JSON.parse(reconnected?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4, "thread-held": 7 },
    });
  });

  it("does not replay an active cursor after its history request fails", async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const path = expectScopedMessagingPath(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path.includes("/messages?")) {
          throw new Error("history request timed out");
        }
        throw new Error(`unexpected request ${path}`);
      }),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await backend.bootstrap();
    backend.subscribe(() => {}, { sinceByPlace: { "channel:channel-1": 4 } });
    const socket = FakeWebSocket.instances[0];
    socket?.open();

    const thread = {
      kind: "thread",
      threadId: "thread-history-failed",
    } as const;
    // selectPlace declares the active delivery scope before its REST history
    // promise resolves. The store releases that history on failure.
    backend.openPlace(thread, 12);
    await expect(backend.fetchMessages(thread, { limit: 50 })).rejects.toThrow(
      "history request timed out",
    );
    backend.releasePlace(thread);

    socket?.close();
    await vi.advanceTimersByTimeAsync(250);
    const reconnected = FakeWebSocket.instances[0];
    reconnected?.open();
    expect(JSON.parse(reconnected?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4 },
    });
    // The screen remains selected, so it is re-declared with an empty cursor
    // instead of the stale pre-failure seq 12.
    expect(JSON.parse(reconnected?.sent[1] ?? "{}")).toEqual({
      type: "open",
      place_id: "thread-history-failed",
      since: 0,
    });
  });

  it("keeps the handshake independent of how many places the Workspace holds", async () => {
    // 作成者は自分が作ったthreadの参加者になる。作った数だけ参加threadが
    // 増えても、握手はその数に比例してはならない。
    const threads = Array.from(
      { length: 1200 },
      (_, index) => `thread-${index}`,
    );
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({
          ...bootstrap,
          threads: threads.map((threadId) => threadSummaryWire(threadId)),
          unread_summaries: [
            ...bootstrap.unread_summaries,
            ...threads.map((threadId) => ({
              place: threadPlaceWire(threadId),
              latest_seq: 3,
              unread_count: 1,
              mention_count: 0,
            })),
          ],
        }),
      ),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    const snapshot = await backend.bootstrap();
    expect(snapshot.unreadSummaries).toHaveLength(1201);
    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event), {
      sinceByPlace: { "channel:channel-1": 4 },
    });
    const socket = FakeWebSocket.instances[0];
    socket?.open();

    // maxHelloCursors(1024)を優に超える参加threadがあっても、握手が運ぶのは
    // このclientが履歴を持っている場所だけ。
    expect(JSON.parse(socket?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4 },
    });
    // 配送は参加で決まるので、cursorを持たないthreadのeventも届く。
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        message: messageWire(
          4,
          "1000番目の枝",
          [],
          threadPlaceWire("thread-999"),
        ),
      },
    });
    expect(events).toMatchObject([
      { type: "message_created", message: { seq: 4, content: "1000番目の枝" } },
    ]);
  });

  it("declares the open place, re-declares it after reconnecting, and closes it", async () => {
    vi.useFakeTimers();
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
    socket?.open();

    backend.openPlace({ kind: "thread", threadId: "thread-open" });
    // 宣言は「ここまで持っている」を運ぶ。開く画面はRESTで履歴を取ってから
    // 開くので、その取得とこの宣言の隙間はserverがここから replay して埋める。
    expect(JSON.parse(socket?.sent[1] ?? "{}")).toEqual({
      type: "open",
      place_id: "thread-open",
      since: 0,
    });
    // 宣言の受領確認は状態を持たない。捨てられるだけで、eventにはならない。
    socket?.message({ type: "open_ack", place_id: "thread-open" });
    expect(events).toEqual([]);

    // 新しいsocketは何も開いていない状態から始まる。画面はそのままなので、
    // helloの直後に宣言し直す。開いている場所はreplay対象でもある。
    socket?.close();
    await vi.advanceTimersByTimeAsync(250);
    const reconnected = FakeWebSocket.instances[0];
    reconnected?.open();
    expect(JSON.parse(reconnected?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4, "thread-open": 0 },
    });
    expect(JSON.parse(reconnected?.sent[1] ?? "{}")).toEqual({
      type: "open",
      place_id: "thread-open",
      since: 0,
    });

    backend.openPlace(null);
    expect(JSON.parse(reconnected?.sent[2] ?? "{}")).toEqual({
      type: "close",
      place_id: "thread-open",
    });
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

function threadPlaceWire(threadId: string) {
  return { kind: "thread", thread_id: threadId };
}

function threadSummaryWire(threadId: string) {
  return {
    thread_id: threadId,
    revision: 1,
    parent_place: channelWire(),
    parent_message_id: "message-1",
    workspace_id: "workspace-1",
    name: threadId,
    message_count: 1,
    last_message_at: "2026-08-01T11:00:00Z",
    last_message: "返信",
    participants: [{ kind: "human", human_id: "human-2" }],
    latest_seq: 1,
  };
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
  place: Record<string, string> = channelWire(),
) {
  return {
    message_id: `message-${seq}`,
    place,
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
    revision: 1,
    deleted: false,
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
