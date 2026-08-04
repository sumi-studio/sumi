// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type { Place } from "./model";
import { parsePlaceKey, placeKey } from "./model";

const CHANNEL: Place = { kind: "channel", channelId: "ch-general" };

const threadWire = {
  thread_id: "th-1",
  parent_place: { kind: "channel", channel_id: "ch-general" },
  parent_message_id: "m-7",
  workspace_id: "ws-1",
  name: "リダイレクトの件",
  message_count: 2,
  last_message_at: "2026-08-04T10:00:00Z",
  last_message: "こちらで続けます",
  participants: [{ kind: "human", human_id: "human-1" }],
  latest_seq: 2,
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("threadはplaceの一種", () => {
  it("thread:<id>としてPlaceKeyを往復する", () => {
    const place: Place = { kind: "thread", threadId: "th-1" };
    expect(placeKey(place)).toBe("thread:th-1");
    expect(parsePlaceKey("thread:th-1")).toEqual(place);
  });
});

describe("ApiMessagingBackend threads", () => {
  it("親placeのthreads口を使い、wireをThreadSummaryへ解決する", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        if (path === "/messaging/places/ch-general/threads") {
          return init?.method === "POST"
            ? json(threadWire, 201)
            : json({ threads: [threadWire] });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend();

    const [listed] = await backend.fetchThreads(CHANNEL);
    expect(listed).toEqual({
      threadId: "th-1",
      parentPlace: CHANNEL,
      parentMessageId: "m-7",
      name: "リダイレクトの件",
      messageCount: 2,
      lastMessageAt: Date.parse("2026-08-04T10:00:00Z"),
      lastMessage: "こちらで続けます",
      participants: [{ kind: "human", humanId: "human-1" }],
      latestSeq: 2,
    });

    await backend.createThread(CHANNEL, "リダイレクトの件", "m-7");
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/ch-general/threads",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        body: JSON.stringify({
          name: "リダイレクトの件",
          parent_message_id: "m-7",
        }),
      }),
    );
  });
});

describe("MockMessagingServer threads", () => {
  it("書くことが参加すること — 件数・最新行・参加者が追従する", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();

    const thread = await server.createThread(CHANNEL, "昼の相談", null);
    expect(thread.messageCount).toBe(0);
    expect(await server.fetchThreads(CHANNEL)).toEqual([thread]);

    const place: Place = { kind: "thread", threadId: thread.threadId };
    const receipt = server.sendMessage({
      place,
      content: "こちらで続けます",
      urgency: "normal",
      replyTo: null,
      clientNonce: "thread-nonce",
      attachments: [],
    });
    await vi.advanceTimersByTimeAsync(300);
    await receipt;

    const [updated] = await server.fetchThreads(CHANNEL);
    expect(updated.messageCount).toBe(1);
    expect(updated.lastMessage).toBe("こちらで続けます");
    expect(await server.fetchMessages(place)).toHaveLength(1);
  });

  it("スレッドの発言はDMとして呼ばない（脇道はチャンネルの扱い）", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();
    const thread = await server.createThread(CHANNEL, "脇道", null);
    const place: Place = { kind: "thread", threadId: thread.threadId };
    const events: string[] = [];
    server.subscribe((event) => {
      if (event.type === "message_created") {
        events.push(event.notify?.reason ?? "none");
      }
    });

    const receipt = server.sendMessage({
      place,
      content: "ひとりごと",
      urgency: "normal",
      replyTo: null,
      clientNonce: "thread-notify",
      attachments: [],
    });
    await vi.advanceTimersByTimeAsync(300);
    await receipt;

    // 自分の発言は自分を呼ばない。DM理由が紛れ込んでいないことも確かめる。
    expect(events).toEqual(["none"]);
  });
});
