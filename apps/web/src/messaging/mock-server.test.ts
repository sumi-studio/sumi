// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { Message } from "./model";

const GENERAL = { kind: "channel", channelId: "ch-general" } as const;
const DEV = { kind: "channel", channelId: "ch-dev" } as const;

afterEach(() => {
  vi.useRealTimers();
});

describe("MockMessagingServer admission", () => {
  it("client文字列の部分一致ではmentionを解決しない", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();

    const receiptPromise = server.sendMessage({
      place: GENERAL,
      content: "@Sumiya に確認します",
      urgency: "normal",
      replyTo: null,
      clientNonce: "partial-mention",
      attachments: [],
    });
    await vi.advanceTimersByTimeAsync(200);
    const receipt = await receiptPromise;
    const messages = await server.fetchMessages(GENERAL);
    const sent = messages.find(
      (message) => message.messageId === receipt.messageId,
    );

    expect(sent?.mentions).toEqual([]);
  });

  it("membership上の完全な表示名をmentionへ解決する", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();

    const receiptPromise = server.sendMessage({
      place: GENERAL,
      content: "@Sumi 確認してください",
      urgency: "normal",
      replyTo: null,
      clientNonce: "exact-mention",
      attachments: [],
    });
    await vi.advanceTimersByTimeAsync(200);
    const receipt = await receiptPromise;
    const messages = await server.fetchMessages(GENERAL);
    const sent = messages.find(
      (message) => message.messageId === receipt.messageId,
    );

    expect(sent?.mentions).toEqual([
      { kind: "personality_agent", personalityAgentId: "a-sumi" },
    ]);
  });

  it("他者のメッセージは編集・削除できない", async () => {
    const server = new MockMessagingServer();
    const before = await server.fetchMessages(DEV);
    const other = before.find(
      (message) => message.author.kind === "personality_agent",
    ) as Message;

    await server.editMessage(DEV, other.messageId, "改ざん");
    await server.deleteMessage(DEV, other.messageId);
    const after = await server.fetchMessages(DEV);
    const unchanged = after.find(
      (message) => message.messageId === other.messageId,
    );

    expect(unchanged?.content).toBe(other.content);
    expect(unchanged?.deleted).toBe(false);
  });

  it("threadへの投稿でsummaryの集計を進める", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();
    const thread = await server.createThread(
      GENERAL,
      "設計レビュー",
      null,
      "thread-aggregate-nonce",
    );
    expect(thread).toMatchObject({ messageCount: 0, lastMessage: "" });

    const place = { kind: "thread", threadId: thread.threadId } as const;
    const receiptPromise = server.sendMessage({
      place,
      content: "枝の一通目",
      urgency: "normal",
      replyTo: null,
      clientNonce: "thread-aggregate-post",
      attachments: [],
    });
    await vi.advanceTimersByTimeAsync(200);
    const receipt = await receiptPromise;

    // 実APIと同じ集計。一覧もmessage上のchipもこの数字を見ている。
    expect(await server.fetchThread(thread.threadId)).toMatchObject({
      messageCount: 1,
      lastMessage: "枝の一通目",
      latestSeq: receipt.seq,
    });
    expect(
      (await server.fetchThread(thread.threadId)).lastMessageAt,
    ).not.toBeNull();

    // tombstoneは件数から外れるが、seqは戻らない。
    await server.deleteMessage(place, receipt.messageId);
    expect(await server.fetchThread(thread.threadId)).toMatchObject({
      messageCount: 0,
      lastMessage: "",
      lastMessageAt: null,
      latestSeq: receipt.seq,
    });
  });

  it("未訪問placeを含む未読集計をbootstrapで返す", async () => {
    const server = new MockMessagingServer();
    const snapshot = await server.bootstrap();
    const dev = snapshot.unreadSummaries.find(
      (summary) =>
        summary.place.kind === "channel" &&
        summary.place.channelId === "ch-dev",
    );

    expect(dev).toMatchObject({ unreadCount: 6, mentionCount: 1 });
  });
});
