// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import type { MessagingAPIError } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type { Message } from "./model";

const GENERAL = { kind: "channel", channelId: "ch-general" } as const;
const DEV = { kind: "channel", channelId: "ch-dev" } as const;

afterEach(() => {
  vi.useRealTimers();
});

describe("MockMessagingServer admission", () => {
  it("returns the API-shaped already-sent edit rejection", async () => {
    const server = new MockMessagingServer();
    await expect(
      server.updateDraftAttachment("missing", { spoiler: true }),
    ).rejects.toEqual(
      expect.objectContaining<Partial<MessagingAPIError>>({
        code: "attachment_already_sent",
        status: 409,
      }),
    );
  });

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

    await server.editMessage(
      DEV,
      other.messageId,
      "改ざん",
      other.revision ?? 1,
    );
    await server.deleteMessage(DEV, other.messageId);
    const after = await server.fetchMessages(DEV);
    const unchanged = after.find(
      (message) => message.messageId === other.messageId,
    );

    expect(unchanged?.content).toBe(other.content);
    expect(unchanged?.deleted).toBe(false);
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
