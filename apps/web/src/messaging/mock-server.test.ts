// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import type { MessagingAPIError } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type { Message } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const GENERAL = { kind: "channel", channelId: "ch-general" } as const;
const DEV = { kind: "channel", channelId: "ch-dev" } as const;

afterEach(() => {
  vi.useRealTimers();
  bindMessagingSessionIdentity(null);
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

    await server.editMessage(DEV, other.messageId, "改ざん");
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

  it("名乗りの書き換えは他のモックへ漏れない", async () => {
    const server = new MockMessagingServer();
    const saved = await server.updateProfile({
      displayName: "余白",
      tagline: "開発",
    });
    expect(saved).toMatchObject({ displayName: "余白", tagline: "開発" });

    const own = await server.bootstrap();
    expect(own.members[0]).toMatchObject({
      displayName: "余白",
      tagline: "開発",
    });

    // 後から作った別のモックは初期状態のまま。開発用の別sessionや別テストが
    // 一方の保存に汚されない。
    const fresh = await new MockMessagingServer().bootstrap();
    expect(fresh.members[0]).toMatchObject({
      displayName: "yohaku",
      tagline: "Founder / デザイン",
    });
  });

  it("保存した名乗りをrevision付きで画面のstoreへ反映する", async () => {
    bindMessagingSessionIdentity("h-yohaku");
    installMessagingBackend(new MockMessagingServer());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));

    await useMessaging.getState().updateProfile({ displayName: "余白" });

    expect(
      useMessaging.getState().membersByKey["human:h-yohaku"],
    ).toMatchObject({
      displayName: "余白",
      revision: 1,
    });
  });
});
