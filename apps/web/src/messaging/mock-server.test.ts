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

describe("MockMessagingServer place edits", () => {
  it("複製の既定名は本番と同じ規則で、コピーのコピーでも重ならない", async () => {
    const server = new MockMessagingServer();

    const copy = await server.duplicateChannel("ch-general");
    expect(copy.name).toBe("general のコピー");
    expect(copy.channelId).not.toBe("ch-general");

    const second = await server.duplicateChannel(copy.channelId);
    expect(second.name).toBe("general のコピー");

    // 名前を指名したときはそれが勝つ。
    const named = await server.duplicateChannel("ch-general", "general-2");
    expect(named.name).toBe("general-2");
  });

  it("長すぎる名前は本番と同じ上限で切り詰めてから「 のコピー」を付ける", async () => {
    const server = new MockMessagingServer();
    const long = "あ".repeat(200);
    await server.updateChannel("ch-general", { name: long });

    const copy = await server.duplicateChannel("ch-general");
    // 200文字の上限は places.name のCHECKそのもの。mockがここで超えた名前を
    // 返すと、手で確かめたときだけ通って本番で弾かれる。
    expect([...copy.name].length).toBe(200);
    expect(copy.name).toBe(`${"あ".repeat(195)} のコピー`);
  });

  it("何も指名しない編集は成功として返さない", async () => {
    const server = new MockMessagingServer();

    await expect(server.updateChannel("ch-general", {})).rejects.toThrow();

    const renamed = await server.updateChannel("ch-general", { name: "設計" });
    expect(renamed.name).toBe("設計");
  });
});
