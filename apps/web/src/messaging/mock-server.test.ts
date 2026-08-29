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

  it("status projection revisions advance once per participant mutation", async () => {
    const server = new MockMessagingServer();
    const first = await server.setStatus("busy", "会議中", null);
    const second = await server.setStatus("away", "外出中", null);

    expect(first.revision).toBe(1);
    expect(second.revision).toBe(2);
    const presence = await server.fetchPresence();
    expect(
      presence.statuses.find(
        (entry) =>
          entry.participant.kind === "human" &&
          entry.participant.humanId === "h-yohaku",
      ),
    ).toMatchObject({ revision: 2, status: "away" });
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

  it("projects a revisioned poll, emits a field-only vote event, and summarizes its question", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();
    const thread = await server.createThread(
      GENERAL,
      "投票スレッド",
      null,
      "thread-poll-nonce",
    );
    const place = { kind: "thread", threadId: thread.threadId } as const;
    const events: unknown[] = [];
    server.subscribe((event) => events.push(event));
    const receiptPromise = server.sendMessage({
      place,
      content: "",
      urgency: "normal",
      replyTo: null,
      clientNonce: "poll-message-nonce",
      attachments: [],
      poll: {
        question: "次の開催日は？",
        options: ["今日", "明日"],
        allowMulti: false,
        closesAt: null,
      },
    });
    await vi.advanceTimersByTimeAsync(200);
    const receipt = await receiptPromise;
    const created = (await server.fetchMessages(place)).find(
      (message) => message.messageId === receipt.messageId,
    );
    expect(created?.poll).toMatchObject({
      question: "次の開催日は？",
      revision: 0,
    });
    expect(await server.fetchThread(thread.threadId)).toMatchObject({
      lastMessage: "次の開催日は？",
    });
    const optionId = created?.poll?.options[0]?.optionId;
    if (!optionId) throw new Error("poll option missing");

    const voted = await server.votePoll(place, receipt.messageId, [optionId]);

    expect(voted.poll?.revision).toBe(1);
    expect(voted.poll?.options[0]).toMatchObject({
      optionId,
      voters: [{ kind: "human", humanId: "h-yohaku" }],
    });
    expect(events.at(-1)).toMatchObject({
      type: "poll_updated",
      place,
      messageId: receipt.messageId,
      poll: { revision: 1 },
    });
    expect(events.at(-1)).not.toHaveProperty("message");
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
  it("reconciles every place creation by operation nonce", async () => {
    const server = new MockMessagingServer();
    const channel = await server.createChannel(
      "workspace-1",
      "incident",
      "",
      false,
      "mock-create-once",
    );
    const channelReplay = await server.createChannel(
      "workspace-1",
      "incident",
      "",
      false,
      "mock-create-once",
    );
    expect(channelReplay.channelId).toBe(channel.channelId);

    const participants = [
      { kind: "human", humanId: "human-b" },
      { kind: "human", humanId: "human-c" },
    ] as const;
    const group = await server.createGroupDM(
      [...participants],
      "mock-group-once",
    );
    const groupReplay = await server.createGroupDM(
      [...participants].reverse(),
      "mock-group-once",
    );
    expect(groupReplay.dmId).toBe(group.dmId);
    await expect(
      server.createGroupDM(
        [
          { kind: "human", humanId: "human-b" },
          { kind: "human", humanId: "human-d" },
        ],
        "mock-group-once",
      ),
    ).rejects.toThrow("idempotency conflict");

    const duplicate = await server.duplicateChannel(
      "ch-general",
      "mock-duplicate-once",
    );
    const duplicateReplay = await server.duplicateChannel(
      "ch-general",
      "mock-duplicate-once",
    );
    expect(duplicateReplay.channelId).toBe(duplicate.channelId);
  });

  it("複製の既定名は本番と同じ規則で、コピーのコピーでも重ならない", async () => {
    const server = new MockMessagingServer();

    const copy = await server.duplicateChannel("ch-general", "copy-once");
    expect(copy.name).toBe("general のコピー");
    expect(copy.channelId).not.toBe("ch-general");

    const second = await server.duplicateChannel(copy.channelId, "copy-twice");
    expect(second.name).toBe("general のコピー");

    // 名前を指名したときはそれが勝つ。
    const named = await server.duplicateChannel(
      "ch-general",
      "copy-named",
      "general-2",
    );
    expect(named.name).toBe("general-2");
  });

  it("長すぎる名前は本番と同じ上限で切り詰めてから「 のコピー」を付ける", async () => {
    const server = new MockMessagingServer();
    const long = "あ".repeat(200);
    await server.updateChannel("ch-general", { name: long });

    const copy = await server.duplicateChannel("ch-general", "copy-long");
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

  it("renameを新しいeventとACKで即時に配る", async () => {
    const server = new MockMessagingServer();
    const initial = await server.bootstrap();
    const before = initial.channels.find(
      (channel) => channel.channelId === "ch-general",
    );
    const events: import("./model").ServerEvent[] = [];
    server.subscribe((event) => events.push(event));

    const renamed = await server.updateChannel("ch-general", {
      name: "即時反映",
    });
    const event = events.at(-1);

    expect(before?.name).not.toBe("即時反映");
    expect(renamed).toMatchObject({
      name: "即時反映",
      revision: (before?.revision ?? 0) + 1,
    });
    expect(event).toMatchObject({
      type: "place_updated",
      channel: {
        name: "即時反映",
        revision: (before?.revision ?? 0) + 1,
      },
    });
    if (event?.type !== "place_updated")
      throw new Error("missing rename event");
    expect(event.channel).not.toBe(renamed);
  });
});
