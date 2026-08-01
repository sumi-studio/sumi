import { describe, expect, it } from "vitest";
import type { Message, ParticipantRef } from "./model";
import { participantKey } from "./model";
import {
  buildRows,
  GROUPING_WINDOW_MS,
  mentionCount,
  unreadCount,
  upsertMessage,
} from "./timeline";

const SELF: ParticipantRef = { kind: "human", humanId: "h-self" };
const OTHER: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a-other",
};

const BASE_AT = new Date("2026-08-01T10:00:00").getTime();

function message(overrides: Partial<Message> & { seq: number }): Message {
  return {
    messageId: `m${overrides.seq}`,
    place: { kind: "channel", channelId: "c1" },
    author: OTHER,
    content: `msg ${overrides.seq}`,
    mentions: [],
    urgency: "normal",
    reactions: [],
    replyTo: null,
    createdAt: BASE_AT + overrides.seq * 1_000,
    editedAt: null,
    deleted: false,
    ...overrides,
  };
}

describe("upsertMessage", () => {
  it("seq順を保って挿入する", () => {
    const list = [message({ seq: 1 }), message({ seq: 3 })];
    const next = upsertMessage(list, message({ seq: 2 }));
    expect(next.map((entry) => entry.seq)).toEqual([1, 2, 3]);
  });

  it("同じmessageIdは置換する（編集反映）", () => {
    const list = [message({ seq: 1 }), message({ seq: 2 })];
    const next = upsertMessage(list, message({ seq: 2, content: "edited" }));
    expect(next).toHaveLength(2);
    expect(next[1].content).toBe("edited");
  });

  it("同じseqの別IDは受け入れない", () => {
    const list = [message({ seq: 1 })];
    const next = upsertMessage(list, {
      ...message({ seq: 1 }),
      messageId: "duplicate",
    });
    expect(next).toHaveLength(1);
    expect(next[0].messageId).toBe("m1");
  });
});

describe("unreadCount / mentionCount", () => {
  const selfKey = participantKey(SELF);
  const list = [
    message({ seq: 1 }),
    message({ seq: 2, author: SELF }),
    message({ seq: 3, mentions: [SELF] }),
    message({ seq: 4, mentions: [SELF], urgency: "fyi" }),
    message({ seq: 5, deleted: true }),
  ];

  it("自分の発言・削除済みは未読に数えない", () => {
    expect(unreadCount(list, 0, selfKey)).toBe(3);
    expect(unreadCount(list, 3, selfKey)).toBe(1);
    expect(unreadCount(list, 4, selfKey)).toBe(0);
  });

  it("FYIのmentionはバッジに数えない", () => {
    expect(mentionCount(list, 0, selfKey)).toBe(1);
  });
});

describe("buildRows", () => {
  const selfKey = participantKey(SELF);

  function rows(messages: Message[], unreadLineSeq: number | null) {
    return buildRows({
      messages,
      pending: [],
      selfKey,
      unreadLineSeq,
      self: SELF,
      now: BASE_AT,
    });
  }

  it("先頭と日付変化点に日付区切りを入れる", () => {
    const result = rows(
      [
        message({ seq: 1, createdAt: BASE_AT - 24 * 60 * 60_000 }),
        message({ seq: 2, createdAt: BASE_AT }),
      ],
      null,
    );
    expect(result.map((row) => row.kind)).toEqual([
      "date",
      "message",
      "date",
      "message",
    ]);
  });

  it("7分以内の同一著者はグルーピングし、窓を超えたら切る", () => {
    const result = rows(
      [
        message({ seq: 1, createdAt: BASE_AT }),
        message({ seq: 2, createdAt: BASE_AT + 60_000 }),
        message({ seq: 3, createdAt: BASE_AT + 60_000 + GROUPING_WINDOW_MS }),
      ],
      null,
    );
    const messages = result.filter((row) => row.kind === "message");
    expect(
      messages.map((row) => row.kind === "message" && row.grouped),
    ).toEqual([false, true, false]);
  });

  it("返信と著者交代はグルーピングを切る", () => {
    const result = rows(
      [
        message({ seq: 1 }),
        message({ seq: 2, replyTo: "m1" }),
        message({ seq: 3, author: SELF, createdAt: BASE_AT + 4_000 }),
      ],
      null,
    );
    const messages = result.filter((row) => row.kind === "message");
    expect(
      messages.map((row) => row.kind === "message" && row.grouped),
    ).toEqual([false, false, false]);
  });

  it("未読ラインは他者の最初の未読の直前に一度だけ入り、自分の発言では入らない", () => {
    const result = rows(
      [
        message({ seq: 1 }),
        message({ seq: 2, author: SELF }),
        message({ seq: 3 }),
        message({ seq: 4 }),
      ],
      1,
    );
    const kinds = result.map((row) =>
      row.kind === "message" ? `msg${row.message.seq}` : row.kind,
    );
    expect(kinds).toEqual(["date", "msg1", "msg2", "unread", "msg3", "msg4"]);
  });

  it("未読ラインがグルーピングを切る", () => {
    const result = rows(
      [
        message({ seq: 1, createdAt: BASE_AT }),
        message({ seq: 2, createdAt: BASE_AT + 1_000 }),
      ],
      1,
    );
    const messages = result.filter((row) => row.kind === "message");
    expect(
      messages.map((row) => row.kind === "message" && row.grouped),
    ).toEqual([false, false]);
  });

  it("pendingは末尾に自分のメッセージとして並ぶ", () => {
    const result = buildRows({
      messages: [message({ seq: 1 })],
      pending: [
        {
          clientNonce: "n1",
          content: "sending",
          mentions: [],
          urgency: "normal",
          replyTo: null,
          createdAt: BASE_AT + 5_000,
        },
      ],
      selfKey,
      unreadLineSeq: null,
      self: SELF,
      now: BASE_AT,
    });
    const last = result[result.length - 1];
    expect(last.kind).toBe("message");
    if (last.kind === "message") {
      expect(last.pending).toBe(true);
      expect(participantKey(last.message.author)).toBe(selfKey);
    }
  });
});
