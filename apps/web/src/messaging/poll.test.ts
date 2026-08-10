// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type { MessagePoll, Place } from "./model";
import { isPollClosed, pollVoteCount } from "./model";

const CHANNEL: Place = { kind: "channel", channelId: "ch-general" };

const pollWire = {
  question: "リリースはいつ？",
  allow_multi: false,
  closes_at: "2026-08-05T12:00:00Z",
  options: [
    {
      option_id: "o-1",
      text: "今日",
      voters: [{ kind: "human", human_id: "human-1" }],
    },
    { option_id: "o-2", text: "明日", voters: [] },
  ],
};

const messageWire = {
  message_id: "m-1",
  place: { kind: "channel", channel_id: "ch-general" },
  seq: 1,
  author: { kind: "human", human_id: "human-1" },
  content: "",
  mentions: [],
  attachments: [],
  urgency: "normal",
  reactions: [],
  poll: pollWire,
  reply_to: null,
  client_nonce: "n-1",
  created_at: "2026-08-05T09:00:00Z",
  edited_at: null,
  deleted: false,
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

describe("投票の読み方", () => {
  const poll: MessagePoll = {
    question: "?",
    allowMulti: false,
    closesAt: Date.parse("2026-08-05T12:00:00Z"),
    options: [
      {
        optionId: "o-1",
        text: "今日",
        voters: [
          { kind: "human", humanId: "h1" },
          { kind: "personality_agent", personalityAgentId: "a1" },
        ],
      },
      { optionId: "o-2", text: "明日", voters: [] },
    ],
  };

  it("票数はvotersから導く（別に数を持たない）", () => {
    expect(pollVoteCount(poll)).toBe(2);
  });

  it("締切を過ぎたら閉じる。締切なしは閉じない", () => {
    expect(isPollClosed(poll, Date.parse("2026-08-05T11:59:00Z"))).toBe(false);
    // 境界の瞬間は「締め切った」側。
    expect(isPollClosed(poll, Date.parse("2026-08-05T12:00:00Z"))).toBe(true);
    expect(isPollClosed({ ...poll, closesAt: null }, Date.now())).toBe(false);
  });
});

describe("ApiMessagingBackend polls", () => {
  it("投票付き送信と、回答の置き換えをwireへ写す", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        if (path.endsWith("/poll/vote")) return json({ message: messageWire });
        if (
          path === "/messaging/places/ch-general/messages" &&
          init?.method === "POST"
        ) {
          return json(
            {
              client_nonce: "n-1",
              message_id: "m-1",
              seq: 1,
              created: true,
            },
            201,
          );
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend();

    await backend.sendMessage({
      place: CHANNEL,
      content: "",
      urgency: "normal",
      replyTo: null,
      clientNonce: "n-1",
      attachments: [],
      poll: {
        question: "リリースはいつ？",
        allowMulti: true,
        closesAt: Date.parse("2026-08-05T12:00:00Z"),
        options: ["今日", "明日"],
      },
    });
    const sendBody = JSON.parse(
      String(fetchMock.mock.calls[0]?.[1]?.body ?? "{}"),
    );
    expect(sendBody.poll).toEqual({
      question: "リリースはいつ？",
      allow_multi: true,
      closes_at: "2026-08-05T12:00:00.000Z",
      options: ["今日", "明日"],
    });

    // 空配列は取り消し。同じ口を使う。
    await backend.votePoll(CHANNEL, "m-1", []);
    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/ch-general/messages/m-1/poll/vote",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ option_ids: [] }),
      }),
    );
  });
});

describe("MockMessagingServer polls", () => {
  it("単一選択は最後の1つだけ残り、空配列で取り消せる", async () => {
    vi.useFakeTimers();
    const server = new MockMessagingServer();
    const events: { type: string }[] = [];
    server.subscribe((event) => events.push(event));

    const receipt = server.sendMessage({
      place: CHANNEL,
      content: "",
      urgency: "normal",
      replyTo: null,
      clientNonce: "poll-nonce",
      attachments: [],
      poll: {
        question: "リリースはいつ？",
        allowMulti: false,
        closesAt: null,
        options: ["今日", "明日"],
      },
    });
    await vi.advanceTimersByTimeAsync(300);
    const { messageId } = await receipt;

    const poll = (await server.fetchMessages(CHANNEL)).at(-1)?.poll;
    if (!poll) throw new Error("poll did not travel with its message");
    expect(poll.options.map((option) => option.text)).toEqual(["今日", "明日"]);
    const [today, tomorrow] = poll.options.map((option) => option.optionId);

    await server.votePoll(CHANNEL, messageId, [today]);
    await server.votePoll(CHANNEL, messageId, [tomorrow]);
    const afterChange = (await server.fetchMessages(CHANNEL)).at(-1)?.poll;
    if (!afterChange) throw new Error("poll vanished");
    expect(afterChange.options[0]?.voters).toEqual([]);
    expect(afterChange.options[1]?.voters).toHaveLength(1);
    expect(pollVoteCount(afterChange)).toBe(1);

    await server.votePoll(CHANNEL, messageId, []);
    const afterWithdraw = (await server.fetchMessages(CHANNEL)).at(-1)?.poll;
    expect(pollVoteCount(afterWithdraw as MessagePoll)).toBe(0);
    expect(
      events.filter((event) => event.type === "poll_updated"),
    ).toHaveLength(3);
  });
});
