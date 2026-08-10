// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  MemberProfile,
  Message,
  ParticipantKey,
  PollInput,
  MessagePoll as PollModel,
  Urgency,
} from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { Composer } from "./composer";
import { MessagePoll } from "./message-poll";

const SELF = { kind: "human", humanId: "me" } as const;
const OTHER = { kind: "human", humanId: "you" } as const;
const AGENT = {
  kind: "personality_agent",
  personalityAgentId: "sumi",
} as const;

function member(
  participant: MemberProfile["participant"],
  displayName: string,
): [ParticipantKey, MemberProfile] {
  return [
    participantKey(participant),
    { participant, displayName, tagline: "" },
  ];
}

const MEMBERS = Object.fromEntries([
  member(SELF, "わたし"),
  member(OTHER, "ハル"),
  member(AGENT, "スミ"),
]);

function poll(overrides: Partial<PollModel> = {}): PollModel {
  return {
    question: "リリースはいつにしますか？",
    allowMulti: false,
    closesAt: null,
    options: [
      { optionId: "o-1", text: "今日", voters: [OTHER, AGENT] },
      { optionId: "o-2", text: "明日", voters: [SELF] },
    ],
    ...overrides,
  };
}

function message(pollValue: PollModel | null): Message {
  return {
    messageId: "m-1",
    place: { kind: "channel", channelId: "ch-1" },
    seq: 1,
    author: OTHER,
    content: "",
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: pollValue,
    replyTo: null,
    createdAt: 0,
    editedAt: null,
    deleted: false,
  };
}

const votes: { messageId: string; optionIds: string[] }[] = [];
const sends: {
  content: string;
  urgency: Urgency;
  poll: PollInput | null | undefined;
}[] = [];

beforeEach(() => {
  votes.length = 0;
  sends.length = 0;
  act(() =>
    useMessaging.setState({
      selfKey: participantKey(SELF),
      membersByKey: MEMBERS,
      votePoll: (target, optionIds) =>
        votes.push({ messageId: target.messageId, optionIds }),
    }),
  );
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("MessagePoll", () => {
  it("票数と割合を出し、自分の選択だけを押された状態にする", () => {
    render(<MessagePoll message={message(poll())} />);

    expect(screen.getByText("リリースはいつにしますか？")).toBeInTheDocument();
    // 割合は votes/total から導く。2/3 と 1/3。
    expect(screen.getByText("2票 · 67%")).toBeInTheDocument();
    expect(screen.getByText("1票 · 33%")).toBeInTheDocument();
    // 合計とルールの説明。
    expect(screen.getByText("3票")).toBeInTheDocument();
    expect(screen.getByText("1つだけ選べます")).toBeInTheDocument();

    const [today, tomorrow] = screen.getAllByRole("button");
    expect(today).toHaveAttribute("aria-pressed", "false");
    expect(tomorrow).toHaveAttribute("aria-pressed", "true");
    // 誰が入れたかは見える（v0に匿名投票はない）。
    expect(today).toHaveAttribute("title", "ハル、スミ");
  });

  it("単一選択では選び直しが置き換えになり、同じものを押すと取り消しになる", () => {
    render(<MessagePoll message={message(poll())} />);
    const [today, tomorrow] = screen.getAllByRole("button");

    fireEvent.click(today);
    expect(votes.at(-1)).toEqual({ messageId: "m-1", optionIds: ["o-1"] });

    // 自分が既に入れている選択肢をもう一度押すのが取り消し。別の道具ではない。
    fireEvent.click(tomorrow);
    expect(votes.at(-1)).toEqual({ messageId: "m-1", optionIds: [] });
  });

  it("複数選択では既にある自分の票に足し、外すときだけ引く", () => {
    render(<MessagePoll message={message(poll({ allowMulti: true }))} />);
    expect(screen.getByText("複数選べます")).toBeInTheDocument();
    const [today, tomorrow] = screen.getAllByRole("button");

    fireEvent.click(today);
    expect(votes.at(-1)).toEqual({
      messageId: "m-1",
      optionIds: ["o-2", "o-1"],
    });

    fireEvent.click(tomorrow);
    expect(votes.at(-1)).toEqual({ messageId: "m-1", optionIds: [] });
  });

  it("締切を過ぎたら結果だけ。押せるものが残っていると嘘になる", () => {
    vi.useFakeTimers();
    vi.setSystemTime(Date.parse("2026-08-05T12:00:00Z"));
    render(
      <MessagePoll
        message={message(
          poll({ closesAt: Date.parse("2026-08-05T11:00:00Z") }),
        )}
      />,
    );

    expect(screen.getByText("締め切りました")).toBeInTheDocument();
    for (const option of screen.getAllByRole("button")) {
      expect(option).toBeDisabled();
    }
    fireEvent.click(screen.getAllByRole("button")[0]);
    expect(votes).toHaveLength(0);
    // 締め切っても結果は消えない。
    expect(screen.getByText("2票 · 67%")).toBeInTheDocument();
  });

  it("サーバーが採番する前（楽観的描画）は押せない", () => {
    render(
      <MessagePoll
        message={message(
          poll({
            options: [
              { optionId: "pending:0", text: "今日", voters: [] },
              { optionId: "pending:1", text: "明日", voters: [] },
            ],
          }),
        )}
      />,
    );

    for (const option of screen.getAllByRole("button")) {
      expect(option).toBeDisabled();
    }
    expect(screen.getByText("0票")).toBeInTheDocument();
  });

  it("問いを持たないメッセージには何も出さない", () => {
    const { container } = render(<MessagePoll message={message(null)} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("composerからの投票作成", () => {
  beforeEach(() => {
    act(() =>
      useMessaging.setState({
        activePlaceKey: "channel:ch-1",
        capabilities: {
          ...useMessaging.getState().capabilities,
          polls: true,
        },
        channels: [
          {
            channelId: "ch-1",
            workspaceId: "ws-1",
            name: "general",
            topic: "",
            visibility: "public",
          voice: false,
          },
        ],
        draftByPlace: {},
        messagesByPlace: {},
        editingMessageId: null,
        replyTargetId: null,
        send: (content, urgency, _attachments, pollInput) =>
          sends.push({ content, urgency, poll: pollInput }),
      }),
    );
  });

  it("入力欄の入口から投票を立て、通常の送信に乗せる", () => {
    render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "作成メニューを開く" }));
    fireEvent.click(screen.getByRole("button", { name: /投票を作成/ }));

    const dialog = screen.getByRole("dialog", { name: "投票を作成" });
    expect(dialog).toBeInTheDocument();
    // 質問と2つの選択肢が埋まるまで送れない。
    const submit = screen.getByRole("button", { name: "投票を送信" });
    expect(submit).toBeDisabled();

    fireEvent.change(
      screen.getByPlaceholderText("例: リリースはいつにしますか？"),
      {
        target: { value: "  リリースはいつ？  " },
      },
    );
    fireEvent.change(screen.getByLabelText("選択肢 1"), {
      target: { value: "今日" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: "明日" },
    });
    expect(submit).toBeEnabled();

    fireEvent.click(
      screen.getByRole("button", { name: "複数選べるようにする" }),
    );
    fireEvent.click(submit);

    expect(sends).toEqual([
      {
        content: "",
        urgency: "normal",
        poll: {
          question: "リリースはいつ？",
          allowMulti: true,
          closesAt: null,
          options: ["今日", "明日"],
        },
      },
    ]);
    // 送ったらダイアログは閉じる。
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("同じ選択肢は作らせない", () => {
    render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "作成メニューを開く" }));
    fireEvent.click(screen.getByRole("button", { name: /投票を作成/ }));

    fireEvent.change(
      screen.getByPlaceholderText("例: リリースはいつにしますか？"),
      {
        target: { value: "?" },
      },
    );
    fireEvent.change(screen.getByLabelText("選択肢 1"), {
      target: { value: "今日" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: " 今日 " },
    });

    expect(screen.getByText("同じ選択肢は作れません")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "投票を送信" })).toBeDisabled();
    expect(sends).toHaveLength(0);
  });

  it("締切を選ぶと相対の分数から時刻を決める", () => {
    vi.useFakeTimers();
    vi.setSystemTime(Date.parse("2026-08-05T09:00:00Z"));
    render(<Composer />);
    fireEvent.click(screen.getByRole("button", { name: "作成メニューを開く" }));
    fireEvent.click(screen.getByRole("button", { name: /投票を作成/ }));

    fireEvent.change(
      screen.getByPlaceholderText("例: リリースはいつにしますか？"),
      {
        target: { value: "続けますか？" },
      },
    );
    fireEvent.change(screen.getByLabelText("選択肢 1"), {
      target: { value: "はい" },
    });
    fireEvent.change(screen.getByLabelText("選択肢 2"), {
      target: { value: "いいえ" },
    });
    fireEvent.click(screen.getByRole("button", { name: "1時間" }));
    fireEvent.click(screen.getByRole("button", { name: "投票を送信" }));

    expect(sends[0]?.poll?.closesAt).toBe(Date.parse("2026-08-05T10:00:00Z"));
  });
});
