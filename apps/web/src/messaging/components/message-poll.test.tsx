// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Message, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { MessagePoll } from "./message-poll";

const SELF: ParticipantRef = { kind: "human", humanId: "self" };
const OTHER: ParticipantRef = { kind: "human", humanId: "other" };
const votePoll = vi.fn();

function message(closesAt: number | null = null): Message {
  return {
    messageId: "message-1",
    place: { kind: "channel", channelId: "general" },
    seq: 1,
    author: OTHER,
    content: "",
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: {
      question: "いつ出しますか？",
      allowMulti: false,
      closesAt,
      options: [
        { optionId: "today", text: "今日", voters: [SELF] },
        { optionId: "tomorrow", text: "明日", voters: [OTHER] },
      ],
    },
    replyTo: null,
    createdAt: Date.now(),
    editedAt: null,
    deleted: false,
  };
}

beforeEach(() => {
  votePoll.mockReset();
  useMessaging.setState({
    selfKey: participantKey(SELF),
    membersByKey: {
      [participantKey(SELF)]: {
        participant: SELF,
        displayName: "Yohaku",
        tagline: "",
      },
      [participantKey(OTHER)]: {
        participant: OTHER,
        displayName: "Haru",
        tagline: "",
      },
    },
    votePoll,
  });
});

afterEach(cleanup);

describe("MessagePoll", () => {
  it("単一選択は全体置換として送り、同じ選択の再クリックは取り下げる", () => {
    const value = message();
    render(<MessagePoll message={value} />);

    fireEvent.click(screen.getByRole("button", { name: /明日/ }));
    expect(votePoll).toHaveBeenLastCalledWith(value, ["tomorrow"]);

    fireEvent.click(screen.getByRole("button", { name: /今日/ }));
    expect(votePoll).toHaveBeenLastCalledWith(value, []);
  });

  it("投票者を表示用titleに持ち、締切後は結果だけになる", () => {
    render(<MessagePoll message={message(Date.now() - 1)} />);

    expect(screen.getByRole("button", { name: /今日/ })).toHaveAttribute(
      "title",
      "Yohaku",
    );
    expect(
      screen
        .getAllByRole("button")
        .every((button) => button.hasAttribute("disabled")),
    ).toBe(true);
    expect(screen.getByText("締め切りました")).toBeInTheDocument();
  });
});
