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
import type { Message, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { MessagePoll } from "./message-poll";

const SELF: ParticipantRef = { kind: "human", humanId: "self" };
const OTHER: ParticipantRef = { kind: "human", humanId: "other" };
const votePoll = vi.fn();

function optionButton(text: string): HTMLButtonElement {
  const button = screen
    .getAllByRole<HTMLButtonElement>("button")
    .find(
      (candidate) =>
        candidate.hasAttribute("aria-pressed") &&
        candidate.textContent?.includes(text),
    );
  if (!button) throw new Error(`missing poll option: ${text}`);
  return button;
}

function message(closesAt: number | null = null, allowMulti = false): Message {
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
      allowMulti,
      closesAt,
      revision: 3,
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
  votePoll.mockImplementation(
    async (_message: Message, optionIds: string[]) => {
      useMessaging.setState((state) => ({
        pollVoteByMessage: {
          ...state.pollVoteByMessage,
          "message-1": {
            optionIds: [...optionIds],
            intent: (state.pollVoteByMessage["message-1"]?.intent ?? 0) + 1,
            pending: true,
            failed: false,
          },
        },
      }));
    },
  );
  useMessaging.setState((state) => ({
    selfKey: participantKey(SELF),
    capabilities: { ...state.capabilities, polls: true },
    pollVoteByMessage: {},
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
  }));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("MessagePoll", () => {
  it("single-select sends a whole replacement and clicking mine withdraws", () => {
    const value = message();
    render(<MessagePoll message={value} />);

    const mine = optionButton("今日");
    expect(mine).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(mine);
    expect(votePoll).toHaveBeenLastCalledWith(value, []);
  });

  it("rapid multi-select clicks base the second intent on the first local intent", () => {
    const value = message(null, true);
    render(<MessagePoll message={value} />);

    fireEvent.click(optionButton("明日"));
    fireEvent.click(optionButton("今日"));

    expect(votePoll).toHaveBeenNthCalledWith(1, value, ["today", "tomorrow"]);
    expect(votePoll).toHaveBeenNthCalledWith(2, value, ["tomorrow"]);
  });

  it("uses unique voters as the multi-choice percentage denominator", () => {
    const value = message(null, true);
    if (!value.poll) throw new Error("poll fixture missing");
    value.poll.options[1] = {
      optionId: "tomorrow",
      text: "明日",
      voters: [SELF, OTHER],
    };
    render(<MessagePoll message={value} />);

    expect(screen.getByText("1票 · 50%")).toBeInTheDocument();
    expect(screen.getByText("2票 · 100%")).toBeInTheDocument();
    expect(screen.getByText("2人が投票")).toBeInTheDocument();
  });

  it("closes at exact equality and flips through its deadline timer", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-30T00:00:00Z"));
    render(<MessagePoll message={message(Date.now() + 1_000)} />);
    const poll = screen.getByRole("region", { name: /投票:/ });
    expect(poll).toHaveAttribute("data-poll-closed", "false");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1_000);
    });

    expect(poll).toHaveAttribute("data-poll-closed", "true");
    expect(screen.getByText("締め切りました")).toBeInTheDocument();
    expect(optionButton("今日")).toBeDisabled();
    fireEvent.click(optionButton("今日"));
    expect(votePoll).not.toHaveBeenCalled();
  });

  it("discloses voter names with an operable control instead of title-only text", () => {
    render(<MessagePoll message={message()} />);
    const disclosure = screen.getByRole("button", {
      name: "今日の投票者を表示",
    });
    disclosure.focus();
    expect(disclosure).toHaveFocus();
    expect(disclosure).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(disclosure);

    expect(
      screen.getByRole("button", { name: "今日の投票者を閉じる" }),
    ).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("Yohaku")).toBeVisible();
  });

  it("shows only the latest failed intent as a retryable alert", () => {
    const value = message();
    useMessaging.setState({
      pollVoteByMessage: {
        [value.messageId]: {
          optionIds: ["tomorrow"],
          intent: 8,
          pending: false,
          failed: true,
        },
      },
    });
    render(<MessagePoll message={value} />);

    expect(screen.getByRole("alert")).toHaveTextContent(
      "投票を反映できませんでした",
    );
    fireEvent.click(screen.getByRole("button", { name: "もう一度" }));
    expect(votePoll).toHaveBeenCalledWith(value, ["tomorrow"]);
  });

  it("renders results but disables voting when the capability is absent", () => {
    useMessaging.setState((state) => ({
      capabilities: { ...state.capabilities, polls: false },
    }));
    render(<MessagePoll message={message()} />);

    expect(screen.getByText("この接続では回答できません")).toBeVisible();
    expect(optionButton("今日")).toHaveAttribute("aria-disabled", "true");
    expect(optionButton("今日")).toBeDisabled();
  });
});
