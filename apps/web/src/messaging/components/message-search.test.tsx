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
import type { MessageSearchResult, ThreadSummary } from "../model";
import { MessageSearch } from "./message-search";

const mocks = vi.hoisted(() => ({ searchMessages: vi.fn() }));
const state = {
  searchMessages: mocks.searchMessages,
  channels: [
    {
      channelId: "channel-1",
      workspaceId: "workspace-1",
      name: "general",
      topic: "",
      visibility: "public",
    },
  ],
  dms: [],
  threadsById: {} as Record<string, ThreadSummary>,
  membersByKey: {
    "human:author-1": {
      participant: { kind: "human" as const, humanId: "author-1" },
      displayName: "ヨハク",
      tagline: "",
    },
  },
  selfKey: "human:self",
};

vi.mock("../store", () => ({
  useMessaging: (selector: (value: typeof state) => unknown) => selector(state),
}));

function result(): MessageSearchResult {
  return {
    messageId: "message-1",
    place: { kind: "channel", channelId: "channel-1" },
    seq: 7,
    author: { kind: "human", humanId: "author-1" },
    snippet: "明日の予定を確認します",
    createdAt: Date.UTC(2026, 0, 1),
  };
}

async function advance(milliseconds: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(milliseconds);
  });
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.resetAllMocks();
  state.threadsById = {};
});

describe("MessageSearch", () => {
  it("debounces Japanese input and jumps through the result identity", async () => {
    mocks.searchMessages.mockResolvedValueOnce([result()]);
    const onJump = vi.fn();
    render(<MessageSearch onJump={onJump} />);
    const input = screen.getByPlaceholderText("検索");

    fireEvent.change(input, { target: { value: "予定" } });
    expect(mocks.searchMessages).not.toHaveBeenCalled();
    await advance(300);

    expect(mocks.searchMessages).toHaveBeenCalledWith("予定");
    const hit = screen.getByRole("button", { name: /# general/ });
    expect(hit).toBeInTheDocument();
    fireEvent.click(hit);
    expect(onJump).toHaveBeenCalledWith({
      placeKey: "channel:channel-1",
      seq: 7,
      messageId: "message-1",
    });
  });

  it("waits for IME composition to finish before scheduling a search", async () => {
    render(<MessageSearch onJump={vi.fn()} />);
    const input = screen.getByPlaceholderText("検索");
    fireEvent.compositionStart(input);
    fireEvent.change(input, { target: { value: "予定" } });
    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    await advance(300);
    expect(mocks.searchMessages).not.toHaveBeenCalled();
    fireEvent.compositionEnd(input, { currentTarget: { value: "予定" } });
    await advance(300);
    expect(mocks.searchMessages).toHaveBeenCalledWith("予定");
  });

  it("labels a thread result with its thread name", async () => {
    mocks.searchMessages.mockResolvedValueOnce([
      {
        ...result(),
        place: { kind: "thread", threadId: "thread-1" },
      },
    ]);
    state.threadsById = {
      "thread-1": {
        threadId: "thread-1",
        revision: 1,
        parentPlace: { kind: "channel", channelId: "channel-1" },
        parentMessageId: null,
        workspaceId: "workspace-1",
        name: "認証リダイレクト",
        messageCount: 1,
        lastMessageAt: null,
        lastMessage: "",
        participants: [],
        latestSeq: 1,
      },
    };
    render(<MessageSearch onJump={vi.fn()} />);

    fireEvent.change(screen.getByPlaceholderText("検索"), {
      target: { value: "予定" },
    });
    await advance(300);

    expect(screen.getByRole("button", { name: /認証リダイレクト/ })).toBeInTheDocument();
    expect(screen.queryByText("DM")).not.toBeInTheDocument();
  });

  it("closes on Escape and an outside pointer", async () => {
    mocks.searchMessages.mockResolvedValue([]);
    render(
      <>
        <MessageSearch onJump={vi.fn()} />
        <button type="button">outside</button>
      </>,
    );
    const input = screen.getByPlaceholderText("検索");
    fireEvent.change(input, { target: { value: "予定" } });
    expect(screen.getByText(/「予定」の検索結果/)).toBeInTheDocument();
    fireEvent.keyDown(input, { key: "Escape" });
    expect(screen.queryByText(/「予定」の検索結果/)).not.toBeInTheDocument();

    fireEvent.focus(input);
    fireEvent.change(input, { target: { value: "予定" } });
    fireEvent.pointerDown(screen.getByRole("button", { name: "outside" }));
    expect(screen.queryByText(/「予定」の検索結果/)).not.toBeInTheDocument();
  });
});
