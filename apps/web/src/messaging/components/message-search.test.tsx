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
import type { MessageSearchResult } from "../model";
import { useMessaging } from "../store";
import { MessageSearch } from "./message-search";

const mocks = vi.hoisted(() => ({ searchMessages: vi.fn() }));

beforeEach(() => {
  mocks.searchMessages.mockResolvedValue([]);
  useMessaging.setState({
    channels: [],
    dms: [],
    searchMessages: mocks.searchMessages,
  });
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.clearAllMocks();
});

function deferred<T>() {
  let resolve: (value: T) => void = () => {};
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function result(snippet: string, messageId: string): MessageSearchResult {
  return {
    messageId,
    place: { kind: "channel", channelId: "channel-1" },
    seq: 1,
    author: { kind: "human", humanId: "author-1" },
    snippet,
    createdAt: Date.UTC(2026, 0, 1),
  };
}

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

describe("MessageSearch", () => {
  it("フォーカスや入力で幅が変わらない", () => {
    render(<MessageSearch onJump={vi.fn()} />);
    const input = screen.getByPlaceholderText("検索");

    // 幅がフォーカスで伸びると、右隣のアイコンが動いて押し損ねる。
    expect(input.className).not.toMatch(/focus(-visible)?:w-/);
    expect(input.className).not.toMatch(/transition-\[width\]/);
  });

  it("IME変換確定のEnterでは検索を走らせない", () => {
    render(<MessageSearch onJump={vi.fn()} />);
    const input = screen.getByPlaceholderText("検索");

    fireEvent.change(input, { target: { value: "設計" } });
    fireEvent.keyDown(input, { key: "Enter", isComposing: true });

    expect(mocks.searchMessages).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: "Enter" });

    expect(mocks.searchMessages).toHaveBeenCalledWith("設計");
  });

  it("入力変更時点で実行中の検索を無効化する", async () => {
    vi.useFakeTimers();
    const alpha = deferred<MessageSearchResult[]>();
    mocks.searchMessages.mockReturnValueOnce(alpha.promise);

    render(<MessageSearch onJump={vi.fn()} />);
    const input = screen.getByPlaceholderText("検索");

    fireEvent.change(input, { target: { value: "alpha" } });
    await advance(300);
    expect(mocks.searchMessages).toHaveBeenCalledWith("alpha");

    fireEvent.change(input, { target: { value: "bravo" } });
    await act(async () => {
      alpha.resolve([result("古い結果", "message-alpha")]);
    });

    expect(screen.getByText(/「bravo」の検索結果/)).toBeInTheDocument();
    expect(screen.queryByText("古い結果")).not.toBeInTheDocument();
  });

  it("検索失敗を0件と区別する", async () => {
    vi.useFakeTimers();
    mocks.searchMessages.mockRejectedValueOnce(new Error("offline"));

    render(<MessageSearch onJump={vi.fn()} />);
    fireEvent.change(screen.getByPlaceholderText("検索"), {
      target: { value: "alpha" },
    });
    await advance(300);

    expect(screen.getByText("検索に失敗しました")).toBeInTheDocument();
    expect(
      screen.queryByText("一致するメッセージはありません"),
    ).not.toBeInTheDocument();
  });

  it("queryをserverと同じ200 UTF-8 bytes以内に収める", async () => {
    vi.useFakeTimers();
    render(<MessageSearch onJump={vi.fn()} />);
    const input = screen.getByPlaceholderText("検索");

    fireEvent.change(input, { target: { value: "あ".repeat(67) } });
    expect(input).toHaveValue("あ".repeat(66));
    await advance(300);

    expect(mocks.searchMessages).toHaveBeenCalledWith("あ".repeat(66));
  });
});
