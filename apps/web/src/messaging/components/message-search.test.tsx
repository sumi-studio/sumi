// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
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
  vi.clearAllMocks();
});

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
});
