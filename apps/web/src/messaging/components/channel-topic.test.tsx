// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useMessaging } from "../store";
import { ChannelTopic } from "./messaging-screen";

const updateChannelTopic = vi.fn();

beforeEach(() => {
  updateChannelTopic.mockResolvedValue(undefined);
  useMessaging.setState({ updateChannelTopic });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("ChannelTopic", () => {
  it("IME変換確定のEnterでは保存しない", () => {
    render(<ChannelTopic channelId="c1" topic="設計の話" />);
    fireEvent.click(screen.getByRole("button", { name: /設計の話/ }));
    const input = screen.getByPlaceholderText("トピックを設定");

    fireEvent.change(input, { target: { value: "設計と実装の話" } });
    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229 });

    expect(updateChannelTopic).not.toHaveBeenCalled();
    expect(input).toBeInTheDocument();
  });

  it("変換後のEnterで保存する", () => {
    render(<ChannelTopic channelId="c1" topic="設計の話" />);
    fireEvent.click(screen.getByRole("button", { name: /設計の話/ }));
    const input = screen.getByPlaceholderText("トピックを設定");
    fireEvent.change(input, { target: { value: "設計と実装の話" } });

    fireEvent.keyDown(input, { key: "Enter" });

    expect(updateChannelTopic).toHaveBeenCalledWith("c1", "設計と実装の話");
  });
});
