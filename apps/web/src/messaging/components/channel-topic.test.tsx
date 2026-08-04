// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useMessaging } from "../store";
import { ChannelTopic } from "./messaging-screen";

const mocks = vi.hoisted(() => ({ updateChannelTopic: vi.fn() }));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => vi.fn(),
}));

beforeEach(() => {
  mocks.updateChannelTopic.mockResolvedValue(undefined);
  useMessaging.setState({ updateChannelTopic: mocks.updateChannelTopic });
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

    expect(mocks.updateChannelTopic).not.toHaveBeenCalled();
    expect(input).toBeInTheDocument();
  });

  it("変換が終わったあとのEnterで保存する", () => {
    render(<ChannelTopic channelId="c1" topic="設計の話" />);
    fireEvent.click(screen.getByRole("button", { name: /設計の話/ }));
    const input = screen.getByPlaceholderText("トピックを設定");

    fireEvent.change(input, { target: { value: "設計と実装の話" } });
    fireEvent.keyDown(input, { key: "Enter" });

    expect(mocks.updateChannelTopic).toHaveBeenCalledWith(
      "c1",
      "設計と実装の話",
    );
  });
});
