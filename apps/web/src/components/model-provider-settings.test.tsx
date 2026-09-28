// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ModelProviderSettings } from "./model-provider-settings";

afterEach(cleanup);
it("offers API connections and explains that subscription sign-in is unavailable", async () => {
  const connectionsAPI = {
    list: vi
      .fn()
      .mockResolvedValue({
        available: true,
        connections: [],
        selection: null,
        activation: "next_start",
      }),
    save: vi.fn(),
    remove: vi.fn(),
    select: vi.fn(),
    beginChatGPTLogin: vi.fn(),
    chatGPTLogin: vi.fn(),
    cancelChatGPTLogin: vi.fn(),
    saveChatGPTSettings: vi.fn(),
  };
  const usageAPI = {
    overview: async () => ({ sources: [], waits: [] }),
    setBudget: vi.fn(),
    clearBudget: vi.fn(),
  };
  render(
    <ModelProviderSettings
      open
      onOpenChange={() => {}}
      connectionsAPI={connectionsAPI}
      usageAPI={usageAPI}
    />,
  );
  expect(
    await screen.findByText("現在はサーバーの既定設定を使っています。"),
  ).toBeInTheDocument();
  expect(
    screen.getByText(/サーバーが対応していればChatGPTのサブスクリプション/),
  ).toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: /ChatGPT/ }),
  ).not.toBeInTheDocument();
});
