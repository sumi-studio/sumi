// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SduiView } from "@sumi/sdui";
import { ChatScreen } from "./chat-screen";

const state = vi.hoisted(() => ({
  sendMessage: vi.fn(() => true),
}));

vi.mock("../agent/store", () => ({
  useConversation: () => ({
    conversation: {
      entryOrder: ["card:reminder"],
      entries: {
        "card:reminder": {
          kind: "card",
          id: "card:reminder",
          runId: null,
          toolCallId: "tool:reminder",
          node: {
            type: "reminder",
            props: {
              title: "薬を飲む",
              at: "2026-08-01T09:00:00+09:00",
              actions: [
                { label: "完了にする", action: "reminder.complete:arbitrary" },
              ],
            },
          },
          timestamp: null,
        },
      },
      runOrder: [],
      runs: {},
    },
    running: false,
    connection: "connected",
    ready: "ready",
    lastError: null,
    connect: vi.fn(),
    disconnect: vi.fn(),
    sendMessage: state.sendMessage,
    abort: vi.fn(),
    decideApproval: vi.fn(),
  }),
}));

vi.mock("@sumi/ui/ai-elements/conversation", () => {
  const Container = ({ children }: { children: ReactNode }) => <>{children}</>;
  return {
    Conversation: Container,
    ConversationContent: Container,
    ConversationItem: Container,
    ConversationProvider: Container,
    ConversationViewport: Container,
    ConversationScrollButton: () => null,
    useConversationScroll: () => ({
      scrollToEnd: vi.fn(),
      scrollToMessage: vi.fn(),
    }),
    useConversationVisibility: () => ({ visibleMessageIds: [] }),
  };
});

vi.mock("./app-navigation", () => ({
  AppNavigation: () => null,
}));

vi.mock("./chat-prompt-input", () => ({
  ChatPromptInput: () => null,
}));

vi.mock("./timeline-scrubber", () => ({
  createConversationTimeline: () => ({
    ticks: [],
    messageIds: [],
    visibleRange: null,
  }),
  MobileTimelineSheet: () => null,
  TimelineScrubber: () => null,
}));

afterEach(() => {
  state.sendMessage.mockClear();
});

describe("SDUI action boundary", () => {
  it("keeps an unwired card action disabled and cannot send a user message", async () => {
    render(<ChatScreen />);

    const action = await screen.findByRole("button", { name: "完了にする" });
    expect(action).toBeDisabled();
    fireEvent.click(action);

    expect(state.sendMessage).not.toHaveBeenCalled();
  });

  it("preserves authored action strings when an explicit action handler exists", () => {
    const onAction = vi.fn();
    render(
      <SduiView
        node={{
          type: "confirm",
          props: {
            title: "予定を確定しますか？",
            confirm: {
              label: "確定",
              action: "calendar.commit:v2/with-arbitrary-value",
            },
            cancel: { label: "キャンセル", action: "calendar.cancel" },
          },
        }}
        onAction={onAction}
      />,
    );

    const confirm = screen.getByRole("button", { name: "確定" });
    expect(confirm).toBeEnabled();
    fireEvent.click(confirm);

    expect(onAction).toHaveBeenCalledWith(
      "calendar.commit:v2/with-arbitrary-value",
      "確定",
    );
  });
});
