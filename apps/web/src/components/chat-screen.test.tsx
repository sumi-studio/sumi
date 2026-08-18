// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { SduiView } from "@sumi/sdui";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import {
  forwardRef,
  type ReactNode,
  StrictMode,
  useImperativeHandle,
} from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatScreen } from "./chat-screen";

const state = vi.hoisted(() => ({
  sendMessage: vi.fn(() => true),
  scrollToEnd: vi.fn(),
  recoverableDrafts: [] as Array<{
    idempotencyKey: string;
    text: string;
    reason: string;
  }>,
  restoreDraft: vi.fn<(key: string) => string | undefined>(),
  releaseConnection: vi.fn(),
  acquireConnection: vi.fn(),
  disconnect: vi.fn(),
  resumeMountedConnection: vi.fn(),
  ready: "ready" as "ready" | "not_ready",
  connection: "connected" as "connecting" | "connected" | "closed",
}));

state.acquireConnection.mockImplementation(() => state.releaseConnection);

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
    connection: state.connection,
    ready: state.ready,
    lastError: null,
    recoverableDrafts: state.recoverableDrafts,
    acquireConnection: state.acquireConnection,
    disconnect: state.disconnect,
    resumeMountedConnection: state.resumeMountedConnection,
    sendMessage: state.sendMessage,
    restoreDraft: state.restoreDraft,
    discardDraft: vi.fn(),
    abort: vi.fn(),
    decideApproval: vi.fn(),
  }),
}));

vi.mock("./conversation-virtualizer", () => ({
  ConversationVirtualizer: forwardRef(
    (
      {
        items,
        renderItem,
        busy,
        ariaLabel,
      }: {
        items: { id: string }[];
        renderItem: (item: { id: string }) => ReactNode;
        busy: boolean;
        ariaLabel: string;
      },
      ref,
    ) => {
      useImperativeHandle(ref, () => ({
        isAtEnd: () => false,
        scrollToEnd: state.scrollToEnd,
        scrollToMessage: vi.fn(() => true),
      }));
      return (
        <div role="log" aria-label={ariaLabel} aria-busy={busy}>
          {items.map((item) => (
            <div key={item.id}>{renderItem(item)}</div>
          ))}
        </div>
      );
    },
  ),
}));

vi.mock("./chat-prompt-input", () => ({
  ChatPromptInput: ({
    value,
    onValueChange,
  }: {
    value: string;
    onValueChange: (value: string) => void;
  }) => (
    <textarea
      aria-label="テスト入力欄"
      value={value}
      onChange={(event) => onValueChange(event.target.value)}
    />
  ),
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
  cleanup();
  state.sendMessage.mockClear();
  state.scrollToEnd.mockClear();
  state.recoverableDrafts = [];
  state.restoreDraft.mockReset();
  state.acquireConnection.mockClear();
  state.releaseConnection.mockClear();
  state.disconnect.mockClear();
  state.resumeMountedConnection.mockClear();
  state.ready = "ready";
  state.connection = "connected";
});

describe("SDUI action boundary", () => {
  it("releases the exact connection lease from each StrictMode effect", () => {
    const view = render(
      <StrictMode>
        <ChatScreen installationId="installation-1" authorityEpoch="1" />
      </StrictMode>,
    );

    expect(state.acquireConnection).toHaveBeenCalledTimes(2);
    expect(state.releaseConnection).toHaveBeenCalledTimes(1);
    view.unmount();
    expect(state.releaseConnection).toHaveBeenCalledTimes(2);
  });

  it("renders conversation items through the virtualized log", () => {
    render(<ChatScreen installationId="installation-1" authorityEpoch="1" />);

    expect(screen.getByRole("log", { name: "Sumiとの会話" })).toHaveAttribute(
      "aria-busy",
      "false",
    );
  });

  it("keeps an unwired card action disabled and cannot send a user message", async () => {
    render(<ChatScreen installationId="installation-1" authorityEpoch="1" />);

    const action = await screen.findByRole(
      "button",
      { name: "完了にする" },
      { timeout: 3_000 },
    );
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

  it("returns a recoverable message to an empty composer without resending it", () => {
    state.recoverableDrafts = [
      {
        idempotencyKey: "recoverable-1",
        text: "失われてはいけない入力",
        reason: "superseded",
      },
    ];
    state.restoreDraft.mockReturnValue("失われてはいけない入力");

    render(<ChatScreen installationId="installation-1" authorityEpoch="1" />);
    fireEvent.click(
      screen.getByRole("button", {
        name: "入力欄に戻す",
      }),
    );

    expect(state.restoreDraft).toHaveBeenCalledWith("recoverable-1");
    expect(screen.getByRole("textbox", { name: "テスト入力欄" })).toHaveValue(
      "失われてはいけない入力",
    );
    expect(state.sendMessage).not.toHaveBeenCalled();
  });

  it("does not overwrite text already being composed", () => {
    state.recoverableDrafts = [
      {
        idempotencyKey: "recoverable-2",
        text: "あとで戻す入力",
        reason: "unavailable",
      },
    ];

    render(<ChatScreen installationId="installation-1" authorityEpoch="1" />);
    fireEvent.change(screen.getByRole("textbox", { name: "テスト入力欄" }), {
      target: { value: "いま書いている入力" },
    });

    expect(
      screen.getByRole("button", {
        name: "入力欄に戻す",
      }),
    ).toBeDisabled();
    expect(state.restoreDraft).not.toHaveBeenCalled();
    expect(screen.getByRole("textbox", { name: "テスト入力欄" })).toHaveValue(
      "いま書いている入力",
    );
  });

  it("names an unavailable agent and keeps an explicit retry path", () => {
    state.ready = "not_ready";
    render(<ChatScreen installationId="installation-1" authorityEpoch="1" />);

    expect(screen.getAllByRole("status")[0]).toHaveTextContent(
      "エージェント利用不可",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "エージェントを起動できませんでした",
    );
    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    expect(state.disconnect).toHaveBeenCalledTimes(1);
    expect(state.resumeMountedConnection).toHaveBeenCalledTimes(1);
  });

  it("keeps an automatic reconnect visible while the agent stays unavailable", () => {
    // The socket reconnects on its own after the API states a runtime it
    // could not start, so the stated cause must not read as a settled state
    // the Human can only escape by pressing 再試行.
    state.ready = "not_ready";
    state.connection = "closed";
    render(<ChatScreen installationId="installation-1" authorityEpoch="1" />);

    expect(screen.getAllByRole("status")[0]).toHaveTextContent(
      "エージェント利用不可（再接続中）",
    );
  });
});
