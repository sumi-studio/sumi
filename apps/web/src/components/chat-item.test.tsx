// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatItemView } from "./chat-item";

afterEach(cleanup);

describe("ChatItemView", () => {
  it("uses a dedicated semantic control to reveal message actions", () => {
    render(
      <ChatItemView
        item={{
          kind: "user",
          id: "user-1",
          text: "こんにちは",
          attachments: [],
          timestamp: null,
          delivery: "durable",
        }}
      />,
    );

    const showActions = screen.getByRole("button", {
      name: "メッセージの操作を表示",
    });
    expect(showActions).toHaveAttribute("aria-expanded", "false");
    const controls = showActions.getAttribute("aria-controls");
    expect(controls).not.toBeNull();
    expect(document.getElementById(controls ?? "")).toHaveAttribute("hidden");
    expect(showActions.querySelector("button")).toBeNull();

    fireEvent.click(showActions);
    expect(showActions).toHaveAttribute("aria-expanded", "true");
    expect(document.getElementById(controls ?? "")).not.toHaveAttribute(
      "hidden",
    );
    expect(screen.getByRole("button", { name: "コピー" })).toBeEnabled();
  });

  it("disables both approval choices while the store has a submission latch", () => {
    const onApprovalDecision = vi.fn();
    render(
      <ChatItemView
        item={{
          kind: "approval",
          id: "approval:1",
          runId: null,
          requestId: "approval-1",
          request: {
            id: "approval-1",
            tool_call_id: "tool-1",
            tool_name: "bash",
            action: { reviewable: { command: "git status" } },
            args_summary: { command: "git status" },
          },
          summary: "git status を実行します",
          reason: "確認が必要です",
          status: "pending",
          decision: null,
          timestamp: null,
        }}
        sendingApprovalRequestId="approval-1"
        onApprovalDecision={onApprovalDecision}
      />,
    );

    expect(screen.getByRole("button", { name: "今回のみ許可" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "拒否" })).toBeDisabled();
    expect(screen.getByRole("status")).toHaveTextContent("承認を送信中");
    fireEvent.click(screen.getByRole("button", { name: "拒否" }));
    expect(onApprovalDecision).not.toHaveBeenCalled();
  });
});
