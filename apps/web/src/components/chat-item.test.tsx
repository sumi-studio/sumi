// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ChatItem } from "../agent/model";
import { userItemText } from "../lib/user-item-text";
import { ChatItemView } from "./chat-item";

afterEach(cleanup);

describe("ChatItemView", () => {
  it("does not add activity lifecycle banners to the conversation", () => {
    const item = {
      kind: "agent-run" as const,
      audience: "direct_chat" as const,
      id: "run-1",
      startedSeq: 1,
      endedSeq: null,
      status: "running" as const,
      trace: [],
    };
    const view = render(<ChatItemView item={item} />);
    expect(view.container).toBeEmptyDOMElement();
    view.rerender(
      <ChatItemView item={{ ...item, status: "complete", endedSeq: 2 }} />,
    );
    expect(view.container).toBeEmptyDOMElement();
  });

  it("keeps message actions in the tree without a reveal toggle", () => {
    render(
      <ChatItemView
        item={{
          kind: "user",
          id: "user-1",
          text: "こんにちは",
          attachments: [],
          timestamp: "2026-08-01T09:00:00+09:00",
          delivery: "durable",
        }}
      />,
    );

    // Hover/focus reveal replaces the old toggle: the actions are always in
    // the accessibility tree and reachable by keyboard.
    expect(
      screen.queryByRole("button", { name: "メッセージの操作を表示" }),
    ).toBeNull();
    expect(screen.getByRole("button", { name: "コピー" })).toBeEnabled();
    expect(screen.getByText("09:00")).toBeInTheDocument();
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

  it("distinguishes foundation rejection after approval from Human denial", () => {
    render(
      <ChatItemView
        item={{
          kind: "approval",
          id: "approval:rejected",
          runId: null,
          requestId: "approval-rejected",
          request: {
            id: "approval-rejected",
            tool_call_id: "tool-rejected",
            tool_name: "bash",
            action: { reviewable: { command: "git status" } },
            args_summary: { command: "git status" },
          },
          summary: "git status を実行します",
          reason: "確認が必要です",
          status: "rejected",
          decision: { type: "approve_once" },
          timestamp: null,
        }}
      />,
    );

    expect(
      screen.getByText("承認内容を実行できませんでした"),
    ).toBeInTheDocument();
    expect(screen.queryByText("拒否しました")).toBeNull();
    expect(screen.queryByRole("button", { name: "今回のみ許可" })).toBeNull();
  });
});

it("keeps operation input and complete failed output inspectable in place", () => {
  const item = {
    kind: "trace" as const,
    id: "result:read",
    runId: "run:1",
    traceId: "read",
    phase: "result" as const,
    trace: {
      type: "tool" as const,
      id: "read",
      name: "read_file",
      route: "normal" as const,
      label: "read_fileでエラー",
      args: { path: "notes/long.md", range: { from: 2, to: 8 } },
      result: {
        error: "Permission denied",
        details: "The entire error remains available",
      },
      status: "error" as const,
    },
  };
  const view = render(<ChatItemView item={item} />);
  expect(screen.getByText("失敗")).toBeVisible();
  const trigger = screen.getByRole("button");
  fireEvent.click(trigger);
  expect(screen.getByRole("region", { name: "入力" })).toHaveTextContent(
    '"from": 2',
  );
  expect(screen.getByRole("region", { name: "結果" })).toHaveTextContent(
    "The entire error remains available",
  );
  view.rerender(
    <ChatItemView
      item={{ ...item, trace: { ...item.trace, label: "記録済みの失敗" } }}
    />,
  );
  expect(trigger).toHaveAttribute("aria-expanded", "true");
  expect(screen.getByRole("region", { name: "結果" })).toHaveTextContent(
    "Permission denied",
  );
});

it("shows poll answers and withdrawals without inventing authored message text", () => {
  const id = "01992000-0000-7000-8000-000000000008";
  const item: Extract<ChatItem, { kind: "user" }> = {
    kind: "user",
    id: "poll-answer",
    text: "",
    attachments: [],
    timestamp: "2026-09-09T08:00:00Z",
    delivery: "durable",
    source: {
      version: 2,
      tenant_id: "tenant",
      personality_agent_id: id,
      actor: { kind: "human", principal_id: id, display_name: "Yohaku" },
      source: {
        surface: "messaging",
        kind: "messaging_poll_vote",
        event_id: id,
        workspace_id: id,
        installation_id: id,
        authority_epoch: 1,
        place: { id, kind: "channel", name: "Planning" },
        message_id: id,
        message_revision: 1,
        message_seq: 1,
        occurred_at: "2026-09-09T08:00:00Z",
        poll_vote: {
          poll_revision: 2,
          question: "いつにしますか？",
          selected_options: [{ option_id: id, text: "午後" }],
        },
      },
    },
  };
  const view = render(<ChatItemView item={item} />);
  expect(screen.getByText(/Yohaku · 投票/)).toBeVisible();
  expect(screen.getByText(/選択：午後/)).toBeVisible();
  expect(userItemText(item)).toBe("いつにしますか？\n選択：午後");
  if (item.source?.source.kind !== "messaging_poll_vote")
    throw Error("Missing vote");
  item.source.source.poll_vote = {
    ...item.source.source.poll_vote,
    selected_options: [],
  };
  view.rerender(<ChatItemView item={item} />);
  expect(screen.getByText(/回答を撤回しました/)).toBeVisible();
  expect(userItemText(item)).toBe("いつにしますか？\n回答を撤回しました");
  expect(item.text).toBe("");
});

it("shows process completion as operation metadata while keeping its body empty", () => {
  const item: Extract<ChatItem, { kind: "user" }> = {
    kind: "user",
    id: "process",
    text: "",
    attachments: [],
    delivery: "durable",
    timestamp: "2026-09-09T10:00:00Z",
    source: {
      version: 2,
      tenant_id: "tenant",
      personality_agent_id: "01992000-0000-7000-8000-000000000001",
      actor: {
        kind: "personality_agent",
        principal_id: "01992000-0000-7000-8000-000000000001",
      },
      source: {
        surface: "workspace_operation",
        kind: "process_completed",
        event_id: "01992000-0000-7000-8000-000000000021",
        operation_id: "a".repeat(64),
        originating_tool_call_id: "start-call",
        occurred_at: "2026-09-09T10:00:00Z",
        result: {
          state: "succeeded",
          exit_code: 0,
          stdout_bytes: 512,
          stderr_bytes: 0,
          output_truncated: false,
        },
      },
    },
  };
  render(<ChatItemView item={item} />);
  expect(screen.getByText("ワークスペース処理")).toBeVisible();
  expect(screen.getByText(/終了コード 0/)).toBeVisible();
  expect(screen.queryByText(/Messaging/)).toBeNull();
  expect(item.text).toBe("");
});
