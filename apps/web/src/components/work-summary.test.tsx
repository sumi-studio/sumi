// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { AgentRun } from "../agent/work-summary";
import { TraceRow, WorkSummary } from "./work-summary";

afterEach(cleanup);

function makeRun(status: AgentRun["status"]): AgentRun {
  return {
    audience: "direct_chat",
    kind: "agent-run",
    id: "run-1",
    startedSeq: 1,
    endedSeq: status === "complete" ? 2 : null,
    status,
    trace: [
      {
        type: "tool",
        id: "tool-1",
        name: "read_file",
        route: "normal",
        label: "read_fileを完了",
        args: {},
        result: undefined,
        status: "done",
      },
    ],
  };
}

describe("WorkSummary", () => {
  it("keeps inspected work visible when the run ends", () => {
    const view = render(<WorkSummary run={makeRun("running")} />);
    expect(screen.getByText("作業中")).toBeVisible();
    expect(screen.getByText("read_fileを完了")).toBeVisible();

    view.rerender(<WorkSummary run={makeRun("complete")} />);
    expect(screen.getByText("作業が終了しました")).toBeVisible();
    expect(screen.getByText("read_fileを完了")).toBeVisible();
  });

  it("lets an explicit user toggle win over the automatic state", () => {
    const view = render(<WorkSummary run={makeRun("running")} />);

    // The user closes the section mid-run; it must not spring back open.
    fireEvent.click(screen.getByRole("button", { name: /作業中/ }));
    expect(screen.queryByText("read_fileを完了")).toBeNull();

    view.rerender(<WorkSummary run={makeRun("complete")} />);
    expect(screen.queryByText("read_fileを完了")).toBeNull();

    // Reopening after the run ended also sticks.
    fireEvent.click(screen.getByRole("button", { name: /作業が終了しました/ }));
    expect(screen.getByText("read_fileを完了")).toBeVisible();
  });
});

describe("one tool execution", () => {
  it.each([
    ["done", "完了", "read_fileを完了", "file contents"],
    ["error", "失敗", "read_fileでエラー", "permission denied"],
    ["cancelled", "中止", "read_fileを中止", undefined],
  ] as const)("updates the same expanded operation to %s", (status, label, title, result) => {
    const running = {
      type: "tool" as const,
      id: "exact-call",
      name: "read_file",
      route: "normal" as const,
      label: "read_fileを実行中",
      args: { path: "note.txt" },
      result: undefined,
      progress: "reading bytes",
      status: "running" as const,
    };
    const view = render(<TraceRow event={running} open />);
    const operation = view.container.querySelector("details");
    expect(view.container.querySelectorAll("details")).toHaveLength(1);
    expect(screen.getByText("実行中")).toBeVisible();
    expect(screen.getByText("進行状況")).toBeVisible();
    expect(screen.getByText("入力")).toBeVisible();
    view.rerender(
      <TraceRow event={{ ...running, status, label: title, result }} open />,
    );
    expect(view.container.querySelector("details")).toBe(operation);
    expect(operation).toHaveAttribute("open");
    expect(view.container.querySelectorAll("details")).toHaveLength(1);
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.queryByText("実行中")).toBeNull();
    expect(screen.queryByText("進行状況")).toBeNull();
    expect(screen.getByText("入力")).toBeVisible();
    const resultLabel = status === "done" ? "結果" : "理由";
    expect(screen.getByText(resultLabel)).toBeVisible();
    expect(screen.getByRole("region", { name: resultLabel })).toHaveTextContent(
      result ?? "操作は中止されました",
    );
  });
});

it.each([
  ["judged block", "execution_review_blocked", true, "block", true],
  ["timeout", "execution_review_blocked", false, "block", false],
  ["escalation", "escalation_review_blocked", true, "block", false],
  ["policy", "policy_denied", true, "block", false],
] as const)("labels only proven automatic review denial: %s", (_, error, judged, outcome, automatic) => {
  const event = {
    type: "tool" as const,
    id: "denied-call",
    name: "bash",
    route: "normal" as const,
    label: "bashでエラー",
    args: {},
    status: "error" as const,
    result: {
      content: [{ type: "text", text: "actual public failure" }],
      details: {
        error,
        reason: "actual public failure",
        review: { judged, outcome, rationale: "missing authority" },
      },
    },
  };
  render(<TraceRow event={event} open />);
  const reason = screen.getByRole("region", { name: "理由" });
  if (automatic) {
    expect(reason).toHaveTextContent("自動レビューにより拒否されました");
    expect(reason).toHaveTextContent("missing authority");
  } else {
    expect(reason).not.toHaveTextContent("自動レビューにより拒否されました");
    expect(reason).toHaveTextContent("actual public failure");
  }
});

it("keeps manual denial and cancellation separate from automatic review", () => {
  const event = {
    type: "tool" as const,
    id: "manual",
    name: "bash",
    route: "normal" as const,
    label: "bashを中止",
    args: {},
    status: "cancelled" as const,
    approvalResolution: "denied" as const,
    result: {
      content: [{ type: "text", text: "Approval denied" }],
      details: { error: "Approval denied" },
    },
  };
  const view = render(<TraceRow event={event} open />);
  expect(screen.getByRole("region", { name: "理由" })).toHaveTextContent(
    "承認が拒否されました",
  );
  expect(screen.getByRole("region", { name: "理由" })).not.toHaveTextContent(
    "自動レビュー",
  );
  view.rerender(
    <TraceRow
      event={{
        ...event,
        approvalResolution: "cancelled",
        result: {
          content: [
            {
              type: "text",
              text: "approval was cancelled after process restart before tool execution",
            },
          ],
          details: { error: "approval_cancelled" },
        },
      }}
      open
    />,
  );
  expect(screen.getByRole("region", { name: "理由" })).toHaveTextContent(
    "process restart",
  );
  expect(screen.getByRole("region", { name: "理由" })).not.toHaveTextContent(
    "自動レビュー",
  );
});

it("retains an ordinary top-level tool error when details contain only metadata", () => {
  render(
    <TraceRow
      open
      event={{
        type: "tool",
        id: "ordinary-failure",
        name: "edit_file",
        route: "normal",
        label: "edit_fileでエラー",
        args: {},
        status: "error",
        result: {
          error: "ファイルを更新できませんでした。接続が切れています。",
          details: { code: "unavailable", path: "note" },
        },
      }}
    />,
  );
  expect(screen.getByRole("region", { name: "理由" })).toHaveTextContent(
    "ファイルを更新できませんでした。接続が切れています。",
  );
});
