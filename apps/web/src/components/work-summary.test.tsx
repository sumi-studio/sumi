// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { AgentRun } from "../agent/work-summary";
import { WorkSummary } from "./work-summary";

afterEach(cleanup);

function makeRun(status: AgentRun["status"]): AgentRun {
  return {
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
  it("is expanded by default while the run is active and collapses when it ends", () => {
    const view = render(<WorkSummary run={makeRun("running")} />);
    expect(screen.getByText("作業中")).toBeVisible();
    expect(screen.getByText("read_fileを完了")).toBeVisible();

    view.rerender(<WorkSummary run={makeRun("complete")} />);
    expect(screen.getByText("作業が終了しました")).toBeVisible();
    expect(screen.queryByText("read_fileを完了")).toBeNull();
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
