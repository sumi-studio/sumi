import { describe, expect, it } from "vitest";
import { WorkspaceAPIError } from "../api-client";
import { workspaceMutationErrorMessage } from "./workspace-home";

describe("workspaceMutationErrorMessage", () => {
  it.each([
    ["last_administrator", "最後の管理者"],
    ["forbidden", "管理範囲"],
    ["owner_protected", "Workspace Owner"],
    ["membership_not_active", "参加状態はすでに終了"],
    ["conflict", "競合"],
  ])("maps canonical %s failures without flattening policy", (code, message) => {
    expect(
      workspaceMutationErrorMessage(new WorkspaceAPIError(code, 409)),
    ).toContain(message);
  });

  it("keeps unknown failures generic", () => {
    expect(
      workspaceMutationErrorMessage(new Error("database detail")),
    ).not.toContain("database detail");
  });
});
