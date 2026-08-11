import { describe, expect, it } from "vitest";
import { placePath } from "./place-route";

describe("placePath", () => {
  it.each([
    ["channel:channel-1", "/w/workspace-1/messaging/c/channel-1"],
    ["dm:dm-1", "/w/workspace-1/messaging/dm/dm-1"],
    ["group_dm:group-1", "/w/workspace-1/messaging/group/group-1"],
  ] as const)("keeps %s inside its exact Workspace", (place, expected) => {
    expect(placePath("workspace-1", place)).toBe(expected);
  });
});
