// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { useConversation } from "../agent/store";
import {
  captureFeedbackDiagnostics,
  readServedRelease,
  recordFeedbackOrigin,
  resetFeedbackOrigin,
} from "./diagnostics";

afterEach(() => {
  resetFeedbackOrigin();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it("retains the failing source state before navigation without copying message data or URL secrets", () => {
  useConversation.setState({
    connection: "closed",
    lastError: "contains a private message",
    recoverableDrafts: [],
  });
  recordFeedbackOrigin("/direct?token=secret#private", "workspace-test");
  useConversation.setState({ connection: "connecting", lastError: null });
  recordFeedbackOrigin("/feedback");
  const diagnostic = captureFeedbackDiagnostics();
  expect(diagnostic.source).toMatchObject({
    path: "/direct",
    workspace_id: "workspace-test",
    states: { agent_connection: "closed", agent_error: "present" },
  });
  expect(JSON.stringify(diagnostic)).not.toMatch(/secret|private message/);
  resetFeedbackOrigin();
  expect(captureFeedbackDiagnostics().source).toBeUndefined();
  recordFeedbackOrigin("/join/invite-secret");
  expect(captureFeedbackDiagnostics().source?.path).toBe("/other");
});
it("does not treat HTML from the dev server or a failed manifest fetch as a release", async () => {
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => {
          throw new SyntaxError("html");
        },
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ release_sha: "a".repeat(40) }),
      })
      .mockRejectedValueOnce(new TypeError("offline")),
  );
  expect(await readServedRelease()).toBeUndefined();
  expect(await readServedRelease()).toBe("a".repeat(40));
  expect(await readServedRelease()).toBeUndefined();
});
