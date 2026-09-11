// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { useConversation } from "../agent/store";
import {
  captureFeedbackDiagnostics,
  readServedRelease,
  recordFeedbackOrigin,
  resetFeedbackOrigin,
  startFeedbackEvidence,
} from "./diagnostics";

let stopEvidence: (() => void) | undefined;
afterEach(() => {
  stopEvidence?.();
  stopEvidence = undefined;
  resetFeedbackOrigin();
  document.body.replaceChildren();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
it("captures the current source state on demand and deduplicates route observations", () => {
  stopEvidence = startFeedbackEvidence();
  useConversation.setState({ connection: "connecting", lastError: null });
  recordFeedbackOrigin("/direct");
  useConversation.setState({ connection: "closed", lastError: "offline" });
  recordFeedbackOrigin("/direct?token=hidden");
  const snapshot = captureFeedbackDiagnostics();
  expect(snapshot.source?.states.agent_connection).toBe("closed");
  expect(snapshot.client_events).toMatchObject([
    { kind: "navigation", path: "/direct" },
  ]);
  recordFeedbackOrigin("/");
  expect(snapshot.source?.path).toBe("/direct");
  expect(snapshot.client_events).toHaveLength(1);
});

it("bounds recent evidence, sanitizes errors, and clones each captured ring", () => {
  stopEvidence = startFeedbackEvidence();
  for (let i = 0; i < 30; i++) {
    window.dispatchEvent(
      new ErrorEvent("error", {
        message: `Failure ${i}: https://user:password@example.com/api?token=url-secret#fragment token=token-secret Authorization: Bearer bearer-secret password="password secret"`,
        filename: "https://example.com/assets/app.js?token=file-secret#hash",
        lineno: 12,
        colno: 4,
      }),
    );
  }
  const snapshot = captureFeedbackDiagnostics();
  expect(snapshot.client_events).toHaveLength(24);
  expect(snapshot.client_events?.[0].summary).toContain("Failure 6");
  expect(snapshot.client_events?.[0].summary).toContain(
    "https://example.com/api",
  );
  expect(snapshot.client_events?.[0].summary).toContain("app.js:12:4");
  expect(JSON.stringify(snapshot.client_events)).not.toMatch(
    /url-secret|token-secret|bearer-secret|password secret|file-secret|fragment|user:password/,
  );
  window.dispatchEvent(new ErrorEvent("error", { message: "x".repeat(400) }));
  expect(
    captureFeedbackDiagnostics().client_events?.at(-1)?.summary,
  ).toHaveLength(240);
  expect(snapshot.client_events?.at(-1)?.summary).toContain("Failure 29");
});

it("records element types and safe destinations without typed values, labels, or message text", () => {
  stopEvidence = startFeedbackEvidence();
  document.body.innerHTML = `<input value="typed-secret" aria-label="private-label"><textarea>draft-secret</textarea><button aria-label="private-label">message-secret</button><a href="/direct?token=link-secret#fragment">private conversation</a><div contenteditable="true">editor-secret</div>`;
  for (const element of document.body.children) {
    element.dispatchEvent(new Event("input", { bubbles: true }));
    element.dispatchEvent(
      new KeyboardEvent("keydown", { key: "s", bubbles: true }),
    );
    element.dispatchEvent(new MouseEvent("click", { bubbles: true }));
  }
  const events = captureFeedbackDiagnostics().client_events;
  expect(events?.map((event) => event.summary)).toEqual([
    "input",
    "textarea",
    "button",
    "a → /direct",
  ]);
  expect(JSON.stringify(events)).not.toMatch(/secret|private|fragment/);
});

it("records rejection messages without serializing arbitrary rejection objects", () => {
  stopEvidence = startFeedbackEvidence();
  for (const reason of [
    new Error("Request failed /api?token=hidden#fragment api_key=key-secret"),
    { password: "object-secret" },
  ]) {
    const event = new Event("unhandledrejection");
    Object.defineProperty(event, "reason", { value: reason });
    window.dispatchEvent(event);
  }
  const events = captureFeedbackDiagnostics().client_events;
  expect(events?.[0]).toMatchObject({
    kind: "unhandledrejection",
    summary: "Error: Request failed /api api_key=[redacted]",
  });
  expect(events?.[1].summary).toBe("Unhandled promise rejection");
  expect(JSON.stringify(events)).not.toMatch(
    /hidden|fragment|key-secret|object-secret/,
  );
});

it("clears evidence on identity reset and unmount without retaining duplicate listeners", () => {
  const oldStop = startFeedbackEvidence();
  recordFeedbackOrigin("/direct");
  stopEvidence = startFeedbackEvidence();
  oldStop();
  expect(captureFeedbackDiagnostics().client_events).toEqual([]);
  expect(captureFeedbackDiagnostics().source).toBeUndefined();
  window.dispatchEvent(new ErrorEvent("error", { message: "single listener" }));
  expect(captureFeedbackDiagnostics().client_events).toHaveLength(1);
  resetFeedbackOrigin();
  expect(captureFeedbackDiagnostics().client_events).toEqual([]);
  window.dispatchEvent(new ErrorEvent("error", { message: "new identity" }));
  expect(captureFeedbackDiagnostics().client_events).toHaveLength(1);
  stopEvidence();
  window.dispatchEvent(new ErrorEvent("error", { message: "unmounted" }));
  expect(captureFeedbackDiagnostics().client_events).toEqual([]);
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
