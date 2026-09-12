// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import {
  confirmAuthFlow,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";
import {
  captureEnrollmentInvitation,
  clearEnrollmentInvitation,
  readEnrollmentInvitation,
} from "./enrollment-invitation-state";

const token = "i".repeat(43),
  nonce = "n".repeat(43);
const proof = {
  flow_id: "flow1",
  outcome: "proof_required",
  expires_at: "2026-09-19T00:00:00Z",
};
const confirmation = {
  ...proof,
  outcome: "confirmation_required",
  next_action: "create_account",
  continuation: "/",
};
const terminal = { ...proof, outcome: "account_created", continuation: "/" };
function capture() {
  history.replaceState(null, "", `/#invite=${token}`);
  captureEnrollmentInvitation();
}
function responses(...values: unknown[]) {
  const mock = vi.fn();
  for (const value of values) {
    mock
      .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
      .mockResolvedValueOnce(Response.json(value));
  }
  vi.stubGlobal("fetch", mock);
  return mock;
}
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  clearEnrollmentInvitation();
  history.replaceState(null, "", "/");
});
it.each([
  "google.com",
  "github.com",
  "email_link",
] as const)("binds invitation at %s startup and keeps it through confirmation", async (provider) => {
  capture();
  const mock = responses(proof, confirmation, terminal);
  await startAuthFlow({
    intent: "sign_up",
    provider,
    email: "tester@example.com",
    continuation: "/",
    nonce,
  });
  expect(JSON.parse(mock.mock.calls[1][1].body).invite_token).toBe(token);
  history.replaceState(null, "", "/?auth_callback=1");
  expect(captureEnrollmentInvitation()).toBe(token);
  await resolveAuthFlow({ flowId: "flow1", nonce, idToken: "id-token" });
  expect(readEnrollmentInvitation()).toBe(token);
  await confirmAuthFlow({ flowId: "flow1", nonce, action: "create_account" });
  expect(readEnrollmentInvitation()).toBeNull();
});
it("does not clear invitation when an unexpected terminal startup response is rejected", async () => {
  capture();
  responses(terminal);
  await expect(
    startAuthFlow({
      intent: "sign_up",
      provider: "google.com",
      continuation: "/",
      nonce,
    }),
  ).rejects.toThrow();
  expect(readEnrollmentInvitation()).toBe(token);
});
it("includes the invitation when storage denied but its current-page capture succeeded", async () => {
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new Error("denied");
  });
  vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
    throw new Error("denied");
  });
  capture();
  const mock = responses(proof);
  await startAuthFlow({
    intent: "sign_up",
    provider: "google.com",
    continuation: "/",
    nonce,
  });
  expect(JSON.parse(mock.mock.calls[1][1].body).invite_token).toBe(token);
});

it("retains invite on rejected resolution and clears only a recovered terminal result", async () => {
  capture();
  let mock = vi
    .fn()
    .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
    .mockResolvedValueOnce(
      Response.json({ error: "invitation_required" }, { status: 403 }),
    );
  vi.stubGlobal("fetch", mock);
  await expect(
    resolveAuthFlow({ flowId: "flow1", nonce, idToken: "id-token" }),
  ).rejects.toMatchObject({ status: 403 });
  expect(readEnrollmentInvitation()).toBe(token);
  mock = vi
    .fn()
    .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
    .mockRejectedValueOnce(new TypeError("connection lost"))
    .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
    .mockResolvedValueOnce(Response.json(terminal));
  vi.stubGlobal("fetch", mock);
  await expect(
    resolveAuthFlow({ flowId: "flow1", nonce, idToken: "id-token" }),
  ).resolves.toMatchObject({ outcome: "account_created" });
  expect(readEnrollmentInvitation()).toBeNull();
  expect(mock.mock.calls[3][0]).toBe("/auth/flows/status");
});
