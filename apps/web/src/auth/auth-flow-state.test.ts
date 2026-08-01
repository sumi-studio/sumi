// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  cleanupPendingEmailFlowStorage,
  consumePendingCredentialRecovery,
  loadPendingEmailFlow,
  type PendingEmailAuthFlow,
  savePendingEmailFlow,
} from "./auth-flow-state";

const state = "A".repeat(24);

beforeEach(() => {
  localStorage.clear();
  cleanupPendingEmailFlowStorage();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("pending credential recovery state", () => {
  it("loads a bounded provider credential only in its random email-flow scope", () => {
    const flow = pendingFlow();
    savePendingEmailFlow(state, flow);

    expect(loadPendingEmailFlow(state)).toEqual(flow);
    expect(loadPendingEmailFlow("B".repeat(24))).toBeNull();
  });

  it("is single-use after the email callback consumes its scoped flow", () => {
    const flow = pendingFlow();
    savePendingEmailFlow(state, flow);
    consumePendingCredentialRecovery(state, flow);
    expect(loadPendingEmailFlow(state)).toBeNull();
    expect(() => consumePendingCredentialRecovery(state, flow)).toThrow(
      "already consumed",
    );
  });

  it("fails closed when the credential-bearing record cannot be removed", () => {
    const flow = pendingFlow();
    savePendingEmailFlow(state, flow);
    vi.spyOn(Storage.prototype, "removeItem").mockImplementationOnce(() => {
      throw new DOMException("storage unavailable", "SecurityError");
    });

    expect(() => consumePendingCredentialRecovery(state, flow)).toThrow(
      "storage is unavailable",
    );
    expect(loadPendingEmailFlow(state)).toBeNull();
  });

  it("deletes an expired recovery instead of returning its OAuth credential", () => {
    const flow = pendingFlow();
    if (!flow.credentialRecovery) throw new Error("missing recovery fixture");
    flow.credentialRecovery.expiresAt = new Date(Date.now() - 1).toISOString();
    savePendingEmailFlow(state, flow);

    expect(loadPendingEmailFlow(state)).toBeNull();
    expect(localStorage.length).toBe(0);
  });

  it("rejects credential objects containing profile or email fields", () => {
    const flow = pendingFlow();
    const unsafe = flow as PendingEmailAuthFlow & {
      credentialRecovery: {
        credential: Record<string, unknown>;
      };
    };
    unsafe.credentialRecovery.credential.email = "leak@example.com";
    savePendingEmailFlow(state, unsafe);

    expect(loadPendingEmailFlow(state)).toBeNull();
    expect(localStorage.length).toBe(0);
  });

  it("deletes malformed credential-bearing JSON on access", () => {
    localStorage.setItem(
      `sumi.auth.email-flow.v1.${state}`,
      JSON.stringify({
        ...pendingFlow(),
        credentialRecovery: {
          version: 1,
          provider: "github.com",
          credential: { pendingToken: "credential-must-be-removed" },
        },
      }),
    );

    expect(loadPendingEmailFlow(state)).toBeNull();
    expect(localStorage.length).toBe(0);
  });

  it("finds an abandoned credential behind more than 32 unrelated keys", () => {
    for (let index = 0; index < 64; index += 1) {
      localStorage.setItem(`unrelated.${index}`, "unrelated");
    }
    const flow = pendingFlow();
    if (!flow.credentialRecovery) throw new Error("missing recovery fixture");
    flow.credentialRecovery.expiresAt = new Date(Date.now() - 1).toISOString();
    localStorage.setItem(
      `sumi.auth.email-flow.v1.${state}`,
      JSON.stringify(flow),
    );

    cleanupPendingEmailFlowStorage();

    expect(localStorage.getItem(`sumi.auth.email-flow.v1.${state}`)).toBeNull();
    expect(localStorage.getItem("unrelated.63")).toBe("unrelated");
  });

  it("physically expires the exact credential record while the tab remains open", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-01T10:00:00Z"));
    const flow = pendingFlow();
    const expiresAt = new Date(Date.now() + 1_000).toISOString();
    flow.expiresAt = expiresAt;
    if (!flow.credentialRecovery) throw new Error("missing recovery fixture");
    flow.credentialRecovery.expiresAt = expiresAt;
    savePendingEmailFlow(state, flow);

    vi.advanceTimersByTime(1_001);

    expect(localStorage.getItem(`sumi.auth.email-flow.v1.${state}`)).toBeNull();
  });
});

function pendingFlow(): PendingEmailAuthFlow {
  const expiresAt = new Date(Date.now() + 10 * 60_000).toISOString();
  return {
    flowId: "email-flow",
    nonce: "n".repeat(43),
    intent: "sign_in",
    provider: "email_link",
    email: "existing@example.com",
    expiresAt,
    stage: "link_sent",
    credentialRecovery: {
      version: 1,
      provider: "github.com",
      requestedIntent: "sign_in",
      expiresAt,
      credential: {
        providerId: "github.com",
        signInMethod: "github.com",
        pendingToken: "pending-oauth-token",
      },
    },
  };
}
