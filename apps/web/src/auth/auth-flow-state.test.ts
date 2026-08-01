// @vitest-environment jsdom

import { beforeEach, describe, expect, it } from "vitest";
import {
  clearPendingEmailFlow,
  loadPendingEmailFlow,
  type PendingEmailAuthFlow,
  savePendingEmailFlow,
} from "./auth-flow-state";

const state = "A".repeat(24);

beforeEach(() => {
  localStorage.clear();
});

describe("pending credential recovery state", () => {
  it("loads a bounded provider credential only in its random email-flow scope", () => {
    const flow = pendingFlow();
    savePendingEmailFlow(state, flow);

    expect(loadPendingEmailFlow(state)).toEqual(flow);
    expect(loadPendingEmailFlow("B".repeat(24))).toBeNull();
  });

  it("is single-use after the email callback consumes its scoped flow", () => {
    savePendingEmailFlow(state, pendingFlow());
    clearPendingEmailFlow(state);
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
