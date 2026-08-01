// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { beginEmailLinkAuth, completeEmailLinkAuth } from "./email-link-auth";

const emailMocks = vi.hoisted(() => ({
  auth: { currentUser: null as null | { uid: string } },
  getFirebaseAuth: vi.fn(),
  getIdToken: vi.fn(),
  isSignInWithEmailLink: vi.fn(),
  sendSignInLinkToEmail: vi.fn(),
  signInWithEmailLink: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  resolveAuthFlow: vi.fn(),
  startAuthFlow: vi.fn(),
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: emailMocks.getFirebaseAuth,
}));

vi.mock("firebase/auth", () => ({
  getIdToken: emailMocks.getIdToken,
  isSignInWithEmailLink: emailMocks.isSignInWithEmailLink,
  sendSignInLinkToEmail: emailMocks.sendSignInLinkToEmail,
  signInWithEmailLink: emailMocks.signInWithEmailLink,
}));

vi.mock("./auth-flow-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./auth-flow-client")>()),
  createAuthFlowNonce: emailMocks.createAuthFlowNonce,
  resolveAuthFlow: emailMocks.resolveAuthFlow,
  startAuthFlow: emailMocks.startAuthFlow,
}));

beforeEach(() => {
  localStorage.clear();
  history.replaceState(null, "", "/");
  emailMocks.auth.currentUser = null;
  emailMocks.getFirebaseAuth.mockReturnValue(emailMocks.auth);
  emailMocks.startAuthFlow.mockResolvedValue({
    flowId: "flow-email",
    outcome: "proof_required",
    expiresAt: "2026-08-01T01:00:00Z",
  });
  emailMocks.sendSignInLinkToEmail.mockResolvedValue(undefined);
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("Firebase email-link Koseki flow", () => {
  it("preserves sign-up intent and sends a same-origin continuation", async () => {
    await beginEmailLinkAuth(" human@example.com ", "sign_up");

    expect(emailMocks.startAuthFlow).toHaveBeenCalledWith({
      intent: "sign_up",
      provider: "email_link",
      email: "human@example.com",
      continuation: expect.stringMatching(/^\/\?sumi_auth_state=/),
      nonce: "n".repeat(43),
    });
    const settings = emailMocks.sendSignInLinkToEmail.mock.calls[0]?.[2];
    expect(settings).toMatchObject({ handleCodeInApp: true });
    expect(new URL(settings.url).origin).toBe(location.origin);
  });

  it("returns confirmation_required without silently creating an account", async () => {
    await beginEmailLinkAuth("human@example.com", "sign_in");
    const settings = emailMocks.sendSignInLinkToEmail.mock.calls[0]?.[2];
    const state = new URL(settings.url).searchParams.get("sumi_auth_state");
    history.replaceState(
      null,
      "",
      `/?sumi_auth_state=${state}&mode=signIn&oobCode=proof`,
    );
    emailMocks.isSignInWithEmailLink.mockReturnValue(true);
    emailMocks.signInWithEmailLink.mockResolvedValue({
      user: { uid: "firebase-user" },
    });
    emailMocks.getIdToken.mockResolvedValue("id-token");
    emailMocks.resolveAuthFlow.mockResolvedValue({
      flowId: "flow-email",
      outcome: "confirmation_required",
      nextAction: "create_account",
      continuation: `/?sumi_auth_state=${state}`,
      expiresAt: "2026-08-01T01:00:00Z",
    });

    const completion = await completeEmailLinkAuth();

    expect(completion.flow.intent).toBe("sign_in");
    expect(completion.result).toMatchObject({
      outcome: "confirmation_required",
      nextAction: "create_account",
    });
    expect(emailMocks.resolveAuthFlow).toHaveBeenCalledWith({
      flowId: "flow-email",
      nonce: "n".repeat(43),
      idToken: "id-token",
    });
    expect(location.search).toBe("");
  });
});
