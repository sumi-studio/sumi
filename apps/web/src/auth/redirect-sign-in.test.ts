// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";
import { loadPendingRedirectFlow } from "./auth-flow-state";
import {
  beginRedirectSignIn,
  hasPendingRedirectSignIn,
  RedirectSignInAbandonedError,
  resolveRedirectSignInUser,
  takePendingRedirectSignIn,
} from "./redirect-sign-in";
import { AuthAPIError } from "./session-client";

const mocks = vi.hoisted(() => ({
  getFirebaseAuth: vi.fn(() => ({})),
  getRedirectResult: vi.fn(),
  signInWithRedirect: vi.fn(),
  setCustomParameters: vi.fn(),
  startAuthFlow: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: class GithubAuthProvider {
    providerId = "github.com";
  },
  GoogleAuthProvider: class GoogleAuthProvider {
    providerId = "google.com";
    setCustomParameters = mocks.setCustomParameters;
  },
  getRedirectResult: mocks.getRedirectResult,
  signInWithRedirect: mocks.signInWithRedirect,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: mocks.getFirebaseAuth,
}));

vi.mock("./auth-flow-client", () => ({
  createAuthFlowNonce: mocks.createAuthFlowNonce,
  startAuthFlow: mocks.startAuthFlow,
}));

beforeEach(() => {
  vi.resetAllMocks();
  sessionStorage.clear();
  mocks.getFirebaseAuth.mockReturnValue({});
  mocks.createAuthFlowNonce.mockReturnValue("n".repeat(43));
  mocks.startAuthFlow.mockResolvedValue({
    flowId: "flow-id",
    outcome: "proof_required",
    expiresAt: "2099-08-01T01:00:00Z",
  });
  // signInWithRedirect resolves once navigation has been handed to the
  // browser; in tests that is a promise that simply never settles.
  mocks.signInWithRedirect.mockReturnValue(new Promise(() => {}));
});

describe("beginRedirectSignIn", () => {
  it("registers the Sumi flow, stores a per-tab receipt, then leaves for the provider", async () => {
    const leaving = beginRedirectSignIn({
      provider: "google.com",
      intent: "sign_in",
    });

    await vi.waitFor(() => {
      expect(mocks.signInWithRedirect).toHaveBeenCalledTimes(1);
    });
    expect(mocks.startAuthFlow).toHaveBeenCalledWith({
      intent: "sign_in",
      provider: "google.com",
      continuation: "/",
      nonce: "n".repeat(43),
    });
    const provider = mocks.signInWithRedirect.mock.calls[0][1] as {
      providerId: string;
    };
    expect(provider.providerId).toBe("google.com");
    expect(mocks.setCustomParameters).toHaveBeenCalledWith({
      prompt: "select_account",
    });
    expect(loadPendingRedirectFlow()).toEqual({
      flowId: "flow-id",
      nonce: "n".repeat(43),
      intent: "sign_in",
      provider: "google.com",
      expiresAt: "2099-08-01T01:00:00Z",
      stage: "redirect_sent",
    });
    // The navigation promise never resolves inside this tab.
    await expect(
      Promise.race([leaving, Promise.resolve("settled")]),
    ).resolves.toBe("settled");
  });

  it("uses the GitHub provider without the Google account chooser", async () => {
    void beginRedirectSignIn({ provider: "github.com", intent: "sign_up" });

    await vi.waitFor(() => {
      expect(mocks.signInWithRedirect).toHaveBeenCalledTimes(1);
    });
    expect(mocks.startAuthFlow).toHaveBeenCalledWith({
      intent: "sign_up",
      provider: "github.com",
      continuation: "/",
      nonce: "n".repeat(43),
    });
    const provider = mocks.signInWithRedirect.mock.calls[0][1] as {
      providerId: string;
    };
    expect(provider.providerId).toBe("github.com");
    expect(mocks.setCustomParameters).not.toHaveBeenCalled();
  });

  it("replaces a stale receipt before leaving again", async () => {
    sessionStorage.setItem(
      "sumi.auth.redirect-flow.v1",
      JSON.stringify({
        flowId: "stale-flow",
        nonce: "o".repeat(43),
        intent: "sign_in",
        provider: "github.com",
        expiresAt: "2099-08-01T01:00:00Z",
        stage: "redirect_sent",
      }),
    );

    void beginRedirectSignIn({ provider: "google.com", intent: "sign_in" });

    await vi.waitFor(() => {
      expect(mocks.signInWithRedirect).toHaveBeenCalledTimes(1);
    });
    expect(loadPendingRedirectFlow()?.flowId).toBe("flow-id");
    expect(loadPendingRedirectFlow()?.provider).toBe("google.com");
  });

  it("fails closed without navigating when the receipt cannot be stored", async () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("storage denied", "SecurityError");
    });

    await expect(
      beginRedirectSignIn({ provider: "google.com", intent: "sign_in" }),
    ).rejects.toBeInstanceOf(AuthAPIError);
    expect(mocks.signInWithRedirect).not.toHaveBeenCalled();
  });

  it("aborts without navigating when a restored page cancelled the attempt", async () => {
    let aborted = false;
    const begin = beginRedirectSignIn({
      provider: "google.com",
      intent: "sign_in",
      isAborted: () => aborted,
    });
    // The back/forward-cache restore lands while startAuthFlow is in flight.
    aborted = true;

    await expect(begin).rejects.toBeInstanceOf(RedirectSignInAbandonedError);
    expect(mocks.signInWithRedirect).not.toHaveBeenCalled();
    expect(loadPendingRedirectFlow()).toBeNull();
  });

  it("clears the receipt when leaving the tab fails", async () => {
    const failure = new Error("navigation failed");
    mocks.signInWithRedirect.mockRejectedValue(failure);

    await expect(
      beginRedirectSignIn({ provider: "google.com", intent: "sign_in" }),
    ).rejects.toBe(failure);
    expect(loadPendingRedirectFlow()).toBeNull();
  });
});

describe("redirect return receipt", () => {
  it("reports a saved receipt and claims it exactly once", async () => {
    void beginRedirectSignIn({ provider: "google.com", intent: "sign_in" });
    await vi.waitFor(() => {
      expect(mocks.signInWithRedirect).toHaveBeenCalled();
    });

    expect(hasPendingRedirectSignIn()).toBe(true);
    expect(takePendingRedirectSignIn()?.flowId).toBe("flow-id");
    expect(hasPendingRedirectSignIn()).toBe(false);
    expect(takePendingRedirectSignIn()).toBeNull();
  });
});

describe("resolveRedirectSignInUser", () => {
  it("returns the user from the Firebase redirect result", async () => {
    const user = { uid: "firebase-a" };
    mocks.getRedirectResult.mockResolvedValue({ user });

    await expect(resolveRedirectSignInUser()).resolves.toBe(user);
    expect(mocks.getRedirectResult).toHaveBeenCalledWith({});
  });

  it("fails as abandoned when the return carries no credential", async () => {
    mocks.getRedirectResult.mockResolvedValue(null);

    await expect(resolveRedirectSignInUser()).rejects.toBeInstanceOf(
      RedirectSignInAbandonedError,
    );
  });
});
