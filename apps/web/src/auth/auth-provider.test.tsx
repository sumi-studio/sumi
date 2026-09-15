// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, useAuth } from "./auth-context";
import { getAuthErrorMessage } from "./auth-errors";
import type { PendingRedirectAuthFlow } from "./auth-flow-state";
import {
  loadPendingEmailFlow,
  savePendingEmailFlow,
} from "./auth-flow-state";
import { RedirectSignInAbandonedError } from "./redirect-sign-in";
import {
  AuthAPIError,
  SumiProfileUpdateIndeterminateError,
  SumiSessionCompensatedError,
} from "./session-client";

const authorityBindingA = "A".repeat(43);
const authorityBindingB = `${"B".repeat(42)}E`;

const authMocks = vi.hoisted(() => ({
  getSumiProfile: vi.fn(),
  getSumiSession: vi.fn(),
  logoutSumiSession: vi.fn(),
  updateSumiProfile: vi.fn(),
  verifyCommittedSumiSession: vi.fn(),
  startAuthFlow: vi.fn().mockResolvedValue({
    flowId: "flow-id",
    outcome: "proof_required",
    expiresAt: "2026-08-01T01:00:00Z",
  }),
  resolveAuthFlow: vi.fn().mockResolvedValue({
    flowId: "flow-id",
    outcome: "signed_in",
    continuation: "/",
    expiresAt: "2026-08-01T01:00:00Z",
    humanId: "user-b",
  }),
  confirmAuthFlow: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  discardAuthFlow: vi.fn(),
  abandonEmailCodeFlow: vi.fn(),
  beginEmailCodeAuth: vi.fn(),
  clearPendingEmailLink: vi.fn(),
  completeEmailLink: vi.fn(),
  ensureEmailCompletionSession: vi.fn(),
  finishEmailProof: vi.fn(),
  inspectEmailLink: vi.fn(),
  loadActiveEmailCodeFlow: vi.fn((): unknown => null),
  pendingEmailLink: vi.fn(() => null),
  readEmailCodeStatus: vi.fn(),
  resendEmailCode: vi.fn(),
  verifyEmailCode: vi.fn(),
  beginSameEmailCredentialRecovery: vi.fn(),
  completeSameEmailCredentialRecovery: vi.fn(),
  isSameEmailCredentialCollision: vi.fn<(error: unknown) => boolean>(
    () => false,
  ),
  getFirebaseAuth: vi.fn(),
  onAuthStateChanged: vi.fn(
    (_auth: unknown, _observer: (user: { uid: string } | null) => void) =>
      vi.fn(),
  ),
  signOut: vi.fn(),
  getIdToken: vi.fn(),
  beginRedirectSignIn: vi.fn(),
  hasPendingRedirectSignIn: vi.fn(() => false),
  takePendingRedirectSignIn: vi.fn<() => PendingRedirectAuthFlow | null>(
    () => null,
  ),
  resolveRedirectSignInUser: vi.fn(),
  bindDirectChatAuthority: vi.fn(),
  clearDirectChatAuthority: vi.fn(() => true),
}));

vi.mock("./session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-client")>()),
  getSumiProfile: authMocks.getSumiProfile,
  getSumiSession: authMocks.getSumiSession,
  logoutSumiSession: authMocks.logoutSumiSession,
  updateSumiProfile: authMocks.updateSumiProfile,
  verifyCommittedSumiSession: authMocks.verifyCommittedSumiSession,
}));

vi.mock("./auth-flow-client", () => ({
  confirmAuthFlow: authMocks.confirmAuthFlow,
  createAuthFlowNonce: authMocks.createAuthFlowNonce,
  discardAuthFlow: authMocks.discardAuthFlow,
  resolveAuthFlow: authMocks.resolveAuthFlow,
  startAuthFlow: authMocks.startAuthFlow,
}));

vi.mock("./email-code-auth", () => ({
  abandonEmailCodeFlow: authMocks.abandonEmailCodeFlow,
  beginEmailCodeAuth: authMocks.beginEmailCodeAuth,
  clearPendingEmailLink: authMocks.clearPendingEmailLink,
  completeEmailLink: authMocks.completeEmailLink,
  ensureEmailCompletionSession: authMocks.ensureEmailCompletionSession,
  finishEmailProof: authMocks.finishEmailProof,
  inspectEmailLink: authMocks.inspectEmailLink,
  loadActiveEmailCodeFlow: authMocks.loadActiveEmailCodeFlow,
  pendingEmailLink: authMocks.pendingEmailLink,
  readEmailCodeStatus: authMocks.readEmailCodeStatus,
  resendEmailCode: authMocks.resendEmailCode,
  verifyEmailCode: authMocks.verifyEmailCode,
}));

vi.mock("./credential-recovery", () => ({
  beginSameEmailCredentialRecovery: authMocks.beginSameEmailCredentialRecovery,
  completeSameEmailCredentialRecovery:
    authMocks.completeSameEmailCredentialRecovery,
  isSameEmailCredentialCollision: authMocks.isSameEmailCredentialCollision,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: authMocks.getFirebaseAuth,
}));

vi.mock("./redirect-sign-in", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./redirect-sign-in")>()),
  beginRedirectSignIn: authMocks.beginRedirectSignIn,
  hasPendingRedirectSignIn: authMocks.hasPendingRedirectSignIn,
  takePendingRedirectSignIn: authMocks.takePendingRedirectSignIn,
  resolveRedirectSignInUser: authMocks.resolveRedirectSignInUser,
}));

vi.mock("../agent/auth-authority", () => ({
  bindDirectChatAuthority: authMocks.bindDirectChatAuthority,
  clearDirectChatAuthority: authMocks.clearDirectChatAuthority,
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: class {},
  GoogleAuthProvider: class {
    setCustomParameters() {}
  },
  getIdToken: authMocks.getIdToken,
  onAuthStateChanged: authMocks.onAuthStateChanged,
  signOut: authMocks.signOut,
}));

afterEach(() => {
  cleanup();
});

beforeEach(() => {
  vi.resetAllMocks();
  sessionStorage.clear();
  authMocks.startAuthFlow.mockResolvedValue({
    flowId: "flow-id",
    outcome: "proof_required",
    expiresAt: "2026-08-01T01:00:00Z",
  });
  authMocks.resolveAuthFlow.mockResolvedValue({
    flowId: "flow-id",
    outcome: "signed_in",
    continuation: "/",
    expiresAt: "2026-08-01T01:00:00Z",
    humanId: "user-b",
  });
  authMocks.confirmAuthFlow.mockResolvedValue({
    flowId: "flow-id",
    outcome: "account_created",
    continuation: "/",
    expiresAt: "2026-08-01T01:00:00Z",
    humanId: "user-new",
  });
  authMocks.logoutSumiSession.mockResolvedValue(undefined);
  authMocks.getSumiProfile.mockResolvedValue({
    participant: { kind: "human", humanId: "user-a" },
    displayName: "After",
    tagline: "",
  });
  authMocks.updateSumiProfile.mockResolvedValue({
    id: "user-a",
    displayName: "After",
    profile: {
      participant: { kind: "human", humanId: "user-a" },
      displayName: "After",
      tagline: "",
    },
  });
  authMocks.beginSameEmailCredentialRecovery.mockResolvedValue(undefined);
  authMocks.completeSameEmailCredentialRecovery.mockResolvedValue(
    "provider_linked",
  );
  authMocks.createAuthFlowNonce.mockReturnValue("n".repeat(43));
  authMocks.discardAuthFlow.mockResolvedValue(undefined);
  authMocks.loadActiveEmailCodeFlow.mockReturnValue(null);
  authMocks.pendingEmailLink.mockReturnValue(null);
  // The tab leaves for the provider: the promise that normally navigates away
  // simply never settles in a test.
  authMocks.beginRedirectSignIn.mockReturnValue(new Promise(() => {}));
  authMocks.hasPendingRedirectSignIn.mockReturnValue(false);
  authMocks.takePendingRedirectSignIn.mockReturnValue(null);
  authMocks.isSameEmailCredentialCollision.mockReturnValue(false);
  authMocks.clearDirectChatAuthority.mockReturnValue(true);
  authMocks.onAuthStateChanged.mockImplementation((_auth, _observer) =>
    vi.fn(),
  );
});

function AuthStateProbe() {
  const auth = useAuth();
  const [confirmedTagline, setConfirmedTagline] = useState("none");
  const [profileUpdateResult, setProfileUpdateResult] = useState("idle");
  return (
    <>
      <div data-testid="session-state">{auth.sessionState}</div>
      <div data-testid="user-id">{auth.user?.id ?? "none"}</div>
      <div data-testid="display-name">{auth.user?.displayName ?? "none"}</div>
      <div data-testid="authority-binding">
        {auth.authorityBindingId ?? "none"}
      </div>
      <div data-testid="profile-update-result">{profileUpdateResult}</div>
      <div data-testid="confirmed-tagline">{confirmedTagline}</div>
      <div data-testid="confirmation">
        {auth.confirmation?.action ?? "none"}
      </div>
      <div data-testid="redirect-error">
        {auth.redirectSignInError instanceof Error
          ? auth.redirectSignInError.name
          : "none"}
      </div>
      <div data-testid="redirect-error-message">
        {auth.redirectSignInError
          ? getAuthErrorMessage(auth.redirectSignInError)
          : "none"}
      </div>
      <div data-testid="outcome">
        {auth.outcomeNotice
          ? `${auth.outcomeNotice.outcome}:${auth.outcomeNotice.intent}:${auth.outcomeNotice.intentTransition}`
          : "none"}
      </div>
      <button
        type="button"
        onClick={() => void auth.logout().catch(() => undefined)}
      >
        logout
      </button>
      <button
        type="button"
        onClick={() =>
          void auth.signIn("google", "sign_in").catch(() => undefined)
        }
      >
        sign in
      </button>
      <button
        type="button"
        onClick={() =>
          void auth.confirmIntentTransition().catch(() => undefined)
        }
      >
        confirm transition
      </button>
      <button
        type="button"
        onClick={() =>
          void auth.submitEmailCode("123456").catch(() => undefined)
        }
      >
        complete email
      </button>
      <button
        type="button"
        onClick={() => void auth.refreshSession().catch(() => undefined)}
      >
        refresh session
      </button>
      <button
        type="button"
        onClick={() => {
          setProfileUpdateResult("pending");
          void auth
            .updateDisplayName("After")
            .then(() => setProfileUpdateResult("succeeded"))
            .catch((error) =>
              setProfileUpdateResult(
                error instanceof SumiProfileUpdateIndeterminateError
                  ? "indeterminate"
                  : "rejected",
              ),
            );
        }}
      >
        update display name
      </button>
      <button
        type="button"
        onClick={() =>
          void auth
            .updateProfile({ tagline: "設計" })
            .then((profile) => {
              if (profile !== null) setConfirmedTagline(profile.tagline);
            })
            .catch(() => undefined)
        }
      >
        update tagline
      </button>
    </>
  );
}

/**
 * The receipt the initiating tab persisted in sessionStorage before leaving
 * for the provider. AuthContext only reads flowId, nonce, intent, provider
 * and expiresAt from it; storage validation lives in auth-flow-state.
 */
function pendingRedirectReceipt() {
  return {
    flowId: "flow-id",
    nonce: "n".repeat(43),
    intent: "sign_in" as const,
    provider: "google.com" as const,
    expiresAt: "2099-08-01T01:00:00Z",
    stage: "redirect_sent" as const,
  };
}

/**
 * Mount state after the browser returned from the provider into the same tab:
 * the receipt is present and Firebase hands back this user on this startup.
 */
function simulateRedirectReturn(user: { uid: string }) {
  authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
  authMocks.takePendingRedirectSignIn.mockReturnValue(pendingRedirectReceipt());
  authMocks.resolveRedirectSignInUser.mockResolvedValue(user);
}

describe("canonical Human profile", () => {
  it("commits the returned canonical display name into AuthContext immediately", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a", displayName: "Before" },
    });
    authMocks.updateSumiProfile.mockResolvedValue({
      id: "user-a",
      displayName: "After",
      profile: {
        participant: { kind: "human", humanId: "user-a" },
        displayName: "After",
        tagline: "",
      },
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("After");
    });
    expect(authMocks.updateSumiProfile).toHaveBeenCalledWith({
      displayName: "After",
    });
  });

  it("reconciles a committed profile update whose response was lost", async () => {
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "After" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new TypeError("disconnected"),
    );

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("After");
    });
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(2);
  });

  it.each([
    {
      status: 408,
      result: "succeeded",
      displayName: "After",
      sessionReads: 2,
      profileReads: 1,
    },
    {
      status: 429,
      result: "succeeded",
      displayName: "After",
      sessionReads: 2,
      profileReads: 1,
    },
    {
      status: 503,
      result: "succeeded",
      displayName: "After",
      sessionReads: 2,
      profileReads: 1,
    },
    {
      status: 422,
      result: "rejected",
      displayName: "Before",
      sessionReads: 1,
      profileReads: 0,
    },
  ])("classifies HTTP $status profile responses as $result", async ({
    status,
    result,
    displayName,
    sessionReads,
    profileReads,
  }) => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a", displayName: "Before" },
    });
    authMocks.updateSumiProfile.mockRejectedValue(
      new AuthAPIError("profile response", status),
    );

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("profile-update-result")).toHaveTextContent(
        result,
      );
    });
    expect(screen.getByTestId("display-name")).toHaveTextContent(displayName);
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(sessionReads);
    expect(authMocks.getSumiProfile).toHaveBeenCalledTimes(profileReads);
    expect(screen.getByTestId("authority-binding")).toHaveTextContent(
      authorityBindingA,
    );
  });

  it("reports an ambiguous response with an unchanged canonical profile as indeterminate", async () => {
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new AuthAPIError("upstream unavailable", 503),
    );
    authMocks.getSumiProfile.mockResolvedValue({
      participant: { kind: "human", humanId: "user-a" },
      displayName: "Before",
      tagline: "",
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("profile-update-result")).toHaveTextContent(
        "indeterminate",
      );
    });
    expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "authenticated",
    );
  });

  it("reconciles a committed tagline-only update from the durable profile", async () => {
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new TypeError("disconnected"),
    );
    authMocks.getSumiProfile.mockResolvedValue({
      participant: { kind: "human", humanId: "user-a" },
      displayName: "Before",
      tagline: "設計",
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    fireEvent.click(screen.getByRole("button", { name: "update tagline" }));

    await waitFor(() => {
      expect(screen.getByTestId("confirmed-tagline")).toHaveTextContent("設計");
    });
    expect(authMocks.updateSumiProfile).toHaveBeenCalledWith({
      tagline: "設計",
    });
    expect(authMocks.getSumiProfile).toHaveBeenCalledTimes(1);
  });

  it("resets private state before publishing a reconciled authority binding", async () => {
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingB,
        user: { id: "user-a", displayName: "After" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new TypeError("disconnected"),
    );

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    authMocks.bindDirectChatAuthority.mockClear();

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("After");
    });
    expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
      authorityBindingB,
    );
  });

  it.each([
    "profile read fails",
    "profile identity mismatches",
  ])("keeps a replacement authority coherent when %s", async (outcome) => {
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValue({
        authenticated: true,
        authorityBindingId: authorityBindingB,
        user: { id: "user-a", displayName: "Session B" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new TypeError("disconnected"),
    );
    if (outcome === "profile read fails") {
      authMocks.getSumiProfile.mockRejectedValue(
        new Error("profile unavailable"),
      );
    } else {
      authMocks.getSumiProfile.mockResolvedValue({
        participant: { kind: "human", humanId: "user-b" },
        displayName: "Wrong Human",
        tagline: "",
      });
    }

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    authMocks.bindDirectChatAuthority.mockClear();
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("profile-update-result")).toHaveTextContent(
        "indeterminate",
      );
    });
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "authenticated",
    );
    expect(screen.getByTestId("user-id")).toHaveTextContent("user-a");
    expect(screen.getByTestId("display-name")).toHaveTextContent("Session B");
    expect(screen.getByTestId("authority-binding")).toHaveTextContent(
      authorityBindingB,
    );
    expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
      authorityBindingB,
    );
    expect(authMocks.clearDirectChatAuthority).not.toHaveBeenCalled();

    authMocks.bindDirectChatAuthority.mockClear();
    authMocks.getSumiProfile.mockResolvedValue({
      participant: { kind: "human", humanId: "user-a" },
      displayName: "After",
      tagline: "",
    });
    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );
    await waitFor(() => {
      expect(screen.getByTestId("profile-update-result")).toHaveTextContent(
        "succeeded",
      );
    });
    expect(authMocks.bindDirectChatAuthority).not.toHaveBeenCalled();
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(3);
  });

  it("publishes a replacement authority even when the rename did not commit", async () => {
    authMocks.getSumiProfile.mockResolvedValue({
      participant: { kind: "human", humanId: "user-a" },
      displayName: "Before",
      tagline: "",
    });
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingB,
        user: { id: "user-a", displayName: "Before" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new TypeError("disconnected"),
    );

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    authMocks.bindDirectChatAuthority.mockClear();

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
        authorityBindingB,
      );
    });
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "authenticated",
    );
    expect(screen.getByTestId("display-name")).toHaveTextContent("Before");

    authMocks.bindDirectChatAuthority.mockClear();
    authMocks.getSumiSession.mockResolvedValueOnce({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-a", displayName: "Before" },
    });
    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );
    await waitFor(() => {
      expect(authMocks.getSumiSession).toHaveBeenCalledTimes(3);
    });
    expect(authMocks.bindDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("fails closed when a reconciled authority cannot clear private state", async () => {
    authMocks.getSumiSession
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingA,
        user: { id: "user-a", displayName: "Before" },
      })
      .mockResolvedValueOnce({
        authenticated: true,
        authorityBindingId: authorityBindingB,
        user: { id: "user-a", displayName: "After" },
      });
    authMocks.updateSumiProfile.mockRejectedValue(
      new TypeError("disconnected"),
    );
    authMocks.bindDirectChatAuthority.mockImplementation((bindingID) => {
      if (bindingID === authorityBindingB) {
        throw new Error("private reset failed");
      }
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("display-name")).toHaveTextContent("Before");
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unavailable",
      );
    });
    expect(screen.getByTestId("user-id")).toHaveTextContent("none");
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
  });

  it("does not let a queued profile update invalidate a later logout", async () => {
    let resolveFirstUpdate!: (value: {
      id: string;
      displayName: string;
      profile: {
        participant: { kind: "human"; humanId: string };
        displayName: string;
        tagline: string;
      };
    }) => void;
    const firstUpdate = new Promise<{
      id: string;
      displayName: string;
      profile: {
        participant: { kind: "human"; humanId: string };
        displayName: string;
        tagline: string;
      };
    }>((resolve) => {
      resolveFirstUpdate = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a", displayName: "Before" },
    });
    authMocks.updateSumiProfile.mockReturnValue(firstUpdate);

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );
    await waitFor(() => {
      expect(authMocks.updateSumiProfile).toHaveBeenCalledTimes(1);
    });
    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    resolveFirstUpdate({
      id: "user-a",
      displayName: "After",
      profile: {
        participant: { kind: "human", humanId: "user-a" },
        displayName: "After",
        tagline: "",
      },
    });

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.updateSumiProfile).toHaveBeenCalledTimes(1);
    expect(authMocks.logoutSumiSession).toHaveBeenCalledTimes(1);
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
  });
});

describe("logout authority transition", () => {
  it("keeps the UI unauthenticated when Firebase cleanup setup throws synchronously", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-1" },
    });
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    authMocks.getFirebaseAuth.mockImplementation(() => {
      throw new Error("emulator setup failed");
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.logoutSumiSession).toHaveBeenCalledTimes(1);
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(authMocks.getFirebaseAuth).toHaveBeenCalledTimes(1);
  });

  it("preserves the current conversation authority when Sumi logout fails", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.logoutSumiSession.mockRejectedValue(new Error("logout failed"));
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    expect(authMocks.clearDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("fails closed when server logout succeeds but private authority reset fails", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    authMocks.clearDirectChatAuthority.mockReturnValueOnce(false);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.bindDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unavailable",
      );
      expect(screen.getByTestId("user-id")).toHaveTextContent("none");
    });
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(authMocks.bindDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("does not publish an unauthenticated refresh when private reset fails", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.clearDirectChatAuthority.mockReturnValueOnce(false);

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unavailable",
      );
    });
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(authMocks.bindDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("binds the new identity before publishing a successful sign-in", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    // This mount is the browser returning from the provider: the startup
    // session read is deferred while the receipt's flow is exchanged.
    simulateRedirectReturn({ uid: "firebase-b" });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.verifyCommittedSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
        authorityBindingB,
      );
    });
    expect(authMocks.resolveAuthFlow).toHaveBeenCalledWith({
      flowId: "flow-id",
      nonce: "n".repeat(43),
      idToken: "id-token-b",
    });
    expect(authMocks.verifyCommittedSumiSession).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "authenticated",
    );
    expect(screen.getByTestId("outcome")).toHaveTextContent(
      "signed_in:sign_in:none",
    );
    expect(sessionStorage.getItem("sumi.auth.outcome-notice.v1")).toBeNull();
    // The deferred startup read never probed the old cookie: the exchanged
    // proof alone published the session.
    expect(authMocks.getSumiSession).not.toHaveBeenCalled();
  });

  it("holds session reads while the provider redirect is leaving the tab", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));

    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalledWith({
        provider: "google.com",
        intent: "sign_in",
        isAborted: expect.any(Function),
      });
    });
    // Navigation can lag the click. Until the tab actually leaves, a session
    // re-read must not publish an unauthenticated flicker over the departure.
    fireEvent.click(screen.getByRole("button", { name: "refresh session" }));
    await Promise.resolve();
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(1);
  });

  it("starts bounded email-code recovery for a same-email provider collision", async () => {
    const collision = new Error("credential collision");
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(
      pendingRedirectReceipt(),
    );
    // getRedirectResult rejects with the collision before any credential is
    // produced, so no Firebase sign-out is owed.
    authMocks.resolveRedirectSignInUser.mockRejectedValue(collision);
    authMocks.isSameEmailCredentialCollision.mockImplementation(
      (error) => error === collision,
    );
    authMocks.beginSameEmailCredentialRecovery.mockResolvedValue({
      active: { state: "s".repeat(24), flow: recoveryEmailFlow() },
      challenge: null,
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(authMocks.beginSameEmailCredentialRecovery).toHaveBeenCalledWith(
        collision,
        "google.com",
        "sign_in",
      );
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.signOut).not.toHaveBeenCalled();
    expect(authMocks.resolveAuthFlow).not.toHaveBeenCalled();
  });

  it("links the pending provider only after email proof signed into an existing Human", async () => {
    const firebaseUser = {
      uid: "firebase-existing",
      displayName: null,
      email: "existing@example.com",
    };
    const recovery = {
      version: 1 as const,
      provider: "github.com" as const,
      requestedIntent: "sign_up" as const,
      expiresAt: "2099-08-01T01:00:00Z",
      credential: {
        providerId: "github.com" as const,
        signInMethod: "github.com" as const,
        pendingToken: "pending-token",
      },
    };
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    const emailFlow = { ...recoveryEmailFlow(), credentialRecovery: recovery };
    authMocks.loadActiveEmailCodeFlow.mockReturnValue({
      state: "s".repeat(24),
      flow: emailFlow,
    });
    authMocks.verifyEmailCode.mockImplementation(async (active: unknown) => ({
      active,
      customToken: "custom-token",
    }));
    authMocks.finishEmailProof.mockResolvedValue({
      flow: emailFlow,
      result: {
        flowId: "email-flow",
        outcome: "signed_in",
        continuation: "/",
        expiresAt: "2099-08-01T01:00:00Z",
        humanId: "human-existing",
      },
      firebaseUser,
    });
    authMocks.completeSameEmailCredentialRecovery.mockResolvedValue(
      "provider_linked",
    );
    authMocks.verifyCommittedSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "human-existing" },
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    fireEvent.click(screen.getByRole("button", { name: "complete email" }));

    await waitFor(() => {
      expect(
        authMocks.completeSameEmailCredentialRecovery,
      ).toHaveBeenCalledWith({ recovery, user: firebaseUser });
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    expect(screen.getByTestId("outcome")).toHaveTextContent(
      "provider_linked:sign_up:recovery_proved",
    );
    expect(authMocks.verifyEmailCode).toHaveBeenCalledWith(
      { state: "s".repeat(24), flow: emailFlow },
      "123456",
    );
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(null);
  });

  it("keeps the session, Firebase state, and code flow after a wrong code", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    const active = { state: "s".repeat(24), flow: recoveryEmailFlow() };
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(active);
    authMocks.verifyEmailCode.mockRejectedValue(
      new AuthAPIError("code_mismatch", 422, { attemptsRemaining: 3 }),
    );
    let reported: unknown = null;
    function EmailCodeProbe() {
      const auth = useAuth();
      return (
        <>
          <span data-testid="email-code">
            {auth.emailCode?.email ?? "none"}
          </span>
          <span data-testid="session-state">{auth.sessionState}</span>
          <button
            type="button"
            onClick={() =>
              void auth.submitEmailCode("000000").catch((error) => {
                reported = error;
              })
            }
          >
            submit code
          </button>
        </>
      );
    }

    render(
      <AuthProvider>
        <EmailCodeProbe />
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "submit code" }));

    await waitFor(() => expect(reported).toBeInstanceOf(AuthAPIError));
    expect(getAuthErrorMessage(reported)).toBe(
      "確認コードが正しくありません。あと3回入力できます。",
    );
    expect(authMocks.finishEmailProof).not.toHaveBeenCalled();
    expect(authMocks.logoutSumiSession).not.toHaveBeenCalled();
    expect(authMocks.signOut).not.toHaveBeenCalled();
    expect(authMocks.abandonEmailCodeFlow).not.toHaveBeenCalled();
    expect(screen.getByTestId("email-code")).toHaveTextContent(
      "existing@example.com",
    );
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(null);
  });

  it("does not mint a session for an intent mismatch until explicit confirmation", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    const firebaseUser = {
      uid: "firebase-new",
      displayName: "New Human",
      email: "new@example.com",
    };
    const firebaseAuth = {
      currentUser: firebaseUser,
      authStateReady: vi.fn().mockResolvedValue(undefined),
    };
    authMocks.getFirebaseAuth.mockReturnValue(firebaseAuth);
    simulateRedirectReturn(firebaseUser);
    authMocks.getIdToken
      .mockResolvedValueOnce("id-token-new")
      .mockResolvedValueOnce("id-token-fresh");
    authMocks.resolveAuthFlow.mockResolvedValue({
      flowId: "flow-id",
      outcome: "confirmation_required",
      nextAction: "create_account",
      continuation: "/",
      expiresAt: "2026-08-01T01:00:00Z",
    });
    authMocks.confirmAuthFlow.mockResolvedValue({
      flowId: "flow-id",
      outcome: "account_created",
      continuation: "/",
      expiresAt: "2026-08-01T01:00:00Z",
      humanId: "user-new",
    });
    authMocks.verifyCommittedSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-new" },
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("confirmation")).toHaveTextContent(
        "create_account",
      );
    });
    expect(authMocks.verifyCommittedSumiSession).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "confirm transition" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    expect(authMocks.confirmAuthFlow).toHaveBeenCalledWith({
      flowId: "flow-id",
      nonce: "n".repeat(43),
      action: "create_account",
    });
    expect(authMocks.getIdToken).toHaveBeenLastCalledWith(firebaseUser, true);
    expect(authMocks.resolveAuthFlow).toHaveBeenLastCalledWith({
      flowId: "flow-id",
      nonce: "n".repeat(43),
      idToken: "id-token-fresh",
    });
    expect(screen.getByTestId("outcome")).toHaveTextContent(
      "account_created:sign_in:confirmed",
    );
  });

  it("invalidates pending confirmation when Firebase auth state changes", async () => {
    let authObserver: ((user: { uid: string } | null) => void) | undefined;
    authMocks.onAuthStateChanged.mockImplementation((_auth, observer) => {
      authObserver = observer;
      return vi.fn();
    });
    const firebaseUser = {
      uid: "firebase-new",
      displayName: null,
      email: "new@example.com",
    };
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({ currentUser: firebaseUser });
    simulateRedirectReturn(firebaseUser);
    authMocks.getIdToken.mockResolvedValue("id-token-new");
    authMocks.resolveAuthFlow.mockResolvedValue({
      flowId: "flow-id",
      outcome: "confirmation_required",
      nextAction: "create_account",
      continuation: "/",
      expiresAt: "2026-08-01T01:00:00Z",
    });
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("confirmation")).toHaveTextContent(
        "create_account",
      );
    });

    authObserver?.({ uid: "different-firebase-user" });

    await waitFor(() => {
      expect(screen.getByTestId("confirmation")).toHaveTextContent("none");
    });
    expect(authMocks.confirmAuthFlow).not.toHaveBeenCalled();
  });

  it("logs out a Sumi session committed while the Firebase account changes", async () => {
    const firebaseUser = {
      uid: "firebase-new",
      displayName: null,
      email: "new@example.com",
    };
    const firebaseAuth = {
      currentUser: firebaseUser as { uid: string } | null,
      authStateReady: vi.fn().mockResolvedValue(undefined),
    };
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue(firebaseAuth);
    simulateRedirectReturn(firebaseUser);
    authMocks.getIdToken.mockResolvedValue("id-token-new");
    authMocks.resolveAuthFlow.mockResolvedValue({
      flowId: "flow-id",
      outcome: "confirmation_required",
      nextAction: "create_account",
      continuation: "/",
      expiresAt: "2026-08-01T01:00:00Z",
    });
    authMocks.confirmAuthFlow.mockImplementation(async () => {
      firebaseAuth.currentUser = { uid: "different-firebase-user" };
      return {
        flowId: "flow-id",
        outcome: "account_created",
        continuation: "/",
        expiresAt: "2026-08-01T01:00:00Z",
        humanId: "user-new",
      };
    });
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("confirmation")).toHaveTextContent(
        "create_account",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "confirm transition" }));

    await waitFor(() => {
      expect(authMocks.logoutSumiSession).toHaveBeenCalledTimes(1);
    });
    expect(authMocks.verifyCommittedSumiSession).not.toHaveBeenCalled();
    expect(screen.getByTestId("confirmation")).toHaveTextContent("none");
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
  });

  it("clears old client authority after a committed exchange is compensated", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    simulateRedirectReturn({ uid: "firebase-b" });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.verifyCommittedSumiSession.mockRejectedValue(
      new SumiSessionCompensatedError(
        new AuthAPIError("status unavailable", 503),
      ),
    );
    authMocks.signOut.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    // The compensation clears once; the deferred session read then confirms an
    // unauthenticated server state and clears any residue again.
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalled();
  });

  it("does not establish a stale Firebase success after logout takes the generation", async () => {
    let resolveReturn!: (value: { uid: string }) => void;
    const returnRead = new Promise<{ uid: string }>((resolve) => {
      resolveReturn = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(
      pendingRedirectReceipt(),
    );
    authMocks.resolveRedirectSignInUser.mockReturnValue(returnRead);
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    authMocks.signOut.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(authMocks.resolveRedirectSignInUser).toHaveBeenCalled();
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    // The committed logout cleared the cookie: the deferred session read must
    // see the cleared state, not resurrect the signed-out account.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    resolveReturn({ uid: "firebase-b" });
    // The abandoned Firebase credential is cleaned up: once by logout itself
    // and once when the late redirect return notices the stale generation.
    await waitFor(() => {
      expect(authMocks.signOut).toHaveBeenCalledTimes(2);
    });

    expect(authMocks.verifyCommittedSumiSession).not.toHaveBeenCalled();
    // Logout clears once; the deferred session read confirms the cleared
    // cookie and clears any residue again.
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalled();
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
  });

  it("restores the exchanged identity when a generation-racing logout fails", async () => {
    let resolveEstablishment!: (value: {
      authenticated: true;
      authorityBindingId: string;
      user: { id: string };
    }) => void;
    const establishment = new Promise<{
      authenticated: true;
      authorityBindingId: string;
      user: { id: string };
    }>((resolve) => {
      resolveEstablishment = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    simulateRedirectReturn({ uid: "firebase-b" });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.verifyCommittedSumiSession.mockReturnValue(establishment);
    authMocks.logoutSumiSession.mockRejectedValue(new Error("logout failed"));
    authMocks.signOut.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(authMocks.verifyCommittedSumiSession).toHaveBeenCalled();
    });
    authMocks.bindDirectChatAuthority.mockClear();
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    // The failed logout is ambiguous: the subsequent server read, not the
    // previously exchanged cached identity, must establish who remains signed in.
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });
    resolveEstablishment({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
      expect(screen.getByTestId("user-id")).toHaveTextContent("user-b");
    });
    expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
      authorityBindingB,
    );
    expect(authMocks.clearDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("does not restore old authority when a stale redirect return fails after logout", async () => {
    let rejectReturn!: (error: Error) => void;
    const returnRead = new Promise<never>((_, reject) => {
      rejectReturn = reject;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(
      pendingRedirectReceipt(),
    );
    authMocks.resolveRedirectSignInUser.mockReturnValue(returnRead);
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(authMocks.resolveRedirectSignInUser).toHaveBeenCalled();
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    // The committed logout cleared the cookie before the return settled.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    rejectReturn(new Error("redirect return failed"));
    // A macrotask flush lets the rejected return settle before asserting that
    // nothing was exchanged and no stale authority was restored.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(authMocks.verifyCommittedSumiSession).not.toHaveBeenCalled();
    // Logout clears once; the deferred session read confirms the cleared
    // cookie and clears any residue again.
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalled();
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
  });
});

describe("redirect return resilience", () => {
  it("reports an expired return instead of silently dropping the receipt", async () => {
    // The person spent longer than the flow TTL at the provider. The receipt
    // survives startup, no credential arrives, and the failure is named
    // "expired" rather than absent.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue({
      ...pendingRedirectReceipt(),
      expiresAt: "2020-08-01T01:00:00Z",
    });
    authMocks.resolveRedirectSignInUser.mockRejectedValue(
      new RedirectSignInAbandonedError(),
    );

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("redirect-error")).toHaveTextContent(
        "RedirectSignInExpiredError",
      );
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.resolveAuthFlow).not.toHaveBeenCalled();
  });

  it("still exchanges a live credential whose receipt only looks expired", async () => {
    // A fast device clock makes the receipt look stale. The server owns flow
    // expiry, so the credential is still offered for exchange.
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue({
      ...pendingRedirectReceipt(),
      expiresAt: "2020-08-01T01:00:00Z",
    });
    authMocks.resolveRedirectSignInUser.mockResolvedValue({
      uid: "firebase-b",
    });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.verifyCommittedSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    expect(authMocks.resolveAuthFlow).toHaveBeenCalledWith({
      flowId: "flow-id",
      nonce: "n".repeat(43),
      idToken: "id-token-b",
    });
  });

  it("reports a return whose stored record failed validation", async () => {
    // A raw sessionStorage record existed at startup but no valid receipt can
    // be claimed from it. The completion still runs and reports the miss
    // instead of landing on a silent login screen.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(null);

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("redirect-error")).toHaveTextContent(
        "RedirectSignInAbandonedError",
      );
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.resolveRedirectSignInUser).not.toHaveBeenCalled();
  });

  it("completes an unclaimed return after a back/forward-cache restore", async () => {
    // The tab left for the provider, the page was cached, and Back restored
    // it: the never-settled navigation promise can no longer release the
    // sign-in hold. pageshow runs the normal completion once instead.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalled();
    });
    // The hold is real: a session read stays deferred while navigation lags.
    fireEvent.click(screen.getByRole("button", { name: "refresh session" }));
    await Promise.resolve();
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(1);

    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(
      pendingRedirectReceipt(),
    );
    authMocks.resolveRedirectSignInUser.mockRejectedValue(
      new RedirectSignInAbandonedError(),
    );
    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(screen.getByTestId("redirect-error")).toHaveTextContent(
        "RedirectSignInAbandonedError",
      );
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(2);
  });

  it("releases the navigation hold when a restored page has no return", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalled();
    });
    fireEvent.click(screen.getByRole("button", { name: "refresh session" }));
    await Promise.resolve();
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(1);

    // Restored without a receipt: nothing to complete, but the hold must
    // still be released so login and session reads work again.
    authMocks.hasPendingRedirectSignIn.mockReturnValue(false);
    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(authMocks.getSumiSession).toHaveBeenCalledTimes(2);
    });
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
    expect(screen.getByTestId("redirect-error")).toHaveTextContent("none");
  });

  it("ignores an unpersisted pageshow so normal loads never release the hold", async () => {
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalled();
    });

    await act(async () => {
      const event = new Event("pageshow");
      Object.defineProperty(event, "persisted", { value: false });
      window.dispatchEvent(event);
      await Promise.resolve();
    });

    fireEvent.click(screen.getByRole("button", { name: "refresh session" }));
    await Promise.resolve();
    // The hold survives: only a persisted restore means navigation ended.
    expect(authMocks.getSumiSession).toHaveBeenCalledTimes(1);
    expect(authMocks.takePendingRedirectSignIn).not.toHaveBeenCalled();
  });

  it("completes a second attempt's back/forward-cache return in the same document", async () => {
    // First attempt: leave for the provider, restore via Back, land on the
    // recoverable error. The claim ref belongs to that attempt's receipt —
    // a new attempt writes a new receipt and resets it, so its own restore
    // must complete too instead of staying silent.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalledTimes(1);
    });
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(
      pendingRedirectReceipt(),
    );
    authMocks.resolveRedirectSignInUser.mockRejectedValue(
      new RedirectSignInAbandonedError(),
    );
    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });
    await waitFor(() => {
      expect(screen.getByTestId("redirect-error")).toHaveTextContent(
        "RedirectSignInAbandonedError",
      );
    });

    // Second attempt in the same document: a fresh receipt, a fresh return.
    authMocks.takePendingRedirectSignIn.mockReturnValue({
      ...pendingRedirectReceipt(),
      flowId: "flow-id-2",
    });
    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalledTimes(2);
    });
    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(authMocks.takePendingRedirectSignIn).toHaveBeenCalledTimes(2);
      expect(authMocks.resolveRedirectSignInUser).toHaveBeenCalledTimes(2);
      expect(screen.getByTestId("redirect-error")).toHaveTextContent(
        "RedirectSignInAbandonedError",
      );
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
  });

  it("aborts an in-flight begin when the page is restored mid-registration", async () => {
    // The restore path must mark the still-registering attempt obsolete so a
    // late startAuthFlow resolution cannot drag the tab to the provider, and
    // a later attempt must not inherit the cancellation.
    let isAborted: (() => boolean) | undefined;
    authMocks.beginRedirectSignIn.mockImplementation(
      (options: { isAborted?: () => boolean }) => {
        isAborted = options.isAborted;
        return new Promise<void>(() => undefined);
      },
    );
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalledTimes(1);
    });
    const firstAttemptAborted = isAborted;
    expect(firstAttemptAborted?.()).toBe(false);

    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });
    expect(firstAttemptAborted?.()).toBe(true);
    // The restore also released the navigation hold and read the session.
    await waitFor(() => {
      expect(authMocks.getSumiSession).toHaveBeenCalledTimes(2);
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.beginRedirectSignIn).toHaveBeenCalledTimes(2);
    });
    expect(isAborted).not.toBe(firstAttemptAborted);
    expect(isAborted?.()).toBe(false);
  });

  it("names an expired flow when the server rejects a real credential", async () => {
    // The person finished at the provider after the flow TTL: a credential
    // returns, the exchange runs, and the server's 410 flow_expired must
    // read as "expired — try again", not the generic session failure.
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.hasPendingRedirectSignIn.mockReturnValue(true);
    authMocks.takePendingRedirectSignIn.mockReturnValue(
      pendingRedirectReceipt(),
    );
    authMocks.resolveRedirectSignInUser.mockResolvedValue({
      uid: "firebase-b",
    });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.resolveAuthFlow.mockRejectedValue(
      new AuthAPIError("Authentication flow expired.", 410),
    );

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("redirect-error-message")).toHaveTextContent(
        "ログインの有効期限が切れました",
      );
    });
    expect(authMocks.resolveAuthFlow).toHaveBeenCalledWith({
      flowId: "flow-id",
      nonce: "n".repeat(43),
      idToken: "id-token-b",
    });
    // The server rejected the exchange, so the orphaned Firebase identity is
    // display state only and is signed out.
    expect(authMocks.signOut).toHaveBeenCalled();
  });
});

describe("account switch and closed-flow authority", () => {
  function SwitchProbe() {
    const auth = useAuth();
    return (
      <>
        <div data-testid="session-state">{auth.sessionState}</div>
        <div data-testid="user-id">{auth.user?.id ?? "none"}</div>
        <div data-testid="switch-prompt">
          {auth.accountSwitch
            ? `${auth.accountSwitch.currentUserId}->${auth.accountSwitch.target}`
            : "none"}
        </div>
        <button
          type="button"
          onClick={() => void auth.submitEmailCode("123456").catch(() => undefined)}
        >
          complete email
        </button>
        <button
          type="button"
          onClick={() => void auth.confirmAccountSwitch().catch(() => undefined)}
        >
          confirm switch
        </button>
        <button type="button" onClick={() => auth.cancelAccountSwitch()}>
          cancel switch
        </button>
        <button
          type="button"
          onClick={() => auth.cancelEmailCode()}
        >
          cancel email
        </button>
        <button
          type="button"
          onClick={() => void auth.logout().catch(() => undefined)}
        >
          logout
        </button>
      </>
    );
  }

  function activeEmailFlow() {
    return { state: "s".repeat(24), flow: recoveryEmailFlow() };
  }

  it("offers an explicit switch instead of silently replacing the session", async () => {
    const active = activeEmailFlow();
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a", displayName: "Current" },
    });
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(active);
    authMocks.verifyEmailCode.mockResolvedValue({
      active,
      customToken: "custom-token",
    });
    authMocks.finishEmailProof
      .mockRejectedValueOnce(new AuthAPIError("session_active", 409))
      .mockResolvedValue({
        flow: active.flow,
        result: {
          flowId: "email-flow",
          outcome: "signed_in",
          continuation: "/",
          expiresAt: "2099-08-01T01:00:00Z",
          humanId: "user-b",
        },
        firebaseUser: { uid: "firebase-b", displayName: null, email: null },
      });
    authMocks.verifyCommittedSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });

    render(
      <AuthProvider>
        <SwitchProbe />
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "complete email" }));

    // The session stays with the active Human while the prompt is open.
    await waitFor(() =>
      expect(screen.getByTestId("switch-prompt")).toHaveTextContent(
        "user-a->existing@example.com",
      ),
    );
    expect(screen.getByTestId("user-id")).toHaveTextContent("user-a");
    expect(authMocks.signOut).not.toHaveBeenCalled();

    // Confirming retries the same completion naming the replaced Human.
    fireEvent.click(screen.getByRole("button", { name: "confirm switch" }));
    await waitFor(() => {
      expect(authMocks.finishEmailProof).toHaveBeenLastCalledWith(
        expect.anything(),
        { switchFromUserId: "user-a" },
      );
      expect(screen.getByTestId("user-id")).toHaveTextContent("user-b");
      expect(screen.getByTestId("switch-prompt")).toHaveTextContent("none");
    });
  });

  it("cancelling a switch discards only the interrupted flow", async () => {
    const active = activeEmailFlow();
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a", displayName: "Current" },
    });
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(active);
    authMocks.verifyEmailCode.mockResolvedValue({
      active,
      customToken: "custom-token",
    });
    authMocks.finishEmailProof.mockRejectedValue(
      new AuthAPIError("session_active", 409),
    );

    render(
      <AuthProvider>
        <SwitchProbe />
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "complete email" }));
    await waitFor(() =>
      expect(screen.getByTestId("switch-prompt")).toHaveTextContent("user-a->"),
    );

    fireEvent.click(screen.getByRole("button", { name: "cancel switch" }));
    await waitFor(() =>
      expect(authMocks.discardAuthFlow).toHaveBeenCalledWith(
        "email-flow",
        "n".repeat(43),
      ),
    );
    expect(screen.getByTestId("switch-prompt")).toHaveTextContent("none");
    expect(screen.getByTestId("user-id")).toHaveTextContent("user-a");
    expect(authMocks.logoutSumiSession).not.toHaveBeenCalled();
  });

  it("resumes without a prompt when the conflicting session already ended", async () => {
    const active = activeEmailFlow();
    let sessionReads = 0;
    authMocks.getSumiSession.mockImplementation(async () => {
      sessionReads += 1;
      return sessionReads === 1
        ? {
            authenticated: true,
            authorityBindingId: authorityBindingA,
            user: { id: "user-a" },
          }
        : { authenticated: false };
    });
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(active);
    authMocks.verifyEmailCode.mockResolvedValue({
      active,
      customToken: "custom-token",
    });
    authMocks.finishEmailProof
      .mockRejectedValueOnce(new AuthAPIError("session_active", 409))
      .mockResolvedValue({
        flow: active.flow,
        result: {
          flowId: "email-flow",
          outcome: "signed_in",
          continuation: "/",
          expiresAt: "2099-08-01T01:00:00Z",
          humanId: "user-b",
        },
        firebaseUser: { uid: "firebase-b", displayName: null, email: null },
      });
    authMocks.verifyCommittedSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });

    render(
      <AuthProvider>
        <SwitchProbe />
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "complete email" }));

    await waitFor(() =>
      expect(screen.getByTestId("user-id")).toHaveTextContent("user-b"),
    );
    expect(screen.getByTestId("switch-prompt")).toHaveTextContent("none");
  });

  it("logout names pending email flows so the server closes their replay authority", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    savePendingEmailFlow("s".repeat(24), recoveryEmailFlow());

    render(
      <AuthProvider>
        <SwitchProbe />
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "logout" }));

    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      ),
    );
    expect(authMocks.logoutSumiSession).toHaveBeenCalledWith([
      { flowId: "email-flow", nonce: "n".repeat(43) },
    ]);
    expect(loadPendingEmailFlow("s".repeat(24))).toBeNull();
  });

  it("cancelling a pending email code closes the flow's issuance authority", async () => {
    const active = activeEmailFlow();
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.loadActiveEmailCodeFlow.mockReturnValue(active);

    render(
      <AuthProvider>
        <SwitchProbe />
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "cancel email" }));

    await waitFor(() =>
      expect(authMocks.discardAuthFlow).toHaveBeenCalledWith(
        "email-flow",
        "n".repeat(43),
      ),
    );
  });
});

function dispatchPersistedPageShow() {
  const event = new Event("pageshow");
  Object.defineProperty(event, "persisted", { value: true });
  window.dispatchEvent(event);
}

function recoveryEmailFlow() {
  return {
    flowId: "email-flow",
    nonce: "n".repeat(43),
    intent: "sign_in" as const,
    provider: "email_code" as const,
    email: "existing@example.com",
    expiresAt: "2099-08-01T01:00:00Z",
    stage: "code_sent" as const,
  };
}
