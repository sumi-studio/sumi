// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, useAuth } from "./auth-context";
import { AuthAPIError, SumiSessionCompensatedError } from "./session-client";

const authorityBindingA = "A".repeat(43);
const authorityBindingB = `${"B".repeat(42)}E`;

const authMocks = vi.hoisted(() => ({
  getSumiSession: vi.fn(),
  logoutSumiSession: vi.fn(),
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
  }),
  confirmAuthFlow: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  beginEmailLinkAuth: vi.fn(),
  completeEmailLinkAuth: vi.fn(),
  hasEmailLinkCallback: vi.fn(() => false),
  rejectEmailLinkAuth: vi.fn(),
  getFirebaseAuth: vi.fn(),
  onAuthStateChanged: vi.fn(
    (_auth: unknown, _observer: (user: { uid: string } | null) => void) =>
      vi.fn(),
  ),
  signOut: vi.fn(),
  signInWithPopup: vi.fn(),
  getIdToken: vi.fn(),
  bindDirectChatAuthority: vi.fn(),
  clearDirectChatAuthority: vi.fn(() => true),
}));

vi.mock("./session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-client")>()),
  getSumiSession: authMocks.getSumiSession,
  logoutSumiSession: authMocks.logoutSumiSession,
  verifyCommittedSumiSession: authMocks.verifyCommittedSumiSession,
}));

vi.mock("./auth-flow-client", () => ({
  confirmAuthFlow: authMocks.confirmAuthFlow,
  createAuthFlowNonce: authMocks.createAuthFlowNonce,
  resolveAuthFlow: authMocks.resolveAuthFlow,
  startAuthFlow: authMocks.startAuthFlow,
}));

vi.mock("./email-link-auth", () => ({
  beginEmailLinkAuth: authMocks.beginEmailLinkAuth,
  completeEmailLinkAuth: authMocks.completeEmailLinkAuth,
  hasEmailLinkCallback: authMocks.hasEmailLinkCallback,
  rejectEmailLinkAuth: authMocks.rejectEmailLinkAuth,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: authMocks.getFirebaseAuth,
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
  signInWithPopup: authMocks.signInWithPopup,
  signOut: authMocks.signOut,
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

beforeEach(() => {
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
  });
  authMocks.confirmAuthFlow.mockResolvedValue({
    flowId: "flow-id",
    outcome: "account_created",
    continuation: "/",
    expiresAt: "2026-08-01T01:00:00Z",
  });
  authMocks.logoutSumiSession.mockResolvedValue(undefined);
  authMocks.createAuthFlowNonce.mockReturnValue("n".repeat(43));
  authMocks.hasEmailLinkCallback.mockReturnValue(false);
  authMocks.onAuthStateChanged.mockImplementation((_auth, _observer) =>
    vi.fn(),
  );
});

function AuthStateProbe() {
  const auth = useAuth();
  return (
    <>
      <div data-testid="session-state">{auth.sessionState}</div>
      <div data-testid="user-id">{auth.user?.id ?? "none"}</div>
      <div data-testid="confirmation">
        {auth.confirmation?.action ?? "none"}
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
    </>
  );
}

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
    const firebaseAuth = {};
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue(firebaseAuth);
    authMocks.signInWithPopup.mockResolvedValue({
      user: { uid: "firebase-b" },
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
    authMocks.bindDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));

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
  });

  it("opens the Firebase popup synchronously before the flow start settles", async () => {
    let resolveStart!: (value: {
      flowId: string;
      outcome: "proof_required";
      expiresAt: string;
    }) => void;
    const start = new Promise<{
      flowId: string;
      outcome: "proof_required";
      expiresAt: string;
    }>((resolve) => {
      resolveStart = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.startAuthFlow.mockReturnValue(start);
    authMocks.signInWithPopup.mockResolvedValue({
      user: {
        uid: "firebase-b",
        displayName: null,
        email: "b@example.com",
      },
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
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));

    expect(authMocks.signInWithPopup).toHaveBeenCalledTimes(1);
    expect(authMocks.startAuthFlow).toHaveBeenCalledTimes(1);
    resolveStart({
      flowId: "flow-id",
      outcome: "proof_required",
      expiresAt: "2026-08-01T01:00:00Z",
    });
    await waitFor(() => {
      expect(authMocks.verifyCommittedSumiSession).toHaveBeenCalled();
    });
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
    authMocks.signInWithPopup.mockResolvedValue({
      user: firebaseUser,
    });
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
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
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
    authMocks.signInWithPopup.mockResolvedValue({ user: firebaseUser });
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
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
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
    authMocks.signInWithPopup.mockResolvedValue({ user: firebaseUser });
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
      };
    });
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
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
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockResolvedValue({
      user: { uid: "firebase-b" },
    });
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
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
  });

  it("does not establish a stale Firebase success after logout takes the generation", async () => {
    let resolvePopup!: (value: { user: { uid: string } }) => void;
    const popup = new Promise<{ user: { uid: string } }>((resolve) => {
      resolvePopup = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockReturnValue(popup);
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    authMocks.signOut.mockResolvedValue(undefined);
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

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.signInWithPopup).toHaveBeenCalled();
    });
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    resolvePopup({ user: { uid: "firebase-b" } });
    await waitFor(() => {
      expect(authMocks.signOut).toHaveBeenCalled();
    });

    expect(authMocks.verifyCommittedSumiSession).not.toHaveBeenCalled();
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
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
    authMocks.signInWithPopup.mockResolvedValue({
      user: { uid: "firebase-b" },
    });
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
      expect(screen.getByTestId("user-id")).toHaveTextContent("user-a");
    });
    authMocks.bindDirectChatAuthority.mockClear();
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.verifyCommittedSumiSession).toHaveBeenCalled();
    });
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
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

  it("does not restore old authority when a stale Firebase popup fails after logout", async () => {
    let rejectPopup!: (error: Error) => void;
    const popup = new Promise<never>((_, reject) => {
      rejectPopup = reject;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockReturnValue(popup);
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
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

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.signInWithPopup).toHaveBeenCalled();
    });
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    rejectPopup(new Error("popup failed"));
    await Promise.resolve();

    expect(authMocks.verifyCommittedSumiSession).not.toHaveBeenCalled();
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
  });
});
