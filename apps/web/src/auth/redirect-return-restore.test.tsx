// @vitest-environment jsdom

// Redirect-restore UX regression tests: these keep the REAL
// redirect-sign-in.ts, auth-flow-state.ts, AuthGate, and LoginScreen — only
// the network and Firebase SDK boundaries are mocked — so a back/forward-cache
// restore exercises the same receipt lifecycle and the same form a person
// sees. (Structure follows the independent fresh-acceptance probes.)

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { useEffect, useRef } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, useAuth } from "./auth-context";
import { AuthGate } from "./auth-gate";

const mocks = vi.hoisted(() => ({
  getSumiSession: vi.fn(),
  verifyCommittedSumiSession: vi.fn(),
  logoutSumiSession: vi.fn(),
  getSumiProfile: vi.fn(),
  updateSumiProfile: vi.fn(),
  startAuthFlow: vi.fn(),
  resolveAuthFlow: vi.fn(),
  confirmAuthFlow: vi.fn(),
  getRedirectResult: vi.fn(),
  signInWithRedirect: vi.fn(),
  signOut: vi.fn(),
  getIdToken: vi.fn(),
  onAuthStateChanged: vi.fn(),
  beginEmailLinkAuth: vi.fn(),
  completeEmailLinkAuth: vi.fn(),
  hasEmailLinkCallback: vi.fn(() => false),
  rejectEmailLinkAuth: vi.fn(),
  beginSameEmailCredentialRecovery: vi.fn(),
  completeSameEmailCredentialRecovery: vi.fn(),
  getFirebaseAuth: vi.fn(() => ({})),
  bindDirectChatAuthority: vi.fn(),
  clearDirectChatAuthority: vi.fn(() => true),
}));

vi.mock("./session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-client")>()),
  getSumiSession: mocks.getSumiSession,
  verifyCommittedSumiSession: mocks.verifyCommittedSumiSession,
  logoutSumiSession: mocks.logoutSumiSession,
  getSumiProfile: mocks.getSumiProfile,
  updateSumiProfile: mocks.updateSumiProfile,
}));

vi.mock("./auth-flow-client", () => ({
  startAuthFlow: mocks.startAuthFlow,
  resolveAuthFlow: mocks.resolveAuthFlow,
  confirmAuthFlow: mocks.confirmAuthFlow,
  createAuthFlowNonce: () => "n".repeat(43),
}));

vi.mock("./email-link-auth", () => ({
  beginEmailLinkAuth: mocks.beginEmailLinkAuth,
  completeEmailLinkAuth: mocks.completeEmailLinkAuth,
  hasEmailLinkCallback: mocks.hasEmailLinkCallback,
  rejectEmailLinkAuth: mocks.rejectEmailLinkAuth,
}));

vi.mock("./credential-recovery", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./credential-recovery")>()),
  beginSameEmailCredentialRecovery: mocks.beginSameEmailCredentialRecovery,
  completeSameEmailCredentialRecovery:
    mocks.completeSameEmailCredentialRecovery,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: mocks.getFirebaseAuth,
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: class GithubAuthProvider {
    providerId = "github.com";
  },
  GoogleAuthProvider: class GoogleAuthProvider {
    providerId = "google.com";
    setCustomParameters() {}
  },
  getRedirectResult: mocks.getRedirectResult,
  signInWithRedirect: mocks.signInWithRedirect,
  signOut: mocks.signOut,
  getIdToken: mocks.getIdToken,
  onAuthStateChanged: mocks.onAuthStateChanged,
}));

vi.mock("../agent/auth-authority", () => ({
  bindDirectChatAuthority: mocks.bindDirectChatAuthority,
  clearDirectChatAuthority: mocks.clearDirectChatAuthority,
}));

// AuthGate support: the session-identity stores are module state behind the
// gate; bind/get pairs keep it satisfied without touching real stores.
const identityStore = vi.hoisted(() => ({
  messaging: { current: null as string | null },
  workspace: { current: null as string | null },
  workspaceScope: { current: null as string | null },
}));

vi.mock("../messaging/store", () => ({
  bindMessagingSessionIdentity: (id: string | null) => {
    identityStore.messaging.current = id;
  },
  getMessagingSessionIdentity: () => identityStore.messaging.current,
  suspendMessagingTransport: () => undefined,
}));

vi.mock("../workspace/store", () => ({
  bindWorkspaceSessionIdentity: (id: string | null, scope: string | null) => {
    identityStore.workspace.current = id;
    identityStore.workspaceScope.current = scope;
  },
  getWorkspaceSessionIdentity: () => identityStore.workspace.current,
  getWorkspaceSessionScopeKey: () => identityStore.workspaceScope.current,
}));

vi.mock("../participant/app-store", () => ({
  useParticipantApps: {
    getState: () => ({ bindParticipant: () => undefined }),
  },
}));

function Probe() {
  const auth = useAuth();
  const transitions = useRef<string[]>([]);
  useEffect(() => {
    transitions.current.push(auth.sessionState);
  }, [auth.sessionState]);
  (globalThis as Record<string, unknown>).__transitions = transitions.current;
  return <div data-testid="session-state">{auth.sessionState}</div>;
}

function dispatchPersistedPageShow() {
  const event = new Event("pageshow");
  Object.defineProperty(event, "persisted", { value: true });
  window.dispatchEvent(event);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function transitions(): string[] {
  return (globalThis as Record<string, unknown>).__transitions as string[];
}

const googleButton = () =>
  screen.getByRole("button", { name: /Googleで続ける/ });
const emailInput = () =>
  screen.getByLabelText("メールアドレス") as HTMLInputElement;

afterEach(() => {
  cleanup();
  delete (globalThis as Record<string, unknown>).__transitions;
});

beforeEach(() => {
  vi.resetAllMocks();
  sessionStorage.clear();
  identityStore.messaging.current = null;
  identityStore.workspace.current = null;
  identityStore.workspaceScope.current = null;
  mocks.getFirebaseAuth.mockReturnValue({});
  mocks.getSumiSession.mockResolvedValue({ authenticated: false });
  mocks.startAuthFlow.mockResolvedValue({
    flowId: "flow-1",
    outcome: "proof_required",
    expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
  });
  mocks.verifyCommittedSumiSession.mockResolvedValue({
    authenticated: true,
    authorityBindingId: `${"B".repeat(42)}E`,
    user: { id: "user-b", displayName: null },
  });
  // The navigation promise legitimately never settles inside the tab.
  mocks.signInWithRedirect.mockReturnValue(new Promise(() => {}));
  mocks.signOut.mockResolvedValue(undefined);
  mocks.getIdToken.mockResolvedValue("id-token");
  mocks.onAuthStateChanged.mockImplementation(() => vi.fn());
});

describe("redirect restore UX", () => {
  it("a page restored while the flow was still registering is not dragged to the provider", async () => {
    // The person taps Google, changes their mind while the Sumi flow
    // registration is still in flight, leaves via Back, then returns. The
    // restore marks the attempt obsolete: when the registration resolves it
    // must not navigate this tab away again.
    const started = deferred<{
      flowId: string;
      outcome: string;
      expiresAt: string;
    }>();
    mocks.startAuthFlow.mockReturnValue(started.promise);

    render(
      <AuthProvider>
        <Probe />
        <AuthGate>{null}</AuthGate>
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      ),
    );
    fireEvent.change(emailInput(), {
      target: { value: "person@example.com" },
    });

    fireEvent.click(googleButton());
    await waitFor(() => expect(mocks.startAuthFlow).toHaveBeenCalledTimes(1));
    expect(mocks.signInWithRedirect).not.toHaveBeenCalled();

    // The tab leaves and is restored before the registration resolves.
    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });
    // The restore released the navigation hold and ran a session read.
    await waitFor(() => expect(mocks.getSumiSession).toHaveBeenCalledTimes(2));

    await act(async () => {
      started.resolve({
        flowId: "flow-1",
        outcome: "proof_required",
        expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
      });
      await Promise.resolve();
      await Promise.resolve();
    });

    // No second trip to the provider, no receipt left behind, and the
    // attempt reports the same recoverable outcome as a post-navigation
    // cancel — through the click handler that was still awaiting signIn.
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "ログインは完了しませんでした",
      ),
    );
    expect(mocks.signInWithRedirect).not.toHaveBeenCalled();
    expect(sessionStorage.getItem("sumi.auth.redirect-flow.v1")).toBeNull();
    expect(googleButton()).toBeEnabled();
    expect(emailInput().value).toBe("person@example.com");

    // A retry in the same document navigates normally.
    mocks.startAuthFlow.mockResolvedValue({
      flowId: "flow-2",
      outcome: "proof_required",
      expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
    });
    fireEvent.click(googleButton());
    await waitFor(() =>
      expect(mocks.signInWithRedirect).toHaveBeenCalledTimes(1),
    );
    expect(sessionStorage.getItem("sumi.auth.redirect-flow.v1")).not.toBeNull();
  });

  it("a cancelled return keeps the mounted login form and its local state through the background read", async () => {
    // Ordinary cancel: tap Google, provider page, Back. The completion
    // reports the recoverable error and reconciles the session in the
    // background — without unmounting the login form, so a typed address
    // survives the whole round trip.
    const sessionRead = deferred<{ authenticated: false }>();
    mocks.getSumiSession
      .mockResolvedValueOnce({ authenticated: false }) // initial mount read
      .mockReturnValueOnce(sessionRead.promise); // the post-return read
    mocks.getRedirectResult.mockResolvedValue(null);

    render(
      <AuthProvider>
        <Probe />
        <AuthGate>{null}</AuthGate>
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      ),
    );
    fireEvent.change(emailInput(), {
      target: { value: "person@example.com" },
    });

    fireEvent.click(googleButton());
    await waitFor(() =>
      expect(mocks.signInWithRedirect).toHaveBeenCalledTimes(1),
    );

    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });

    // The error is set and the reconciliation read is in flight — while the
    // login form stays mounted with its typed address.
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "ログインは完了しませんでした",
      ),
    );
    await waitFor(() => expect(mocks.getSumiSession).toHaveBeenCalledTimes(2));
    expect(
      screen.queryByText("ログイン状態を確認しています…"),
    ).not.toBeInTheDocument();
    expect(emailInput().value).toBe("person@example.com");
    expect(transitions().filter((s) => s === "checking")).toHaveLength(1);

    await act(async () => {
      sessionRead.resolve({ authenticated: false });
      await Promise.resolve();
    });

    expect(emailInput().value).toBe("person@example.com");
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
    expect(transitions().filter((s) => s === "checking")).toHaveLength(1);
  });

  it("the background reconciliation still publishes a session committed elsewhere", async () => {
    // The read is not decorative: when the server reports a session — a
    // return exchanged in another tab, or a commit under a lost response —
    // the gate must still transition to the authenticated workspace.
    const sessionRead = deferred<{
      authenticated: true;
      authorityBindingId: string;
      user: { id: string; displayName: string | null };
    }>();
    mocks.getSumiSession
      .mockResolvedValueOnce({ authenticated: false })
      .mockReturnValueOnce(sessionRead.promise);
    mocks.getRedirectResult.mockResolvedValue(null);

    render(
      <AuthProvider>
        <Probe />
        <AuthGate>
          <div data-testid="workspace">workspace</div>
        </AuthGate>
      </AuthProvider>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      ),
    );

    fireEvent.click(googleButton());
    await waitFor(() =>
      expect(mocks.signInWithRedirect).toHaveBeenCalledTimes(1),
    );
    await act(async () => {
      dispatchPersistedPageShow();
      await Promise.resolve();
    });
    await waitFor(() => expect(mocks.getSumiSession).toHaveBeenCalledTimes(2));

    await act(async () => {
      sessionRead.resolve({
        authenticated: true,
        authorityBindingId: `${"B".repeat(42)}E`,
        user: { id: "user-b", displayName: null },
      });
      await Promise.resolve();
    });

    await waitFor(() =>
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      ),
    );
    await waitFor(() =>
      expect(screen.getByTestId("workspace")).toBeInTheDocument(),
    );
    // Direct unauthenticated → authenticated transition: no checking flash.
    expect(transitions().filter((s) => s === "checking")).toHaveLength(1);
  });
});
