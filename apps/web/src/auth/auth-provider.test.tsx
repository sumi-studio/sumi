// @vitest-environment jsdom

import { TooltipProvider } from "@sumi/ui/components/tooltip";
import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SettingsPopover } from "../components/app-navigation";
import { Sidebar } from "../messaging/components/sidebar";
import { MockMessagingServer } from "../messaging/mock-server";
import { setActiveMessagingScope } from "../messaging/scope";
import {
  bindMessagingScope,
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../messaging/store";
import { AuthProvider, useAuth } from "./auth-context";
import { AuthAPIError, SumiSessionCompensatedError } from "./session-client";

const authorityBindingA = "A".repeat(43);
const authorityBindingB = `${"B".repeat(42)}E`;

function confirmedProfile(id: string, displayName: string, revision = 1) {
  return {
    participant: { kind: "human" as const, humanId: id },
    displayName,
    tagline: "",
    revision,
  };
}

const authMocks = vi.hoisted(() => ({
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
  }),
  confirmAuthFlow: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  beginEmailLinkAuth: vi.fn(),
  completeEmailLinkAuth: vi.fn(),
  hasEmailLinkCallback: vi.fn(() => false),
  rejectEmailLinkAuth: vi.fn(),
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
  signInWithPopup: vi.fn(),
  getIdToken: vi.fn(),
  bindDirectChatAuthority: vi.fn(),
  clearDirectChatAuthority: vi.fn(() => true),
}));

vi.mock("./session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-client")>()),
  getSumiSession: authMocks.getSumiSession,
  logoutSumiSession: authMocks.logoutSumiSession,
  updateSumiProfile: authMocks.updateSumiProfile,
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

vi.mock("./credential-recovery", () => ({
  beginSameEmailCredentialRecovery: authMocks.beginSameEmailCredentialRecovery,
  completeSameEmailCredentialRecovery:
    authMocks.completeSameEmailCredentialRecovery,
  isSameEmailCredentialCollision: authMocks.isSameEmailCredentialCollision,
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

vi.mock("../messaging/place-route", () => ({
  usePlaceNavigate: () => vi.fn(),
}));

vi.mock("./provider-settings", () => ({
  ProviderSettings: () => null,
}));

vi.mock("../theme/theme-provider", () => ({
  useTheme: () => ({ theme: "system", setTheme: vi.fn() }),
}));

vi.mock("../participant/app-menu", () => ({
  ParticipantAppsMenu: () => null,
}));

afterEach(() => {
  cleanup();
  bindMessagingSessionIdentity(null);
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
  });
  authMocks.confirmAuthFlow.mockResolvedValue({
    flowId: "flow-id",
    outcome: "account_created",
    continuation: "/",
    expiresAt: "2026-08-01T01:00:00Z",
  });
  authMocks.logoutSumiSession.mockResolvedValue(undefined);
  authMocks.updateSumiProfile.mockResolvedValue({
    id: "user-a",
    displayName: "After",
    profile: confirmedProfile("user-a", "After"),
  });
  authMocks.beginEmailLinkAuth.mockResolvedValue(undefined);
  authMocks.beginSameEmailCredentialRecovery.mockResolvedValue(undefined);
  authMocks.completeSameEmailCredentialRecovery.mockResolvedValue(
    "provider_linked",
  );
  authMocks.createAuthFlowNonce.mockReturnValue("n".repeat(43));
  authMocks.hasEmailLinkCallback.mockReturnValue(false);
  authMocks.isSameEmailCredentialCollision.mockReturnValue(false);
  authMocks.clearDirectChatAuthority.mockReturnValue(true);
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
      <div data-testid="display-name">{auth.user?.displayName ?? "none"}</div>
      <div data-testid="confirmation">
        {auth.confirmation?.action ?? "none"}
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
        onClick={() => void auth.completeEmailLink().catch(() => undefined)}
      >
        complete email
      </button>
      <button
        type="button"
        onClick={() =>
          void auth.updateDisplayName("After").catch(() => undefined)
        }
      >
        update display name
      </button>
    </>
  );
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
      profile: confirmedProfile("user-a", "After"),
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
    expect(authMocks.updateSumiProfile).toHaveBeenCalledWith("After");
  });

  it("projects a self profile_updated into the sidebar and settings from one confirmed profile", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "h-yohaku", displayName: "session fallback" },
    });
    const server = new MockMessagingServer();
    bindMessagingSessionIdentity("h-yohaku");
    installMessagingBackend(server);
    useMessaging.getState().init();

    render(
      <AuthProvider>
        <TooltipProvider>
          <AuthStateProbe />
          <Sidebar selectedPlaceKey={null} workspaceId="ws-sumi" />
          <SettingsPopover />
        </TooltipProvider>
      </AuthProvider>,
    );
    await waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    await waitFor(() =>
      expect(screen.getByTestId("display-name")).toHaveTextContent("yohaku"),
    );

    // 別タブでの保存はこのtabではprofile_updatedだけとして届く。
    await server.updateProfile({ displayName: "別タブの確定名" });

    await waitFor(() =>
      expect(screen.getByTestId("display-name")).toHaveTextContent(
        "別タブの確定名",
      ),
    );
    expect(screen.getAllByText("別タブの確定名")).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    const displayName = screen.getByRole("textbox", { name: "表示名" });
    expect(displayName).toHaveValue("別タブの確定名");

    // Workspace transportを外しても、認証UIはparticipant-globalな投影を読む。
    setActiveMessagingScope({
      workspaceId: "ws-sumi",
      installationId: "installation-sumi",
      authorityEpoch: "1",
    });
    bindMessagingScope(null);
    expect(useMessaging.getState().self).toBeNull();
    expect(screen.getByTestId("display-name")).toHaveTextContent(
      "別タブの確定名",
    );

    // 同じ参加者で再bindしても、bootstrapと認証UIは同じ確定値に収束する。
    installMessagingBackend(server);
    useMessaging.getState().init();
    await waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    expect(screen.getByTestId("display-name")).toHaveTextContent(
      "別タブの確定名",
    );
  });

  it("projects the /auth/profile ACK while Messaging is unbound and keeps it after rebind", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "h-yohaku", displayName: "session fallback" },
    });
    authMocks.updateSumiProfile.mockResolvedValue({
      id: "h-yohaku",
      displayName: "After",
      profile: confirmedProfile("h-yohaku", "After"),
    });
    const server = new MockMessagingServer();
    bindMessagingSessionIdentity("h-yohaku");
    installMessagingBackend(server);
    useMessaging.getState().init();

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    await waitFor(() =>
      expect(screen.getByTestId("display-name")).toHaveTextContent("yohaku"),
    );

    setActiveMessagingScope({
      workspaceId: "ws-sumi",
      installationId: "installation-sumi",
      authorityEpoch: "1",
    });
    bindMessagingScope(null);
    expect(useMessaging.getState().ready).toBe(false);

    fireEvent.click(
      screen.getByRole("button", { name: "update display name" }),
    );
    await waitFor(() =>
      expect(screen.getByTestId("display-name")).toHaveTextContent("After"),
    );

    installMessagingBackend(server);
    useMessaging.getState().init();
    await waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    expect(screen.getByTestId("display-name")).toHaveTextContent("After");
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

  it("publishes a replacement authority even when the rename did not commit", async () => {
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
      profile: ReturnType<typeof confirmedProfile>;
    }) => void;
    const firstUpdate = new Promise<{
      id: string;
      displayName: string;
      profile: ReturnType<typeof confirmedProfile>;
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
      profile: confirmedProfile("user-a", "After"),
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
    expect(screen.getByTestId("outcome")).toHaveTextContent(
      "signed_in:sign_in:none",
    );
    expect(sessionStorage.getItem("sumi.auth.outcome-notice.v1")).toBeNull();
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

  it("starts bounded magic-link recovery for a same-email provider collision", async () => {
    const collision = new Error("credential collision");
    authMocks.getSumiSession.mockResolvedValue({ authenticated: false });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockRejectedValue(collision);
    authMocks.isSameEmailCredentialCollision.mockImplementation(
      (error) => error === collision,
    );
    authMocks.beginSameEmailCredentialRecovery.mockResolvedValue(undefined);

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
      expect(authMocks.beginSameEmailCredentialRecovery).toHaveBeenCalledWith(
        collision,
        "google.com",
        "sign_in",
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
    authMocks.completeEmailLinkAuth.mockResolvedValue({
      flow: {
        flowId: "email-flow",
        nonce: "n".repeat(43),
        intent: "sign_in",
        provider: "email_link",
        email: "existing@example.com",
        expiresAt: "2099-08-01T01:00:00Z",
        stage: "firebase_complete",
        credentialRecovery: recovery,
      },
      result: {
        flowId: "email-flow",
        outcome: "signed_in",
        continuation: "/",
        expiresAt: "2099-08-01T01:00:00Z",
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
