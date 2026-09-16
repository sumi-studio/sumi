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
import { FirebaseError } from "firebase/app";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { noteAuthTeardown } from "./auth-transition";
import {
  type PendingProviderRedirect,
  peekPendingProviderRedirect,
  savePendingProviderRedirect,
} from "./provider-redirect";
import { ProviderSettings } from "./provider-settings";
import { AuthAPIError } from "./session-client";

interface MockUser {
  uid: string;
  email?: string;
  emailVerified?: boolean;
  providerData: Array<{ providerId: string }>;
}

const settingsMocks = vi.hoisted(() => ({
  currentUser: null as MockUser | null,
  authObserver: null as ((user: MockUser | null) => void) | null,
  getFirebaseAuth: vi.fn(),
  onAuthStateChanged: vi.fn(),
  getIdToken: vi.fn(),
  getIdTokenResult: vi.fn(),
  getRedirectResult: vi.fn(),
  linkWithRedirect: vi.fn(),
  reauthenticateWithRedirect: vi.fn(),
  reload: vi.fn(),
  signOut: vi.fn(),
  updateCurrentUser: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  startProviderOperation: vi.fn(),
  completeProviderOperation: vi.fn(),
  failProviderOperation: vi.fn(),
  statusProviderOperation: vi.fn(),
  getProviderMethods: vi.fn(),
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: settingsMocks.getFirebaseAuth,
}));

vi.mock("./auth-flow-client", () => ({
  createAuthFlowNonce: settingsMocks.createAuthFlowNonce,
}));

vi.mock("./provider-operation-client", () => ({
  startProviderOperation: settingsMocks.startProviderOperation,
  completeProviderOperation: settingsMocks.completeProviderOperation,
  failProviderOperation: settingsMocks.failProviderOperation,
  statusProviderOperation: settingsMocks.statusProviderOperation,
  getProviderMethods: settingsMocks.getProviderMethods,
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: class GithubAuthProvider {},
  GoogleAuthProvider: class GoogleAuthProvider {
    setCustomParameters() {}
  },
  getIdToken: settingsMocks.getIdToken,
  getIdTokenResult: settingsMocks.getIdTokenResult,
  getRedirectResult: settingsMocks.getRedirectResult,
  linkWithRedirect: settingsMocks.linkWithRedirect,
  onAuthStateChanged: settingsMocks.onAuthStateChanged,
  reauthenticateWithRedirect: settingsMocks.reauthenticateWithRedirect,
  reload: settingsMocks.reload,
  signOut: settingsMocks.signOut,
  updateCurrentUser: settingsMocks.updateCurrentUser,
}));

const pendingLinkStatus = {
  operationId: "operation-1",
  provider: "google.com",
  operation: "link",
  status: "pending",
  outcome: "client_operation_required",
  clientOperation: "firebase_link_with_credential",
  completionTokenNotBefore: "2020-01-01T00:00:00Z",
  noticeRequired: false,
};

const linkedResult = {
  operationId: "operation-1",
  provider: "google.com",
  operation: "link",
  status: "completed",
  outcome: "provider_linked",
  noticeRequired: true,
};

const LEGACY_PENDING_KEY = "sumi.auth.provider-pending.v1";
const PENDING_KEY = `sumi.auth.provider-pending.v2/${encodeURIComponent("firebase-user-a")}/${encodeURIComponent("human-a")}`;
const NONCE = "n".repeat(43);

// A sent redirect never resolves inside the tab: the browser navigates away.
const leaveForProvider = () => new Promise<never>(() => {});

function storedPending() {
  return JSON.parse(localStorage.getItem(PENDING_KEY) ?? "null");
}

function storedPendingOperation(overrides: Record<string, unknown> = {}) {
  return {
    version: 1,
    firebaseUid: "firebase-user-a",
    humanId: "human-a",
    provider: "google.com",
    operation: "link",
    nonce: NONCE,
    phase: "link_ready",
    operationId: "operation-1",
    completionTokenNotBefore: "2020-01-01T00:00:00Z",
    ...overrides,
  };
}

function redirectMarker(
  overrides: Partial<PendingProviderRedirect> = {},
): PendingProviderRedirect {
  return {
    version: 1,
    kind: "link",
    provider: "google.com",
    nonce: NONCE,
    firebaseUid: "firebase-user-a",
    humanId: "human-a",
    sentAt: new Date().toISOString(),
    ...overrides,
  };
}

function linkCredential(user: MockUser) {
  return { user, operationType: "link" };
}

function setSignInClaims(signInProvider: string, authTimeAgeSeconds = 0) {
  settingsMocks.getIdTokenResult.mockResolvedValue({
    token: `fresh-${signInProvider}-token`,
    claims: {
      auth_time: Math.floor(Date.now() / 1000) - authTimeAgeSeconds,
      firebase: { sign_in_provider: signInProvider },
    },
  });
}

beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
  settingsMocks.currentUser = {
    uid: "firebase-user-a",
    providerData: [{ providerId: "password" }],
  };
  settingsMocks.authObserver = null;
  settingsMocks.getFirebaseAuth.mockImplementation(() => ({
    currentUser: settingsMocks.currentUser,
  }));
  settingsMocks.onAuthStateChanged.mockImplementation((_auth, observer) => {
    settingsMocks.authObserver = observer;
    observer(settingsMocks.currentUser);
    return vi.fn();
  });
  settingsMocks.getIdToken.mockResolvedValue("id-token");
  setSignInClaims("password");
  settingsMocks.startProviderOperation.mockResolvedValue({
    operationId: "operation-1",
    outcome: "client_operation_required",
    clientOperation: "firebase_link_with_credential",
    completionTokenNotBefore: "2020-01-01T00:00:00Z",
    noticeRequired: false,
  });
  settingsMocks.completeProviderOperation.mockResolvedValue(linkedResult);
  settingsMocks.failProviderOperation.mockResolvedValue(undefined);
  settingsMocks.statusProviderOperation.mockResolvedValue(pendingLinkStatus);
  // Default: the authoritative read is unavailable, so the local Firebase
  // view renders unchanged. Tests that exercise it set a resolved value.
  settingsMocks.getProviderMethods.mockRejectedValue(
    new Error("provider methods unavailable"),
  );
  settingsMocks.getRedirectResult.mockResolvedValue(null);
  settingsMocks.linkWithRedirect.mockImplementation(leaveForProvider);
  settingsMocks.reauthenticateWithRedirect.mockImplementation(leaveForProvider);
  settingsMocks.reload.mockResolvedValue(undefined);
  settingsMocks.updateCurrentUser.mockImplementation(async (_auth, user) => {
    settingsMocks.currentUser = user;
    settingsMocks.authObserver?.(settingsMocks.currentUser);
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.useRealTimers();
});

describe("provider settings", () => {
  it("shows a verified email-code address as a sign-in method without a password provider", () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "github.com" }],
      email: "person@example.com",
      emailVerified: true,
    };

    render(<ProviderSettings humanId="human-a" />);

    expect(screen.getByText("メール")).toBeVisible();
  });

  it("renders the server's live provider list over the stale local cache", async () => {
    // This tab's Firebase cache still carries a GitHub entry that another
    // browser already removed remotely.
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [
        { providerId: "password" },
        { providerId: "github.com" },
      ],
    };
    settingsMocks.getProviderMethods.mockResolvedValue({
      providers: [],
      email: true,
    });

    render(<ProviderSettings humanId="human-a" />);

    expect(
      screen.getByRole("button", { name: "GitHubの解除を開始" }),
    ).toBeVisible();
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "GitHubを追加" }),
      ).toBeVisible(),
    );
    expect(screen.getByText("メール")).toBeVisible();
    // The cached user is corrected in memory so the method can be added
    // again; nothing was persisted or reloaded to do it.
    expect(
      settingsMocks.currentUser?.providerData.map(
        ({ providerId }) => providerId,
      ),
    ).toEqual(["password"]);
    expect(settingsMocks.reload).not.toHaveBeenCalled();
  });

  it("shows a method the server reports even while the local cache lacks it", async () => {
    settingsMocks.getProviderMethods.mockResolvedValue({
      providers: ["google.com"],
      email: true,
    });

    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Googleの解除を開始" }),
      ).toBeVisible(),
    );
  });

  it("drops the email row when the server reports the method unusable", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "password" }],
      email: "person@example.com",
      emailVerified: true,
    };
    settingsMocks.getProviderMethods.mockResolvedValue({
      providers: [],
      email: false,
    });

    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.queryByText("メール")).not.toBeInTheDocument(),
    );
  });

  it("sends a same-tab redirect for the link and completes it on return", async () => {
    render(<ProviderSettings humanId="human-a" />);

    expect(screen.getByText("メール")).toBeVisible();
    const add = screen.getByRole("button", { name: "Googleを追加" });
    expect(add).toHaveClass("h-11");
    fireEvent.click(add);

    await waitFor(() =>
      expect(settingsMocks.linkWithRedirect).toHaveBeenCalledTimes(1),
    );
    expect(storedPending()).toMatchObject({
      nonce: NONCE,
      operationId: "operation-1",
      phase: "link_ready",
    });
    expect(peekPendingProviderRedirect()).toMatchObject({
      kind: "link",
      provider: "google.com",
      nonce: NONCE,
      firebaseUid: "firebase-user-a",
      humanId: "human-a",
    });
    // The tab is leaving now; the completion happens after the return.

    cleanup();
    settingsMocks.currentUser?.providerData.push({
      providerId: "google.com",
    });
    settingsMocks.getRedirectResult.mockResolvedValue(
      linkCredential(settingsMocks.currentUser as MockUser),
    );
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      );
    });
    expect(settingsMocks.completeProviderOperation).toHaveBeenCalledWith({
      operationId: "operation-1",
      nonce: NONCE,
      idToken: "id-token",
    });
    expect(
      JSON.parse(
        sessionStorage.getItem("sumi.auth.provider-notice.v1") ?? "null",
      ),
    ).toMatchObject({
      firebaseUid: "firebase-user-a",
      humanId: "human-a",
      operation: "linked",
    });
    expect(storedPending()).toBeNull();
    expect(peekPendingProviderRedirect()).toBeNull();
  });

  it("never leaves for the provider until the backend operation is durably acknowledged", async () => {
    let rejectStart: ((error: unknown) => void) | undefined;
    settingsMocks.startProviderOperation.mockImplementation(
      () =>
        new Promise((_resolve, reject) => {
          rejectStart = reject;
        }),
    );
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));
    await waitFor(() =>
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(1),
    );
    expect(settingsMocks.linkWithRedirect).not.toHaveBeenCalled();

    rejectStart?.(new AuthAPIError("proof_mismatch", 403));
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "再認証を確認できませんでした",
      ),
    );
    expect(settingsMocks.linkWithRedirect).not.toHaveBeenCalled();
    expect(storedPending()).toBeNull();
  });

  it("retries start with the same nonce before sending the redirect", async () => {
    settingsMocks.startProviderOperation
      .mockRejectedValueOnce(new TypeError("request not sent"))
      .mockResolvedValueOnce({
        operationId: "operation-1",
        outcome: "client_operation_required",
        clientOperation: "firebase_link_with_credential",
        completionTokenNotBefore: "2020-01-01T00:00:00Z",
        noticeRequired: false,
      });
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));

    await waitFor(() =>
      expect(settingsMocks.linkWithRedirect).toHaveBeenCalledTimes(1),
    );
    expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(2);
    expect(settingsMocks.startProviderOperation.mock.calls[0]?.[0].nonce).toBe(
      NONCE,
    );
    expect(settingsMocks.startProviderOperation.mock.calls[1]?.[0].nonce).toBe(
      NONCE,
    );
  });

  it("keeps a resumable pending operation when the redirect cannot be sent", async () => {
    settingsMocks.linkWithRedirect.mockRejectedValue(
      new FirebaseError("auth/web-storage-unsupported", "storage unavailable"),
    );
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));

    await waitFor(() => expect(screen.getByRole("alert")).toBeInTheDocument());
    expect(peekPendingProviderRedirect()).toBeNull();
    expect(storedPending()).toMatchObject({ phase: "link_ready" });
    expect(
      screen.getByRole("button", { name: "Googleで認証を続ける" }),
    ).toBeVisible();
  });

  it("settles a cancelled provider return without touching the link", async () => {
    localStorage.setItem(PENDING_KEY, JSON.stringify(storedPendingOperation()));
    savePendingProviderRedirect(redirectMarker());
    settingsMocks.getRedirectResult.mockResolvedValue(null);
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "認証をキャンセルしました。",
      ),
    );
    expect(settingsMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "operation-1",
      nonce: NONCE,
      outcome: "cancelled",
    });
    expect(settingsMocks.completeProviderOperation).not.toHaveBeenCalled();
    expect(storedPending()).toBeNull();
    expect(peekPendingProviderRedirect()).toBeNull();
  });

  it("reports a credential already bound elsewhere from the redirect return", async () => {
    localStorage.setItem(PENDING_KEY, JSON.stringify(storedPendingOperation()));
    savePendingProviderRedirect(redirectMarker());
    settingsMocks.getRedirectResult.mockRejectedValue(
      new FirebaseError("auth/credential-already-in-use", "in use"),
    );
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "このログイン方法は別のアカウントで使用されています。",
      ),
    );
    expect(settingsMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "operation-1",
      nonce: NONCE,
      outcome: "credential_in_use",
    });
    expect(storedPending()).toBeNull();
  });

  it("does not adopt a redirect result belonging to a different Firebase user", async () => {
    localStorage.setItem(PENDING_KEY, JSON.stringify(storedPendingOperation()));
    savePendingProviderRedirect(redirectMarker());
    const otherUser: MockUser = {
      uid: "firebase-user-b",
      providerData: [{ providerId: "password" }, { providerId: "google.com" }],
    };
    settingsMocks.getRedirectResult.mockResolvedValue(
      linkCredential(otherUser),
    );
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "ログイン方法を変更できませんでした",
      ),
    );
    expect(settingsMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "operation-1",
      nonce: NONCE,
      outcome: "firebase_operation_failed",
    });
    expect(settingsMocks.completeProviderOperation).not.toHaveBeenCalled();
  });

  it("reconciles a link that was applied by a result this tab never read", async () => {
    localStorage.setItem(PENDING_KEY, JSON.stringify(storedPendingOperation()));
    savePendingProviderRedirect(redirectMarker());
    // The result was consumed elsewhere (for example a shared sign-in
    // return), but the provider made it onto the account.
    settingsMocks.currentUser?.providerData.push({
      providerId: "google.com",
    });
    settingsMocks.getRedirectResult.mockResolvedValue(null);
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      ),
    );
    expect(settingsMocks.completeProviderOperation).toHaveBeenCalled();
    expect(storedPending()).toBeNull();
  });

  it("replays the receipt nonce when it no longer matches the stored change", async () => {
    // Another tab started a different change for the same account while this
    // tab was away: its record survives untouched, and the returned link is
    // recovered through the server's nonce-idempotent begin instead of being
    // silently dropped.
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(
        storedPendingOperation({
          provider: "github.com",
          nonce: "m".repeat(43),
          operationId: "operation-github",
        }),
      ),
    );
    savePendingProviderRedirect(redirectMarker());
    settingsMocks.currentUser?.providerData.push({
      providerId: "google.com",
    });
    settingsMocks.getRedirectResult.mockResolvedValue(
      linkCredential(settingsMocks.currentUser as MockUser),
    );
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledWith(
        expect.objectContaining({
          operation: "link",
          nonce: NONCE,
        }),
      ),
    );
    expect(await screen.findByText("Googleを追加しました")).toBeInTheDocument();
    // The recovered write never claimed the slot: GitHub's own record stays.
    expect(storedPending()).toMatchObject({
      provider: "github.com",
      nonce: "m".repeat(43),
    });
    expect(peekPendingProviderRedirect()).toBeNull();
  });

  it("reports a reauth receipt that no longer matches a pending change", async () => {
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(
        storedPendingOperation({
          operation: "unlink",
          phase: "unlink_starting",
          nonce: "m".repeat(43),
        }),
      ),
    );
    savePendingProviderRedirect(
      redirectMarker({ kind: "reauth", provider: "github.com" }),
    );
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "この認証結果は保留中の変更と一致しませんでした",
      ),
    );
    expect(settingsMocks.getRedirectResult).not.toHaveBeenCalled();
    // The other nonce's unlink intent stays resumable under its own scope.
    expect(storedPending()).toMatchObject({ nonce: "m".repeat(43) });
    expect(peekPendingProviderRedirect()).toBeNull();
  });

  it("reconciles a response-lost link completion after transient status failures", async () => {
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(storedPendingOperation({ phase: "link_mutated" })),
    );
    savePendingProviderRedirect(redirectMarker());
    settingsMocks.currentUser?.providerData.push({
      providerId: "google.com",
    });
    settingsMocks.getRedirectResult.mockResolvedValue(
      linkCredential(settingsMocks.currentUser as MockUser),
    );
    settingsMocks.completeProviderOperation.mockRejectedValueOnce(
      new TypeError("response lost after commit"),
    );
    settingsMocks.statusProviderOperation
      .mockRejectedValueOnce(new TypeError("status temporarily unavailable"))
      .mockRejectedValueOnce(new TypeError("status still unavailable"))
      .mockResolvedValueOnce(linkedResult);
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      );
    });
    expect(settingsMocks.completeProviderOperation).toHaveBeenCalledTimes(1);
    expect(settingsMocks.statusProviderOperation).toHaveBeenCalledTimes(3);
    expect(storedPending()).toBeNull();
  });

  it("keeps an unresolved completion and exposes an explicit same-operation resume", async () => {
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(storedPendingOperation({ phase: "link_mutated" })),
    );
    savePendingProviderRedirect(redirectMarker());
    settingsMocks.currentUser?.providerData.push({
      providerId: "google.com",
    });
    settingsMocks.getRedirectResult.mockResolvedValue(
      linkCredential(settingsMocks.currentUser as MockUser),
    );
    settingsMocks.completeProviderOperation.mockRejectedValue(
      new TypeError("request unavailable"),
    );
    settingsMocks.statusProviderOperation.mockRejectedValue(
      new TypeError("status unavailable"),
    );
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(
      () => {
        expect(screen.getByRole("alert")).toHaveTextContent(
          "追加結果をまだ確認できません",
        );
      },
      { timeout: 2_500 },
    );
    expect(
      screen.getByRole("button", { name: "Googleの追加結果を確認" }),
    ).toBeVisible();
    expect(storedPending()).toMatchObject({
      nonce: NONCE,
      operationId: "operation-1",
      phase: "link_mutated",
    });

    cleanup();
    settingsMocks.statusProviderOperation.mockReset();
    settingsMocks.statusProviderOperation.mockResolvedValue(linkedResult);
    render(<ProviderSettings humanId="human-a" />);
    const resume = await screen.findByRole("button", {
      name: "Googleの追加結果を確認",
    });
    fireEvent.click(resume);

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      ),
    );
    expect(settingsMocks.startProviderOperation).not.toHaveBeenCalled();
    expect(settingsMocks.linkWithRedirect).not.toHaveBeenCalled();
  });

  it("resends the provider redirect when a link_ready operation is resumed", async () => {
    localStorage.setItem(PENDING_KEY, JSON.stringify(storedPendingOperation()));
    render(<ProviderSettings humanId="human-a" />);

    const resume = await screen.findByRole("button", {
      name: "Googleで認証を続ける",
    });
    fireEvent.click(resume);

    await waitFor(() =>
      expect(settingsMocks.linkWithRedirect).toHaveBeenCalledTimes(1),
    );
    expect(peekPendingProviderRedirect()).toMatchObject({
      kind: "link",
      nonce: NONCE,
    });
    expect(settingsMocks.startProviderOperation).not.toHaveBeenCalled();
  });

  it("unlinks with a recent sign-in proof without a provider trip", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "password" }, { providerId: "google.com" }],
    };
    const unlinkedResult = {
      operationId: "operation-2",
      provider: "google.com",
      operation: "unlink",
      status: "completed",
      outcome: "provider_unlinked",
      noticeRequired: true,
    };
    settingsMocks.startProviderOperation.mockResolvedValue(unlinkedResult);
    settingsMocks.statusProviderOperation.mockResolvedValue(unlinkedResult);
    // A real reload() merges providerData and cannot drop the provider the
    // backend deleted; the confirmed outcome reconciles the local user.
    settingsMocks.reload.mockImplementation(async () => {});
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleの解除を開始" }));
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを解除しました",
      );
    });
    expect(settingsMocks.reauthenticateWithRedirect).not.toHaveBeenCalled();
    expect(settingsMocks.startProviderOperation).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: "unlink",
        nonce: NONCE,
        idToken: "fresh-password-token",
      }),
    );
    expect(screen.getByRole("button", { name: "Googleを追加" })).toBeVisible();
  });

  it("reauthenticates by redirect for an unlink and resumes it on return", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [
        { providerId: "google.com" },
        { providerId: "github.com" },
      ],
    };
    // The Google session is old, so the unlink needs a fresh proof through
    // the other linked method.
    setSignInClaims("google.com", 3_600);
    const unlinkedResult = {
      operationId: "operation-2",
      provider: "google.com",
      operation: "unlink",
      status: "completed",
      outcome: "provider_unlinked",
      noticeRequired: true,
    };
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleの解除を開始" }));
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));

    await waitFor(() =>
      expect(settingsMocks.reauthenticateWithRedirect).toHaveBeenCalledTimes(1),
    );
    expect(storedPending()).toMatchObject({
      operation: "unlink",
      provider: "google.com",
      phase: "unlink_starting",
    });
    expect(peekPendingProviderRedirect()).toMatchObject({
      kind: "reauth",
      provider: "github.com",
      nonce: NONCE,
    });

    cleanup();
    settingsMocks.getRedirectResult.mockResolvedValue({
      user: settingsMocks.currentUser,
      operationType: "reauthenticate",
    });
    setSignInClaims("github.com");
    settingsMocks.startProviderOperation.mockResolvedValue(unlinkedResult);
    settingsMocks.statusProviderOperation.mockResolvedValue(unlinkedResult);
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを解除しました",
      );
    });
    expect(settingsMocks.startProviderOperation).toHaveBeenCalledWith(
      expect.objectContaining({ operation: "unlink", nonce: NONCE }),
    );
    expect(storedPending()).toBeNull();
  });

  it("abandons a cancelled unlink reauthentication that was never sent", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [
        { providerId: "google.com" },
        { providerId: "github.com" },
      ],
    };
    setSignInClaims("google.com", 3_600);
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(
        storedPendingOperation({
          operation: "unlink",
          phase: "unlink_starting",
        }),
      ),
    );
    savePendingProviderRedirect(
      redirectMarker({ kind: "reauth", provider: "github.com" }),
    );
    settingsMocks.getRedirectResult.mockResolvedValue(null);
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "認証をキャンセルしました。",
      ),
    );
    expect(settingsMocks.startProviderOperation).not.toHaveBeenCalled();
    // Nothing reached the server, so the intent is abandoned outright — it
    // must not survive restart as a pending change that blocks everything.
    expect(storedPending()).toBeNull();
    expect(
      screen.getByRole("button", { name: "GitHubの解除を開始" }),
    ).toBeEnabled();
  });

  it("keeps a sent unlink resumable when the return cannot be confirmed", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [
        { providerId: "google.com" },
        { providerId: "github.com" },
      ],
    };
    setSignInClaims("google.com", 3_600);
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(
        storedPendingOperation({
          operation: "unlink",
          phase: "unlink_sent",
          operationId: "operation-2",
        }),
      ),
    );
    savePendingProviderRedirect(
      redirectMarker({ kind: "reauth", provider: "github.com" }),
    );
    settingsMocks.getRedirectResult.mockResolvedValue(null);
    render(<ProviderSettings humanId="human-a" />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "認証をキャンセルしました。",
      ),
    );
    // The request may already have reached the server: the uncertain
    // receipt is never discarded — it stays resumable for reconciliation.
    expect(storedPending()).toMatchObject({ phase: "unlink_sent" });
    expect(
      screen.getByRole("button", { name: "Googleの解除を再開" }),
    ).toBeVisible();
  });

  it("replays a backend-owned unlink with the same nonce and tolerates transient status loss", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "password" }, { providerId: "google.com" }],
    };
    const unlinkedResult = {
      operationId: "operation-2",
      provider: "google.com",
      operation: "unlink",
      status: "completed",
      outcome: "provider_unlinked",
      noticeRequired: true,
    };
    settingsMocks.startProviderOperation
      .mockRejectedValueOnce(new TypeError("response lost after commit"))
      .mockResolvedValueOnce(unlinkedResult);
    settingsMocks.statusProviderOperation
      .mockRejectedValueOnce(new TypeError("transient status loss"))
      .mockResolvedValueOnce(unlinkedResult);
    // reload() merges providerData and cannot drop a backend-deleted
    // provider; the confirmed outcome reconciles the local user.
    settingsMocks.reload.mockImplementation(async () => {});
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleの解除を開始" }));
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));

    await waitFor(() =>
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(2),
    );
    expect(settingsMocks.reauthenticateWithRedirect).not.toHaveBeenCalled();
    expect(settingsMocks.startProviderOperation.mock.calls[0]?.[0].nonce).toBe(
      NONCE,
    );
    expect(settingsMocks.startProviderOperation.mock.calls[1]?.[0].nonce).toBe(
      NONCE,
    );
    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを解除しました",
      );
    });
    expect(settingsMocks.statusProviderOperation).toHaveBeenCalledTimes(2);
    expect(screen.getByRole("button", { name: "Googleを追加" })).toBeVisible();
  });

  it("hides another account's pending operation without destroying it", async () => {
    sessionStorage.setItem(
      "sumi.auth.provider-notice.v1",
      JSON.stringify({
        version: 1,
        firebaseUid: "firebase-user-a",
        humanId: "human-a",
        provider: "google.com",
        operation: "linked",
      }),
    );
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(storedPendingOperation({ phase: "starting" })),
    );
    render(<ProviderSettings humanId="human-a" />);
    expect(screen.getByText("Googleを追加しました")).toBeVisible();

    settingsMocks.currentUser = {
      uid: "firebase-user-b",
      providerData: [{ providerId: "password" }],
    };
    act(() => settingsMocks.authObserver?.(settingsMocks.currentUser));

    await waitFor(() => {
      expect(
        screen.queryByText("Googleを追加しました"),
      ).not.toBeInTheDocument();
      expect(screen.queryByText(/再開できます/)).not.toBeInTheDocument();
    });
    expect(sessionStorage.getItem("sumi.auth.provider-notice.v1")).toBeNull();
    // User A's unfinished change stays stored under its own scope.
    expect(storedPending()).toMatchObject({ firebaseUid: "firebase-user-a" });

    sessionStorage.setItem(
      "sumi.auth.provider-notice.v1",
      JSON.stringify({
        version: 1,
        firebaseUid: "firebase-user-b",
        humanId: "human-a",
        provider: "github.com",
        operation: "linked",
      }),
    );
    act(() => settingsMocks.authObserver?.(null));
    expect(sessionStorage.getItem("sumi.auth.provider-notice.v1")).toBeNull();
  });

  it("preserves scoped recovery state through an initial null before auth restoration", async () => {
    settingsMocks.currentUser = null;
    settingsMocks.onAuthStateChanged.mockImplementation((_auth, observer) => {
      settingsMocks.authObserver = observer;
      return vi.fn();
    });
    sessionStorage.setItem(
      "sumi.auth.provider-notice.v1",
      JSON.stringify({
        version: 1,
        firebaseUid: "firebase-user-a",
        humanId: "human-a",
        provider: "google.com",
        operation: "linked",
      }),
    );
    localStorage.setItem(
      PENDING_KEY,
      JSON.stringify(
        storedPendingOperation({ operationId: "operation-restored" }),
      ),
    );

    render(<ProviderSettings humanId="human-a" />);
    expect(
      sessionStorage.getItem("sumi.auth.provider-notice.v1"),
    ).not.toBeNull();
    expect(storedPending()).not.toBeNull();

    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "password" }],
    };
    act(() => settingsMocks.authObserver?.(settingsMocks.currentUser));

    expect(await screen.findByText("Googleを追加しました")).toBeVisible();
    expect(
      screen.getByRole("button", { name: "Googleで認証を続ける" }),
    ).toBeVisible();
    expect(
      sessionStorage.getItem("sumi.auth.provider-notice.v1"),
    ).not.toBeNull();
    expect(storedPending()).not.toBeNull();
  });

  it("adopts a legacy shared-key pending record for the same scope", async () => {
    sessionStorage.setItem(
      LEGACY_PENDING_KEY,
      JSON.stringify(storedPendingOperation()),
    );
    render(<ProviderSettings humanId="human-a" />);

    expect(
      await screen.findByRole("button", { name: "Googleで認証を続ける" }),
    ).toBeVisible();
    // The record moved to its scoped key; the shared slot is retired.
    expect(storedPending()).toMatchObject({ nonce: NONCE });
    expect(sessionStorage.getItem(LEGACY_PENDING_KEY)).toBeNull();
  });

  it("cannot repopulate provider state from an old account's in-flight callback", async () => {
    let resolveStart:
      | ((result: {
          operationId: string;
          outcome: string;
          clientOperation: string;
          completionTokenNotBefore: string;
          noticeRequired: boolean;
        }) => void)
      | undefined;
    settingsMocks.startProviderOperation.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveStart = resolve;
        }),
    );
    render(<ProviderSettings humanId="human-a" />);
    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));
    await waitFor(() =>
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(1),
    );

    settingsMocks.currentUser = {
      uid: "firebase-user-b",
      providerData: [{ providerId: "password" }],
    };
    act(() => settingsMocks.authObserver?.(settingsMocks.currentUser));
    await waitFor(() =>
      expect(screen.queryByText(/再開できます/)).not.toBeInTheDocument(),
    );

    await act(async () => {
      resolveStart?.({
        operationId: "operation-old-account",
        outcome: "client_operation_required",
        clientOperation: "firebase_link_with_credential",
        completionTokenNotBefore: "2020-01-01T00:00:00Z",
        noticeRequired: false,
      });
      await Promise.resolve();
    });

    expect(settingsMocks.linkWithRedirect).not.toHaveBeenCalled();
    // The late callback must not persist user B state or adopt user A's op.
    expect(storedPending()?.firebaseUid).not.toBe("firebase-user-b");
    expect(screen.queryByText(/再開できます/)).not.toBeInTheDocument();
  });

  it("never writes the removed provider back after an account switch", async () => {
    // The unlink commits server-side while the account changes hands: the
    // late completion must not patch or persist the old Firebase user.
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "password" }, { providerId: "google.com" }],
    };
    let resolveStart: ((result: unknown) => void) | undefined;
    settingsMocks.startProviderOperation.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveStart = resolve;
        }),
    );
    const unlinkedResult = {
      operationId: "operation-2",
      provider: "google.com",
      operation: "unlink",
      status: "completed",
      outcome: "provider_unlinked",
      noticeRequired: true,
    };
    settingsMocks.statusProviderOperation.mockResolvedValue(unlinkedResult);
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleの解除を開始" }));
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));
    await waitFor(() =>
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(1),
    );

    // The account switches to B while the server call is in flight.
    const userB = {
      uid: "firebase-user-b",
      providerData: [{ providerId: "password" }],
    };
    settingsMocks.currentUser = userB;
    act(() => settingsMocks.authObserver?.(userB));

    await act(async () => {
      resolveStart?.(unlinkedResult);
      await Promise.resolve();
      await Promise.resolve();
    });

    // B's providerData was never touched, A's user was never persisted, and
    // A's finished unlink stays recoverable under its own scope.
    expect(userB.providerData).toEqual([{ providerId: "password" }]);
    expect(settingsMocks.signOut).not.toHaveBeenCalled();
    expect(storedPending()).toMatchObject({
      operation: "unlink",
      phase: "unlink_sent",
    });
    expect(screen.queryByText("Googleを解除しました")).not.toBeInTheDocument();
  });

  it("honours a sign-out initiated while the post-unlink reload is queued", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "password" }, { providerId: "google.com" }],
    };
    const unlinkedResult = {
      operationId: "operation-2",
      provider: "google.com",
      operation: "unlink",
      status: "completed",
      outcome: "provider_unlinked",
      noticeRequired: true,
    };
    settingsMocks.startProviderOperation.mockResolvedValue(unlinkedResult);
    settingsMocks.statusProviderOperation.mockResolvedValue(unlinkedResult);
    // A teardown is initiated inside the reload window — the queued auth
    // write ordering can then place this operation's persist last.
    settingsMocks.reload.mockImplementation(async () => {
      noteAuthTeardown("firebase-user-a");
    });
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleの解除を開始" }));
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));

    await waitFor(() => expect(settingsMocks.signOut).toHaveBeenCalledTimes(1));
    expect(settingsMocks.startProviderOperation).toHaveBeenCalledWith(
      expect.objectContaining({ operation: "unlink", nonce: NONCE }),
    );
  });

  it("keeps another account's pending record when a new change is started", async () => {
    // User A left an unfinished link; user B signs in and starts one. B's
    // write targets B's own key — A's record can never be clobbered.
    localStorage.setItem(PENDING_KEY, JSON.stringify(storedPendingOperation()));
    settingsMocks.currentUser = {
      uid: "firebase-user-b",
      providerData: [{ providerId: "password" }],
    };
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(
      await screen.findByRole("button", { name: "GitHubを追加" }),
    );

    await waitFor(() =>
      expect(settingsMocks.linkWithRedirect).toHaveBeenCalledTimes(1),
    );
    expect(storedPending()).toMatchObject({
      firebaseUid: "firebase-user-a",
      provider: "google.com",
      phase: "link_ready",
    });
    const bKey = `sumi.auth.provider-pending.v2/${encodeURIComponent("firebase-user-b")}/${encodeURIComponent("human-a")}`;
    expect(JSON.parse(localStorage.getItem(bKey) ?? "null")).toMatchObject({
      firebaseUid: "firebase-user-b",
      provider: "github.com",
      phase: "link_ready",
    });
  });

  it("maps a lost network reply to actionable recovery copy", async () => {
    settingsMocks.startProviderOperation.mockRejectedValue(
      new TypeError("Failed to fetch"),
    );
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));

    await waitFor(
      () => {
        expect(screen.getByRole("alert")).toHaveTextContent(
          "結果をまだ確認できません。接続を確認して「再開」を押してください。",
        );
      },
      { timeout: 2_500 },
    );
    expect(screen.getByRole("alert")).not.toHaveTextContent("Failed to fetch");
  });

  it("explains and disables removal of the final Firebase login method", () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [{ providerId: "google.com" }],
    };
    render(<ProviderSettings humanId="human-a" />);

    expect(
      screen.getByRole("button", { name: "Googleの解除を開始" }),
    ).toBeDisabled();
    expect(
      screen.getByText(
        "最後のログイン方法は解除できません。先に別の方法を追加してください。",
      ),
    ).toBeVisible();
    expect(settingsMocks.reauthenticateWithRedirect).not.toHaveBeenCalled();
  });
});
