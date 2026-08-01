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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ProviderSettings } from "./provider-settings";
import { AuthAPIError } from "./session-client";

interface MockUser {
  uid: string;
  providerData: Array<{ providerId: string }>;
}

const settingsMocks = vi.hoisted(() => ({
  currentUser: null as MockUser | null,
  authObserver: null as ((user: MockUser | null) => void) | null,
  getFirebaseAuth: vi.fn(),
  onAuthStateChanged: vi.fn(),
  getIdToken: vi.fn(),
  getIdTokenResult: vi.fn(),
  linkWithPopup: vi.fn(),
  reauthenticateWithPopup: vi.fn(),
  reload: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  startProviderOperation: vi.fn(),
  completeProviderOperation: vi.fn(),
  failProviderOperation: vi.fn(),
  statusProviderOperation: vi.fn(),
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
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: class GithubAuthProvider {},
  GoogleAuthProvider: class GoogleAuthProvider {
    setCustomParameters() {}
  },
  getIdToken: settingsMocks.getIdToken,
  getIdTokenResult: settingsMocks.getIdTokenResult,
  linkWithPopup: settingsMocks.linkWithPopup,
  onAuthStateChanged: settingsMocks.onAuthStateChanged,
  reauthenticateWithPopup: settingsMocks.reauthenticateWithPopup,
  reload: settingsMocks.reload,
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

beforeEach(() => {
  sessionStorage.clear();
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
  settingsMocks.getIdTokenResult.mockResolvedValue({
    token: "fresh-email-token",
    claims: {
      auth_time: Math.floor(Date.now() / 1000),
      firebase: { sign_in_provider: "password" },
    },
  });
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
  settingsMocks.linkWithPopup.mockImplementation(async () => {
    settingsMocks.currentUser?.providerData.push({ providerId: "google.com" });
    return { user: settingsMocks.currentUser };
  });
  settingsMocks.reauthenticateWithPopup.mockResolvedValue({});
  settingsMocks.reload.mockResolvedValue(undefined);
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.useRealTimers();
});

describe("provider settings", () => {
  it("persists an identity-scoped link notice and exposes dense 44px actions", async () => {
    render(<ProviderSettings humanId="human-a" />);

    expect(screen.getByText("メールリンク")).toBeVisible();
    const add = screen.getByRole("button", { name: "Googleを追加" });
    expect(add).toHaveClass("h-11");
    fireEvent.click(add);

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      );
    });
    expect(settingsMocks.completeProviderOperation).toHaveBeenCalledWith({
      operationId: "operation-1",
      nonce: "n".repeat(43),
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
    expect(sessionStorage.getItem("sumi.auth.provider-pending.v1")).toBeNull();
  });

  it("never mutates Firebase until the backend operation is durably acknowledged", async () => {
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
    expect(settingsMocks.linkWithPopup).not.toHaveBeenCalled();

    rejectStart?.(new AuthAPIError("proof_mismatch", 403));
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "再認証を確認できませんでした",
      ),
    );
    expect(settingsMocks.linkWithPopup).not.toHaveBeenCalled();
    expect(sessionStorage.getItem("sumi.auth.provider-pending.v1")).toBeNull();
  });

  it("retries a request-not-sent start with the same persisted nonce", async () => {
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
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(2),
    );
    expect(settingsMocks.startProviderOperation.mock.calls[0]?.[0].nonce).toBe(
      "n".repeat(43),
    );
    expect(settingsMocks.startProviderOperation.mock.calls[1]?.[0].nonce).toBe(
      "n".repeat(43),
    );
    expect(settingsMocks.linkWithPopup).toHaveBeenCalledTimes(1);
  });

  it("reconciles a response-lost link completion after transient status failures", async () => {
    settingsMocks.completeProviderOperation.mockRejectedValueOnce(
      new TypeError("response lost after commit"),
    );
    settingsMocks.statusProviderOperation
      .mockRejectedValueOnce(new TypeError("status temporarily unavailable"))
      .mockRejectedValueOnce(new TypeError("status still unavailable"))
      .mockResolvedValueOnce(linkedResult);
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      );
    });
    expect(settingsMocks.completeProviderOperation).toHaveBeenCalledTimes(1);
    expect(settingsMocks.statusProviderOperation).toHaveBeenCalledTimes(3);
    expect(sessionStorage.getItem("sumi.auth.provider-pending.v1")).toBeNull();
  });

  it("keeps an unresolved completion and exposes an explicit same-operation resume", async () => {
    settingsMocks.completeProviderOperation.mockRejectedValue(
      new TypeError("request unavailable"),
    );
    settingsMocks.statusProviderOperation.mockRejectedValue(
      new TypeError("status unavailable"),
    );
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));

    await waitFor(
      () => {
        expect(screen.getByRole("alert")).toHaveTextContent(
          "追加結果をまだ確認できません",
        );
      },
      { timeout: 2_500 },
    );
    expect(
      screen.getByRole("button", { name: "Googleの追加を再開" }),
    ).toBeVisible();
    expect(
      JSON.parse(
        sessionStorage.getItem("sumi.auth.provider-pending.v1") ?? "null",
      ),
    ).toMatchObject({
      nonce: "n".repeat(43),
      operationId: "operation-1",
      phase: "link_mutated",
    });

    cleanup();
    settingsMocks.statusProviderOperation.mockReset();
    settingsMocks.statusProviderOperation.mockResolvedValue(linkedResult);
    render(<ProviderSettings humanId="human-a" />);
    const resume = await screen.findByRole("button", {
      name: "Googleの追加を再開",
    });
    fireEvent.click(resume);

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました",
      ),
    );
    expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(2);
    expect(settingsMocks.startProviderOperation.mock.calls[0]?.[0].nonce).toBe(
      settingsMocks.startProviderOperation.mock.calls[1]?.[0].nonce,
    );
  });

  it("replays a backend-owned unlink with the same nonce and tolerates transient status loss", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user-a",
      providerData: [
        { providerId: "google.com" },
        { providerId: "github.com" },
      ],
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
    settingsMocks.reload.mockImplementation(async () => {
      if (settingsMocks.currentUser) {
        settingsMocks.currentUser.providerData = [{ providerId: "github.com" }];
      }
    });
    render(<ProviderSettings humanId="human-a" />);

    fireEvent.click(screen.getByRole("button", { name: "Googleの解除を開始" }));
    expect(
      screen.getByText(/別のリンク済み方法で再認証してから解除します/),
    ).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));

    await waitFor(() =>
      expect(settingsMocks.startProviderOperation).toHaveBeenCalledTimes(2),
    );
    expect(settingsMocks.reauthenticateWithPopup).toHaveBeenCalledTimes(1);
    expect(settingsMocks.startProviderOperation.mock.calls[0]?.[0].nonce).toBe(
      "n".repeat(43),
    );
    expect(settingsMocks.startProviderOperation.mock.calls[1]?.[0].nonce).toBe(
      "n".repeat(43),
    );
    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを解除しました",
      );
    });
    expect(settingsMocks.statusProviderOperation).toHaveBeenCalledTimes(2);
  });

  it("clears notices and pending state on an account switch and logout", async () => {
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
    sessionStorage.setItem(
      "sumi.auth.provider-pending.v1",
      JSON.stringify({
        version: 1,
        firebaseUid: "firebase-user-a",
        humanId: "human-a",
        provider: "github.com",
        operation: "link",
        nonce: "n".repeat(43),
        phase: "starting",
      }),
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
    });
    expect(sessionStorage.getItem("sumi.auth.provider-notice.v1")).toBeNull();
    expect(sessionStorage.getItem("sumi.auth.provider-pending.v1")).toBeNull();

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
      expect(
        sessionStorage.getItem("sumi.auth.provider-pending.v1"),
      ).toBeNull(),
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

    expect(settingsMocks.linkWithPopup).not.toHaveBeenCalled();
    expect(sessionStorage.getItem("sumi.auth.provider-pending.v1")).toBeNull();
    expect(screen.queryByText(/再開できます/)).not.toBeInTheDocument();
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
    expect(settingsMocks.reauthenticateWithPopup).not.toHaveBeenCalled();
  });
});
