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
import { ProviderSettings } from "./provider-settings";

const settingsMocks = vi.hoisted(() => ({
  currentUser: null as null | {
    uid: string;
    providerData: Array<{ providerId: string }>;
  },
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

beforeEach(() => {
  sessionStorage.clear();
  settingsMocks.currentUser = {
    uid: "firebase-user",
    providerData: [{ providerId: "password" }],
  };
  settingsMocks.getFirebaseAuth.mockImplementation(() => ({
    currentUser: settingsMocks.currentUser,
  }));
  settingsMocks.onAuthStateChanged.mockImplementation((_auth, observer) => {
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
  settingsMocks.completeProviderOperation.mockResolvedValue({
    operationId: "operation-1",
    outcome: "provider_linked",
    noticeRequired: true,
  });
  settingsMocks.failProviderOperation.mockResolvedValue(undefined);
  settingsMocks.statusProviderOperation.mockResolvedValue({
    operationId: "operation-1",
    outcome: "provider_linked",
    noticeRequired: true,
  });
  settingsMocks.linkWithPopup.mockImplementation(async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user",
      providerData: [{ providerId: "password" }, { providerId: "google.com" }],
    };
    return { user: settingsMocks.currentUser };
  });
  settingsMocks.reauthenticateWithPopup.mockResolvedValue({});
  settingsMocks.reload.mockResolvedValue(undefined);
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("provider settings", () => {
  it("shows linked methods, completes a provider link, and keeps a session notice", async () => {
    render(<ProviderSettings />);

    expect(screen.getByText("メールリンク")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Googleを追加" }));

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを追加しました。",
      );
    });
    expect(settingsMocks.linkWithPopup).toHaveBeenCalledTimes(1);
    expect(settingsMocks.completeProviderOperation).toHaveBeenCalledWith({
      operationId: "operation-1",
      nonce: "n".repeat(43),
      idToken: "id-token",
    });
    expect(sessionStorage.getItem("sumi.auth.provider-notice.v1")).toContain(
      '"operation":"linked"',
    );
  });

  it("requires an explicit alternate-provider reauth before backend unlink", async () => {
    settingsMocks.currentUser = {
      uid: "firebase-user",
      providerData: [
        { providerId: "google.com" },
        { providerId: "github.com" },
      ],
    };
    settingsMocks.startProviderOperation.mockResolvedValue({
      operationId: "operation-2",
      outcome: "provider_unlinked",
      noticeRequired: true,
    });
    settingsMocks.reload.mockImplementation(async () => {
      settingsMocks.currentUser = {
        uid: "firebase-user",
        providerData: [{ providerId: "github.com" }],
      };
    });
    render(<ProviderSettings />);

    fireEvent.click(screen.getByRole("button", { name: "Googleを解除" }));
    expect(
      screen.getByText(/別のリンク済み方法で再認証してから解除します/),
    ).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "再認証して解除" }));

    await waitFor(() => {
      expect(settingsMocks.reauthenticateWithPopup).toHaveBeenCalledTimes(1);
    });
    expect(settingsMocks.startProviderOperation).toHaveBeenCalledWith(
      expect.objectContaining({
        provider: "google.com",
        operation: "unlink",
        idToken: "id-token",
      }),
    );
    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(
        "Googleを解除しました。",
      );
    });
  });

  it("disables removal of the only Firebase login method", () => {
    settingsMocks.currentUser = {
      uid: "firebase-user",
      providerData: [{ providerId: "google.com" }],
    };
    render(<ProviderSettings />);

    expect(screen.getByRole("button", { name: "Googleを解除" })).toBeDisabled();
    expect(
      screen.getByText(
        "最後のログイン方法は解除できません。先に別の方法を追加してください。",
      ),
    ).toBeVisible();
    expect(settingsMocks.reauthenticateWithPopup).not.toHaveBeenCalled();
  });
});
