// @vitest-environment jsdom

import { FirebaseError } from "firebase/app";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  beginSameEmailCredentialRecovery,
  completeSameEmailCredentialRecovery,
} from "./credential-recovery";

const recoveryMocks = vi.hoisted(() => ({
  auth: {
    currentUser: null as null | { uid: string },
  },
  credentialFromError: vi.fn(),
  credentialFromJSON: vi.fn(),
  getIdToken: vi.fn(),
  linkWithCredential: vi.fn(),
  beginEmailLinkAuth: vi.fn(),
  createAuthFlowNonce: vi.fn(() => "n".repeat(43)),
  startProviderOperation: vi.fn(),
  completeProviderOperation: vi.fn(),
  failProviderOperation: vi.fn(),
  statusProviderOperation: vi.fn(),
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: {
    credentialFromError(error: unknown) {
      return recoveryMocks.credentialFromError(error);
    },
  },
  GoogleAuthProvider: {
    credentialFromError(error: unknown) {
      return recoveryMocks.credentialFromError(error);
    },
  },
  OAuthCredential: {
    fromJSON(value: unknown) {
      return recoveryMocks.credentialFromJSON(value);
    },
  },
  getIdToken: recoveryMocks.getIdToken,
  linkWithCredential: recoveryMocks.linkWithCredential,
}));

vi.mock("./email-link-auth", () => ({
  beginEmailLinkAuth: recoveryMocks.beginEmailLinkAuth,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: () => recoveryMocks.auth,
}));

vi.mock("./auth-flow-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./auth-flow-client")>()),
  createAuthFlowNonce: recoveryMocks.createAuthFlowNonce,
}));

vi.mock("./provider-operation-client", () => ({
  startProviderOperation: recoveryMocks.startProviderOperation,
  completeProviderOperation: recoveryMocks.completeProviderOperation,
  failProviderOperation: recoveryMocks.failProviderOperation,
  statusProviderOperation: recoveryMocks.statusProviderOperation,
}));

beforeEach(() => {
  vi.resetAllMocks();
  recoveryMocks.auth.currentUser = { uid: "firebase-existing" };
  recoveryMocks.createAuthFlowNonce.mockReturnValue("n".repeat(43));
  recoveryMocks.beginEmailLinkAuth.mockResolvedValue(undefined);
  recoveryMocks.getIdToken
    .mockResolvedValueOnce("email-proof-token")
    .mockResolvedValueOnce("fresh-linked-token");
  recoveryMocks.startProviderOperation.mockResolvedValue({
    operationId: "provider-operation",
    outcome: "client_operation_required",
    clientOperation: "firebase_link_with_credential",
    completionTokenNotBefore: new Date(Date.now() - 1_000).toISOString(),
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
    noticeRequired: false,
  });
  recoveryMocks.completeProviderOperation.mockResolvedValue({
    operationId: "provider-operation",
    outcome: "provider_linked",
    noticeRequired: true,
  });
  recoveryMocks.failProviderOperation.mockResolvedValue(undefined);
});

describe("same-email Firebase credential recovery", () => {
  it("retains only the bounded OAuth credential fields and starts email proof", async () => {
    const error = new FirebaseError(
      "auth/account-exists-with-different-credential",
      "collision",
      { email: "existing@example.com", displayName: "must not persist" },
    );
    recoveryMocks.credentialFromError.mockReturnValue({
      providerId: "github.com",
      signInMethod: "github.com",
      toJSON: () => ({
        providerId: "github.com",
        signInMethod: "github.com",
        pendingToken: "pending-oauth-token",
        email: "must-not-be-copied@example.com",
      }),
    });

    await beginSameEmailCredentialRecovery(error, "github.com", "sign_up");

    expect(recoveryMocks.beginEmailLinkAuth).toHaveBeenCalledWith(
      "existing@example.com",
      "sign_in",
      {
        provider: "github.com",
        requestedIntent: "sign_up",
        credential: {
          providerId: "github.com",
          signInMethod: "github.com",
          pendingToken: "pending-oauth-token",
        },
      },
    );
  });

  it("starts same_email_recovery and completes it with the linked user's fresh proof", async () => {
    const user = { uid: "firebase-existing" };
    const credential = {
      providerId: "github.com",
      signInMethod: "github.com",
    };
    recoveryMocks.credentialFromJSON.mockReturnValue(credential);
    recoveryMocks.linkWithCredential.mockResolvedValue({ user });

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: recovery("github.com"),
        user: user as never,
      }),
    ).resolves.toBe("provider_linked");

    expect(recoveryMocks.startProviderOperation).toHaveBeenCalledWith({
      provider: "github.com",
      operation: "link",
      decisionPath: "same_email_recovery",
      nonce: "n".repeat(43),
      idToken: "email-proof-token",
    });
    expect(recoveryMocks.linkWithCredential).toHaveBeenCalledWith(
      user,
      credential,
    );
    expect(recoveryMocks.completeProviderOperation).toHaveBeenCalledWith({
      operationId: "provider-operation",
      nonce: "n".repeat(43),
      idToken: "fresh-linked-token",
    });
  });

  it("terminalizes a credential already linked to another Firebase user", async () => {
    const user = { uid: "firebase-existing" };
    recoveryMocks.credentialFromJSON.mockReturnValue({
      providerId: "google.com",
      signInMethod: "google.com",
    });
    recoveryMocks.linkWithCredential.mockRejectedValue(
      new FirebaseError(
        "auth/credential-already-in-use",
        "credential already belongs elsewhere",
      ),
    );

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: recovery("google.com"),
        user: user as never,
      }),
    ).rejects.toMatchObject({ code: "auth/credential-already-in-use" });
    expect(recoveryMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "provider-operation",
      nonce: "n".repeat(43),
      outcome: "credential_in_use",
    });
    expect(recoveryMocks.completeProviderOperation).not.toHaveBeenCalled();
  });

  it("fails the operation if Firebase changed users before the link mutation", async () => {
    const user = { uid: "firebase-existing" };
    recoveryMocks.credentialFromJSON.mockReturnValue({
      providerId: "github.com",
      signInMethod: "github.com",
    });
    recoveryMocks.startProviderOperation.mockImplementationOnce(async () => {
      recoveryMocks.auth.currentUser = { uid: "different-firebase-user" };
      return {
        operationId: "provider-operation",
        outcome: "client_operation_required",
        clientOperation: "firebase_link_with_credential",
        completionTokenNotBefore: new Date(Date.now() - 1_000).toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
        noticeRequired: false,
      };
    });

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: recovery("github.com"),
        user: user as never,
      }),
    ).rejects.toThrow("account changed");
    expect(recoveryMocks.linkWithCredential).not.toHaveBeenCalled();
    expect(recoveryMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "provider-operation",
      nonce: "n".repeat(43),
      outcome: "firebase_operation_failed",
    });
  });

  it("terminalizes without completion when Firebase changes users during link", async () => {
    const user = { uid: "firebase-existing" };
    recoveryMocks.credentialFromJSON.mockReturnValue({
      providerId: "github.com",
      signInMethod: "github.com",
    });
    recoveryMocks.linkWithCredential.mockImplementationOnce(async () => {
      recoveryMocks.auth.currentUser = { uid: "different-firebase-user" };
      return { user };
    });

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: recovery("github.com"),
        user: user as never,
      }),
    ).rejects.toThrow("account changed during");
    expect(recoveryMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "provider-operation",
      nonce: "n".repeat(43),
      outcome: "firebase_operation_failed",
    });
    expect(recoveryMocks.completeProviderOperation).not.toHaveBeenCalled();
  });

  it("rechecks the Firebase user after obtaining the fresh completion token", async () => {
    const user = { uid: "firebase-existing" };
    recoveryMocks.credentialFromJSON.mockReturnValue({
      providerId: "github.com",
      signInMethod: "github.com",
    });
    recoveryMocks.linkWithCredential.mockResolvedValue({ user });
    recoveryMocks.getIdToken
      .mockReset()
      .mockResolvedValueOnce("email-proof-token")
      .mockImplementationOnce(async () => {
        recoveryMocks.auth.currentUser = { uid: "different-firebase-user" };
        return "fresh-linked-token";
      });

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: recovery("github.com"),
        user: user as never,
      }),
    ).rejects.toThrow("account changed during");
    expect(recoveryMocks.failProviderOperation).toHaveBeenCalledWith({
      operationId: "provider-operation",
      nonce: "n".repeat(43),
      outcome: "firebase_operation_failed",
    });
    expect(recoveryMocks.completeProviderOperation).not.toHaveBeenCalled();
  });

  it("rejects a reconstructed credential for a different provider", async () => {
    recoveryMocks.credentialFromJSON.mockReturnValue({
      providerId: "google.com",
      signInMethod: "google.com",
    });

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: recovery("github.com"),
        user: { uid: "firebase-existing" } as never,
      }),
    ).rejects.toThrow("credential is invalid");
    expect(recoveryMocks.startProviderOperation).not.toHaveBeenCalled();
    expect(recoveryMocks.linkWithCredential).not.toHaveBeenCalled();
  });

  it("rejects an expired recovery before starting any backend mutation", async () => {
    const expired = recovery("github.com");
    expired.expiresAt = new Date(Date.now() - 1).toISOString();

    await expect(
      completeSameEmailCredentialRecovery({
        recovery: expired,
        user: { uid: "firebase-existing" } as never,
      }),
    ).rejects.toThrow("expired");
    expect(recoveryMocks.startProviderOperation).not.toHaveBeenCalled();
    expect(recoveryMocks.linkWithCredential).not.toHaveBeenCalled();
  });
});

function recovery(provider: "google.com" | "github.com") {
  return {
    version: 1 as const,
    provider,
    requestedIntent: "sign_in" as const,
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
    credential: {
      providerId: provider,
      signInMethod: provider,
      pendingToken: "pending-oauth-token",
    },
  };
}
