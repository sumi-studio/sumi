import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  completeProviderOperation,
  failProviderOperation,
  startProviderOperation,
  statusProviderOperation,
} from "./provider-operation-client";

const clientMocks = vi.hoisted(() => ({ postAuthJSON: vi.fn() }));

vi.mock("./session-client", () => ({
  postAuthJSON: clientMocks.postAuthJSON,
}));

beforeEach(() => {
  clientMocks.postAuthJSON.mockReset();
});

describe("provider operation client", () => {
  it("starts a provider link with the exact authenticated account-settings contract", async () => {
    clientMocks.postAuthJSON.mockResolvedValue({
      operation_id: "operation-1",
      outcome: "client_operation_required",
      client_operation: "firebase_link_with_credential",
      completion_token_not_before: "2026-08-01T09:00:01Z",
      expires_at: "2026-08-01T09:10:00Z",
    });

    await expect(
      startProviderOperation({
        provider: "google.com",
        operation: "link",
        nonce: "nonce",
        idToken: "initial-token",
      }),
    ).resolves.toMatchObject({
      operationId: "operation-1",
      outcome: "client_operation_required",
      clientOperation: "firebase_link_with_credential",
      noticeRequired: false,
    });
    expect(clientMocks.postAuthJSON).toHaveBeenCalledWith(
      "/auth/providers/operations",
      {
        provider: "google.com",
        operation: "link",
        decision_path: "account_settings",
        nonce: "nonce",
        id_token: "initial-token",
      },
    );
  });

  it("uses the fresh completion token and preserves the required notice", async () => {
    clientMocks.postAuthJSON.mockResolvedValue({
      operation_id: "operation-1",
      outcome: "provider_linked",
      notice_required: true,
    });

    await expect(
      completeProviderOperation({
        operationId: "operation-1",
        nonce: "nonce",
        idToken: "fresh-token",
      }),
    ).resolves.toEqual({
      operationId: "operation-1",
      outcome: "provider_linked",
      noticeRequired: true,
    });
    expect(clientMocks.postAuthJSON).toHaveBeenCalledWith(
      "/auth/providers/operations/complete",
      {
        operation_id: "operation-1",
        nonce: "nonce",
        id_token: "fresh-token",
      },
    );
  });

  it("marks collision recovery as a distinct audited decision path", async () => {
    clientMocks.postAuthJSON.mockResolvedValue({
      operation_id: "operation-recovery",
      outcome: "client_operation_required",
      client_operation: "firebase_link_with_credential",
      completion_token_not_before: "2026-08-01T09:00:01Z",
      expires_at: "2026-08-01T09:10:00Z",
    });

    await startProviderOperation({
      provider: "github.com",
      operation: "link",
      decisionPath: "same_email_recovery",
      nonce: "recovery-nonce",
      idToken: "email-proof-token",
    });

    expect(clientMocks.postAuthJSON).toHaveBeenCalledWith(
      "/auth/providers/operations",
      {
        provider: "github.com",
        operation: "link",
        decision_path: "same_email_recovery",
        nonce: "recovery-nonce",
        id_token: "email-proof-token",
      },
    );
  });

  it("supports bounded failure and status recovery routes", async () => {
    clientMocks.postAuthJSON
      .mockResolvedValueOnce({
        operation_id: "operation-1",
        outcome: "cancelled",
      })
      .mockResolvedValueOnce({
        operation_id: "operation-1",
        provider: "google.com",
        operation: "link",
        status: "completed",
        outcome: "provider_linked",
        notice_required: true,
      });

    await failProviderOperation({
      operationId: "operation-1",
      nonce: "nonce",
      outcome: "cancelled",
    });
    await expect(
      statusProviderOperation({ operationId: "operation-1", nonce: "nonce" }),
    ).resolves.toMatchObject({
      provider: "google.com",
      operation: "link",
      status: "completed",
      outcome: "provider_linked",
    });
    expect(clientMocks.postAuthJSON.mock.calls).toEqual([
      [
        "/auth/providers/operations/fail",
        {
          operation_id: "operation-1",
          nonce: "nonce",
          outcome: "cancelled",
        },
      ],
      [
        "/auth/providers/operations/status",
        { operation_id: "operation-1", nonce: "nonce" },
      ],
    ]);
  });

  it("rejects unknown semantic outcomes instead of guessing", async () => {
    clientMocks.postAuthJSON.mockResolvedValue({
      operation_id: "operation-1",
      outcome: "silently_linked",
    });

    await expect(
      statusProviderOperation({ operationId: "operation-1", nonce: "nonce" }),
    ).rejects.toThrow("Invalid provider operation response.");
  });
});
