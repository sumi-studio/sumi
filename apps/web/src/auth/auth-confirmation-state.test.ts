// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { savePendingConfirmation } from "./auth-confirmation-state";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("pending auth confirmation storage", () => {
  it("keeps the in-memory flow usable when session storage rejects writes", () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("storage disabled", "SecurityError");
    });

    expect(() =>
      savePendingConfirmation({
        flowId: "flow-id",
        nonce: "n".repeat(43),
        intent: "sign_in",
        provider: "google.com",
        expiresAt: "2026-08-01T01:00:00Z",
        action: "create_account",
        firebaseUID: "firebase-user",
        account: { displayName: null, email: "human@example.com" },
      }),
    ).not.toThrow();
  });
});
