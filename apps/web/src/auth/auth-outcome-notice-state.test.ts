// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  type AuthOutcomeScope,
  clearAuthOutcomeNotice,
  publishAuthOutcomeNotice,
  takeAuthOutcomeNotice,
} from "./auth-outcome-notice-state";

const scope: AuthOutcomeScope = {
  firebaseUID: "firebase-user-a",
  humanId: "human-a",
};
const noticeKey = "sumi.auth.outcome-notice.v1";
const receiptHistoryKey = "sumi.auth.outcome-receipts.v1";

beforeEach(() => {
  sessionStorage.clear();
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-01T00:00:00.000Z"));
});

afterEach(() => {
  vi.useRealTimers();
});

function publish(receiptId: string) {
  return publishAuthOutcomeNotice({
    scope,
    outcome: "signed_in",
    intent: "sign_in",
    receiptId,
  });
}

describe("auth outcome notice state", () => {
  it("claims immediate display before dismissal or reload can replay it", () => {
    expect(publish("terminal-one")).toMatchObject({
      outcome: "signed_in",
      receiptId: "terminal-one",
    });
    expect(sessionStorage.getItem(noticeKey)).toBeNull();

    // Dismissal clears only active display state. The claimed receipt remains
    // and a reload cannot recreate the same terminal notice.
    clearAuthOutcomeNotice();
    expect(takeAuthOutcomeNotice(scope)).toBeNull();
    expect(publish("terminal-one")).toBeNull();
  });

  it("keeps a bounded replay history through later notices and logout cleanup", () => {
    expect(publish("terminal-old-a")).not.toBeNull();
    expect(publish("terminal-old-b")).not.toBeNull();
    clearAuthOutcomeNotice();
    expect(publish("terminal-new")).not.toBeNull();

    expect(publish("terminal-old-a")).toBeNull();
    expect(publish("terminal-old-b")).toBeNull();
    expect(
      JSON.parse(sessionStorage.getItem(receiptHistoryKey) ?? "null").receipts,
    ).toHaveLength(3);
  });

  it("rejects malformed, stale, future, and cross-account state", () => {
    sessionStorage.setItem(
      noticeKey,
      JSON.stringify({
        version: 1,
        ...scope,
        outcome: "signed_in",
        intent: "sign_in",
        intentTransition: "none",
        receiptId: "terminal-malformed",
        createdAt: "2026-08-01T00:00:00.000Z",
        expiresAt: "2026-08-01T00:10:00.000Z",
        email: "private@example.com",
      }),
    );
    expect(takeAuthOutcomeNotice(scope)).toBeNull();

    sessionStorage.setItem(
      noticeKey,
      JSON.stringify({
        version: 1,
        ...scope,
        outcome: "signed_in",
        intent: "sign_in",
        intentTransition: "none",
        receiptId: "terminal-future",
        createdAt: "2026-08-01T00:02:00.000Z",
        expiresAt: "2026-08-01T00:12:00.000Z",
      }),
    );
    expect(takeAuthOutcomeNotice(scope)).toBeNull();

    expect(publish("terminal-other-user")).not.toBeNull();
    sessionStorage.setItem(
      noticeKey,
      JSON.stringify({
        version: 1,
        ...scope,
        outcome: "signed_in",
        intent: "sign_in",
        intentTransition: "none",
        receiptId: "terminal-cross-account",
        createdAt: "2026-08-01T00:00:00.000Z",
        expiresAt: "2026-08-01T00:10:00.000Z",
      }),
    );
    expect(
      takeAuthOutcomeNotice({
        firebaseUID: "firebase-user-b",
        humanId: "human-a",
      }),
    ).toBeNull();
  });

  it("stores only scoped outcome metadata and non-sensitive receipt IDs", () => {
    expect(
      publishAuthOutcomeNotice({
        scope,
        outcome: "provider_linked",
        intent: "sign_up",
        intentTransition: "recovery_proved",
        receiptId: "terminal-provider-link",
      }),
    ).toMatchObject({ outcome: "provider_linked" });
    const serialized = sessionStorage.getItem(receiptHistoryKey) ?? "";

    expect(Object.keys(JSON.parse(serialized))).toEqual([
      "version",
      "receipts",
    ]);
    expect(serialized).not.toMatch(/email|credential|token|profile|nonce/i);
  });
});
