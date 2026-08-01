// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import {
  AuthOutcomeNotice,
  authOutcomeNoticeCopy,
} from "./auth-outcome-notice";
import type { AuthOutcomeNotice as AuthOutcomeNoticeState } from "./auth-outcome-notice-state";

const baseNotice = {
  version: 1 as const,
  firebaseUID: "firebase-user",
  humanId: "human-user",
  receiptId: "terminal-receipt",
  createdAt: "2026-08-01T00:00:00.000Z",
  expiresAt: "2026-08-01T00:10:00.000Z",
};

afterEach(cleanup);

describe("AuthOutcomeNotice", () => {
  it("uses distinct visible copy for every terminal auth outcome", () => {
    expect(
      authOutcomeNoticeCopy({
        ...baseNotice,
        outcome: "account_created",
        intent: "sign_up",
        intentTransition: "none",
      }),
    ).toBe("Sumiアカウントを作成しました。");
    expect(
      authOutcomeNoticeCopy({
        ...baseNotice,
        outcome: "signed_in",
        intent: "sign_in",
        intentTransition: "none",
      }),
    ).toBe("Sumiにログインしました。");
    expect(
      authOutcomeNoticeCopy({
        ...baseNotice,
        outcome: "provider_linked",
        intent: "sign_in",
        intentTransition: "none",
      }),
    ).toBe("ログイン後、選択したログイン方法を追加しました。");
  });

  it("states an intent transition only when it was explicitly confirmed", () => {
    const notice: AuthOutcomeNoticeState = {
      ...baseNotice,
      outcome: "account_created",
      intent: "sign_in",
      intentTransition: "confirmed",
    };

    render(<AuthOutcomeNotice notice={notice} onDismiss={() => undefined} />);

    expect(
      screen.getByText(
        "ログインから新規登録への変更を確認し、Sumiアカウントを作成しました。",
      ),
    ).toBeInTheDocument();
  });

  it("explains proven existing-account recovery without implying a new account", () => {
    expect(
      authOutcomeNoticeCopy({
        ...baseNotice,
        outcome: "provider_linked",
        intent: "sign_up",
        intentTransition: "recovery_proved",
      }),
    ).toBe(
      "新規登録を開始後、既存のSumiアカウントをメールで確認してログインし、選択したログイン方法を追加しました。",
    );
  });
});
