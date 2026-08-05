// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  AuthOutcomeNotice,
  authOutcomeNoticeAutoDismissMilliseconds,
  authOutcomeNoticeCopy,
  authOutcomeNoticeExitMilliseconds,
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

function firePointer(
  target: Element,
  type: "down" | "move" | "up",
  values: { pointerId: number; clientY: number; timeStamp?: number },
) {
  const event = new Event(`pointer${type}`, { bubbles: true });
  for (const [name, value] of Object.entries(values)) {
    Object.defineProperty(event, name, { value });
  }
  fireEvent(target, event);
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

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

  it("starts its upward exit after three seconds and dismisses after it completes", async () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        authOutcomeNoticeAutoDismissMilliseconds - 1,
      );
    });
    expect(onDismiss).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(screen.getByRole("status")).toHaveAttribute("data-exiting", "true");
    expect(onDismiss).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(authOutcomeNoticeExitMilliseconds - 1);
    });
    expect(onDismiss).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(onDismiss).toHaveBeenCalledOnce();
  });

  it("restarts the full three-second timer after a hover ends", async () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    const notice = screen.getByRole("status");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_900);
    });
    fireEvent.pointerEnter(notice, { pointerType: "mouse" });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(onDismiss).not.toHaveBeenCalled();

    fireEvent.pointerLeave(notice, { pointerType: "mouse" });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        authOutcomeNoticeAutoDismissMilliseconds - 1,
      );
    });
    expect(onDismiss).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(notice).toHaveAttribute("data-exiting", "true");
  });

  it("resets the timer for pointer and click interactions", async () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    const notice = screen.getByRole("status");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_900);
    });
    fireEvent.pointerDown(notice, { pointerId: 1, clientY: 200 });
    fireEvent.pointerUp(notice, { pointerId: 1, clientY: 200 });
    fireEvent.click(notice);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(
        authOutcomeNoticeAutoDismissMilliseconds - 1,
      );
    });
    expect(onDismiss).not.toHaveBeenCalled();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(notice).toHaveAttribute("data-exiting", "true");
  });

  it("only follows an upward drag and dismisses after a sufficient pull", async () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    const notice = screen.getByRole("status");
    firePointer(notice, "down", { pointerId: 1, clientY: 200, timeStamp: 0 });
    firePointer(notice, "move", { pointerId: 1, clientY: 260, timeStamp: 20 });
    expect(notice).toHaveStyle({ "--auth-outcome-notice-drag-y": "0px" });
    firePointer(notice, "move", { pointerId: 1, clientY: 110, timeStamp: 50 });
    expect(notice).toHaveStyle({ "--auth-outcome-notice-drag-y": "-90px" });
    firePointer(notice, "up", { pointerId: 1, clientY: 110, timeStamp: 50 });

    expect(notice).toHaveAttribute("data-exiting", "true");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(authOutcomeNoticeExitMilliseconds);
    });
    expect(onDismiss).toHaveBeenCalledOnce();
  });

  it("returns from a short upward drag instead of dismissing", () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    const notice = screen.getByRole("status");
    firePointer(notice, "down", { pointerId: 1, clientY: 200, timeStamp: 0 });
    firePointer(notice, "move", { pointerId: 1, clientY: 180, timeStamp: 50 });
    firePointer(notice, "up", { pointerId: 1, clientY: 180, timeStamp: 50 });

    expect(notice).not.toHaveAttribute("data-exiting");
    expect(notice).toHaveStyle({ "--auth-outcome-notice-drag-y": "0px" });
    expect(onDismiss).not.toHaveBeenCalled();
  });

  it("also dismisses a short but fast upward flick", () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    const notice = screen.getByRole("status");
    firePointer(notice, "down", { pointerId: 1, clientY: 200, timeStamp: 10 });
    firePointer(notice, "up", { pointerId: 1, clientY: 170, timeStamp: 30 });

    expect(notice).toHaveAttribute("data-exiting", "true");
  });

  it("keeps a manual close button and lets its exit complete", async () => {
    const onDismiss = vi.fn();
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "signed_in",
          intent: "sign_in",
          intentTransition: "none",
        }}
        onDismiss={onDismiss}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "通知を閉じる" }));

    expect(onDismiss).not.toHaveBeenCalled();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(authOutcomeNoticeExitMilliseconds);
    });
    expect(onDismiss).toHaveBeenCalledOnce();
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
