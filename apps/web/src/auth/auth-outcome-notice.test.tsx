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
  authOutcomeNoticeCopy,
  authOutcomeNoticeExitMilliseconds,
  authOutcomeNoticeReadingMilliseconds,
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

const shortNotice: AuthOutcomeNoticeState = {
  ...baseNotice,
  outcome: "signed_in",
  intent: "sign_in",
  intentTransition: "none",
};

const longNotice: AuthOutcomeNoticeState = {
  ...baseNotice,
  receiptId: "terminal-recovery",
  outcome: "provider_linked",
  intent: "sign_up",
  intentTransition: "recovery_proved",
};

let hasFocus: ReturnType<typeof vi.spyOn>;

function setVisibility(state: DocumentVisibilityState) {
  act(() => {
    Object.defineProperty(document, "visibilityState", {
      configurable: true,
      value: state,
    });
    document.dispatchEvent(new Event("visibilitychange"));
  });
}

function firePointer(
  target: Node | Window,
  type:
    | "pointerdown"
    | "pointerenter"
    | "pointerleave"
    | "pointerup"
    | "pointercancel"
    | "lostpointercapture",
  values: {
    pointerId?: number;
    pointerType?: string;
    clientY?: number;
    timeStamp?: number;
  } = {},
) {
  const event = new Event(type, { bubbles: true });
  const defaults = {
    pointerId: 1,
    pointerType: "touch",
    clientY: 200,
    timeStamp: 10,
  };
  for (const [name, value] of Object.entries({ ...defaults, ...values })) {
    Object.defineProperty(event, name, { value });
  }
  fireEvent(target, event);
}

async function advance(milliseconds: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(milliseconds);
  });
}

function getNoticeSurface() {
  return screen.getByTestId("auth-outcome-notice");
}

beforeEach(() => {
  vi.useFakeTimers();
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: "visible",
  });
  hasFocus = vi.spyOn(document, "hasFocus").mockReturnValue(true);
});

afterEach(() => {
  cleanup();
  hasFocus.mockRestore();
  vi.useRealTimers();
});

describe("AuthOutcomeNotice", () => {
  it("uses distinct copy for each terminal receipt outcome", () => {
    expect(
      authOutcomeNoticeCopy({
        ...baseNotice,
        outcome: "account_created",
        intent: "sign_up",
        intentTransition: "none",
      }),
    ).toBe("Sumiアカウントを作成しました。");
    expect(authOutcomeNoticeCopy(shortNotice)).toBe("Sumiにログインしました。");
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
    render(
      <AuthOutcomeNotice
        notice={{
          ...baseNotice,
          outcome: "account_created",
          intent: "sign_in",
          intentTransition: "confirmed",
        }}
        onDismiss={() => undefined}
      />,
    );

    expect(
      screen.getByText(
        "ログインから新規登録への変更を確認し、Sumiアカウントを作成しました。",
      ),
    ).toBeInTheDocument();
  });

  it("keeps long terminal transition copy readable longer than short copy", () => {
    const shortDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );
    const longDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(longNotice),
    );

    expect(shortDuration).toBeGreaterThanOrEqual(6_000);
    expect(longDuration).toBeGreaterThan(shortDuration);
  });

  it("dismisses only after visible reading time and the terminal transition", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );

    await advance(readingDuration - 1);
    expect(getNoticeSurface()).not.toHaveAttribute("data-exiting");
    expect(onDismiss).not.toHaveBeenCalled();

    await advance(1);
    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
    expect(onDismiss).not.toHaveBeenCalled();

    await advance(authOutcomeNoticeExitMilliseconds - 1);
    expect(onDismiss).not.toHaveBeenCalled();
    await advance(1);
    expect(onDismiss).toHaveBeenCalledOnce();
  });

  it("does not spend reading time while hidden and resets it when visible", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );

    await advance(readingDuration - 100);
    setVisibility("hidden");
    await advance(readingDuration * 2);
    expect(onDismiss).not.toHaveBeenCalled();

    setVisibility("visible");
    await advance(readingDuration - 1);
    expect(getNoticeSurface()).not.toHaveAttribute("data-exiting");
    await advance(1);
    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
  });

  it("starts a fresh readable interval only after the window regains focus", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );

    await advance(readingDuration - 100);
    hasFocus.mockReturnValue(false);
    fireEvent.blur(window);
    await advance(readingDuration * 2);
    expect(onDismiss).not.toHaveBeenCalled();

    hasFocus.mockReturnValue(true);
    fireEvent.focus(window);
    await advance(readingDuration - 1);
    expect(getNoticeSurface()).not.toHaveAttribute("data-exiting");
    await advance(1);
    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
  });

  it("pauses while its close action has keyboard focus", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );
    const close = screen.getByRole("button", { name: "通知を閉じる" });

    close.focus();
    await advance(readingDuration * 2);
    expect(getNoticeSurface()).not.toHaveAttribute("data-exiting");

    close.blur();
    await advance(readingDuration);
    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
  });

  it("preserves hover through blur so refocus cannot dismiss under the reader", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );
    const notice = getNoticeSurface();

    firePointer(notice, "pointerenter", { pointerType: "mouse" });
    hasFocus.mockReturnValue(false);
    fireEvent.blur(window);
    await advance(readingDuration * 2);
    expect(notice).not.toHaveAttribute("data-exiting");

    hasFocus.mockReturnValue(true);
    fireEvent.focus(window);
    await advance(readingDuration * 2);
    expect(notice).not.toHaveAttribute("data-exiting");

    firePointer(notice, "pointerleave", { pointerType: "mouse" });
    await advance(readingDuration);
    expect(notice).toHaveAttribute("data-exiting", "true");
  });

  it("releases an interaction that starts on the close control and ends outside", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );

    firePointer(
      screen.getByRole("button", { name: "通知を閉じる" }),
      "pointerdown",
    );
    firePointer(window, "pointerup", { clientY: 400, timeStamp: 100 });
    await advance(readingDuration);

    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
  });

  it.each([
    "pointercancel",
    "lostpointercapture",
  ] as const)("cleans up a drag after %s and restarts its timer", async (terminalEvent) => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);
    const readingDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );
    const notice = getNoticeSurface();

    firePointer(notice, "pointerdown");
    firePointer(
      terminalEvent === "pointercancel" ? window : notice,
      terminalEvent,
      { clientY: 160, timeStamp: 50 },
    );
    expect(notice).not.toHaveAttribute("data-exiting");
    await advance(readingDuration);
    expect(notice).toHaveAttribute("data-exiting", "true");
  });

  it("keeps a keyboard-accessible manual close and waits for its quiet exit", async () => {
    const onDismiss = vi.fn();
    render(<AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />);

    const close = screen.getByRole("button", { name: "通知を閉じる" });
    close.focus();
    fireEvent.click(close);

    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
    expect(onDismiss).not.toHaveBeenCalled();
    await advance(authOutcomeNoticeExitMilliseconds);
    expect(onDismiss).toHaveBeenCalledOnce();
  });

  it("gives a replacement receipt its own full reading interval", async () => {
    const onDismiss = vi.fn();
    const view = render(
      <AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />,
    );
    const shortDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(shortNotice),
    );
    const longDuration = authOutcomeNoticeReadingMilliseconds(
      authOutcomeNoticeCopy(longNotice),
    );

    await advance(shortDuration - 1);
    view.rerender(
      <AuthOutcomeNotice notice={longNotice} onDismiss={onDismiss} />,
    );
    expect(screen.getByText(authOutcomeNoticeCopy(longNotice))).toBeVisible();

    await advance(longDuration - 1);
    expect(onDismiss).not.toHaveBeenCalled();
    expect(getNoticeSurface()).not.toHaveAttribute("data-exiting");
    await advance(1);
    expect(getNoticeSurface()).toHaveAttribute("data-exiting", "true");
  });

  it("cleans every timer and listener on unmount", async () => {
    const onDismiss = vi.fn();
    const view = render(
      <AuthOutcomeNotice notice={shortNotice} onDismiss={onDismiss} />,
    );
    view.unmount();

    await advance(
      authOutcomeNoticeReadingMilliseconds(authOutcomeNoticeCopy(shortNotice)) +
        authOutcomeNoticeExitMilliseconds,
    );
    firePointer(window, "pointerup");
    fireEvent.focus(window);
    expect(onDismiss).not.toHaveBeenCalled();
  });
});
