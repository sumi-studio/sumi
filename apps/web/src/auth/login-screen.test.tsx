// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { LoginScreen } from "./login-screen";

const loginMocks = vi.hoisted(() => ({
  useAuth: vi.fn(),
}));

vi.mock("./auth-context", () => ({
  useAuth: loginMocks.useAuth,
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("LoginScreen email-link callback", () => {
  it("requires an explicit choice before replacing an authenticated account", () => {
    const completeEmailLink = vi.fn();
    const rejectEmailLink = vi.fn();
    loginMocks.useAuth.mockReturnValue({
      authenticated: true,
      cancelIntentTransition: vi.fn(),
      completeEmailLink,
      confirmation: null,
      configured: true,
      confirmIntentTransition: vi.fn(),
      emailLinkCallbackPending: true,
      logout: vi.fn(),
      rejectEmailLink,
      sendEmailLink: vi.fn(),
      sessionState: "authenticated",
      signIn: vi.fn(),
    });

    render(<LoginScreen />);

    expect(completeEmailLink).not.toHaveBeenCalled();
    expect(
      screen.getByRole("heading", { name: "アカウントを切り替えますか？" }),
    ).toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", { name: "現在のアカウントを使い続ける" }),
    );
    expect(rejectEmailLink).toHaveBeenCalledTimes(1);
    expect(completeEmailLink).not.toHaveBeenCalled();
  });

  it("shows the Firebase account attached to a pending confirmation", () => {
    loginMocks.useAuth.mockReturnValue({
      authenticated: false,
      cancelIntentTransition: vi.fn(),
      completeEmailLink: vi.fn(),
      confirmation: {
        action: "create_account",
        account: { displayName: "New User", email: "new@example.com" },
        firebaseUID: "firebase-new",
      },
      configured: true,
      confirmIntentTransition: vi.fn(),
      emailLinkCallbackPending: false,
      logout: vi.fn(),
      rejectEmailLink: vi.fn(),
      sendEmailLink: vi.fn(),
      sessionState: "unauthenticated",
      signIn: vi.fn(),
    });

    render(<LoginScreen />);

    expect(
      screen.getByText("対象アカウント: new@example.com"),
    ).toBeInTheDocument();
  });
});
