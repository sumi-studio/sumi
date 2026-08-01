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

  it("keeps the switch handler as the sole callback owner across logout", async () => {
    let finishLogout!: () => void;
    const logout = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finishLogout = resolve;
        }),
    );
    const completeEmailLink = vi.fn().mockResolvedValue(undefined);
    const authState = {
      authenticated: true,
      cancelIntentTransition: vi.fn(),
      completeEmailLink,
      confirmation: null,
      configured: true,
      confirmIntentTransition: vi.fn(),
      emailLinkCallbackPending: true,
      logout,
      rejectEmailLink: vi.fn(),
      sendEmailLink: vi.fn(),
      sessionState: "authenticated",
      signIn: vi.fn(),
    };
    loginMocks.useAuth.mockImplementation(() => authState);
    const { rerender } = render(<LoginScreen />);

    fireEvent.click(
      screen.getByRole("button", {
        name: "現在のセッションを終了して切り替える",
      }),
    );
    authState.authenticated = false;
    authState.sessionState = "unauthenticated";
    rerender(<LoginScreen />);

    expect(completeEmailLink).not.toHaveBeenCalled();
    await act(async () => finishLogout());
    await waitFor(() => expect(completeEmailLink).toHaveBeenCalledTimes(1));
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
