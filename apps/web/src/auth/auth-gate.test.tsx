// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AuthGate } from "./auth-gate";

const gateMocks = vi.hoisted(() => ({
  useAuth: vi.fn(),
}));

vi.mock("./auth-context", () => ({
  useAuth: gateMocks.useAuth,
}));

vi.mock("./login-screen", () => ({
  LoginScreen: () => <div data-testid="login-screen">login</div>,
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("AuthGate email-link callback", () => {
  it("shows the callback UI before the authenticated direct-chat fast path", () => {
    gateMocks.useAuth.mockReturnValue({
      canUseDirectChat: true,
      dismissOutcomeNotice: vi.fn(),
      emailLinkCallbackPending: true,
      loading: false,
      outcomeNotice: null,
      sessionState: "authenticated",
      refreshSession: vi.fn(),
    });

    render(
      <AuthGate>
        <div data-testid="protected-chat">chat</div>
      </AuthGate>,
    );

    expect(screen.getByTestId("login-screen")).toBeInTheDocument();
    expect(screen.queryByTestId("protected-chat")).not.toBeInTheDocument();
  });
});
