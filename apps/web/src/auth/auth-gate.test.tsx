// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
} from "../messaging/store";
import {
  bindWorkspaceSessionIdentity,
  getWorkspaceSessionIdentity,
  getWorkspaceSessionScopeKey,
} from "../workspace/store";
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
  bindWorkspaceSessionIdentity(null, null);
  bindMessagingSessionIdentity(null);
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

describe("AuthGate Workspace authority binding", () => {
  it("rebinds Workspace scope when the same Human receives a new opaque authority binding", () => {
    bindWorkspaceSessionIdentity("human-1", "binding-a");
    bindMessagingSessionIdentity("human-1");
    gateMocks.useAuth.mockReturnValue({
      authorityBindingId: "binding-b",
      canUseDirectChat: true,
      emailLinkCallbackPending: false,
      loading: false,
      sessionState: "authenticated",
      refreshSession: vi.fn(),
      user: { id: "human-1" },
    });

    render(
      <AuthGate>
        <div data-testid="protected-chat">chat</div>
      </AuthGate>,
    );

    expect(screen.getByTestId("protected-chat")).toBeInTheDocument();
    expect(getMessagingSessionIdentity()).toBe("human-1");
    expect(getWorkspaceSessionIdentity()).toBe("human-1");
    expect(getWorkspaceSessionScopeKey()).toBe("binding-b");
  });
});
