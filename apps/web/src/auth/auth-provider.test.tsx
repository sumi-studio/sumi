// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AuthProvider, useAuth } from "./auth-context";
import { AuthAPIError, SumiSessionCompensatedError } from "./session-client";

const authorityBindingA = "A".repeat(43);
const authorityBindingB = `${"B".repeat(42)}E`;

const authMocks = vi.hoisted(() => ({
  getSumiSession: vi.fn(),
  logoutSumiSession: vi.fn(),
  establishSumiSession: vi.fn(),
  getFirebaseAuth: vi.fn(),
  signOut: vi.fn(),
  signInWithPopup: vi.fn(),
  getIdToken: vi.fn(),
  bindDirectChatAuthority: vi.fn(),
  clearDirectChatAuthority: vi.fn(),
}));

vi.mock("./session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-client")>()),
  getSumiSession: authMocks.getSumiSession,
  logoutSumiSession: authMocks.logoutSumiSession,
  establishSumiSession: authMocks.establishSumiSession,
}));

vi.mock("./firebase", () => ({
  getFirebaseAuth: authMocks.getFirebaseAuth,
}));

vi.mock("../agent/auth-authority", () => ({
  bindDirectChatAuthority: authMocks.bindDirectChatAuthority,
  clearDirectChatAuthority: authMocks.clearDirectChatAuthority,
}));

vi.mock("firebase/auth", () => ({
  GithubAuthProvider: class {},
  GoogleAuthProvider: class {
    setCustomParameters() {}
  },
  getIdToken: authMocks.getIdToken,
  signInWithPopup: authMocks.signInWithPopup,
  signOut: authMocks.signOut,
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function AuthStateProbe() {
  const auth = useAuth();
  return (
    <>
      <div data-testid="session-state">{auth.sessionState}</div>
      <div data-testid="user-id">{auth.user?.id ?? "none"}</div>
      <button
        type="button"
        onClick={() => void auth.logout().catch(() => undefined)}
      >
        logout
      </button>
      <button
        type="button"
        onClick={() => void auth.signIn("google").catch(() => undefined)}
      >
        sign in
      </button>
    </>
  );
}

describe("logout authority transition", () => {
  it("keeps the UI unauthenticated when Firebase cleanup setup throws synchronously", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-1" },
    });
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    authMocks.getFirebaseAuth.mockImplementation(() => {
      throw new Error("emulator setup failed");
    });

    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.logoutSumiSession).toHaveBeenCalledTimes(1);
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(authMocks.getFirebaseAuth).toHaveBeenCalledTimes(1);
  });

  it("preserves the current conversation authority when Sumi logout fails", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.logoutSumiSession.mockRejectedValue(new Error("logout failed"));
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "logout" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    expect(authMocks.clearDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("binds the new identity before publishing a successful sign-in", async () => {
    const firebaseAuth = {};
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue(firebaseAuth);
    authMocks.signInWithPopup.mockResolvedValue({
      user: { uid: "firebase-b" },
    });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.establishSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.bindDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));

    await waitFor(() => {
      expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
        authorityBindingB,
      );
    });
    expect(authMocks.establishSumiSession).toHaveBeenCalledWith("id-token-b");
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "authenticated",
    );
  });

  it("clears old client authority after a committed exchange is compensated", async () => {
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockResolvedValue({
      user: { uid: "firebase-b" },
    });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.establishSumiSession.mockRejectedValue(
      new SumiSessionCompensatedError(
        new AuthAPIError("status unavailable", 503),
      ),
    );
    authMocks.signOut.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
  });

  it("does not establish a stale Firebase success after logout takes the generation", async () => {
    let resolvePopup!: (value: { user: { uid: string } }) => void;
    const popup = new Promise<{ user: { uid: string } }>((resolve) => {
      resolvePopup = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockReturnValue(popup);
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    authMocks.signOut.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    resolvePopup({ user: { uid: "firebase-b" } });
    await waitFor(() => {
      expect(authMocks.signOut).toHaveBeenCalled();
    });

    expect(authMocks.establishSumiSession).not.toHaveBeenCalled();
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
  });

  it("restores the exchanged identity when a generation-racing logout fails", async () => {
    let resolveEstablishment!: (value: {
      authenticated: true;
      authorityBindingId: string;
      user: { id: string };
    }) => void;
    const establishment = new Promise<{
      authenticated: true;
      authorityBindingId: string;
      user: { id: string };
    }>((resolve) => {
      resolveEstablishment = resolve;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockResolvedValue({
      user: { uid: "firebase-b" },
    });
    authMocks.getIdToken.mockResolvedValue("id-token-b");
    authMocks.establishSumiSession.mockReturnValue(establishment);
    authMocks.logoutSumiSession.mockRejectedValue(new Error("logout failed"));
    authMocks.signOut.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("user-id")).toHaveTextContent("user-a");
    });
    authMocks.bindDirectChatAuthority.mockClear();
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    await waitFor(() => {
      expect(authMocks.establishSumiSession).toHaveBeenCalled();
    });
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    resolveEstablishment({
      authenticated: true,
      authorityBindingId: authorityBindingB,
      user: { id: "user-b" },
    });

    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
      expect(screen.getByTestId("user-id")).toHaveTextContent("user-b");
    });
    expect(authMocks.bindDirectChatAuthority).toHaveBeenCalledWith(
      authorityBindingB,
    );
    expect(authMocks.clearDirectChatAuthority).not.toHaveBeenCalled();
  });

  it("does not restore old authority when a stale Firebase popup fails after logout", async () => {
    let rejectPopup!: (error: Error) => void;
    const popup = new Promise<never>((_, reject) => {
      rejectPopup = reject;
    });
    authMocks.getSumiSession.mockResolvedValue({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-a" },
    });
    authMocks.getFirebaseAuth.mockReturnValue({});
    authMocks.signInWithPopup.mockReturnValue(popup);
    authMocks.logoutSumiSession.mockResolvedValue(undefined);
    render(
      <AuthProvider>
        <AuthStateProbe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "authenticated",
      );
    });
    authMocks.clearDirectChatAuthority.mockClear();

    fireEvent.click(screen.getByRole("button", { name: "sign in" }));
    fireEvent.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => {
      expect(screen.getByTestId("session-state")).toHaveTextContent(
        "unauthenticated",
      );
    });
    rejectPopup(new Error("popup failed"));
    await Promise.resolve();

    expect(authMocks.establishSumiSession).not.toHaveBeenCalled();
    expect(authMocks.clearDirectChatAuthority).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("session-state")).toHaveTextContent(
      "unauthenticated",
    );
  });
});
