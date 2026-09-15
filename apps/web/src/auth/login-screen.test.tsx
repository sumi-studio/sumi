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
import type { EmailLinkInspection } from "./email-code-auth";
import { LoginScreen, normalizeCodeInput } from "./login-screen";
import { AuthAPIError } from "./session-client";

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

function authState(overrides: Record<string, unknown> = {}) {
  return {
    authenticated: false,
    cancelEmailCode: vi.fn(),
    cancelIntentTransition: vi.fn(),
    confirmation: null,
    configured: true,
    confirmIntentTransition: vi.fn(),
    continueEmailLink: vi.fn().mockResolvedValue(undefined),
    credentialRecoveryEmailSent: false,
    dismissEmailLink: vi.fn(),
    dismissRedirectSignInError: vi.fn(),
    emailCode: null,
    emailLinkPending: false,
    inspectEmailLink: vi.fn(),
    logout: vi.fn().mockResolvedValue(undefined),
    redirectSignInError: null,
    refreshEmailCode: vi.fn().mockResolvedValue(undefined),
    resendEmailCode: vi.fn().mockResolvedValue(undefined),
    sessionState: "unauthenticated",
    signIn: vi.fn(),
    startEmailCode: vi.fn().mockResolvedValue(undefined),
    submitEmailCode: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

function codeFlow(overrides: Record<string, unknown> = {}) {
  return {
    email: "person@example.com",
    intent: "sign_in",
    recovery: false,
    challenge: {
      flowId: "flow-id",
      flowStatus: "pending",
      email: "person@example.com",
      flowExpiresAt: new Date(Date.now() + 30 * 60_000).toISOString(),
      challengeExpiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
      attemptsRemaining: 5,
      delivery: "sent",
      resendAvailableAt: new Date(Date.now() - 60_000).toISOString(),
    },
    ...overrides,
  };
}

function inspection(
  overrides: Partial<EmailLinkInspection> = {},
): EmailLinkInspection {
  return {
    flowId: "flow-id",
    intent: "sign_in",
    email: "person@example.com",
    state: "usable",
    sameBrowser: false,
    session: "none",
    expiresAt: new Date(Date.now() + 10 * 60_000).toISOString(),
    ...overrides,
  };
}

describe("LoginScreen email code", () => {
  it("starts the code flow from the existing email form", async () => {
    const state = authState();
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    fireEvent.change(screen.getByPlaceholderText("メールアドレス"), {
      target: { value: "person@example.com" },
    });
    fireEvent.click(screen.getByRole("button", { name: "メールでログイン" }));

    await waitFor(() =>
      expect(state.startEmailCode).toHaveBeenCalledWith(
        "person@example.com",
        "sign_in",
      ),
    );
  });

  it("accepts one-time-code autofill or paste and submits the normalized code once", async () => {
    const state = authState({ emailCode: codeFlow() });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    const input = screen.getByLabelText("確認コード");
    expect(input).toHaveAttribute("autocomplete", "one-time-code");
    expect(input).toHaveAttribute("inputmode", "numeric");
    expect(
      screen.getByRole("heading", { name: "確認コードを入力" }),
    ).toBeInTheDocument();
    fireEvent.change(input, { target: { value: "１２３ ４５６" } });

    await waitFor(() =>
      expect(state.submitEmailCode).toHaveBeenCalledWith("123456"),
    );
    expect(state.submitEmailCode).toHaveBeenCalledTimes(1);
  });

  it("shows the remaining attempts and keeps the form for another try", async () => {
    const state = authState({
      emailCode: codeFlow(),
      submitEmailCode: vi
        .fn()
        .mockRejectedValue(
          new AuthAPIError("code_mismatch", 422, { attemptsRemaining: 2 }),
        ),
    });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    fireEvent.change(screen.getByLabelText("確認コード"), {
      target: { value: "000000" },
    });

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "確認コードが正しくありません。あと2回入力できます。",
    );
    expect(screen.getByLabelText("確認コード")).toHaveValue("");
  });

  it("waits for the resend interval, then resends", async () => {
    const waiting = codeFlow();
    waiting.challenge.resendAvailableAt = new Date(
      Date.now() + 20_000,
    ).toISOString();
    const state = authState({ emailCode: waiting });
    loginMocks.useAuth.mockReturnValue(state);
    const { rerender } = render(<LoginScreen />);

    expect(
      screen.getByRole("button", { name: /再送信（\d+秒）/ }),
    ).toBeDisabled();

    loginMocks.useAuth.mockReturnValue({ ...state, emailCode: codeFlow() });
    rerender(<LoginScreen />);
    fireEvent.click(screen.getByRole("button", { name: "コードを再送信" }));

    await waitFor(() => expect(state.resendEmailCode).toHaveBeenCalledTimes(1));
    expect(await screen.findByText(/メールを再送信しました/)).toBeVisible();
  });

  it("reports failed delivery as recoverable", () => {
    const failed = codeFlow();
    failed.challenge.delivery = "failed";
    loginMocks.useAuth.mockReturnValue(authState({ emailCode: failed }));
    render(<LoginScreen />);

    expect(
      screen.getByText(/メールを送信できませんでした。/),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "コードを再送信" }),
    ).toBeEnabled();
  });

  it("explains that a provider collision is waiting for email proof", () => {
    loginMocks.useAuth.mockReturnValue(
      authState({ emailCode: codeFlow({ recovery: true }) }),
    );
    render(<LoginScreen />);

    expect(
      screen.getByText(/選択したログイン方法を追加します/),
    ).toBeInTheDocument();
  });

  it("normalizes spaced, hyphenated, and full-width codes", () => {
    expect(normalizeCodeInput("12-34 56")).toBe("123456");
    expect(normalizeCodeInput("１２３４５６７")).toBe("123456");
  });
});

describe("LoginScreen email link", () => {
  it("continues this browser's own flow without another choice", async () => {
    const inspected = inspection({ sameBrowser: true });
    const state = authState({
      emailLinkPending: true,
      inspectEmailLink: vi.fn().mockResolvedValue(inspected),
    });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    await waitFor(() =>
      expect(state.continueEmailLink).toHaveBeenCalledWith(inspected, false),
    );
  });

  it("requires an explicit choice before another browser takes over", async () => {
    const inspected = inspection();
    const state = authState({
      emailLinkPending: true,
      inspectEmailLink: vi.fn().mockResolvedValue(inspected),
    });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    const proceed = await screen.findByRole("button", {
      name: "このブラウザで続ける",
    });
    expect(state.continueEmailLink).not.toHaveBeenCalled();
    fireEvent.click(proceed);

    await waitFor(() =>
      expect(state.continueEmailLink).toHaveBeenCalledWith(inspected, true),
    );
  });

  it("requires an explicit choice before replacing an authenticated account", async () => {
    const state = authState({
      authenticated: true,
      emailLinkPending: true,
      inspectEmailLink: vi
        .fn()
        .mockResolvedValue(inspection({ session: "other_account" })),
      sessionState: "authenticated",
    });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      screen.getByRole("heading", { name: "アカウントを切り替えますか？" }),
    ).toBeInTheDocument();
    fireEvent.click(
      await screen.findByRole("button", {
        name: "現在のアカウントを使い続ける",
      }),
    );
    expect(state.dismissEmailLink).toHaveBeenCalledTimes(1);
    expect(state.continueEmailLink).not.toHaveBeenCalled();
    expect(state.logout).not.toHaveBeenCalled();
  });

  it("logs out before completing a chosen account switch", async () => {
    let finishLogout!: () => void;
    const inspected = inspection({
      session: "other_account",
      sameBrowser: false,
    });
    const state = authState({
      authenticated: true,
      emailLinkPending: true,
      inspectEmailLink: vi.fn().mockResolvedValue(inspected),
      logout: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            finishLogout = resolve;
          }),
      ),
      sessionState: "authenticated",
    });
    loginMocks.useAuth.mockImplementation(() => state);
    const { rerender } = render(<LoginScreen />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "現在のセッションを終了して切り替える",
      }),
    );
    state.authenticated = false;
    state.sessionState = "unauthenticated";
    rerender(<LoginScreen />);

    expect(state.continueEmailLink).not.toHaveBeenCalled();
    await act(async () => finishLogout());
    await waitFor(() =>
      expect(state.continueEmailLink).toHaveBeenCalledWith(inspected, true),
    );
    expect(state.inspectEmailLink).toHaveBeenCalledTimes(1);
  });

  it("does not offer to continue an already used link", async () => {
    const state = authState({
      emailLinkPending: true,
      inspectEmailLink: vi
        .fn()
        .mockResolvedValue(inspection({ state: "consumed" })),
    });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      await screen.findByText(/すでに使用されています/),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "このブラウザで続ける" }),
    ).not.toBeInTheDocument();
    expect(state.continueEmailLink).not.toHaveBeenCalled();
  });
});

describe("LoginScreen confirmation", () => {
  it("shows the Firebase account attached to a pending confirmation", () => {
    loginMocks.useAuth.mockReturnValue(
      authState({
        confirmation: {
          action: "create_account",
          account: { displayName: "New User", email: "new@example.com" },
          firebaseUID: "firebase-new",
        },
      }),
    );

    render(<LoginScreen />);

    expect(
      screen.getByText("対象アカウント: new@example.com"),
    ).toBeInTheDocument();
  });
});
