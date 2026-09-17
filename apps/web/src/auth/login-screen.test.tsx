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
import type { EmailLinkInspection } from "./email-code-auth";
import { LoginScreen, normalizeCodeInput } from "./login-screen";
import { AuthAPIError } from "./session-client";

const loginMocks = vi.hoisted(() => ({
  useAuth: vi.fn(),
  getSignInMethods: vi.fn().mockResolvedValue({ emailCode: true }),
}));

vi.mock("./auth-context", () => ({
  useAuth: loginMocks.useAuth,
}));

vi.mock("./provider-operation-client", () => ({
  getSignInMethods: loginMocks.getSignInMethods,
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

    fireEvent.change(await screen.findByPlaceholderText("メールアドレス"), {
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

    const input = await screen.findByLabelText("確認コード");
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

    fireEvent.change(await screen.findByLabelText("確認コード"), {
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
      await screen.findByRole("button", { name: /再送信（\d+秒）/ }),
    ).toBeDisabled();

    loginMocks.useAuth.mockReturnValue({ ...state, emailCode: codeFlow() });
    rerender(<LoginScreen />);
    fireEvent.click(screen.getByRole("button", { name: "コードを再送信" }));

    await waitFor(() => expect(state.resendEmailCode).toHaveBeenCalledTimes(1));
    expect(await screen.findByText(/メールを再送信しました/)).toBeVisible();
  });

  it("reports failed delivery as recoverable", async () => {
    const failed = codeFlow();
    failed.challenge.delivery = "failed";
    loginMocks.useAuth.mockReturnValue(authState({ emailCode: failed }));
    render(<LoginScreen />);

    expect(
      await screen.findByText(/メールを送信できませんでした。/),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "コードを再送信" }),
    ).toBeEnabled();
  });

  it("explains that a provider collision is waiting for email proof", async () => {
    loginMocks.useAuth.mockReturnValue(
      authState({ emailCode: codeFlow({ recovery: true }) }),
    );
    render(<LoginScreen />);

    expect(
      await screen.findByText(/選択したログイン方法を追加します/),
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
      expect(state.continueEmailLink).toHaveBeenCalledWith(inspected, true, {
        switchFromUserId: undefined,
      }),
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

  it("completes a chosen account switch through the consented resolve, without logging out", async () => {
    const inspected = inspection({
      session: "other_account",
      sameBrowser: false,
    });
    // The real continueEmailLink throws link_invalid once logout clears the
    // pending link; model that coupling so this test fails if the click path
    // ever logs out before completing the link again.
    let linkCleared = false;
    const state = authState({
      authenticated: true,
      user: { id: "human-current" },
      emailLinkPending: true,
      inspectEmailLink: vi.fn().mockResolvedValue(inspected),
      logout: vi.fn(async () => {
        linkCleared = true;
      }),
      continueEmailLink: vi.fn(async () => {
        if (linkCleared) throw new AuthAPIError("link_invalid", 404);
      }),
      sessionState: "authenticated",
    });
    loginMocks.useAuth.mockImplementation(() => state);
    render(<LoginScreen />);

    fireEvent.click(
      await screen.findByRole("button", {
        name: "現在のセッションを終了して切り替える",
      }),
    );

    await waitFor(() =>
      expect(state.continueEmailLink).toHaveBeenCalledWith(inspected, true, {
        switchFromUserId: "human-current",
      }),
    );
    expect(state.logout).not.toHaveBeenCalled();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
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

describe("LoginScreen without an email sender", () => {
  it("offers OAuth sign-in instead of a dead email submit", async () => {
    loginMocks.getSignInMethods.mockResolvedValue({ emailCode: false });
    const state = authState();
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      await screen.findByText(
        /メールでのログイン・新規登録は現在利用できません/,
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByPlaceholderText("メールアドレス"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "メールでログイン" }),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Googleで続ける" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "GitHubで続ける" }),
    ).toBeInTheDocument();
    expect(state.startEmailCode).not.toHaveBeenCalled();
  });

  it("never enables an email submit while the capability read is pending", async () => {
    let resolveMethods: (value: { emailCode: boolean }) => void = () => {};
    loginMocks.getSignInMethods.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveMethods = resolve;
        }),
    );
    const state = authState();
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      await screen.findByText(/利用できるログイン方法を確認しています/),
    ).toBeInTheDocument();
    expect(
      screen.queryByPlaceholderText("メールアドレス"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "メールでログイン" }),
    ).not.toBeInTheDocument();
    // OAuth stays usable through the pending read.
    const google = screen.getByRole("button", { name: "Googleで続ける" });
    expect(google).toBeEnabled();
    fireEvent.click(google);
    await waitFor(() =>
      expect(state.signIn).toHaveBeenCalledWith("google", "sign_in"),
    );

    // A late "unavailable" answer lands in the disabled state, not a form.
    resolveMethods({ emailCode: false });
    expect(
      await screen.findByText(
        /メールでのログイン・新規登録は現在利用できません/,
      ),
    ).toBeInTheDocument();
    expect(state.startEmailCode).not.toHaveBeenCalled();
  });

  it("reports a failed capability read and retries into the enabled form", async () => {
    loginMocks.getSignInMethods
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce({ emailCode: true });
    const state = authState();
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      await screen.findByText(
        /メールでのログインが利用できるか確認できませんでした/,
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByPlaceholderText("メールアドレス"),
    ).not.toBeInTheDocument();
    // OAuth stays usable through the failure.
    expect(
      screen.getByRole("button", { name: "GitHubで続ける" }),
    ).toBeEnabled();

    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    expect(
      await screen.findByPlaceholderText("メールアドレス"),
    ).toBeInTheDocument();
    expect(loginMocks.getSignInMethods).toHaveBeenCalledTimes(2);
  });

  it("retries a failed capability read into the disabled state", async () => {
    loginMocks.getSignInMethods
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce({ emailCode: false });
    loginMocks.useAuth.mockReturnValue(authState());
    render(<LoginScreen />);

    fireEvent.click(await screen.findByRole("button", { name: "再試行" }));
    expect(
      await screen.findByText(
        /メールでのログイン・新規登録は現在利用できません/,
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByPlaceholderText("メールアドレス"),
    ).not.toBeInTheDocument();
  });

  it("blocks a saved code step when email turns out unavailable", async () => {
    loginMocks.getSignInMethods.mockResolvedValue({ emailCode: false });
    const state = authState({ emailCode: codeFlow() });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      await screen.findByText(/このコードは確認できません/),
    ).toBeInTheDocument();
    // No unusable controls remain: no input, submit, resend, or "sent" copy.
    expect(screen.queryByLabelText("確認コード")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /コードを再送信/ }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText(/確認コードを送信しました/),
    ).not.toBeInTheDocument();
    expect(state.refreshEmailCode).not.toHaveBeenCalled();
    expect(state.resendEmailCode).not.toHaveBeenCalled();
    expect(state.submitEmailCode).not.toHaveBeenCalled();

    // The exit clears the saved flow and returns to usable methods.
    fireEvent.click(screen.getByRole("button", { name: "別の方法でログイン" }));
    expect(state.cancelEmailCode).toHaveBeenCalledTimes(1);
  });

  it("does not inspect a pending link email sign-in cannot complete", async () => {
    loginMocks.getSignInMethods.mockResolvedValue({ emailCode: false });
    const state = authState({ emailLinkPending: true });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    expect(
      await screen.findByText(/このリンクでは続けられません/),
    ).toBeInTheDocument();
    // Inspection is deliberately skipped — the route is unmounted; no race.
    expect(state.inspectEmailLink).not.toHaveBeenCalled();
    expect(state.continueEmailLink).not.toHaveBeenCalled();

    // The dismiss clears the pending link so the panel is not left waiting.
    fireEvent.click(screen.getByRole("button", { name: "閉じる" }));
    expect(state.dismissEmailLink).toHaveBeenCalledTimes(1);
  });

  it("can dismiss a link while availability is pending without late completion", async () => {
    let resolveMethods: (value: { emailCode: boolean }) => void = () => {};
    loginMocks.getSignInMethods.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveMethods = resolve;
        }),
    );
    const state = authState({ emailLinkPending: true });
    loginMocks.useAuth.mockReturnValue(state);
    const { rerender } = render(<LoginScreen />);

    fireEvent.click(screen.getByRole("button", { name: "閉じる" }));
    expect(state.dismissEmailLink).toHaveBeenCalledTimes(1);
    loginMocks.useAuth.mockReturnValue({ ...state, emailLinkPending: false });
    rerender(<LoginScreen />);
    resolveMethods({ emailCode: true });
    expect(
      await screen.findByRole("button", { name: "メールでログイン" }),
    ).toBeEnabled();
    expect(state.inspectEmailLink).not.toHaveBeenCalled();
    expect(state.continueEmailLink).not.toHaveBeenCalled();
  });

  it("holds a pending link until the capability answer, then inspects", async () => {
    let resolveMethods: (value: { emailCode: boolean }) => void = () => {};
    loginMocks.getSignInMethods.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveMethods = resolve;
        }),
    );
    const inspected = inspection({ sameBrowser: true });
    const state = authState({
      emailLinkPending: true,
      inspectEmailLink: vi.fn().mockResolvedValue(inspected),
    });
    loginMocks.useAuth.mockReturnValue(state);
    render(<LoginScreen />);

    // While the read is in flight no inspect request is issued.
    expect(await screen.findByText(/確認しています…/)).toBeInTheDocument();
    expect(state.inspectEmailLink).not.toHaveBeenCalled();

    resolveMethods({ emailCode: true });
    await waitFor(() =>
      expect(state.continueEmailLink).toHaveBeenCalledWith(inspected, false),
    );
  });
});
