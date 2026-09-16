import { Button } from "@sumi/ui/components/button";
import { LoaderCircle } from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { FaGithub } from "react-icons/fa";
import { FcGoogle } from "react-icons/fc";
import { type SignInProvider, useAuth } from "./auth-context";
import { getAuthErrorMessage } from "./auth-errors";
import type { AuthIntent } from "./auth-flow-client";
import type { EmailLinkInspection } from "./email-code-auth";
import { captureEnrollmentInvitation } from "./enrollment-invitation-state";
import {
  inspectEnrollmentInvitation,
  isEnrollmentInvitationUnavailable,
} from "./enrollment-invitations";
import { RegistrationSecretaryPanel } from "./registration-secretary-panel";
import { AuthAPIError } from "./session-client";

const providers: Array<{ id: SignInProvider; label: string }> = [
  { id: "google", label: "Googleで続ける" },
  { id: "github", label: "GitHubで続ける" },
];

const emailStatusPollMs = 5_000;

export function LoginScreen() {
  const {
    accountSwitch,
    authenticated,
    cancelAccountSwitch,
    cancelEmailCode,
    cancelIntentTransition,
    confirmAccountSwitch,
    confirmation,
    configured,
    confirmIntentTransition,
    continueEmailLink,
    dismissEmailLink,
    dismissRedirectSignInError,
    emailCode,
    emailLinkPending,
    inspectEmailLink,
    redirectSignInError,
    refreshEmailCode,
    resendEmailCode,
    sessionState,
    signIn,
    startEmailCode,
    submitEmailCode,
    user,
  } = useAuth();
  const [invitation, setInvitation] = useState(captureEnrollmentInvitation);
  useEffect(() => {
    const capture = () => {
      const token = captureEnrollmentInvitation();
      setInvitation(token);
      if (token) setIntent("sign_up");
    };
    window.addEventListener("hashchange", capture);
    return () => window.removeEventListener("hashchange", capture);
  }, []);
  const [invitationStatus, setInvitationStatus] = useState<
    "checking" | "valid" | "invalid" | "error" | "none"
  >(invitation ? "checking" : "none");
  const [invitationAttempt, setInvitationAttempt] = useState(0);
  const [intent, setIntent] = useState<AuthIntent>(
    invitation ? "sign_up" : "sign_in",
  );
  const [busy, setBusy] = useState<
    | SignInProvider
    | "email"
    | "code"
    | "resend"
    | "link"
    | "confirm"
    | "cancel"
    | null
  >(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [now, setNow] = useState(() => Date.now());
  const [linkInspection, setLinkInspection] =
    useState<EmailLinkInspection | null>(null);
  const linkInspectionStarted = useRef(false);
  const codeInput = useRef<HTMLInputElement>(null);

  // biome-ignore lint/correctness/useExhaustiveDependencies: invitationAttempt explicitly retries inspection.
  useEffect(() => {
    if (!invitation) {
      setInvitationStatus("none");
      return;
    }
    let mounted = true;
    setInvitationStatus("checking");
    void inspectEnrollmentInvitation(invitation).then(
      () => {
        if (mounted) setInvitationStatus("valid");
      },
      (reason: unknown) => {
        if (mounted)
          setInvitationStatus(
            isEnrollmentInvitationUnavailable(reason) ? "invalid" : "error",
          );
      },
    );
    return () => {
      mounted = false;
    };
  }, [invitation, invitationAttempt]);

  // Inspecting a link is read-only. Only this browser's own flow continues
  // without a choice, and never over an active session.
  useEffect(() => {
    if (
      linkInspectionStarted.current ||
      !configured ||
      !emailLinkPending ||
      (sessionState !== "unauthenticated" && sessionState !== "authenticated")
    ) {
      return;
    }
    linkInspectionStarted.current = true;
    setBusy("link");
    setError(null);
    void inspectEmailLink()
      .then(async (inspection) => {
        setLinkInspection(inspection);
        if (
          sessionState === "unauthenticated" &&
          inspection.sameBrowser &&
          (inspection.state === "usable" || inspection.state === "proved_here")
        ) {
          await continueEmailLink(inspection, false);
        }
      })
      .catch((nextError: unknown) => setError(getAuthErrorMessage(nextError)))
      .finally(() => setBusy(null));
  }, [
    configured,
    continueEmailLink,
    emailLinkPending,
    inspectEmailLink,
    sessionState,
  ]);

  // The code form follows proofs finished in another tab or browser.
  useEffect(() => {
    if (!emailCode || emailLinkPending) return;
    const poll = globalThis.setInterval(() => {
      void refreshEmailCode().catch((nextError: unknown) => {
        if (
          nextError instanceof AuthAPIError &&
          (nextError.message === "continued_in_other_browser" ||
            nextError.message === "flow_expired")
        ) {
          setError(getAuthErrorMessage(nextError));
        }
      });
    }, emailStatusPollMs);
    const tick = globalThis.setInterval(() => setNow(Date.now()), 1_000);
    return () => {
      globalThis.clearInterval(poll);
      globalThis.clearInterval(tick);
    };
  }, [emailCode, emailLinkPending, refreshEmailCode]);

  // A back/forward-cache restore revives this component with the spinner that
  // was showing when the tab left for the provider. The awaited navigation
  // promise can never settle, so clear the busy state here; the auth context
  // releases its own hold and reports the return's outcome separately.
  useEffect(() => {
    const onPageShow = (event: PageTransitionEvent) => {
      if (event.persisted) setBusy(null);
    };
    window.addEventListener("pageshow", onPageShow);
    return () => window.removeEventListener("pageshow", onPageShow);
  }, []);

  const handleSignIn = async (provider: SignInProvider) => {
    if (
      busy ||
      !configured ||
      (intent === "sign_up" && invitationStatus !== "valid")
    ) {
      return;
    }
    setBusy(provider);
    setError(null);
    dismissRedirectSignInError?.();
    try {
      // This leaves the tab for the provider and normally never returns here.
      // The spinner stays until the browser navigates away.
      await signIn(provider, intent);
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
      setBusy(null);
    }
  };

  const handleStartEmailCode = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (
      busy ||
      !configured ||
      (intent === "sign_up" && invitationStatus !== "valid")
    )
      return;
    setBusy("email");
    setError(null);
    setNotice(null);
    dismissRedirectSignInError?.();
    try {
      await startEmailCode(email, intent);
      setCode("");
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

  const submitCode = async (value: string) => {
    if (busy || value.length !== 6) return;
    setBusy("code");
    setError(null);
    setNotice(null);
    try {
      await submitEmailCode(value);
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
      setCode("");
      codeInput.current?.focus();
    } finally {
      setBusy(null);
    }
  };

  const handleCodeChange = (raw: string) => {
    const digits = normalizeCodeInput(raw);
    setCode(digits);
    // One-time-code autofill and paste submit as soon as the code is complete.
    if (digits.length === 6 && code.length !== 6) void submitCode(digits);
  };

  const handleResend = async () => {
    if (busy) return;
    setBusy("resend");
    setError(null);
    setNotice(null);
    try {
      await resendEmailCode();
      setNotice(
        "メールを再送信しました。最新のメールに記載されたコードを入力してください。",
      );
      setCode("");
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

  const handleContinueLink = async (switchAccount: boolean) => {
    if (busy || !linkInspection) return;
    setBusy("link");
    setError(null);
    try {
      // The switch click is the explicit consent: resolve carries
      // switch_from_user_id and the server replaces the session atomically.
      // Logging out first would clear the pending link and leave a window
      // with no account at all.
      await continueEmailLink(linkInspection, !linkInspection.sameBrowser, {
        switchFromUserId: switchAccount ? user?.id : undefined,
      });
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

  const handleDismissLink = () => {
    setLinkInspection(null);
    setError(null);
    dismissEmailLink();
  };

  // A redirect that came back without a session reports itself here: its
  // failure happened during startup, outside any click handler.
  const displayedError =
    error ??
    (redirectSignInError ? getAuthErrorMessage(redirectSignInError) : null);
  const resendAt = emailCode?.challenge
    ? Date.parse(emailCode.challenge.resendAvailableAt)
    : Number.NaN;
  const resendWait =
    Number.isFinite(resendAt) && resendAt > now
      ? Math.ceil((resendAt - now) / 1000)
      : 0;
  const codeExpiry = emailCode?.challenge
    ? formatClock(emailCode.challenge.challengeExpiresAt)
    : null;

  return (
    <main className="fixed inset-0 z-50 flex min-h-dvh flex-col overflow-y-auto bg-neutral-50 text-foreground dark:bg-background">
      <header className="flex h-12 shrink-0 items-center px-5">
        <span className="font-semibold text-[15px]">Sumi</span>
      </header>
      <div className="grid flex-1 place-items-center px-5 pb-16">
        <section aria-labelledby="login-title" className="w-full max-w-[25rem]">
          <div className="rounded-2xl border bg-background px-6 py-8 shadow-sm sm:px-8">
            <div className="mb-7 text-center">
              <div className="mx-auto mb-4 grid size-11 place-items-center rounded-xl bg-foreground font-semibold text-background">
                S
              </div>
              <p className="mb-1.5 font-medium text-muted-foreground text-sm">
                Sumi
              </p>
              <h1
                id="login-title"
                className="font-semibold text-2xl tracking-[-0.025em]"
              >
                {accountSwitch
                  ? "アカウントを切り替えますか？"
                  : emailLinkPending
                    ? authenticated
                      ? "アカウントを切り替えますか？"
                      : "メールのリンクで続ける"
                    : confirmation
                      ? "続行方法の確認"
                      : emailCode
                        ? "確認コードを入力"
                        : intent === "sign_in"
                          ? "アカウントにログイン"
                          : "Sumiへようこそ"}
              </h1>
            </div>

            {accountSwitch ? (
              <div className="space-y-4">
                <p className="rounded-lg bg-muted px-3 py-2.5 text-sm">
                  現在のログイン:{" "}
                  {accountSwitch.currentDisplayName ?? "別のアカウント"}
                </p>
                <p className="text-muted-foreground text-sm leading-6">
                  {accountSwitch.target}
                  のログインを完了すると、現在のセッションは終了し、このブラウザはそのアカウントへ切り替わります。
                </p>
                <Button
                  type="button"
                  onClick={() => {
                    setBusy("confirm");
                    setError(null);
                    void confirmAccountSwitch()
                      .catch((nextError: unknown) => {
                        setError(getAuthErrorMessage(nextError));
                      })
                      .finally(() => setBusy(null));
                  }}
                  disabled={busy !== null}
                  className="h-11 w-full rounded-lg"
                >
                  {busy === "confirm" && (
                    <LoaderCircle className="size-5 animate-spin" />
                  )}
                  切り替える
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => {
                    setBusy("cancel");
                    cancelAccountSwitch();
                    setBusy(null);
                  }}
                  disabled={busy !== null}
                  className="h-11 w-full rounded-lg"
                >
                  キャンセル
                </Button>
              </div>
            ) : emailLinkPending ? (
              <EmailLinkPanel
                authenticated={authenticated}
                busy={busy !== null}
                inspection={linkInspection}
                sessionUnavailable={sessionState === "unavailable"}
                onContinue={(switchAccount) =>
                  void handleContinueLink(switchAccount)
                }
                onDismiss={handleDismissLink}
              />
            ) : confirmation ? (
              confirmation.action === "create_account" ? (
                <RegistrationSecretaryPanel
                  confirmation={confirmation}
                  accountLabel={confirmationAccountLabel(confirmation)}
                  busy={busy === "confirm"}
                  onConfirm={async () => {
                    setBusy("confirm");
                    setError(null);
                    try {
                      await confirmIntentTransition();
                    } finally {
                      setBusy(null);
                    }
                  }}
                  onCancel={async () => {
                    setBusy("cancel");
                    try {
                      await cancelIntentTransition();
                    } finally {
                      setBusy(null);
                    }
                  }}
                  onError={setError}
                />
              ) : (
                <div className="space-y-4">
                  <p className="rounded-lg bg-muted px-3 py-2.5 text-sm">
                    対象アカウント: {confirmationAccountLabel(confirmation)}
                  </p>
                  <p className="text-muted-foreground text-sm leading-6">
                    新規登録を選択しましたが、この認証情報は既存のSumiアカウントに登録されています。ログインして続けますか？
                  </p>
                  <Button
                    type="button"
                    onClick={() => {
                      setBusy("confirm");
                      setError(null);
                      void confirmIntentTransition()
                        .catch((nextError: unknown) => {
                          setError(getAuthErrorMessage(nextError));
                        })
                        .finally(() => setBusy(null));
                    }}
                    disabled={busy !== null}
                    className="h-11 w-full rounded-lg"
                  >
                    {busy === "confirm" && (
                      <LoaderCircle className="size-5 animate-spin" />
                    )}
                    ログインして続ける
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => {
                      setBusy("cancel");
                      void cancelIntentTransition().finally(() =>
                        setBusy(null),
                      );
                    }}
                    disabled={busy !== null}
                    className="h-11 w-full rounded-lg"
                  >
                    キャンセル
                  </Button>
                </div>
              )
            ) : emailCode ? (
              <div className="space-y-4">
                {emailCode.recovery && (
                  <p
                    role="status"
                    className="rounded-lg bg-amber-50 px-3 py-2.5 text-amber-800 text-sm dark:bg-amber-950/30 dark:text-amber-200"
                  >
                    このメールアドレスは既存のアカウントに登録されています。確認コードでログインすると、選択したログイン方法を追加します。
                  </p>
                )}
                <p className="text-muted-foreground text-sm leading-6">
                  <span className="break-all font-medium text-foreground">
                    {emailCode.email}
                  </span>
                  に6桁の確認コードを送信しました。
                </p>
                <form
                  onSubmit={(event) => {
                    event.preventDefault();
                    void submitCode(code);
                  }}
                  className="space-y-3"
                >
                  <label htmlFor="sumi-auth-code" className="sr-only">
                    確認コード
                  </label>
                  <input
                    ref={codeInput}
                    id="sumi-auth-code"
                    name="one-time-code"
                    type="text"
                    inputMode="numeric"
                    autoComplete="one-time-code"
                    // biome-ignore lint/a11y/noAutofocus: the code field is the only next step after sending the email.
                    autoFocus
                    required
                    value={code}
                    onChange={(event) => handleCodeChange(event.target.value)}
                    disabled={busy !== null || !configured}
                    placeholder="000000"
                    aria-describedby="sumi-auth-code-help"
                    className="h-11 w-full rounded-lg border bg-background px-3 text-center text-base tabular-nums tracking-[0.3em] outline-none focus-visible:ring-3 focus-visible:ring-ring/40 disabled:opacity-50"
                  />
                  <Button
                    type="submit"
                    disabled={busy !== null || code.length !== 6}
                    className="h-11 w-full rounded-lg"
                  >
                    {busy === "code" && (
                      <LoaderCircle className="size-5 animate-spin" />
                    )}
                    確認して続ける
                  </Button>
                </form>
                <p
                  id="sumi-auth-code-help"
                  role="status"
                  className={
                    emailCode.challenge?.delivery === "failed"
                      ? "rounded-lg bg-amber-50 px-3 py-2.5 text-amber-800 text-sm dark:bg-amber-950/30 dark:text-amber-200"
                      : "text-muted-foreground text-xs leading-5"
                  }
                >
                  {emailCode.challenge?.delivery === "failed"
                    ? "メールを送信できませんでした。アドレスを確認して、コードを再送信してください。"
                    : `メール内のリンクからも続けられます。${
                        codeExpiry ? `コードは${codeExpiry}まで有効です。` : ""
                      }`}
                </p>
                {notice && (
                  <p
                    role="status"
                    className="rounded-lg bg-emerald-50 px-3 py-2.5 text-emerald-800 text-sm dark:bg-emerald-950/30 dark:text-emerald-200"
                  >
                    {notice}
                  </p>
                )}
                <div className="flex items-center justify-between gap-2">
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setError(null);
                      setNotice(null);
                      setCode("");
                      cancelEmailCode();
                    }}
                    disabled={busy !== null}
                  >
                    メールアドレスを変更
                  </Button>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => void handleResend()}
                    disabled={busy !== null || resendWait > 0}
                  >
                    {busy === "resend" && (
                      <LoaderCircle className="size-4 animate-spin" />
                    )}
                    {resendWait > 0
                      ? `再送信（${resendWait}秒）`
                      : "コードを再送信"}
                  </Button>
                </div>
              </div>
            ) : (
              <>
                {invitationStatus === "valid" ? (
                  <div className="mb-4 grid grid-cols-2 rounded-lg bg-muted p-1">
                    {(
                      [
                        ["sign_in", "ログイン"],
                        ["sign_up", "招待を受け取る"],
                      ] as const
                    ).map(([value, label]) => (
                      <button
                        key={value}
                        type="button"
                        aria-pressed={intent === value}
                        onClick={() => setIntent(value)}
                        disabled={busy !== null}
                        className="rounded-md px-3 py-2 font-medium text-sm aria-pressed:bg-background aria-pressed:shadow-sm"
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                ) : (
                  <p
                    className="mb-5 text-muted-foreground text-sm leading-6"
                    role="status"
                  >
                    {invitationStatus === "checking"
                      ? "招待を確認しています…"
                      : invitationStatus === "invalid"
                        ? "この招待は期限切れ、取消済み、または使用済みです。招待した方に新しいリンクを依頼してください。"
                        : invitationStatus === "error"
                          ? "招待を確認できませんでした。接続を確認して、もう一度お試しください。"
                          : "現在、Sumiの新規利用には招待が必要です。すでに利用している方はログインしてください。"}
                    {invitationStatus === "error" && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() =>
                          setInvitationAttempt((value) => value + 1)
                        }
                      >
                        再試行
                      </Button>
                    )}
                    {invitationStatus === "invalid" && (
                      <button
                        type="button"
                        className="mt-2 block underline underline-offset-4"
                        onClick={() => setIntent("sign_in")}
                      >
                        既存のアカウントでログイン
                      </button>
                    )}
                  </p>
                )}
                <form onSubmit={handleStartEmailCode} className="space-y-3">
                  <label htmlFor="sumi-auth-email" className="sr-only">
                    メールアドレス
                  </label>
                  <input
                    id="sumi-auth-email"
                    type="email"
                    autoComplete="email"
                    required
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    disabled={busy !== null || !configured}
                    placeholder="メールアドレス"
                    className="h-11 w-full rounded-lg border bg-background px-3 text-sm outline-none focus-visible:ring-3 focus-visible:ring-ring/40 disabled:opacity-50"
                  />
                  <Button
                    type="submit"
                    disabled={busy !== null || !configured}
                    className="h-11 w-full rounded-lg"
                  >
                    {busy === "email" && (
                      <LoaderCircle className="size-5 animate-spin" />
                    )}
                    {intent === "sign_in"
                      ? "メールでログイン"
                      : "メールで新規登録"}
                  </Button>
                </form>
                <div className="my-4 flex items-center gap-3 text-muted-foreground text-xs">
                  <span className="h-px flex-1 bg-border" />
                  または
                  <span className="h-px flex-1 bg-border" />
                </div>
                <div className="space-y-3">
                  {providers.map((provider) => (
                    <Button
                      key={provider.id}
                      type="button"
                      variant="outline"
                      onClick={() => void handleSignIn(provider.id)}
                      disabled={busy !== null || !configured}
                      className="h-11 w-full justify-center gap-2.5 rounded-lg bg-background text-sm"
                    >
                      {busy === provider.id ? (
                        <LoaderCircle className="size-5 animate-spin" />
                      ) : provider.id === "github" ? (
                        <FaGithub className="size-5" />
                      ) : (
                        <FcGoogle className="size-5" />
                      )}
                      {provider.label}
                    </Button>
                  ))}
                </div>
                {intent === "sign_up" && invitationStatus === "valid" && (
                  <p className="pt-1 text-center text-muted-foreground text-xs leading-5">
                    Sumi Localの秘書を引き継ぐ場合は
                    <button
                      type="button"
                      className="underline underline-offset-4"
                      onClick={() => setIntent("sign_in")}
                      disabled={busy !== null}
                    >
                      ログインして移動を選ぶ
                    </button>
                  </p>
                )}
              </>
            )}

            {!configured && (
              <p
                role="status"
                className="mt-4 rounded-lg bg-amber-50 px-3 py-2.5 text-amber-800 text-sm dark:bg-amber-950/30 dark:text-amber-200"
              >
                Firebase Authentication が設定されていません。
              </p>
            )}
            {displayedError && (
              <p
                role="alert"
                className="mt-4 rounded-lg bg-red-50 px-3 py-2.5 text-red-700 text-sm dark:bg-red-950/30 dark:text-red-300"
              >
                {displayedError}
              </p>
            )}
          </div>
          <p className="mt-5 px-4 text-center text-muted-foreground text-xs leading-relaxed">
            続行すると、Sumiの利用規約とプライバシーポリシーに同意したものとみなされます。
          </p>
        </section>
      </div>
    </main>
  );
}

function EmailLinkPanel({
  authenticated,
  busy,
  inspection,
  sessionUnavailable,
  onContinue,
  onDismiss,
}: {
  authenticated: boolean;
  busy: boolean;
  inspection: EmailLinkInspection | null;
  sessionUnavailable: boolean;
  onContinue: (switchAccount: boolean) => void;
  onDismiss: () => void;
}) {
  const closeButton = (label: string) => (
    <Button
      type="button"
      variant="outline"
      onClick={onDismiss}
      disabled={busy}
      className="h-11 w-full rounded-lg"
    >
      {label}
    </Button>
  );
  if (sessionUnavailable) {
    return (
      <div className="space-y-4">
        <p className="text-muted-foreground text-sm leading-6">
          現在のSumiセッションを確認できないため、メールのリンクを処理できません。
        </p>
        {closeButton("閉じる")}
      </div>
    );
  }
  if (!inspection) {
    return (
      <div className="space-y-4 text-center">
        {busy && <LoaderCircle className="mx-auto size-6 animate-spin" />}
        <p className="text-muted-foreground text-sm leading-6">
          {busy
            ? "メールのリンクを確認しています…"
            : "リンクを確認できませんでした。"}
        </p>
        {!busy && closeButton("閉じる")}
      </div>
    );
  }
  const ended: Partial<Record<EmailLinkInspection["state"], string>> = {
    completed: "このログインはすでに完了しています。",
    consumed:
      "このメールのコードまたはリンクはすでに使用されています。ログインを始めた画面を確認してください。",
    superseded:
      "新しいメールが送信されています。最新のメールのリンクを開くか、コードを入力してください。",
    expired:
      "このリンクの有効期限が切れました。もう一度メールアドレスを入力してください。",
  };
  const endedMessage = ended[inspection.state];
  if (endedMessage) {
    return (
      <div className="space-y-4">
        <p className="text-muted-foreground text-sm leading-6">
          {endedMessage}
        </p>
        {closeButton(authenticated ? "Sumiに戻る" : "閉じる")}
      </div>
    );
  }
  const account = (
    <p className="rounded-lg bg-muted px-3 py-2.5 text-sm">
      対象アカウント: <span className="break-all">{inspection.email}</span>
    </p>
  );
  if (authenticated) {
    if (inspection.session === "same_account") {
      return (
        <div className="space-y-4">
          {account}
          <p className="text-muted-foreground text-sm leading-6">
            すでにこのアカウントでログインしています。
          </p>
          {closeButton("Sumiに戻る")}
        </div>
      );
    }
    return (
      <div className="space-y-4">
        {account}
        <p className="text-muted-foreground text-sm leading-6">
          現在のSumiセッションを終了し、メールのアカウントへ切り替えます。自動では切り替わりません。
        </p>
        <Button
          type="button"
          onClick={() => onContinue(true)}
          disabled={busy}
          className="h-11 w-full rounded-lg"
        >
          {busy && <LoaderCircle className="size-5 animate-spin" />}
          現在のセッションを終了して切り替える
        </Button>
        {closeButton("現在のアカウントを使い続ける")}
      </div>
    );
  }
  if (inspection.sameBrowser) {
    return (
      <div className="space-y-4 text-center">
        {busy && <LoaderCircle className="mx-auto size-6 animate-spin" />}
        <p className="text-muted-foreground text-sm leading-6">
          {busy ? "ログインしています…" : "ログインを完了できませんでした。"}
        </p>
        {!busy && (
          <Button
            type="button"
            onClick={() => onContinue(false)}
            className="h-11 w-full rounded-lg"
          >
            もう一度試す
          </Button>
        )}
      </div>
    );
  }
  return (
    <div className="space-y-4">
      {account}
      <p className="text-muted-foreground text-sm leading-6">
        このブラウザでログインを続けますか？続けると、ログインを始めたアプリや画面ではこのコードを使えなくなります。
      </p>
      <Button
        type="button"
        onClick={() => onContinue(false)}
        disabled={busy}
        className="h-11 w-full rounded-lg"
      >
        {busy && <LoaderCircle className="size-5 animate-spin" />}
        このブラウザで続ける
      </Button>
      {closeButton("キャンセル（元の画面でコードを入力する）")}
    </div>
  );
}

/** Keeps ASCII digits from typed, pasted, full-width, or spaced codes. */
export function normalizeCodeInput(raw: string): string {
  return raw
    .replace(/[０-９]/g, (digit) =>
      String.fromCharCode(digit.charCodeAt(0) - 0xfee0),
    )
    .replace(/\D/g, "")
    .slice(0, 6);
}

function formatClock(value: string): string | null {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return null;
  return date.toLocaleTimeString("ja-JP", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function confirmationAccountLabel({
  account,
  firebaseUID,
}: {
  account: { displayName: string | null; email: string | null };
  firebaseUID: string;
}): string {
  return account.email ?? account.displayName ?? `Firebase ${firebaseUID}`;
}
