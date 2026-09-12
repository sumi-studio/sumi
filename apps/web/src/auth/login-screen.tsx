import { Button } from "@sumi/ui/components/button";
import { LoaderCircle } from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { FaGithub } from "react-icons/fa";
import { FcGoogle } from "react-icons/fc";
import { type SignInProvider, useAuth } from "./auth-context";
import { getAuthErrorMessage } from "./auth-errors";
import type { AuthIntent } from "./auth-flow-client";
import { captureEnrollmentInvitation } from "./enrollment-invitation-state";
import {
  inspectEnrollmentInvitation,
  isEnrollmentInvitationUnavailable,
} from "./enrollment-invitations";

const providers: Array<{ id: SignInProvider; label: string }> = [
  { id: "google", label: "Googleで続ける" },
  { id: "github", label: "GitHubで続ける" },
];

export function LoginScreen() {
  const {
    authenticated,
    cancelIntentTransition,
    completeEmailLink,
    confirmation,
    configured,
    confirmIntentTransition,
    credentialRecoveryEmailSent,
    emailLinkCallbackPending,
    logout,
    rejectEmailLink,
    sendEmailLink,
    sessionState,
    signIn,
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
    SignInProvider | "email" | "confirm" | "cancel" | null
  >(null);
  const [error, setError] = useState<string | null>(null);
  const [email, setEmail] = useState("");
  const [emailSent, setEmailSent] = useState(false);
  const emailCallbackStarted = useRef(false);

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

  useEffect(() => {
    if (
      emailCallbackStarted.current ||
      !configured ||
      !emailLinkCallbackPending ||
      sessionState !== "unauthenticated"
    ) {
      return;
    }
    emailCallbackStarted.current = true;
    setBusy("email");
    setError(null);
    void completeEmailLink()
      .catch((nextError: unknown) => {
        setError(getAuthErrorMessage(nextError));
      })
      .finally(() => setBusy(null));
  }, [completeEmailLink, configured, emailLinkCallbackPending, sessionState]);

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
    try {
      await signIn(provider, intent);
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

  const handleEmailLink = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (
      busy ||
      !configured ||
      (intent === "sign_up" && invitationStatus !== "valid")
    )
      return;
    setBusy("email");
    setError(null);
    setEmailSent(false);
    try {
      await sendEmailLink(email, intent);
      setEmailSent(true);
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

  const handleEmailLinkAccountSwitch = async () => {
    if (busy || emailCallbackStarted.current) return;
    // Claim the callback before logout changes sessionState. Otherwise the
    // unauthenticated callback effect can race this handler and consume the
    // same one-time email link a second time.
    emailCallbackStarted.current = true;
    setBusy("email");
    setError(null);
    let logoutCompleted = false;
    try {
      await logout();
      logoutCompleted = true;
      await completeEmailLink();
    } catch (nextError) {
      if (!logoutCompleted) emailCallbackStarted.current = false;
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

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
                {emailLinkCallbackPending
                  ? authenticated
                    ? "アカウントを切り替えますか？"
                    : "メールリンクを確認しています"
                  : confirmation
                    ? "続行方法の確認"
                    : intent === "sign_in"
                      ? "アカウントにログイン"
                      : "Sumiへようこそ"}
              </h1>
            </div>

            {emailLinkCallbackPending ? (
              authenticated ? (
                <div className="space-y-4">
                  <p className="text-muted-foreground text-sm leading-6">
                    現在のSumiセッションを終了し、メールリンクのアカウントへ切り替えます。自動では切り替わりません。
                  </p>
                  <Button
                    type="button"
                    onClick={() => void handleEmailLinkAccountSwitch()}
                    disabled={busy !== null}
                    className="h-11 w-full rounded-lg"
                  >
                    {busy === "email" && (
                      <LoaderCircle className="size-5 animate-spin" />
                    )}
                    現在のセッションを終了して切り替える
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={rejectEmailLink}
                    disabled={busy !== null}
                    className="h-11 w-full rounded-lg"
                  >
                    現在のアカウントを使い続ける
                  </Button>
                </div>
              ) : (
                <div className="space-y-4 text-center">
                  {sessionState === "unavailable" ? (
                    <p className="text-muted-foreground text-sm leading-6">
                      現在のSumiセッションを確認できないため、メールリンクを処理できません。
                    </p>
                  ) : (
                    <>
                      <LoaderCircle className="mx-auto size-6 animate-spin" />
                      <p className="text-muted-foreground text-sm leading-6">
                        Firebaseのメールリンクを確認しています…
                      </p>
                    </>
                  )}
                  <Button
                    type="button"
                    variant="outline"
                    onClick={rejectEmailLink}
                    disabled={busy !== null}
                    className="h-11 w-full rounded-lg"
                  >
                    メールリンクをキャンセル
                  </Button>
                </div>
              )
            ) : confirmation ? (
              <div className="space-y-4">
                <p className="rounded-lg bg-muted px-3 py-2.5 text-sm">
                  対象アカウント: {confirmationAccountLabel(confirmation)}
                </p>
                <p className="text-muted-foreground text-sm leading-6">
                  {confirmation.action === "create_account"
                    ? "ログインを選択しましたが、この認証情報に対応するSumiアカウントはまだありません。新規登録して続けますか？"
                    : "新規登録を選択しましたが、この認証情報は既存のSumiアカウントに登録されています。ログインして続けますか？"}
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
                  {confirmation.action === "create_account"
                    ? "新規登録して続ける"
                    : "ログインして続ける"}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => {
                    setBusy("cancel");
                    void cancelIntentTransition().finally(() => setBusy(null));
                  }}
                  disabled={busy !== null}
                  className="h-11 w-full rounded-lg"
                >
                  キャンセル
                </Button>
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
                <form onSubmit={handleEmailLink} className="space-y-3">
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
                {emailSent && (
                  <p
                    role="status"
                    className="mt-4 rounded-lg bg-emerald-50 px-3 py-2.5 text-emerald-800 text-sm dark:bg-emerald-950/30 dark:text-emerald-200"
                  >
                    ログインリンクを送信しました。このブラウザでメールを開いてください。
                  </p>
                )}
                {credentialRecoveryEmailSent && (
                  <p
                    role="status"
                    className="mt-4 rounded-lg bg-amber-50 px-3 py-2.5 text-amber-800 text-sm dark:bg-amber-950/30 dark:text-amber-200"
                  >
                    既存アカウントを確認するメールリンクを送信しました。このブラウザでリンクを開くと、ログイン後に選択した方法を追加します。
                  </p>
                )}
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
            {error && (
              <p
                role="alert"
                className="mt-4 rounded-lg bg-red-50 px-3 py-2.5 text-red-700 text-sm dark:bg-red-950/30 dark:text-red-300"
              >
                {error}
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

function confirmationAccountLabel({
  account,
  firebaseUID,
}: {
  account: { displayName: string | null; email: string | null };
  firebaseUID: string;
}): string {
  return account.email ?? account.displayName ?? `Firebase ${firebaseUID}`;
}
