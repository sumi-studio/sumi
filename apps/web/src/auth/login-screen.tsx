import { Button } from "@sumi/ui/components/button";
import { LoaderCircle } from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { FaGithub } from "react-icons/fa";
import { FcGoogle } from "react-icons/fc";
import { type SignInProvider, useAuth } from "./auth-context";
import { getAuthErrorMessage } from "./auth-errors";
import type { AuthIntent } from "./auth-flow-client";
import { hasEmailLinkCallback } from "./email-link-auth";

const providers: Array<{ id: SignInProvider; label: string }> = [
  { id: "google", label: "Googleで続ける" },
  { id: "github", label: "GitHubで続ける" },
];

export function LoginScreen() {
  const {
    cancelIntentTransition,
    completeEmailLink,
    confirmation,
    configured,
    confirmIntentTransition,
    sendEmailLink,
    signIn,
  } = useAuth();
  const [intent, setIntent] = useState<AuthIntent>("sign_in");
  const [busy, setBusy] = useState<
    SignInProvider | "email" | "confirm" | "cancel" | null
  >(null);
  const [error, setError] = useState<string | null>(null);
  const [email, setEmail] = useState("");
  const [emailSent, setEmailSent] = useState(false);
  const emailCallbackStarted = useRef(false);

  useEffect(() => {
    if (
      emailCallbackStarted.current ||
      !configured ||
      !hasEmailLinkCallback()
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
  }, [completeEmailLink, configured]);

  const handleSignIn = async (provider: SignInProvider) => {
    if (busy || !configured) {
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
    if (busy || !configured) return;
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
                {confirmation
                  ? "続行方法の確認"
                  : intent === "sign_in"
                    ? "アカウントにログイン"
                    : "アカウントを新規登録"}
              </h1>
            </div>

            {confirmation ? (
              <div className="space-y-4">
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
                <div className="mb-4 grid grid-cols-2 rounded-lg bg-muted p-1">
                  {(
                    [
                      ["sign_in", "ログイン"],
                      ["sign_up", "新規登録"],
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
