import { useState } from "react";
import { type SignInProvider, useAuth } from "./auth-context";
import { getAuthErrorMessage } from "./auth-errors";

const providers: Array<{ id: SignInProvider; label: string; mark: string }> = [
  { id: "google", label: "Googleで続ける", mark: "G" },
  { id: "github", label: "GitHubで続ける", mark: "GH" },
];

export function LoginScreen() {
  const { configured, signIn } = useAuth();
  const [busy, setBusy] = useState<SignInProvider | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleSignIn = async (provider: SignInProvider) => {
    if (busy || !configured) {
      return;
    }
    setBusy(provider);
    setError(null);
    try {
      await signIn(provider);
    } catch (nextError) {
      setError(getAuthErrorMessage(nextError));
    } finally {
      setBusy(null);
    }
  };

  return (
    <main className="fixed inset-0 z-50 grid min-h-dvh place-items-center overflow-y-auto bg-neutral-50 px-5 text-foreground dark:bg-background">
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
              アカウントにログイン
            </h1>
          </div>

          <div className="space-y-3">
            {providers.map((provider) => (
              <button
                key={provider.id}
                type="button"
                onClick={() => void handleSignIn(provider.id)}
                disabled={busy !== null || !configured}
                className="flex h-11 w-full items-center justify-center gap-2.5 rounded-lg border bg-background text-sm disabled:cursor-not-allowed disabled:opacity-50"
              >
                <span
                  aria-hidden="true"
                  className="grid min-w-5 place-items-center font-semibold text-xs"
                >
                  {busy === provider.id ? "…" : provider.mark}
                </span>
                {provider.label}
              </button>
            ))}
          </div>

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
      </section>
    </main>
  );
}
