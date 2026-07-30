import { Button } from "@sumi/ui/components/button";
import { LoaderCircle } from "lucide-react";
import { useState } from "react";
import { FaGithub } from "react-icons/fa";
import { FcGoogle } from "react-icons/fc";
import { type SignInProvider, useAuth } from "./auth-context";
import { getAuthErrorMessage } from "./auth-errors";

const providers: Array<{ id: SignInProvider; label: string }> = [
  { id: "google", label: "Googleで続ける" },
  { id: "github", label: "GitHubで続ける" },
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
                アカウントにログイン
              </h1>
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
