import { Button } from "@sumi/ui/components/button";
import type { ReactNode } from "react";
import { useAuth } from "./auth-context";
import { LoginScreen } from "./login-screen";

export function AuthGate({ children }: { children: ReactNode }) {
  const {
    canUseDirectChat,
    emailLinkCallbackPending,
    loading,
    sessionState,
    refreshSession,
  } = useAuth();

  if (emailLinkCallbackPending) {
    return <LoginScreen />;
  }
  if (canUseDirectChat) {
    return children;
  }
  if (loading || sessionState === "checking") {
    return <AuthStatus title="ログイン状態を確認しています…" />;
  }
  if (sessionState === "unauthenticated") {
    return <LoginScreen />;
  }
  return (
    <AuthStatus
      title="Sumiに接続できません"
      detail="ログイン状態を確認できませんでした。"
      action={
        <Button type="button" onClick={() => void refreshSession()}>
          再試行
        </Button>
      }
    />
  );
}

function AuthStatus({
  title,
  detail,
  action,
}: {
  title: string;
  detail?: string;
  action?: ReactNode;
}) {
  return (
    <main className="grid min-h-dvh place-items-center bg-background px-5 text-foreground">
      <section
        aria-live="polite"
        className="flex max-w-sm flex-col items-center gap-3 text-center"
      >
        <h1 className="font-semibold text-xl">{title}</h1>
        {detail && (
          <p className="text-muted-foreground text-sm leading-6">{detail}</p>
        )}
        {action}
      </section>
    </main>
  );
}
