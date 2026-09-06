import { Button } from "@sumi/ui/components/button";
import { PortalContainerBoundary } from "@sumi/ui/components/portal-container";
import {
  Activity,
  type ReactNode,
  useLayoutEffect,
  useReducer,
  useRef,
} from "react";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  suspendMessagingTransport,
} from "../messaging/store";
import { useParticipantApps } from "../participant/app-store";
import {
  bindWorkspaceSessionIdentity,
  getWorkspaceSessionIdentity,
  getWorkspaceSessionScopeKey,
} from "../workspace/store";
import { AuthContext, type AuthContextValue, useAuth } from "./auth-context";
import { LoginScreen } from "./login-screen";

export function AuthGate({ children }: { children: ReactNode }) {
  const auth = useAuth();
  const retainedAuth = useRef<AuthContextValue | null>(null);
  const {
    sessionSuspended,
    emailLinkCallbackPending,
    loading,
    sessionState,
    refreshSession,
  } = auth;
  // Hidden children keep their last verified context: an inner auth gate must
  // not replace a form while Activity has paused its effects.
  const protectedAuth = sessionSuspended ? retainedAuth.current : auth;
  const { authorityBindingId, canUseDirectChat, user } = protectedAuth ?? auth;
  const identity = canUseDirectChat
    ? (user?.id ?? (sessionState === "preissued" ? "preissued" : null))
    : null;
  const workspaceScopeKey = canUseDirectChat
    ? sessionState === "preissued"
      ? "preissued"
      : (authorityBindingId ?? null)
    : null;
  const sessionScopeReady =
    !canUseDirectChat || (identity !== null && workspaceScopeKey !== null);
  const identityMatches =
    getMessagingSessionIdentity() === identity &&
    getWorkspaceSessionIdentity() === identity &&
    getWorkspaceSessionScopeKey() === workspaceScopeKey;
  const [, rerender] = useReducer((value: number) => value + 1, 0);

  useLayoutEffect(() => {
    if (sessionSuspended) {
      suspendMessagingTransport();
      return;
    }
    if (identityMatches) return;
    bindWorkspaceSessionIdentity(identity, workspaceScopeKey);
    bindMessagingSessionIdentity(identity);
    if (identity === null) {
      void useParticipantApps.getState().bindParticipant(null);
    }
    rerender();
  }, [identity, identityMatches, sessionSuspended, workspaceScopeKey]);

  useLayoutEffect(() => {
    if (!sessionSuspended)
      retainedAuth.current = canUseDirectChat ? auth : null;
  }, [auth, canUseDirectChat, sessionSuspended]);

  if (emailLinkCallbackPending) {
    return <LoginScreen />;
  }
  const ready = canUseDirectChat && sessionScopeReady && identityMatches;
  let status: ReactNode = null;
  if (loading || sessionState === "checking") {
    status = <AuthStatus title="ログイン状態を確認しています…" />;
  } else if (canUseDirectChat && !sessionSuspended && !ready) {
    status = <AuthStatus title="セッションを切り替えています…" />;
  } else if (sessionState === "unauthenticated") {
    status = <LoginScreen />;
  } else if (!ready || sessionSuspended) {
    status = (
      <AuthStatus
        title="Sumiに接続できません"
        detail={
          sessionSuspended
            ? "ログアウトの結果を確認できませんでした。編集中の内容を保持して、操作を一時停止しています。"
            : "ログイン状態を確認できませんでした。"
        }
        action={
          <Button type="button" onClick={() => void refreshSession()}>
            再試行
          </Button>
        }
      />
    );
  }
  return (
    <>
      {canUseDirectChat && protectedAuth && (
        <AuthContext value={protectedAuth}>
          <Activity
            key={identity}
            mode={ready && !sessionSuspended ? "visible" : "hidden"}
          >
            <PortalContainerBoundary>{children}</PortalContainerBoundary>
          </Activity>
        </AuthContext>
      )}
      {status}
    </>
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
