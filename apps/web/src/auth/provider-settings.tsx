import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@sumi/ui/components/alert-dialog";
import { Button } from "@sumi/ui/components/button";
import { FirebaseError } from "firebase/app";
import {
  GithubAuthProvider,
  GoogleAuthProvider,
  getIdToken,
  getIdTokenResult,
  linkWithPopup,
  onAuthStateChanged,
  reauthenticateWithPopup,
  reload,
  type User,
} from "firebase/auth";
import { Check, Link2, Unlink } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { createAuthFlowNonce } from "./auth-flow-client";
import { getFirebaseAuth } from "./firebase";
import {
  completeProviderOperation,
  failProviderOperation,
  type ManagedProvider,
  type ProviderOperationResult,
  startProviderOperation,
  statusProviderOperation,
} from "./provider-operation-client";
import { AuthAPIError } from "./session-client";

const PROVIDERS: Array<{ id: ManagedProvider; label: string }> = [
  { id: "google.com", label: "Google" },
  { id: "github.com", label: "GitHub" },
];
const PROVIDER_NOTICE_KEY = "sumi.auth.provider-notice.v1";

interface ProviderNotice {
  provider: ManagedProvider;
  operation: "linked" | "unlinked";
}

export function ProviderSettings() {
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);
  const [busyProvider, setBusyProvider] = useState<ManagedProvider | null>(
    null,
  );
  const [unlinkTarget, setUnlinkTarget] = useState<ManagedProvider | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<ProviderNotice | null>(() =>
    loadProviderNotice(),
  );

  useEffect(() => {
    try {
      const auth = getFirebaseAuth();
      setFirebaseUser(auth.currentUser);
      return onAuthStateChanged(auth, setFirebaseUser);
    } catch {
      setFirebaseUser(null);
    }
  }, []);

  const linkedProviders = useMemo(
    () =>
      new Set(
        firebaseUser?.providerData.map(({ providerId }) => providerId) ?? [],
      ),
    [firebaseUser],
  );
  const usableMethodCount = linkedProviders.size;

  const refreshUser = useCallback(async (user: User) => {
    await reload(user);
    setFirebaseUser(getFirebaseAuth().currentUser);
  }, []);

  const publishNotice = useCallback((nextNotice: ProviderNotice) => {
    sessionStorage.setItem(PROVIDER_NOTICE_KEY, JSON.stringify(nextNotice));
    setNotice(nextNotice);
  }, []);

  const linkProvider = useCallback(
    async (provider: ManagedProvider) => {
      if (!firebaseUser || busyProvider) return;
      setBusyProvider(provider);
      setError(null);
      const nonce = createAuthFlowNonce();
      let operationId: string | null = null;
      let firebaseLinked = false;
      const startPromise = getIdToken(firebaseUser, true).then((idToken) =>
        startProviderOperation({
          provider,
          operation: "link",
          nonce,
          idToken,
        }),
      );
      // Open the popup in the original click task. Awaiting the ID token or
      // server operation first lets browsers classify it as unsolicited.
      const popupPromise = linkWithPopup(
        firebaseUser,
        createFirebaseProvider(provider),
      ).then(
        (linked) => ({ linked, error: null }),
        (popupError: unknown) => ({ linked: null, error: popupError }),
      );
      try {
        const started = await startPromise;
        operationId = started.operationId;
        if (
          started.outcome !== "client_operation_required" ||
          started.clientOperation !== "firebase_link_with_credential" ||
          !started.completionTokenNotBefore
        ) {
          throw new Error("Invalid provider link response.");
        }
        const popupResult = await popupPromise;
        if (popupResult.error) throw popupResult.error;
        const linked = popupResult.linked;
        if (!linked) throw new Error("Provider popup returned no account.");
        firebaseLinked = true;
        await waitUntil(started.completionTokenNotBefore);
        let completed: ProviderOperationResult;
        try {
          completed = await completeProviderOperation({
            operationId: started.operationId,
            nonce,
            idToken: await getIdToken(linked.user, true),
          });
        } catch (completionError) {
          completed = await statusProviderOperation({
            operationId: started.operationId,
            nonce,
          }).catch(() => {
            throw completionError;
          });
        }
        if (
          completed.outcome !== "provider_linked" &&
          completed.outcome !== "provider_already_linked"
        ) {
          throw new Error("Invalid provider link completion.");
        }
        await refreshUser(linked.user);
        if (completed.noticeRequired) {
          publishNotice({ provider, operation: "linked" });
        }
      } catch (nextError) {
        if (!operationId) {
          const started = await startPromise.catch(() => null);
          operationId = started?.operationId ?? null;
        }
        if (operationId && !firebaseLinked) {
          await failProviderOperation({
            operationId,
            nonce,
            outcome: providerFailureOutcome(nextError),
          }).catch(() => undefined);
        }
        setError(providerSettingsError(nextError));
      } finally {
        setBusyProvider(null);
      }
    },
    [busyProvider, firebaseUser, publishNotice, refreshUser],
  );

  const unlinkProvider = useCallback(
    async (provider: ManagedProvider) => {
      if (!firebaseUser || busyProvider) return;
      setBusyProvider(provider);
      setError(null);
      try {
        const result = await startProviderOperation({
          provider,
          operation: "unlink",
          nonce: createAuthFlowNonce(),
          idToken: await reauthenticateForUnlink(firebaseUser, provider),
        });
        if (result.outcome !== "provider_unlinked") {
          throw new Error("Invalid provider unlink response.");
        }
        await refreshUser(firebaseUser);
        if (result.noticeRequired) {
          publishNotice({ provider, operation: "unlinked" });
        }
        setUnlinkTarget(null);
      } catch (nextError) {
        setError(providerSettingsError(nextError));
      } finally {
        setBusyProvider(null);
      }
    },
    [busyProvider, firebaseUser, publishNotice, refreshUser],
  );

  const dismissNotice = () => {
    sessionStorage.removeItem(PROVIDER_NOTICE_KEY);
    setNotice(null);
  };

  return (
    <div className="border-border border-t px-2.5 py-2">
      <p className="mb-1.5 font-medium text-xs">ログイン方法</p>
      {notice && (
        <div
          role="status"
          className="mb-2 flex max-w-60 items-start gap-2 rounded-lg bg-emerald-50 px-2 py-1.5 text-emerald-800 text-xs"
        >
          <Check className="mt-0.5 size-3.5 shrink-0" />
          <span className="flex-1">
            {providerLabel(notice.provider)}を
            {notice.operation === "linked"
              ? "追加しました。"
              : "解除しました。"}
          </span>
          <button
            type="button"
            onClick={dismissNotice}
            aria-label="通知を閉じる"
          >
            ×
          </button>
        </div>
      )}
      {!firebaseUser ? (
        <p className="max-w-60 text-muted-foreground text-xs leading-5">
          ログイン方法を管理するには、いったんログアウトして再ログインしてください。
        </p>
      ) : (
        <div className="space-y-1">
          {linkedProviders.has("password") && (
            <ProviderRow label="メールリンク" linked />
          )}
          {PROVIDERS.map(({ id, label }) => {
            const linked = linkedProviders.has(id);
            const lastMethod = linked && usableMethodCount <= 1;
            return (
              <ProviderRow
                key={id}
                label={label}
                linked={linked}
                action={
                  linked ? (
                    <Button
                      size="xs"
                      variant="destructive"
                      disabled={busyProvider !== null || lastMethod}
                      onClick={() => setUnlinkTarget(id)}
                      aria-label={`${label}を解除`}
                    >
                      <Unlink />
                      解除
                    </Button>
                  ) : (
                    <Button
                      size="xs"
                      variant="outline"
                      disabled={busyProvider !== null}
                      onClick={() => void linkProvider(id)}
                      aria-label={`${label}を追加`}
                    >
                      <Link2 />
                      {busyProvider === id ? "処理中" : "追加"}
                    </Button>
                  )
                }
              />
            );
          })}
          {usableMethodCount <= 1 && (
            <p className="max-w-60 text-muted-foreground text-xs leading-5">
              最後のログイン方法は解除できません。先に別の方法を追加してください。
            </p>
          )}
        </div>
      )}
      {error && (
        <p
          role="alert"
          className="mt-2 max-w-60 text-red-600 text-xs leading-5"
        >
          {error}
        </p>
      )}
      <AlertDialog
        open={unlinkTarget !== null}
        onOpenChange={(open) => !open && setUnlinkTarget(null)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {unlinkTarget ? providerLabel(unlinkTarget) : "プロバイダー"}
              を解除しますか？
            </AlertDialogTitle>
            <AlertDialogDescription>
              解除すると、この方法ではログインできなくなります。別のリンク済み方法で再認証してから解除します。最後のログイン方法は解除できません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={busyProvider !== null}>
              キャンセル
            </AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              disabled={!unlinkTarget || busyProvider !== null}
              onClick={() => unlinkTarget && void unlinkProvider(unlinkTarget)}
            >
              再認証して解除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function ProviderRow({
  label,
  linked,
  action,
}: {
  label: string;
  linked: boolean;
  action?: React.ReactNode;
}) {
  return (
    <div className="flex min-h-8 items-center gap-2 text-xs">
      <span className="flex-1">{label}</span>
      <span className="text-muted-foreground">
        {linked ? "リンク済み" : "未追加"}
      </span>
      {action}
    </div>
  );
}

async function reauthenticateForUnlink(
  user: User,
  target: ManagedProvider,
): Promise<string> {
  const alternate = PROVIDERS.find(
    ({ id }) =>
      id !== target &&
      user.providerData.some(({ providerId }) => providerId === id),
  );
  if (alternate) {
    await reauthenticateWithPopup(user, createFirebaseProvider(alternate.id));
    return getIdToken(user, true);
  }
  const token = await getIdTokenResult(user, true);
  const firebaseClaims = token.claims.firebase;
  const signInProvider =
    isObject(firebaseClaims) &&
    typeof firebaseClaims.sign_in_provider === "string"
      ? firebaseClaims.sign_in_provider
      : "";
  const authTime = token.claims.auth_time;
  const recentlyAuthenticated =
    typeof authTime === "number" &&
    Date.now() / 1000 - authTime >= -60 &&
    Date.now() / 1000 - authTime <= 240;
  if (
    signInProvider === "password" &&
    recentlyAuthenticated &&
    user.providerData.some(({ providerId }) => providerId === "password")
  ) {
    return token.token;
  }
  throw new Error(
    "別のログイン方法で再認証できません。ログアウトし、メールリンクで再ログインしてから5分以内にもう一度お試しください。",
  );
}

function createFirebaseProvider(provider: ManagedProvider) {
  if (provider === "github.com") return new GithubAuthProvider();
  const google = new GoogleAuthProvider();
  google.setCustomParameters({ prompt: "select_account" });
  return google;
}

async function waitUntil(timestamp: string): Promise<void> {
  const delay = Date.parse(timestamp) - Date.now() + 50;
  if (delay > 10_000) throw new Error("Provider completion window is invalid.");
  if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
}

function providerFailureOutcome(error: unknown) {
  if (error instanceof FirebaseError) {
    if (error.code === "auth/credential-already-in-use") {
      return "credential_in_use" as const;
    }
    if (error.code === "auth/popup-closed-by-user") return "cancelled" as const;
  }
  return "firebase_operation_failed" as const;
}

function providerSettingsError(error: unknown): string {
  if (error instanceof AuthAPIError) {
    switch (error.message) {
      case "recent_reauth_required":
        return "別のログイン方法で再認証してから、5分以内にもう一度お試しください。";
      case "last_login_method":
        return "最後のログイン方法は解除できません。先に別の方法を追加してください。";
      case "provider_operation_pending":
        return "別のログイン方法の変更が処理中です。少し待ってからお試しください。";
      case "provider_unavailable":
        return "ログイン方法を変更できませんでした。時間をおいて再試行してください。";
      case "proof_mismatch":
        return "再認証を確認できませんでした。もう一度お試しください。";
    }
  }
  if (error instanceof FirebaseError) {
    if (error.code === "auth/popup-closed-by-user") {
      return "再認証がキャンセルされました。";
    }
    if (error.code === "auth/credential-already-in-use") {
      return "このログイン方法は別のアカウントで使用されています。";
    }
    if (error.code === "auth/provider-already-linked") {
      return "このログイン方法はすでに追加されています。";
    }
  }
  return error instanceof Error && error.message
    ? error.message
    : "ログイン方法を変更できませんでした。";
}

function providerLabel(provider: ManagedProvider): string {
  return provider === "github.com" ? "GitHub" : "Google";
}

function loadProviderNotice(): ProviderNotice | null {
  try {
    const value: unknown = JSON.parse(
      sessionStorage.getItem(PROVIDER_NOTICE_KEY) ?? "null",
    );
    if (
      isObject(value) &&
      (value.provider === "google.com" || value.provider === "github.com") &&
      (value.operation === "linked" || value.operation === "unlinked")
    ) {
      return { provider: value.provider, operation: value.operation };
    }
  } catch {
    // Ignore malformed session-local presentation state.
  }
  return null;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
