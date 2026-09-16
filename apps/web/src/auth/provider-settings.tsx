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
  getRedirectResult,
  linkWithRedirect,
  onAuthStateChanged,
  reauthenticateWithRedirect,
  reload,
  type User,
  updateCurrentUser,
} from "firebase/auth";
import {
  Check,
  CircleAlert,
  Link2,
  LoaderCircle,
  RotateCcw,
  Unlink,
} from "lucide-react";
import {
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createAuthFlowNonce } from "./auth-flow-client";
import { hasPendingRedirectFlowRecord } from "./auth-flow-state";
import { getFirebaseAuth } from "./firebase";
import {
  completeProviderOperation,
  failProviderOperation,
  type ManagedProvider,
  type ProviderOperation,
  type ProviderOperationResult,
  startProviderOperation,
  statusProviderOperation,
} from "./provider-operation-client";
import {
  clearPendingProviderRedirect,
  type PendingProviderRedirect,
  peekPendingProviderRedirect,
  savePendingProviderRedirect,
  takePendingProviderRedirect,
} from "./provider-redirect";
import { AuthAPIError } from "./session-client";

const PROVIDERS: Array<{ id: ManagedProvider; label: string }> = [
  { id: "google.com", label: "Google" },
  { id: "github.com", label: "GitHub" },
];
const PROVIDER_NOTICE_KEY = "sumi.auth.provider-notice.v1";
const PROVIDER_PENDING_KEY = "sumi.auth.provider-pending.v1";
const RECOVERY_ATTEMPTS = 3;

interface ProviderScope {
  firebaseUid: string;
  humanId: string;
}

interface ProviderNotice extends ProviderScope {
  version: 1;
  provider: ManagedProvider;
  operation: "linked" | "unlinked";
}

type PendingPhase =
  | "starting"
  | "link_ready"
  | "link_mutated"
  | "unlink_starting";

interface PendingProviderOperation extends ProviderScope {
  version: 1;
  provider: ManagedProvider;
  operation: ProviderOperation;
  nonce: string;
  phase: PendingPhase;
  operationId?: string;
  completionTokenNotBefore?: string;
}

export function ProviderSettings({ humanId }: { humanId: string }) {
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);
  const [providerRevision, setProviderRevision] = useState(0);
  const [busyProvider, setBusyProvider] = useState<ManagedProvider | null>(
    null,
  );
  const [busyLabel, setBusyLabel] = useState<string | null>(null);
  const [unlinkTarget, setUnlinkTarget] = useState<ManagedProvider | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<ProviderNotice | null>(null);
  const [pendingOperation, setPendingOperation] =
    useState<PendingProviderOperation | null>(null);
  // A redirect return is consumed once; this fences StrictMode's repeated
  // mount effects and the pageshow restore path.
  const resolvingRedirect = useRef(false);

  useEffect(() => {
    try {
      const auth = getFirebaseAuth();
      const observe = (nextUser: User | null) => {
        setFirebaseUser(nextUser);
        if (!nextUser) {
          clearProviderSessionState();
          setNotice(null);
          setPendingOperation(null);
        }
      };
      // Firebase may expose a synchronous null currentUser while restoring its
      // persisted session. The first auth-state emission is authoritative;
      // clearing scoped recovery state before it would abandon valid work.
      return onAuthStateChanged(auth, observe);
    } catch {
      setFirebaseUser(null);
    }
  }, []);

  useEffect(() => {
    if (!firebaseUser || !humanId) {
      setNotice(null);
      setPendingOperation(null);
      return;
    }
    const scope = { firebaseUid: firebaseUser.uid, humanId };
    setNotice(loadScopedNotice(scope));
    setPendingOperation(loadScopedPendingOperation(scope));
  }, [firebaseUser, humanId]);

  const linkedProviders = useMemo(() => {
    // Firebase reload mutates the existing User object, so this revision is
    // the explicit signal to read providerData again.
    void providerRevision;
    return new Set(
      firebaseUser?.providerData.map(({ providerId }) => providerId) ?? [],
    );
  }, [firebaseUser, providerRevision]);
  // A verified address signs in with a Sumi email code even without the
  // Firebase password provider. The server still owns the last-method check.
  const emailMethod = Boolean(
    linkedProviders.has("password") ||
      (firebaseUser?.email && firebaseUser.emailVerified),
  );
  const usableMethodCount =
    linkedProviders.size +
    (emailMethod && !linkedProviders.has("password") ? 1 : 0);

  const scope = useMemo<ProviderScope | null>(
    () =>
      firebaseUser && humanId
        ? { firebaseUid: firebaseUser.uid, humanId }
        : null,
    [firebaseUser, humanId],
  );
  const activeScopeRef = useRef<ProviderScope | null>(scope);
  activeScopeRef.current = scope;
  const scopedNotice =
    notice && scope && sameScope(notice, scope) ? notice : null;
  const scopedPendingOperation =
    pendingOperation && scope && sameScope(pendingOperation, scope)
      ? pendingOperation
      : null;

  const refreshUser = useCallback(async (user: User) => {
    await reload(user);
    setFirebaseUser(getFirebaseAuth().currentUser);
    setProviderRevision((revision) => revision + 1);
  }, []);

  const persistPending = useCallback((next: PendingProviderOperation) => {
    if (!activeScopeRef.current || !sameScope(next, activeScopeRef.current)) {
      throw new ProviderAccountChangedError();
    }
    // The operation record is durable: a PWA restart during the provider
    // redirect drops sessionStorage but must not orphan the server-side
    // pending operation. The pre-redirect sessionStorage key is retired.
    localStorage.setItem(PROVIDER_PENDING_KEY, JSON.stringify(next));
    sessionStorage.removeItem(PROVIDER_PENDING_KEY);
    setPendingOperation(next);
  }, []);

  const clearPending = useCallback((expected: ProviderScope) => {
    if (
      !activeScopeRef.current ||
      !sameScope(expected, activeScopeRef.current)
    ) {
      return;
    }
    const stored = readPendingJSON();
    if (hasScope(stored) && !sameScope(stored, expected)) return;
    localStorage.removeItem(PROVIDER_PENDING_KEY);
    sessionStorage.removeItem(PROVIDER_PENDING_KEY);
    setPendingOperation((current) =>
      current && sameScope(current, expected) ? null : current,
    );
  }, []);

  const publishNotice = useCallback(
    (provider: ManagedProvider, operation: "linked" | "unlinked") => {
      if (!scope) return;
      if (
        !activeScopeRef.current ||
        !sameScope(scope, activeScopeRef.current)
      ) {
        return;
      }
      const nextNotice: ProviderNotice = {
        version: 1,
        ...scope,
        provider,
        operation,
      };
      sessionStorage.setItem(PROVIDER_NOTICE_KEY, JSON.stringify(nextNotice));
      setNotice(nextNotice);
    },
    [scope],
  );

  const operationFor = useCallback(
    (provider: ManagedProvider, operation: ProviderOperation) => {
      if (!scope) throw new Error("ログイン情報を確認できませんでした。");
      if (scopedPendingOperation) {
        if (
          scopedPendingOperation.provider !== provider ||
          scopedPendingOperation.operation !== operation
        ) {
          throw new ProviderOperationStillPendingError(
            "別のログイン方法の変更が保留中です。先にその変更を再開してください。",
          );
        }
        return scopedPendingOperation;
      }
      const next: PendingProviderOperation = {
        version: 1,
        ...scope,
        provider,
        operation,
        nonce: createAuthFlowNonce(),
        phase: operation === "link" ? "starting" : "unlink_starting",
      };
      persistPending(next);
      return next;
    },
    [persistPending, scope, scopedPendingOperation],
  );

  /**
   * Leaves this tab for the provider. The receipt binds the navigation to the
   * pending operation's nonce so the return can only ever resume this exact
   * change for this exact account. The promise normally never settles: the
   * browser navigates away first.
   */
  const sendProviderRedirect = useCallback(
    async (
      operation: PendingProviderOperation,
      provider: ManagedProvider,
      kind: PendingProviderRedirect["kind"],
      user: User,
    ) => {
      const marker: PendingProviderRedirect = {
        version: 1,
        kind,
        provider,
        nonce: operation.nonce,
        firebaseUid: operation.firebaseUid,
        humanId: operation.humanId,
        sentAt: new Date().toISOString(),
      };
      if (!savePendingProviderRedirect(marker)) {
        throw new AuthAPIError(
          "Redirect state could not be stored in this browser.",
          0,
        );
      }
      try {
        if (kind === "link") {
          await linkWithRedirect(user, createFirebaseProvider(provider));
        } else {
          await reauthenticateWithRedirect(
            user,
            createFirebaseProvider(provider),
          );
        }
      } catch (redirectError) {
        clearPendingProviderRedirect();
        throw redirectError;
      }
      // A resolved promise without a navigation means the redirect was never
      // sent; drop the receipt so no later mount mistakes it for a return.
      clearPendingProviderRedirect();
      throw new ProviderRedirectNotSentError();
    },
    [],
  );

  /**
   * Runs the backend-owned unlink once a fresh reauthentication proof exists.
   * The nonce makes the start replayable: a return that already committed —
   * or an app reload after the mutation — reconciles to the same result
   * instead of deleting twice.
   */
  const finishUnlinkOperation = useCallback(
    async (
      operation: PendingProviderOperation,
      user: User,
      idToken: string,
    ) => {
      const result = await startWithSameNonce(
        operation,
        idToken,
        persistPending,
      );
      if (result.outcome !== "provider_unlinked") {
        throw resultStateError(result);
      }
      await confirmTerminalStatus(result, operation);
      await refreshUser(user);
      // Firebase reload() merges providerData and can never observe a
      // provider the backend deleted; the server's provider_unlinked outcome
      // is authoritative. Reconcile and persist the local user so the removed
      // method does not survive an app restart.
      Object.assign(user, {
        providerData: user.providerData.filter(
          (entry) => entry.providerId !== operation.provider,
        ),
      });
      await updateCurrentUser(getFirebaseAuth(), user);
      clearPending(operation);
      if (result.noticeRequired) {
        publishNotice(operation.provider, "unlinked");
      }
      setUnlinkTarget(null);
    },
    [clearPending, persistPending, publishNotice, refreshUser],
  );

  /**
   * Completes the Firebase link whose credential just returned, then proves
   * it to the backend with a token minted after the operation began.
   */
  const finishLinkFromRedirect = useCallback(
    async (operation: PendingProviderOperation, user: User) => {
      const mutated: PendingProviderOperation = {
        ...operation,
        phase: "link_mutated",
      };
      persistPending(mutated);
      setBusyLabel(`${providerLabel(operation.provider)}の追加を確定中`);
      const completed = await reconcileLinkCompletion(mutated, user);
      await finishSuccessfulOperation(
        completed,
        mutated,
        user,
        refreshUser,
        clearPending,
        publishNotice,
      );
    },
    [clearPending, persistPending, publishNotice, refreshUser],
  );

  const settleLinkRedirectOutcome = useCallback(
    async (
      operation: PendingProviderOperation,
      outcome: "credential_in_use" | "firebase_operation_failed" | "cancelled",
    ) => {
      const settled = await settleKnownLinkFailure(operation, outcome);
      if (settled) clearPending(operation);
      setError(
        providerSettingsError(new TerminalProviderOperationError(outcome)),
      );
    },
    [clearPending],
  );

  /**
   * Claims this tab's provider-redirect receipt and finishes the pending
   * operation it serves. Called on mount — the settings popover reopens on a
   * return — and on a back/forward-cache restore. The receipt is taken before
   * any async work, so the return is consumed exactly once.
   */
  const resolveReturnedProviderRedirect = useCallback(async () => {
    const currentScope = activeScopeRef.current;
    if (!currentScope || resolvingRedirect.current) return;
    const marker = peekPendingProviderRedirect();
    if (!marker) return;
    const pending = loadScopedPendingOperation(currentScope);
    if (
      !pending ||
      !sameScope(marker, currentScope) ||
      marker.nonce !== pending.nonce ||
      (marker.kind === "link" &&
        (pending.operation !== "link" ||
          pending.provider !== marker.provider)) ||
      (marker.kind === "reauth" && pending.operation !== "unlink")
    ) {
      // The return cannot belong to this account's pending change. The
      // receipt is stale; the operation itself stays resumable under its own
      // scope.
      clearPendingProviderRedirect();
      return;
    }
    if (hasPendingRedirectFlowRecord()) {
      // A sign-in return owns this navigation's Firebase result. Leave the
      // receipt; once that settles, a later mount reconciles this operation.
      return;
    }
    takePendingProviderRedirect();
    resolvingRedirect.current = true;
    // The reauth trip authenticates with an alternate provider, but the busy
    // row and the settled change both belong to the unlink target.
    setBusyProvider(pending.provider);
    setBusyLabel(`${providerLabel(marker.provider)}の認証結果を確認中`);
    setError(null);
    try {
      const user = getFirebaseAuth().currentUser;
      if (!user || user.uid !== currentScope.firebaseUid) {
        // The redirect result is bound to the account that sent it; reading
        // it under a different Firebase user can never be this operation.
        setError("ログイン状態が変わりました。もう一度お試しください。");
        return;
      }
      let credential: Awaited<ReturnType<typeof getRedirectResult>>;
      try {
        credential = await getRedirectResult(getFirebaseAuth());
      } catch (redirectError) {
        if (marker.kind === "link") {
          if (
            redirectError instanceof FirebaseError &&
            redirectError.code === "auth/redirect-operation-pending"
          ) {
            setError(providerSettingsError(redirectError));
            return;
          }
          await settleLinkRedirectOutcome(
            pending,
            providerFailureOutcome(redirectError),
          );
          return;
        }
        // Reauthentication never mutated anything: the unlink stays
        // resumable and the person only sees why it did not run.
        setError(providerSettingsError(redirectError));
        return;
      }
      if (marker.kind === "link") {
        if (credential?.operationType && credential.operationType !== "link") {
          // A result from a different event cannot have applied this link.
          credential = null;
        }
        if (credential) {
          if (credential.user.uid !== currentScope.firebaseUid) {
            await settleLinkRedirectOutcome(
              pending,
              "firebase_operation_failed",
            );
            return;
          }
          await finishLinkFromRedirect(pending, credential.user);
          return;
        }
        // No usable result arrived: the person cancelled or came back early.
        // Another flow may already have consumed a completed link, so only
        // declare the cancel after a live read shows the provider absent.
        await refreshUser(user);
        if (
          getFirebaseAuth().currentUser?.providerData.some(
            ({ providerId }) => providerId === marker.provider,
          )
        ) {
          await finishLinkFromRedirect(pending, user);
          return;
        }
        await settleLinkRedirectOutcome(pending, "cancelled");
        return;
      }
      // Reauthentication for an unlink.
      if (credential && credential.user.uid !== currentScope.firebaseUid) {
        setError(
          "別のアカウントで認証されました。同じアカウントでもう一度お試しください。",
        );
        return;
      }
      // A reauth result — or a proof another flow already applied — counts
      // only when the token itself is fresh enough for the server window.
      const reauthUser =
        credential && credential.user.uid === user.uid ? credential.user : user;
      const idToken = await recentReauthToken(reauthUser, pending.provider);
      if (!idToken) {
        setError("認証をキャンセルしました。");
        return;
      }
      setBusyLabel(`${providerLabel(pending.provider)}の解除を確定中`);
      await finishUnlinkOperation(pending, reauthUser, idToken);
    } catch (nextError) {
      if (nextError instanceof TerminalProviderOperationError) {
        clearPending(pending);
      }
      if (
        activeScopeRef.current &&
        sameScope(pending, activeScopeRef.current)
      ) {
        setError(providerSettingsError(nextError));
      }
    } finally {
      resolvingRedirect.current = false;
      setBusyProvider(null);
      setBusyLabel(null);
    }
  }, [
    clearPending,
    finishLinkFromRedirect,
    finishUnlinkOperation,
    refreshUser,
    settleLinkRedirectOutcome,
  ]);

  // A returning provider redirect is finished on mount: the settings popover
  // reopens for the receipt's Human and this component resumes its own work.
  useEffect(() => {
    if (!firebaseUser || !humanId) return;
    void resolveReturnedProviderRedirect();
  }, [firebaseUser, humanId, resolveReturnedProviderRedirect]);

  // A back/forward-cache restore revives this page after the tab came back
  // from the provider without reloading. The receipt still names the return.
  useEffect(() => {
    const onPageShow = (event: PageTransitionEvent) => {
      if (event.persisted) void resolveReturnedProviderRedirect();
    };
    window.addEventListener("pageshow", onPageShow);
    return () => window.removeEventListener("pageshow", onPageShow);
  }, [resolveReturnedProviderRedirect]);

  const prepareLinkProvider = useCallback(
    async (provider: ManagedProvider) => {
      if (!firebaseUser || busyProvider) return;
      setBusyProvider(provider);
      setBusyLabel(`${providerLabel(provider)}の追加を準備中`);
      setError(null);
      let operation: PendingProviderOperation | null = null;
      try {
        operation = operationFor(provider, "link");
        const started = await startWithSameNonce(
          operation,
          await getIdToken(firebaseUser, true),
          persistPending,
        );
        if (isSuccessfulLink(started)) {
          await finishSuccessfulOperation(
            started,
            operation,
            firebaseUser,
            refreshUser,
            clearPending,
            publishNotice,
          );
          return;
        }
        assertLinkReady(started);
        operation = {
          ...operation,
          operationId: started.operationId,
          completionTokenNotBefore: started.completionTokenNotBefore,
          phase: linkedProviders.has(provider) ? "link_mutated" : "link_ready",
        };
        persistPending(operation);
        if (operation.phase === "link_mutated") {
          setBusyLabel(`${providerLabel(provider)}の追加結果を確認中`);
          const completed = await reconcileLinkCompletion(
            operation,
            firebaseUser,
          );
          await finishSuccessfulOperation(
            completed,
            operation,
            firebaseUser,
            refreshUser,
            clearPending,
            publishNotice,
          );
          return;
        }
        // The durable operation exists. Same-tab redirect needs no gesture,
        // so the provider trip starts now and the return resumes it.
        setBusyLabel(`${providerLabel(provider)}で認証中`);
        await sendProviderRedirect(operation, provider, "link", firebaseUser);
      } catch (nextError) {
        if (isDefinitiveStartFailure(nextError, operation)) {
          if (operation) clearPending(operation);
        } else if (nextError instanceof TerminalProviderOperationError) {
          if (operation) clearPending(operation);
        }
        if (
          !operation ||
          (activeScopeRef.current &&
            sameScope(operation, activeScopeRef.current))
        ) {
          setError(providerSettingsError(nextError));
        }
      } finally {
        setBusyProvider(null);
        setBusyLabel(null);
      }
    },
    [
      busyProvider,
      clearPending,
      firebaseUser,
      linkedProviders,
      operationFor,
      persistPending,
      publishNotice,
      refreshUser,
      sendProviderRedirect,
    ],
  );

  /**
   * Resume affordance for an interrupted link: sends the provider redirect
   * again, or reconciles a mutation that already reached Firebase.
   */
  const continueLinkProvider = useCallback(
    async (provider: ManagedProvider) => {
      if (!firebaseUser || busyProvider) return;
      const operation = scopedPendingOperation;
      if (
        !operation ||
        operation.provider !== provider ||
        operation.operation !== "link" ||
        (operation.phase !== "link_ready" && operation.phase !== "link_mutated")
      ) {
        setError(
          "追加の準備が完了していません。先に追加操作を再開してください。",
        );
        return;
      }
      setBusyProvider(provider);
      setError(null);
      try {
        if (
          operation.phase === "link_mutated" ||
          linkedProviders.has(provider)
        ) {
          setBusyLabel(`${providerLabel(provider)}の追加を確定中`);
          const completed = await reconcileLinkCompletion(
            operation.phase === "link_mutated"
              ? operation
              : { ...operation, phase: "link_mutated" },
            firebaseUser,
          );
          await finishSuccessfulOperation(
            completed,
            operation,
            firebaseUser,
            refreshUser,
            clearPending,
            publishNotice,
          );
          return;
        }
        setBusyLabel(`${providerLabel(provider)}で認証中`);
        await sendProviderRedirect(operation, provider, "link", firebaseUser);
      } catch (nextError) {
        if (nextError instanceof TerminalProviderOperationError) {
          clearPending(operation);
        }
        if (
          activeScopeRef.current &&
          sameScope(operation, activeScopeRef.current)
        ) {
          setError(providerSettingsError(nextError));
        }
      } finally {
        setBusyProvider(null);
        setBusyLabel(null);
      }
    },
    [
      busyProvider,
      clearPending,
      firebaseUser,
      linkedProviders,
      publishNotice,
      refreshUser,
      scopedPendingOperation,
      sendProviderRedirect,
    ],
  );

  const unlinkProvider = useCallback(
    async (provider: ManagedProvider) => {
      if (!firebaseUser || busyProvider) return;
      setBusyProvider(provider);
      setBusyLabel(`${providerLabel(provider)}の解除を再認証中`);
      setError(null);
      let operation: PendingProviderOperation | null = null;
      try {
        operation = operationFor(provider, "unlink");
        // A recent sign-in or a just-finished redirect reauth already proves
        // control; only an older session goes back to the provider.
        const recent = await recentReauthToken(firebaseUser, provider);
        if (recent) {
          setBusyLabel(`${providerLabel(provider)}の解除を確定中`);
          await finishUnlinkOperation(operation, firebaseUser, recent);
          return;
        }
        const alternate = PROVIDERS.find(
          ({ id }) => id !== provider && linkedProviders.has(id),
        );
        if (!alternate) {
          throw new Error(
            "別のログイン方法で再認証できません。ログアウトし、メールの確認コードで再ログインしてから5分以内にもう一度お試しください。",
          );
        }
        await sendProviderRedirect(
          operation,
          alternate.id,
          "reauth",
          firebaseUser,
        );
      } catch (nextError) {
        if (
          isDefinitiveStartFailure(nextError, operation) ||
          nextError instanceof TerminalProviderOperationError
        ) {
          if (operation) clearPending(operation);
        }
        if (
          !operation ||
          (activeScopeRef.current &&
            sameScope(operation, activeScopeRef.current))
        ) {
          setError(providerSettingsError(nextError));
        }
      } finally {
        setBusyProvider(null);
        setBusyLabel(null);
      }
    },
    [
      busyProvider,
      clearPending,
      finishUnlinkOperation,
      firebaseUser,
      linkedProviders,
      operationFor,
      sendProviderRedirect,
    ],
  );

  const dismissNotice = () => {
    sessionStorage.removeItem(PROVIDER_NOTICE_KEY);
    setNotice(null);
  };

  return (
    <section
      className="border-border border-t px-3 py-2"
      aria-label="ログイン方法"
    >
      <div className="flex min-h-11 items-center justify-between gap-3">
        <div>
          <h3 className="font-semibold text-[13px] tracking-tight">
            ログイン方法
          </h3>
          <p className="text-muted-foreground text-[11px] leading-4">
            アカウントへの入口を管理
          </p>
        </div>
        {busyLabel && (
          <div
            role="status"
            className="flex items-center gap-1.5 text-muted-foreground text-[11px]"
          >
            <LoaderCircle
              className="size-3.5 animate-spin"
              aria-hidden="true"
            />
            <span>{busyLabel}</span>
          </div>
        )}
      </div>

      {scopedNotice && (
        <div
          role="status"
          className="flex min-h-11 items-center gap-2 border-border border-t text-emerald-700 text-xs dark:text-emerald-400"
        >
          <Check className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="flex-1">
            {providerLabel(scopedNotice.provider)}を
            {scopedNotice.operation === "linked"
              ? "追加しました"
              : "解除しました"}
          </span>
          <button
            type="button"
            onClick={dismissNotice}
            className="grid size-11 place-items-center text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
            aria-label="通知を閉じる"
          >
            ×
          </button>
        </div>
      )}

      {scopedPendingOperation && !busyProvider && (
        <div
          role="status"
          className="flex min-h-11 items-center gap-2 border-border border-t text-amber-700 text-xs dark:text-amber-400"
        >
          <RotateCcw className="size-3.5 shrink-0" aria-hidden="true" />
          <span className="flex-1">
            {pendingOperationMessage(scopedPendingOperation)}
          </span>
        </div>
      )}

      {!firebaseUser ? (
        <p className="border-border border-t py-3 text-muted-foreground text-xs leading-5">
          ログイン方法を管理するには、ログアウトして再ログインしてください。
        </p>
      ) : (
        <div className="border-border border-t">
          {emailMethod && <ProviderRow label="メール" linked />}
          {PROVIDERS.map(({ id, label }) => {
            const linked = linkedProviders.has(id);
            const lastMethod = linked && usableMethodCount <= 1;
            const pending = scopedPendingOperation?.provider === id;
            const blockedByOtherPending = Boolean(
              scopedPendingOperation && !pending,
            );
            const resumeLink =
              pending && scopedPendingOperation.operation === "link";
            const resumeUnlink =
              pending && scopedPendingOperation.operation === "unlink";
            const linkPhase = resumeLink ? scopedPendingOperation.phase : null;
            const needsProviderTrip = linkPhase === "link_ready" && !linked;
            const confirmsLinkedResult =
              linkPhase === "link_mutated" ||
              (linkPhase === "link_ready" && linked);
            return (
              <ProviderRow
                key={id}
                label={label}
                linked={linked}
                action={
                  resumeLink ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-11 min-w-11 px-2"
                      disabled={busyProvider !== null}
                      onClick={() => void continueLinkProvider(id)}
                      aria-label={
                        needsProviderTrip
                          ? `${label}で認証を続ける`
                          : confirmsLinkedResult
                            ? `${label}の追加結果を確認`
                            : `${label}の追加を再開`
                      }
                    >
                      <RotateCcw className="size-3.5" />
                      {needsProviderTrip
                        ? `${label}で続ける`
                        : confirmsLinkedResult
                          ? "結果を確認"
                          : "再開"}
                    </Button>
                  ) : linked ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-11 min-w-11 px-2 text-destructive hover:text-destructive"
                      disabled={
                        busyProvider !== null ||
                        lastMethod ||
                        blockedByOtherPending
                      }
                      onClick={() => setUnlinkTarget(id)}
                      aria-label={`${label}の解除を${resumeUnlink ? "再開" : "開始"}`}
                    >
                      {resumeUnlink ? (
                        <RotateCcw className="size-3.5" />
                      ) : (
                        <Unlink className="size-3.5" />
                      )}
                      {resumeUnlink ? "再開" : "解除"}
                    </Button>
                  ) : (
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-11 min-w-11 px-2"
                      disabled={busyProvider !== null || blockedByOtherPending}
                      onClick={() => void prepareLinkProvider(id)}
                      aria-label={`${label}を${pending ? "再開" : "追加"}`}
                    >
                      {pending ? (
                        <RotateCcw className="size-3.5" />
                      ) : (
                        <Link2 className="size-3.5" />
                      )}
                      {busyProvider === id
                        ? "処理中"
                        : pending
                          ? "再開"
                          : "追加"}
                    </Button>
                  )
                }
              />
            );
          })}
          {usableMethodCount <= 1 && (
            <p className="border-border border-t py-2 text-muted-foreground text-[11px] leading-4">
              最後のログイン方法は解除できません。先に別の方法を追加してください。
            </p>
          )}
        </div>
      )}

      {error && (
        <div
          role="alert"
          className="flex gap-2 border-border border-t py-2.5 text-destructive text-xs leading-5"
        >
          <CircleAlert
            className="mt-0.5 size-3.5 shrink-0"
            aria-hidden="true"
          />
          <span>{error}</span>
        </div>
      )}

      <AlertDialog
        open={unlinkTarget !== null}
        onOpenChange={(open) => !open && setUnlinkTarget(null)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {unlinkTarget ? providerLabel(unlinkTarget) : "ログイン方法"}
              を解除しますか？
            </AlertDialogTitle>
            <AlertDialogDescription>
              解除後は、この方法でログインできません。別のリンク済み方法で再認証してから解除します。最後のログイン方法は解除できません。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel
              className="min-h-11"
              disabled={busyProvider !== null}
            >
              解除しない
            </AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              className="min-h-11"
              disabled={!unlinkTarget || busyProvider !== null}
              onClick={() => unlinkTarget && void unlinkProvider(unlinkTarget)}
            >
              再認証して解除
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  );
}

function ProviderRow({
  label,
  linked,
  action,
}: {
  label: string;
  linked: boolean;
  action?: ReactNode;
}) {
  return (
    <div className="flex min-h-11 items-center gap-2 border-border border-b text-xs last:border-b-0">
      <span className="flex-1 font-medium text-[13px] tracking-tight">
        {label}
      </span>
      <span className="text-muted-foreground text-[11px]">
        {linked ? "リンク済み" : "未追加"}
      </span>
      {action}
    </div>
  );
}

async function startWithSameNonce(
  operation: PendingProviderOperation,
  idToken: string,
  persist: (operation: PendingProviderOperation) => void,
): Promise<ProviderOperationResult> {
  let lastError: unknown;
  for (let attempt = 0; attempt < RECOVERY_ATTEMPTS; attempt++) {
    try {
      const result = await startProviderOperation({
        provider: operation.provider,
        operation: operation.operation,
        nonce: operation.nonce,
        idToken,
      });
      persist({
        ...operation,
        operationId: result.operationId,
        ...(result.completionTokenNotBefore
          ? { completionTokenNotBefore: result.completionTokenNotBefore }
          : {}),
      });
      return result;
    } catch (error) {
      lastError = error;
      if (
        !isRetryableProviderError(error) ||
        attempt === RECOVERY_ATTEMPTS - 1
      ) {
        throw error;
      }
      await recoveryPause(attempt);
    }
  }
  throw lastError;
}

async function reconcileLinkCompletion(
  operation: PendingProviderOperation,
  user: User,
): Promise<ProviderOperationResult> {
  if (!operation.operationId || !operation.completionTokenNotBefore) {
    throw new Error("追加処理の状態を確認できませんでした。");
  }
  await waitUntil(operation.completionTokenNotBefore);
  const idToken = await getIdToken(user, true);
  let lastError: unknown;
  for (let attempt = 0; attempt < RECOVERY_ATTEMPTS; attempt++) {
    const before = await readOperationStatus(operation).catch((error) => {
      lastError = error;
      return null;
    });
    if (before) {
      const terminal = terminalLinkResult(before);
      if (terminal) return terminal;
    }
    try {
      const completed = await completeProviderOperation({
        operationId: operation.operationId,
        nonce: operation.nonce,
        idToken,
      });
      const terminal = terminalLinkResult(completed);
      if (terminal) return terminal;
    } catch (error) {
      lastError = error;
    }
    const after = await readOperationStatus(operation).catch((error) => {
      lastError = error;
      return null;
    });
    if (after) {
      const terminal = terminalLinkResult(after);
      if (terminal) return terminal;
    }
    if (attempt < RECOVERY_ATTEMPTS - 1) await recoveryPause(attempt);
  }
  throw new ProviderOperationStillPendingError(
    "追加結果をまだ確認できません。接続を確認して「再開」を押してください。",
    lastError,
  );
}

async function settleKnownLinkFailure(
  operation: PendingProviderOperation,
  outcome: "credential_in_use" | "firebase_operation_failed" | "cancelled",
): Promise<boolean> {
  if (!operation.operationId) return false;
  for (let attempt = 0; attempt < RECOVERY_ATTEMPTS; attempt++) {
    try {
      await failProviderOperation({
        operationId: operation.operationId,
        nonce: operation.nonce,
        outcome,
      });
      return true;
    } catch {
      const status = await readOperationStatus(operation).catch(() => null);
      if (status?.status === "failed") return true;
      if (attempt < RECOVERY_ATTEMPTS - 1) await recoveryPause(attempt);
    }
  }
  return false;
}

async function confirmTerminalStatus(
  result: ProviderOperationResult,
  operation: PendingProviderOperation,
): Promise<void> {
  const withId = { ...operation, operationId: result.operationId };
  for (let attempt = 0; attempt < RECOVERY_ATTEMPTS; attempt++) {
    try {
      const status = await readOperationStatus(withId);
      if (status.outcome === result.outcome) return;
      if (status.status === "failed") throw terminalResultError(status);
    } catch (error) {
      if (!isRetryableProviderError(error)) throw error;
    }
    if (attempt < RECOVERY_ATTEMPTS - 1) await recoveryPause(attempt);
  }
  // The mutation response itself is terminal. A status outage must not turn a
  // confirmed backend-owned unlink back into a client-owned pending action.
}

async function readOperationStatus(
  operation: PendingProviderOperation,
): Promise<ProviderOperationResult> {
  if (!operation.operationId) {
    throw new Error("変更処理の識別子がありません。");
  }
  return statusProviderOperation({
    operationId: operation.operationId,
    nonce: operation.nonce,
  });
}

function terminalLinkResult(
  result: ProviderOperationResult,
): ProviderOperationResult | null {
  if (isSuccessfulLink(result)) return result;
  if (result.status === "failed" || isTerminalFailureOutcome(result.outcome)) {
    throw terminalResultError(result);
  }
  return null;
}

async function finishSuccessfulOperation(
  result: ProviderOperationResult,
  operation: PendingProviderOperation,
  user: User,
  refresh: (user: User) => Promise<void>,
  clear: (expected: ProviderScope) => void,
  publish: (
    provider: ManagedProvider,
    operation: "linked" | "unlinked",
  ) => void,
): Promise<void> {
  await refresh(user);
  clear(operation);
  if (result.noticeRequired) publish(operation.provider, "linked");
}

function assertLinkReady(result: ProviderOperationResult): void {
  if (
    result.outcome !== "client_operation_required" ||
    result.clientOperation !== "firebase_link_with_credential" ||
    !result.completionTokenNotBefore
  ) {
    throw resultStateError(result);
  }
}

function isSuccessfulLink(result: ProviderOperationResult): boolean {
  return (
    result.outcome === "provider_linked" ||
    result.outcome === "provider_already_linked"
  );
}

function isTerminalFailureOutcome(
  outcome: ProviderOperationResult["outcome"],
): boolean {
  return (
    outcome === "credential_in_use" ||
    outcome === "firebase_operation_failed" ||
    outcome === "cancelled" ||
    outcome === "last_login_method"
  );
}

function terminalResultError(
  result: ProviderOperationResult,
): TerminalProviderOperationError {
  return new TerminalProviderOperationError(result.outcome);
}

function resultStateError(result: ProviderOperationResult): Error {
  if (isTerminalFailureOutcome(result.outcome) || result.status === "failed") {
    return terminalResultError(result);
  }
  return new ProviderOperationStillPendingError(
    "変更結果をまだ確認できません。接続を確認して「再開」を押してください。",
  );
}

/**
 * A token whose auth_time is fresh enough to prove control for an unlink.
 * Mirrors the server's rule: recent, and signed in through a different method
 * than the one being removed — an OAuth provider linked to this account, a
 * password, or a Sumi email-code custom session.
 */
async function recentReauthToken(
  user: User,
  target: ManagedProvider,
): Promise<string | null> {
  const token = await getIdTokenResult(user, true);
  const firebaseClaims = token.claims.firebase;
  const signInProvider =
    isObject(firebaseClaims) &&
    typeof firebaseClaims.sign_in_provider === "string"
      ? firebaseClaims.sign_in_provider
      : "";
  const authTime = token.claims.auth_time;
  const age = typeof authTime === "number" ? Date.now() / 1000 - authTime : NaN;
  const recent = Number.isFinite(age) && age >= -60 && age <= 240;
  if (!recent || signInProvider === target) return null;
  if (
    (signInProvider === "google.com" || signInProvider === "github.com") &&
    user.providerData.some(({ providerId }) => providerId === signInProvider)
  ) {
    return token.token;
  }
  if (
    signInProvider === "password" &&
    user.providerData.some(({ providerId }) => providerId === "password")
  ) {
    return token.token;
  }
  // Sumi mints custom tokens only after an email code or link proof.
  if (signInProvider === "custom" && user.email && user.emailVerified) {
    return token.token;
  }
  return null;
}

function createFirebaseProvider(provider: ManagedProvider) {
  if (provider === "github.com") return new GithubAuthProvider();
  const google = new GoogleAuthProvider();
  google.setCustomParameters({ prompt: "select_account" });
  return google;
}

async function waitUntil(timestamp: string): Promise<void> {
  const delay = Date.parse(timestamp) - Date.now() + 50;
  if (delay > 10_000) throw new Error("追加処理の開始時刻が不正です。");
  if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
}

async function recoveryPause(attempt: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 100 * (attempt + 1)));
}

function providerFailureOutcome(error: unknown) {
  if (error instanceof FirebaseError) {
    if (error.code === "auth/credential-already-in-use") {
      return "credential_in_use" as const;
    }
    if (
      error.code === "auth/popup-closed-by-user" ||
      error.code === "auth/redirect-cancelled-by-user"
    ) {
      return "cancelled" as const;
    }
  }
  return "firebase_operation_failed" as const;
}

function isRetryableProviderError(error: unknown): boolean {
  if (
    error instanceof TerminalProviderOperationError ||
    error instanceof ProviderAccountChangedError
  ) {
    return false;
  }
  if (!(error instanceof AuthAPIError)) return true;
  return error.status >= 500 || error.message === "provider_unavailable";
}

function isDefinitiveStartFailure(
  error: unknown,
  operation: PendingProviderOperation | null,
): boolean {
  return (
    Boolean(operation && !operation.operationId) &&
    error instanceof AuthAPIError &&
    !isRetryableProviderError(error)
  );
}

function providerSettingsError(error: unknown): string {
  if (error instanceof ProviderOperationStillPendingError) return error.message;
  if (error instanceof ProviderRedirectNotSentError) {
    return "認証ページへ移動できませんでした。もう一度お試しください。";
  }
  if (error instanceof TerminalProviderOperationError) {
    switch (error.outcome) {
      case "credential_in_use":
        return "このログイン方法は別のアカウントで使用されています。";
      case "last_login_method":
        return "最後のログイン方法は解除できません。先に別の方法を追加してください。";
      case "cancelled":
        return "認証をキャンセルしました。";
      default:
        return "ログイン方法を変更できませんでした。接続を確認して再試行してください。";
    }
  }
  if (error instanceof AuthAPIError) {
    switch (error.message) {
      case "recent_reauth_required":
        return "別のログイン方法で再認証してから、5分以内にもう一度お試しください。";
      case "last_login_method":
        return "最後のログイン方法は解除できません。先に別の方法を追加してください。";
      case "provider_operation_pending":
        return "別のログイン方法の変更が処理中です。保留中の変更を再開してください。";
      case "provider_unavailable":
        return "結果をまだ確認できません。接続を確認して再試行してください。";
      case "proof_mismatch":
        return "再認証を確認できませんでした。もう一度お試しください。";
    }
  }
  if (error instanceof FirebaseError) {
    if (
      error.code === "auth/popup-closed-by-user" ||
      error.code === "auth/redirect-cancelled-by-user"
    ) {
      return "認証をキャンセルしました。";
    }
    if (error.code === "auth/credential-already-in-use") {
      return "このログイン方法は別のアカウントで使用されています。";
    }
    if (error.code === "auth/provider-already-linked") {
      return "このログイン方法はすでに追加されています。";
    }
    if (error.code === "auth/user-mismatch") {
      return "別のアカウントで認証されました。同じアカウントでもう一度お試しください。";
    }
    if (error.code === "auth/redirect-operation-pending") {
      return "別の認証を処理しています。少し待ってから、もう一度お試しください。";
    }
  }
  return error instanceof Error && error.message
    ? error.message
    : "ログイン方法を変更できませんでした。";
}

function providerLabel(provider: ManagedProvider): string {
  return provider === "github.com" ? "GitHub" : "Google";
}

function pendingOperationMessage(operation: PendingProviderOperation): string {
  const label = providerLabel(operation.provider);
  if (operation.operation === "unlink") {
    return `${label}の解除を再開できます`;
  }
  switch (operation.phase) {
    case "starting":
      return `${label}の追加準備を再開できます`;
    case "link_ready":
      return `${label}で認証を続けてください`;
    case "link_mutated":
      return `${label}の追加結果を確認できます`;
    default:
      return `${label}の変更を再開できます`;
  }
}

function loadScopedNotice(scope: ProviderScope): ProviderNotice | null {
  const value = readSessionJSON(PROVIDER_NOTICE_KEY);
  if (!isProviderNotice(value) || !sameScope(value, scope)) {
    if (value !== null) sessionStorage.removeItem(PROVIDER_NOTICE_KEY);
    return null;
  }
  return value;
}

function loadScopedPendingOperation(
  scope: ProviderScope,
): PendingProviderOperation | null {
  const value = readPendingJSON();
  if (value === null) return null;
  if (!isPendingProviderOperation(value)) {
    localStorage.removeItem(PROVIDER_PENDING_KEY);
    return null;
  }
  // A different account's unfinished change is left in place: its own scope
  // can still resume it, and this session simply never adopts it.
  if (!sameScope(value, scope)) return null;
  return value;
}

function clearProviderSessionState(): void {
  sessionStorage.removeItem(PROVIDER_NOTICE_KEY);
  sessionStorage.removeItem(PROVIDER_PENDING_KEY);
  clearPendingProviderRedirect();
  // The durable operation record deliberately survives logout: the change is
  // still pending server-side, and only its own account scope can resume it.
}

function readSessionJSON(key: string): unknown {
  try {
    return JSON.parse(sessionStorage.getItem(key) ?? "null") as unknown;
  } catch {
    sessionStorage.removeItem(key);
    return null;
  }
}

function readPendingJSON(): unknown {
  try {
    return JSON.parse(
      localStorage.getItem(PROVIDER_PENDING_KEY) ??
        sessionStorage.getItem(PROVIDER_PENDING_KEY) ??
        "null",
    ) as unknown;
  } catch {
    localStorage.removeItem(PROVIDER_PENDING_KEY);
    sessionStorage.removeItem(PROVIDER_PENDING_KEY);
    return null;
  }
}

function isProviderNotice(value: unknown): value is ProviderNotice {
  return (
    hasScope(value) &&
    value.version === 1 &&
    isManagedProvider(value.provider) &&
    (value.operation === "linked" || value.operation === "unlinked")
  );
}

function isPendingProviderOperation(
  value: unknown,
): value is PendingProviderOperation {
  return (
    hasScope(value) &&
    value.version === 1 &&
    isManagedProvider(value.provider) &&
    (value.operation === "link" || value.operation === "unlink") &&
    typeof value.nonce === "string" &&
    value.nonce.length >= 32 &&
    value.nonce.length <= 128 &&
    (value.phase === "starting" ||
      value.phase === "link_ready" ||
      value.phase === "link_mutated" ||
      value.phase === "unlink_starting") &&
    (value.operationId === undefined ||
      (typeof value.operationId === "string" &&
        value.operationId.length <= 128)) &&
    (value.completionTokenNotBefore === undefined ||
      (typeof value.completionTokenNotBefore === "string" &&
        Number.isFinite(Date.parse(value.completionTokenNotBefore))))
  );
}

function hasScope(
  value: unknown,
): value is Record<string, unknown> & ProviderScope {
  return (
    isObject(value) &&
    typeof value.firebaseUid === "string" &&
    value.firebaseUid.length > 0 &&
    value.firebaseUid.length <= 128 &&
    typeof value.humanId === "string" &&
    value.humanId.length > 0 &&
    value.humanId.length <= 256
  );
}

function sameScope(value: ProviderScope, scope: ProviderScope): boolean {
  return (
    value.firebaseUid === scope.firebaseUid && value.humanId === scope.humanId
  );
}

function isManagedProvider(value: unknown): value is ManagedProvider {
  return value === "google.com" || value === "github.com";
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

class TerminalProviderOperationError extends Error {
  readonly outcome: ProviderOperationResult["outcome"];

  constructor(outcome: ProviderOperationResult["outcome"]) {
    super(outcome);
    this.name = "TerminalProviderOperationError";
    this.outcome = outcome;
  }
}

class ProviderOperationStillPendingError extends Error {
  readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "ProviderOperationStillPendingError";
    this.cause = cause;
  }
}

class ProviderAccountChangedError extends Error {
  constructor() {
    super("Account changed during provider operation.");
    this.name = "ProviderAccountChangedError";
  }
}

class ProviderRedirectNotSentError extends Error {
  constructor() {
    super("The provider redirect finished without leaving this tab.");
    this.name = "ProviderRedirectNotSentError";
  }
}
