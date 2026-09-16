import {
  getIdToken,
  onAuthStateChanged,
  signOut,
  type User,
} from "firebase/auth";
import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { flushSync } from "react-dom";
import {
  bindDirectChatAuthority,
  clearDirectChatAuthority,
} from "../agent/auth-authority";
import { startPushSubscriptionLogoutCleanup } from "../messaging/push";
import {
  clearPendingConfirmation,
  loadPendingConfirmation,
  type PendingAuthConfirmation,
  savePendingConfirmation,
} from "./auth-confirmation-state";
import {
  type AuthIntent,
  confirmAuthFlow,
  discardAuthFlow,
  type EmailChallengeStatus,
  resolveAuthFlow,
} from "./auth-flow-client";
import {
  clearActiveEmailFlowState,
  clearPendingEmailFlow,
  clearPendingRedirectFlow,
  isExpiredFlow,
  listPendingEmailFlows,
  loadPendingEmailFlow,
  loadPendingRedirectFlow,
  type PendingRedirectAuthFlow,
  type RecoverableProvider,
} from "./auth-flow-state";
import {
  type AuthOutcomeNotice,
  clearAuthOutcomeNotice,
  hasPendingAuthOutcomeNotice,
  publishAuthOutcomeNotice,
  takeAuthOutcomeNotice,
} from "./auth-outcome-notice-state";
import {
  publishSessionEnded,
  subscribeSessionEnded,
} from "./auth-session-broadcast";
import { noteAuthTeardown } from "./auth-transition";
import {
  beginSameEmailCredentialRecovery,
  completeSameEmailCredentialRecovery,
  isSameEmailCredentialCollision,
} from "./credential-recovery";
import {
  type ActiveEmailCodeFlow,
  abandonEmailCodeFlow,
  beginEmailCodeAuth,
  clearPendingEmailLink,
  completeEmailLink as completeEmailLinkProof,
  type EmailLinkInspection,
  type EmailProof,
  ensureEmailCompletionSession,
  finishEmailProof,
  inspectEmailLink as inspectPendingEmailLink,
  loadActiveEmailCodeFlow,
  pendingEmailLink,
  readEmailCodeStatus,
  resendEmailCode as requestEmailCodeResend,
  verifyEmailCode,
} from "./email-code-auth";
import { getFirebaseAuth } from "./firebase";
import { isFirebaseConfigured } from "./firebase-config";
import { clearPendingProviderRedirect } from "./provider-redirect";
import {
  beginRedirectSignIn,
  hasPendingRedirectSignIn,
  RedirectSignInAbandonedError,
  RedirectSignInExpiredError,
  resolveRedirectSignInUser,
  takePendingRedirectSignIn,
} from "./redirect-sign-in";
import {
  AuthAPIError,
  type ConfirmedSumiProfile,
  canonicalizeSumiDisplayName,
  getSumiProfile,
  getSumiSession,
  logoutSumiSession,
  type SumiProfilePatch,
  SumiProfileUpdateIndeterminateError,
  SumiSessionCompensatedError,
  SumiSessionCompensationFailedError,
  type SumiSessionStatus,
  updateSumiProfile,
  verifyCommittedSumiSession,
} from "./session-client";

export type SignInProvider = "google" | "github";
export type AuthSessionState =
  | "checking"
  | "authenticated"
  | "unauthenticated"
  | "preissued"
  | "unavailable";

export const preissuedSessionMode =
  import.meta.env.VITE_SUMI_AUTH_MODE === "preissued";
const preissuedUserID = import.meta.env.VITE_SUMI_PREISSUED_USER_ID?.trim();

/**
 * Browser session cookies and the authenticated WebSocket are one origin
 * boundary. Only the isolated, pre-issued browser fixture deliberately skips
 * that boundary.
 */
export function hasAllowedAuthOrigin({
  apiBaseURL,
  authMode,
  pageOrigin,
}: {
  apiBaseURL?: string;
  authMode?: string;
  pageOrigin?: string;
}): boolean {
  if (!pageOrigin) return false;
  try {
    const pageURL = new URL(pageOrigin);
    const configuredBase = apiBaseURL?.trim();
    const apiURL = configuredBase ? new URL(configuredBase, pageURL) : pageURL;
    if (
      apiURL.pathname !== "/" ||
      apiURL.search ||
      apiURL.hash ||
      apiURL.username ||
      apiURL.password
    ) {
      return false;
    }
    return apiURL.origin === pageURL.origin || authMode === "preissued";
  } catch {
    return false;
  }
}

const authOriginAllowed = hasAllowedAuthOrigin({
  apiBaseURL: import.meta.env.VITE_API_BASE_URL,
  authMode: import.meta.env.VITE_SUMI_AUTH_MODE,
  pageOrigin: globalThis.location?.origin,
});

export interface AuthUser {
  id: string;
  displayName: string | null;
  email: string | null;
  photoURL: string | null;
}

export interface EmailCodeView {
  email: string;
  intent: AuthIntent;
  /** A provider collision waits for this email proof before linking. */
  recovery: boolean;
  challenge: EmailChallengeStatus | null;
}

/**
 * A sign-in resolved to a Human while this jar already belongs to another.
 * The person chooses: switch (the server retires the active session and
 * issues the new one) or cancel (the interrupted flow is discarded).
 */
export interface AccountSwitchPrompt {
  currentUserId: string;
  currentDisplayName: string | null;
  /** What the interrupted sign-in was for, e.g. its email address. */
  target: string;
}

interface PendingAccountSwitch extends AccountSwitchPrompt {
  resume: (switchFromUserId: string) => Promise<void>;
  abandon: () => Promise<void>;
}

export interface AuthContextValue {
  configured: boolean;
  loading: boolean;
  sessionState: AuthSessionState;
  sessionSuspended: boolean;
  authenticated: boolean;
  canUseDirectChat: boolean;
  authorityBindingId: string | null;
  user: AuthUser | null;
  confirmation: PendingAuthConfirmation | null;
  outcomeNotice: AuthOutcomeNotice | null;
  emailCode: EmailCodeView | null;
  emailLinkPending: boolean;
  accountSwitch: AccountSwitchPrompt | null;
  confirmAccountSwitch: () => Promise<void>;
  cancelAccountSwitch: () => void;
  credentialRecoveryEmailSent: boolean;
  redirectSignInPending: boolean;
  redirectSignInError: unknown;
  dismissRedirectSignInError: () => void;
  signIn: (provider: SignInProvider, intent: AuthIntent) => Promise<void>;
  startEmailCode: (email: string, intent: AuthIntent) => Promise<void>;
  submitEmailCode: (code: string) => Promise<void>;
  resendEmailCode: () => Promise<void>;
  refreshEmailCode: () => Promise<void>;
  cancelEmailCode: () => void;
  inspectEmailLink: () => Promise<EmailLinkInspection>;
  continueEmailLink: (
    inspection: EmailLinkInspection,
    adopt: boolean,
    options?: { switchFromUserId?: string },
  ) => Promise<void>;
  dismissEmailLink: () => void;
  confirmIntentTransition: () => Promise<void>;
  cancelIntentTransition: () => Promise<void>;
  dismissOutcomeNotice: () => void;
  updateProfile: (
    patch: SumiProfilePatch,
  ) => Promise<ConfirmedSumiProfile | null>;
  updateDisplayName: (displayName: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshSession: (options?: {
    background?: boolean;
  }) => Promise<AuthSessionState>;
}

export const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<SumiSessionStatus>({
    authenticated: false,
  });
  const [sessionState, setSessionState] = useState<AuthSessionState>(
    preissuedSessionMode
      ? "preissued"
      : authOriginAllowed
        ? "checking"
        : "unavailable",
  );
  const [sessionSuspended, setSessionSuspended] = useState(false);
  const logoutPending = useRef(false);
  const sessionRevalidationRequired = useRef(false);
  // BroadcastChannel reaches other contexts on this origin — including this
  // page's own subscription — so the sender token lets us skip our own echo.
  const authBroadcastSender = useRef(
    globalThis.crypto?.randomUUID?.() ??
      `auth-${Math.random().toString(36).slice(2)}`,
  ).current;
  const [confirmation, setConfirmation] =
    useState<PendingAuthConfirmation | null>(() => loadPendingConfirmation());
  const [outcomeNotice, setOutcomeNotice] = useState<AuthOutcomeNotice | null>(
    null,
  );
  const [emailCodeFlow, setEmailCodeFlow] =
    useState<ActiveEmailCodeFlow | null>(() => loadActiveEmailCodeFlow());
  const [emailChallenge, setEmailChallenge] =
    useState<EmailChallengeStatus | null>(null);
  const [emailLinkPending, setEmailLinkPending] = useState(
    () => pendingEmailLink() !== null,
  );
  const [accountSwitch, setAccountSwitch] =
    useState<PendingAccountSwitch | null>(null);
  const [credentialRecoveryEmailSent, setCredentialRecoveryEmailSent] =
    useState(false);
  // Every state-changing auth operation claims a generation. Late session
  // reads must never re-authorize the chat after logout has started.
  const authGeneration = useRef(0);
  const sessionMutation = useRef<Promise<void>>(Promise.resolve());
  // Read during the first render, before any effect: a returning provider
  // redirect must survive the initial session read and StrictMode's repeated
  // effects without being claimed twice.
  const [redirectSignInPending, setRedirectSignInPending] = useState(
    () =>
      !preissuedSessionMode && authOriginAllowed && hasPendingRedirectSignIn(),
  );
  const [redirectSignInError, setRedirectSignInError] = useState<unknown>(null);
  const redirectReturnClaimed = useRef(false);
  // Each signIn gets an attempt id; a restored page or a newer attempt makes
  // an in-flight begin obsolete, and it must not navigate the tab away.
  const redirectBeginSeq = useRef(0);
  const redirectBeginCancelled = useRef(0);
  const signInPending = useRef(redirectSignInPending);
  const serverSession = useRef<SumiSessionStatus>(session);

  const claimSavedOutcomeNotice = useCallback(
    (nextSession: Extract<SumiSessionStatus, { authenticated: true }>) => {
      if (!hasPendingAuthOutcomeNotice()) return;
      try {
        const firebaseUID = getFirebaseAuth().currentUser?.uid;
        if (!firebaseUID) return;
        const saved = takeAuthOutcomeNotice({
          firebaseUID,
          humanId: nextSession.user.id,
        });
        if (saved) setOutcomeNotice(saved);
      } catch {
        // A notice is optional display state. Never relax identity checks when
        // Firebase state is unavailable during restoration.
      }
    },
    [],
  );

  const publishOutcomeNotice = useCallback(
    ({
      firebaseUID,
      humanId,
      outcome,
      intent,
      intentTransition = "none",
      receiptId,
    }: {
      firebaseUID: string;
      humanId: string;
      outcome: "account_created" | "signed_in" | "provider_linked";
      intent: AuthIntent;
      intentTransition?: "none" | "confirmed" | "recovery_proved";
      receiptId: string;
    }) => {
      const notice = publishAuthOutcomeNotice({
        scope: { firebaseUID, humanId },
        outcome,
        intent,
        intentTransition,
        receiptId,
      });
      if (notice) setOutcomeNotice(notice);
    },
    [],
  );

  const nextGeneration = useCallback(() => {
    authGeneration.current += 1;
    return authGeneration.current;
  }, []);

  const isCurrentGeneration = useCallback(
    (generation: number) => authGeneration.current === generation,
    [],
  );

  const serializeSessionMutation = useCallback(
    async <T,>(operation: () => Promise<T>): Promise<T> => {
      const previous = sessionMutation.current;
      let complete!: () => void;
      sessionMutation.current = new Promise<void>((resolve) => {
        complete = resolve;
      });
      await previous.catch(() => undefined);
      try {
        return await operation();
      } finally {
        complete();
      }
    },
    [],
  );

  const refreshSession = useCallback(
    async (options?: { background?: boolean }): Promise<AuthSessionState> => {
      if (preissuedSessionMode) {
        const generation = nextGeneration();
        if (isCurrentGeneration(generation)) {
          setSession({ authenticated: false });
          setSessionState("preissued");
        }
        return "preissued";
      }
      if (!authOriginAllowed) {
        setSession({ authenticated: false });
        setSessionState("unavailable");
        return "unavailable";
      }
      // A provider redirect can still be in flight when a component effect runs:
      // on this startup its return is being exchanged. A server read during that
      // interval must not cancel it.
      if (signInPending.current || logoutPending.current) return "checking";
      const generation = nextGeneration();
      // A dropped socket does not end the authenticated session. Keep the
      // workspace mounted while checking it so drafts and uploads survive a
      // temporary network failure. Initial authentication still blocks the UI.
      // A background read — reconciliation after a failed redirect return —
      // must not bounce the unauthenticated login form through "checking"
      // either: its local state (a typed address, a link-sent notice) survives
      // unless the read produces a real transition.
      if (
        (!serverSession.current.authenticated &&
          options?.background !== true) ||
        sessionRevalidationRequired.current
      ) {
        setSessionState("checking");
      }
      let nextSession: SumiSessionStatus;
      try {
        nextSession = await getSumiSession();
      } catch (error) {
        if (!isCurrentGeneration(generation)) return "checking";
        let nextState = classifySessionFailure(error);
        if (
          nextState === "unavailable" &&
          serverSession.current.authenticated
        ) {
          // A failed logout may already have cleared the cookie. Keep the work
          // paused until a fresh server read establishes who is authenticated.
          if (sessionRevalidationRequired.current) {
            setSessionState("unavailable");
            return "unavailable";
          }
          return "authenticated";
        }
        const sessionRejected = nextState === "unauthenticated";
        flushSync(() => {
          if (sessionRejected && !clearDirectChatAuthority()) {
            nextState = "unavailable";
          }
          sessionRevalidationRequired.current = false;
          setSessionSuspended(false);
          serverSession.current = { authenticated: false };
          setSession({ authenticated: false });
          setSessionState(nextState);
        });
        if (sessionRejected) {
          clearAuthOutcomeNotice();
          setOutcomeNotice(null);
        }
        return nextState;
      }
      if (!isCurrentGeneration(generation)) return "checking";
      try {
        let nextState: AuthSessionState = nextSession.authenticated
          ? "authenticated"
          : "unauthenticated";
        flushSync(() => {
          if (nextSession.authenticated) {
            clearPendingConfirmation();
            setConfirmation(null);
            bindDirectChatAuthority(nextSession.authorityBindingId);
          } else if (!clearDirectChatAuthority()) {
            nextState = "unavailable";
          }
          sessionRevalidationRequired.current = false;
          setSessionSuspended(false);
          serverSession.current = nextSession;
          setSession(nextSession);
          setSessionState(nextState);
        });
        if (nextSession.authenticated) claimSavedOutcomeNotice(nextSession);
        else {
          clearAuthOutcomeNotice();
          setOutcomeNotice(null);
        }
        return nextState;
      } catch {
        // A failed private-state reset is an authority-transition failure,
        // not a transient session read that can retain the previous workspace.
        flushSync(() => {
          clearDirectChatAuthority();
          sessionRevalidationRequired.current = false;
          setSessionSuspended(false);
          serverSession.current = { authenticated: false };
          setSession({ authenticated: false });
          setSessionState("unavailable");
        });
        return "unavailable";
      }
    },
    [claimSavedOutcomeNotice, isCurrentGeneration, nextGeneration],
  );

  useEffect(() => {
    void refreshSession();
  }, [refreshSession]);

  useEffect(() => {
    if (
      preissuedSessionMode ||
      !authOriginAllowed ||
      (!confirmation && !hasPendingAuthOutcomeNotice())
    ) {
      return;
    }
    let unsubscribe: () => void = () => undefined;
    try {
      unsubscribe = onAuthStateChanged(getFirebaseAuth(), (firebaseUser) => {
        if (confirmation && firebaseUser?.uid !== confirmation.firebaseUID) {
          nextGeneration();
          clearPendingConfirmation();
          setConfirmation(null);
        }
        setOutcomeNotice((current) =>
          current && current.firebaseUID !== firebaseUser?.uid ? null : current,
        );
        if (firebaseUser && serverSession.current.authenticated) {
          claimSavedOutcomeNotice(serverSession.current);
        }
      });
    } catch {
      if (confirmation) {
        clearPendingConfirmation();
        setConfirmation(null);
      }
    }
    return unsubscribe;
  }, [claimSavedOutcomeNotice, confirmation, nextGeneration]);

  /**
   * A failed completion may still have committed a session — for this flow's
   * Human or for a later choice made in another tab. Publishing the jar's
   * actual state keeps the UI honest; a blind logout would erase another
   * account's legitimate sign-in.
   */
  const reconcileSessionState = useCallback(async () => {
    let status: SumiSessionStatus;
    try {
      status = await getSumiSession();
    } catch {
      return;
    }
    flushSync(() => {
      if (status.authenticated) {
        bindDirectChatAuthority(status.authorityBindingId);
      } else {
        clearDirectChatAuthority();
      }
      serverSession.current = status;
      setSession(status);
      setSessionState(
        status.authenticated ? "authenticated" : "unauthenticated",
      );
    });
  }, []);

  /**
   * A 409 session_active means this jar's session belongs to a different
   * Human than the interrupted sign-in. The interrupted work keeps its flow
   * authority while the person decides: confirming retries the completion
   * with switch_from_user_id, which the server turns into a session
   * replacement; cancelling discards the interrupted flow.
   */
  const offerAccountSwitch = useCallback(
    async (
      target: string,
      resume: (switchFromUserId: string) => Promise<void>,
      abandon: () => Promise<void>,
    ): Promise<void> => {
      let live: SumiSessionStatus;
      try {
        live = await getSumiSession();
      } catch {
        live = { authenticated: false };
      }
      if (!live.authenticated) {
        // The conflicting session is already gone — no choice is needed.
        await resume("");
        return;
      }
      setAccountSwitch({
        currentUserId: live.user.id,
        currentDisplayName: live.user.displayName,
        target,
        resume,
        abandon,
      });
    },
    [],
  );

  const confirmAccountSwitch = useCallback(async () => {
    const pending = accountSwitch;
    if (!pending) return;
    setAccountSwitch(null);
    await pending.resume(pending.currentUserId);
  }, [accountSwitch]);

  const cancelAccountSwitch = useCallback(() => {
    const pending = accountSwitch;
    setAccountSwitch(null);
    if (pending) void pending.abandon().catch(() => undefined);
  }, [accountSwitch]);

  /**
   * Turns a proven Firebase credential into the server-owned Sumi session.
   * Shared by the redirect return and any later provider proof: a Firebase UID
   * alone never authorizes a session.
   */
  const exchangeFirebaseProof = useCallback(
    async ({
      generation,
      flow,
      user,
      switchFromUserId,
    }: {
      generation: number;
      flow: PendingRedirectAuthFlow;
      user: User;
      switchFromUserId?: string;
    }): Promise<boolean> => {
      let confirmationRequired = false;
      await serializeSessionMutation(async () => {
        if (!isCurrentGeneration(generation)) return;
        const idToken = await getIdToken(user, true);
        if (!isCurrentGeneration(generation)) return;
        const resolved = await resolveAuthFlow({
          flowId: flow.flowId,
          nonce: flow.nonce,
          idToken,
          switchFromUserId,
        });
        if (resolved.outcome === "confirmation_required") {
          const pending: PendingAuthConfirmation = {
            flowId: resolved.flowId,
            nonce: flow.nonce,
            intent: flow.intent,
            provider: flow.provider,
            expiresAt: resolved.expiresAt,
            action: resolved.nextAction,
            firebaseUID: user.uid,
            account: firebaseAccount(user),
          };
          savePendingConfirmation(pending);
          confirmationRequired = true;
          if (isCurrentGeneration(generation)) setConfirmation(pending);
          return;
        }
        const nextSession = await verifyCommittedSumiSession({
          compensate: () => discardAuthFlow(flow.flowId, flow.nonce),
        });
        if (
          resolved.outcome !== "signed_in" &&
          resolved.outcome !== "account_created"
        ) {
          throw new AuthAPIError("Invalid authentication flow response.", 0);
        }
        if (nextSession.user.id !== resolved.humanId) {
          // The jar's session belongs to a different Human than the resolved
          // one — a later choice in another tab stands unless the person
          // explicitly switches.
          throw new AuthAPIError("session_active", 409);
        }
        publishOutcomeNotice({
          firebaseUID: user.uid,
          humanId: nextSession.user.id,
          outcome: resolved.outcome,
          intent: flow.intent,
          receiptId: resolved.flowId,
        });
        // The HttpOnly authority changed even if logout claimed the UI
        // generation while this serialized exchange was in flight.
        flushSync(() => {
          bindDirectChatAuthority(nextSession.authorityBindingId);
          serverSession.current = nextSession;
          if (!isCurrentGeneration(generation)) return;
          setSession(nextSession);
          setSessionState("authenticated");
        });
      });
      return confirmationRequired;
    },
    [isCurrentGeneration, publishOutcomeNotice, serializeSessionMutation],
  );

  // A switch confirmation retries the exchange later, through whatever the
  // current callback identity is.
  const exchangeFirebaseProofRef = useRef(exchangeFirebaseProof);
  exchangeFirebaseProofRef.current = exchangeFirebaseProof;

  /**
   * Completes a provider redirect once, on startup. The persisted receipt —
   * not the Firebase account — names the flow whose proof may be exchanged.
   */
  const redirectCompletionActive = useRef(false);

  const completeRedirectSignIn = useCallback(async () => {
    const generation = nextGeneration();
    signInPending.current = true;
    redirectCompletionActive.current = true;
    let firebaseSignInCompleted = false;
    let confirmationRequired = false;
    let user: User | null = null;
    let flow: PendingRedirectAuthFlow | null = null;
    try {
      flow = takePendingRedirectSignIn();
      if (!flow) {
        // A raw record existed but failed validation (or a reload claimed it
        // first). The return must report an outcome, not land silently on
        // the login screen.
        throw new RedirectSignInAbandonedError();
      }
      try {
        user = await resolveRedirectSignInUser();
      } catch (error) {
        if (
          isSameEmailCredentialCollision(error) &&
          isCurrentGeneration(generation)
        ) {
          const started = await beginSameEmailCredentialRecovery(
            error,
            flow.provider,
            flow.intent,
          );
          if (isCurrentGeneration(generation)) {
            setEmailCodeFlow(started.active);
            setEmailChallenge(started.challenge);
            setCredentialRecoveryEmailSent(true);
          }
          return;
        }
        // No provider result arrived. When the receipt also outlived its
        // expiry the person timed out at the provider rather than
        // cancelling — say so specifically. A live credential would still
        // have been exchanged: the server, not this clock, owns expiry.
        if (error instanceof RedirectSignInAbandonedError) {
          throw isExpiredFlow(flow) ? new RedirectSignInExpiredError() : error;
        }
        throw error;
      }
      firebaseSignInCompleted = true;
      if (!isCurrentGeneration(generation)) {
        // A logout claimed the generation while the return was being read. It
        // owns the terminal Firebase state as well as the server cookie.
        await signOutFirebaseBestEffort();
        return;
      }
      confirmationRequired = await exchangeFirebaseProof({
        generation,
        flow,
        user,
      });
      if (!isCurrentGeneration(generation) && !confirmationRequired) {
        await signOutFirebaseBestEffort();
      }
    } catch (error) {
      if (
        isSessionActiveError(error) &&
        isCurrentGeneration(generation) &&
        user &&
        flow
      ) {
        // The resolved Human differs from the jar's active session. Keep the
        // Firebase credential and the flow alive while the person decides —
        // switching retries the exchange with switch_from_user_id, cancelling
        // discards the flow.
        const resolvedUser = user;
        const resolvedFlow = flow;
        const target =
          resolvedUser.displayName ??
          resolvedUser.email ??
          resolvedFlow.provider;
        const resume = async (switchFromUserId: string) => {
          await exchangeFirebaseProofRef.current({
            generation: nextGeneration(),
            flow: resolvedFlow,
            user: resolvedUser,
            switchFromUserId: switchFromUserId || undefined,
          });
        };
        const abandon = async () => {
          try {
            await discardAuthFlow(resolvedFlow.flowId, resolvedFlow.nonce);
          } catch {
            // Epoch closure and expiry still fence its issuance.
          }
          await signOutFirebaseBestEffort();
        };
        await offerAccountSwitch(target, resume, abandon);
        return;
      }
      if (
        (error instanceof SumiSessionCompensatedError ||
          error instanceof SumiSessionCompensationFailedError) &&
        isCurrentGeneration(generation)
      ) {
        let authorityCleared = true;
        flushSync(() => {
          authorityCleared = clearDirectChatAuthority();
          serverSession.current = { authenticated: false };
          setSession({ authenticated: false });
          setSessionState(
            authorityCleared && error instanceof SumiSessionCompensatedError
              ? "unauthenticated"
              : "unavailable",
          );
        });
      }
      // A Firebase account is display state, not Sumi authorization. Do not
      // retain it when the server-owned identity binding/exchange failed.
      if (firebaseSignInCompleted && !confirmationRequired) {
        await signOutFirebaseBestEffort();
      }
      if (isCurrentGeneration(generation)) setRedirectSignInError(error);
    } finally {
      signInPending.current = false;
      redirectCompletionActive.current = false;
      setRedirectSignInPending(false);
      const publishedSession =
        serverSession.current.authenticated && isCurrentGeneration(generation);
      if (!publishedSession) {
        // The startup read was deliberately deferred until the return settled.
        // It must still run when the exchange committed under a generation it
        // lost — the cookie may hold a session the UI never published, and a
        // competing operation's own read may have run inside the deferred
        // window and returned "checking" without reaching the server. It runs
        // in the background so a failed return keeps the login form mounted
        // with its typed state instead of flashing a checking screen.
        await refreshSession({ background: true });
      }
    }
  }, [
    exchangeFirebaseProof,
    isCurrentGeneration,
    nextGeneration,
    offerAccountSwitch,
    refreshSession,
  ]);

  useEffect(() => {
    if (!redirectSignInPending || redirectReturnClaimed.current) return;
    // Survives StrictMode's remount: the return is exchanged exactly once.
    redirectReturnClaimed.current = true;
    void completeRedirectSignIn();
  }, [completeRedirectSignIn, redirectSignInPending]);

  useEffect(() => {
    const onPageShow = (event: PageTransitionEvent) => {
      if (!event.persisted) return;
      // The page was restored from the back/forward cache after this tab
      // left for a provider. The navigation promise that held signInPending
      // can never settle now, so the hold must be released here or every
      // login stays disabled and refreshSession keeps returning "checking".
      // A begin still awaiting its flow registration belongs to a page the
      // person already left and returned to: it must not navigate this tab
      // away again once its server call resolves.
      redirectBeginCancelled.current = redirectBeginSeq.current;
      if (redirectCompletionActive.current) {
        // An in-flight completion resumes with the restored page and its
        // finally still releases the hold and settles the session.
        return;
      }
      if (!redirectReturnClaimed.current && hasPendingRedirectSignIn()) {
        // Back or a closed provider view returned before the result was
        // read: run the normal completion. A missing credential reports
        // the recoverable "not completed" error.
        setRedirectSignInPending(true);
        return;
      }
      if (signInPending.current) {
        signInPending.current = false;
        void refreshSession({ background: true });
      }
    };
    window.addEventListener("pageshow", onPageShow);
    return () => window.removeEventListener("pageshow", onPageShow);
  }, [refreshSession]);

  const signIn = useCallback(
    async (providerName: SignInProvider, intent: AuthIntent) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      nextGeneration();
      setCredentialRecoveryEmailSent(false);
      setRedirectSignInError(null);
      setAccountSwitch(null);
      // Each attempt writes a fresh receipt, so each return — including a
      // back/forward-cache restore of this same document — is a new return
      // that must be allowed to complete once. Resetting here, before the
      // tab can leave, cannot reopen the StrictMode replay window: the
      // completion effect still needs a pending receipt to claim.
      redirectReturnClaimed.current = false;
      // The tab is about to leave for the provider. Hold the session read so a
      // navigation that a browser delays cannot be mistaken for a logout.
      signInPending.current = true;
      const attempt = ++redirectBeginSeq.current;
      try {
        await beginRedirectSignIn({
          provider: authFlowProvider(providerName),
          intent,
          isAborted: () =>
            attempt !== redirectBeginSeq.current ||
            attempt <= redirectBeginCancelled.current,
        });
        // A real provider navigation never lets this promise settle; reaching
        // here means begin finished without leaving the tab. Release the
        // hold unless a newer attempt now owns it.
        if (attempt === redirectBeginSeq.current) signInPending.current = false;
      } catch (error) {
        if (attempt === redirectBeginSeq.current) signInPending.current = false;
        throw error;
      }
    },
    [nextGeneration],
  );

  const dismissRedirectSignInError = useCallback(() => {
    setRedirectSignInError(null);
  }, []);

  const startEmailCode = useCallback(
    async (email: string, intent: AuthIntent) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      nextGeneration();
      setCredentialRecoveryEmailSent(false);
      setAccountSwitch(null);
      signInPending.current = true;
      try {
        const started = await beginEmailCodeAuth(email, intent);
        setEmailCodeFlow(started.active);
        setEmailChallenge(started.challenge);
      } finally {
        signInPending.current = false;
      }
    },
    [nextGeneration],
  );

  const forgetEmailCodeFlow = useCallback((active: ActiveEmailCodeFlow) => {
    abandonEmailCodeFlow(active);
    setEmailCodeFlow((current) =>
      current?.state === active.state ? null : current,
    );
    setEmailChallenge(null);
    setCredentialRecoveryEmailSent(false);
  }, []);

  /**
   * A flow that finished, closed, or moved elsewhere cannot be retried here.
   * Another tab of this browser may already hold its session.
   */
  const settleEndedEmailFlow = useCallback(
    (active: ActiveEmailCodeFlow, error: unknown) => {
      if (!(error instanceof AuthAPIError)) return;
      if (
        error.message === "continued_in_other_browser" ||
        error.message === "flow_consumed" ||
        error.message === "flow_closed" ||
        error.message === "flow_expired" ||
        error.message === "invalid_flow"
      ) {
        forgetEmailCodeFlow(active);
      }
      if (
        error.message === "flow_consumed" ||
        error.message === "flow_closed"
      ) {
        void refreshSession({ background: true });
      }
    },
    [forgetEmailCodeFlow, refreshSession],
  );

  // A switch confirmation retries the interrupted completion later, through
  // whatever the current callback identity is.
  const completeEmailProofRef = useRef<
    (
      obtainProof: () => Promise<EmailProof>,
      options?: { switchFromUserId?: string },
    ) => Promise<void>
  >(() => Promise.resolve());

  /**
   * Mailbox proof errors leave the session untouched. After a proof, the
   * Firebase exchange and flow resolution share the provider compensation:
   * a failure there is retried with the same flow authority. A
   * session_active answer is not an error to bury — it asks the person
   * whether to replace the account this jar currently belongs to.
   */
  const completeEmailProof = useCallback(
    async (
      obtainProof: () => Promise<EmailProof>,
      options?: { switchFromUserId?: string },
    ) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      const generation = nextGeneration();
      signInPending.current = true;
      try {
        const proof = await obtainProof();
        try {
          const completed = await finishEmailProof(proof, options);
          await serializeSessionMutation(async () => {
            if (!isCurrentGeneration(generation)) return;
            let recoveryOutcome:
              | "provider_linked"
              | "provider_already_linked"
              | null = null;
            if (completed.flow.credentialRecovery) {
              if (completed.result.outcome !== "signed_in") {
                throw new AuthAPIError(
                  "Provider recovery requires an existing Sumi account.",
                  0,
                );
              }
              recoveryOutcome = await completeSameEmailCredentialRecovery({
                recovery: completed.flow.credentialRecovery,
                user: completed.firebaseUser,
              });
            }
            if (completed.result.outcome === "confirmation_required") {
              const pending: PendingAuthConfirmation = {
                flowId: completed.result.flowId,
                nonce: completed.flow.nonce,
                intent: completed.flow.intent,
                provider: "email_code",
                expiresAt: completed.result.expiresAt,
                action: completed.result.nextAction,
                firebaseUID: completed.firebaseUser.uid,
                account: {
                  displayName: completed.firebaseUser.displayName,
                  email: completed.firebaseUser.email,
                },
              };
              savePendingConfirmation(pending);
              setConfirmation(pending);
              setEmailCodeFlow(null);
              setEmailChallenge(null);
              setEmailLinkPending(false);
              return;
            }
            const nextSession = await verifyCommittedSumiSession({
              // If this flow committed a session the status read cannot
              // confirm, discard closes exactly this flow's authority and
              // revokes what it minted — never another account's session.
              compensate: () =>
                discardAuthFlow(completed.result.flowId, completed.flow.nonce),
            });
            if (
              completed.result.outcome !== "signed_in" &&
              completed.result.outcome !== "account_created"
            ) {
              throw new AuthAPIError(
                "Invalid authentication flow response.",
                0,
              );
            }
            if (nextSession.user.id !== completed.result.humanId) {
              // The session in this jar belongs to a different Human than
              // the one this flow resolved to — the person's later choice
              // stands unless they explicitly switch back.
              throw new AuthAPIError("session_active", 409);
            }
            const recoveryIntent =
              completed.flow.credentialRecovery?.requestedIntent ??
              completed.flow.intent;
            publishOutcomeNotice({
              firebaseUID: completed.firebaseUser.uid,
              humanId: nextSession.user.id,
              outcome:
                recoveryOutcome === "provider_linked"
                  ? "provider_linked"
                  : completed.result.outcome,
              intent: recoveryIntent,
              intentTransition:
                completed.flow.credentialRecovery &&
                recoveryIntent === "sign_up"
                  ? "recovery_proved"
                  : "none",
              receiptId: completed.result.flowId,
            });
            flushSync(() => {
              bindDirectChatAuthority(nextSession.authorityBindingId);
              serverSession.current = nextSession;
              if (!isCurrentGeneration(generation)) return;
              setSession(nextSession);
              setSessionState("authenticated");
              setEmailCodeFlow(null);
              setEmailChallenge(null);
              setEmailLinkPending(false);
              setCredentialRecoveryEmailSent(false);
            });
          });
        } catch (error) {
          if (isCurrentGeneration(generation)) {
            if (isSessionActiveError(error)) {
              const resume = completeEmailProofRef.current;
              await offerAccountSwitch(
                proof.active.flow.email,
                (switchFromUserId) =>
                  resume(() => Promise.resolve(proof), {
                    switchFromUserId: switchFromUserId || undefined,
                  }),
                async () => {
                  try {
                    await discardAuthFlow(
                      proof.active.flow.flowId,
                      proof.active.flow.nonce,
                    );
                  } catch {
                    // Epoch closure and expiry still fence its issuance.
                  }
                  forgetEmailCodeFlow(proof.active);
                },
              );
              return;
            }
            await reconcileSessionState();
          }
          await signOutFirebaseBestEffort();
          throw error;
        }
      } finally {
        signInPending.current = false;
      }
    },
    [
      forgetEmailCodeFlow,
      isCurrentGeneration,
      nextGeneration,
      offerAccountSwitch,
      publishOutcomeNotice,
      reconcileSessionState,
      serializeSessionMutation,
    ],
  );
  completeEmailProofRef.current = completeEmailProof;

  const submitEmailCode = useCallback(
    async (code: string) => {
      const active = emailCodeFlow;
      if (!active) {
        throw new AuthAPIError("invalid_flow", 400);
      }
      try {
        await completeEmailProof(() => verifyEmailCode(active, code));
      } catch (error) {
        settleEndedEmailFlow(active, error);
        throw error;
      }
    },
    [completeEmailProof, emailCodeFlow, settleEndedEmailFlow],
  );

  const resendEmailCode = useCallback(async () => {
    const active = emailCodeFlow;
    if (!active) throw new AuthAPIError("invalid_flow", 400);
    try {
      setEmailChallenge(await requestEmailCodeResend(active.flow));
    } catch (error) {
      settleEndedEmailFlow(active, error);
      throw error;
    }
  }, [emailCodeFlow, settleEndedEmailFlow]);

  const emailCompletionRecoveries = useRef(new Set<string>());

  const refreshEmailCode = useCallback(async () => {
    const active = emailCodeFlow;
    if (!active) return;
    try {
      const status = await readEmailCodeStatus(active.flow);
      setEmailChallenge(status);
      if (status.flowStatus !== "completed" || signInPending.current) return;
      // A flow settled by another tab of this browser left its session here.
      if (loadPendingEmailFlow(active.state) === null) {
        forgetEmailCodeFlow(active);
        void refreshSession({ background: true });
        return;
      }
      // Still stored and completed: this browser may have lost the completion
      // response. Recover it once through the server's replay; later polls
      // leave the session alone, and a manual retry after the window reports
      // flow_consumed, which ends the flow.
      if (emailCompletionRecoveries.current.has(active.state)) return;
      emailCompletionRecoveries.current.add(active.state);
      let live: SumiSessionStatus;
      try {
        live = await getSumiSession();
      } catch {
        live = { authenticated: false };
      }
      if (live.authenticated) {
        if (status.humanId && live.user.id !== status.humanId) {
          // The completed sign-in resolved to a different Human than the one
          // this jar now belongs to. Only an explicit switch replaces the
          // person's later choice; the flow keeps its authority meanwhile.
          const resume = completeEmailProofRef.current;
          await offerAccountSwitch(
            active.flow.email,
            (switchFromUserId) =>
              resume(() => verifyEmailCode(active, ""), {
                switchFromUserId: switchFromUserId || undefined,
              }),
            async () => {
              try {
                await discardAuthFlow(active.flow.flowId, active.flow.nonce);
              } catch {
                // Epoch closure and expiry still fence its issuance.
              }
              forgetEmailCodeFlow(active);
            },
          );
          return;
        }
        forgetEmailCodeFlow(active);
        void refreshSession({ background: true });
        return;
      }
      await completeEmailProofRef.current(() => verifyEmailCode(active, ""));
    } catch (error) {
      settleEndedEmailFlow(active, error);
      throw error;
    }
  }, [
    emailCodeFlow,
    forgetEmailCodeFlow,
    offerAccountSwitch,
    refreshSession,
    settleEndedEmailFlow,
  ]);

  const cancelEmailCode = useCallback(() => {
    nextGeneration();
    if (emailCodeFlow) {
      const { flow } = emailCodeFlow;
      forgetEmailCodeFlow(emailCodeFlow);
      // Cancelling abandons the flow: close its server-side issuance so a
      // lost completion cannot deliver a session to this jar later.
      void discardAuthFlow(flow.flowId, flow.nonce).catch(() => undefined);
    }
  }, [emailCodeFlow, forgetEmailCodeFlow, nextGeneration]);

  const inspectEmailLink = useCallback(async () => {
    const link = pendingEmailLink();
    if (!link) throw new AuthAPIError("link_invalid", 404);
    return inspectPendingEmailLink(link);
  }, []);

  const continueEmailLink = useCallback(
    async (
      inspection: EmailLinkInspection,
      adopt: boolean,
      options?: { switchFromUserId?: string },
    ) => {
      const link = pendingEmailLink();
      if (!link) throw new AuthAPIError("link_invalid", 404);
      await completeEmailProof(
        () => completeEmailLinkProof(link, inspection, adopt),
        options,
      );
    },
    [completeEmailProof],
  );

  const dismissEmailLink = useCallback(() => {
    clearPendingEmailLink();
    setEmailLinkPending(false);
    // A proof finished in this tab may have been saved as the active flow.
    setEmailCodeFlow(loadActiveEmailCodeFlow());
  }, []);

  /**
   * Runs the confirmation exchange for a saved pending confirmation. Split
   * from the context method so an account-switch confirmation can retry the
   * same step with switch_from_user_id.
   */
  const runIntentConfirmation = useCallback(
    async (pending: PendingAuthConfirmation, switchFromUserId?: string) => {
      const generation = nextGeneration();
      await serializeSessionMutation(async () => {
        const auth = getFirebaseAuth();
        await auth.authStateReady();
        const firebaseUser = auth.currentUser;
        if (!firebaseUser || firebaseUser.uid !== pending.firebaseUID) {
          clearPendingConfirmation();
          setConfirmation(null);
          throw new AuthAPIError(
            "Firebase account changed before confirmation.",
            0,
          );
        }
        const request = {
          flowId: pending.flowId,
          nonce: pending.nonce,
          idToken: await getIdToken(firebaseUser, true),
        };
        const refreshed = await resolveAuthFlow({
          ...request,
          switchFromUserId,
        });
        let confirmed: Awaited<ReturnType<typeof confirmAuthFlow>>;
        if (
          auth.currentUser?.uid === pending.firebaseUID &&
          (refreshed.outcome === "signed_in" ||
            refreshed.outcome === "account_created")
        ) {
          // The confirmation committed earlier but its issuance was refused
          // or its response was lost; this resolve replayed the same
          // completion for any channel.
          confirmed = refreshed;
        } else {
          if (
            refreshed.outcome !== "confirmation_required" ||
            refreshed.nextAction !== pending.action ||
            auth.currentUser?.uid !== pending.firebaseUID
          ) {
            clearPendingConfirmation();
            setConfirmation(null);
            throw new AuthAPIError(
              "Authentication confirmation is no longer valid.",
              0,
            );
          }
          confirmed = await confirmAuthFlow({
            flowId: pending.flowId,
            nonce: pending.nonce,
            action: pending.action,
            switchFromUserId,
          });
        }
        if (
          !isCurrentGeneration(generation) ||
          auth.currentUser?.uid !== pending.firebaseUID
        ) {
          const identityError = new AuthAPIError(
            "Firebase account changed during confirmation.",
            0,
          );
          try {
            await logoutSumiSession();
          } catch (logoutError) {
            flushSync(() => {
              clearDirectChatAuthority();
              serverSession.current = { authenticated: false };
              clearPendingConfirmation();
              setConfirmation(null);
              setSession({ authenticated: false });
              setSessionState("unavailable");
            });
            throw new SumiSessionCompensationFailedError(
              identityError,
              logoutError,
            );
          }
          flushSync(() => {
            const authorityCleared = clearDirectChatAuthority();
            serverSession.current = { authenticated: false };
            clearPendingConfirmation();
            setConfirmation(null);
            setSession({ authenticated: false });
            setSessionState(
              authorityCleared ? "unauthenticated" : "unavailable",
            );
          });
          throw new SumiSessionCompensatedError(identityError);
        }
        if (pending.provider === "email_code") {
          await ensureEmailCompletionSession(request, confirmed, {
            switchFromUserId,
          });
        }
        const nextSession = await verifyCommittedSumiSession({
          compensate: () => discardAuthFlow(pending.flowId, pending.nonce),
        });
        if (nextSession.user.id !== confirmed.humanId) {
          throw new AuthAPIError("session_active", 409);
        }
        publishOutcomeNotice({
          firebaseUID: firebaseUser.uid,
          humanId: nextSession.user.id,
          outcome: confirmed.outcome,
          intent: pending.intent,
          intentTransition: "confirmed",
          receiptId: confirmed.flowId,
        });
        flushSync(() => {
          bindDirectChatAuthority(nextSession.authorityBindingId);
          serverSession.current = nextSession;
          clearPendingConfirmation();
          setConfirmation(null);
          if (!isCurrentGeneration(generation)) return;
          setSession(nextSession);
          setSessionState("authenticated");
        });
      });
    },
    [
      isCurrentGeneration,
      nextGeneration,
      publishOutcomeNotice,
      serializeSessionMutation,
    ],
  );

  const confirmIntentTransition = useCallback(
    async (options?: { switchFromUserId?: string }) => {
      const pending = confirmation;
      if (!pending || preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError(
          "Authentication confirmation is unavailable.",
          0,
        );
      }
      try {
        await runIntentConfirmation(pending, options?.switchFromUserId);
      } catch (error) {
        if (isSessionActiveError(error)) {
          await offerAccountSwitch(
            pending.account.displayName ??
              pending.account.email ??
              "このアカウント",
            (switchFromUserId) =>
              runIntentConfirmation(pending, switchFromUserId || undefined),
            async () => {
              try {
                await discardAuthFlow(pending.flowId, pending.nonce);
              } catch {
                // The flow's own expiry still fences its issuance.
              }
              clearPendingConfirmation();
              setConfirmation(null);
              await signOutFirebaseBestEffort();
            },
          );
          return;
        }
        throw error;
      }
    },
    [confirmation, offerAccountSwitch, runIntentConfirmation],
  );

  const cancelIntentTransition = useCallback(async () => {
    nextGeneration();
    const pending = confirmation;
    clearPendingConfirmation();
    setConfirmation(null);
    if (pending) {
      // Cancelling abandons the flow: close its server-side issuance.
      try {
        await discardAuthFlow(pending.flowId, pending.nonce);
      } catch {
        // The flow's own expiry still fences its issuance.
      }
    }
    await signOutFirebaseBestEffort();
  }, [confirmation, nextGeneration]);

  const dismissOutcomeNotice = useCallback(() => {
    clearAuthOutcomeNotice();
    setOutcomeNotice(null);
  }, []);

  const updateProfile = useCallback(
    async (patch: SumiProfilePatch): Promise<ConfirmedSumiProfile | null> => {
      const generation = nextGeneration();
      return serializeSessionMutation(async () => {
        if (!isCurrentGeneration(generation)) return null;
        const current = serverSession.current;
        if (!current.authenticated) {
          throw new AuthAPIError("Authentication is unavailable.", 401);
        }
        const requested: SumiProfilePatch = {};
        if (patch.displayName !== undefined) {
          requested.displayName = canonicalizeSumiDisplayName(
            patch.displayName,
          );
        }
        if (patch.tagline !== undefined) {
          requested.tagline = patch.tagline.trim();
        }
        let updatedUser: Awaited<ReturnType<typeof updateSumiProfile>>;
        try {
          updatedUser = await updateSumiProfile(requested);
        } catch (error) {
          if (!isCurrentGeneration(generation)) return null;
          if (isDefinitiveProfileUpdateRejection(error)) {
            throw error;
          }
          let reconciled: SumiSessionStatus;
          try {
            reconciled = await getSumiSession();
          } catch (reconciliationError) {
            if (!isCurrentGeneration(generation)) return null;
            throw new SumiProfileUpdateIndeterminateError(
              new AggregateError(
                [error, reconciliationError],
                "Profile update and reconciliation both failed.",
              ),
            );
          }
          if (!isCurrentGeneration(generation)) return null;
          if (
            !reconciled.authenticated ||
            reconciled.user.id !== current.user.id
          ) {
            flushSync(() => {
              const authorityCleared = clearDirectChatAuthority();
              serverSession.current = { authenticated: false };
              setSession({ authenticated: false });
              setSessionState(
                !reconciled.authenticated && authorityCleared
                  ? "unauthenticated"
                  : "unavailable",
              );
            });
            throw new SumiProfileUpdateIndeterminateError(error);
          }
          try {
            flushSync(() => {
              if (
                reconciled.authorityBindingId !== current.authorityBindingId
              ) {
                bindDirectChatAuthority(reconciled.authorityBindingId);
              }
              serverSession.current = reconciled;
              setSession(reconciled);
              setSessionState("authenticated");
            });
          } catch (bindingError) {
            flushSync(() => {
              clearDirectChatAuthority();
              serverSession.current = { authenticated: false };
              setSession({ authenticated: false });
              setSessionState("unavailable");
            });
            throw new SumiProfileUpdateIndeterminateError(
              new AggregateError(
                [error, bindingError],
                "Profile reconciliation could not replace browser authority.",
              ),
            );
          }
          let reconciledProfile: ConfirmedSumiProfile;
          try {
            reconciledProfile = await getSumiProfile();
          } catch (reconciliationError) {
            if (!isCurrentGeneration(generation)) return null;
            throw new SumiProfileUpdateIndeterminateError(
              new AggregateError(
                [error, reconciliationError],
                "Profile update and reconciliation both failed.",
              ),
            );
          }
          if (!isCurrentGeneration(generation)) return null;
          if (reconciledProfile.participant.humanId !== current.user.id) {
            throw new SumiProfileUpdateIndeterminateError(error);
          }
          const reconciledSession: SumiSessionStatus = {
            ...reconciled,
            user: {
              ...reconciled.user,
              displayName: reconciledProfile.displayName,
            },
          };
          serverSession.current = reconciledSession;
          setSession(reconciledSession);
          setSessionState("authenticated");
          const displayNameMatches =
            requested.displayName === undefined ||
            canonicalizeSumiDisplayName(reconciledProfile.displayName) ===
              requested.displayName;
          const taglineMatches =
            requested.tagline === undefined ||
            reconciledProfile.tagline === requested.tagline;
          if (displayNameMatches && taglineMatches) {
            return reconciledProfile;
          }
          throw new SumiProfileUpdateIndeterminateError(error);
        }
        if (!isCurrentGeneration(generation)) return null;
        if (updatedUser.id !== current.user.id) {
          throw new AuthAPIError("Profile identity changed.", 409);
        }
        const nextSession: SumiSessionStatus = {
          ...current,
          user: updatedUser,
        };
        serverSession.current = nextSession;
        setSession(nextSession);
        return updatedUser.profile;
      });
    },
    [isCurrentGeneration, nextGeneration, serializeSessionMutation],
  );

  const updateDisplayName = useCallback(
    async (displayName: string) => {
      await updateProfile({ displayName });
    },
    [updateProfile],
  );

  /**
   * The local half of a committed session end, shared by explicit logout and
   * a logout another tab announced. The caller must already have established
   * that the server session is gone; generation claiming stays with the
   * caller. Every pending flow is dropped locally so nothing can replay it
   * into a session.
   */
  const teardownSessionState = useCallback((): boolean => {
    const pendingFlows = listPendingEmailFlows();
    let authorityCleared = true;
    flushSync(() => {
      authorityCleared = clearDirectChatAuthority();
      sessionRevalidationRequired.current = false;
      setSessionSuspended(false);
      serverSession.current = { authenticated: false };
      setSession({ authenticated: false });
      setSessionState(authorityCleared ? "unauthenticated" : "unavailable");
      clearPendingConfirmation();
      setConfirmation(null);
      for (const flow of pendingFlows) {
        clearPendingEmailFlow(flow.state);
      }
      if (emailCodeFlow) {
        clearActiveEmailFlowState(emailCodeFlow.state);
      }
      setEmailCodeFlow(null);
      setEmailChallenge(null);
      clearPendingEmailLink();
      setEmailLinkPending(false);
      clearPendingRedirectFlow();
      clearPendingProviderRedirect();
      emailCompletionRecoveries.current.clear();
      setAccountSwitch(null);
      setCredentialRecoveryEmailSent(false);
      clearAuthOutcomeNotice();
      setOutcomeNotice(null);
    });
    return authorityCleared;
  }, [emailCodeFlow]);

  const logout = useCallback(async () => {
    if (logoutPending.current) return;
    const generation = nextGeneration();
    clearAuthOutcomeNotice();
    setOutcomeNotice(null);
    logoutPending.current = true;
    sessionRevalidationRequired.current = true;
    // Pause authenticated effects and dispose Messaging before the request can
    // clear its cookie. Local forms remain mounted inside the hidden Activity.
    flushSync(() => {
      setSessionSuspended(true);
      setSessionState("checking");
    });
    try {
      await serializeSessionMutation(async () => {
        // Logout ends this jar's work: the epoch covers most of it, but
        // flows bound to an epoch this jar no longer presents are named
        // explicitly so the server can still close them by nonce.
        const flows: Array<{ flowId: string; nonce: string }> =
          listPendingEmailFlows().map(({ flowId, nonce }) => ({
            flowId,
            nonce,
          }));
        const pending = confirmation;
        if (pending) {
          flows.push({ flowId: pending.flowId, nonce: pending.nonce });
        }
        const redirect = loadPendingRedirectFlow();
        if (redirect) {
          flows.push({ flowId: redirect.flowId, nonce: redirect.nonce });
        }
        await logoutSumiSession(flows);
      });
    } catch (error) {
      logoutPending.current = false;
      if (!isCurrentGeneration(generation)) return;
      await refreshSession();
      throw error;
    }
    logoutPending.current = false;
    // The server session ended: other live tabs of this browser must drop the
    // same identity rather than keep showing a session that no longer exists.
    publishSessionEnded(authBroadcastSender);
    startPushSubscriptionLogoutCleanup();
    let authorityCleared = true;
    if (isCurrentGeneration(generation)) {
      // Server logout is the authority transition. Commit it before touching
      // optional Firebase/emulator display-state cleanup, which may throw
      // synchronously during setup.
      authorityCleared = teardownSessionState();
    }
    await signOutFirebaseBestEffort();
    if (!authorityCleared) {
      throw new Error("Direct-chat private state could not be cleared");
    }
  }, [
    confirmation,
    isCurrentGeneration,
    nextGeneration,
    refreshSession,
    serializeSessionMutation,
    teardownSessionState,
  ]);

  /**
   * Another live tab of this browser ended the session. Verify the jar's real
   * state before acting: a sign-in that already replaced the session wins and
   * is adopted; otherwise run the same local teardown a local logout
   * performs, including the Firebase sign-out a delayed provider lookup can
   * otherwise reinstall.
   */
  const honourRemoteSessionEnd = useCallback(async () => {
    if (preissuedSessionMode || !authOriginAllowed) return;
    // A local explicit logout already owns the same teardown.
    if (logoutPending.current) return;
    await serializeSessionMutation(async () => {
      let live: SumiSessionStatus;
      try {
        live = await getSumiSession();
      } catch {
        // The broadcast proves the old session ended; a read we cannot
        // complete cannot prove a replacement session exists.
        live = { authenticated: false };
      }
      if (live.authenticated) {
        if (
          serverSession.current.authenticated &&
          serverSession.current.user.id === live.user.id
        ) {
          return;
        }
        // The jar's newer session — another tab already signed in again or
        // switched — is adopted instead of torn down.
        flushSync(() => {
          bindDirectChatAuthority(live.authorityBindingId);
          sessionRevalidationRequired.current = false;
          setSessionSuspended(false);
          serverSession.current = live;
          setSession(live);
          setSessionState("authenticated");
        });
        return;
      }
      // Claim the generation exactly as an explicit logout does, so in-flight
      // completions cannot republish the ended identity, then run the same
      // local teardown.
      nextGeneration();
      teardownSessionState();
      await signOutFirebaseBestEffort();
    });
  }, [nextGeneration, serializeSessionMutation, teardownSessionState]);

  useEffect(() => {
    if (preissuedSessionMode || !authOriginAllowed) return;
    return subscribeSessionEnded(authBroadcastSender, () => {
      void honourRemoteSessionEnd();
    });
  }, [honourRemoteSessionEnd]);

  const user = useMemo<AuthUser | null>(() => {
    if (sessionState === "preissued" && preissuedUserID) {
      return {
        id: preissuedUserID,
        displayName: null,
        email: null,
        photoURL: null,
      };
    }
    if (!session.authenticated) {
      return null;
    }
    return {
      id: session.user.id,
      displayName: session.user.displayName,
      email: null,
      photoURL: null,
    };
  }, [session, sessionState]);

  const emailCode = useMemo<EmailCodeView | null>(
    () =>
      emailCodeFlow
        ? {
            email: emailCodeFlow.flow.email,
            intent: emailCodeFlow.flow.intent,
            recovery: emailCodeFlow.flow.credentialRecovery !== undefined,
            challenge: emailChallenge,
          }
        : null,
    [emailChallenge, emailCodeFlow],
  );

  const authorityBindingId =
    sessionState === "authenticated" && session.authenticated
      ? session.authorityBindingId
      : null;

  const value = useMemo<AuthContextValue>(
    () => ({
      configured: isFirebaseConfigured,
      loading: sessionState === "checking",
      sessionState,
      sessionSuspended,
      authenticated: sessionState === "authenticated" && session.authenticated,
      canUseDirectChat:
        sessionState === "authenticated" || sessionState === "preissued",
      authorityBindingId,
      user,
      confirmation,
      outcomeNotice,
      emailCode,
      emailLinkPending,
      accountSwitch,
      confirmAccountSwitch,
      cancelAccountSwitch,
      credentialRecoveryEmailSent,
      redirectSignInPending,
      redirectSignInError,
      dismissRedirectSignInError,
      signIn,
      startEmailCode,
      submitEmailCode,
      resendEmailCode,
      refreshEmailCode,
      cancelEmailCode,
      inspectEmailLink,
      continueEmailLink,
      dismissEmailLink,
      confirmIntentTransition,
      cancelIntentTransition,
      dismissOutcomeNotice,
      updateProfile,
      updateDisplayName,
      logout,
      refreshSession,
    }),
    [
      accountSwitch,
      authorityBindingId,
      cancelAccountSwitch,
      cancelIntentTransition,
      confirmation,
      cancelEmailCode,
      confirmAccountSwitch,
      confirmIntentTransition,
      continueEmailLink,
      dismissEmailLink,
      emailCode,
      emailLinkPending,
      inspectEmailLink,
      logout,
      refreshEmailCode,
      resendEmailCode,
      startEmailCode,
      submitEmailCode,
      credentialRecoveryEmailSent,
      dismissOutcomeNotice,
      dismissRedirectSignInError,
      redirectSignInError,
      redirectSignInPending,
      refreshSession,
      session.authenticated,
      sessionState,
      sessionSuspended,
      signIn,
      outcomeNotice,
      user,
      updateProfile,
      updateDisplayName,
    ],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const value = useContext(AuthContext);
  if (!value) {
    throw new Error("useAuth must be used inside AuthProvider.");
  }
  return value;
}

export function classifySessionFailure(error: unknown): AuthSessionState {
  if (error instanceof AuthAPIError) {
    if (error.status === 401 || error.status === 403) {
      return "unauthenticated";
    }
  }
  return "unavailable";
}

function isSessionActiveError(error: unknown): boolean {
  return (
    error instanceof AuthAPIError &&
    error.status === 409 &&
    error.message === "session_active"
  );
}

function isDefinitiveProfileUpdateRejection(error: unknown): boolean {
  return (
    error instanceof AuthAPIError &&
    error.status >= 400 &&
    error.status < 500 &&
    error.status !== 408 &&
    error.status !== 429
  );
}

function authFlowProvider(providerName: SignInProvider): RecoverableProvider {
  return providerName === "github" ? "github.com" : "google.com";
}

function firebaseAccount(user: {
  displayName: string | null;
  email: string | null;
}): PendingAuthConfirmation["account"] {
  return { displayName: user.displayName, email: user.email };
}

async function signOutFirebaseBestEffort(): Promise<void> {
  try {
    const auth = getFirebaseAuth();
    // Mark the identity being torn down before awaiting so an in-flight
    // provider operation that still holds this account can honour the
    // transition instead of persisting it again.
    noteAuthTeardown(auth.currentUser?.uid);
    await signOut(auth).catch(() => undefined);
  } catch {
    // Firebase is cleanup-only after Sumi authority has ended.
  }
}
