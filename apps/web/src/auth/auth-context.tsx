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
  resolveAuthFlow,
} from "./auth-flow-client";
import type {
  PendingRedirectAuthFlow,
  RecoverableProvider,
} from "./auth-flow-state";
import {
  type AuthOutcomeNotice,
  clearAuthOutcomeNotice,
  hasPendingAuthOutcomeNotice,
  publishAuthOutcomeNotice,
  takeAuthOutcomeNotice,
} from "./auth-outcome-notice-state";
import {
  beginSameEmailCredentialRecovery,
  completeSameEmailCredentialRecovery,
  isSameEmailCredentialCollision,
} from "./credential-recovery";
import {
  beginEmailLinkAuth,
  completeEmailLinkAuth,
  hasEmailLinkCallback,
  rejectEmailLinkAuth,
} from "./email-link-auth";
import { getFirebaseAuth } from "./firebase";
import { isFirebaseConfigured } from "./firebase-config";
import {
  beginRedirectSignIn,
  hasPendingRedirectSignIn,
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
  emailLinkCallbackPending: boolean;
  credentialRecoveryEmailSent: boolean;
  redirectSignInPending: boolean;
  redirectSignInError: unknown;
  dismissRedirectSignInError: () => void;
  signIn: (provider: SignInProvider, intent: AuthIntent) => Promise<void>;
  sendEmailLink: (email: string, intent: AuthIntent) => Promise<void>;
  completeEmailLink: () => Promise<void>;
  rejectEmailLink: () => void;
  confirmIntentTransition: () => Promise<void>;
  cancelIntentTransition: () => Promise<void>;
  dismissOutcomeNotice: () => void;
  updateProfile: (
    patch: SumiProfilePatch,
  ) => Promise<ConfirmedSumiProfile | null>;
  updateDisplayName: (displayName: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshSession: () => Promise<AuthSessionState>;
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
  const [confirmation, setConfirmation] =
    useState<PendingAuthConfirmation | null>(() => loadPendingConfirmation());
  const [outcomeNotice, setOutcomeNotice] = useState<AuthOutcomeNotice | null>(
    null,
  );
  const [emailLinkCallbackPending, setEmailLinkCallbackPending] = useState(() =>
    hasEmailLinkCallback(),
  );
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

  const refreshSession = useCallback(async (): Promise<AuthSessionState> => {
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
    if (
      !serverSession.current.authenticated ||
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
      if (nextState === "unavailable" && serverSession.current.authenticated) {
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
  }, [claimSavedOutcomeNotice, isCurrentGeneration, nextGeneration]);

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
   * Turns a proven Firebase credential into the server-owned Sumi session.
   * Shared by the redirect return and any later provider proof: a Firebase UID
   * alone never authorizes a session.
   */
  const exchangeFirebaseProof = useCallback(
    async ({
      generation,
      flow,
      user,
    }: {
      generation: number;
      flow: PendingRedirectAuthFlow;
      user: User;
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
        const nextSession = await verifyCommittedSumiSession();
        if (
          resolved.outcome !== "signed_in" &&
          resolved.outcome !== "account_created"
        ) {
          throw new AuthAPIError("Invalid authentication flow response.", 0);
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

  /**
   * Completes a provider redirect once, on startup. The persisted receipt —
   * not the Firebase account — names the flow whose proof may be exchanged.
   */
  const completeRedirectSignIn = useCallback(async () => {
    const generation = nextGeneration();
    signInPending.current = true;
    let firebaseSignInCompleted = false;
    let confirmationRequired = false;
    try {
      const flow = takePendingRedirectSignIn();
      if (!flow) return;
      let user: User;
      try {
        user = await resolveRedirectSignInUser();
      } catch (error) {
        if (
          isSameEmailCredentialCollision(error) &&
          isCurrentGeneration(generation)
        ) {
          await beginSameEmailCredentialRecovery(
            error,
            flow.provider,
            flow.intent,
          );
          if (isCurrentGeneration(generation)) {
            setCredentialRecoveryEmailSent(true);
          }
          return;
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
      setRedirectSignInPending(false);
      const publishedSession =
        serverSession.current.authenticated && isCurrentGeneration(generation);
      if (!publishedSession) {
        // The startup read was deliberately deferred until the return settled.
        // It must still run when the exchange committed under a generation it
        // lost — the cookie may hold a session the UI never published, and a
        // competing operation's own read may have run inside the deferred
        // window and returned "checking" without reaching the server.
        await refreshSession();
      }
    }
  }, [
    exchangeFirebaseProof,
    isCurrentGeneration,
    nextGeneration,
    refreshSession,
  ]);

  useEffect(() => {
    if (!redirectSignInPending || redirectReturnClaimed.current) return;
    // Survives StrictMode's remount: the return is exchanged exactly once.
    redirectReturnClaimed.current = true;
    void completeRedirectSignIn();
  }, [completeRedirectSignIn, redirectSignInPending]);

  const signIn = useCallback(
    async (providerName: SignInProvider, intent: AuthIntent) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      nextGeneration();
      setCredentialRecoveryEmailSent(false);
      setRedirectSignInError(null);
      // The tab is about to leave for the provider. Hold the session read so a
      // navigation that a browser delays cannot be mistaken for a logout.
      signInPending.current = true;
      try {
        await beginRedirectSignIn({
          provider: authFlowProvider(providerName),
          intent,
        });
      } catch (error) {
        signInPending.current = false;
        throw error;
      }
    },
    [nextGeneration],
  );

  const dismissRedirectSignInError = useCallback(() => {
    setRedirectSignInError(null);
  }, []);

  const sendEmailLink = useCallback(
    async (email: string, intent: AuthIntent) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      nextGeneration();
      setCredentialRecoveryEmailSent(false);
      signInPending.current = true;
      try {
        await beginEmailLinkAuth(email, intent);
      } finally {
        signInPending.current = false;
      }
    },
    [nextGeneration],
  );

  const completeEmailLink = useCallback(async () => {
    if (preissuedSessionMode || !authOriginAllowed) {
      throw new AuthAPIError("Authentication is unavailable.", 0);
    }
    const generation = nextGeneration();
    signInPending.current = true;
    try {
      const completed = await completeEmailLinkAuth();
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
            provider: "email_link",
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
          setEmailLinkCallbackPending(false);
          return;
        }
        const nextSession = await verifyCommittedSumiSession();
        if (
          completed.result.outcome !== "signed_in" &&
          completed.result.outcome !== "account_created"
        ) {
          throw new AuthAPIError("Invalid authentication flow response.", 0);
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
            completed.flow.credentialRecovery && recoveryIntent === "sign_up"
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
          setEmailLinkCallbackPending(false);
          setCredentialRecoveryEmailSent(false);
        });
      });
    } catch (error) {
      if (isCurrentGeneration(generation)) {
        let logoutCompleted = true;
        try {
          await logoutSumiSession();
        } catch {
          logoutCompleted = false;
        }
        let authorityCleared = true;
        flushSync(() => {
          authorityCleared = clearDirectChatAuthority();
          serverSession.current = { authenticated: false };
          setSession({ authenticated: false });
          setSessionState(
            logoutCompleted && authorityCleared
              ? "unauthenticated"
              : "unavailable",
          );
        });
      }
      await signOutFirebaseBestEffort();
      throw error;
    } finally {
      signInPending.current = false;
    }
  }, [
    isCurrentGeneration,
    nextGeneration,
    publishOutcomeNotice,
    serializeSessionMutation,
  ]);

  const rejectEmailLink = useCallback(() => {
    rejectEmailLinkAuth();
    setEmailLinkCallbackPending(false);
    setCredentialRecoveryEmailSent(false);
  }, []);

  const confirmIntentTransition = useCallback(async () => {
    const pending = confirmation;
    if (!pending || preissuedSessionMode || !authOriginAllowed) {
      throw new AuthAPIError("Authentication confirmation is unavailable.", 0);
    }
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
      const idToken = await getIdToken(firebaseUser, true);
      const refreshed = await resolveAuthFlow({
        flowId: pending.flowId,
        nonce: pending.nonce,
        idToken,
      });
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
      const confirmed = await confirmAuthFlow({
        flowId: pending.flowId,
        nonce: pending.nonce,
        action: pending.action,
      });
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
          setSessionState(authorityCleared ? "unauthenticated" : "unavailable");
        });
        throw new SumiSessionCompensatedError(identityError);
      }
      const nextSession = await verifyCommittedSumiSession();
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
  }, [
    confirmation,
    isCurrentGeneration,
    nextGeneration,
    publishOutcomeNotice,
    serializeSessionMutation,
  ]);

  const cancelIntentTransition = useCallback(async () => {
    nextGeneration();
    clearPendingConfirmation();
    setConfirmation(null);
    await signOutFirebaseBestEffort();
  }, [nextGeneration]);

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
        await logoutSumiSession();
      });
    } catch (error) {
      logoutPending.current = false;
      if (!isCurrentGeneration(generation)) return;
      await refreshSession();
      throw error;
    }
    logoutPending.current = false;
    startPushSubscriptionLogoutCleanup();
    let authorityCleared = true;
    if (isCurrentGeneration(generation)) {
      // Server logout is the authority transition. Commit it before touching
      // optional Firebase/emulator display-state cleanup, which may throw
      // synchronously during setup.
      flushSync(() => {
        authorityCleared = clearDirectChatAuthority();
        sessionRevalidationRequired.current = false;
        setSessionSuspended(false);
        serverSession.current = { authenticated: false };
        setSession({ authenticated: false });
        setSessionState(authorityCleared ? "unauthenticated" : "unavailable");
      });
    }
    await signOutFirebaseBestEffort();
    if (!authorityCleared) {
      throw new Error("Direct-chat private state could not be cleared");
    }
  }, [
    isCurrentGeneration,
    nextGeneration,
    refreshSession,
    serializeSessionMutation,
  ]);

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
      emailLinkCallbackPending,
      credentialRecoveryEmailSent,
      redirectSignInPending,
      redirectSignInError,
      dismissRedirectSignInError,
      signIn,
      sendEmailLink,
      completeEmailLink,
      rejectEmailLink,
      confirmIntentTransition,
      cancelIntentTransition,
      dismissOutcomeNotice,
      updateProfile,
      updateDisplayName,
      logout,
      refreshSession,
    }),
    [
      authorityBindingId,
      cancelIntentTransition,
      confirmation,
      completeEmailLink,
      confirmIntentTransition,
      logout,
      emailLinkCallbackPending,
      credentialRecoveryEmailSent,
      dismissOutcomeNotice,
      dismissRedirectSignInError,
      redirectSignInError,
      redirectSignInPending,
      rejectEmailLink,
      refreshSession,
      session.authenticated,
      sessionState,
      sessionSuspended,
      sendEmailLink,
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
    await signOut(auth).catch(() => undefined);
  } catch {
    // Firebase is cleanup-only after Sumi authority has ended.
  }
}
