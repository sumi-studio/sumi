import {
  type AuthProvider as FirebaseAuthProvider,
  GithubAuthProvider,
  GoogleAuthProvider,
  getIdToken,
  onAuthStateChanged,
  signInWithPopup,
  signOut,
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
import { useMessaging } from "../messaging/store";
import {
  clearPendingConfirmation,
  loadPendingConfirmation,
  type PendingAuthConfirmation,
  savePendingConfirmation,
} from "./auth-confirmation-state";
import {
  type AuthIntent,
  confirmAuthFlow,
  createAuthFlowNonce,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";
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
  AuthAPIError,
  canonicalizeSumiDisplayName,
  getSumiSession,
  logoutSumiSession,
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
  authenticated: boolean;
  canUseDirectChat: boolean;
  authorityBindingId: string | null;
  user: AuthUser | null;
  confirmation: PendingAuthConfirmation | null;
  outcomeNotice: AuthOutcomeNotice | null;
  emailLinkCallbackPending: boolean;
  credentialRecoveryEmailSent: boolean;
  signIn: (provider: SignInProvider, intent: AuthIntent) => Promise<void>;
  sendEmailLink: (email: string, intent: AuthIntent) => Promise<void>;
  completeEmailLink: () => Promise<void>;
  rejectEmailLink: () => void;
  confirmIntentTransition: () => Promise<void>;
  cancelIntentTransition: () => Promise<void>;
  dismissOutcomeNotice: () => void;
  updateDisplayName: (displayName: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshSession: () => Promise<AuthSessionState>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

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
  const signInPending = useRef(false);
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
    // Firebase popups can remain open while a component effect runs. A server
    // read during that interval must not cancel the popup's eventual exchange.
    if (signInPending.current) return "checking";
    const generation = nextGeneration();
    setSessionState("checking");
    try {
      const nextSession = await getSumiSession();
      if (!isCurrentGeneration(generation)) return "checking";
      serverSession.current = nextSession;
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
        setSession(nextSession);
        setSessionState(nextState);
      });
      if (nextSession.authenticated) claimSavedOutcomeNotice(nextSession);
      else {
        clearAuthOutcomeNotice();
        setOutcomeNotice(null);
      }
      return nextState;
    } catch (error) {
      if (!isCurrentGeneration(generation)) return "checking";
      setSession({ authenticated: false });
      const nextState = classifySessionFailure(error);
      setSessionState(nextState);
      return nextState;
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

  const signIn = useCallback(
    async (providerName: SignInProvider, intent: AuthIntent) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      const generation = nextGeneration();
      const auth = getFirebaseAuth();
      const provider = createProvider(providerName);
      const flowProvider = authFlowProvider(providerName);
      const nonce = createAuthFlowNonce();
      let firebaseSignInCompleted = false;
      let confirmationRequired = false;
      signInPending.current = true;
      setCredentialRecoveryEmailSent(false);
      let popup: ReturnType<typeof signInWithPopup> | null = null;
      try {
        // Firebase documents that popup auth may be blocked when invoked outside
        // a click handler. Invoke it before the first await, while starting the
        // persisted Sumi flow concurrently; proof resolution still waits for both.
        popup = signInWithPopup(auth, provider);
        const [started, result] = await Promise.all([
          startAuthFlow({
            intent,
            provider: flowProvider,
            continuation: "/",
            nonce,
          }),
          popup,
        ]);
        firebaseSignInCompleted = true;
        if (!isCurrentGeneration(generation)) {
          await signOut(auth).catch(() => undefined);
          return;
        }
        await serializeSessionMutation(async () => {
          if (!isCurrentGeneration(generation)) return;
          const idToken = await getIdToken(result.user, true);
          if (!isCurrentGeneration(generation)) return;
          const resolved = await resolveAuthFlow({
            flowId: started.flowId,
            nonce,
            idToken,
          });
          if (resolved.outcome === "confirmation_required") {
            const pending: PendingAuthConfirmation = {
              flowId: resolved.flowId,
              nonce,
              intent,
              provider: flowProvider,
              expiresAt: resolved.expiresAt,
              action: resolved.nextAction,
              firebaseUID: result.user.uid,
              account: firebaseAccount(result.user),
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
            firebaseUID: result.user.uid,
            humanId: nextSession.user.id,
            outcome: resolved.outcome,
            intent,
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
        if (!isCurrentGeneration(generation) && !confirmationRequired) {
          // A logout that began while the provider popup was open owns the
          // terminal Firebase state as well as the server cookie.
          await signOut(auth).catch(() => undefined);
        }
      } catch (error) {
        if (!firebaseSignInCompleted && popup) {
          const popupResult = await popup.catch(() => null);
          firebaseSignInCompleted = popupResult !== null;
        }
        if (
          !firebaseSignInCompleted &&
          isSameEmailCredentialCollision(error) &&
          isCurrentGeneration(generation)
        ) {
          await beginSameEmailCredentialRecovery(error, flowProvider, intent);
          if (isCurrentGeneration(generation)) {
            setCredentialRecoveryEmailSent(true);
          }
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
          await signOut(auth).catch(() => undefined);
        }
        throw error;
      } finally {
        signInPending.current = false;
      }
    },
    [
      isCurrentGeneration,
      nextGeneration,
      publishOutcomeNotice,
      serializeSessionMutation,
    ],
  );

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

  const updateDisplayName = useCallback(
    async (displayName: string) => {
      const generation = nextGeneration();
      await serializeSessionMutation(async () => {
        if (!isCurrentGeneration(generation)) return;
        const current = serverSession.current;
        if (!current.authenticated) {
          throw new AuthAPIError("Authentication is unavailable.", 401);
        }
        const requestedDisplayName = canonicalizeSumiDisplayName(displayName);
        let updatedUser: { id: string; displayName: string };
        try {
          updatedUser = await updateSumiProfile(requestedDisplayName);
        } catch (error) {
          if (!isCurrentGeneration(generation)) return;
          if (
            error instanceof AuthAPIError &&
            (error.status < 200 || error.status >= 300)
          ) {
            throw error;
          }

          let reconciled: SumiSessionStatus;
          try {
            reconciled = await getSumiSession();
          } catch (reconciliationError) {
            if (!isCurrentGeneration(generation)) return;
            throw new SumiProfileUpdateIndeterminateError(
              new AggregateError(
                [error, reconciliationError],
                "Profile update and reconciliation both failed.",
              ),
            );
          }
          if (!isCurrentGeneration(generation)) return;
          if (
            !reconciled.authenticated ||
            reconciled.user.id !== current.user.id
          ) {
            const authorityCleared = clearDirectChatAuthority();
            serverSession.current = { authenticated: false };
            setSession({ authenticated: false });
            setSessionState(
              !reconciled.authenticated && authorityCleared
                ? "unauthenticated"
                : "unavailable",
            );
            throw new SumiProfileUpdateIndeterminateError(error);
          }
          if (reconciled.authorityBindingId !== current.authorityBindingId) {
            try {
              bindDirectChatAuthority(reconciled.authorityBindingId);
            } catch (bindingError) {
              clearDirectChatAuthority();
              serverSession.current = { authenticated: false };
              setSession({ authenticated: false });
              setSessionState("unavailable");
              throw new SumiProfileUpdateIndeterminateError(
                new AggregateError(
                  [error, bindingError],
                  "Profile reconciliation could not replace browser authority.",
                ),
              );
            }
          }
          serverSession.current = reconciled;
          setSession(reconciled);
          setSessionState("authenticated");
          if (
            reconciled.user.displayName === null ||
            canonicalizeSumiDisplayName(reconciled.user.displayName) !==
              requestedDisplayName
          ) {
            if (reconciled.user.displayName === current.user.displayName) {
              throw error;
            }
            throw new SumiProfileUpdateIndeterminateError(error);
          }
          return;
        }
        if (!isCurrentGeneration(generation)) return;
        if (updatedUser.id !== current.user.id) {
          throw new AuthAPIError("Profile identity changed.", 409);
        }
        const nextSession: SumiSessionStatus = {
          ...current,
          user: updatedUser,
        };
        serverSession.current = nextSession;
        setSession(nextSession);
      });
    },
    [isCurrentGeneration, nextGeneration, serializeSessionMutation],
  );

  const logout = useCallback(async () => {
    // AuthGate unmounts ChatScreen as soon as this enters checking, closing
    // the already-upgraded socket before the cookie is cleared server-side.
    const generation = nextGeneration();
    clearAuthOutcomeNotice();
    setOutcomeNotice(null);
    // Commit AuthGate's unmount before the first await below. ChatScreen's
    // cleanup then closes its upgraded socket before this request clears the
    // cookie that authorized it.
    flushSync(() => setSessionState("checking"));
    try {
      await serializeSessionMutation(async () => {
        await logoutSumiSession();
      });
    } catch (error) {
      if (!isCurrentGeneration(generation)) return;
      const retainedSession = serverSession.current;
      setSession(retainedSession);
      setSessionState(
        retainedSession.authenticated ? "authenticated" : "unauthenticated",
      );
      throw error;
    }
    let authorityCleared = true;
    if (isCurrentGeneration(generation)) {
      // Server logout is the authority transition. Commit it before touching
      // optional Firebase/emulator display-state cleanup, which may throw
      // synchronously during setup.
      flushSync(() => {
        authorityCleared = clearDirectChatAuthority();
        serverSession.current = { authenticated: false };
        setSession({ authenticated: false });
        setSessionState(authorityCleared ? "unauthenticated" : "unavailable");
      });
    }
    await signOutFirebaseBestEffort();
    if (!authorityCleared) {
      throw new Error("Direct-chat private state could not be cleared");
    }
  }, [isCurrentGeneration, nextGeneration, serializeSessionMutation]);

  const messagingSelf = useMessaging((state) => state.self);
  const messagingSelfProfile = useMessaging((state) =>
    state.self ? state.membersByKey[state.selfKey] : undefined,
  );

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
    // Messaging has the authoritative, revisioned presentation profile once
    // its self participant is known. The session copy remains only as the
    // bootstrap fallback before that projection exists.
    const messagingDisplayName =
      messagingSelf?.kind === "human" &&
      messagingSelf.humanId === session.user.id
        ? messagingSelfProfile?.displayName
        : undefined;
    return {
      id: session.user.id,
      displayName: messagingDisplayName ?? session.user.displayName,
      email: null,
      photoURL: null,
    };
  }, [messagingSelf, messagingSelfProfile, session, sessionState]);

  const authorityBindingId =
    sessionState === "authenticated" && session.authenticated
      ? session.authorityBindingId
      : null;

  const value = useMemo<AuthContextValue>(
    () => ({
      configured: isFirebaseConfigured,
      loading: sessionState === "checking",
      sessionState,
      authenticated: sessionState === "authenticated" && session.authenticated,
      canUseDirectChat:
        sessionState === "authenticated" || sessionState === "preissued",
      authorityBindingId,
      user,
      confirmation,
      outcomeNotice,
      emailLinkCallbackPending,
      credentialRecoveryEmailSent,
      signIn,
      sendEmailLink,
      completeEmailLink,
      rejectEmailLink,
      confirmIntentTransition,
      cancelIntentTransition,
      dismissOutcomeNotice,
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
      rejectEmailLink,
      refreshSession,
      session.authenticated,
      sessionState,
      sendEmailLink,
      signIn,
      outcomeNotice,
      user,
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

function createProvider(providerName: SignInProvider): FirebaseAuthProvider {
  if (providerName === "github") {
    return new GithubAuthProvider();
  }
  const provider = new GoogleAuthProvider();
  provider.setCustomParameters({ prompt: "select_account" });
  return provider;
}

function authFlowProvider(
  providerName: SignInProvider,
): "google.com" | "github.com" {
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
