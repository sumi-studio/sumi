import {
  type AuthProvider as FirebaseAuthProvider,
  GithubAuthProvider,
  GoogleAuthProvider,
  getIdToken,
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
import {
  clearPendingConfirmation,
  loadPendingConfirmation,
  type PendingAuthConfirmation,
  savePendingConfirmation,
} from "./auth-confirmation-state";
import {
  type AuthFlowProvider,
  type AuthIntent,
  confirmAuthFlow,
  createAuthFlowNonce,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";
import { beginEmailLinkAuth, completeEmailLinkAuth } from "./email-link-auth";
import { getFirebaseAuth } from "./firebase";
import { isFirebaseConfigured } from "./firebase-config";
import {
  AuthAPIError,
  getSumiSession,
  logoutSumiSession,
  SumiSessionCompensatedError,
  SumiSessionCompensationFailedError,
  type SumiSessionStatus,
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

interface AuthContextValue {
  configured: boolean;
  loading: boolean;
  sessionState: AuthSessionState;
  authenticated: boolean;
  canUseDirectChat: boolean;
  user: AuthUser | null;
  confirmation: PendingAuthConfirmation | null;
  signIn: (provider: SignInProvider, intent: AuthIntent) => Promise<void>;
  sendEmailLink: (email: string, intent: AuthIntent) => Promise<void>;
  completeEmailLink: () => Promise<void>;
  confirmIntentTransition: () => Promise<void>;
  cancelIntentTransition: () => Promise<void>;
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
  // Every state-changing auth operation claims a generation. Late session
  // reads must never re-authorize the chat after logout has started.
  const authGeneration = useRef(0);
  const sessionMutation = useRef<Promise<void>>(Promise.resolve());
  const signInPending = useRef(false);
  const serverSession = useRef<SumiSessionStatus>(session);

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
      return nextState;
    } catch (error) {
      if (!isCurrentGeneration(generation)) return "checking";
      setSession({ authenticated: false });
      const nextState = classifySessionFailure(error);
      setSessionState(nextState);
      return nextState;
    }
  }, [isCurrentGeneration, nextGeneration]);

  useEffect(() => {
    void refreshSession();
  }, [refreshSession]);

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
      try {
        const started = await startAuthFlow({
          intent,
          provider: flowProvider,
          continuation: "/",
          nonce,
        });
        if (!isCurrentGeneration(generation)) return;
        const result = await signInWithPopup(auth, provider);
        firebaseSignInCompleted = true;
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
            };
            savePendingConfirmation(pending);
            confirmationRequired = true;
            if (isCurrentGeneration(generation)) setConfirmation(pending);
            return;
          }
          const nextSession = await verifyCommittedSumiSession();
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
    [isCurrentGeneration, nextGeneration, serializeSessionMutation],
  );

  const sendEmailLink = useCallback(
    async (email: string, intent: AuthIntent) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      nextGeneration();
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
        if (completed.result.outcome === "confirmation_required") {
          const pending: PendingAuthConfirmation = {
            flowId: completed.result.flowId,
            nonce: completed.flow.nonce,
            intent: completed.flow.intent,
            provider: "email_link",
            expiresAt: completed.result.expiresAt,
            action: completed.result.nextAction,
          };
          savePendingConfirmation(pending);
          setConfirmation(pending);
          return;
        }
        const nextSession = await verifyCommittedSumiSession();
        flushSync(() => {
          bindDirectChatAuthority(nextSession.authorityBindingId);
          serverSession.current = nextSession;
          if (!isCurrentGeneration(generation)) return;
          setSession(nextSession);
          setSessionState("authenticated");
        });
      });
    } catch (error) {
      await signOutFirebaseBestEffort();
      throw error;
    } finally {
      signInPending.current = false;
    }
  }, [isCurrentGeneration, nextGeneration, serializeSessionMutation]);

  const confirmIntentTransition = useCallback(async () => {
    const pending = confirmation;
    if (!pending || preissuedSessionMode || !authOriginAllowed) {
      throw new AuthAPIError("Authentication confirmation is unavailable.", 0);
    }
    const generation = nextGeneration();
    await serializeSessionMutation(async () => {
      await confirmAuthFlow({
        flowId: pending.flowId,
        nonce: pending.nonce,
        action: pending.action,
      });
      const nextSession = await verifyCommittedSumiSession();
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
    serializeSessionMutation,
  ]);

  const cancelIntentTransition = useCallback(async () => {
    nextGeneration();
    clearPendingConfirmation();
    setConfirmation(null);
    await signOutFirebaseBestEffort();
  }, [nextGeneration]);

  const logout = useCallback(async () => {
    // AuthGate unmounts ChatScreen as soon as this enters checking, closing
    // the already-upgraded socket before the cookie is cleared server-side.
    const generation = nextGeneration();
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

  const user = useMemo<AuthUser | null>(() => {
    if (!session.authenticated) {
      return null;
    }
    return {
      id: session.user.id,
      displayName: null,
      email: null,
      photoURL: null,
    };
  }, [session]);

  const value = useMemo<AuthContextValue>(
    () => ({
      configured: isFirebaseConfigured,
      loading: sessionState === "checking",
      sessionState,
      authenticated: sessionState === "authenticated" && session.authenticated,
      canUseDirectChat:
        sessionState === "authenticated" || sessionState === "preissued",
      user,
      confirmation,
      signIn,
      sendEmailLink,
      completeEmailLink,
      confirmIntentTransition,
      cancelIntentTransition,
      logout,
      refreshSession,
    }),
    [
      cancelIntentTransition,
      confirmation,
      completeEmailLink,
      confirmIntentTransition,
      logout,
      refreshSession,
      session.authenticated,
      sessionState,
      sendEmailLink,
      signIn,
      user,
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

function authFlowProvider(providerName: SignInProvider): AuthFlowProvider {
  return providerName === "github" ? "github.com" : "google.com";
}

async function signOutFirebaseBestEffort(): Promise<void> {
  try {
    const auth = getFirebaseAuth();
    await signOut(auth).catch(() => undefined);
  } catch {
    // Firebase is cleanup-only after Sumi authority has ended.
  }
}
