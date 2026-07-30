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
import { getFirebaseAuth } from "./firebase";
import { isFirebaseConfigured } from "./firebase-config";
import {
  AuthAPIError,
  exchangeFirebaseIDToken,
  getSumiSession,
  logoutSumiSession,
  type SumiSessionStatus,
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
  signIn: (provider: SignInProvider) => Promise<void>;
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
  // Every state-changing auth operation claims a generation. Late session
  // reads must never re-authorize the chat after logout has started.
  const authGeneration = useRef(0);
  const sessionMutation = useRef<Promise<void>>(Promise.resolve());
  const signInPending = useRef(false);

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
      setSession(nextSession);
      const nextState = nextSession.authenticated
        ? "authenticated"
        : "unauthenticated";
      setSessionState(nextState);
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
    async (providerName: SignInProvider) => {
      if (preissuedSessionMode || !authOriginAllowed) {
        throw new AuthAPIError("Authentication is unavailable.", 0);
      }
      const generation = nextGeneration();
      const auth = getFirebaseAuth();
      const provider = createProvider(providerName);
      let firebaseSignInCompleted = false;
      signInPending.current = true;
      try {
        const result = await signInWithPopup(auth, provider);
        firebaseSignInCompleted = true;
        await serializeSessionMutation(async () => {
          if (!isCurrentGeneration(generation)) return;
          const idToken = await getIdToken(result.user, true);
          if (!isCurrentGeneration(generation)) return;
          await exchangeFirebaseIDToken(idToken);
          const nextSession = await getSumiSession();
          if (!nextSession.authenticated) {
            throw new Error("Sumi session was not established.");
          }
          if (!isCurrentGeneration(generation)) return;
          setSession(nextSession);
          setSessionState("authenticated");
        });
        if (!isCurrentGeneration(generation)) {
          // A logout that began while the provider popup was open owns the
          // terminal Firebase state as well as the server cookie.
          await signOut(auth).catch(() => undefined);
        }
      } catch (error) {
        // A Firebase account is display state, not Sumi authorization. Do not
        // retain it when the server-owned identity binding/exchange failed.
        if (firebaseSignInCompleted) {
          await signOut(auth).catch(() => undefined);
        }
        throw error;
      } finally {
        signInPending.current = false;
      }
    },
    [isCurrentGeneration, nextGeneration, serializeSessionMutation],
  );

  const logout = useCallback(async () => {
    // AuthGate unmounts ChatScreen as soon as this enters checking, closing
    // the already-upgraded socket before the cookie is cleared server-side.
    const previousSession = session;
    const generation = nextGeneration();
    // Commit AuthGate's unmount before the first await below. ChatScreen's
    // cleanup then closes its upgraded socket before this request clears the
    // cookie that authorized it.
    flushSync(() => setSessionState("checking"));
    try {
      await serializeSessionMutation(async () => {
        await logoutSumiSession();
      });
      if (!isCurrentGeneration(generation)) return;
      setSession({ authenticated: false });
      setSessionState("unauthenticated");
      await signOut(getFirebaseAuth()).catch(() => undefined);
    } catch (error) {
      if (!isCurrentGeneration(generation)) return;
      setSession(previousSession);
      setSessionState(
        previousSession.authenticated ? "authenticated" : "unauthenticated",
      );
      throw error;
    }
  }, [isCurrentGeneration, nextGeneration, serializeSessionMutation, session]);

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
      signIn,
      logout,
      refreshSession,
    }),
    [logout, refreshSession, session.authenticated, sessionState, signIn, user],
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
