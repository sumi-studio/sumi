import {
  type AuthProvider as FirebaseAuthProvider,
  type User as FirebaseUser,
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
  useState,
} from "react";
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
  const [firebaseUser, setFirebaseUser] = useState<FirebaseUser | null>(null);
  const [sessionState, setSessionState] =
    useState<AuthSessionState>("checking");
  const [firebaseLoading, setFirebaseLoading] = useState(isFirebaseConfigured);

  const refreshSession = useCallback(async (): Promise<AuthSessionState> => {
    setSessionState("checking");
    try {
      const nextSession = await getSumiSession();
      setSession(nextSession);
      const nextState = nextSession.authenticated
        ? "authenticated"
        : "unauthenticated";
      setSessionState(nextState);
      return nextState;
    } catch (error) {
      setSession({ authenticated: false });
      const nextState = classifySessionFailure(error);
      setSessionState(nextState);
      return nextState;
    }
  }, []);

  useEffect(() => {
    void refreshSession();
  }, [refreshSession]);

  useEffect(() => {
    if (!isFirebaseConfigured) {
      setFirebaseLoading(false);
      return;
    }
    return onAuthStateChanged(
      getFirebaseAuth(),
      (nextUser) => {
        setFirebaseUser(nextUser);
        setFirebaseLoading(false);
      },
      () => {
        setFirebaseUser(null);
        setFirebaseLoading(false);
      },
    );
  }, []);

  const signIn = useCallback(async (providerName: SignInProvider) => {
    const auth = getFirebaseAuth();
    const provider = createProvider(providerName);
    const result = await signInWithPopup(auth, provider);
    const idToken = await getIdToken(result.user, true);
    await exchangeFirebaseIDToken(idToken);
    const nextSession = await getSumiSession();
    if (!nextSession.authenticated) {
      throw new Error("Sumi session was not established.");
    }
    setFirebaseUser(result.user);
    setSession(nextSession);
    setSessionState("authenticated");
  }, []);

  const logout = useCallback(async () => {
    // The Sumi authorization cookie is cleared before Firebase display state.
    await logoutSumiSession();
    setSession({ authenticated: false });
    setSessionState("unauthenticated");
    setFirebaseUser(null);
    await signOut(getFirebaseAuth());
  }, []);

  const user = useMemo<AuthUser | null>(() => {
    if (!session.authenticated) {
      return null;
    }
    return {
      id: session.user.id,
      displayName: firebaseUser?.displayName ?? null,
      email: firebaseUser?.email ?? null,
      photoURL: firebaseUser?.photoURL ?? null,
    };
  }, [firebaseUser, session]);

  const value = useMemo<AuthContextValue>(
    () => ({
      configured: isFirebaseConfigured,
      loading: sessionState === "checking" || firebaseLoading,
      sessionState,
      authenticated: sessionState === "authenticated" && session.authenticated,
      canUseDirectChat:
        sessionState === "authenticated" || sessionState === "preissued",
      user,
      signIn,
      logout,
      refreshSession,
    }),
    [
      firebaseLoading,
      logout,
      refreshSession,
      session.authenticated,
      sessionState,
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
    // The current direct-chat fixture can pre-issue a valid HttpOnly cookie
    // without exposing the auth control-plane routes.
    if (error.status === 404) {
      return "preissued";
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
