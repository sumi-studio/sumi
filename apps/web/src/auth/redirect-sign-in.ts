import {
  type AuthProvider as FirebaseAuthProvider,
  GithubAuthProvider,
  GoogleAuthProvider,
  getRedirectResult,
  signInWithRedirect,
  type User,
} from "firebase/auth";
import {
  type AuthIntent,
  createAuthFlowNonce,
  startAuthFlow,
} from "./auth-flow-client";
import {
  clearPendingRedirectFlow,
  loadPendingRedirectFlow,
  type PendingRedirectAuthFlow,
  type RecoverableProvider,
  savePendingRedirectFlow,
  takePendingRedirectFlow,
} from "./auth-flow-state";
import { getFirebaseAuth } from "./firebase";
import { AuthAPIError } from "./session-client";

/**
 * The browser came back without a provider result: the person cancelled, used
 * the back button, or the browser discarded Firebase's pending redirect.
 */
export class RedirectSignInAbandonedError extends Error {
  constructor() {
    super("The provider redirect returned without a Firebase credential.");
    this.name = "RedirectSignInAbandonedError";
  }
}

export function hasPendingRedirectSignIn(): boolean {
  return loadPendingRedirectFlow() !== null;
}

/**
 * Starts the Sumi flow, persists its receipt, then leaves this tab for the
 * provider. Normal sign-in never opens a popup: popup blocking breaks it and
 * an installed PWA has no usable popup surface.
 */
export async function beginRedirectSignIn({
  provider,
  intent,
}: {
  provider: RecoverableProvider;
  intent: AuthIntent;
}): Promise<never> {
  clearPendingRedirectFlow();
  const auth = getFirebaseAuth();
  const nonce = createAuthFlowNonce();
  const started = await startAuthFlow({
    intent,
    provider,
    continuation: "/",
    nonce,
  });
  const flow: PendingRedirectAuthFlow = {
    flowId: started.flowId,
    nonce,
    intent,
    provider,
    expiresAt: started.expiresAt,
    stage: "redirect_sent",
  };
  if (!savePendingRedirectFlow(flow)) {
    throw new AuthAPIError(
      "Redirect sign-in state could not be stored in this browser.",
      0,
    );
  }
  try {
    return await signInWithRedirect(auth, createRedirectProvider(provider));
  } catch (error) {
    clearPendingRedirectFlow();
    throw error;
  }
}

/**
 * Claims the receipt for this return. It is removed before the Firebase result
 * is read so a reload, a retry, or React's double-invoked effects can never
 * exchange the same flow twice.
 */
export function takePendingRedirectSignIn(): PendingRedirectAuthFlow | null {
  return takePendingRedirectFlow();
}

export async function resolveRedirectSignInUser(): Promise<User> {
  const credential = await getRedirectResult(getFirebaseAuth());
  if (!credential) throw new RedirectSignInAbandonedError();
  return credential.user;
}

function createRedirectProvider(
  provider: RecoverableProvider,
): FirebaseAuthProvider {
  if (provider === "github.com") {
    return new GithubAuthProvider();
  }
  const google = new GoogleAuthProvider();
  google.setCustomParameters({ prompt: "select_account" });
  return google;
}
