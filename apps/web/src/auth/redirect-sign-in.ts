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
  hasPendingRedirectFlowRecord,
  type PendingRedirectAuthFlow,
  type RecoverableProvider,
  savePendingRedirectFlow,
  takePendingRedirectFlow,
} from "./auth-flow-state";
import { getFirebaseAuth } from "./firebase";
import { AuthAPIError } from "./session-client";

/**
 * The attempt ended without a provider result: the person cancelled, used the
 * back button, came back before the tab could leave, or the browser discarded
 * Firebase's pending redirect.
 */
export class RedirectSignInAbandonedError extends Error {
  constructor() {
    super("The provider redirect returned without a Firebase credential.");
    this.name = "RedirectSignInAbandonedError";
  }
}

/**
 * The return outlived the Sumi flow's server-side expiry and no provider
 * result arrived to exchange. Reported distinctly from an abandonment so the
 * person learns the attempt timed out rather than appearing cancelled.
 */
export class RedirectSignInExpiredError extends Error {
  constructor() {
    super("The provider redirect returned after the sign-in flow expired.");
    this.name = "RedirectSignInExpiredError";
  }
}

/**
 * Whether startup must attempt a redirect completion. A raw record — even a
 * malformed or expired one — still drives the completion path so the person
 * sees a concrete outcome instead of a silent login screen.
 */
export function hasPendingRedirectSignIn(): boolean {
  return hasPendingRedirectFlowRecord();
}

/**
 * Starts the Sumi flow, persists its receipt, then leaves this tab for the
 * provider. Normal sign-in never opens a popup: popup blocking breaks it and
 * an installed PWA has no usable popup surface.
 */
export async function beginRedirectSignIn({
  provider,
  intent,
  isAborted,
}: {
  provider: RecoverableProvider;
  intent: AuthIntent;
  /**
   * Checked once the server-side flow registration resolves. A page restored
   * from the back/forward cache — or an attempt superseded by a newer one —
   * means this attempt must not navigate the tab away again.
   */
  isAborted?: () => boolean;
}): Promise<void> {
  clearPendingRedirectFlow();
  const auth = getFirebaseAuth();
  const nonce = createAuthFlowNonce();
  const started = await startAuthFlow({
    intent,
    provider,
    continuation: "/",
    nonce,
  });
  if (isAborted?.()) {
    // The person already returned to this page while the flow was being
    // registered. No receipt was written yet, so nothing needs cleanup:
    // report the attempt as abandoned and stay on the login screen.
    throw new RedirectSignInAbandonedError();
  }
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
    await signInWithRedirect(auth, createRedirectProvider(provider));
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
