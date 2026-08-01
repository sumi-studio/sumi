import {
  getIdToken,
  isSignInWithEmailLink,
  sendSignInLinkToEmail,
  signInWithEmailLink,
} from "firebase/auth";
import {
  type AuthFlowResult,
  type AuthIntent,
  createAuthFlowNonce,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";
import {
  clearEmailFlowLocation,
  clearPendingEmailFlow,
  createEmailFlowState,
  emailFlowContinuation,
  emailFlowStateFromLocation,
  loadPendingEmailFlow,
  type PendingEmailAuthFlow,
  savePendingEmailFlow,
} from "./auth-flow-state";
import { getFirebaseAuth } from "./firebase";
import { AuthAPIError } from "./session-client";

export interface EmailLinkFlowCompletion {
  flow: PendingEmailAuthFlow;
  result: Exclude<AuthFlowResult, { outcome: "proof_required" }>;
}

export function hasEmailLinkCallback(): boolean {
  return emailFlowStateFromLocation() !== null;
}

export async function beginEmailLinkAuth(
  rawEmail: string,
  intent: AuthIntent,
): Promise<void> {
  const email = rawEmail.trim();
  if (!email || email.length > 320) {
    throw new AuthAPIError("Invalid email address.", 0);
  }
  const state = createEmailFlowState();
  const nonce = createAuthFlowNonce();
  const continuation = emailFlowContinuation(state);
  const started = await startAuthFlow({
    intent,
    provider: "email_link",
    email,
    continuation,
    nonce,
  });
  const pending: PendingEmailAuthFlow = {
    flowId: started.flowId,
    nonce,
    intent,
    provider: "email_link",
    email,
    expiresAt: started.expiresAt,
    stage: "link_sent",
  };
  savePendingEmailFlow(state, pending);
  try {
    await sendSignInLinkToEmail(getFirebaseAuth(), email, {
      url: new URL(continuation, globalThis.location.origin).href,
      handleCodeInApp: true,
    });
  } catch (error) {
    clearPendingEmailFlow(state);
    throw error;
  }
}

export async function completeEmailLinkAuth(): Promise<EmailLinkFlowCompletion> {
  const state = emailFlowStateFromLocation();
  if (!state) {
    throw new AuthAPIError("Email link state is missing.", 0);
  }
  const pending = loadPendingEmailFlow(state);
  if (!pending) {
    throw new AuthAPIError(
      "This email link must be opened in the browser that requested it.",
      0,
    );
  }
  const auth = getFirebaseAuth();
  let user = auth.currentUser;
  if (pending.stage === "link_sent") {
    if (!isSignInWithEmailLink(auth, globalThis.location.href)) {
      throw new AuthAPIError("Invalid Firebase email link.", 0);
    }
    const credential = await signInWithEmailLink(
      auth,
      pending.email,
      globalThis.location.href,
    );
    user = credential.user;
    pending.stage = "firebase_complete";
    savePendingEmailFlow(state, pending);
  }
  if (!user) {
    throw new AuthAPIError("Firebase email verification is unavailable.", 0);
  }
  const idToken = await getIdToken(user, true);
  const result = await resolveAuthFlow({
    flowId: pending.flowId,
    nonce: pending.nonce,
    idToken,
  });
  clearPendingEmailFlow(state);
  clearEmailFlowLocation();
  return { flow: pending, result };
}
