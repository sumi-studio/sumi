import {
  getIdToken,
  isSignInWithEmailLink,
  sendSignInLinkToEmail,
  signInWithEmailLink,
  type User,
} from "firebase/auth";
import {
  type AuthFlowResult,
  type AuthIntent,
  createAuthFlowNonce,
  resolveAuthFlow,
  startAuthFlow,
} from "./auth-flow-client";
import {
  cleanupPendingEmailFlowStorage,
  clearEmailFlowLocation,
  clearPendingEmailFlow,
  consumePendingCredentialRecovery,
  createEmailFlowState,
  emailFlowContinuation,
  emailFlowStateFromLocation,
  loadPendingEmailFlow,
  type PendingCredentialRecovery,
  type PendingEmailAuthFlow,
  type SerializedOAuthCredential,
  savePendingEmailFlow,
} from "./auth-flow-state";
import { getFirebaseAuth } from "./firebase";
import { AuthAPIError } from "./session-client";

export interface EmailLinkFlowCompletion {
  flow: PendingEmailAuthFlow;
  result: Exclude<AuthFlowResult, { outcome: "proof_required" }>;
  firebaseUser: User;
}

export function hasEmailLinkCallback(): boolean {
  return emailFlowStateFromLocation() !== null;
}

export function rejectEmailLinkAuth(): void {
  const state = emailFlowStateFromLocation();
  if (state) clearPendingEmailFlow(state);
  clearEmailFlowLocation();
}

export async function beginEmailLinkAuth(
  rawEmail: string,
  intent: AuthIntent,
  recovery?: {
    provider: "google.com" | "github.com";
    requestedIntent: AuthIntent;
    credential: SerializedOAuthCredential;
  },
): Promise<void> {
  cleanupPendingEmailFlowStorage();
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
    ...(recovery
      ? {
          credentialRecovery: boundedRecovery(recovery, started.expiresAt),
        }
      : {}),
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

function boundedRecovery(
  recovery: Omit<PendingCredentialRecovery, "version" | "expiresAt">,
  flowExpiresAt: string,
): PendingCredentialRecovery {
  const flowExpiry = Date.parse(flowExpiresAt);
  if (!Number.isFinite(flowExpiry)) {
    throw new AuthAPIError("Invalid authentication flow expiry.", 0);
  }
  return {
    version: 1,
    ...recovery,
    expiresAt: new Date(
      Math.min(flowExpiry, Date.now() + 10 * 60_000),
    ).toISOString(),
  };
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
  const consumesCredential = pending.credentialRecovery !== undefined;
  if (consumesCredential) {
    consumePendingCredentialRecovery(state, pending);
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
    if (!consumesCredential) savePendingEmailFlow(state, pending);
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
  if (!consumesCredential) clearPendingEmailFlow(state);
  clearEmailFlowLocation();
  return {
    flow: pending,
    result,
    firebaseUser: user,
  };
}
