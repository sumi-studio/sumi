import { FirebaseError } from "firebase/app";
import {
  GithubAuthProvider,
  GoogleAuthProvider,
  getIdToken,
  linkWithCredential,
  OAuthCredential,
  type User,
} from "firebase/auth";
import { type AuthIntent, createAuthFlowNonce } from "./auth-flow-client";
import type {
  PendingCredentialRecovery,
  RecoverableProvider,
  SerializedOAuthCredential,
} from "./auth-flow-state";
import { beginEmailLinkAuth } from "./email-link-auth";
import { getFirebaseAuth } from "./firebase";
import {
  completeProviderOperation,
  failProviderOperation,
  startProviderOperation,
  statusProviderOperation,
} from "./provider-operation-client";
import { AuthAPIError } from "./session-client";

const MAX_CREDENTIAL_BYTES = 24 * 1024;

export async function beginSameEmailCredentialRecovery(
  error: unknown,
  expectedProvider: RecoverableProvider,
  requestedIntent: AuthIntent,
): Promise<void> {
  if (
    !(error instanceof FirebaseError) ||
    error.code !== "auth/account-exists-with-different-credential"
  ) {
    throw error;
  }
  const email = collisionEmail(error);
  const credential = extractCredential(error, expectedProvider);
  await beginEmailLinkAuth(email, "sign_in", {
    provider: expectedProvider,
    requestedIntent,
    credential: serializeCredential(credential, expectedProvider),
  });
}

export function isSameEmailCredentialCollision(error: unknown): boolean {
  return (
    error instanceof FirebaseError &&
    error.code === "auth/account-exists-with-different-credential"
  );
}

export async function completeSameEmailCredentialRecovery({
  recovery,
  user,
}: {
  recovery: PendingCredentialRecovery;
  user: User;
}): Promise<"provider_linked" | "provider_already_linked"> {
  if (Date.parse(recovery.expiresAt) <= Date.now()) {
    throw new AuthAPIError("Provider credential recovery expired.", 0);
  }
  const credential = deserializeCredential(recovery);
  const capturedFirebaseUID = user.uid;
  const nonce = createAuthFlowNonce();
  if (!isCurrentFirebaseUser(capturedFirebaseUID)) {
    throw new AuthAPIError(
      "Firebase account changed before provider recovery.",
      0,
    );
  }
  const emailProofToken = await getIdToken(user, true);
  if (!isCurrentFirebaseUser(capturedFirebaseUID)) {
    throw new AuthAPIError(
      "Firebase account changed before provider recovery.",
      0,
    );
  }
  const started = await startProviderOperation({
    provider: recovery.provider,
    operation: "link",
    decisionPath: "same_email_recovery",
    nonce,
    idToken: emailProofToken,
  });
  if (
    started.outcome !== "client_operation_required" ||
    started.clientOperation !== "firebase_link_with_credential" ||
    !started.completionTokenNotBefore ||
    !started.expiresAt ||
    Date.parse(started.expiresAt) <= Date.now()
  ) {
    throw new AuthAPIError("Provider recovery could not be started.", 0);
  }

  if (!isCurrentFirebaseUser(capturedFirebaseUID)) {
    await failChangedUserRecovery(started.operationId, nonce, "before");
  }

  let linkedUser: User;
  try {
    const linked = await linkWithCredential(user, credential);
    linkedUser = linked.user;
  } catch (error) {
    await failStartedRecovery(started.operationId, nonce, error);
    throw error;
  }
  if (
    linkedUser.uid !== capturedFirebaseUID ||
    !isCurrentFirebaseUser(capturedFirebaseUID)
  ) {
    await failChangedUserRecovery(started.operationId, nonce, "during");
  }

  await waitUntil(started.completionTokenNotBefore);
  const completionToken = await getIdToken(linkedUser, true);
  if (!isCurrentFirebaseUser(capturedFirebaseUID)) {
    await failChangedUserRecovery(started.operationId, nonce, "during");
  }
  let completed: Awaited<ReturnType<typeof completeProviderOperation>>;
  try {
    completed = await completeProviderOperation({
      operationId: started.operationId,
      nonce,
      idToken: completionToken,
    });
  } catch (error) {
    const recovered = await statusProviderOperation({
      operationId: started.operationId,
      nonce,
    }).catch(() => null);
    if (
      recovered?.outcome === "provider_linked" ||
      recovered?.outcome === "provider_already_linked"
    ) {
      assertTerminalFirebaseUser(capturedFirebaseUID);
      return recovered.outcome;
    }
    throw error;
  }
  if (
    completed.outcome === "provider_linked" ||
    completed.outcome === "provider_already_linked"
  ) {
    assertTerminalFirebaseUser(capturedFirebaseUID);
    return completed.outcome;
  }
  throw new AuthAPIError("Provider recovery did not complete.", 0);
}

function collisionEmail(error: FirebaseError): string {
  const email = error.customData?.email;
  if (typeof email !== "string" || email.length === 0 || email.length > 320) {
    throw new AuthAPIError("Provider collision email is unavailable.", 0);
  }
  return email;
}

function extractCredential(
  error: FirebaseError,
  provider: RecoverableProvider,
): OAuthCredential {
  const credential =
    provider === "google.com"
      ? GoogleAuthProvider.credentialFromError(error)
      : GithubAuthProvider.credentialFromError(error);
  if (
    !credential ||
    credential.providerId !== provider ||
    credential.signInMethod !== provider
  ) {
    throw new AuthAPIError("Provider collision credential is unavailable.", 0);
  }
  return credential;
}

function serializeCredential(
  credential: OAuthCredential,
  provider: RecoverableProvider,
): SerializedOAuthCredential {
  const raw = credential.toJSON() as Record<string, unknown>;
  const serialized: SerializedOAuthCredential = {
    providerId: provider,
    signInMethod: provider,
  };
  for (const key of [
    "idToken",
    "accessToken",
    "secret",
    "nonce",
    "pendingToken",
  ] as const) {
    const value = raw[key];
    if (value !== undefined && value !== null) {
      if (typeof value !== "string" || value.length > 12_288) {
        throw new AuthAPIError("Provider collision credential is invalid.", 0);
      }
      if (value.length > 0) serialized[key] = value;
    }
  }
  if (
    !serialized.idToken &&
    !serialized.accessToken &&
    !serialized.pendingToken
  ) {
    throw new AuthAPIError("Provider collision credential is invalid.", 0);
  }
  if (JSON.stringify(serialized).length > MAX_CREDENTIAL_BYTES) {
    throw new AuthAPIError("Provider collision credential is too large.", 0);
  }
  return serialized;
}

function deserializeCredential(
  recovery: PendingCredentialRecovery,
): OAuthCredential {
  if (JSON.stringify(recovery.credential).length > MAX_CREDENTIAL_BYTES) {
    throw new AuthAPIError("Provider recovery credential is too large.", 0);
  }
  const credential = OAuthCredential.fromJSON(recovery.credential);
  if (
    !credential ||
    credential.providerId !== recovery.provider ||
    credential.signInMethod !== recovery.provider
  ) {
    throw new AuthAPIError("Provider recovery credential is invalid.", 0);
  }
  return credential;
}

async function failStartedRecovery(
  operationId: string,
  nonce: string,
  error: unknown,
): Promise<void> {
  const outcome =
    error instanceof FirebaseError &&
    error.code === "auth/credential-already-in-use"
      ? "credential_in_use"
      : "firebase_operation_failed";
  await failProviderOperation({ operationId, nonce, outcome }).catch(
    () => undefined,
  );
}

function isCurrentFirebaseUser(uid: string): boolean {
  try {
    return getFirebaseAuth().currentUser?.uid === uid;
  } catch {
    return false;
  }
}

function assertTerminalFirebaseUser(uid: string): void {
  if (!isCurrentFirebaseUser(uid)) {
    throw new AuthAPIError(
      "Firebase account changed after provider recovery completed.",
      0,
    );
  }
}

async function failChangedUserRecovery(
  operationId: string,
  nonce: string,
  timing: "before" | "during",
): Promise<never> {
  await failProviderOperation({
    operationId,
    nonce,
    outcome: "firebase_operation_failed",
  }).catch(() => undefined);
  throw new AuthAPIError(
    `Firebase account changed ${timing} provider recovery.`,
    0,
  );
}

async function waitUntil(timestamp: string): Promise<void> {
  const delay = Date.parse(timestamp) - Date.now() + 50;
  if (!Number.isFinite(delay) || delay > 10_000) {
    throw new AuthAPIError("Provider recovery timing is invalid.", 0);
  }
  if (delay > 0) {
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
}
