import {
  clearEnrollmentInvitation,
  readEnrollmentInvitation,
} from "./enrollment-invitation-state";
import {
  AuthAPIError,
  postAuthJSON,
  postAuthNoContent,
} from "./session-client";

export type AuthIntent = "sign_in" | "sign_up";
export type AuthFlowProvider = "email_code" | "google.com" | "github.com";
export type AuthConfirmationAction = "create_account" | "sign_in";

export type AuthFlowResult =
  | {
      flowId: string;
      outcome: "proof_required";
      expiresAt: string;
      emailChallenge?: EmailChallengeStatus;
    }
  | {
      flowId: string;
      outcome: "confirmation_required";
      nextAction: AuthConfirmationAction;
      continuation: string;
      expiresAt: string;
    }
  | {
      flowId: string;
      outcome: "signed_in" | "account_created";
      continuation: string;
      expiresAt: string;
      /** The Human the terminal outcome resolved to. */
      humanId: string;
    };

/** Sumi email proof state; never contains the code or link token. */
export interface EmailChallengeStatus {
  flowId: string;
  flowStatus: string;
  email: string;
  flowExpiresAt: string;
  challengeExpiresAt: string;
  attemptsRemaining: number;
  delivery: "" | "pending" | "sent" | "failed" | "cancelled";
  resendAvailableAt: string;
  /** Set once the flow resolved to a Human; absent while still pending. */
  humanId?: string;
}

export interface StartAuthFlowRequest {
  intent: AuthIntent;
  provider: AuthFlowProvider;
  email?: string;
  continuation: string;
  nonce: string;
}

export class AuthFlowRecoveryFailedError extends AggregateError {
  constructor(
    mutationError: unknown,
    recoveryError: unknown,
    logoutError: unknown,
  ) {
    super(
      [mutationError, recoveryError, logoutError],
      "Authentication flow response was ambiguous and recovery discard failed.",
    );
    this.name = "AuthFlowRecoveryFailedError";
  }
}

export function createAuthFlowNonce(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "");
}

export async function startAuthFlow(
  request: StartAuthFlowRequest,
): Promise<Extract<AuthFlowResult, { outcome: "proof_required" }>> {
  const body: Record<string, string> = {
    intent: request.intent,
    provider: request.provider,
    continuation: request.continuation,
    nonce: request.nonce,
  };
  if (request.provider === "email_code") {
    body.email = request.email ?? "";
  }
  const invitation = readEnrollmentInvitation();
  if (invitation) body.invite_token = invitation;
  const result = parseAuthFlowResult(await postAuthJSON("/auth/flows", body));
  if (result.outcome !== "proof_required") {
    throw new AuthAPIError("Invalid authentication flow response.", 0);
  }
  return result;
}

export async function resolveAuthFlow({
  flowId,
  nonce,
  idToken,
  switchFromUserId,
}: {
  flowId: string;
  nonce: string;
  idToken: string;
  /** The person's explicit choice to replace this jar's active Human. */
  switchFromUserId?: string;
}): Promise<Exclude<AuthFlowResult, { outcome: "proof_required" }>> {
  if (!idToken || idToken.length > 12 * 1024) {
    throw new AuthAPIError("Invalid Firebase ID token.", 0);
  }
  let result: AuthFlowResult;
  try {
    result = parseAuthFlowResult(
      await postAuthJSON("/auth/flows/resolve", {
        flow_id: flowId,
        nonce,
        id_token: idToken,
        ...(switchFromUserId ? { switch_from_user_id: switchFromUserId } : {}),
      }),
    );
  } catch (error) {
    if (!isAmbiguousMutationError(error)) throw error;
    result = await recoverAmbiguousFlowMutation({
      flowId,
      nonce,
      mutationError: error,
      accept: (recovered) => recovered.outcome !== "proof_required",
    });
  }
  if (result.outcome === "proof_required") {
    throw new AuthAPIError("Invalid authentication flow response.", 0);
  }
  if (result.outcome === "signed_in" || result.outcome === "account_created")
    clearEnrollmentInvitation();
  return result;
}

export async function confirmAuthFlow({
  flowId,
  nonce,
  action,
  switchFromUserId,
}: {
  flowId: string;
  nonce: string;
  action: AuthConfirmationAction;
  /** The person's explicit choice to replace this jar's active Human. */
  switchFromUserId?: string;
}): Promise<
  Extract<AuthFlowResult, { outcome: "signed_in" | "account_created" }>
> {
  let result: AuthFlowResult;
  try {
    result = parseAuthFlowResult(
      await postAuthJSON("/auth/flows/confirm", {
        flow_id: flowId,
        nonce,
        action,
        ...(switchFromUserId ? { switch_from_user_id: switchFromUserId } : {}),
      }),
    );
  } catch (error) {
    if (!isAmbiguousMutationError(error)) throw error;
    result = await recoverAmbiguousFlowMutation({
      flowId,
      nonce,
      mutationError: error,
      accept: (recovered) =>
        recovered.outcome === "signed_in" ||
        recovered.outcome === "account_created",
    });
  }
  if (result.outcome !== "signed_in" && result.outcome !== "account_created") {
    throw new AuthAPIError("Invalid authentication flow response.", 0);
  }
  clearEnrollmentInvitation();
  return result;
}

/**
 * Cancels one flow's server-side issuance authority and revokes the sessions
 * it already minted, by the nonce that owns it. This is the scoped
 * compensation for an ambiguous mutation: it never disturbs a session a
 * later, different sign-in choice established in this jar.
 */
export async function discardAuthFlow(
  flowId: string,
  nonce: string,
): Promise<void> {
  await postAuthNoContent("/auth/flows/discard", { flow_id: flowId, nonce });
}

async function recoverAmbiguousFlowMutation({
  flowId,
  nonce,
  mutationError,
  accept,
}: {
  flowId: string;
  nonce: string;
  mutationError: unknown;
  accept: (result: AuthFlowResult) => boolean;
}): Promise<AuthFlowResult> {
  let recoveryError: unknown = new Error(
    "Authentication flow status was not terminal.",
  );
  try {
    const recovered = parseAuthFlowResult(
      await postAuthJSON("/auth/flows/status", {
        flow_id: flowId,
        nonce,
      }),
    );
    if (accept(recovered)) return recovered;
  } catch (error) {
    recoveryError = error;
  }
  try {
    // Recovery could not prove the outcome, so the flow must not keep the
    // ability to issue a session later. Discarding it revokes exactly what
    // it may have committed — never another account's session.
    await discardAuthFlow(flowId, nonce);
  } catch (discardError) {
    throw new AuthFlowRecoveryFailedError(
      mutationError,
      recoveryError,
      discardError,
    );
  }
  throw mutationError;
}

function isAmbiguousMutationError(error: unknown): boolean {
  if (!(error instanceof AuthAPIError)) return true;
  return (
    error.status === 0 ||
    (error.status >= 200 && error.status < 300) ||
    error.status === 408 ||
    error.status === 429 ||
    error.status >= 500
  );
}

function parseAuthFlowResult(value: unknown): AuthFlowResult {
  if (!isObject(value)) invalidResponse();
  const flowId = requiredString(value.flow_id, 256);
  const outcome = value.outcome;
  const expiresAt = optionalString(value.expires_at, 64);
  const continuation = optionalString(value.continuation, 2_048);

  if (outcome === "proof_required" && expiresAt) {
    if (value.email_challenge === undefined) {
      return { flowId, outcome, expiresAt };
    }
    const emailChallenge = parseEmailChallenge(value.email_challenge);
    if (emailChallenge.flowId !== flowId) invalidResponse();
    return { flowId, outcome, expiresAt, emailChallenge };
  }
  if (outcome === "confirmation_required") {
    const nextAction = value.next_action;
    if (
      (nextAction === "create_account" || nextAction === "sign_in") &&
      continuation &&
      expiresAt
    ) {
      return { flowId, outcome, nextAction, continuation, expiresAt };
    }
  }
  if (
    (outcome === "signed_in" || outcome === "account_created") &&
    continuation &&
    expiresAt &&
    typeof value.human_id === "string" &&
    value.human_id.length > 0 &&
    value.human_id.length <= 256
  ) {
    return { flowId, outcome, continuation, expiresAt, humanId: value.human_id };
  }
  return invalidResponse();
}

export function parseEmailChallenge(value: unknown): EmailChallengeStatus {
  if (!isObject(value)) invalidResponse();
  const attempts = value.attempts_remaining;
  const delivery = value.delivery;
  if (
    typeof attempts !== "number" ||
    !Number.isInteger(attempts) ||
    attempts < 0 ||
    attempts > 5 ||
    (delivery !== "" &&
      delivery !== "pending" &&
      delivery !== "sent" &&
      delivery !== "failed" &&
      delivery !== "cancelled")
  ) {
    invalidResponse();
  }
  return {
    flowId: requiredString(value.flow_id, 256),
    flowStatus: requiredString(value.flow_status, 64),
    email: requiredString(value.email, 320),
    flowExpiresAt: requiredString(value.flow_expires_at, 64),
    challengeExpiresAt: requiredString(value.challenge_expires_at, 64),
    attemptsRemaining: attempts,
    delivery,
    resendAvailableAt: requiredString(value.resend_available_at, 64),
    ...(typeof value.human_id === "string" &&
    value.human_id.length > 0 &&
    value.human_id.length <= 256
      ? { humanId: value.human_id }
      : {}),
  };
}

function requiredString(value: unknown, maxLength: number): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maxLength
  ) {
    return invalidResponse();
  }
  return value;
}

function optionalString(value: unknown, maxLength: number): string | undefined {
  if (value === undefined) return undefined;
  return requiredString(value, maxLength);
}

function invalidResponse(): never {
  throw new AuthAPIError("Invalid authentication flow response.", 0);
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
