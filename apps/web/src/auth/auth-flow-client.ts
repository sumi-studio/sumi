import {
  AuthAPIError,
  logoutSumiSession,
  postAuthJSON,
} from "./session-client";

export type AuthIntent = "sign_in" | "sign_up";
export type AuthFlowProvider = "email_link" | "google.com" | "github.com";
export type AuthConfirmationAction = "create_account" | "sign_in";

export type AuthFlowResult =
  | {
      flowId: string;
      outcome: "proof_required";
      expiresAt: string;
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
    };

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
      "Authentication flow response was ambiguous and recovery logout failed.",
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
  if (request.provider === "email_link") {
    body.email = request.email ?? "";
  }
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
}: {
  flowId: string;
  nonce: string;
  idToken: string;
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
  return result;
}

export async function confirmAuthFlow({
  flowId,
  nonce,
  action,
}: {
  flowId: string;
  nonce: string;
  action: AuthConfirmationAction;
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
  return result;
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
    await logoutSumiSession();
  } catch (logoutError) {
    throw new AuthFlowRecoveryFailedError(
      mutationError,
      recoveryError,
      logoutError,
    );
  }
  throw mutationError;
}

function isAmbiguousMutationError(error: unknown): boolean {
  if (!(error instanceof AuthAPIError)) return true;
  return (
    error.status === 0 ||
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
    return { flowId, outcome, expiresAt };
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
    expiresAt
  ) {
    return { flowId, outcome, continuation, expiresAt };
  }
  return invalidResponse();
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
