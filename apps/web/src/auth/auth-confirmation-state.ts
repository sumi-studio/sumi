import type {
  AuthConfirmationAction,
  AuthFlowProvider,
  AuthIntent,
} from "./auth-flow-client";

const confirmationKey = "sumi.auth.confirmation.v1";

export interface PendingAuthConfirmation {
  flowId: string;
  nonce: string;
  intent: AuthIntent;
  provider: AuthFlowProvider;
  expiresAt: string;
  action: AuthConfirmationAction;
}

export function savePendingConfirmation(
  confirmation: PendingAuthConfirmation,
): void {
  sessionStorage.setItem(confirmationKey, JSON.stringify(confirmation));
}

export function loadPendingConfirmation(): PendingAuthConfirmation | null {
  try {
    const parsed: unknown = JSON.parse(
      sessionStorage.getItem(confirmationKey) ?? "null",
    );
    return isPendingConfirmation(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function clearPendingConfirmation(): void {
  try {
    sessionStorage.removeItem(confirmationKey);
  } catch {
    // An in-memory confirmation can still complete in storage-restricted browsers.
  }
}

function isPendingConfirmation(
  value: unknown,
): value is PendingAuthConfirmation {
  return (
    typeof value === "object" &&
    value !== null &&
    "flowId" in value &&
    typeof value.flowId === "string" &&
    value.flowId.length > 0 &&
    value.flowId.length <= 256 &&
    "nonce" in value &&
    typeof value.nonce === "string" &&
    /^[A-Za-z0-9_-]{43}$/.test(value.nonce) &&
    "intent" in value &&
    (value.intent === "sign_in" || value.intent === "sign_up") &&
    "provider" in value &&
    (value.provider === "email_link" ||
      value.provider === "google.com" ||
      value.provider === "github.com") &&
    "expiresAt" in value &&
    typeof value.expiresAt === "string" &&
    value.expiresAt.length > 0 &&
    value.expiresAt.length <= 64 &&
    "action" in value &&
    (value.action === "create_account" || value.action === "sign_in")
  );
}
