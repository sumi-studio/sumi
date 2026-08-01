import type { AuthFlowProvider, AuthIntent } from "./auth-flow-client";

const emailFlowPrefix = "sumi.auth.email-flow.v1.";
export const emailFlowStateParameter = "sumi_auth_state";

export interface PendingAuthFlow {
  flowId: string;
  nonce: string;
  intent: AuthIntent;
  provider: AuthFlowProvider;
  expiresAt: string;
}

export interface PendingEmailAuthFlow extends PendingAuthFlow {
  provider: "email_link";
  email: string;
  stage: "link_sent" | "firebase_complete";
  credentialRecovery?: PendingCredentialRecovery;
}

export type RecoverableProvider = "google.com" | "github.com";

export interface SerializedOAuthCredential {
  providerId: RecoverableProvider;
  signInMethod: RecoverableProvider;
  idToken?: string;
  accessToken?: string;
  secret?: string;
  nonce?: string;
  pendingToken?: string;
}

export interface PendingCredentialRecovery {
  version: 1;
  provider: RecoverableProvider;
  requestedIntent: AuthIntent;
  expiresAt: string;
  credential: SerializedOAuthCredential;
}

export function createEmailFlowState(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(18));
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary)
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/, "");
}

export function emailFlowContinuation(state: string): string {
  return `/?${emailFlowStateParameter}=${encodeURIComponent(state)}`;
}

export function emailFlowStateFromLocation(): string | null {
  try {
    const state = new URL(globalThis.location.href).searchParams.get(
      emailFlowStateParameter,
    );
    return state && /^[A-Za-z0-9_-]{24}$/.test(state) ? state : null;
  } catch {
    return null;
  }
}

export function savePendingEmailFlow(
  state: string,
  flow: PendingEmailAuthFlow,
): void {
  localStorage.setItem(`${emailFlowPrefix}${state}`, JSON.stringify(flow));
}

export function loadPendingEmailFlow(
  state: string,
): PendingEmailAuthFlow | null {
  try {
    const parsed: unknown = JSON.parse(
      localStorage.getItem(`${emailFlowPrefix}${state}`) ?? "null",
    );
    if (!isPendingEmailFlow(parsed)) return null;
    if (
      parsed.credentialRecovery &&
      Date.parse(parsed.credentialRecovery.expiresAt) <= Date.now()
    ) {
      clearPendingEmailFlow(state);
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export function clearPendingEmailFlow(state: string): void {
  try {
    localStorage.removeItem(`${emailFlowPrefix}${state}`);
  } catch {
    // A completed server flow must not be undone by storage cleanup failure.
  }
}

export function clearEmailFlowLocation(): void {
  try {
    const url = new URL(globalThis.location.href);
    url.searchParams.delete(emailFlowStateParameter);
    for (const parameter of [
      "apiKey",
      "oobCode",
      "mode",
      "lang",
      "continueUrl",
    ]) {
      url.searchParams.delete(parameter);
    }
    history.replaceState(
      history.state,
      "",
      `${url.pathname}${url.search}${url.hash}`,
    );
  } catch {
    // Authentication state remains valid even if the browser blocks history cleanup.
  }
}

function isPendingEmailFlow(value: unknown): value is PendingEmailAuthFlow {
  return (
    isPendingFlow(value) &&
    value.provider === "email_link" &&
    "email" in value &&
    typeof value.email === "string" &&
    value.email.length > 0 &&
    value.email.length <= 320 &&
    "stage" in value &&
    (value.stage === "link_sent" || value.stage === "firebase_complete") &&
    (!("credentialRecovery" in value) ||
      value.credentialRecovery === undefined ||
      isPendingCredentialRecovery(value.credentialRecovery, value.expiresAt))
  );
}

function isPendingCredentialRecovery(
  value: unknown,
  flowExpiresAt: string,
): value is PendingCredentialRecovery {
  if (
    typeof value !== "object" ||
    value === null ||
    !("version" in value) ||
    value.version !== 1 ||
    !("provider" in value) ||
    (value.provider !== "google.com" && value.provider !== "github.com") ||
    !("requestedIntent" in value) ||
    (value.requestedIntent !== "sign_in" &&
      value.requestedIntent !== "sign_up") ||
    !("expiresAt" in value) ||
    typeof value.expiresAt !== "string" ||
    value.expiresAt.length === 0 ||
    value.expiresAt.length > 64 ||
    !Number.isFinite(Date.parse(value.expiresAt)) ||
    Date.parse(value.expiresAt) > Date.now() + 10 * 60_000 + 5_000 ||
    !Number.isFinite(Date.parse(flowExpiresAt)) ||
    Date.parse(value.expiresAt) > Date.parse(flowExpiresAt) ||
    !("credential" in value)
  ) {
    return false;
  }
  return isSerializedOAuthCredential(value.credential, value.provider);
}

function isSerializedOAuthCredential(
  value: unknown,
  provider: RecoverableProvider,
): value is SerializedOAuthCredential {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  const allowed = new Set([
    "providerId",
    "signInMethod",
    "idToken",
    "accessToken",
    "secret",
    "nonce",
    "pendingToken",
  ]);
  if (JSON.stringify(record).length > 24 * 1024) return false;
  if (Object.keys(record).some((key) => !allowed.has(key))) return false;
  if (record.providerId !== provider || record.signInMethod !== provider) {
    return false;
  }
  for (const key of [
    "idToken",
    "accessToken",
    "secret",
    "nonce",
    "pendingToken",
  ] as const) {
    const field = record[key];
    if (
      field !== undefined &&
      (typeof field !== "string" || field.length > 12_288)
    ) {
      return false;
    }
  }
  return [record.idToken, record.accessToken, record.pendingToken].some(
    (field) => typeof field === "string" && field.length > 0,
  );
}

function isPendingFlow(value: unknown): value is PendingAuthFlow {
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
    value.expiresAt.length <= 64
  );
}
