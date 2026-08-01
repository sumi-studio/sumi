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
    return isPendingEmailFlow(parsed) ? parsed : null;
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
    (value.stage === "link_sent" || value.stage === "firebase_complete")
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
