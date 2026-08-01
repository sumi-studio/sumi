import type { AuthFlowProvider, AuthIntent } from "./auth-flow-client";

const emailFlowPrefix = "sumi.auth.email-flow.v1.";
const consumedEmailFlowPrefix = "sumi.auth.email-flow-consumed.v1.";
const credentialCleanupTimers = new Map<
  string,
  ReturnType<typeof globalThis.setTimeout>
>();
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

interface ConsumedEmailFlowMarker {
  version: 1;
  state: string;
  flowId: string;
  claim: string;
  expiresAt: string;
}

export class PendingEmailFlowStorageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PendingEmailFlowStorageError";
  }
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
  cleanupPendingEmailFlowStorage();
  const key = `${emailFlowPrefix}${state}`;
  const raw = JSON.stringify(flow);
  localStorage.setItem(key, raw);
  if (flow.credentialRecovery && isPendingEmailFlow(flow)) {
    scheduleCredentialCleanup(state, raw, flow.credentialRecovery.expiresAt);
  }
}

export function loadPendingEmailFlow(
  state: string,
): PendingEmailAuthFlow | null {
  if (!isEmailFlowState(state)) return null;
  cleanupPendingEmailFlowStorage();
  const key = `${emailFlowPrefix}${state}`;
  try {
    if (localStorage.getItem(`${consumedEmailFlowPrefix}${state}`) !== null) {
      cancelCredentialCleanup(key);
      removeStorageItemBestEffort(key);
      return null;
    }
    const raw = localStorage.getItem(key);
    if (raw === null) return null;
    const parsed: unknown = JSON.parse(raw);
    if (!isPendingEmailFlow(parsed) || isExpiredPendingEmailFlow(parsed)) {
      cancelCredentialCleanup(key);
      removeStorageItemBestEffort(key);
      return null;
    }
    if (parsed.credentialRecovery) {
      scheduleCredentialCleanup(
        state,
        raw,
        parsed.credentialRecovery.expiresAt,
      );
    }
    return parsed;
  } catch {
    cancelCredentialCleanup(key);
    removeStorageItemBestEffort(key);
    return null;
  }
}

export function clearPendingEmailFlow(state: string): void {
  if (!isEmailFlowState(state)) return;
  const flowKey = `${emailFlowPrefix}${state}`;
  cancelCredentialCleanup(flowKey);
  removeStorageItemBestEffort(flowKey);
  removeStorageItemBestEffort(`${consumedEmailFlowPrefix}${state}`);
}

export function consumePendingCredentialRecovery(
  state: string,
  expected: PendingEmailAuthFlow,
): void {
  if (!isEmailFlowState(state) || !expected.credentialRecovery) {
    throw new PendingEmailFlowStorageError(
      "Pending credential recovery is unavailable.",
    );
  }
  const flowKey = `${emailFlowPrefix}${state}`;
  const consumedKey = `${consumedEmailFlowPrefix}${state}`;
  const expectedRaw = JSON.stringify(expected);
  const marker: ConsumedEmailFlowMarker = {
    version: 1,
    state,
    flowId: expected.flowId,
    claim: createEmailFlowState(),
    expiresAt: expected.credentialRecovery.expiresAt,
  };
  const markerRaw = JSON.stringify(marker);

  try {
    if (localStorage.getItem(consumedKey) !== null) {
      removeStorageItemBestEffort(flowKey);
      throw new PendingEmailFlowStorageError(
        "Pending credential recovery was already consumed.",
      );
    }
    if (localStorage.getItem(flowKey) !== expectedRaw) {
      throw new PendingEmailFlowStorageError(
        "Pending credential recovery changed before use.",
      );
    }

    // The credential-free marker fences stale same-state consumers. The
    // credential-bearing record is then removed and its absence verified
    // before any Firebase provider mutation is allowed to begin.
    localStorage.setItem(consumedKey, markerRaw);
    if (localStorage.getItem(consumedKey) !== markerRaw) {
      throw new PendingEmailFlowStorageError(
        "Pending credential recovery could not be claimed.",
      );
    }
    if (localStorage.getItem(flowKey) !== expectedRaw) {
      throw new PendingEmailFlowStorageError(
        "Pending credential recovery changed while being claimed.",
      );
    }
    localStorage.removeItem(flowKey);
    if (localStorage.getItem(flowKey) !== null) {
      throw new PendingEmailFlowStorageError(
        "Pending credential recovery could not be deleted.",
      );
    }
    cancelCredentialCleanup(flowKey);
  } catch (error) {
    if (error instanceof PendingEmailFlowStorageError) throw error;
    throw new PendingEmailFlowStorageError(
      "Pending credential recovery storage is unavailable.",
    );
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

function isExpiredPendingEmailFlow(flow: PendingEmailAuthFlow): boolean {
  const flowExpiry = Date.parse(flow.expiresAt);
  return (
    !Number.isFinite(flowExpiry) ||
    flowExpiry <= Date.now() ||
    (flow.credentialRecovery !== undefined &&
      Date.parse(flow.credentialRecovery.expiresAt) <= Date.now())
  );
}

export function cleanupPendingEmailFlowStorage(): void {
  try {
    const keys: string[] = [];
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index);
      if (
        key?.startsWith(emailFlowPrefix) ||
        key?.startsWith(consumedEmailFlowPrefix)
      ) {
        keys.push(key);
      }
    }
    for (const key of keys) cleanupStorageKey(key);
    for (const key of credentialCleanupTimers.keys()) {
      if (localStorage.getItem(key) === null) cancelCredentialCleanup(key);
    }
  } catch {
    // Exact-state reads and credential consumption still fail closed when
    // storage itself is unavailable.
  }
}

function cleanupStorageKey(key: string): void {
  try {
    const raw = localStorage.getItem(key);
    if (raw === null) return;
    const parsed: unknown = JSON.parse(raw);
    if (key.startsWith(consumedEmailFlowPrefix)) {
      if (!isConsumedEmailFlowMarker(parsed)) removeStorageItemBestEffort(key);
      else if (Date.parse(parsed.expiresAt) <= Date.now()) {
        removeStorageItemBestEffort(key);
      }
      return;
    }
    if (!isPendingEmailFlow(parsed) || isExpiredPendingEmailFlow(parsed)) {
      cancelCredentialCleanup(key);
      removeStorageItemBestEffort(key);
      return;
    }
    const state = key.slice(emailFlowPrefix.length);
    if (localStorage.getItem(`${consumedEmailFlowPrefix}${state}`) !== null) {
      cancelCredentialCleanup(key);
      removeStorageItemBestEffort(key);
      return;
    }
    if (parsed.credentialRecovery) {
      scheduleCredentialCleanup(
        state,
        raw,
        parsed.credentialRecovery.expiresAt,
      );
    }
  } catch {
    if (key.startsWith(emailFlowPrefix)) cancelCredentialCleanup(key);
    removeStorageItemBestEffort(key);
  }
}

function isConsumedEmailFlowMarker(
  value: unknown,
): value is ConsumedEmailFlowMarker {
  return (
    typeof value === "object" &&
    value !== null &&
    "version" in value &&
    value.version === 1 &&
    "state" in value &&
    typeof value.state === "string" &&
    isEmailFlowState(value.state) &&
    "flowId" in value &&
    typeof value.flowId === "string" &&
    value.flowId.length > 0 &&
    value.flowId.length <= 256 &&
    "claim" in value &&
    typeof value.claim === "string" &&
    isEmailFlowState(value.claim) &&
    "expiresAt" in value &&
    typeof value.expiresAt === "string" &&
    Number.isFinite(Date.parse(value.expiresAt))
  );
}

function removeStorageItemBestEffort(key: string): void {
  try {
    localStorage.removeItem(key);
  } catch {
    // Callers that must prove deletion use consumePendingCredentialRecovery.
  }
}

function scheduleCredentialCleanup(
  state: string,
  expectedRaw: string,
  expiresAt: string,
): void {
  const key = `${emailFlowPrefix}${state}`;
  cancelCredentialCleanup(key);
  const expiry = Date.parse(expiresAt);
  if (!Number.isFinite(expiry)) return;
  const delay = Math.max(0, expiry - Date.now() + 1);
  const timer = globalThis.setTimeout(() => {
    credentialCleanupTimers.delete(key);
    expireCredentialRecord(state, expectedRaw, expiresAt);
  }, delay);
  credentialCleanupTimers.set(key, timer);
}

function expireCredentialRecord(
  state: string,
  expectedRaw: string,
  expectedExpiry: string,
): void {
  const key = `${emailFlowPrefix}${state}`;
  try {
    if (Date.parse(expectedExpiry) > Date.now()) {
      scheduleCredentialCleanup(state, expectedRaw, expectedExpiry);
      return;
    }
    const currentRaw = localStorage.getItem(key);
    if (currentRaw !== expectedRaw) return;
    const current: unknown = JSON.parse(currentRaw);
    if (
      !isPendingEmailFlow(current) ||
      current.credentialRecovery?.expiresAt !== expectedExpiry ||
      Date.parse(current.credentialRecovery.expiresAt) > Date.now()
    ) {
      return;
    }
    removeStorageItemBestEffort(key);
  } catch {
    removeStorageItemBestEffort(key);
  }
}

function cancelCredentialCleanup(key: string): void {
  const timer = credentialCleanupTimers.get(key);
  if (timer !== undefined) globalThis.clearTimeout(timer);
  credentialCleanupTimers.delete(key);
}

function isEmailFlowState(state: string): boolean {
  return /^[A-Za-z0-9_-]{24}$/.test(state);
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

cleanupPendingEmailFlowStorage();
