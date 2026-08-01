import type { AuthIntent } from "./auth-flow-client";

const noticeKey = "sumi.auth.outcome-notice.v1";
const receiptHistoryKey = "sumi.auth.outcome-receipts.v1";
const noticeLifetimeMilliseconds = 10 * 60_000;
const futureClockSkewMilliseconds = 60_000;
const maximumReceipts = 32;

export type AuthTerminalOutcome =
  | "account_created"
  | "signed_in"
  | "provider_linked";

export interface AuthOutcomeScope {
  firebaseUID: string;
  humanId: string;
}

export interface AuthOutcomeNotice extends AuthOutcomeScope {
  version: 1;
  outcome: AuthTerminalOutcome;
  intent: AuthIntent;
  intentTransition: "none" | "confirmed" | "recovery_proved";
  receiptId: string;
  createdAt: string;
  expiresAt: string;
}

interface AuthOutcomeReceipt {
  receiptId: string;
  expiresAt: string;
}

interface AuthOutcomeReceiptHistory {
  version: 1;
  receipts: AuthOutcomeReceipt[];
}

export function publishAuthOutcomeNotice({
  scope,
  outcome,
  intent,
  intentTransition = "none",
  receiptId,
}: {
  scope: AuthOutcomeScope;
  outcome: AuthTerminalOutcome;
  intent: AuthIntent;
  intentTransition?: AuthOutcomeNotice["intentTransition"];
  receiptId: string;
}): AuthOutcomeNotice | null {
  const createdAt = new Date();
  const notice: AuthOutcomeNotice = {
    version: 1,
    ...scope,
    outcome,
    intent,
    intentTransition,
    receiptId,
    createdAt: createdAt.toISOString(),
    expiresAt: new Date(
      createdAt.getTime() + noticeLifetimeMilliseconds,
    ).toISOString(),
  };
  if (!isAuthOutcomeNotice(notice)) return null;
  try {
    sessionStorage.setItem(noticeKey, JSON.stringify(notice));
  } catch {
    // An in-memory notice still gives this page one opportunity to report it.
    return notice;
  }
  // Claim before returning the notice to React. A dismissal or reload cannot
  // resurrect pending state after this display has been committed.
  return takeAuthOutcomeNotice(scope);
}

export function hasPendingAuthOutcomeNotice(): boolean {
  const value = readSessionJSON(noticeKey);
  if (isAuthOutcomeNotice(value) && !isExpired(value)) return true;
  if (value !== null) clearStoredNotice();
  return false;
}

/**
 * Returns a matching notice once and records its non-sensitive terminal
 * receipt before deleting the pending payload. Receipt history is bounded and
 * deliberately survives logout so an old receipt cannot be replayed later.
 */
export function takeAuthOutcomeNotice(
  scope: AuthOutcomeScope,
): AuthOutcomeNotice | null {
  const value = readSessionJSON(noticeKey);
  if (
    !isAuthOutcomeNotice(value) ||
    !sameScope(value, scope) ||
    isExpired(value)
  ) {
    clearStoredNotice();
    return null;
  }

  const receipts = loadReceiptHistory();
  if (receipts.some((receipt) => receipt.receiptId === value.receiptId)) {
    clearStoredNotice();
    return null;
  }

  persistReceiptHistory([
    ...receipts,
    { receiptId: value.receiptId, expiresAt: value.expiresAt },
  ]);
  clearStoredNotice();
  return value;
}

/** Clears only an undisplayed notice; replay receipts remain until expiry. */
export function clearAuthOutcomeNotice(): void {
  clearStoredNotice();
}

function loadReceiptHistory(): AuthOutcomeReceipt[] {
  const value = readSessionJSON(receiptHistoryKey);
  if (value === null) return [];
  if (!isReceiptHistory(value)) {
    clearReceiptHistory();
    return [];
  }
  const receipts = value.receipts.filter((receipt) => !isExpired(receipt));
  if (receipts.length !== value.receipts.length)
    persistReceiptHistory(receipts);
  return receipts;
}

function persistReceiptHistory(receipts: AuthOutcomeReceipt[]): void {
  const bounded = receipts.slice(-maximumReceipts);
  try {
    sessionStorage.setItem(
      receiptHistoryKey,
      JSON.stringify({
        version: 1,
        receipts: bounded,
      } satisfies AuthOutcomeReceiptHistory),
    );
  } catch {
    // Storage failures cannot prevent the current page from reporting once.
  }
}

function clearStoredNotice(): void {
  try {
    sessionStorage.removeItem(noticeKey);
  } catch {
    // The UI state remains the source of truth after a storage failure.
  }
}

function clearReceiptHistory(): void {
  try {
    sessionStorage.removeItem(receiptHistoryKey);
  } catch {
    // A later successful navigation will retry bounded cleanup.
  }
}

function readSessionJSON(key: string): unknown {
  try {
    return JSON.parse(sessionStorage.getItem(key) ?? "null") as unknown;
  } catch {
    try {
      sessionStorage.removeItem(key);
    } catch {
      // Malformed state is never trusted even when cleanup is unavailable.
    }
    return null;
  }
}

function isAuthOutcomeNotice(value: unknown): value is AuthOutcomeNotice {
  if (!hasScope(value) || value.version !== 1) return false;
  if (
    !hasOnlyKeys(value, [
      "version",
      "firebaseUID",
      "humanId",
      "outcome",
      "intent",
      "intentTransition",
      "receiptId",
      "createdAt",
      "expiresAt",
    ]) ||
    (value.outcome !== "account_created" &&
      value.outcome !== "signed_in" &&
      value.outcome !== "provider_linked") ||
    (value.intent !== "sign_in" && value.intent !== "sign_up") ||
    (value.intentTransition !== "none" &&
      value.intentTransition !== "confirmed" &&
      value.intentTransition !== "recovery_proved") ||
    !isReceiptId(value.receiptId) ||
    !isTimestamp(value.createdAt) ||
    !isTimestamp(value.expiresAt) ||
    !hasBoundedTiming(value.createdAt, value.expiresAt)
  ) {
    return false;
  }
  if (value.intentTransition === "confirmed") {
    return (
      (value.outcome === "account_created" && value.intent === "sign_in") ||
      (value.outcome === "signed_in" && value.intent === "sign_up")
    );
  }
  return (
    value.intentTransition !== "recovery_proved" ||
    (value.intent === "sign_up" &&
      (value.outcome === "signed_in" || value.outcome === "provider_linked"))
  );
}

function isReceiptHistory(value: unknown): value is AuthOutcomeReceiptHistory {
  return (
    isObject(value) &&
    value.version === 1 &&
    hasOnlyKeys(value, ["version", "receipts"]) &&
    Array.isArray(value.receipts) &&
    value.receipts.length <= maximumReceipts &&
    value.receipts.every(
      (receipt) =>
        isObject(receipt) &&
        hasOnlyKeys(receipt, ["receiptId", "expiresAt"]) &&
        isReceiptId(receipt.receiptId) &&
        isTimestamp(receipt.expiresAt) &&
        Date.parse(receipt.expiresAt) <=
          Date.now() + noticeLifetimeMilliseconds + futureClockSkewMilliseconds,
    )
  );
}

function hasScope(
  value: unknown,
): value is Record<string, unknown> & AuthOutcomeScope {
  return (
    isObject(value) &&
    typeof value.firebaseUID === "string" &&
    value.firebaseUID.length > 0 &&
    value.firebaseUID.length <= 128 &&
    typeof value.humanId === "string" &&
    value.humanId.length > 0 &&
    value.humanId.length <= 256
  );
}

function hasOnlyKeys(value: Record<string, unknown>, keys: string[]): boolean {
  return (
    Object.keys(value).length === keys.length &&
    Object.keys(value).every((key) => keys.includes(key))
  );
}

function isReceiptId(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_-]{1,256}$/.test(value);
}

function isTimestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length <= 64 &&
    Number.isFinite(Date.parse(value))
  );
}

function hasBoundedTiming(createdAt: string, expiresAt: string): boolean {
  const created = Date.parse(createdAt);
  const expires = Date.parse(expiresAt);
  const now = Date.now();
  return (
    created <= now + futureClockSkewMilliseconds &&
    expires > created &&
    expires <= now + noticeLifetimeMilliseconds + futureClockSkewMilliseconds &&
    expires - created <= noticeLifetimeMilliseconds
  );
}

function isExpired(value: { expiresAt: string }): boolean {
  return Date.parse(value.expiresAt) <= Date.now();
}

function sameScope(value: AuthOutcomeScope, scope: AuthOutcomeScope): boolean {
  return (
    value.firebaseUID === scope.firebaseUID && value.humanId === scope.humanId
  );
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
