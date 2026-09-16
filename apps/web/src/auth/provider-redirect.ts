/**
 * Provider account operations (link a login method, reauthenticate before
 * unlink) leave this tab for the OAuth provider and return through Firebase's
 * redirect result. The pending provider operation itself lives in
 * provider-settings state; this marker is the per-tab receipt that says "this
 * tab sent a provider redirect whose result has not been read yet".
 *
 * sessionStorage is deliberate: the receipt names this tab's in-flight
 * navigation only. Another tab of the same browser must not consume a return
 * it never sent, and the durable operation record (localStorage, scoped by
 * firebaseUid + humanId) stays resumable on its own if the tab dies.
 */

const providerRedirectKey = "sumi.auth.provider-redirect.v1";

export type ProviderRedirectKind = "link" | "reauth";

export interface PendingProviderRedirect {
  version: 1;
  kind: ProviderRedirectKind;
  /** The provider the person authenticates with at the OAuth page. */
  provider: "google.com" | "github.com";
  /** Nonce of the pending provider operation this redirect serves. */
  nonce: string;
  firebaseUid: string;
  humanId: string;
  sentAt: string;
}

export function savePendingProviderRedirect(
  redirect: PendingProviderRedirect,
): boolean {
  const raw = JSON.stringify(redirect);
  try {
    sessionStorage.setItem(providerRedirectKey, raw);
    return sessionStorage.getItem(providerRedirectKey) === raw;
  } catch {
    return false;
  }
}

/**
 * Reads the receipt without claiming it. The settings popover uses this to
 * reopen on a return; only a scoped match may take and consume the result.
 */
export function peekPendingProviderRedirect(): PendingProviderRedirect | null {
  try {
    const parsed: unknown = JSON.parse(
      sessionStorage.getItem(providerRedirectKey) ?? "null",
    );
    return isPendingProviderRedirect(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

/**
 * Read and delete the receipt. A redirect return is consumed exactly once by
 * the tab that sent it; a reload or StrictMode remount cannot claim it again.
 */
export function takePendingProviderRedirect(): PendingProviderRedirect | null {
  const redirect = peekPendingProviderRedirect();
  clearPendingProviderRedirect();
  return redirect;
}

export function clearPendingProviderRedirect(): void {
  try {
    sessionStorage.removeItem(providerRedirectKey);
  } catch {
    // A lost receipt only means no result is claimed; the operation record
    // remains resumable and its server-side expiry still fences it.
  }
}

function isPendingProviderRedirect(
  value: unknown,
): value is PendingProviderRedirect {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    record.version === 1 &&
    (record.kind === "link" || record.kind === "reauth") &&
    (record.provider === "google.com" || record.provider === "github.com") &&
    typeof record.nonce === "string" &&
    record.nonce.length >= 32 &&
    record.nonce.length <= 128 &&
    typeof record.firebaseUid === "string" &&
    record.firebaseUid.length > 0 &&
    record.firebaseUid.length <= 128 &&
    typeof record.humanId === "string" &&
    record.humanId.length > 0 &&
    record.humanId.length <= 256 &&
    typeof record.sentAt === "string" &&
    Number.isFinite(Date.parse(record.sentAt))
  );
}
