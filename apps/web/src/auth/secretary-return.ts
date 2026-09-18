import { AuthAPIError, fetchCSRFToken } from "./session-client";

/**
 * The signed-in owner's view of one secretary-return session. Proof fields
 * and the transfer key never appear here — they belong to the grant holder,
 * the Local command. `surrendered_by` names the forward transfer that
 * brought the secretary to Cloud, when one did: it is the lineage a
 * same-install receiver asserts when it reclaims its surrendered copy.
 */
export interface SecretaryReturnSession {
  session_id: string;
  transfer_id: string;
  persona_id: string;
  status:
    | "awaiting_destination"
    | "sealed"
    | "cancelling"
    | "completed"
    | "aborted"
    | "cancelled"
    | "expired";
  admit_until: string;
  source_placement_id: string;
  surrendered_by?: string;
  state_only: true;
  not_included: Array<{ name: string; owner: string; reason: string }>;
  preflight?: {
    active_jobs: number;
    model_intent_kind?: string;
    files: string;
  };
  arrival?: {
    content_sha256: string;
    continuity: Record<string, number>;
    rows: Record<string, number>;
  };
}

export interface SecretaryReturnCreated {
  return_url: string;
  session: SecretaryReturnSession;
}

/**
 * The return routes are not mounted on this API — the feature is off
 * (SUMI_TRANSFER_PUBLIC_BASE_URL unset). Distinct from an ordinary 404:
 * every mounted route's "nothing here" answer is the JSON "return session
 * not found", so a 404 without it means the surface itself is absent.
 */
export class ReturnFeatureDisabledError extends AuthAPIError {
  constructor() {
    super("return_feature_disabled", 404);
    this.name = "ReturnFeatureDisabledError";
  }
}

export function isReturnFeatureDisabled(error: unknown): boolean {
  return error instanceof ReturnFeatureDisabledError;
}

/**
 * Shared-file handling on return is still an undecided product choice, so
 * the API refuses to admit a new move. Nothing has begun — this is the
 * honest "not available yet" state, distinct from a route that is absent.
 */
export class ReturnPolicyUndecidedError extends AuthAPIError {
  constructor(detail: string) {
    super(detail, 409);
    this.name = "ReturnPolicyUndecidedError";
  }
}

export function isReturnPolicyUndecided(error: unknown): boolean {
  return error instanceof ReturnPolicyUndecidedError;
}

async function readBody(response: Response): Promise<Record<string, unknown>> {
  const parsed: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    const detail =
      parsed !== null &&
      typeof parsed === "object" &&
      "error" in parsed &&
      typeof parsed.error === "string"
        ? parsed.error
        : "return request failed";
    if (response.status === 404 && detail !== "return session not found") {
      throw new ReturnFeatureDisabledError();
    }
    if (
      parsed !== null &&
      typeof parsed === "object" &&
      "code" in parsed &&
      parsed.code === "file_policy_undecided"
    ) {
      throw new ReturnPolicyUndecidedError(detail);
    }
    const error = new AuthAPIError(detail, response.status);
    if (
      response.status === 409 &&
      parsed !== null &&
      typeof parsed === "object" &&
      "session" in parsed &&
      isReturnSession(parsed.session)
    ) {
      // A 409 create answer names the open session so a lost return-URL
      // reply still recovers its status; the grant is never repeated.
      (error as AuthAPIError & { session?: SecretaryReturnSession }).session =
        parsed.session;
    }
    throw error;
  }
  return (parsed ?? {}) as Record<string, unknown>;
}

/**
 * A sibling tab can rotate the shared CSRF cookie between this tab's token
 * fetch and the POST that uses it, so one ordinary call can land a 401 that
 * says nothing about the session. One immediate retry carries the token the
 * jar now holds; a 401 that persists is a real refusal — a signed-out,
 * rotated or foreign session still ends the attempt, never an endless loop.
 */
async function postReturn(
  path: string,
  body: Record<string, string>,
): Promise<Record<string, unknown>> {
  let response: Response | null = null;
  for (let attempt = 0; attempt < 2; attempt++) {
    const csrfToken = await fetchCSRFToken();
    response = await fetch(path, {
      method: "POST",
      credentials: "include",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "X-CSRF-Token": csrfToken,
      },
      body: JSON.stringify(body),
    });
    if (response.status !== 401) break;
  }
  if (response === null) throw new AuthAPIError("return request failed", 0);
  return readBody(response);
}

async function getReturn(path: string): Promise<Record<string, unknown>> {
  const response = await fetch(path, {
    method: "GET",
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  return readBody(response);
}

function isReturnSession(value: unknown): value is SecretaryReturnSession {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as SecretaryReturnSession).session_id === "string" &&
    typeof (value as SecretaryReturnSession).status === "string"
  );
}

/**
 * Opens a return session for this account's secretary and returns the
 * one-time return URL the Local command asks for. A 409 answer means an
 * open session already exists — its status comes back on the error so the
 * owner can continue or cancel it; the grant is never repeated.
 */
export async function createSecretaryReturn(): Promise<SecretaryReturnCreated> {
  const body = await postReturn("/api/secretary-return/sessions", {});
  const session = body.session;
  if (
    typeof body.return_url !== "string" ||
    body.return_url === "" ||
    !isReturnSession(session)
  ) {
    throw new AuthAPIError("Invalid return response.", 0);
  }
  return { return_url: body.return_url, session };
}

/** Reads this account's current return session — the open one, else the
 * most recent. 404 means no return exists. */
export async function readSecretaryReturn(): Promise<SecretaryReturnSession | null> {
  try {
    const body = await getReturn("/api/secretary-return/session");
    return isReturnSession(body) ? body : null;
  } catch (error) {
    if (
      !(error instanceof ReturnFeatureDisabledError) &&
      error instanceof AuthAPIError &&
      error.status === 404
    ) {
      return null;
    }
    throw error;
  }
}

/**
 * The owner's cancel. Before the seal it closes the session and nothing
 * moved; after it the session reports cancelling — the secretary stays
 * sealed on Cloud until the Local command's retire proof arrives (a
 * committed activation can still win, and is never undone by cancelling).
 */
export async function cancelSecretaryReturn(
  sessionId: string,
): Promise<SecretaryReturnSession> {
  const body = await postReturn(
    `/api/secretary-return/sessions/${sessionId}/cancel`,
    {},
  );
  if (!isReturnSession(body)) {
    throw new AuthAPIError("Invalid return response.", 0);
  }
  return body;
}

/** The return URL is a credential. It lives in sessionStorage only — it
 * never enters URLs, logs, or persistent storage beyond the tab's own
 * lifetime. */
const returnURLKey = "sumi.account.return-url.v1";

export interface SavedReturnURL {
  sessionId: string;
  returnURL: string;
}

export function saveReturnURL(sessionId: string, returnURL: string): void {
  try {
    sessionStorage.setItem(returnURLKey, JSON.stringify({ sessionId, returnURL }));
  } catch {
    // The in-memory value still covers this page lifetime.
  }
}

export function loadReturnURL(sessionId: string): SavedReturnURL | null {
  try {
    const parsed: unknown = JSON.parse(
      sessionStorage.getItem(returnURLKey) ?? "null",
    );
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      (parsed as { sessionId?: unknown }).sessionId === sessionId &&
      typeof (parsed as { returnURL?: unknown }).returnURL === "string"
    ) {
      return parsed as SavedReturnURL;
    }
  } catch {
    // Unreadable storage answers as absent.
  }
  return null;
}

export function clearReturnURL(sessionId: string): void {
  try {
    if (loadReturnURL(sessionId) !== null) {
      sessionStorage.removeItem(returnURLKey);
    }
  } catch {
    // Clearing is best-effort.
  }
}
