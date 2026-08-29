import { hasSafeDisplayCharacters } from "../lib/text-length";

const maxAuthResponseBytes = 4_096;
const csrfTokenPattern = /^[A-Za-z0-9_-]{43}$/;
const authorityBindingIDPattern = /^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$/;
export const authRequestTimeoutMilliseconds = 15_000;
const maxDisplayNameCodePoints = 80;
const maxTaglineCodePoints = 100;

export interface SumiSessionUser {
  id: string;
  displayName: string | null;
}

export interface ConfirmedSumiProfile {
  participant: { kind: "human"; humanId: string };
  displayName: string;
  tagline: string;
}

export interface SumiProfilePatch {
  displayName?: string;
  tagline?: string;
}

export interface SumiProfileUpdate {
  id: string;
  displayName: string;
  profile: ConfirmedSumiProfile;
}

export type SumiSessionStatus =
  | { authenticated: false }
  | {
      authenticated: true;
      authorityBindingId: string;
      user: SumiSessionUser;
    };

export class AuthAPIError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "AuthAPIError";
    this.status = status;
  }
}

export class SumiProfileUpdateIndeterminateError extends Error {
  readonly cause: unknown;

  constructor(cause: unknown) {
    super(
      "The profile update may have committed, but its result could not be confirmed.",
    );
    this.name = "SumiProfileUpdateIndeterminateError";
    this.cause = cause;
  }
}

export class SumiSessionCompensatedError extends Error {
  readonly cause: unknown;

  constructor(cause: unknown) {
    super(
      "Sumi session verification failed after exchange; the session was logged out.",
    );
    this.name = "SumiSessionCompensatedError";
    this.cause = cause;
  }
}

export class SumiSessionCompensationFailedError extends AggregateError {
  constructor(exchangeError: unknown, logoutError: unknown) {
    super(
      [exchangeError, logoutError],
      "Sumi session verification failed and compensating logout did not complete.",
    );
    this.name = "SumiSessionCompensationFailedError";
  }
}

async function fetchCSRFToken(): Promise<string> {
  const response = await fetch("/auth/csrf", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal: authRequestSignal(),
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
  const body = await readAuthJSON(response);
  if (
    !isObject(body) ||
    typeof body.csrf_token !== "string" ||
    !csrfTokenPattern.test(body.csrf_token)
  ) {
    throw new AuthAPIError("Invalid authentication response.", response.status);
  }
  return body.csrf_token;
}

export async function postAuthJSON(
  path: `/auth/${string}`,
  body: Record<string, string>,
): Promise<unknown> {
  const csrfToken = await fetchCSRFToken();
  const response = await fetch(path, {
    method: "POST",
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrfToken,
    },
    body: JSON.stringify(body),
    signal: authRequestSignal(),
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
  return readAuthJSON(response);
}

/**
 * A terminal auth-flow response commits HttpOnly authority before the browser can
 * confirm its status. Keep both operations in one mutation and compensate
 * with a Sumi logout if any post-commit status read fails.
 */
export async function verifyCommittedSumiSession(): Promise<
  Extract<SumiSessionStatus, { authenticated: true }>
> {
  try {
    const session = await getSumiSession();
    if (!session.authenticated) {
      throw new AuthAPIError("Sumi session was not established.", 401);
    }
    return session;
  } catch (error) {
    try {
      await logoutSumiSession();
    } catch (logoutError) {
      throw new SumiSessionCompensationFailedError(error, logoutError);
    }
    throw new SumiSessionCompensatedError(error);
  }
}

export async function getSumiSession(): Promise<SumiSessionStatus> {
  const response = await fetch("/auth/session", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal: authRequestSignal(),
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
  const body = await readAuthJSON(response);
  if (!isObject(body) || typeof body.authenticated !== "boolean") {
    throw new AuthAPIError("Invalid authentication response.", response.status);
  }
  if (!body.authenticated) {
    return { authenticated: false };
  }
  if (
    typeof body.authority_binding_id !== "string" ||
    !authorityBindingIDPattern.test(body.authority_binding_id) ||
    !isObject(body.user) ||
    typeof body.user.id !== "string" ||
    body.user.id.length === 0 ||
    body.user.id.length > 256 ||
    (body.user.display_name !== undefined &&
      body.user.display_name !== null &&
      (typeof body.user.display_name !== "string" ||
        (body.user.display_name.length > 0 &&
          Array.from(body.user.display_name).length >
            maxDisplayNameCodePoints)))
  ) {
    throw new AuthAPIError("Invalid authentication response.", response.status);
  }
  return {
    authenticated: true,
    authorityBindingId: body.authority_binding_id,
    user: {
      id: body.user.id,
      displayName:
        typeof body.user.display_name === "string" &&
        body.user.display_name.trim()
          ? body.user.display_name
          : null,
    },
  };
}

export async function getSumiProfile(): Promise<ConfirmedSumiProfile> {
  const response = await fetch("/auth/profile", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal: authRequestSignal(),
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
  return parseSumiProfileResponse(await readAuthJSON(response), response.status)
    .profile;
}

export async function updateSumiProfile(
  patch: SumiProfilePatch,
): Promise<SumiProfileUpdate> {
  const request: Record<string, string> = {};
  if (patch.displayName !== undefined) {
    const displayName = canonicalizeSumiDisplayName(patch.displayName);
    if (
      !displayName ||
      Array.from(displayName).length > maxDisplayNameCodePoints ||
      !hasSafeDisplayCharacters(displayName)
    ) {
      throw new AuthAPIError("Invalid display name.", 400);
    }
    request.display_name = displayName;
  }
  if (patch.tagline !== undefined) {
    const tagline = patch.tagline.trim();
    if (
      Array.from(tagline).length > maxTaglineCodePoints ||
      !hasSafeDisplayCharacters(tagline)
    ) {
      throw new AuthAPIError("Invalid tagline.", 400);
    }
    request.tagline = tagline;
  }
  if (Object.keys(request).length === 0) {
    throw new AuthAPIError("Empty profile update.", 400);
  }
  const body = await postAuthJSON("/auth/profile", request);
  return parseSumiProfileResponse(body, 200);
}

function parseSumiProfileResponse(
  body: unknown,
  status: number,
): SumiProfileUpdate {
  if (
    !isObject(body) ||
    !isObject(body.user) ||
    typeof body.user.id !== "string" ||
    body.user.id.length === 0 ||
    body.user.id.length > 256 ||
    typeof body.user.display_name !== "string" ||
    body.user.display_name.length === 0 ||
    Array.from(body.user.display_name).length > maxDisplayNameCodePoints ||
    !hasSafeDisplayCharacters(body.user.display_name) ||
    !isObject(body.profile) ||
    !isObject(body.profile.participant) ||
    body.profile.participant.kind !== "human" ||
    typeof body.profile.participant.human_id !== "string" ||
    body.profile.participant.human_id !== body.user.id ||
    body.profile.display_name !== body.user.display_name ||
    typeof body.profile.tagline !== "string" ||
    Array.from(body.profile.tagline).length > maxTaglineCodePoints ||
    !hasSafeDisplayCharacters(body.profile.tagline)
  ) {
    throw new AuthAPIError("Invalid authentication response.", status);
  }
  return {
    id: body.user.id,
    displayName: body.user.display_name,
    profile: {
      participant: {
        kind: "human",
        humanId: body.profile.participant.human_id,
      },
      displayName: body.profile.display_name,
      tagline: body.profile.tagline,
    },
  };
}

export function canonicalizeSumiDisplayName(displayName: string): string {
  return displayName.trim().replace(/\s+/gu, " ");
}

export async function logoutSumiSession(): Promise<void> {
  const csrfToken = await fetchCSRFToken();
  const response = await fetch("/auth/logout", {
    method: "POST",
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      "X-CSRF-Token": csrfToken,
    },
    signal: authRequestSignal(),
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function authRequestSignal(): AbortSignal {
  return AbortSignal.timeout(authRequestTimeoutMilliseconds);
}

async function authAPIError(response: Response): Promise<AuthAPIError> {
  let message = "Authentication request failed.";
  try {
    const text = await readAuthResponseText(response);
    const body: unknown = JSON.parse(text);
    if (isObject(body) && typeof body.error === "string" && body.error) {
      message = body.error;
    }
  } catch {
    // The status remains the useful, non-sensitive failure signal.
  }
  return new AuthAPIError(message, response.status);
}

async function readAuthJSON(response: Response): Promise<unknown> {
  let text: string;
  try {
    text = await readAuthResponseText(response);
  } catch {
    throw new AuthAPIError("Invalid authentication response.", response.status);
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new AuthAPIError("Invalid authentication response.", response.status);
  }
}

/**
 * Authentication endpoints have deliberately small JSON contracts. Bound the
 * response before decoding it so a broken or hostile same-origin endpoint
 * cannot make an auth-state check buffer an arbitrary response in the tab.
 */
async function readAuthResponseText(response: Response): Promise<string> {
  const declaredLength = response.headers.get("content-length");
  if (
    declaredLength !== null &&
    (!/^\d+$/.test(declaredLength) ||
      Number(declaredLength) > maxAuthResponseBytes)
  ) {
    throw new Error("Authentication response exceeds the allowed size.");
  }

  const reader = response.body?.getReader();
  if (!reader) return "";

  const chunks: Uint8Array[] = [];
  let received = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      received += value.byteLength;
      if (received > maxAuthResponseBytes) {
        throw new Error("Authentication response exceeds the allowed size.");
      }
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }

  const bytes = new Uint8Array(received);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}
