const maxAuthResponseBytes = 4_096;
const csrfTokenPattern = /^[A-Za-z0-9_-]{43}$/;

export interface SumiSessionUser {
  id: string;
}

export type SumiSessionStatus =
  | { authenticated: false }
  | { authenticated: true; user: SumiSessionUser };

export class AuthAPIError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "AuthAPIError";
    this.status = status;
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

export async function exchangeFirebaseIDToken(idToken: string): Promise<void> {
  if (!idToken || idToken.length > 12 * 1024) {
    throw new AuthAPIError("Invalid Firebase ID token.", 0);
  }
  const csrfToken = await fetchCSRFToken();
  const response = await fetch("/auth/session", {
    method: "POST",
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrfToken,
    },
    body: JSON.stringify({ id_token: idToken }),
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
}

/**
 * The exchange response commits HttpOnly authority before the browser can
 * confirm its status. Keep both operations in one mutation and compensate
 * with a Sumi logout if any post-commit status read fails.
 */
export async function establishSumiSession(
  idToken: string,
): Promise<Extract<SumiSessionStatus, { authenticated: true }>> {
  let exchangeCommitted = false;
  try {
    await exchangeFirebaseIDToken(idToken);
    exchangeCommitted = true;
    const session = await getSumiSession();
    if (!session.authenticated) {
      throw new AuthAPIError("Sumi session was not established.", 401);
    }
    return session;
  } catch (error) {
    if (exchangeCommitted) {
      try {
        await logoutSumiSession();
      } catch (logoutError) {
        throw new SumiSessionCompensationFailedError(error, logoutError);
      }
      throw new SumiSessionCompensatedError(error);
    }
    throw error;
  }
}

export async function getSumiSession(): Promise<SumiSessionStatus> {
  const response = await fetch("/auth/session", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
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
    !isObject(body.user) ||
    typeof body.user.id !== "string" ||
    body.user.id.length === 0 ||
    body.user.id.length > 256
  ) {
    throw new AuthAPIError("Invalid authentication response.", response.status);
  }
  return { authenticated: true, user: { id: body.user.id } };
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
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
