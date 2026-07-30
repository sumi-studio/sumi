const maxErrorResponseCharacters = 4_096;
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

async function fetchCSRFToken(): Promise<string> {
  const response = await fetch("/auth/csrf", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
  const body: unknown = await response.json();
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

export async function getSumiSession(): Promise<SumiSessionStatus> {
  const response = await fetch("/auth/session", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    throw await authAPIError(response);
  }
  const body: unknown = await response.json();
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
    const text = (await response.text()).slice(0, maxErrorResponseCharacters);
    const body: unknown = JSON.parse(text);
    if (isObject(body) && typeof body.error === "string" && body.error) {
      message = body.error;
    }
  } catch {
    // The status remains the useful, non-sensitive failure signal.
  }
  return new AuthAPIError(message, response.status);
}
