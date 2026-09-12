import {
  AuthAPIError,
  authRequestTimeoutMilliseconds,
  fetchCSRFToken,
  postAuthJSON,
} from "./session-client";

const tokenPattern = /^[A-Za-z0-9_-]{43}$/;
const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export interface EnrollmentInvitation {
  id: string;
  issuedBy: string;
  email?: string;
  createdAt: string;
  expiresAt: string;
  revokedAt?: string;
  consumedAt?: string;
  consumedBy?: string;
}
export interface EnrollmentInvitationList {
  canInvite: true;
  invitations: EnrollmentInvitation[];
}
export function isEnrollmentInvitationUnavailable(error: unknown): boolean {
  return (
    error instanceof AuthAPIError &&
    error.status === 403 &&
    error.message === "invitation_required"
  );
}
export async function inspectEnrollmentInvitation(
  token: string,
): Promise<void> {
  if (!tokenPattern.test(token))
    throw new AuthAPIError("invitation_required", 403);
  const response = object(
    await postAuthJSON("/auth/invitations/inspect", { token }),
  );
  if (response.valid !== true || !date(response.expires_at)) invalid();
}
export async function listEnrollmentInvitations(
  signal?: AbortSignal,
): Promise<EnrollmentInvitationList> {
  const response = await fetch("/auth/invitations", {
    credentials: "include",
    cache: "no-store",
    headers: { Accept: "application/json" },
    signal: requestSignal(signal),
  });
  const value = await readResponse(response, 256 * 1024);
  const body = object(value);
  if (
    body.can_invite !== true ||
    !Array.isArray(body.invitations) ||
    body.invitations.length > 100
  )
    invalid();
  return {
    canInvite: true,
    invitations: body.invitations.map(parseInvitation),
  };
}
export async function createEnrollmentInvitation(
  input: { email?: string; expiresInSeconds?: number } = {},
): Promise<{ invitation: EnrollmentInvitation; token: string }> {
  if (
    input.email !== undefined &&
    (input.email.length > 254 || !input.email.trim())
  )
    throw new AuthAPIError("Invalid invitation email.", 400);
  if (
    input.expiresInSeconds !== undefined &&
    (!Number.isInteger(input.expiresInSeconds) ||
      input.expiresInSeconds < 60 ||
      input.expiresInSeconds > 2592000)
  )
    throw new AuthAPIError("Invalid invitation lifetime.", 400);
  const value = object(
    await mutate("/auth/invitations", {
      ...(input.email !== undefined ? { email: input.email.trim() } : {}),
      ...(input.expiresInSeconds !== undefined
        ? { expires_in_seconds: input.expiresInSeconds }
        : {}),
    }),
  );
  if (typeof value.token !== "string" || !tokenPattern.test(value.token))
    invalid();
  return { invitation: parseInvitation(value.invitation), token: value.token };
}
export async function revokeEnrollmentInvitation(id: string): Promise<void> {
  if (!uuidPattern.test(id))
    throw new AuthAPIError("Invalid invitation ID.", 400);
  await mutate(`/auth/invitations/${encodeURIComponent(id)}/revoke`, {});
}
async function mutate(
  path: string,
  body: Record<string, unknown>,
): Promise<unknown> {
  const csrf = await fetchCSRFToken();
  const response = await fetch(path, {
    method: "POST",
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-CSRF-Token": csrf,
    },
    body: JSON.stringify(body),
    signal: requestSignal(),
  });
  if (response.status === 204) return undefined;
  return readResponse(response, 4096);
}
function requestSignal(signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(authRequestTimeoutMilliseconds);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}
async function readResponse(
  response: Response,
  limit: number,
): Promise<unknown> {
  let value: unknown;
  try {
    const length = response.headers.get("content-length");
    if (length !== null && (!/^\d+$/.test(length) || Number(length) > limit))
      invalid();
    const reader = response.body?.getReader();
    if (!reader) invalid();
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      while (true) {
        const { done, value: chunk } = await reader.read();
        if (done) break;
        size += chunk.byteLength;
        if (size > limit) invalid();
        chunks.push(chunk);
      }
    } finally {
      await reader.cancel().catch(() => undefined);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    value = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new AuthAPIError("Invalid invitation response.", response.status);
  }
  if (!response.ok) {
    const error = object(value).error;
    throw new AuthAPIError(
      typeof error === "string" ? error : "Invitation request failed.",
      response.status,
    );
  }
  return value;
}
function parseInvitation(value: unknown): EnrollmentInvitation {
  const row = object(value);
  if (
    typeof row.id !== "string" ||
    !uuidPattern.test(row.id) ||
    typeof row.issued_by !== "string" ||
    !uuidPattern.test(row.issued_by) ||
    !date(row.created_at) ||
    !date(row.expires_at)
  )
    invalid();
  if (
    row.email !== undefined &&
    (typeof row.email !== "string" || row.email.length > 254)
  )
    invalid();
  for (const field of ["revoked_at", "consumed_at"])
    if (row[field] !== undefined && !date(row[field])) invalid();
  if (
    row.consumed_by !== undefined &&
    (typeof row.consumed_by !== "string" || !uuidPattern.test(row.consumed_by))
  )
    invalid();
  return {
    id: row.id,
    issuedBy: row.issued_by,
    createdAt: row.created_at,
    expiresAt: row.expires_at,
    ...(typeof row.email === "string" ? { email: row.email } : {}),
    ...(typeof row.revoked_at === "string"
      ? { revokedAt: row.revoked_at }
      : {}),
    ...(typeof row.consumed_at === "string"
      ? { consumedAt: row.consumed_at }
      : {}),
    ...(typeof row.consumed_by === "string"
      ? { consumedBy: row.consumed_by }
      : {}),
  };
}
function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    invalid();
  return value as Record<string, unknown>;
}
function date(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length <= 64 &&
    Number.isFinite(Date.parse(value))
  );
}
function invalid(): never {
  throw new AuthAPIError("Invalid invitation response.", 0);
}
