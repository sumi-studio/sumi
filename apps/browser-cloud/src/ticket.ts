/** Viewer tickets are issued by the Sumi API to an authenticated person for
 * one profile (see apps/api/internal/cloudbrowser). The Worker verifies the
 * HMAC with a key derived from the shared runtime token. */

export interface TicketClaims {
  v: 1;
  /** Authenticated human (owner). */
  h: string;
  /** Secretary persona that owns the profile. */
  p: string;
  /** Browser profile id. */
  b: string;
  /** Checkpoint serializer version the viewer expects. */
  s: number;
  /** Expiry, epoch milliseconds. */
  e: number;
  n: string;
}

export const SERIALIZER_VERSION = 1;
const encoder = new TextEncoder();

function fromBase64URL(text: string): Uint8Array {
  const b64 = text.replace(/-/g, "+").replace(/_/g, "/");
  const raw = atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4));
  return Uint8Array.from(raw, (c) => c.charCodeAt(0));
}

async function hmac(key: Uint8Array, data: Uint8Array): Promise<Uint8Array> {
  const imported = await crypto.subtle.importKey("raw", key as BufferSource, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", imported, data as BufferSource));
}

export async function ticketKey(runtimeToken: string): Promise<Uint8Array> {
  return hmac(encoder.encode(runtimeToken), encoder.encode("sumi.cloud-browser.viewer-ticket.v1"));
}

function equal(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= (a[i] as number) ^ (b[i] as number);
  return diff === 0;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export async function verifyTicket(
  ticket: string,
  runtimeToken: string,
  now = Date.now(),
): Promise<TicketClaims | null> {
  if (typeof ticket !== "string" || ticket.length > 1024) return null;
  const [scheme, payload, signature] = ticket.split(".");
  if (scheme !== "sbt1" || !payload || !signature) return null;
  let expected: Uint8Array;
  let claims: TicketClaims;
  try {
    expected = await hmac(await ticketKey(runtimeToken), encoder.encode(payload));
    if (!equal(expected, fromBase64URL(signature))) return null;
    claims = JSON.parse(new TextDecoder().decode(fromBase64URL(payload)));
  } catch {
    return null;
  }
  if (
    claims.v !== 1 ||
    claims.s !== SERIALIZER_VERSION ||
    typeof claims.e !== "number" ||
    claims.e < now ||
    ![claims.h, claims.p, claims.b].every((id) => typeof id === "string" && UUID.test(id))
  )
    return null;
  return claims;
}
