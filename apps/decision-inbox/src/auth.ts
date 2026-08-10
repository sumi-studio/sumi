import { hmac, sha256, timingSafeEqual } from "./crypto";

export const SESSION_COOKIE = "__Host-sumi_decision_session";
const LOCAL_SESSION_COOKIE = "sumi_decision_session";

export interface AuthEnv {
  DB: D1Database;
  SESSION_SIGNING_SECRET: string;
  PUBLISHER_TOKEN: string;
  PUBLISHER_ID: string;
  COOKIE_SECURE?: string;
  SESSION_MAX_AGE_SECONDS?: string;
}

export interface HumanSession {
  rawToken: string;
  sessionHash: string;
  csrfToken: string;
  expiresAt: number;
}

function cookieValue(request: Request, name: string): string | null {
  const cookie = request.headers.get("Cookie");
  if (!cookie) return null;
  for (const part of cookie.split(";")) {
    const [key, ...rest] = part.trim().split("=");
    if (key === name) return rest.join("=");
  }
  return null;
}

export function sessionMaxAge(env: AuthEnv): number {
  const parsed = Number.parseInt(env.SESSION_MAX_AGE_SECONDS ?? "2592000", 10);
  return Number.isFinite(parsed)
    ? Math.min(Math.max(parsed, 3_600), 31_536_000)
    : 2_592_000;
}

export async function publisherFingerprint(env: AuthEnv): Promise<string> {
  return sha256(`publisher:${env.PUBLISHER_ID}`);
}

export async function requirePublisher(
  request: Request,
  env: AuthEnv,
): Promise<string | null> {
  const authorization = request.headers.get("Authorization") ?? "";
  if (!authorization.startsWith("Bearer ")) return null;
  const token = authorization.slice("Bearer ".length);
  if (!(await timingSafeEqual(token, env.PUBLISHER_TOKEN))) return null;
  return publisherFingerprint(env);
}

export async function signedSessionCookie(
  env: AuthEnv,
  rawToken: string,
): Promise<string> {
  const signature = await hmac(
    env.SESSION_SIGNING_SECRET,
    `session:${rawToken}`,
  );
  return `${rawToken}.${signature}`;
}

function sessionCookieName(env: AuthEnv): string {
  return env.COOKIE_SECURE === "false" ? LOCAL_SESSION_COOKIE : SESSION_COOKIE;
}

export function setSessionCookie(env: AuthEnv, signedToken: string): string {
  const secure = env.COOKIE_SECURE !== "false" ? "; Secure" : "";
  return `${sessionCookieName(env)}=${signedToken}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${sessionMaxAge(env)}${secure}`;
}

export function clearSessionCookie(env: AuthEnv): string {
  const secure = env.COOKIE_SECURE !== "false" ? "; Secure" : "";
  return `${sessionCookieName(env)}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${secure}`;
}

export async function requireHuman(
  request: Request,
  env: AuthEnv,
): Promise<HumanSession | null> {
  const encoded = cookieValue(request, sessionCookieName(env));
  if (!encoded) return null;
  const separator = encoded.lastIndexOf(".");
  if (separator <= 0) return null;
  const rawToken = encoded.slice(0, separator);
  const signature = encoded.slice(separator + 1);
  const expected = await hmac(
    env.SESSION_SIGNING_SECRET,
    `session:${rawToken}`,
  );
  if (!(await timingSafeEqual(signature, expected))) return null;

  const sessionHash = await sha256(`session:${rawToken}`);
  const now = Date.now();
  const row = await env.DB.prepare(
    "SELECT expires_at FROM human_sessions WHERE session_hash = ? AND expires_at > ?",
  )
    .bind(sessionHash, now)
    .first<{ expires_at: number }>();
  if (!row) return null;
  await env.DB.prepare(
    "UPDATE human_sessions SET last_seen_at = ? WHERE session_hash = ?",
  )
    .bind(now, sessionHash)
    .run();
  return {
    rawToken,
    sessionHash,
    csrfToken: await hmac(env.SESSION_SIGNING_SECRET, `csrf:${rawToken}`),
    expiresAt: row.expires_at,
  };
}

export async function hasValidCsrf(
  request: Request,
  session: HumanSession,
): Promise<boolean> {
  const provided = request.headers.get("X-CSRF-Token") ?? "";
  if (!(await timingSafeEqual(provided, session.csrfToken))) return false;
  const origin = request.headers.get("Origin");
  if (!origin) return false;
  return origin === new URL(request.url).origin;
}
