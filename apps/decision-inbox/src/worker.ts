import {
  type AuthEnv,
  clearSessionCookie,
  type HumanSession,
  hasValidCsrf,
  requireHuman,
  requirePublisher,
  sessionMaxAge,
  setSessionCookie,
  signedSessionCookie,
} from "./auth";
import { type CallbackDelivery, deliverDecisionCallback } from "./callback";
import {
  bootstrapSchema,
  callbackUrlSchema,
  createDecisionSchema,
  type DecisionRequest,
  mintBootstrapSchema,
  pushSubscriptionSchema,
  responseSchema,
} from "./contracts";
import { hmac, randomToken, sha256, timingSafeEqual } from "./crypto";
import {
  DECISION_SELECT,
  type DecisionRow,
  decisionFromRow,
  expireRequests,
  getDecision,
} from "./db";
import {
  type PushEnv,
  prunePushSubscriptions,
  sendDecisionPush,
  storePushSubscription,
} from "./push";

export interface Env extends AuthEnv, PushEnv {
  ASSETS: Fetcher;
  HUMAN_BOOTSTRAP_SECRET: string;
  CALLBACK_SIGNING_SECRET: string;
  CALLBACK_URL?: string;
}

class ApiFault extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly fields?: Record<string, string[]>,
  ) {
    super(message);
  }
}

const API_SECURITY_HEADERS = {
  "Cache-Control": "no-store",
  "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
  "Referrer-Policy": "no-referrer",
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
} as const;

const APP_SECURITY_HEADERS = {
  "Content-Security-Policy":
    "default-src 'self'; connect-src 'self'; img-src 'self' data:; manifest-src 'self'; script-src 'self'; style-src 'self'; worker-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
  "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
  "Referrer-Policy": "no-referrer",
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
} as const;

function json(data: unknown, status = 200, headers?: HeadersInit): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...API_SECURITY_HEADERS,
      "Content-Type": "application/json; charset=utf-8",
      ...headers,
    },
  });
}

function errorResponse(error: ApiFault): Response {
  return json(
    {
      error: {
        code: error.code,
        message: error.message,
        ...(error.fields ? { fields: error.fields } : {}),
      },
    },
    error.status,
  );
}

async function readJson(request: Request, limit = 16_384): Promise<unknown> {
  const declared = Number.parseInt(
    request.headers.get("Content-Length") ?? "0",
    10,
  );
  if (declared > limit)
    throw new ApiFault(413, "body_too_large", "Request body is too large");
  const text = await request.text();
  if (new TextEncoder().encode(text).byteLength > limit) {
    throw new ApiFault(413, "body_too_large", "Request body is too large");
  }
  try {
    return JSON.parse(text);
  } catch {
    throw new ApiFault(400, "invalid_json", "Request body must be valid JSON");
  }
}

function validationFault(error: {
  flatten: () => { fieldErrors: Record<string, string[] | undefined> };
}): ApiFault {
  const flattened = error.flatten().fieldErrors;
  const fields: Record<string, string[]> = {};
  for (const [key, value] of Object.entries(flattened)) {
    if (value) fields[key] = value;
  }
  return new ApiFault(
    422,
    "invalid_request",
    "Some fields need attention",
    fields,
  );
}

function requireSameOrigin(request: Request): void {
  const origin = request.headers.get("Origin");
  if (!origin || origin !== new URL(request.url).origin) {
    throw new ApiFault(
      403,
      "invalid_origin",
      "This action must come from this app",
    );
  }
}

async function requireHumanWrite(
  request: Request,
  env: Env,
): Promise<HumanSession> {
  const session = await requireHuman(request, env);
  if (!session)
    throw new ApiFault(
      401,
      "human_auth_required",
      "Open the private sign-in link again",
    );
  if (!(await hasValidCsrf(request, session))) {
    throw new ApiFault(403, "csrf_failed", "Refresh the app and try again");
  }
  return session;
}

async function enforceRateLimit(
  env: Env,
  identity: string,
  route: string,
  limit: number,
  windowSeconds: number,
): Promise<void> {
  const now = Date.now();
  const bucket = Math.floor(now / (windowSeconds * 1_000));
  const key = await sha256(
    `rate:${identity}:${route}:${env.SESSION_SIGNING_SECRET}`,
  );
  const row = await env.DB.prepare(
    `INSERT INTO rate_limits (key, bucket, count, updated_at)
     VALUES (?, ?, 1, ?)
     ON CONFLICT (key, bucket) DO UPDATE SET count = count + 1, updated_at = excluded.updated_at
     RETURNING count`,
  )
    .bind(key, bucket, now)
    .first<{ count: number }>();
  if ((row?.count ?? limit + 1) > limit) {
    throw new ApiFault(
      429,
      "rate_limited",
      "Too many requests. Wait a moment and try again",
    );
  }
}

function remoteIdentity(request: Request): string {
  return (
    request.headers.get("CF-Connecting-IP") ??
    request.headers.get("X-Forwarded-For") ??
    "local"
  );
}

function requestEnvelope(request: Request, decision: DecisionRequest) {
  return {
    request: decision,
    statusUrl: new URL(
      `/api/publisher/requests/${decision.id}`,
      request.url,
    ).toString(),
    humanUrl: new URL(`/requests/${decision.id}`, request.url).toString(),
  };
}

function configuredCallbackUrl(
  env: Env,
  callback: { url?: string } | undefined,
): string | null {
  if (!callback) return null;
  const configured = callbackUrlSchema.safeParse(env.CALLBACK_URL ?? "");
  if (!configured.success) {
    throw new ApiFault(
      503,
      "callback_unavailable",
      "Callback delivery is not configured for this deployment",
    );
  }
  if (callback.url && callback.url !== configured.data) {
    throw new ApiFault(
      422,
      "callback_url_mismatch",
      "Callback URL must exactly match this deployment's configured destination",
    );
  }
  return configured.data;
}

function callbackDeliveryFromRow(
  env: Env,
  row: DecisionRow,
  decision: DecisionRequest,
): CallbackDelivery | null {
  const configured = callbackUrlSchema.safeParse(env.CALLBACK_URL ?? "");
  if (
    !configured.success ||
    !row.callback_url ||
    row.callback_url !== configured.data ||
    !row.callback_delivery_id ||
    !row.callback_delivery_created_at
  ) {
    return null;
  }
  return {
    callbackUrl: configured.data,
    deliveryId: row.callback_delivery_id,
    deliveryCreatedAt: row.callback_delivery_created_at,
    decision,
  };
}

async function pushSessionState(env: Env) {
  await prunePushSubscriptions(env.DB);
  const subscriptions = await env.DB.prepare(
    "SELECT COUNT(*) AS count FROM push_subscriptions",
  ).first<{ count: number }>();
  return {
    vapidPublicKey: env.VAPID_PUBLIC_KEY,
    pushSubscriptionCount: subscriptions?.count ?? 0,
  };
}

async function handlePublisherCreate(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const fingerprint = await requirePublisher(request, env);
  if (!fingerprint)
    throw new ApiFault(
      401,
      "publisher_auth_required",
      "A valid publisher bearer token is required",
    );
  await enforceRateLimit(env, fingerprint, "publisher-create", 60, 60);
  const idempotencyKey = request.headers.get("Idempotency-Key")?.trim();
  if (
    !idempotencyKey ||
    idempotencyKey.length < 8 ||
    idempotencyKey.length > 128
  ) {
    throw new ApiFault(
      400,
      "idempotency_key_required",
      "Send an Idempotency-Key header between 8 and 128 characters",
    );
  }
  const parsed = createDecisionSchema.safeParse(await readJson(request));
  if (!parsed.success) throw validationFault(parsed.error);
  const input = parsed.data;
  const now = Date.now();
  const callbackUrl = configuredCallbackUrl(env, input.callback);
  const expiresAt = Date.parse(input.expiresAt);
  if (expiresAt <= now + 30_000 || expiresAt > now + 7 * 86_400_000) {
    throw new ApiFault(
      422,
      "invalid_expiry",
      "Expiry must be between 30 seconds and 7 days from now",
    );
  }

  const payloadHash = await sha256(JSON.stringify(input));
  const id = randomToken();
  const insertion = await env.DB.prepare(
    `INSERT OR IGNORE INTO decision_requests (
      id, publisher_fingerprint, idempotency_key, payload_hash, title, body, source_label,
      choices_json, allow_free_text, callback_url, correlation_id, status, expires_at,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
  )
    .bind(
      id,
      fingerprint,
      idempotencyKey,
      payloadHash,
      input.title,
      input.body,
      input.source,
      JSON.stringify(input.choices),
      input.allowFreeText ? 1 : 0,
      callbackUrl,
      input.callback?.correlationId ?? null,
      expiresAt,
      now,
      now,
    )
    .run();

  const row = await env.DB.prepare(
    `${DECISION_SELECT} WHERE r.publisher_fingerprint = ? AND r.idempotency_key = ?`,
  )
    .bind(fingerprint, idempotencyKey)
    .first<DecisionRow>();
  if (!row)
    throw new ApiFault(
      500,
      "request_missing",
      "The request could not be read after creation",
    );
  if (!(await timingSafeEqual(row.payload_hash, payloadHash))) {
    throw new ApiFault(
      409,
      "idempotency_conflict",
      "That Idempotency-Key was already used for different content",
    );
  }
  const decision = decisionFromRow(row);
  const created = (insertion.meta.changes ?? 0) > 0;
  if (created) ctx.waitUntil(sendDecisionPush(env, decision));
  return json(requestEnvelope(request, decision), created ? 201 : 200, {
    Location: new URL(
      `/api/publisher/requests/${decision.id}`,
      request.url,
    ).toString(),
  });
}

async function handlePublisherList(
  request: Request,
  env: Env,
): Promise<Response> {
  const fingerprint = await requirePublisher(request, env);
  if (!fingerprint)
    throw new ApiFault(
      401,
      "publisher_auth_required",
      "A valid publisher bearer token is required",
    );
  await enforceRateLimit(env, fingerprint, "publisher-read", 240, 60);
  await expireRequests(env.DB, Date.now());
  const mode = new URL(request.url).searchParams.get("status") ?? "pending";
  const predicate = mode === "all" ? "" : "AND r.status = 'pending'";
  const results = await env.DB.prepare(
    `${DECISION_SELECT} WHERE r.publisher_fingerprint = ? ${predicate} ORDER BY r.created_at DESC LIMIT 100`,
  )
    .bind(fingerprint)
    .all<DecisionRow>();
  return json({ requests: results.results.map(decisionFromRow) });
}

async function requireOwnedDecision(
  request: Request,
  env: Env,
  id: string,
): Promise<{ fingerprint: string; row: DecisionRow }> {
  const fingerprint = await requirePublisher(request, env);
  if (!fingerprint)
    throw new ApiFault(
      401,
      "publisher_auth_required",
      "A valid publisher bearer token is required",
    );
  await enforceRateLimit(env, fingerprint, "publisher-read", 240, 60);
  await expireRequests(env.DB, Date.now(), id);
  const row = await env.DB.prepare(
    `${DECISION_SELECT} WHERE r.id = ? AND r.publisher_fingerprint = ?`,
  )
    .bind(id, fingerprint)
    .first<DecisionRow>();
  if (!row) throw new ApiFault(404, "not_found", "Decision request not found");
  return { fingerprint, row };
}

async function handlePublisherGet(
  request: Request,
  env: Env,
  id: string,
): Promise<Response> {
  const { row } = await requireOwnedDecision(request, env, id);
  return json(requestEnvelope(request, decisionFromRow(row)));
}

async function handlePublisherCancel(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  id: string,
): Promise<Response> {
  const { fingerprint, row } = await requireOwnedDecision(request, env, id);
  await enforceRateLimit(env, fingerprint, "publisher-cancel", 60, 60);
  if (row.status === "resolved" || row.status === "expired") {
    throw new ApiFault(
      409,
      "terminal_request",
      `A ${row.status} request cannot be cancelled`,
    );
  }
  let cancelled = false;
  if (row.status === "pending") {
    const now = Date.now();
    const callbackDeliveryId = row.callback_url ? randomToken() : null;
    const cancellation = await env.DB.prepare(
      `UPDATE decision_requests
       SET status = 'cancelled', cancelled_at = ?, updated_at = ?,
           callback_delivery_id = CASE WHEN callback_url IS NULL THEN NULL ELSE ? END,
           callback_delivery_created_at = CASE WHEN callback_url IS NULL THEN NULL ELSE ? END
       WHERE id = ? AND status = 'pending'`,
    )
      .bind(now, now, callbackDeliveryId, now, id)
      .run();
    cancelled = (cancellation.meta.changes ?? 0) > 0;
  }
  const updated = await getDecision(env.DB, id);
  if (!updated)
    throw new ApiFault(404, "not_found", "Decision request not found");
  if (!cancelled && updated.status !== "cancelled") {
    throw new ApiFault(
      409,
      "terminal_request",
      `A ${updated.status} request cannot be cancelled`,
    );
  }
  const decision = decisionFromRow(updated);
  const callbackDelivery = callbackDeliveryFromRow(env, updated, decision);
  if (cancelled && callbackDelivery) {
    ctx.waitUntil(deliverDecisionCallback(env, callbackDelivery));
  }
  return json(requestEnvelope(request, decision));
}

async function handleMintBootstrap(
  request: Request,
  env: Env,
): Promise<Response> {
  const fingerprint = await requirePublisher(request, env);
  if (!fingerprint)
    throw new ApiFault(
      401,
      "publisher_auth_required",
      "A valid publisher bearer token is required",
    );
  await enforceRateLimit(env, fingerprint, "bootstrap-mint", 10, 3_600);
  const parsed = mintBootstrapSchema.safeParse(await readJson(request));
  if (!parsed.success) throw validationFault(parsed.error);
  const token = randomToken(32);
  const tokenHash = await sha256(`bootstrap:${token}`);
  const now = Date.now();
  const expiresAt = now + parsed.data.expiresInSeconds * 1_000;
  await env.DB.prepare(
    "INSERT INTO bootstrap_tokens (token_hash, created_at, expires_at, source) VALUES (?, ?, ?, 'publisher')",
  )
    .bind(tokenHash, now, expiresAt)
    .run();
  return json(
    {
      bootstrapToken: token,
      expiresAt: new Date(expiresAt).toISOString(),
      loginUrl: `${new URL("/", request.url).toString()}#bootstrap=${encodeURIComponent(token)}`,
    },
    201,
  );
}

async function handleBootstrap(request: Request, env: Env): Promise<Response> {
  requireSameOrigin(request);
  await enforceRateLimit(
    env,
    remoteIdentity(request),
    "human-bootstrap",
    10,
    900,
  );
  const parsed = bootstrapSchema.safeParse(await readJson(request));
  if (!parsed.success) throw validationFault(parsed.error);
  const now = Date.now();
  const tokenHash = await sha256(`bootstrap:${parsed.data.token}`);
  const isConfiguredToken = await timingSafeEqual(
    parsed.data.token,
    env.HUMAN_BOOTSTRAP_SECRET,
  );
  if (isConfiguredToken) {
    await env.DB.prepare(
      "INSERT OR IGNORE INTO bootstrap_tokens (token_hash, created_at, expires_at, source) VALUES (?, ?, ?, 'configured')",
    )
      .bind(tokenHash, now, now + 365 * 86_400_000)
      .run();
  }
  const consumed = await env.DB.prepare(
    "UPDATE bootstrap_tokens SET consumed_at = ? WHERE token_hash = ? AND consumed_at IS NULL AND expires_at > ? RETURNING token_hash",
  )
    .bind(now, tokenHash, now)
    .first<{ token_hash: string }>();
  if (!consumed)
    throw new ApiFault(
      401,
      "invalid_bootstrap",
      "This private sign-in link is invalid, expired, or already used",
    );

  const rawSession = randomToken(32);
  const sessionHash = await sha256(`session:${rawSession}`);
  const expiresAt = now + sessionMaxAge(env) * 1_000;
  await env.DB.prepare(
    "INSERT INTO human_sessions (session_hash, created_at, expires_at, last_seen_at) VALUES (?, ?, ?, ?)",
  )
    .bind(sessionHash, now, expiresAt, now)
    .run();
  const signed = await signedSessionCookie(env, rawSession);
  const csrfToken = await hmac(
    env.SESSION_SIGNING_SECRET,
    `csrf:${rawSession}`,
  );
  const push = await pushSessionState(env);
  return json(
    {
      authenticated: true,
      csrfToken,
      expiresAt: new Date(expiresAt).toISOString(),
      ...push,
    },
    200,
    { "Set-Cookie": setSessionCookie(env, signed) },
  );
}

async function handleSession(request: Request, env: Env): Promise<Response> {
  const session = await requireHuman(request, env);
  if (!session)
    throw new ApiFault(
      401,
      "human_auth_required",
      "Open the private sign-in link again",
    );
  const push = await pushSessionState(env);
  return json({
    authenticated: true,
    csrfToken: session.csrfToken,
    expiresAt: new Date(session.expiresAt).toISOString(),
    ...push,
  });
}

async function handleLogout(request: Request, env: Env): Promise<Response> {
  const session = await requireHumanWrite(request, env);
  await env.DB.prepare("DELETE FROM human_sessions WHERE session_hash = ?")
    .bind(session.sessionHash)
    .run();
  return json({ authenticated: false }, 200, {
    "Set-Cookie": clearSessionCookie(env),
  });
}

async function handleHumanList(request: Request, env: Env): Promise<Response> {
  const session = await requireHuman(request, env);
  if (!session)
    throw new ApiFault(
      401,
      "human_auth_required",
      "Open the private sign-in link again",
    );
  await enforceRateLimit(env, session.sessionHash, "human-read", 240, 60);
  await expireRequests(env.DB, Date.now());
  const mode = new URL(request.url).searchParams.get("view") ?? "pending";
  const predicate =
    mode === "history" ? "r.status <> 'pending'" : "r.status = 'pending'";
  const results = await env.DB.prepare(
    `${DECISION_SELECT} WHERE ${predicate} ORDER BY r.updated_at DESC LIMIT 100`,
  ).all<DecisionRow>();
  return json({ requests: results.results.map(decisionFromRow) });
}

async function handleHumanGet(
  request: Request,
  env: Env,
  id: string,
): Promise<Response> {
  const session = await requireHuman(request, env);
  if (!session)
    throw new ApiFault(
      401,
      "human_auth_required",
      "Open the private sign-in link again",
    );
  await enforceRateLimit(env, session.sessionHash, "human-read", 240, 60);
  await expireRequests(env.DB, Date.now(), id);
  const row = await getDecision(env.DB, id);
  if (!row) throw new ApiFault(404, "not_found", "Decision request not found");
  return json({ request: decisionFromRow(row) });
}

async function handleHumanRespond(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  id: string,
): Promise<Response> {
  const session = await requireHumanWrite(request, env);
  await enforceRateLimit(env, session.sessionHash, "human-respond", 30, 60);
  const parsed = responseSchema.safeParse(await readJson(request));
  if (!parsed.success) throw validationFault(parsed.error);
  const now = Date.now();
  await expireRequests(env.DB, now, id);
  const existing = await getDecision(env.DB, id);
  if (!existing)
    throw new ApiFault(404, "not_found", "Decision request not found");
  const choices = decisionFromRow(existing).choices;
  if (
    parsed.data.choiceId &&
    !choices.some((choice) => choice.id === parsed.data.choiceId)
  ) {
    throw new ApiFault(
      422,
      "invalid_choice",
      "That choice does not belong to this request",
    );
  }
  if (parsed.data.reply && existing.allow_free_text !== 1) {
    throw new ApiFault(
      422,
      "reply_not_allowed",
      "This request does not accept a written reply",
    );
  }

  const resolutionKey = await sha256(
    `${session.sessionHash}:${parsed.data.idempotencyKey}`,
  );
  if (existing.status === "resolved") {
    if (existing.resolution_key === resolutionKey)
      return json({ request: decisionFromRow(existing) });
    throw new ApiFault(
      409,
      "already_resolved",
      "This request was already resolved on another attempt",
    );
  }
  if (existing.status !== "pending") {
    throw new ApiFault(
      409,
      "terminal_request",
      `A ${existing.status} request cannot be answered`,
    );
  }

  const responseId = randomToken();
  const callbackDeliveryId = existing.callback_url ? randomToken() : null;
  const operations = await env.DB.batch([
    env.DB.prepare(
      `UPDATE decision_requests
       SET status = 'resolved', resolved_at = ?, updated_at = ?, resolution_key = ?,
           callback_delivery_id = CASE WHEN callback_url IS NULL THEN NULL ELSE ? END,
           callback_delivery_created_at = CASE WHEN callback_url IS NULL THEN NULL ELSE ? END
       WHERE id = ? AND status = 'pending' AND expires_at > ?`,
    ).bind(now, now, resolutionKey, callbackDeliveryId, now, id, now),
    env.DB.prepare(
      `INSERT OR IGNORE INTO decision_responses (
        id, request_id, choice_id, reply, idempotency_key_hash, created_at
      ) SELECT ?, ?, ?, ?, ?, ?
      WHERE EXISTS (
        SELECT 1 FROM decision_requests WHERE id = ? AND status = 'resolved' AND resolution_key = ?
      )`,
    ).bind(
      responseId,
      id,
      parsed.data.choiceId ?? null,
      parsed.data.reply ?? null,
      resolutionKey,
      now,
      id,
      resolutionKey,
    ),
  ]);
  const updated = await getDecision(env.DB, id);
  if (!updated)
    throw new ApiFault(404, "not_found", "Decision request not found");
  if (updated.resolution_key !== resolutionKey) {
    throw new ApiFault(
      409,
      "already_resolved",
      "This request was already resolved on another attempt",
    );
  }
  const decision = decisionFromRow(updated);
  const callbackDelivery = callbackDeliveryFromRow(env, updated, decision);
  if ((operations[0].meta.changes ?? 0) > 0 && callbackDelivery) {
    ctx.waitUntil(deliverDecisionCallback(env, callbackDelivery));
  }
  return json({ request: decision });
}

async function handlePushSubscribe(
  request: Request,
  env: Env,
): Promise<Response> {
  const session = await requireHumanWrite(request, env);
  await enforceRateLimit(env, session.sessionHash, "push-write", 20, 3_600);
  const parsed = pushSubscriptionSchema.safeParse(
    await readJson(request, 8_192),
  );
  if (!parsed.success) throw validationFault(parsed.error);
  const now = Date.now();
  if (parsed.data.expirationTime && parsed.data.expirationTime <= now) {
    throw new ApiFault(
      422,
      "expired_subscription",
      "The push subscription has already expired",
    );
  }
  const endpointHash = await sha256(`push:${parsed.data.endpoint}`);
  await storePushSubscription(
    env.DB,
    endpointHash,
    {
      endpoint: parsed.data.endpoint,
      expirationTime: parsed.data.expirationTime ?? null,
      p256dh: parsed.data.keys.p256dh,
      auth: parsed.data.keys.auth,
    },
    now,
  );
  return json({ subscribed: true }, 201);
}

async function handlePushUnsubscribe(
  request: Request,
  env: Env,
): Promise<Response> {
  const session = await requireHumanWrite(request, env);
  await enforceRateLimit(env, session.sessionHash, "push-write", 20, 3_600);
  const body = await readJson(request, 4_096);
  const endpoint =
    typeof body === "object" && body !== null && "endpoint" in body
      ? String(body.endpoint)
      : "";
  if (!endpoint || endpoint.length > 2_048)
    throw new ApiFault(422, "invalid_endpoint", "A push endpoint is required");
  const endpointHash = await sha256(`push:${endpoint}`);
  await env.DB.prepare("DELETE FROM push_subscriptions WHERE endpoint_hash = ?")
    .bind(endpointHash)
    .run();
  return json({ subscribed: false });
}

async function routeApi(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;

  if (request.method === "GET" && path === "/api/health") {
    return json({
      ok: true,
      pushConfigured: Boolean(env.VAPID_PUBLIC_KEY && env.VAPID_PRIVATE_KEY),
    });
  }
  if (request.method === "POST" && path === "/api/publisher/requests")
    return handlePublisherCreate(request, env, ctx);
  if (request.method === "GET" && path === "/api/publisher/requests")
    return handlePublisherList(request, env);
  if (request.method === "POST" && path === "/api/publisher/bootstrap-tokens")
    return handleMintBootstrap(request, env);
  const publisherMatch = path.match(
    /^\/api\/publisher\/requests\/([A-Za-z0-9_-]{20,64})(\/cancel)?$/u,
  );
  if (publisherMatch && request.method === "GET" && !publisherMatch[2])
    return handlePublisherGet(request, env, publisherMatch[1]);
  if (publisherMatch && request.method === "POST" && publisherMatch[2])
    return handlePublisherCancel(request, env, ctx, publisherMatch[1]);

  if (request.method === "POST" && path === "/api/auth/bootstrap")
    return handleBootstrap(request, env);
  if (request.method === "GET" && path === "/api/human/session")
    return handleSession(request, env);
  if (request.method === "POST" && path === "/api/human/logout")
    return handleLogout(request, env);
  if (request.method === "GET" && path === "/api/human/requests")
    return handleHumanList(request, env);
  const humanMatch = path.match(
    /^\/api\/human\/requests\/([A-Za-z0-9_-]{20,64})(\/respond)?$/u,
  );
  if (humanMatch && request.method === "GET" && !humanMatch[2])
    return handleHumanGet(request, env, humanMatch[1]);
  if (humanMatch && request.method === "POST" && humanMatch[2])
    return handleHumanRespond(request, env, ctx, humanMatch[1]);
  if (request.method === "POST" && path === "/api/human/push-subscriptions")
    return handlePushSubscribe(request, env);
  if (request.method === "DELETE" && path === "/api/human/push-subscriptions")
    return handlePushUnsubscribe(request, env);

  throw new ApiFault(404, "not_found", "API route not found");
}

async function fetchHandler(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname.startsWith("/api/")) {
    try {
      return await routeApi(request, env, ctx);
    } catch (error: unknown) {
      if (error instanceof ApiFault) return errorResponse(error);
      console.error(
        JSON.stringify({
          event: "api_error",
          method: request.method,
          path: url.pathname,
        }),
      );
      return errorResponse(
        new ApiFault(
          500,
          "internal_error",
          "The request could not be completed",
        ),
      );
    }
  }

  const asset = await env.ASSETS.fetch(request);
  const headers = new Headers(asset.headers);
  for (const [key, value] of Object.entries(APP_SECURITY_HEADERS))
    headers.set(key, value);
  if (url.pathname === "/" || url.pathname.startsWith("/requests/")) {
    headers.set("Cache-Control", "no-cache");
  }
  return new Response(asset.body, {
    status: asset.status,
    statusText: asset.statusText,
    headers,
  });
}

async function scheduledHandler(
  _controller: ScheduledController,
  env: Env,
  ctx: ExecutionContext,
): Promise<void> {
  const now = Date.now();
  ctx.waitUntil(
    (async () => {
      await env.DB.batch([
        env.DB.prepare(
          "UPDATE decision_requests SET status = 'expired', updated_at = ? WHERE status = 'pending' AND expires_at <= ?",
        ).bind(now, now),
        env.DB.prepare("DELETE FROM human_sessions WHERE expires_at <= ?").bind(
          now,
        ),
        env.DB.prepare(
          "DELETE FROM bootstrap_tokens WHERE expires_at <= ? OR consumed_at < ?",
        ).bind(now, now - 7 * 86_400_000),
        env.DB.prepare("DELETE FROM rate_limits WHERE updated_at < ?").bind(
          now - 2 * 86_400_000,
        ),
      ]);
      await prunePushSubscriptions(env.DB, now);
    })(),
  );
}

export default {
  fetch: fetchHandler,
  scheduled: scheduledHandler,
} satisfies ExportedHandler<Env>;
