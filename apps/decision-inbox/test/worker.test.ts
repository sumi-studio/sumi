import {
  createExecutionContext,
  waitOnExecutionContext,
} from "cloudflare:test";
import { env } from "cloudflare:workers";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { deliverDecisionCallback } from "../src/callback";
import type { DecisionRequest } from "../src/contracts";
import { hmac, sha256 } from "../src/crypto";
import type { DecisionRow } from "../src/db";
import { MAX_ACTIVE_PUSH_SUBSCRIPTIONS } from "../src/push";
import worker from "../src/worker";

const origin = "https://decision.test";
const publisherHeaders = {
  Authorization: "Bearer test-publisher-token-which-is-long",
  "Content-Type": "application/json",
};
const defaultExpiresAt = new Date(Date.now() + 3_600_000).toISOString();

async function call(path: string, init?: RequestInit): Promise<Response> {
  const context = createExecutionContext();
  const response = await worker.fetch(
    new Request(`${origin}${path}`, init),
    env,
    context,
  );
  await waitOnExecutionContext(context);
  return response;
}

function decisionInput(overrides: Record<string, unknown> = {}) {
  return {
    title: "Choose the cutover window",
    body: "Production checks passed. Which window should the agent use?",
    source: "Codex · Workspace cutover",
    choices: [
      { id: "now", label: "Proceed now", tone: "positive" },
      { id: "later", label: "Wait until morning", tone: "neutral" },
      { id: "stop", label: "Cancel the cutover", tone: "destructive" },
    ],
    allowFreeText: true,
    expiresAt: defaultExpiresAt,
    ...overrides,
  };
}

async function createDecision(
  key = "request-key-0001",
  overrides: Record<string, unknown> = {},
) {
  return call("/api/publisher/requests", {
    method: "POST",
    headers: { ...publisherHeaders, "Idempotency-Key": key },
    body: JSON.stringify(decisionInput(overrides)),
  });
}

async function signIn(token = "test-human-bootstrap-token-which-is-long") {
  const response = await call("/api/auth/bootstrap", {
    method: "POST",
    headers: { "Content-Type": "application/json", Origin: origin },
    body: JSON.stringify({ token }),
  });
  const body = (await response.json()) as { csrfToken: string };
  const cookie = response.headers.get("Set-Cookie")?.split(";", 1)[0] ?? "";
  return { response, body, cookie };
}

beforeEach(async () => {
  await env.DB.batch([
    env.DB.prepare("DELETE FROM decision_responses"),
    env.DB.prepare("DELETE FROM decision_requests"),
    env.DB.prepare("DELETE FROM human_sessions"),
    env.DB.prepare("DELETE FROM bootstrap_tokens"),
    env.DB.prepare("DELETE FROM push_subscriptions"),
    env.DB.prepare("DELETE FROM rate_limits"),
  ]);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("publisher contract", () => {
  it("creates idempotently and rejects key reuse for different content", async () => {
    const created = await createDecision();
    expect(created.status).toBe(201);
    const first = (await created.json()) as {
      request: { id: string };
      statusUrl: string;
      humanUrl: string;
    };
    expect(first.request.id).toMatch(/^[A-Za-z0-9_-]{20,64}$/u);
    expect(first.statusUrl).toBe(
      `${origin}/api/publisher/requests/${first.request.id}`,
    );
    expect(first.humanUrl).toBe(`${origin}/requests/${first.request.id}`);

    const replay = await createDecision();
    expect(replay.status).toBe(200);
    expect(
      ((await replay.json()) as { request: { id: string } }).request.id,
    ).toBe(first.request.id);

    const conflict = await createDecision("request-key-0001", {
      title: "Different decision",
    });
    expect(conflict.status).toBe(409);
    expect((await conflict.json()) as object).toMatchObject({
      error: { code: "idempotency_conflict" },
    });
  });

  it("lists only its unresolved requests and cancellation is idempotent", async () => {
    const created = (await (
      await createDecision("request-key-cancel")
    ).json()) as { request: { id: string } };
    const list = await call("/api/publisher/requests", {
      headers: publisherHeaders,
    });
    expect(list.status).toBe(200);
    expect(
      ((await list.json()) as { requests: unknown[] }).requests,
    ).toHaveLength(1);

    const cancel = await call(
      `/api/publisher/requests/${created.request.id}/cancel`,
      {
        method: "POST",
        headers: publisherHeaders,
      },
    );
    expect(cancel.status).toBe(200);
    expect((await cancel.json()) as object).toMatchObject({
      request: { status: "cancelled" },
    });

    const replay = await call(
      `/api/publisher/requests/${created.request.id}/cancel`,
      {
        method: "POST",
        headers: publisherHeaders,
      },
    );
    expect(replay.status).toBe(200);
    const pending = await call("/api/publisher/requests", {
      headers: publisherHeaders,
    });
    expect(
      ((await pending.json()) as { requests: unknown[] }).requests,
    ).toHaveLength(0);
  });

  it("mints an additional one-time Human bootstrap token", async () => {
    const minted = await call("/api/publisher/bootstrap-tokens", {
      method: "POST",
      headers: publisherHeaders,
      body: JSON.stringify({ expiresInSeconds: 600 }),
    });
    expect(minted.status).toBe(201);
    const payload = (await minted.json()) as {
      bootstrapToken: string;
      loginUrl: string;
    };
    expect(payload.loginUrl).toContain("#bootstrap=");
    const login = await signIn(payload.bootstrapToken);
    expect(login.response.status).toBe(200);
    const replay = await signIn(payload.bootstrapToken);
    expect(replay.response.status).toBe(401);
  });

  it("uses only the deployment callback URL and rejects a mismatch", async () => {
    const mismatch = await createDecision("request-key-callback-mismatch", {
      callback: {
        url: "https://other.example.test/hooks/decision-inbox",
        correlationId: "cutover-42",
      },
    });
    expect(mismatch.status).toBe(422);
    expect((await mismatch.json()) as object).toMatchObject({
      error: { code: "callback_url_mismatch" },
    });

    const accepted = await createDecision("request-key-callback-configured", {
      callback: { correlationId: "cutover-42" },
    });
    expect(accepted.status).toBe(201);
    const requestId = ((await accepted.json()) as { request: { id: string } })
      .request.id;
    const stored = await env.DB.prepare(
      "SELECT callback_url FROM decision_requests WHERE id = ?",
    )
      .bind(requestId)
      .first<{ callback_url: string }>();
    expect(stored?.callback_url).toBe(
      "https://publisher.example.test/hooks/decision-inbox",
    );
  });
});

describe("Human decision flow", () => {
  it("exchanges a bootstrap token once and keeps session material HttpOnly", async () => {
    const first = await signIn();
    expect(first.response.status).toBe(200);
    expect(first.response.headers.get("Set-Cookie")).toContain("HttpOnly");
    expect(first.response.headers.get("Set-Cookie")).toContain(
      "SameSite=Strict",
    );
    expect(first.response.headers.get("Set-Cookie")).toContain("Secure");
    expect(first.body.csrfToken).toBeTruthy();

    const replay = await signIn();
    expect(replay.response.status).toBe(401);
  });

  it("resolves in one durable transition and makes same-key replay idempotent", async () => {
    const created = (await (
      await createDecision("request-key-answer")
    ).json()) as { request: { id: string } };
    const { cookie, body } = await signIn();
    const headers = {
      "Content-Type": "application/json",
      Cookie: cookie,
      Origin: origin,
      "X-CSRF-Token": body.csrfToken,
    };
    const responseBody = {
      choiceId: "now",
      reply: "Use the checked release head.",
      idempotencyKey: "response-key-0001",
    };
    const resolved = await call(
      `/api/human/requests/${created.request.id}/respond`,
      {
        method: "POST",
        headers,
        body: JSON.stringify(responseBody),
      },
    );
    expect(resolved.status).toBe(200);
    const resolution = (await resolved.json()) as {
      request: { status: string; response: { id: string } };
    };
    expect(resolution.request.status).toBe("resolved");

    const replay = await call(
      `/api/human/requests/${created.request.id}/respond`,
      {
        method: "POST",
        headers,
        body: JSON.stringify(responseBody),
      },
    );
    expect(replay.status).toBe(200);
    expect(
      ((await replay.json()) as typeof resolution).request.response.id,
    ).toBe(resolution.request.response.id);

    const secondDecision = await call(
      `/api/human/requests/${created.request.id}/respond`,
      {
        method: "POST",
        headers,
        body: JSON.stringify({
          choiceId: "later",
          idempotencyKey: "response-key-0002",
        }),
      },
    );
    expect(secondDecision.status).toBe(409);
    const stored = await env.DB.prepare(
      "SELECT COUNT(*) AS count FROM decision_responses WHERE request_id = ?",
    )
      .bind(created.request.id)
      .first<{ count: number }>();
    expect(stored?.count).toBe(1);
  });

  it("orders the pending queue by soonest expiry first", async () => {
    const later = (await (
      await createDecision("request-key-order-later", {
        expiresAt: new Date(Date.now() + 7_200_000).toISOString(),
      })
    ).json()) as { request: { id: string } };
    const sooner = (await (
      await createDecision("request-key-order-sooner", {
        expiresAt: new Date(Date.now() + 1_800_000).toISOString(),
      })
    ).json()) as { request: { id: string } };
    const { cookie } = await signIn();
    const list = await call("/api/human/requests?view=pending", {
      headers: { Cookie: cookie },
    });
    expect(list.status).toBe(200);
    const body = (await list.json()) as { requests: { id: string }[] };
    expect(body.requests.map((entry) => entry.id)).toEqual([
      sooner.request.id,
      later.request.id,
    ]);
  });

  it("orders equal-expiry pending rows deterministically in D1", async () => {
    const first = (await (
      await createDecision("request-key-order-tie-first")
    ).json()) as { request: { id: string } };
    const second = (await (
      await createDecision("request-key-order-tie-second")
    ).json()) as { request: { id: string } };
    const expiresAt = Date.now() + 3_600_000;
    await env.DB.batch([
      env.DB.prepare(
        "UPDATE decision_requests SET expires_at = ?, created_at = ? WHERE id = ?",
      ).bind(expiresAt, 10, first.request.id),
      env.DB.prepare(
        "UPDATE decision_requests SET expires_at = ?, created_at = ? WHERE id = ?",
      ).bind(expiresAt, 10, second.request.id),
    ]);
    const { cookie } = await signIn();
    const list = await call("/api/human/requests?view=pending", {
      headers: { Cookie: cookie },
    });
    expect(list.status).toBe(200);
    const body = (await list.json()) as { requests: { id: string }[] };
    expect(body.requests.slice(0, 2).map((entry) => entry.id)).toEqual(
      [first.request.id, second.request.id].sort(),
    );
  });

  it("does not accept Human writes without same-origin CSRF proof", async () => {
    const created = (await (
      await createDecision("request-key-csrf")
    ).json()) as { request: { id: string } };
    const { cookie } = await signIn();
    const response = await call(
      `/api/human/requests/${created.request.id}/respond`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", Cookie: cookie },
        body: JSON.stringify({
          choiceId: "now",
          idempotencyKey: "response-key-csrf",
        }),
      },
    );
    expect(response.status).toBe(403);
    expect((await response.json()) as object).toMatchObject({
      error: { code: "csrf_failed" },
    });
  });

  it("expires stale requests on read and refuses a late response", async () => {
    const created = (await (
      await createDecision("request-key-expiry")
    ).json()) as { request: { id: string } };
    await env.DB.prepare(
      "UPDATE decision_requests SET expires_at = ? WHERE id = ?",
    )
      .bind(Date.now() - 1, created.request.id)
      .run();
    const { cookie, body } = await signIn();
    const detail = await call(`/api/human/requests/${created.request.id}`, {
      headers: { Cookie: cookie },
    });
    expect((await detail.json()) as object).toMatchObject({
      request: { status: "expired" },
    });

    const response = await call(
      `/api/human/requests/${created.request.id}/respond`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Cookie: cookie,
          Origin: origin,
          "X-CSRF-Token": body.csrfToken,
        },
        body: JSON.stringify({
          choiceId: "now",
          idempotencyKey: "response-key-late",
        }),
      },
    );
    expect(response.status).toBe(409);
  });

  it("persists one stable signed callback delivery across a replay", async () => {
    const sent = vi.fn().mockResolvedValue(new Response(null, { status: 202 }));
    vi.stubGlobal("fetch", sent);
    const created = (await (
      await createDecision("request-key-callback-replay", {
        callback: { correlationId: "cutover-42" },
      })
    ).json()) as { request: { id: string } };
    const { cookie, body } = await signIn();
    const headers = {
      "Content-Type": "application/json",
      Cookie: cookie,
      Origin: origin,
      "X-CSRF-Token": body.csrfToken,
    };
    const answer = {
      choiceId: "now",
      reply: "Use the checked release head.",
      idempotencyKey: "response-key-callback",
    };
    const resolved = await call(
      `/api/human/requests/${created.request.id}/respond`,
      { method: "POST", headers, body: JSON.stringify(answer) },
    );
    expect(resolved.status).toBe(200);
    const decision = ((await resolved.json()) as { request: DecisionRequest })
      .request;
    const storedBeforeReplay = await env.DB.prepare(
      "SELECT * FROM decision_requests WHERE id = ?",
    )
      .bind(created.request.id)
      .first<DecisionRow>();
    expect(storedBeforeReplay?.callback_delivery_id).toBeTruthy();
    expect(storedBeforeReplay?.callback_delivery_created_at).toBeTruthy();
    expect(sent).toHaveBeenCalledOnce();

    const firstInit = sent.mock.calls[0]?.[1] as RequestInit;
    const firstBody = String(firstInit.body);
    const firstHeaders = new Headers(firstInit.headers);
    const parsedBody = JSON.parse(firstBody) as {
      schema: string;
      delivery: { id: string; createdAt: string };
    };
    expect(parsedBody).toMatchObject({
      schema: "sumi.decision.callback.v1",
      delivery: {
        id: storedBeforeReplay?.callback_delivery_id,
        createdAt: new Date(
          storedBeforeReplay?.callback_delivery_created_at ?? 0,
        ).toISOString(),
      },
    });
    expect(firstHeaders.get("X-Sumi-Decision-Delivery-Id")).toBe(
      storedBeforeReplay?.callback_delivery_id,
    );
    expect(firstHeaders.get("X-Sumi-Decision-Signature")).toBe(
      `sha256=${await hmac(env.CALLBACK_SIGNING_SECRET, firstBody)}`,
    );

    await deliverDecisionCallback(
      env,
      {
        callbackUrl: storedBeforeReplay?.callback_url ?? "",
        deliveryId: storedBeforeReplay?.callback_delivery_id ?? "",
        deliveryCreatedAt:
          storedBeforeReplay?.callback_delivery_created_at ?? 0,
        decision,
      },
      sent,
    );
    expect(sent).toHaveBeenCalledTimes(2);
    const replayInit = sent.mock.calls[1]?.[1] as RequestInit;
    expect(replayInit.body).toBe(firstBody);
    expect(
      new Headers(replayInit.headers).get("X-Sumi-Decision-Signature"),
    ).toBe(firstHeaders.get("X-Sumi-Decision-Signature"));

    const apiReplay = await call(
      `/api/human/requests/${created.request.id}/respond`,
      { method: "POST", headers, body: JSON.stringify(answer) },
    );
    expect(apiReplay.status).toBe(200);
    expect(sent).toHaveBeenCalledTimes(2);
    const storedAfterReplay = await env.DB.prepare(
      "SELECT callback_delivery_id, callback_delivery_created_at FROM decision_requests WHERE id = ?",
    )
      .bind(created.request.id)
      .first<{
        callback_delivery_id: string;
        callback_delivery_created_at: number;
      }>();
    expect(storedAfterReplay).toEqual({
      callback_delivery_id: storedBeforeReplay?.callback_delivery_id,
      callback_delivery_created_at:
        storedBeforeReplay?.callback_delivery_created_at,
    });
  });
});

describe("Web Push subscription API", () => {
  function subscription(index: number) {
    return {
      endpoint: `https://push.example.invalid/subscription-${index}`,
      expirationTime: null,
      keys: {
        p256dh: `test-p256dh-key-material-${index}`,
        auth: `test-auth-key-${index}`,
      },
    };
  }

  it("refreshes last-seen and evicts the least-recent endpoint at the cap", async () => {
    const { cookie, body } = await signIn();
    const now = Date.now();
    for (let index = 0; index < MAX_ACTIVE_PUSH_SUBSCRIPTIONS; index += 1) {
      const value = subscription(index);
      await env.DB.prepare(
        `INSERT INTO push_subscriptions (
          endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
        ) VALUES (?, ?, NULL, ?, ?, ?, ?)`,
      )
        .bind(
          await sha256(`push:${value.endpoint}`),
          value.endpoint,
          value.keys.p256dh,
          value.keys.auth,
          now - 20_000 + index,
          now - 20_000 + index,
        )
        .run();
    }

    const newest = subscription(99);
    const response = await call("/api/human/push-subscriptions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: cookie,
        Origin: origin,
        "X-CSRF-Token": body.csrfToken,
      },
      body: JSON.stringify(newest),
    });
    expect(response.status).toBe(201);
    const rows = await env.DB.prepare(
      "SELECT endpoint_hash, last_seen_at FROM push_subscriptions ORDER BY last_seen_at DESC",
    ).all<{ endpoint_hash: string; last_seen_at: number }>();
    expect(rows.results).toHaveLength(MAX_ACTIVE_PUSH_SUBSCRIPTIONS);
    expect(rows.results.map((row) => row.endpoint_hash)).toContain(
      await sha256(`push:${newest.endpoint}`),
    );
    expect(rows.results.map((row) => row.endpoint_hash)).not.toContain(
      await sha256(`push:${subscription(0).endpoint}`),
    );

    const newestHash = await sha256(`push:${newest.endpoint}`);
    const beforeRefresh = Date.now() - 10_000;
    await env.DB.prepare(
      "UPDATE push_subscriptions SET last_seen_at = ? WHERE endpoint_hash = ?",
    )
      .bind(beforeRefresh, newestHash)
      .run();
    const refresh = await call("/api/human/push-subscriptions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: cookie,
        Origin: origin,
        "X-CSRF-Token": body.csrfToken,
      },
      body: JSON.stringify(newest),
    });
    expect(refresh.status).toBe(201);
    const refreshed = await env.DB.prepare(
      "SELECT last_seen_at FROM push_subscriptions WHERE endpoint_hash = ?",
    )
      .bind(newestHash)
      .first<{ last_seen_at: number }>();
    expect(refreshed?.last_seen_at).toBeGreaterThan(beforeRefresh);
  });

  it("keeps the active cap under concurrent subscribe requests", async () => {
    const { cookie, body } = await signIn();
    const responses = await Promise.all(
      Array.from({ length: 10 }, (_, index) =>
        call("/api/human/push-subscriptions", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Cookie: cookie,
            Origin: origin,
            "X-CSRF-Token": body.csrfToken,
          },
          body: JSON.stringify(subscription(index)),
        }),
      ),
    );
    expect(responses.every((response) => response.status === 201)).toBe(true);
    const stored = await env.DB.prepare(
      "SELECT COUNT(*) AS count FROM push_subscriptions",
    ).first<{ count: number }>();
    expect(stored?.count).toBe(MAX_ACTIVE_PUSH_SUBSCRIPTIONS);
  });
});
