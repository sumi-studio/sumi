import {
  createExecutionContext,
  waitOnExecutionContext,
} from "cloudflare:test";
import { env } from "cloudflare:workers";
import { beforeEach, describe, expect, it } from "vitest";
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
    callback: { correlationId: "cutover-42" },
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
});
