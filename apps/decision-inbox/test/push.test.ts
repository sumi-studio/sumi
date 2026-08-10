import { env } from "cloudflare:workers";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DecisionRequest } from "../src/contracts";
import { sendDecisionPush } from "../src/push";

const decision: DecisionRequest = {
  id: "decision_request_for_push_test",
  title: "Choose the cutover window",
  body: "A bounded test request",
  source: "Codex",
  choices: [
    { id: "yes", label: "Proceed", tone: "positive" },
    { id: "no", label: "Stop", tone: "destructive" },
  ],
  allowFreeText: false,
  status: "pending",
  expiresAt: new Date(Date.now() + 3_600_000).toISOString(),
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
  correlationId: null,
  response: null,
};

beforeEach(async () => {
  await env.DB.prepare("DELETE FROM push_subscriptions").run();
});

describe("Web Push subscription lifecycle", () => {
  it("removes a subscription when the push service reports it gone", async () => {
    await env.DB.prepare(
      `INSERT INTO push_subscriptions (
        endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
      ) VALUES (?, ?, NULL, ?, ?, ?, ?)`,
    )
      .bind(
        "endpoint-hash",
        "https://push.example.invalid/subscription",
        "test-p256dh-key-material",
        "test-auth-key",
        Date.now(),
        Date.now(),
      )
      .run();
    const sender = vi.fn().mockRejectedValue({ statusCode: 410 });

    await sendDecisionPush(env, decision, sender);

    expect(sender).toHaveBeenCalledOnce();
    const stored = await env.DB.prepare(
      "SELECT COUNT(*) AS count FROM push_subscriptions",
    ).first<{ count: number }>();
    expect(stored?.count).toBe(0);
  });

  it("drops locally expired subscriptions without contacting push", async () => {
    await env.DB.prepare(
      `INSERT INTO push_subscriptions (
        endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
    )
      .bind(
        "expired-endpoint-hash",
        "https://push.example.invalid/expired",
        Date.now() - 1,
        "test-p256dh-key-material",
        "test-auth-key",
        Date.now(),
        Date.now(),
      )
      .run();
    const sender = vi.fn();

    await sendDecisionPush(env, decision, sender);

    expect(sender).not.toHaveBeenCalled();
    const stored = await env.DB.prepare(
      "SELECT COUNT(*) AS count FROM push_subscriptions",
    ).first<{ count: number }>();
    expect(stored?.count).toBe(0);
  });
});
