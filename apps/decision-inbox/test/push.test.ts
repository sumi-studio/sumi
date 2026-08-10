import { env } from "cloudflare:workers";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { DecisionRequest } from "../src/contracts";
import {
  MAX_ACTIVE_PUSH_SUBSCRIPTIONS,
  PUSH_SUBSCRIPTION_MAX_AGE_MS,
  PUSH_SUBSCRIPTION_MAX_IDLE_MS,
  sendDecisionPush,
} from "../src/push";

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

  it("prunes idle and over-age subscriptions before delivery", async () => {
    const now = Date.now();
    await env.DB.batch([
      env.DB.prepare(
        `INSERT INTO push_subscriptions (
          endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
        ) VALUES (?, ?, NULL, ?, ?, ?, ?)`,
      ).bind(
        "idle-endpoint-hash",
        "https://push.example.invalid/idle",
        "test-p256dh-key-material-idle",
        "test-auth-key-idle",
        now - PUSH_SUBSCRIPTION_MAX_IDLE_MS - 1,
        now - PUSH_SUBSCRIPTION_MAX_IDLE_MS - 1,
      ),
      env.DB.prepare(
        `INSERT INTO push_subscriptions (
          endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
        ) VALUES (?, ?, NULL, ?, ?, ?, ?)`,
      ).bind(
        "active-endpoint-hash",
        "https://push.example.invalid/active",
        "test-p256dh-key-material-active",
        "test-auth-key-active",
        now,
        now,
      ),
      env.DB.prepare(
        `INSERT INTO push_subscriptions (
          endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
        ) VALUES (?, ?, NULL, ?, ?, ?, ?)`,
      ).bind(
        "aged-endpoint-hash",
        "https://push.example.invalid/aged",
        "test-p256dh-key-material-aged",
        "test-auth-key-aged",
        now - PUSH_SUBSCRIPTION_MAX_AGE_MS - 1,
        now,
      ),
    ]);
    const sender = vi.fn().mockResolvedValue(undefined);

    await sendDecisionPush(env, decision, sender);

    expect(sender).toHaveBeenCalledOnce();
    expect(sender.mock.calls[0]?.[0]).toMatchObject({
      endpoint: "https://push.example.invalid/active",
    });
    const rows = await env.DB.prepare(
      "SELECT endpoint_hash FROM push_subscriptions",
    ).all<{ endpoint_hash: string }>();
    expect(rows.results).toEqual([{ endpoint_hash: "active-endpoint-hash" }]);
  });

  it("bounds both retained subscriptions and per-decision fan-out", async () => {
    const now = Date.now();
    await env.DB.batch(
      Array.from({ length: MAX_ACTIVE_PUSH_SUBSCRIPTIONS + 3 }, (_, index) =>
        env.DB.prepare(
          `INSERT INTO push_subscriptions (
              endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
            ) VALUES (?, ?, NULL, ?, ?, ?, ?)`,
        ).bind(
          `endpoint-hash-${index}`,
          `https://push.example.invalid/${index}`,
          `test-p256dh-key-material-${index}`,
          `test-auth-key-${index}`,
          now + index,
          now + index,
        ),
      ),
    );
    const sender = vi.fn().mockResolvedValue(undefined);

    await sendDecisionPush(env, decision, sender);

    expect(sender).toHaveBeenCalledTimes(MAX_ACTIVE_PUSH_SUBSCRIPTIONS);
    const stored = await env.DB.prepare(
      "SELECT COUNT(*) AS count FROM push_subscriptions",
    ).first<{ count: number }>();
    expect(stored?.count).toBe(MAX_ACTIVE_PUSH_SUBSCRIPTIONS);
  });
});
