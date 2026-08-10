import webpush from "web-push";
import type { DecisionRequest } from "./contracts";

export interface PushEnv {
  DB: D1Database;
  VAPID_PUBLIC_KEY: string;
  VAPID_PRIVATE_KEY: string;
  VAPID_SUBJECT: string;
}

interface SubscriptionRow {
  endpoint_hash: string;
  endpoint: string;
  expiration_time: number | null;
  p256dh: string;
  auth: string;
}

export const MAX_ACTIVE_PUSH_SUBSCRIPTIONS = 4;
export const PUSH_SUBSCRIPTION_MAX_IDLE_MS = 45 * 86_400_000;
export const PUSH_SUBSCRIPTION_MAX_AGE_MS = 180 * 86_400_000;

export interface PushSubscriptionRecord {
  endpoint: string;
  expirationTime: number | null;
  p256dh: string;
  auth: string;
}

export type PushSender = (
  subscription: Parameters<typeof webpush.sendNotification>[0],
  payload: string,
  options: Parameters<typeof webpush.sendNotification>[2],
) => Promise<unknown>;

function pushStatusCode(error: unknown): number {
  if (error instanceof webpush.WebPushError) return error.statusCode;
  if (
    typeof error === "object" &&
    error !== null &&
    "statusCode" in error &&
    typeof error.statusCode === "number"
  ) {
    return error.statusCode;
  }
  return 0;
}

export async function storePushSubscription(
  db: D1Database,
  endpointHash: string,
  subscription: PushSubscriptionRecord,
  now = Date.now(),
): Promise<void> {
  const staleBefore = now - PUSH_SUBSCRIPTION_MAX_IDLE_MS;
  const createdBefore = now - PUSH_SUBSCRIPTION_MAX_AGE_MS;
  await db.batch([
    db
      .prepare(
        `DELETE FROM push_subscriptions
         WHERE (expiration_time IS NOT NULL AND expiration_time <= ?)
            OR last_seen_at < ?
            OR created_at < ?`,
      )
      .bind(now, staleBefore, createdBefore),
    db
      .prepare(
        `INSERT INTO push_subscriptions (
          endpoint_hash, endpoint, expiration_time, p256dh, auth, created_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (endpoint_hash) DO UPDATE SET
          endpoint = excluded.endpoint,
          expiration_time = excluded.expiration_time,
          p256dh = excluded.p256dh,
          auth = excluded.auth,
          last_seen_at = excluded.last_seen_at`,
      )
      .bind(
        endpointHash,
        subscription.endpoint,
        subscription.expirationTime,
        subscription.p256dh,
        subscription.auth,
        now,
        now,
      ),
    db
      .prepare(
        `DELETE FROM push_subscriptions
         WHERE endpoint_hash NOT IN (
           SELECT endpoint_hash
           FROM push_subscriptions
           ORDER BY
             CASE WHEN endpoint_hash = ? THEN 0 ELSE 1 END,
             last_seen_at DESC,
             created_at DESC,
             endpoint_hash DESC
           LIMIT ?
         )`,
      )
      .bind(endpointHash, MAX_ACTIVE_PUSH_SUBSCRIPTIONS),
  ]);
}

export async function prunePushSubscriptions(
  db: D1Database,
  now = Date.now(),
): Promise<void> {
  const staleBefore = now - PUSH_SUBSCRIPTION_MAX_IDLE_MS;
  const createdBefore = now - PUSH_SUBSCRIPTION_MAX_AGE_MS;
  await db
    .prepare(
      `DELETE FROM push_subscriptions
       WHERE (expiration_time IS NOT NULL AND expiration_time <= ?)
          OR last_seen_at < ?
          OR created_at < ?
          OR endpoint_hash NOT IN (
            SELECT endpoint_hash
            FROM push_subscriptions
            WHERE (expiration_time IS NULL OR expiration_time > ?)
              AND last_seen_at >= ?
              AND created_at >= ?
            ORDER BY last_seen_at DESC, created_at DESC, endpoint_hash DESC
            LIMIT ?
          )`,
    )
    .bind(
      now,
      staleBefore,
      createdBefore,
      now,
      staleBefore,
      createdBefore,
      MAX_ACTIVE_PUSH_SUBSCRIPTIONS,
    )
    .run();
}

export async function sendDecisionPush(
  env: PushEnv,
  decision: DecisionRequest,
  sender: PushSender = (subscription, payload, options) =>
    webpush.sendNotification(subscription, payload, options),
): Promise<void> {
  if (!env.VAPID_PUBLIC_KEY || !env.VAPID_PRIVATE_KEY || !env.VAPID_SUBJECT)
    return;
  const now = Date.now();
  await prunePushSubscriptions(env.DB, now);
  const subscriptions = await env.DB.prepare(
    `SELECT endpoint_hash, endpoint, expiration_time, p256dh, auth
     FROM push_subscriptions
     ORDER BY last_seen_at DESC, created_at DESC, endpoint_hash DESC
     LIMIT ?`,
  )
    .bind(MAX_ACTIVE_PUSH_SUBSCRIPTIONS)
    .all<SubscriptionRow>();
  if (subscriptions.results.length === 0) return;

  webpush.setVapidDetails(
    env.VAPID_SUBJECT,
    env.VAPID_PUBLIC_KEY,
    env.VAPID_PRIVATE_KEY,
  );
  const dead: string[] = [];
  await Promise.all(
    subscriptions.results.map(async (subscription) => {
      if (subscription.expiration_time && subscription.expiration_time <= now) {
        dead.push(subscription.endpoint_hash);
        return;
      }
      try {
        await sender(
          {
            endpoint: subscription.endpoint,
            expirationTime: subscription.expiration_time,
            keys: { auth: subscription.auth, p256dh: subscription.p256dh },
          },
          JSON.stringify({
            title: "Decision needed",
            body: `${decision.source} · ${decision.title}`,
            tag: `decision-${decision.id}`,
            data: { url: `/requests/${decision.id}` },
          }),
          {
            TTL: Math.max(
              0,
              Math.floor((Date.parse(decision.expiresAt) - now) / 1_000),
            ),
          },
        );
      } catch (error: unknown) {
        const statusCode = pushStatusCode(error);
        if (statusCode === 404 || statusCode === 410)
          dead.push(subscription.endpoint_hash);
      }
    }),
  );

  if (dead.length > 0) {
    await Promise.all(
      dead.map((endpointHash) =>
        env.DB.prepare("DELETE FROM push_subscriptions WHERE endpoint_hash = ?")
          .bind(endpointHash)
          .run(),
      ),
    );
  }
}
