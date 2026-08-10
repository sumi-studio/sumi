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

export async function sendDecisionPush(
  env: PushEnv,
  decision: DecisionRequest,
  sender: PushSender = (subscription, payload, options) =>
    webpush.sendNotification(subscription, payload, options),
): Promise<void> {
  if (!env.VAPID_PUBLIC_KEY || !env.VAPID_PRIVATE_KEY || !env.VAPID_SUBJECT)
    return;
  const now = Date.now();
  const subscriptions = await env.DB.prepare(
    "SELECT endpoint_hash, endpoint, expiration_time, p256dh, auth FROM push_subscriptions",
  ).all<SubscriptionRow>();
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
