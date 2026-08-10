import type { DecisionRequest } from "./contracts";
import { hmac } from "./crypto";

export interface CallbackEnv {
  DB: D1Database;
  CALLBACK_SIGNING_SECRET: string;
}

export interface CallbackDelivery {
  callbackUrl: string;
  deliveryId: string;
  deliveryCreatedAt: number;
  decision: DecisionRequest;
}

export type CallbackSender = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export function callbackBody(delivery: CallbackDelivery): string {
  return JSON.stringify({
    schema: "sumi.decision.callback.v1",
    delivery: {
      id: delivery.deliveryId,
      createdAt: new Date(delivery.deliveryCreatedAt).toISOString(),
    },
    type: `decision.${delivery.decision.status}`,
    request: delivery.decision,
  });
}

export async function deliverDecisionCallback(
  env: CallbackEnv,
  delivery: CallbackDelivery,
  sender: CallbackSender = fetch,
): Promise<void> {
  const body = callbackBody(delivery);
  const signature = await hmac(env.CALLBACK_SIGNING_SECRET, body);
  let status = 0;
  try {
    const response = await sender(delivery.callbackUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Sumi-Decision-Delivery-Id": delivery.deliveryId,
        "X-Sumi-Decision-Signature": `sha256=${signature}`,
      },
      body,
      redirect: "manual",
      signal: AbortSignal.timeout(10_000),
    });
    status = response.status;
  } catch {
    status = 0;
  }
  await env.DB.prepare(
    "UPDATE decision_requests SET callback_attempted_at = ?, callback_status = ? WHERE id = ? AND callback_delivery_id = ?",
  )
    .bind(Date.now(), status, delivery.decision.id, delivery.deliveryId)
    .run();
}
