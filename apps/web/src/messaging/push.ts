import { requireActiveMessagingBoundary, scopedMessagingPath } from "./scope";

const SW_URL = "/sw.js";
const PUSH_KEY_PATH = "/messaging/push-key";
const SUBSCRIPTIONS_PATH = "/messaging/push-subscriptions";
const MAX_PUSH_RESPONSE_BYTES = 4_096;
export const PUSH_PLATFORM_TIMEOUT_MS = 3_000;
export const PUSH_REQUEST_TIMEOUT_MS = 10_000;

let pushGeneration = 0;

export function decodeApplicationServerKey(base64url: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/.test(base64url)) {
    throw new Error("Invalid Web Push application server key");
  }
  const normalized = base64url.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized.padEnd(
    normalized.length + ((4 - (normalized.length % 4)) % 4),
    "=",
  );
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

export function isPushSupported(): boolean {
  return (
    typeof navigator !== "undefined" &&
    "serviceWorker" in navigator &&
    typeof globalThis.PushManager !== "undefined"
  );
}

export async function registerPushServiceWorker(): Promise<ServiceWorkerRegistration | null> {
  if (!isPushSupported()) return null;
  return await boundedPlatformCall(
    navigator.serviceWorker.register(SW_URL, {
      scope: "/",
      type: "module",
    }),
  );
}

export async function enablePushSubscription(): Promise<boolean> {
  if (
    !isPushSupported() ||
    typeof Notification === "undefined" ||
    Notification.permission !== "granted"
  ) {
    return false;
  }

  let boundary: ReturnType<typeof requireActiveMessagingBoundary>;
  try {
    boundary = requireActiveMessagingBoundary();
  } catch {
    return false;
  }
  const generation = ++pushGeneration;
  const registration = await registerPushServiceWorker();
  if (!currentPushAttempt(generation, boundary.signal) || !registration) {
    return false;
  }
  const ready = await boundedPlatformCall(navigator.serviceWorker.ready);
  if (!currentPushAttempt(generation, boundary.signal)) return false;
  const manager = (ready ?? registration).pushManager;
  if (!manager) return false;

  let subscription = await boundedPlatformCall(manager.getSubscription());
  if (!currentPushAttempt(generation, boundary.signal)) return false;
  if (!subscription) {
    const key = await fetchApplicationServerKey(
      scopedMessagingPath(PUSH_KEY_PATH, boundary.scope),
      boundary.signal,
    );
    if (!key || !currentPushAttempt(generation, boundary.signal)) return false;
    subscription = await boundedPlatformCall(
      manager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: key as BufferSource,
      }),
    );
  }
  if (!subscription || !currentPushAttempt(generation, boundary.signal)) {
    return false;
  }
  return await postSubscription(
    subscription,
    scopedMessagingPath(SUBSCRIPTIONS_PATH, boundary.scope),
    boundary.signal,
    generation,
  );
}

// Server logout revokes and removes the session-bound row. Browser-side
// unsubscribe is physical cleanup only, so it starts after server success and
// is never allowed to hold the public logout promise open.
export function startPushSubscriptionLogoutCleanup(): void {
  const generation = ++pushGeneration;
  if (!isPushSupported()) return;
  void (async () => {
    const registration = await boundedPlatformCall(
      navigator.serviceWorker.getRegistration(SW_URL),
    );
    if (generation !== pushGeneration || !registration?.pushManager) return;
    const subscription = await boundedPlatformCall(
      registration.pushManager.getSubscription(),
    );
    if (generation !== pushGeneration || !subscription) return;
    await boundedPlatformCall(subscription.unsubscribe());
  })().catch(() => undefined);
}

async function fetchApplicationServerKey(
  path: string,
  scopeSignal: AbortSignal,
): Promise<Uint8Array | null> {
  const request = requestSignal(scopeSignal);
  try {
    const response = await fetch(path, {
      credentials: "include",
      cache: "no-store",
      headers: { Accept: "application/json" },
      signal: request.signal,
    });
    if (!response.ok) return null;
    const body = await readBoundedJSON(response);
    if (
      !isObject(body) ||
      typeof body.public_key !== "string" ||
      body.public_key.length === 0 ||
      body.public_key.length > 200
    ) {
      return null;
    }
    try {
      return decodeApplicationServerKey(body.public_key);
    } catch {
      return null;
    }
  } catch {
    return null;
  } finally {
    request.dispose();
  }
}

async function postSubscription(
  subscription: PushSubscription,
  path: string,
  scopeSignal: AbortSignal,
  generation: number,
): Promise<boolean> {
  const serialized = subscription.toJSON();
  if (
    typeof serialized.endpoint !== "string" ||
    !isObject(serialized.keys) ||
    typeof serialized.keys.p256dh !== "string" ||
    typeof serialized.keys.auth !== "string"
  ) {
    return false;
  }
  const request = requestSignal(scopeSignal);
  try {
    const response = await fetch(path, {
      method: "POST",
      credentials: "include",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        endpoint: serialized.endpoint,
        keys: {
          p256dh: serialized.keys.p256dh,
          auth: serialized.keys.auth,
        },
      }),
      signal: request.signal,
    });
    return response.ok && currentPushAttempt(generation, scopeSignal);
  } catch {
    return false;
  } finally {
    request.dispose();
  }
}

function currentPushAttempt(generation: number, signal: AbortSignal): boolean {
  return generation === pushGeneration && !signal.aborted;
}

async function boundedPlatformCall<T>(
  operation: Promise<T>,
): Promise<T | null> {
  let timeout: ReturnType<typeof globalThis.setTimeout> | undefined;
  try {
    return await Promise.race([
      operation.catch(() => null),
      new Promise<null>((resolve) => {
        timeout = globalThis.setTimeout(
          () => resolve(null),
          PUSH_PLATFORM_TIMEOUT_MS,
        );
      }),
    ]);
  } finally {
    if (timeout !== undefined) globalThis.clearTimeout(timeout);
  }
}

function requestSignal(scopeSignal: AbortSignal): {
  signal: AbortSignal;
  dispose: () => void;
} {
  const controller = new AbortController();
  const abort = () => controller.abort();
  scopeSignal.addEventListener("abort", abort, { once: true });
  const timeout = globalThis.setTimeout(abort, PUSH_REQUEST_TIMEOUT_MS);
  return {
    signal: controller.signal,
    dispose: () => {
      globalThis.clearTimeout(timeout);
      scopeSignal.removeEventListener("abort", abort);
    },
  };
}

async function readBoundedJSON(response: Response): Promise<unknown> {
  const declared = response.headers.get("content-length");
  if (
    declared &&
    (!/^\d+$/.test(declared) || Number(declared) > MAX_PUSH_RESPONSE_BYTES)
  ) {
    throw new Error("Web Push response is too large");
  }
  const text = await response.text();
  if (new TextEncoder().encode(text).byteLength > MAX_PUSH_RESPONSE_BYTES) {
    throw new Error("Web Push response is too large");
  }
  return JSON.parse(text) as unknown;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
