import { requireActiveMessagingBoundary, scopedMessagingPath } from "./scope";

const SW_URL = "/sw.js";
const PUSH_KEY_PATH = "/messaging/push-key";
const SUBSCRIPTIONS_PATH = "/messaging/push-subscriptions";
const MAX_PUSH_RESPONSE_BYTES = 4_096;
export const PUSH_PLATFORM_TIMEOUT_MS = 3_000;
export const PUSH_REQUEST_TIMEOUT_MS = 10_000;

let pushGeneration = 0;
let pushOwner = "";
export type DevicePushState =
  | "idle"
  | "enabling"
  | "enabled"
  | "disabled"
  | "error"
  | "disabling"
  | "disable-error";
let deviceState: DevicePushState = "idle";
const listeners = new Set<() => void>();
let pendingEnable: Promise<boolean> | null = null;
let pendingEnableGeneration = -1;
let pendingEnableSignal: AbortSignal | null = null;
const disabledOwners = new Set<string>();
const preferenceKey = (owner: string) => `sumi:device-push:off:${owner}`;
function isDisabled(): boolean {
  try {
    return localStorage.getItem(preferenceKey(pushOwner)) === "true";
  } catch {
    return disabledOwners.has(pushOwner);
  }
}
function setDisabled(disabled: boolean): void {
  if (disabled) disabledOwners.add(pushOwner);
  else disabledOwners.delete(pushOwner);
  try {
    if (disabled) localStorage.setItem(preferenceKey(pushOwner), "true");
    else localStorage.removeItem(preferenceKey(pushOwner));
  } catch {
    /* The current page still remembers the preference. */
  }
}
function publishState(state: DevicePushState): void {
  deviceState = state;
  for (const listener of listeners) listener();
}
export const getDevicePushState = () => deviceState;
export function subscribeDevicePush(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
export function setDevicePushOwner(owner: string): void {
  if (pushOwner === owner) return;
  ++pushGeneration;
  pushOwner = owner;
  publishState("idle");
}

export function refreshDevicePushPreference(): void {
  ++pushGeneration;
  publishState("idle");
}

/** Reconciliation never overrides an explicit choice to disable this device. */
export async function enablePushSubscription(
  explicit = false,
): Promise<boolean> {
  if (explicit) setDisabled(false);
  if (isDisabled()) {
    if (
      deviceState !== "disabled" &&
      deviceState !== "disable-error" &&
      deviceState !== "disabling"
    )
      await disablePushSubscription();
    return false;
  }
  let signal: AbortSignal;
  try {
    signal = requireActiveMessagingBoundary().signal;
  } catch {
    return false;
  }
  if (
    pendingEnable &&
    pendingEnableGeneration === pushGeneration &&
    pendingEnableSignal === signal &&
    !signal.aborted
  )
    return pendingEnable;
  const owner = pushOwner;
  const operation = performEnablePushSubscription().catch(() => false);
  pendingEnable = operation;
  const generation = pushGeneration;
  pendingEnableGeneration = generation;
  pendingEnableSignal = signal;
  publishState("enabling");
  try {
    const enabled = await operation;
    if (generation === pushGeneration && owner === pushOwner)
      publishState(enabled ? "enabled" : "error");
    return enabled;
  } catch {
    if (generation === pushGeneration && owner === pushOwner)
      publishState("error");
    return false;
  } finally {
    if (pendingEnable === operation) pendingEnable = null;
  }
}

export async function disablePushSubscription(): Promise<boolean> {
  setDisabled(true);
  const generation = ++pushGeneration;
  const owner = pushOwner;
  publishState("disabling");
  let boundary: ReturnType<typeof requireActiveMessagingBoundary> | null = null;
  try {
    boundary = requireActiveMessagingBoundary();
    // Let a previously posted registration settle before deleting the endpoint.
    await pendingEnable;
    if (
      generation !== pushGeneration ||
      owner !== pushOwner ||
      boundary.signal.aborted
    )
      return false;
    const registrationResult = await boundedPlatformCall(
      navigator.serviceWorker
        .getRegistration(SW_URL)
        .then((value) => ({ value })),
    );
    if (!registrationResult)
      throw new Error("Cannot inspect browser registration");
    const registration = registrationResult.value;
    const subscriptionResult = registration
      ? await boundedPlatformCall(
          registration.pushManager
            .getSubscription()
            .then((value) => ({ value })),
        )
      : { value: null };
    if (!subscriptionResult)
      throw new Error("Cannot inspect browser subscription");
    const subscription = subscriptionResult.value;
    if (
      generation !== pushGeneration ||
      owner !== pushOwner ||
      boundary.signal.aborted
    )
      return false;
    if (!subscription) {
      publishState("disabled");
      return true;
    }
    const request = requestSignal(boundary.signal);
    let removed = false;
    try {
      const response = await fetch(
        scopedMessagingPath(SUBSCRIPTIONS_PATH, boundary.scope),
        {
          method: "DELETE",
          credentials: "include",
          cache: "no-store",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ endpoint: subscription.endpoint }),
          signal: request.signal,
        },
      );
      removed = response.ok;
    } finally {
      request.dispose();
    }
    if (
      generation !== pushGeneration ||
      owner !== pushOwner ||
      boundary.signal.aborted
    )
      return false;
    // Server removal is authoritative. Physical browser cleanup is best effort.
    if (removed) await boundedPlatformCall(subscription.unsubscribe());
    if (generation === pushGeneration && owner === pushOwner)
      publishState(removed ? "disabled" : "disable-error");
    return removed;
  } catch {
    if (generation === pushGeneration && owner === pushOwner)
      publishState("disable-error");
    return false;
  } finally {
    // A workspace boundary can retire while browser operations are pending.
    // Keep the user's off preference but allow the replacement scope to retry.
    if (
      boundary?.signal.aborted &&
      generation === pushGeneration &&
      owner === pushOwner
    )
      publishState("idle");
  }
}

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

async function performEnablePushSubscription(): Promise<boolean> {
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
