import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  decodeApplicationServerKey,
  enablePushSubscription,
  PUSH_PLATFORM_TIMEOUT_MS,
  startPushSubscriptionLogoutCleanup,
} from "./push";
import { setActiveMessagingScope } from "./scope";

const scope = {
  workspaceId: "workspace-1",
  installationId: "installation-1",
  authorityEpoch: "1",
};

function fakeSubscription(endpoint = "https://push.example.test/device") {
  return {
    endpoint,
    toJSON: () => ({
      endpoint,
      keys: { p256dh: "p256dh", auth: "auth" },
    }),
    unsubscribe: vi.fn(async () => true),
  } as unknown as PushSubscription;
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

function installPushPlatform(
  options: {
    subscription?: PushSubscription | null;
    register?: Promise<ServiceWorkerRegistration>;
    subscribe?: Promise<PushSubscription>;
  } = {},
) {
  const subscription = options.subscription ?? null;
  const manager = {
    getSubscription: vi.fn(async () => subscription),
    subscribe: vi.fn(
      () => options.subscribe ?? Promise.resolve(fakeSubscription()),
    ),
  };
  const registration = {
    pushManager: manager,
  } as unknown as ServiceWorkerRegistration;
  const serviceWorker = {
    register: vi.fn(() => options.register ?? Promise.resolve(registration)),
    ready: Promise.resolve(registration),
    getRegistration: vi.fn(async () => registration),
  };
  vi.stubGlobal("navigator", { serviceWorker });
  vi.stubGlobal("PushManager", class PushManager {});
  vi.stubGlobal("Notification", { permission: "granted" });
  return { manager, registration, serviceWorker };
}

describe("generic Web Push subscription", () => {
  beforeEach(() => {
    setActiveMessagingScope(scope);
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    setActiveMessagingScope(null);
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("decodes the VAPID base64url alphabet", () => {
    expect(Array.from(decodeApplicationServerKey("-_8"))).toEqual([251, 255]);
    expect(() => decodeApplicationServerKey("not valid!")).toThrow();
  });

  it("does nothing until notification permission was explicitly granted", async () => {
    const platform = installPushPlatform();
    vi.stubGlobal("Notification", { permission: "default" });

    await expect(enablePushSubscription()).resolves.toBe(false);
    expect(platform.serviceWorker.register).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("registers one existing subscription under the exact current scope", async () => {
    const existing = fakeSubscription();
    const platform = installPushPlatform({ subscription: existing });
    vi.mocked(fetch).mockResolvedValueOnce(new Response(null, { status: 204 }));

    await expect(enablePushSubscription()).resolves.toBe(true);
    expect(platform.manager.subscribe).not.toHaveBeenCalled();
    expect(fetch).toHaveBeenCalledWith(
      "/messaging/push-subscriptions?workspace_id=workspace-1&installation_id=installation-1&authority_epoch=1",
      expect.objectContaining({ method: "POST", credentials: "include" }),
    );
    const request = vi.mocked(fetch).mock.calls[0]?.[1];
    expect(JSON.parse(String(request?.body))).toEqual({
      endpoint: "https://push.example.test/device",
      keys: { p256dh: "p256dh", auth: "auth" },
    });
  });

  it("subscribes with the deployment key without sending notification content", async () => {
    const platform = installPushPlatform();
    vi.mocked(fetch)
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ public_key: "AQID" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));

    await expect(enablePushSubscription()).resolves.toBe(true);
    expect(platform.manager.subscribe).toHaveBeenCalledWith({
      userVisibleOnly: true,
      applicationServerKey: new Uint8Array([1, 2, 3]),
    });
    const posted = String(vi.mocked(fetch).mock.calls[1]?.[1]?.body);
    expect(posted).not.toMatch(/body|content|attachment|author|title|reason/);
  });

  it("drops a late registration when the exact scope changes", async () => {
    const registration = deferred<ServiceWorkerRegistration>();
    const platform = installPushPlatform({ register: registration.promise });
    const enabled = enablePushSubscription();
    setActiveMessagingScope({ ...scope, authorityEpoch: "2" });
    registration.resolve(platform.registration);

    await expect(enabled).resolves.toBe(false);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("bounds a browser API that never settles", async () => {
    vi.useFakeTimers();
    installPushPlatform({
      register: new Promise<ServiceWorkerRegistration>(() => undefined),
    });
    const enabled = enablePushSubscription();
    await vi.advanceTimersByTimeAsync(PUSH_PLATFORM_TIMEOUT_MS + 1);
    await expect(enabled).resolves.toBe(false);
  });

  it("starts physical logout cleanup without waiting for a hung unsubscribe", () => {
    const subscription = fakeSubscription();
    vi.mocked(subscription.unsubscribe).mockReturnValue(
      new Promise<boolean>(() => undefined),
    );
    installPushPlatform({ subscription });

    expect(startPushSubscriptionLogoutCleanup()).toBeUndefined();
  });
});
