import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  decodeApplicationServerKey,
  disablePushSubscription,
  enablePushSubscription,
  getDevicePushState,
  PUSH_PLATFORM_TIMEOUT_MS,
  setDevicePushOwner,
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
    setDevicePushOwner(`test-${Math.random()}`);
    setActiveMessagingScope(scope);
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    setActiveMessagingScope(null);
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("keeps a failed registration retryable rather than claiming enabled", async () => {
    installPushPlatform({ subscription: fakeSubscription() });
    vi.mocked(fetch).mockResolvedValueOnce(new Response(null, { status: 503 }));
    expect(await enablePushSubscription()).toBe(false);
    expect(getDevicePushState()).toBe("error");
    vi.mocked(fetch).mockResolvedValueOnce(new Response(null, { status: 204 }));
    expect(await enablePushSubscription(true)).toBe(true);
    expect(getDevicePushState()).toBe("enabled");
  });

  it("deletes the server registration before physical unsubscribe and keeps off across reconciliation", async () => {
    const subscription = fakeSubscription();
    installPushPlatform({ subscription });
    vi.mocked(fetch).mockResolvedValue(new Response(null, { status: 204 }));
    expect(await disablePushSubscription()).toBe(true);
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining("push-subscriptions"),
      expect.objectContaining({ method: "DELETE" }),
    );
    expect(subscription.unsubscribe).toHaveBeenCalledOnce();
    vi.mocked(fetch).mockClear();
    expect(await enablePushSubscription()).toBe(false);
    expect(fetch).not.toHaveBeenCalled();
    setDevicePushOwner("a-different-human");
    expect(await enablePushSubscription()).toBe(true);
  });

  it("waits for an in-flight server registration before deleting it", async () => {
    installPushPlatform({ subscription: fakeSubscription() });
    const response = deferred<Response>();
    vi.mocked(fetch)
      .mockReturnValueOnce(response.promise)
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const enabling = enablePushSubscription();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
    const disabling = disablePushSubscription();
    expect(fetch).toHaveBeenCalledOnce();
    response.resolve(new Response(null, { status: 204 }));
    expect(await enabling).toBe(false);
    expect(await disabling).toBe(true);
    expect(vi.mocked(fetch).mock.calls.map((call) => call[1]?.method)).toEqual([
      "POST",
      "DELETE",
    ]);
    expect(getDevicePushState()).toBe("disabled");
  });

  it("retries an interrupted disable under the replacement scope", async () => {
    const platform = installPushPlatform({ subscription: fakeSubscription() });
    const registration = deferred<ServiceWorkerRegistration>();
    platform.serviceWorker.getRegistration.mockReturnValueOnce(
      registration.promise,
    );
    const disabling = disablePushSubscription();
    await vi.waitFor(() =>
      expect(platform.serviceWorker.getRegistration).toHaveBeenCalled(),
    );
    setActiveMessagingScope({ ...scope, authorityEpoch: "2" });
    registration.resolve(platform.registration);
    expect(await disabling).toBe(false);
    expect(getDevicePushState()).toBe("idle");
    vi.mocked(fetch).mockResolvedValueOnce(new Response(null, { status: 204 }));
    expect(await enablePushSubscription()).toBe(false);
    expect(getDevicePushState()).toBe("disabled");
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining("authority_epoch=2"),
      expect.objectContaining({ method: "DELETE" }),
    );
  });

  it("uses another tab's latest on preference instead of stale memory", async () => {
    const values = new Map<string, string>();
    vi.stubGlobal("localStorage", {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
      removeItem: (key: string) => values.delete(key),
    });
    installPushPlatform({ subscription: fakeSubscription() });
    vi.mocked(fetch).mockResolvedValue(new Response(null, { status: 204 }));
    expect(await disablePushSubscription()).toBe(true);
    values.clear(); // Another tab explicitly enabled this device.
    expect(await enablePushSubscription()).toBe(true);
    expect(getDevicePushState()).toBe("enabled");
  });

  it("retains failed disable for retry without re-registering", async () => {
    const subscription = fakeSubscription();
    installPushPlatform({ subscription });
    vi.mocked(fetch).mockResolvedValueOnce(new Response(null, { status: 503 }));
    expect(await disablePushSubscription()).toBe(false);
    expect(getDevicePushState()).toBe("disable-error");
    expect(subscription.unsubscribe).not.toHaveBeenCalled();
    expect(await enablePushSubscription()).toBe(false);
    expect(getDevicePushState()).toBe("disable-error");
    vi.mocked(fetch).mockResolvedValueOnce(new Response(null, { status: 204 }));
    expect(await disablePushSubscription()).toBe(true);
  });

  it("does not publish an old account's completion after identity changes", async () => {
    installPushPlatform({ subscription: fakeSubscription() });
    const response = deferred<Response>();
    vi.mocked(fetch).mockReturnValue(response.promise);
    const enabling = enablePushSubscription();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalled());
    setDevicePushOwner("replacement-human");
    response.resolve(new Response(null, { status: 204 }));
    expect(await enabling).toBe(false);
    expect(getDevicePushState()).toBe("idle");
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
