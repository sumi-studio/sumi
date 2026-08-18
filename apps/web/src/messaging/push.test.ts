// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  decodeApplicationServerKey,
  disablePushSubscription,
  enablePushSubscription,
  isPushSupported,
} from "./push";
import { setActiveMessagingScope } from "./scope";
import {
  expectScopedMessagingPath,
  MESSAGING_SCOPE,
} from "./scope.test-support";

interface FakeSubscription {
  endpoint: string;
  toJSON: () => unknown;
  unsubscribe: () => Promise<boolean>;
}

function fakeSubscription(endpoint: string): FakeSubscription {
  return {
    endpoint,
    toJSON: () => ({
      endpoint,
      keys: { p256dh: "p256dh-value", auth: "auth-value" },
    }),
    unsubscribe: vi.fn(async () => true),
  };
}

/** 端末側の一式。subscribe が返すものと、既にある購読を差し替えられる。 */
function stubBrowser(options: {
  permission?: NotificationPermission;
  existing?: FakeSubscription | null;
  subscribed?: FakeSubscription;
  subscribeThrows?: boolean;
}) {
  const subscribe = vi.fn(async () => {
    if (options.subscribeThrows) throw new Error("denied");
    return options.subscribed ?? fakeSubscription("https://push.test/new");
  });
  const pushManager = {
    subscribe,
    getSubscription: vi.fn(async () => options.existing ?? null),
  };
  const registration = { pushManager, scope: "/" };
  const register = vi.fn(async () => registration);
  vi.stubGlobal("navigator", {
    serviceWorker: {
      register,
      ready: Promise.resolve(registration),
      getRegistration: vi.fn(async () => registration),
    },
  });
  vi.stubGlobal("PushManager", class {});
  vi.stubGlobal("Notification", {
    permission: options.permission ?? "granted",
  });
  return { register, subscribe, pushManager };
}

/**
 * Records every request as (scope を剥がした) path + method, and proves in
 * passing that the exact Workspace scope travelled with it.
 */
function stubFetch(
  handler: (path: string, init?: RequestInit) => Response | Promise<Response>,
) {
  const fetchMock = vi.fn(async (path: string | URL, init?: RequestInit) => {
    return await handler(expectScopedMessagingPath(path), init);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

beforeEach(() => {
  vi.restoreAllMocks();
  setActiveMessagingScope(MESSAGING_SCOPE);
});

afterEach(() => {
  setActiveMessagingScope(null);
  vi.unstubAllGlobals();
});

describe("decodeApplicationServerKey", () => {
  it("opens the server's base64url key into the raw bytes subscribe wants", () => {
    // "hello" を base64url にしたもの。padding が無くても読めること。
    expect(Array.from(decodeApplicationServerKey("aGVsbG8"))).toEqual([
      104, 101, 108, 108, 111,
    ]);
  });

  it("restores the base64url alphabet before decoding", () => {
    // 0xFB 0xFF は "+/" を含む標準 base64 になる語。-_ で届いても同じバイト。
    expect(Array.from(decodeApplicationServerKey("-_8"))).toEqual([251, 255]);
  });
});

describe("isPushSupported", () => {
  it("is false where the platform has no push, and that is not a failure", () => {
    vi.stubGlobal("navigator", {});
    expect(isPushSupported()).toBe(false);
  });
});

describe("enablePushSubscription", () => {
  it("does nothing until the person has actually allowed notifications", async () => {
    const { subscribe } = stubBrowser({ permission: "default" });
    const fetchMock = stubFetch(() => jsonResponse({}));
    expect(await enablePushSubscription()).toBe(false);
    expect(subscribe).not.toHaveBeenCalled();
    // 許可を求める瞬間は本人が押したときだけ。ここが勝手に聞きに行かない。
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("subscribes with the server's key and registers the endpoint", async () => {
    const created = fakeSubscription("https://push.test/created");
    const { subscribe } = stubBrowser({ subscribed: created });
    const posted: unknown[] = [];
    stubFetch((path, init) => {
      if (path === "/messaging/push-key") {
        return jsonResponse({ public_key: "aGVsbG8" });
      }
      if (path === "/messaging/push-subscriptions" && init?.method === "POST") {
        posted.push(JSON.parse(String(init.body)));
        return new Response(null, { status: 204 });
      }
      return new Response(null, { status: 404 });
    });

    expect(await enablePushSubscription()).toBe(true);
    expect(subscribe).toHaveBeenCalledWith(
      expect.objectContaining({ userVisibleOnly: true }),
    );
    expect(posted).toEqual([
      {
        endpoint: "https://push.test/created",
        keys: { p256dh: "p256dh-value", auth: "auth-value" },
      },
    ]);
  });

  it("re-sends an existing subscription instead of minting a second one", async () => {
    const existing = fakeSubscription("https://push.test/existing");
    const { subscribe } = stubBrowser({ existing });
    const posted: unknown[] = [];
    stubFetch((path, init) => {
      if (path === "/messaging/push-subscriptions" && init?.method === "POST") {
        posted.push(JSON.parse(String(init.body)));
        return new Response(null, { status: 204 });
      }
      return new Response(null, { status: 404 });
    });

    expect(await enablePushSubscription()).toBe(true);
    expect(subscribe).not.toHaveBeenCalled();
    expect(posted).toHaveLength(1);
  });

  it("gives up quietly when the deployment has no push configured", async () => {
    const { subscribe } = stubBrowser({});
    stubFetch((path) => {
      if (path === "/messaging/push-key") {
        return jsonResponse({ error: "push_unavailable" }, 503);
      }
      return new Response(null, { status: 404 });
    });
    // 503 は「この deployment に push は無い」という正直な答え。会話は壊れない。
    expect(await enablePushSubscription()).toBe(false);
    expect(subscribe).not.toHaveBeenCalled();
  });

  it("stays silent while no Workspace scope is bound", async () => {
    setActiveMessagingScope(null);
    const { subscribe } = stubBrowser({});
    const fetchMock = stubFetch(() => jsonResponse({}));
    // 端末の登録先は「今のインストール」であって、当てずっぽうの宛先ではない。
    expect(await enablePushSubscription()).toBe(false);
    expect(subscribe).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe("disablePushSubscription", () => {
  it("drops the endpoint on both sides so neither can outlive the other", async () => {
    const existing = fakeSubscription("https://push.test/existing");
    stubBrowser({ existing });
    const deleted: unknown[] = [];
    stubFetch((path, init) => {
      if (
        path === "/messaging/push-subscriptions" &&
        init?.method === "DELETE"
      ) {
        deleted.push(JSON.parse(String(init.body)));
      }
      return new Response(null, { status: 204 });
    });

    await disablePushSubscription();
    expect(existing.unsubscribe).toHaveBeenCalled();
    expect(deleted).toEqual([{ endpoint: "https://push.test/existing" }]);
  });
});
