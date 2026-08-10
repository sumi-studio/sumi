import { describe, expect, it, vi } from "vitest";
import { ApiClientError } from "../src/api";
import {
  type BrowserPushManager,
  type BrowserPushSubscription,
  registerPushSubscription,
} from "../src/push-client";

function subscription(
  endpoint: string,
  unsubscribe = vi.fn().mockResolvedValue(true),
): BrowserPushSubscription {
  return {
    toJSON: () => ({
      endpoint,
      expirationTime: null,
      keys: { auth: "test-auth-key", p256dh: "test-p256dh-key-material" },
    }),
    unsubscribe,
  };
}

describe("expired browser Push subscription recovery", () => {
  it("replaces a known-expired browser object before registering", async () => {
    const staleUnsubscribe = vi.fn().mockResolvedValue(true);
    const stale = subscription(
      "https://push.example.invalid/stale",
      staleUnsubscribe,
    );
    const fresh = subscription("https://push.example.invalid/fresh");
    const pushManager: BrowserPushManager = {
      getSubscription: vi.fn().mockResolvedValue(stale),
      subscribe: vi.fn().mockResolvedValue(fresh),
    };
    const register = vi.fn().mockResolvedValue(undefined);
    const applicationServerKey = new Uint8Array([1, 2, 3]);

    const result = await registerPushSubscription({
      pushManager,
      applicationServerKey,
      register,
      replaceExisting: true,
    });

    expect(result).toBe(fresh);
    expect(staleUnsubscribe).toHaveBeenCalledOnce();
    expect(pushManager.subscribe).toHaveBeenCalledOnce();
    expect(register).toHaveBeenCalledOnce();
    expect(register).toHaveBeenCalledWith(fresh.toJSON());
    expect(register).not.toHaveBeenCalledWith(stale.toJSON());
  });

  it("renews once when the server identifies an unmarked subscription as expired", async () => {
    const staleUnsubscribe = vi.fn().mockResolvedValue(true);
    const stale = subscription(
      "https://push.example.invalid/stale",
      staleUnsubscribe,
    );
    const fresh = subscription("https://push.example.invalid/fresh");
    const pushManager: BrowserPushManager = {
      getSubscription: vi.fn().mockResolvedValue(stale),
      subscribe: vi.fn().mockResolvedValue(fresh),
    };
    const register = vi
      .fn()
      .mockRejectedValueOnce(
        new ApiClientError(
          422,
          "expired_subscription",
          "The push subscription has already expired",
        ),
      )
      .mockResolvedValueOnce(undefined);

    const result = await registerPushSubscription({
      pushManager,
      applicationServerKey: new Uint8Array([1, 2, 3]),
      register,
      replaceExisting: false,
    });

    expect(result).toBe(fresh);
    expect(register).toHaveBeenCalledTimes(2);
    expect(register.mock.calls[0]?.[0]).toEqual(stale.toJSON());
    expect(register.mock.calls[1]?.[0]).toEqual(fresh.toJSON());
    expect(staleUnsubscribe).toHaveBeenCalledOnce();
    expect(pushManager.subscribe).toHaveBeenCalledOnce();
  });
});
