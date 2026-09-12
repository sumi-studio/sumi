import { useEffect } from "react";
import {
  enablePushSubscription,
  getDevicePushState,
  isPushSupported,
  refreshDevicePushPreference,
  setDevicePushOwner,
  subscribeDevicePush,
} from "../push";
import { useMessaging } from "../store";

// Shell-lifetime reconciliation for an already granted browser permission.
// Permission itself remains owned by the explicit Messaging banner action.
export function PushSubscriptionBridge() {
  const owner = useMessaging((state) => state.selfKey);
  const enabled = useMessaging((state) => state.capabilities.notifications);
  const ready = useMessaging((state) => state.ready);
  const transportGeneration = useMessaging(
    (state) => state.transportGeneration,
  );

  useEffect(() => {
    // A new transport generation must re-post the browser's durable
    // subscription under the replacement exact Messaging authority.
    void transportGeneration;
    setDevicePushOwner(owner);
    if (
      !enabled ||
      !ready ||
      !isPushSupported() ||
      typeof Notification === "undefined" ||
      Notification.permission !== "granted"
    ) {
      return;
    }
    let active = true;
    const reconcile = () => {
      if (active && document.visibilityState === "visible") {
        void enablePushSubscription();
      }
    };
    const unsubscribe = subscribeDevicePush(() => {
      if (getDevicePushState() === "idle") queueMicrotask(reconcile);
    });
    const storage = (event: StorageEvent) => {
      if (event.key === null || event.key === `sumi:device-push:off:${owner}`)
        refreshDevicePushPreference();
    };
    void enablePushSubscription();
    window.addEventListener("storage", storage);
    window.addEventListener("focus", reconcile);
    window.addEventListener("online", reconcile);
    document.addEventListener("visibilitychange", reconcile);
    return () => {
      active = false;
      unsubscribe();
      window.removeEventListener("storage", storage);
      window.removeEventListener("focus", reconcile);
      window.removeEventListener("online", reconcile);
      document.removeEventListener("visibilitychange", reconcile);
    };
  }, [enabled, ready, transportGeneration, owner]);

  return null;
}
