import { useEffect } from "react";
import { enablePushSubscription, isPushSupported } from "../push";
import { useMessaging } from "../store";

// Shell-lifetime reconciliation for an already granted browser permission.
// Permission itself remains owned by the explicit Messaging banner action.
export function PushSubscriptionBridge() {
  const enabled = useMessaging((state) => state.capabilities.notifications);
  const ready = useMessaging((state) => state.ready);
  const transportGeneration = useMessaging(
    (state) => state.transportGeneration,
  );

  useEffect(() => {
    // A new transport generation must re-post the browser's durable
    // subscription under the replacement exact Messaging authority.
    void transportGeneration;
    if (
      !enabled ||
      !ready ||
      !isPushSupported() ||
      typeof Notification === "undefined" ||
      Notification.permission !== "granted"
    ) {
      return;
    }
    const reconcile = () => {
      if (document.visibilityState === "visible") {
        void enablePushSubscription();
      }
    };
    void enablePushSubscription();
    window.addEventListener("focus", reconcile);
    window.addEventListener("online", reconcile);
    document.addEventListener("visibilitychange", reconcile);
    return () => {
      window.removeEventListener("focus", reconcile);
      window.removeEventListener("online", reconcile);
      document.removeEventListener("visibilitychange", reconcile);
    };
  }, [enabled, ready, transportGeneration]);

  return null;
}
