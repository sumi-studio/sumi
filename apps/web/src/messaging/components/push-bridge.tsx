import { useEffect } from "react";
import { enablePushSubscription, isPushSupported } from "../push";
import { useMessaging } from "../store";

/**
 * 端末を「タブが無いときにも呼べる」状態に保つだけの、何も描かない部品。
 *
 * 通知許可はバナーが取る。ここが引き受けるのは、その許可が既にある端末を
 * 黙って購読済みにしておくこと——一度許可した人が次に開いたとき、別の操作を
 * 求められずに届き続けるべきだからである。
 *
 * 許可が無い端末では何も起きない。購読は許可の副作用であって、通知条件では
 * ない（条件は本人の NotificationSetting がサーバー側に持っている）。
 */
export function PushSubscriptionBridge() {
  const enabled = useMessaging((state) => state.capabilities.notifications);
  // ready を待つのは、購読の POST がセッション確立前に飛ぶのを避けるため。
  const ready = useMessaging((state) => state.ready);

  useEffect(() => {
    if (!enabled || !ready || !isPushSupported()) return;
    if (typeof Notification === "undefined") return;
    if (Notification.permission !== "granted") return;
    void enablePushSubscription();
  }, [enabled, ready]);

  return null;
}
