import { useState, useSyncExternalStore } from "react";
import {
  disablePushSubscription,
  enablePushSubscription,
  getDevicePushState,
  isPushSupported,
  subscribeDevicePush,
} from "../push";

/** Device registration is separate from which conversations should notify. */
export function DevicePushSettings() {
  const state = useSyncExternalStore(
    subscribeDevicePush,
    getDevicePushState,
    () => "idle",
  );
  const [permission, setPermission] = useState(() =>
    typeof Notification === "undefined" ? "default" : Notification.permission,
  );
  const [failure, setFailure] = useState("");
  const supported = isPushSupported();
  const busy = state === "enabling" || state === "disabling";
  const enabled = state === "enabled";
  const toggle = async () => {
    setFailure("");
    if (enabled || state === "disable-error") {
      if (!(await disablePushSubscription()))
        setFailure("通知を停止できませんでした。もう一度お試しください。");
      return;
    }
    try {
      // Keep permission request directly inside the user's button gesture.
      const next =
        Notification.permission === "default"
          ? await Notification.requestPermission()
          : Notification.permission;
      setPermission(next);
      if (next !== "granted") return;
      if (!(await enablePushSubscription(true)))
        setFailure(
          "通知を登録できませんでした。接続を確認して再試行してください。",
        );
    } catch {
      setFailure("通知を登録できませんでした。再試行してください。");
    }
  };
  const help = !supported
    ? "スマートフォンではホーム画面に追加して開いてください。通知にはHTTPSと対応ブラウザが必要です。"
    : permission === "denied"
      ? "ブラウザまたは端末の設定から、Sumiの通知を許可してください。"
      : failure ||
        (state === "disable-error"
          ? "通知を停止できませんでした。再試行してください。"
          : state === "error"
            ? "通知の登録を確認できませんでした。再試行してください。"
            : enabled
              ? "アプリを閉じていても通知を受け取ります。"
              : "この端末で、アプリを閉じている間も通知を受け取れます。");
  return (
    <section
      className="mt-3 border-border/60 border-t px-2 pt-3"
      aria-label="この端末のプッシュ通知"
    >
      <div className="flex items-center justify-between gap-2 text-[13px]">
        <span>この端末への通知</span>
        {supported && permission !== "denied" ? (
          <button
            type="button"
            disabled={busy}
            onClick={() => {
              void toggle();
            }}
            className="rounded-md bg-accent px-2 py-1 text-[12px] hover:bg-accent/70 disabled:opacity-50"
          >
            {busy
              ? "更新中…"
              : enabled
                ? "オフにする"
                : state === "error" || state === "disable-error"
                  ? "再試行"
                  : "オンにする"}
          </button>
        ) : null}
      </div>
      <p
        aria-live="polite"
        className="mt-1.5 text-[11px] text-muted-foreground leading-relaxed"
      >
        {help}
      </p>
    </section>
  );
}
