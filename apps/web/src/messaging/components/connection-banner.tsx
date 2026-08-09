import { useEffect, useRef, useState } from "react";
import { useMessaging } from "../store";

const RECONNECTED_FLASH_MS = 2_500;

/**
 * 接続が切れている間だけメッセージリスト上部に出る細いバナー。
 * 送信自体はHTTPなので直ちに失敗するとは限らないが、liveイベントが
 * 届いていないことをユーザーが認知できるようにする。
 */
export function ConnectionBanner() {
  const connection = useMessaging((state) => state.connection);
  const [flash, setFlash] = useState(false);
  const wasInterrupted = useRef(false);
  const previousConnection = useRef(connection);

  useEffect(() => {
    const previous = previousConnection.current;
    previousConnection.current = connection;
    if (connection !== "connected") {
      if (previous === "connected") {
        wasInterrupted.current = true;
      }
      setFlash(false);
      return;
    }
    if (!wasInterrupted.current) return;
    wasInterrupted.current = false;
    setFlash(true);
    const timer = window.setTimeout(
      () => setFlash(false),
      RECONNECTED_FLASH_MS,
    );
    return () => window.clearTimeout(timer);
  }, [connection]);

  if (connection === "connected" && !flash) return null;

  if (connection === "connected") {
    return (
      <div
        role="status"
        className="shrink-0 bg-emerald-500/10 px-4 py-1 text-center text-[11px] text-emerald-700 sm:px-6 dark:text-emerald-400"
      >
        再接続しました
      </div>
    );
  }

  return (
    <div
      role="status"
      className="shrink-0 bg-amber-500/10 px-4 py-1 text-center text-[11px] text-amber-700 sm:px-6 dark:text-amber-400"
    >
      {connection === "reconnecting"
        ? "再接続中… 新しいメッセージが届いていない可能性があります"
        : "サーバーに接続できません"}
    </div>
  );
}
