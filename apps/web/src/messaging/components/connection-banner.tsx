import { useEffect, useRef, useState } from "react";
import { useMessaging } from "../store";

const RECONNECTED_FLASH_MS = 2_500;
const RECONNECTING_DELAY_MS = 1_500;

/**
 * 接続が切れている間だけメッセージリスト上部に出る細いバナー。
 * 送信自体はHTTPなので直ちに失敗するとは限らないが、liveイベントが
 * 届いていないことをユーザーが認知できるようにする。
 */
export function ConnectionBanner() {
  const connection = useMessaging((state) => state.connection);
  const [flash, setFlash] = useState(false);
  const [interruptionVisible, setInterruptionVisible] = useState(false);
  const interruptionWasVisible = useRef(false);
  const hasConnected = useRef(connection === "connected");
  const wasInterrupted = useRef(false);
  const previousConnection = useRef(connection);

  useEffect(() => {
    const previous = previousConnection.current;
    previousConnection.current = connection;
    if (connection === "connected") {
      hasConnected.current = true;
      if (!wasInterrupted.current) return;
      const showRecovered = interruptionWasVisible.current;
      wasInterrupted.current = false;
      interruptionWasVisible.current = false;
      setInterruptionVisible(false);
      setFlash(false);
      if (!showRecovered) return;
      setFlash(true);
      const timer = window.setTimeout(
        () => setFlash(false),
        RECONNECTED_FLASH_MS,
      );
      return () => window.clearTimeout(timer);
    }

    if (!hasConnected.current) return;
    if (previous === "connected") wasInterrupted.current = true;
    if (!wasInterrupted.current) return;
    setFlash(false);
    if (connection === "disconnected") {
      interruptionWasVisible.current = true;
      setInterruptionVisible(true);
      return;
    }
    const timer = window.setTimeout(
      () => {
        interruptionWasVisible.current = true;
        setInterruptionVisible(true);
      },
      RECONNECTING_DELAY_MS,
    );
    return () => window.clearTimeout(timer);
  }, [connection]);

  if (connection === "connected" && !flash) return null;
  if (!interruptionVisible && !flash) return null;

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
