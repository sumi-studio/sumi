import { Phone, Video } from "lucide-react";
import type { PlaceKey } from "../model";
import { isCallActive, useCall } from "./call-store";

/**
 * ヘッダーの通話開始ボタン。音声とビデオは同じ通話の入り方違いで、別々の
 * 通話ではない——ビデオはカメラを開けて入る、それだけ。
 *
 * 既に通話が続いているplaceでは「参加」の導線がバナー側にあるので、ここは
 * 開始する人のためだけに残す。
 */
export function CallStartButtons({ placeKey: key }: { placeKey: PlaceKey }) {
  const join = useCall((state) => state.join);
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const phase = useCall((state) => state.phase);
  const failure = useCall((state) => state.failure);
  const active = useCall((state) => isCallActive(state, key));

  if (activePlaceKey === key) return null;

  const start = async (withCamera: boolean) => {
    await join(key);
    if (withCamera && useCall.getState().activePlaceKey === key) {
      useCall.getState().toggleCamera();
    }
  };

  const busy = phase === "connecting";

  return (
    <>
      {failure === "unavailable" ? (
        <span className="hidden text-[11px] text-muted-foreground sm:inline">
          通話は未設定です
        </span>
      ) : null}
      <button
        type="button"
        title={active ? "通話に参加" : "音声通話を開始"}
        aria-label={active ? "通話に参加" : "音声通話を開始"}
        disabled={busy}
        onClick={() => void start(false)}
        className="flex size-8 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:opacity-50"
      >
        <Phone className="size-4" />
      </button>
      <button
        type="button"
        title={active ? "ビデオで参加" : "ビデオ通話を開始"}
        aria-label={active ? "ビデオで参加" : "ビデオ通話を開始"}
        disabled={busy}
        onClick={() => void start(true)}
        className="flex size-8 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:opacity-50"
      >
        <Video className="size-4" />
      </button>
    </>
  );
}
