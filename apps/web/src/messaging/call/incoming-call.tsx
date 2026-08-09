import { Phone, PhoneOff } from "lucide-react";
import { useEffect, useMemo } from "react";
import { ParticipantAvatar } from "../components/participant-avatar";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { incomingCallFor, useCall } from "./call-store";
import { startRingtone, stopRingtone } from "./ringtone";

/**
 * 着信。DM・グループDMで相手が通話を始めたときだけ出る（チャンネルの通話は
 * 「入れる場所」であって、誰かが自分を呼んだわけではない——call-store.tsの
 * incomingCallForがその判断を持つ）。
 *
 * 拒否は自分の画面を閉じるだけで、相手の通話は続く。断ったことを相手へ
 * 通知しない——出られない理由は本人のもので、説明を強制しない。
 */
export function IncomingCallModal() {
  const selfKey = useMessaging((state) => state.selfKey);
  // incomingCallForは新しいobjectを返すのでselectorに置かない
  // （storeが変わっていなくても毎回別物になり、購読が回り続ける）。
  const stateByPlace = useCall((state) => state.stateByPlace);
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const dismissedPlaces = useCall((state) => state.dismissedPlaces);
  const incoming = useMemo(
    () =>
      incomingCallFor(
        { stateByPlace, activePlaceKey, dismissedPlaces },
        selfKey,
      ),
    [stateByPlace, activePlaceKey, dismissedPlaces, selfKey],
  );
  const join = useCall((state) => state.join);
  const dismissIncoming = useCall((state) => state.dismissIncoming);
  const membersByKey = useMessaging((state) => state.membersByKey);

  const ringing = incoming !== null;
  useEffect(() => {
    if (!ringing) return;
    startRingtone();
    return () => stopRingtone();
  }, [ringing]);

  useEffect(() => {
    if (!incoming) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") dismissIncoming(incoming.placeKey);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [incoming, dismissIncoming]);

  if (!incoming) return null;

  const fromKey = participantKey(incoming.from);
  const name = membersByKey[fromKey]?.displayName ?? "不明";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div
        role="dialog"
        aria-modal="true"
        aria-label="着信"
        className="w-72 rounded-xl border border-border bg-background p-5 text-center shadow-lg"
      >
        <span className="mx-auto block w-fit">
          <ParticipantAvatar participantKey={fromKey} name={name} size={64} />
        </span>
        <p className="pt-3 font-semibold text-[15px]">{name}</p>
        <p className="pt-0.5 text-[12px] text-muted-foreground">着信中…</p>
        <div className="flex items-center justify-center gap-6 pt-5">
          <button
            type="button"
            aria-label="拒否"
            title="拒否"
            onClick={() => dismissIncoming(incoming.placeKey)}
            className="flex size-11 items-center justify-center rounded-full bg-rose-500 text-white transition-colors hover:bg-rose-600"
          >
            <PhoneOff className="size-5" />
          </button>
          <button
            type="button"
            aria-label="応答"
            title="応答"
            onClick={() => void join(incoming.placeKey)}
            className="flex size-11 items-center justify-center rounded-full bg-emerald-500 text-white transition-colors hover:bg-emerald-600"
          >
            <Phone className="size-5" />
          </button>
        </div>
      </div>
    </div>
  );
}
