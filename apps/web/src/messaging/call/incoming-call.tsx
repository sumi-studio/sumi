import { Phone, PhoneOff } from "lucide-react";
import { useEffect, useMemo } from "react";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { incomingCallFor, useCall } from "./call-store";
import { playRingtone } from "./ringtone";

export function IncomingCall() {
  const selfKey = useMessaging((state) => state.selfKey);
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
  const members = useMessaging((state) => state.membersByKey);
  const join = useCall((state) => state.join);
  const dismiss = useCall((state) => state.dismissIncoming);
  const display = usePlaceDisplay(incoming?.placeKey ?? null);
  useEffect(() => {
    if (!incoming) return;
    return playRingtone();
  }, [incoming]);
  if (!incoming) return null;
  const from =
    members[participantKey(incoming.from)]?.displayName ??
    display?.name ??
    "誰か";
  return (
    <div
      role="dialog"
      aria-label="着信"
      className="fixed right-4 bottom-4 z-50 w-72 rounded-xl border border-border bg-background p-3 shadow-lg"
    >
      <p className="font-medium text-[13px]">{from} から着信</p>
      <p className="mt-0.5 text-[11px] text-muted-foreground">
        {display?.name ?? "ダイレクトメッセージ"}
      </p>
      <div className="mt-3 flex justify-end gap-2">
        <button
          type="button"
          onClick={() => dismiss(incoming.placeKey)}
          className="flex items-center gap-1 rounded-md bg-muted px-2.5 py-1.5 text-[12px]"
        >
          <PhoneOff className="size-3.5" />
          応答しない
        </button>
        <button
          type="button"
          onClick={() => void join(incoming.placeKey)}
          className="flex items-center gap-1 rounded-md bg-emerald-600 px-2.5 py-1.5 font-medium text-[12px] text-white"
        >
          <Phone className="size-3.5" />
          参加
        </button>
      </div>
    </div>
  );
}
