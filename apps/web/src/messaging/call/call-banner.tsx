import { Phone } from "lucide-react";
import { ParticipantAvatar } from "../components/participant-avatar";
import type { PlaceKey } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { isCallActive, useCall } from "./call-store";

/**
 * 「この場所で今通話しています」の帯。まだ入っていない人に、誰がいるかと
 * 入り口だけを見せる。入ったらCallStageに置き換わる。
 */
export function CallBanner({ placeKey: key }: { placeKey: PlaceKey }) {
  const active = useCall((state) => isCallActive(state, key));
  const call = useCall((state) => state.stateByPlace[key]);
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const join = useCall((state) => state.join);
  const membersByKey = useMessaging((state) => state.membersByKey);

  if (!active || activePlaceKey === key) return null;

  const participants = call?.participants ?? [];

  return (
    <div className="flex shrink-0 items-center gap-2 border-border/70 border-b bg-accent/40 px-4 py-2 sm:px-5">
      <Phone className="size-3.5 shrink-0 text-emerald-600" />
      <span className="shrink-0 font-medium text-[12px]">現在通話中</span>
      <span className="flex min-w-0 flex-1 items-center gap-1 overflow-hidden">
        {participants.slice(0, 6).map((entry) => {
          const key = participantKey(entry.participant);
          return (
            <ParticipantAvatar
              key={key}
              participantKey={key}
              name={membersByKey[key]?.displayName ?? "?"}
              size={20}
            />
          );
        })}
        {participants.length > 6 ? (
          <span className="text-[11px] text-muted-foreground">
            ほか{participants.length - 6}人
          </span>
        ) : null}
      </span>
      <button
        type="button"
        onClick={() => void join(key)}
        className="shrink-0 rounded-md bg-primary px-2.5 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90"
      >
        参加
      </button>
    </div>
  );
}
