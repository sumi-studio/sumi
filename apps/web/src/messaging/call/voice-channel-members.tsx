import { MicOff, MonitorUp } from "lucide-react";
import { ParticipantAvatar } from "../components/participant-avatar";
import type { PlaceKey } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { useCall } from "./call-store";

/**
 * ボイスチャンネル名の下にぶら下がる、今その通話にいる人たち。サイドバーは
 * 「どこで誰が話しているか」が名前を開かずに分かる場所であってほしいので、
 * 部屋の中身をその場に出す。
 *
 * 誰もいなければ何も出さない——空の枠は「空だ」以上のことを言わない。
 */
export function VoiceChannelMembers({ placeKey: key }: { placeKey: PlaceKey }) {
  const call = useCall((state) => state.stateByPlace[key]);
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const micEnabled = useCall((state) => state.local.micEnabled);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const selfKey = useMessaging((state) => state.selfKey);

  const participants = call?.participants ?? [];
  if (participants.length === 0) return null;

  return (
    <ul className="mb-0.5 ml-6 space-y-0.5">
      {participants.map((entry) => {
        const memberKey = participantKey(entry.participant);
        const self = memberKey === selfKey;
        // ミュートは自分の分しか手元に無い。他人の分を推測して出さない。
        const muted = self && activePlaceKey === key && !micEnabled;
        return (
          <li
            key={memberKey}
            className="flex items-center gap-1.5 rounded px-2 py-0.5 text-[12px] text-muted-foreground"
          >
            <ParticipantAvatar
              participantKey={memberKey}
              name={membersByKey[memberKey]?.displayName ?? "?"}
              size={16}
            />
            <span className="min-w-0 flex-1 truncate">
              {membersByKey[memberKey]?.displayName ?? "不明"}
            </span>
            {entry.screenShare ? (
              <MonitorUp
                aria-label="画面共有中"
                className="size-3 shrink-0 text-emerald-600"
              />
            ) : null}
            {muted ? (
              <MicOff
                aria-label="ミュート中"
                className="size-3 shrink-0 text-muted-foreground/70"
              />
            ) : null}
          </li>
        );
      })}
    </ul>
  );
}
