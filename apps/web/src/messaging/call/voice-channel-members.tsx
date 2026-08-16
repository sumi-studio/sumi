import { Mic2 } from "lucide-react";
import { useEffect, useState } from "react";
import type { PlaceKey } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { useCall } from "./call-store";

export function VoiceChannelMembers({ placeKey }: { placeKey: PlaceKey }) {
  const call = useCall((state) => state.stateByPlace[placeKey]);
  const speaking = useCall((state) => state.speakingUntil);
  const members = useMessaging((state) => state.membersByKey);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!call?.participants.length) return;
    const timer = window.setInterval(() => setNow(Date.now()), 500);
    return () => window.clearInterval(timer);
  }, [call?.participants.length]);
  if (!call?.participants.length) return null;
  return (
    <div className="ml-7 space-y-0.5 py-1">
      {call.participants.map(({ participant }) => {
        const key = participantKey(participant);
        const active = (speaking[key] ?? 0) > now;
        return (
          <div
            key={key}
            className={`flex items-center gap-1.5 truncate text-[11px] ${active ? "font-medium text-emerald-600 dark:text-emerald-400" : "text-muted-foreground"}`}
          >
            <Mic2 className="size-3 shrink-0" />
            <span className="truncate">
              {members[key]?.displayName ?? "参加者"}
            </span>
          </div>
        );
      })}
    </div>
  );
}
