import { Headphones, PhoneOff } from "lucide-react";
import type { PlaceKey } from "../model";
import { useCall } from "./call-store";

export function VoiceChannelPanel({ placeKey }: { placeKey: PlaceKey }) {
  const activeKey = useCall((state) => state.activePlaceKey);
  const phase = useCall((state) => state.phase);
  const join = useCall((state) => state.join);
  const leave = useCall((state) => state.leave);
  const here = activeKey === placeKey;
  return (
    <button
      type="button"
      onClick={() => (here ? void leave() : void join(placeKey))}
      className={`ml-7 mt-1 flex items-center gap-1.5 rounded-md px-2 py-1 text-[11px] ${here ? "bg-emerald-500/10 text-emerald-700 dark:text-emerald-400" : "text-muted-foreground hover:bg-accent hover:text-foreground"}`}
    >
      {here ? (
        <PhoneOff className="size-3" />
      ) : (
        <Headphones className="size-3" />
      )}
      {here ? (phase === "connecting" ? "接続中…" : "退出") : "参加"}
    </button>
  );
}
