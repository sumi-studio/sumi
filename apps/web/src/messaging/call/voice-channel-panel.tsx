import { Volume2 } from "lucide-react";
import type { PlaceKey } from "../model";
import { isCallActive, useCall } from "./call-store";

/**
 * ボイスチャンネルを開いたが、まだ誰も入っていないときの入り口。
 *
 * 誰かが入っている間はCallBannerが、自分が入っている間はCallStageが同じ場所に
 * 出るので、ここは「空の部屋の扉」だけを担当する。ボイスチャンネルでも下の
 * テキスト列はそのまま使えるので、この帯は列を畳まない。
 */
export function VoiceChannelPanel({ placeKey: key }: { placeKey: PlaceKey }) {
  const active = useCall((state) => isCallActive(state, key));
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const phase = useCall((state) => state.phase);
  const join = useCall((state) => state.join);

  if (active || activePlaceKey === key) return null;

  return (
    <div className="flex shrink-0 items-center gap-2 border-border/70 border-b bg-muted/20 px-4 py-2 sm:px-5">
      <Volume2 className="size-3.5 shrink-0 text-muted-foreground" />
      <span className="min-w-0 flex-1 truncate text-[12px] text-muted-foreground">
        まだ誰も入っていません。入ると通話が始まります
      </span>
      <button
        type="button"
        disabled={phase === "connecting"}
        onClick={() => void join(key)}
        className="shrink-0 rounded-md bg-primary px-2.5 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
      >
        通話に参加
      </button>
    </div>
  );
}
