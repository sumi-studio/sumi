import {
  Mic,
  MicOff,
  MonitorUp,
  PhoneOff,
  Video,
  VideoOff,
  Volume2,
} from "lucide-react";
import { usePlaceDisplay } from "../use-place-name";
import { useCall } from "./call-store";

export function CallBanner() {
  const key = useCall((state) => state.activePlaceKey);
  const phase = useCall((state) => state.phase);
  const local = useCall((state) => state.local);
  const audioBlocked = useCall((state) => state.audioPlaybackBlocked);
  const toggleMic = useCall((state) => state.toggleMicrophone);
  const toggleCamera = useCall((state) => state.toggleCamera);
  const toggleScreen = useCall((state) => state.toggleScreenShare);
  const leave = useCall((state) => state.leave);
  const resumeAudio = useCall((state) => state.resumeAudio);
  const display = usePlaceDisplay(key);
  if (!key || (phase !== "connecting" && phase !== "connected")) return null;
  const control =
    "flex size-8 items-center justify-center rounded-md transition-colors hover:bg-background/70";
  return (
    <div className="flex shrink-0 items-center gap-2 border-border/70 border-b bg-emerald-500/10 px-4 py-2 sm:px-5">
      <span className="size-2 rounded-full bg-emerald-500" />
      <div className="min-w-0 flex-1">
        <p className="truncate font-medium text-[12.5px]">
          {phase === "connecting" ? "通話に接続中…" : "通話中"}
        </p>
        <p className="truncate text-[11px] text-muted-foreground">
          {display?.name ?? "会話"}
        </p>
      </div>
      {audioBlocked ? (
        <button
          type="button"
          onClick={() => void resumeAudio()}
          className={`${control} text-amber-600`}
          title="音声を再生"
        >
          <Volume2 className="size-4" />
        </button>
      ) : null}
      <button
        type="button"
        disabled={phase !== "connected"}
        onClick={toggleMic}
        className={control}
        title={local.micEnabled ? "ミュート" : "ミュート解除"}
      >
        {local.micEnabled ? (
          <Mic className="size-4" />
        ) : (
          <MicOff className="size-4 text-rose-500" />
        )}
      </button>
      <button
        type="button"
        disabled={phase !== "connected"}
        onClick={toggleCamera}
        className={control}
        title={local.cameraEnabled ? "カメラを止める" : "カメラを使う"}
      >
        {local.cameraEnabled ? (
          <Video className="size-4" />
        ) : (
          <VideoOff className="size-4" />
        )}
      </button>
      <button
        type="button"
        disabled={phase !== "connected"}
        onClick={toggleScreen}
        className={`${control} ${local.screenShareEnabled ? "text-emerald-600" : ""}`}
        title="画面共有"
      >
        <MonitorUp className="size-4" />
      </button>
      <button
        type="button"
        onClick={() => void leave()}
        className={`${control} bg-rose-500/15 text-rose-600 hover:bg-rose-500/25`}
        title="退出"
      >
        <PhoneOff className="size-4" />
      </button>
    </div>
  );
}
