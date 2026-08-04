import {
  Mic,
  MicOff,
  MonitorUp,
  PhoneOff,
  Video,
  VideoOff,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { ParticipantAvatar } from "../components/participant-avatar";
import type { ParticipantKey, PlaceKey } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { useCall } from "./call-store";
import type { CallMediaTrack } from "./model";

/**
 * 通話の本体。参加者タイルと自分のコントロールだけを持ち、テキストの列は
 * 畳まない——通話中も同じ画面で会話が続けられることが要件（ADR 0012の
 * 「通話はテキストの上に乗る層」）。
 */

/** 届いた映像を1枚のvideoへ差し込む。transportの実装は知らない。 */
function TrackVideo({ track }: { track: CallMediaTrack }) {
  const ref = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    const element = ref.current;
    if (!element || !track.attach) return;
    track.attach(element);
    return () => {
      track.detach?.();
    };
  }, [track]);

  return (
    <video
      ref={ref}
      autoPlay
      playsInline
      muted={track.kind === "screen"}
      className="size-full rounded-lg bg-black object-contain"
    >
      {/* 音声は別トラックで流れるため、字幕トラックは持たない。 */}
      <track kind="captions" />
    </video>
  );
}

function ParticipantTile({
  participantKey: key,
  speaking,
  video,
}: {
  participantKey: ParticipantKey;
  speaking: boolean;
  video: CallMediaTrack | undefined;
}) {
  const membersByKey = useMessaging((state) => state.membersByKey);
  const name = membersByKey[key]?.displayName ?? "不明";

  return (
    <div
      className={`relative flex aspect-video min-w-0 items-center justify-center overflow-hidden rounded-lg bg-muted/50 ring-2 transition-colors ${
        speaking ? "ring-emerald-500" : "ring-transparent"
      }`}
    >
      {video ? (
        <TrackVideo track={video} />
      ) : (
        <ParticipantAvatar participantKey={key} name={name} size={56} />
      )}
      <span className="absolute bottom-1.5 left-1.5 max-w-[calc(100%-0.75rem)] truncate rounded bg-black/55 px-1.5 py-0.5 text-[11px] text-white">
        {name}
      </span>
    </div>
  );
}

function ControlButton({
  label,
  active,
  danger,
  onClick,
  children,
}: {
  label: string;
  active?: boolean;
  danger?: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      title={label}
      aria-label={label}
      aria-pressed={danger ? undefined : active}
      onClick={onClick}
      className={`flex size-9 items-center justify-center rounded-full transition-colors ${
        danger
          ? "bg-rose-500 text-white hover:bg-rose-600"
          : active
            ? "bg-foreground text-background hover:opacity-90"
            : "bg-muted text-muted-foreground hover:bg-accent hover:text-foreground"
      }`}
    >
      {children}
    </button>
  );
}

/** 自分が通話に入っているときだけ出る、タイルとコントロールの領域。 */
export function CallStage({ placeKey: key }: { placeKey: PlaceKey }) {
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const phase = useCall((state) => state.phase);
  const local = useCall((state) => state.local);
  const tracks = useCall((state) => state.tracks);
  const speakingUntil = useCall((state) => state.speakingUntil);
  const call = useCall((state) => state.stateByPlace[key]);
  const toggleMicrophone = useCall((state) => state.toggleMicrophone);
  const toggleCamera = useCall((state) => state.toggleCamera);
  const toggleScreenShare = useCall((state) => state.toggleScreenShare);
  const leave = useCall((state) => state.leave);
  const selfKey = useMessaging((state) => state.selfKey);
  const [now, setNow] = useState(() => Date.now());

  // 発話リングはTTLで消えるので、通話中だけ緩やかに時計を進める。
  useEffect(() => {
    if (activePlaceKey !== key) return;
    const timer = window.setInterval(() => setNow(Date.now()), 400);
    return () => window.clearInterval(timer);
  }, [activePlaceKey, key]);

  if (activePlaceKey !== key) return null;

  const participants = call?.participants.map((entry) =>
    participantKey(entry.participant),
  ) ?? [selfKey];
  const keys = participants.includes(selfKey)
    ? participants
    : [selfKey, ...participants];
  const cameraByKey = new Map(
    tracks
      .filter((track) => track.kind === "camera")
      .map((track) => [track.participantKey, track]),
  );
  const screens = tracks.filter((track) => track.kind === "screen");

  return (
    <section
      aria-label="通話"
      className="shrink-0 border-border/70 border-b bg-muted/20 px-4 py-3 sm:px-5"
    >
      {phase === "connecting" ? (
        <p className="pb-2 text-[12px] text-muted-foreground">
          接続しています…
        </p>
      ) : null}
      {screens.length > 0 ? (
        <div className="pb-2">
          {screens.map((track) => (
            <div
              key={`${track.participantKey}-screen`}
              className="aspect-video w-full overflow-hidden rounded-lg bg-black"
            >
              <TrackVideo track={track} />
            </div>
          ))}
        </div>
      ) : null}
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
        {keys.map((participant) => (
          <ParticipantTile
            key={participant}
            participantKey={participant}
            speaking={(speakingUntil[participant] ?? 0) > now}
            video={cameraByKey.get(participant)}
          />
        ))}
      </div>
      <div className="flex items-center justify-center gap-2 pt-3">
        <ControlButton
          label={local.micEnabled ? "ミュートする" : "ミュートを解除"}
          active={local.micEnabled}
          onClick={toggleMicrophone}
        >
          {local.micEnabled ? (
            <Mic className="size-4" />
          ) : (
            <MicOff className="size-4" />
          )}
        </ControlButton>
        <ControlButton
          label={local.cameraEnabled ? "カメラを止める" : "カメラを入れる"}
          active={local.cameraEnabled}
          onClick={toggleCamera}
        >
          {local.cameraEnabled ? (
            <Video className="size-4" />
          ) : (
            <VideoOff className="size-4" />
          )}
        </ControlButton>
        <ControlButton
          label={
            local.screenShareEnabled ? "画面共有をやめる" : "画面を共有する"
          }
          active={local.screenShareEnabled}
          onClick={toggleScreenShare}
        >
          <MonitorUp className="size-4" />
        </ControlButton>
        <ControlButton label="通話を終える" danger onClick={() => void leave()}>
          <PhoneOff className="size-4" />
        </ControlButton>
      </div>
    </section>
  );
}
