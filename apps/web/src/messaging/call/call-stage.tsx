import { useEffect, useRef } from "react";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { useCall } from "./call-store";
import type { CallMediaTrack } from "./model";

function VideoTile({ track }: { track: CallMediaTrack }) {
  const ref = useRef<HTMLVideoElement>(null);
  const name = useMessaging(
    (state) =>
      state.membersByKey[track.participantKey]?.displayName ?? "参加者",
  );
  useEffect(() => {
    const element = ref.current;
    if (element) track.attach?.(element);
    return () => track.detach?.();
  }, [track]);
  return (
    <figure className="relative min-h-36 overflow-hidden rounded-lg bg-black">
      {/* biome-ignore lint/a11y/useMediaCaption: LiveKit video tracks contain no audio; remote audio is attached separately. */}
      <video
        ref={ref}
        autoPlay
        playsInline
        className="h-full w-full object-contain"
      />
      <figcaption className="absolute bottom-2 left-2 rounded bg-black/60 px-2 py-0.5 text-[11px] text-white">
        {name}
        {track.kind === "screen" ? " · 画面共有" : ""}
      </figcaption>
    </figure>
  );
}

export function CallStage() {
  const key = useCall((state) => state.activePlaceKey);
  const tracks = useCall((state) => state.tracks);
  const call = useCall((state) => (key ? state.stateByPlace[key] : undefined));
  const members = useMessaging((state) => state.membersByKey);
  if (!key || tracks.length === 0) return null;
  return (
    <section
      aria-label="通話映像"
      className="grid max-h-[42vh] shrink-0 grid-cols-1 gap-2 overflow-y-auto border-border/70 border-b bg-muted/20 p-3 sm:grid-cols-2"
    >
      {tracks.map((track) => (
        <VideoTile
          key={`${track.participantKey}:${track.kind}`}
          track={track}
        />
      ))}
      {call?.participants
        .filter(
          (entry) =>
            !tracks.some(
              (track) =>
                track.participantKey === participantKey(entry.participant),
            ),
        )
        .map((entry) => (
          <div
            key={participantKey(entry.participant)}
            className="grid min-h-36 place-items-center rounded-lg bg-muted text-sm text-muted-foreground"
          >
            {members[participantKey(entry.participant)]?.displayName ??
              "参加者"}
          </div>
        ))}
    </section>
  );
}
