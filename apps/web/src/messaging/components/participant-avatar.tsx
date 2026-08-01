import type { StatusKind } from "../model";

/**
 * 参加者アバター。人間とagentを完全に同じ見た目の文法で扱う
 * （bot badgeのような区別マークは置かない）。
 */

function hueFor(key: string): number {
  let hash = 0;
  for (let index = 0; index < key.length; index += 1) {
    hash = (hash * 31 + key.charCodeAt(index)) | 0;
  }
  return ((hash % 360) + 360) % 360;
}

const STATUS_DOT: Record<StatusKind, string> = {
  available: "bg-emerald-500",
  busy: "bg-rose-500",
  away: "bg-amber-400",
};

export function ParticipantAvatar({
  participantKey,
  name,
  size = 32,
  status,
}: {
  participantKey: string;
  name: string;
  size?: number;
  status?: StatusKind;
}) {
  const hue = hueFor(participantKey);
  return (
    <span
      className="relative inline-flex shrink-0 select-none items-center justify-center rounded-full font-medium"
      style={{
        width: size,
        height: size,
        fontSize: Math.max(11, Math.round(size * 0.42)),
        backgroundColor: `oklch(0.92 0.045 ${hue})`,
        color: `oklch(0.45 0.11 ${hue})`,
      }}
      aria-hidden
    >
      {name.slice(0, 1).toUpperCase()}
      {status ? (
        <span
          className={`absolute right-0 bottom-0 block rounded-full ring-2 ring-background ${STATUS_DOT[status]}`}
          style={{ width: size / 3.2, height: size / 3.2 }}
        />
      ) : null}
    </span>
  );
}
