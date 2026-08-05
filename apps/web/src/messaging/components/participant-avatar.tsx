import { useState } from "react";
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

/** 自己申告ステータスの色と日本語表示。参加者UI全体で1か所に持つ。 */
export const STATUS_DOT: Record<StatusKind, string> = {
  available: "bg-emerald-500",
  busy: "bg-rose-500",
  away: "bg-amber-400",
};

export const STATUS_LABEL: Record<StatusKind, string> = {
  available: "対応可能",
  busy: "取り込み中",
  away: "離席中",
};

export function ParticipantAvatar({
  participantKey,
  name,
  size = 32,
  status,
  src,
}: {
  participantKey: string;
  name: string;
  size?: number;
  status?: StatusKind;
  /** 本人が設定した画像。無ければ頭文字にフォールバックする。 */
  src?: string;
}) {
  const hue = hueFor(participantKey);
  const [failedSrc, setFailedSrc] = useState<string>();
  const showImage = Boolean(src && src !== failedSrc);
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
      {showImage ? (
        // ステータスの点は円の外側に環を持つので、切り抜きは画像だけに掛ける。
        <span className="absolute inset-0 overflow-hidden rounded-full">
          <img
            src={src}
            alt=""
            className="size-full object-cover"
            onError={() => setFailedSrc(src)}
          />
        </span>
      ) : (
        name.slice(0, 1).toUpperCase()
      )}
      {status ? (
        <span
          className={`absolute right-0 bottom-0 block rounded-full ring-2 ring-background ${STATUS_DOT[status]}`}
          style={{ width: size / 3.2, height: size / 3.2 }}
        />
      ) : null}
    </span>
  );
}
