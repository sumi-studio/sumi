/**
 * 直近に使ったリアクション絵文字。
 *
 * 「この端末でさっき何を押したか」は入力の手癖であって、参加者の状態でも
 * 会話の事実でもない。サーバーの正本へ混ぜず localStorage に置く
 * （pre-launchで守るべき履歴も無い）。他の端末に持ち越せないのは承知の上で、
 * そのためだけにサーバー契約を増やす価値はないと判断した。
 */

import { useSyncExternalStore } from "react";
import { DEFAULT_RECENT_EMOJIS } from "./emoji-data";

const STORAGE_KEY = "sumi.messaging.recent-emoji";

/** 覚えておく最大件数。操作チップに出すのはこの先頭3件。 */
const MAX_REMEMBERED = 24;

const listeners = new Set<() => void>();

/** getSnapshotは同じ参照を返し続ける必要がある（毎回新しい配列だと無限再描画）。 */
let cache: string[] | null = null;

function read(): string[] {
  if (cache) return cache;
  let stored: string[] = [];
  try {
    const raw = globalThis.localStorage?.getItem(STORAGE_KEY);
    if (raw) {
      const parsed: unknown = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        stored = parsed.filter(
          (item): item is string => typeof item === "string" && item.length > 0,
        );
      }
    }
  } catch {
    // 壊れた値・プライベートモード。既定に戻すだけで機能は続く。
  }
  cache = stored.slice(0, MAX_REMEMBERED);
  return cache;
}

function write(next: string[]): void {
  cache = next;
  try {
    globalThis.localStorage?.setItem(STORAGE_KEY, JSON.stringify(next));
  } catch {
    // 書けなくてもこのセッションの並びは効く。
  }
  for (const listener of listeners) listener();
}

export function recentEmojis(): string[] {
  return read();
}

/** 使った絵文字を先頭へ。同じものは重複させず前へ繰り上げる。 */
export function noteEmojiUsed(emoji: string): void {
  const current = read();
  const next = [emoji, ...current.filter((item) => item !== emoji)].slice(
    0,
    MAX_REMEMBERED,
  );
  if (
    next.length === current.length &&
    next.every((v, i) => v === current[i])
  ) {
    return;
  }
  write(next);
}

/** テスト用。保存も購読も残さず初期状態へ戻す。 */
export function resetRecentEmojis(): void {
  try {
    globalThis.localStorage?.removeItem(STORAGE_KEY);
  } catch {
    // 消せなくてもキャッシュは捨てる。
  }
  write([]);
}

export function subscribeRecentEmojis(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useRecentEmojis(): string[] {
  return useSyncExternalStore(
    subscribeRecentEmojis,
    recentEmojis,
    recentEmojis,
  );
}

/**
 * 操作チップへ直接出す3つ。まだ何も押していない人にも押せるものを出したいので、
 * 足りない分は既定の3つで埋める。
 */
export function topRecentEmojis(recent: string[], count = 3): string[] {
  const out = [...recent];
  for (const fallback of DEFAULT_RECENT_EMOJIS) {
    if (out.length >= count) break;
    if (!out.includes(fallback)) out.push(fallback);
  }
  return out.slice(0, count);
}
