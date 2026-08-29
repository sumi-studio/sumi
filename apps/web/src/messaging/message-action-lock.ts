/**
 * 操作チップ・絵文字パレットの「対象メッセージの固定」。
 *
 * チップはホバーで出る。パレットを開いたまま別のメッセージへカーソルが
 * 移ると、CSSのホバーだけで制御している限りチップは移動先へ付いていき、
 * 開いているパレットがどのメッセージに効くのか分からなくなる。
 *
 * そこで「今どのメッセージがパネルを開いているか」を1か所に持ち、
 * 開いている間は他のメッセージのホバー表示を止める。閉じるまで対象は動かない。
 * 状態はReactのcontextではなく外部ストアにする（仮想リストの行は頻繁に
 * 生成・破棄され、providerの再描画で全行を巻き込みたくない）。
 */

import { useSyncExternalStore } from "react";

let lockedId: string | null = null;
const listeners = new Set<() => void>();

function emit(): void {
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function snapshot(): string | null {
  return lockedId;
}

/** パネルを開いたメッセージが対象を握る。 */
export function lockMessageActions(messageId: string): void {
  if (lockedId === messageId) return;
  lockedId = messageId;
  emit();
}

/** 自分が握っているときだけ手放す（別の行の開閉に横取りされない）。 */
export function releaseMessageActions(messageId: string): void {
  if (lockedId !== messageId) return;
  lockedId = null;
  emit();
}

export function lockedMessageId(): string | null {
  return lockedId;
}

/** テスト用。 */
export function resetMessageActionLock(): void {
  lockedId = null;
  emit();
}

export function useLockedMessageId(): string | null {
  return useSyncExternalStore(subscribe, snapshot, snapshot);
}
