import type { KeyboardEvent as ReactKeyboardEvent } from "react";

/**
 * IME変換中のキー入力か。
 *
 * 日本語入力では変換を確定するのに Enter を押す。その Enter は「確定」だけの
 * 意味で、送信・保存・タグ追加のような別の意味を持たせてはいけない。
 * isComposing が立たないブラウザ向けに keyCode 229（変換中の合図）も見る。
 */
export function isImeComposing(
  event: ReactKeyboardEvent | KeyboardEvent,
): boolean {
  const native = "nativeEvent" in event ? event.nativeEvent : event;
  return native.isComposing || native.keyCode === 229;
}
