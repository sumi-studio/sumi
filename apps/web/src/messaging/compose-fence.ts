/**
 * composer用の最小のコードフェンス判定。
 *
 * 「```」で開いたコードブロックを閉じる前にEnterを押しても送信せず、
 * 改行としてコードを書き続けられるようにする（Discordと同じ手触り）。
 *
 * CommonMarkのfenced code blockに合わせ、closing fenceはopenerと
 * 同じ文字・opener以上の長さ・後続が空白のみの行だけとする。
 * 「```で開いて~~~を書く」「````で開いて```を書く」「```tsで閉じようとする」
 * はいずれも閉じないため、その位置のEnterは送信ではなく改行になる。
 */
const FENCE_LINE = /^ {0,3}(`{3,}|~{3,})(.*)$/;

interface OpenFence {
  char: string;
  length: number;
}

export function isInsideUnclosedCodeFence(
  value: string,
  caret: number,
): boolean {
  let open: OpenFence | null = null;
  for (const line of value.slice(0, caret).split("\n")) {
    const match = FENCE_LINE.exec(line);
    if (!match) continue;
    const marker = match[1] ?? "";
    const char = marker[0] ?? "";
    const rest = match[2] ?? "";
    if (open) {
      // closing fenceは同じ文字・opener以上の長さ・info stringなし。
      if (
        char === open.char &&
        marker.length >= open.length &&
        rest.trim() === ""
      ) {
        open = null;
      }
      continue;
    }
    // backtick fenceのinfo stringにbacktickは置けない（CommonMark）。
    if (char === "`" && rest.includes("`")) continue;
    open = { char, length: marker.length };
  }
  return open !== null;
}
