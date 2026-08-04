/**
 * composer用の最小のコードフェンス判定。
 *
 * 「```」で開いたコードブロックを閉じる前にEnterを押しても送信せず、
 * 改行としてコードを書き続けられるようにする（Discordと同じ手触り）。
 * CommonMarkのフェンス（``` / ~~~、先頭インデント3つまで）を行単位で
 * 数えるだけの近似で、チャット入力にはこれで十分。
 */
const FENCE_LINE = /^ {0,3}(?:`{3,}|~{3,})/;

export function isInsideUnclosedCodeFence(
  value: string,
  caret: number,
): boolean {
  let open = false;
  for (const line of value.slice(0, caret).split("\n")) {
    if (FENCE_LINE.test(line)) open = !open;
  }
  return open;
}
