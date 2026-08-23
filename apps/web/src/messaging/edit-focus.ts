/**
 * 編集欄の初回フォーカスを「編集欄を開いた回」ごとに一度に限る。
 *
 * 仮想リストの行は編集中でもアンマウント・再マウントされる。マウントのたびに
 * focus() すると、編集を開いたまま長くスクロールして行が描画窓へ戻った瞬間に、
 * composer などへ打っている最中の caret を編集欄が奪う。
 *
 * 「この回はもう focus した」は行の中には持てない（行ごと消える）ので、
 * message-action-lock と同じく行の外の小さな外部状態に置く。鍵は store の
 * `editSession.openedToken`——startEdit / reloadEditConflict だけが進め、
 * 保存の送受で派生する session は引き継ぐ。
 */

let focusedOpenedToken: number | null = null;

/** この回でまだ focus していなければ true を返し、以後の呼び出しは false。 */
export function claimEditFocus(openedToken: number): boolean {
  if (focusedOpenedToken === openedToken) return false;
  focusedOpenedToken = openedToken;
  return true;
}

/** テスト用。 */
export function resetEditFocus(): void {
  focusedOpenedToken = null;
}
