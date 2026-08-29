import { useEffect, useRef } from "react";
import { isImeComposing } from "../../lib/ime";
import { isInsideUnclosedCodeFence } from "../compose-fence";
import { claimEditFocus } from "../edit-focus";
import type { MemberProfile, ParticipantKey } from "../model";
import {
  MentionSuggestions,
  useMentionAutocomplete,
} from "./mention-autocomplete";
import { useImeCommittedTextarea } from "./use-ime-committed-textarea";

const MAX_HEIGHT_PX = 220;

/**
 * メッセージ本文のインライン編集欄。
 *
 * 編集は「その場」で起きる操作なので、画面下のcomposerへ視線と入力位置を
 * 飛ばさず、対象メッセージの本文位置に入力枠を出す。Enterで保存・Escで
 * 取消はcomposerと同じ手癖に揃え、IME変換中のEnter/Escは奪わない。
 *
 * 書きかけの本文はこの欄が持たない。仮想リストの行はスクロールで
 * アンマウントされるので、行ローカルのstateに置くと書きかけが消える。
 * 値と更新は編集セッションの持ち主（store）から渡ってくる。
 */
export function MessageEditor({
  value,
  onChange,
  onSubmit,
  onCancel,
  conflict,
  failure,
  savedWithPendingChanges,
  saving,
  openedToken,
  onReloadConflict,
  membersByKey,
  selfKey,
}: {
  value: string;
  onChange: (content: string) => void;
  onSubmit: () => void;
  onCancel: () => void;
  conflict: { content: string; revision: number } | null;
  failure?: string | null;
  savedWithPendingChanges?: boolean;
  saving: boolean;
  /**
   * 編集欄を開いた回（store の editSession.openedToken）。同じ回で行が
   * 再マウントされてもフォーカスを取り直さない。null なら自動フォーカスしない。
   */
  openedToken: number | null;
  onReloadConflict: () => void;
  membersByKey: Record<ParticipantKey, MemberProfile>;
  selfKey: ParticipantKey;
}) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const mentionAutocomplete = useMentionAutocomplete({
    value,
    onValueChange: onChange,
    inputRef: textareaRef,
    membersByKey,
    selfKey,
  });
  const ime = useImeCommittedTextarea(textareaRef);

  // 開いた瞬間に本文末尾へキャレットを置く（続きを書き足す方が多い）。
  // 「開いた瞬間」だけ。行は編集中でも仮想リストから外れて再マウントされるので、
  // マウントごとに focus() すると composer に打っている最中の caret を奪う。
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea || openedToken === null || !claimEditFocus(openedToken)) {
      return;
    }
    textarea.focus();
    const end = textarea.value.length;
    textarea.setSelectionRange(end, end);
  }, [openedToken]);

  // autogrow: composerと同じ挙動。上限を超えたらこの枠の中でスクロールする。
  // biome-ignore lint/correctness/useExhaustiveDependencies: 入力値の変化を高さ再計算のトリガーにする
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "auto";
    const next = Math.min(textarea.scrollHeight, MAX_HEIGHT_PX);
    textarea.style.height = `${next}px`;
    textarea.style.overflowY =
      textarea.scrollHeight > MAX_HEIGHT_PX ? "auto" : "hidden";
  }, [value]);

  const submit = () => {
    if (conflict || saving) return;
    const text = ime.committedValue(value);
    // compositionend が controlled state に届くより先でも、実値をsessionへ
    // 取り込んでから保存する。store更新は同期なので onSubmit はこの値を読む。
    onChange(text);
    // 空にするのは削除であって編集ではない。取消として扱う。
    if (!text.trim()) {
      onCancel();
      return;
    }
    onSubmit();
  };

  return (
    <div className="relative my-0.5">
      <MentionSuggestions
        autocomplete={mentionAutocomplete}
        className="absolute bottom-full left-0 z-10 mb-1 w-64 overflow-hidden rounded-lg border border-border bg-background shadow-md"
      />
      <textarea
        ref={textareaRef}
        value={value}
        aria-label="メッセージを編集"
        rows={1}
        onChange={mentionAutocomplete.onInputChange}
        onClick={mentionAutocomplete.onInputClick}
        onKeyUp={mentionAutocomplete.onKeyUp}
        onCompositionStart={ime.onCompositionStart}
        onCompositionEnd={(event) => {
          ime.onCompositionEnd();
          mentionAutocomplete.onCompositionEnd(event);
        }}
        onSelect={mentionAutocomplete.onSelectionChange}
        onKeyDown={(event) => {
          // IME変換中のEnter/Escは変換の操作。編集の操作として横取りしない。
          if (isImeComposing(event)) return;
          if (mentionAutocomplete.onKeyDown(event)) return;
          if (event.key === "Escape") {
            event.preventDefault();
            onCancel();
            return;
          }
          if (event.key === "Enter" && !event.shiftKey) {
            const caret = event.currentTarget.selectionStart ?? value.length;
            // 未閉鎖の```の中では改行のままにする（composerと同じ判断）。
            if (isInsideUnclosedCodeFence(value, caret)) return;
            event.preventDefault();
            submit();
          }
        }}
        className="block w-full resize-none rounded-lg border border-border bg-background px-2.5 py-1.5 text-[13.5px] leading-6 outline-none focus:border-ring/60"
      />
      <div className="mt-0.5 flex items-center gap-2 text-[11px] text-muted-foreground">
        {conflict ? (
          <>
            <span role="alert">別の場所で編集されました</span>
            <button
              type="button"
              onClick={onReloadConflict}
              className="rounded px-1.5 py-0.5 transition-colors hover:bg-accent hover:text-foreground"
            >
              新しい本文を読み込む
            </button>
          </>
        ) : saving ? (
          <span role="status">保存中…</span>
        ) : failure ? (
          <span role="alert">{failure}</span>
        ) : savedWithPendingChanges ? (
          <span role="status">保存済み。さらに未保存の変更があります。</span>
        ) : (
          <span>Escでキャンセル・Enterで保存</span>
        )}
        <button
          type="button"
          onClick={onCancel}
          className="rounded px-1.5 py-0.5 transition-colors hover:bg-accent hover:text-foreground"
        >
          キャンセル
        </button>
        <button
          type="button"
          onClick={submit}
          disabled={conflict !== null || saving}
          className="rounded bg-primary px-1.5 py-0.5 font-medium text-primary-foreground transition-opacity hover:opacity-90"
        >
          保存
        </button>
      </div>
    </div>
  );
}
