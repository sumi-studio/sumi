import { useEffect, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { isInsideUnclosedCodeFence } from "../compose-fence";

const MAX_HEIGHT_PX = 220;

/**
 * メッセージ本文のインライン編集欄。
 *
 * 編集は「その場」で起きる操作なので、画面下のcomposerへ視線と入力位置を
 * 飛ばさず、対象メッセージの本文位置に入力枠を出す。Enterで保存・Escで
 * 取消はcomposerと同じ手癖に揃え、IME変換中のEnter/Escは奪わない。
 */
export function MessageEditor({
  initialValue,
  onSubmit,
  onCancel,
}: {
  initialValue: string;
  onSubmit: (content: string) => void;
  onCancel: () => void;
}) {
  const [value, setValue] = useState(initialValue);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // 開いた瞬間に本文末尾へキャレットを置く（続きを書き足す方が多い）。
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.focus();
    const end = textarea.value.length;
    textarea.setSelectionRange(end, end);
  }, []);

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
    const trimmed = value.trim();
    // 空にするのは削除であって編集ではない。取消として扱う。
    if (!trimmed) {
      onCancel();
      return;
    }
    onSubmit(trimmed);
  };

  return (
    <div className="my-0.5">
      <textarea
        ref={textareaRef}
        value={value}
        aria-label="メッセージを編集"
        rows={1}
        onChange={(event) => setValue(event.target.value)}
        onKeyDown={(event) => {
          // IME変換中のEnter/Escは変換の操作。編集の操作として横取りしない。
          if (isImeComposing(event)) return;
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
        <span>Escでキャンセル・Enterで保存</span>
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
          className="rounded bg-primary px-1.5 py-0.5 font-medium text-primary-foreground transition-opacity hover:opacity-90"
        >
          保存
        </button>
      </div>
    </div>
  );
}
