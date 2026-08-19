import { type RefObject, useCallback, useRef } from "react";

/**
 * IME中にボタンで送信・保存する経路でも、textareaにすでに反映された実値を
 * 使う。blur は Safari / ソフトキーボードに変換を確定させるための境界であり、
 * state の更新を待たずに直後の DOM 値を読む。
 */
export function useImeCommittedTextarea(
  textareaRef: RefObject<HTMLTextAreaElement | null>,
) {
  const composing = useRef(false);

  const onCompositionStart = useCallback(() => {
    composing.current = true;
  }, []);

  const onCompositionEnd = useCallback(() => {
    composing.current = false;
  }, []);

  const committedValue = useCallback(
    (fallback: string) => {
      if (composing.current) {
        composing.current = false;
        textareaRef.current?.blur();
      }
      return textareaRef.current?.value ?? fallback;
    },
    [textareaRef],
  );

  return { onCompositionStart, onCompositionEnd, committedValue };
}
