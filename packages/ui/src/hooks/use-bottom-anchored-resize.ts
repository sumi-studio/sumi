import { useCallback, useLayoutEffect, useRef } from "react";

interface ResizeAnchor {
  bottom: number;
  viewport: HTMLElement;
}

/**
 * 通常フロー内の要素を、画面上の下端を保ったままリサイズする。
 * capture()を状態変更の直前に呼ぶと、次のlayout effectで差分を補正する。
 */
export function useBottomAnchoredResize<T extends HTMLElement>() {
  const elementRef = useRef<T>(null);
  const resizeAnchorRef = useRef<ResizeAnchor | null>(null);

  const capture = useCallback(() => {
    const element = elementRef.current;
    const viewport = element?.closest<HTMLElement>(
      '[data-slot="message-scroller-viewport"]',
    );
    if (element && viewport) {
      resizeAnchorRef.current = {
        bottom: element.getBoundingClientRect().bottom,
        viewport,
      };
    }
  }, []);

  const restore = useCallback(() => {
    const element = elementRef.current;
    const anchor = resizeAnchorRef.current;
    if (!element || !anchor) {
      return;
    }

    const delta = element.getBoundingClientRect().bottom - anchor.bottom;
    if (Math.abs(delta) > 0.5) {
      anchor.viewport.scrollTop += delta;
    }
    resizeAnchorRef.current = null;
  }, []);

  useLayoutEffect(restore);

  return { elementRef, capture, restore };
}
