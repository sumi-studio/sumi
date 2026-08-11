import {
  type RefCallback,
  type RefObject,
  useCallback,
  useEffect,
  useRef,
} from "react";
import { isImeComposing } from "../../lib/ime";
import { applyUserScrollDelta } from "../../lib/user-scroll-intent";

/** 開いているオーバーレイの「閉じる」手続き。排他のために 1 か所へ集める。 */
const openOverlays = new Set<() => void>();

/** オーバーレイ上のホイールを渡す既定のメッセージ一覧。 */
export function conversationViewport(): HTMLElement | null {
  return document.querySelector<HTMLElement>(
    '[data-slot="conversation-viewport"]',
  );
}

/** その要素がこの方向のホイールをまだ消費できるか。 */
function consumesWheel(element: Element, deltaY: number, deltaX: number) {
  const style = window.getComputedStyle(element);
  if (deltaY !== 0 && /(auto|scroll|overlay)/.test(style.overflowY)) {
    const room = element.scrollHeight - element.clientHeight;
    if (room > 1) {
      const atTop = element.scrollTop <= 0;
      const atBottom = element.scrollTop >= room - 1;
      if (!(deltaY < 0 && atTop) && !(deltaY > 0 && atBottom)) return true;
    }
  }
  if (deltaX !== 0 && /(auto|scroll|overlay)/.test(style.overflowX)) {
    const room = element.scrollWidth - element.clientWidth;
    if (room > 1) {
      const atStart = element.scrollLeft <= 0;
      const atEnd = element.scrollLeft >= room - 1;
      if (!(deltaX < 0 && atStart) && !(deltaX > 0 && atEnd)) return true;
    }
  }
  return false;
}

function forwardWheel(
  event: WheelEvent,
  host: HTMLElement,
  resolveTarget: () => HTMLElement | null,
) {
  // ピンチズーム（ctrl+wheel）はブラウザに任せる。
  if (event.defaultPrevented || event.ctrlKey) return;
  let node = event.target instanceof Element ? event.target : null;
  while (node && host.contains(node)) {
    if (consumesWheel(node, event.deltaY, event.deltaX)) return;
    node = node.parentElement;
  }
  const target = resolveTarget();
  if (!target) return;
  event.preventDefault();
  const scaleX =
    event.deltaMode === WheelEvent.DOM_DELTA_LINE
      ? 16
      : event.deltaMode === WheelEvent.DOM_DELTA_PAGE
        ? target.clientWidth
        : 1;
  const scaleY =
    event.deltaMode === WheelEvent.DOM_DELTA_LINE
      ? 16
      : event.deltaMode === WheelEvent.DOM_DELTA_PAGE
        ? target.clientHeight
        : 1;
  applyUserScrollDelta(target, {
    left: event.deltaX * scaleX,
    top: event.deltaY * scaleY,
  });
}

/** portal 越しのパネルから下のスクロール領域へホイールを渡す ref。 */
export function useWheelPassthrough<T extends HTMLElement = HTMLElement>(
  resolveTarget: () => HTMLElement | null = conversationViewport,
): RefCallback<T> {
  const resolveRef = useRef(resolveTarget);
  resolveRef.current = resolveTarget;
  return useCallback((node: T | null) => {
    if (!node) return;
    const onWheel = (event: WheelEvent) =>
      forwardWheel(event, node, () => resolveRef.current());
    node.addEventListener("wheel", onWheel, { passive: false });
    return () => node.removeEventListener("wheel", onWheel);
  }, []);
}

export interface OverlayPanelOptions {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** パネル上のホイールを渡す先。既定はメッセージ一覧。 */
  scrollPassthrough?: () => HTMLElement | null;
}

export interface OverlayPanelBindings<T extends HTMLElement> {
  open: boolean;
  triggerRef: RefObject<T | null>;
  panelRef: RefObject<HTMLDivElement | null>;
  triggerProps: {
    ref: RefCallback<T>;
    "aria-expanded": boolean;
    onClick: () => void;
  };
  panelProps: { ref: RefCallback<HTMLDivElement> };
  toggle: () => void;
  close: () => void;
}

export function useOverlayPanel<T extends HTMLElement = HTMLButtonElement>({
  open,
  onOpenChange,
  scrollPassthrough = conversationViewport,
}: OverlayPanelOptions): OverlayPanelBindings<T> {
  const triggerRef = useRef<T | null>(null);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const onOpenChangeRef = useRef(onOpenChange);
  onOpenChangeRef.current = onOpenChange;
  const passthroughRef = useRef(scrollPassthrough);
  passthroughRef.current = scrollPassthrough;
  const restoreFocusRef = useRef(true);
  const focusInsideRef = useRef(false);
  const wasOpenRef = useRef(false);

  const requestClose = useCallback((restoreFocus: boolean) => {
    restoreFocusRef.current = restoreFocus;
    onOpenChangeRef.current(false);
  }, []);

  const close = useCallback(() => requestClose(true), [requestClose]);
  const toggle = useCallback(() => {
    if (open) requestClose(true);
    else onOpenChangeRef.current(true);
  }, [open, requestClose]);

  // 新しいオーバーレイを開いたら先に開いていたものを閉じる。
  useEffect(() => {
    if (!open) return;
    const closeSelf = () => requestClose(false);
    const others = [...openOverlays];
    openOverlays.clear();
    openOverlays.add(closeSelf);
    for (const other of others) other();
    return () => {
      openOverlays.delete(closeSelf);
    };
  }, [open, requestClose]);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (!target) return;
      if (panelRef.current?.contains(target)) return;
      if (triggerRef.current?.contains(target)) return;
      requestClose(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || isImeComposing(event)) return;
      requestClose(true);
    };
    document.addEventListener("pointerdown", onPointerDown, true);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown, true);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open, requestClose]);

  useEffect(() => {
    if (!open) return;
    const inside = (node: Node | null) =>
      !!node &&
      (panelRef.current?.contains(node) === true ||
        triggerRef.current?.contains(node) === true);
    focusInsideRef.current = inside(document.activeElement);
    const onFocusIn = (event: FocusEvent) => {
      focusInsideRef.current = inside(event.target as Node | null);
    };
    document.addEventListener("focusin", onFocusIn);
    return () => document.removeEventListener("focusin", onFocusIn);
  }, [open]);

  useEffect(() => {
    if (open) {
      wasOpenRef.current = true;
      restoreFocusRef.current = true;
      return;
    }
    if (!wasOpenRef.current) return;
    wasOpenRef.current = false;
    if (!restoreFocusRef.current) return;
    const active = document.activeElement;
    const movedAway =
      active !== null && active !== document.body && !focusInsideRef.current;
    if (movedAway) return;
    triggerRef.current?.focus?.();
  }, [open]);

  const setTrigger = useCallback<RefCallback<T>>((node) => {
    triggerRef.current = node;
    return () => {
      triggerRef.current = null;
    };
  }, []);

  const setPanel = useCallback<RefCallback<HTMLDivElement>>((node) => {
    panelRef.current = node;
    if (!node) return;
    const onWheel = (event: WheelEvent) =>
      forwardWheel(event, node, () => passthroughRef.current());
    node.addEventListener("wheel", onWheel, { passive: false });
    return () => {
      node.removeEventListener("wheel", onWheel);
      panelRef.current = null;
    };
  }, []);

  return {
    open,
    triggerRef,
    panelRef,
    triggerProps: { ref: setTrigger, "aria-expanded": open, onClick: toggle },
    panelProps: { ref: setPanel },
    toggle,
    close,
  };
}
