import { Check, X } from "lucide-react";
import {
  type CSSProperties,
  type PointerEvent,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import "./auth-outcome-notice.css";
import type { AuthOutcomeNotice as AuthOutcomeNoticeState } from "./auth-outcome-notice-state";

// Outcome notices are useful confirmation, not persistent status. Keep them
// long enough to read the transition copy while avoiding a notification that
// blocks the workspace until it is manually closed.
export const authOutcomeNoticeAutoDismissMilliseconds = 3_000;
export const authOutcomeNoticeExitMilliseconds = 180;

const dismissDragDistancePixels = 72;
const dismissDragVelocityPixelsPerMillisecond = 0.65;

interface DragState {
  pointerId: number;
  startY: number;
  lastY: number;
  startTime: number;
  lastTime: number;
}

export function AuthOutcomeNotice({
  notice,
  onDismiss,
}: {
  notice: AuthOutcomeNoticeState;
  onDismiss: () => void;
}) {
  return (
    <AuthOutcomeNoticeContent
      key={notice.receiptId}
      notice={notice}
      onDismiss={onDismiss}
    />
  );
}

function AuthOutcomeNoticeContent({
  notice,
  onDismiss,
}: {
  notice: AuthOutcomeNoticeState;
  onDismiss: () => void;
}) {
  const [isExiting, setIsExiting] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [dragOffset, setDragOffset] = useState(0);
  const dismissTimeoutRef = useRef<number | null>(null);
  const exitTimeoutRef = useRef<number | null>(null);
  const onDismissRef = useRef(onDismiss);
  const exitingRef = useRef(false);
  const hoveringRef = useRef(false);
  const focusedRef = useRef(false);
  const pointerInteractingRef = useRef(false);
  const dragRef = useRef<DragState | null>(null);

  onDismissRef.current = onDismiss;

  const clearDismissTimeout = useCallback(() => {
    if (dismissTimeoutRef.current === null) return;
    window.clearTimeout(dismissTimeoutRef.current);
    dismissTimeoutRef.current = null;
  }, []);

  const clearExitTimeout = useCallback(() => {
    if (exitTimeoutRef.current === null) return;
    window.clearTimeout(exitTimeoutRef.current);
    exitTimeoutRef.current = null;
  }, []);

  const isInteracting = useCallback(
    () =>
      hoveringRef.current ||
      focusedRef.current ||
      pointerInteractingRef.current,
    [],
  );

  const beginExit = useCallback(() => {
    if (exitingRef.current) return;

    exitingRef.current = true;
    clearDismissTimeout();
    setIsDragging(false);
    setIsExiting(true);
    exitTimeoutRef.current = window.setTimeout(() => {
      exitTimeoutRef.current = null;
      onDismissRef.current();
    }, authOutcomeNoticeExitMilliseconds);
  }, [clearDismissTimeout]);

  const restartDismissTimer = useCallback(() => {
    clearDismissTimeout();
    if (exitingRef.current || isInteracting()) return;

    dismissTimeoutRef.current = window.setTimeout(
      beginExit,
      authOutcomeNoticeAutoDismissMilliseconds,
    );
  }, [beginExit, clearDismissTimeout, isInteracting]);

  useEffect(() => {
    // Each terminal receipt is keyed at the boundary above, so every mounted
    // notice begins with its own full reading window.
    exitingRef.current = false;
    hoveringRef.current = false;
    focusedRef.current = false;
    pointerInteractingRef.current = false;
    dragRef.current = null;
    setIsExiting(false);
    setIsDragging(false);
    setDragOffset(0);
    restartDismissTimer();

    return () => {
      clearDismissTimeout();
      clearExitTimeout();
    };
  }, [clearDismissTimeout, clearExitTimeout, restartDismissTimer]);

  const handlePointerEnter = (event: PointerEvent<HTMLDivElement>) => {
    if (event.pointerType === "touch") return;
    hoveringRef.current = true;
    clearDismissTimeout();
  };

  const handlePointerLeave = (event: PointerEvent<HTMLDivElement>) => {
    if (event.pointerType === "touch") return;
    hoveringRef.current = false;
    restartDismissTimer();
  };

  const handleFocus = () => {
    focusedRef.current = true;
    clearDismissTimeout();
  };

  const handleBlur = () => {
    focusedRef.current = false;
    restartDismissTimer();
  };

  const handlePointerDown = (event: PointerEvent<HTMLDivElement>) => {
    if (exitingRef.current) return;

    pointerInteractingRef.current = true;
    clearDismissTimeout();
    if (isNativeInteractiveTarget(event.target)) return;

    const time = pointerEventTime(event);
    dragRef.current = {
      pointerId: event.pointerId,
      startY: event.clientY,
      lastY: event.clientY,
      startTime: time,
      lastTime: time,
    };
    setIsDragging(true);
    if (typeof event.currentTarget.setPointerCapture === "function") {
      event.currentTarget.setPointerCapture(event.pointerId);
    }
  };

  const handlePointerMove = (event: PointerEvent<HTMLDivElement>) => {
    const drag = dragRef.current;
    if (!drag || drag.pointerId !== event.pointerId || exitingRef.current)
      return;

    const nextOffset = Math.min(0, event.clientY - drag.startY);
    drag.lastY = event.clientY;
    drag.lastTime = pointerEventTime(event);
    setDragOffset(nextOffset);
  };

  const finishPointerInteraction = (
    event: PointerEvent<HTMLDivElement>,
    cancelled: boolean,
  ) => {
    const drag = dragRef.current;
    pointerInteractingRef.current = false;
    if (!drag || drag.pointerId !== event.pointerId) {
      restartDismissTimer();
      return;
    }

    dragRef.current = null;
    setIsDragging(false);
    if (
      typeof event.currentTarget.hasPointerCapture === "function" &&
      event.currentTarget.hasPointerCapture(event.pointerId)
    ) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }

    drag.lastY = event.clientY;
    drag.lastTime = pointerEventTime(event);
    const offset = Math.min(0, drag.lastY - drag.startY);
    const elapsed = drag.lastTime - drag.startTime;
    const velocity = elapsed > 0 ? offset / elapsed : 0;
    const shouldDismiss =
      !cancelled &&
      (offset <= -dismissDragDistancePixels ||
        velocity <= -dismissDragVelocityPixelsPerMillisecond);

    if (shouldDismiss) {
      beginExit();
      return;
    }

    setDragOffset(0);
    restartDismissTimer();
  };

  return (
    <div
      role="status"
      data-testid="auth-outcome-notice"
      data-exiting={isExiting || undefined}
      data-dragging={isDragging || undefined}
      className="auth-outcome-notice fixed inset-x-4 top-4 z-50 mx-auto flex max-w-md items-center gap-2 rounded-lg border bg-background px-3 py-2.5 text-sm shadow-sm"
      style={
        {
          "--auth-outcome-notice-drag-y": `${dragOffset}px`,
        } as CSSProperties
      }
      onPointerEnter={handlePointerEnter}
      onPointerLeave={handlePointerLeave}
      onPointerDown={handlePointerDown}
      onPointerMove={handlePointerMove}
      onPointerUp={(event) => finishPointerInteraction(event, false)}
      onPointerCancel={(event) => finishPointerInteraction(event, true)}
      onFocus={handleFocus}
      onBlur={handleBlur}
      onClick={restartDismissTimer}
      onKeyDown={restartDismissTimer}
    >
      <Check className="size-4 shrink-0 text-emerald-600" aria-hidden="true" />
      <span className="flex-1">{authOutcomeNoticeCopy(notice)}</span>
      <button
        type="button"
        onClick={beginExit}
        aria-label="通知を閉じる"
        className="rounded-sm p-1 text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <X className="size-4" aria-hidden="true" />
      </button>
    </div>
  );
}

function isNativeInteractiveTarget(target: EventTarget | null): boolean {
  return (
    target instanceof Element &&
    target.closest("button, a, input, select, textarea, [role='button']") !==
      null
  );
}

function pointerEventTime(event: PointerEvent<HTMLElement>): number {
  return event.timeStamp > 0 ? event.timeStamp : performance.now();
}

export function authOutcomeNoticeCopy(notice: AuthOutcomeNoticeState): string {
  switch (notice.outcome) {
    case "account_created":
      return notice.intentTransition === "confirmed"
        ? "ログインから新規登録への変更を確認し、Sumiアカウントを作成しました。"
        : "Sumiアカウントを作成しました。";
    case "signed_in":
      return notice.intentTransition === "confirmed"
        ? "新規登録からログインへの変更を確認し、既存のSumiアカウントにログインしました。"
        : "Sumiにログインしました。";
    case "provider_linked":
      return notice.intentTransition === "recovery_proved"
        ? "新規登録を開始後、既存のSumiアカウントをメールで確認してログインし、選択したログイン方法を追加しました。"
        : "ログイン後、選択したログイン方法を追加しました。";
  }
}
