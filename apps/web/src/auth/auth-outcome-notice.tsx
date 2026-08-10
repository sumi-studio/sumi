import { Check, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { AuthOutcomeNotice as AuthOutcomeNoticeState } from "./auth-outcome-notice-state";

export const authOutcomeNoticeExitMilliseconds = 160;

const minimumReadingMilliseconds = 6_000;
const maximumReadingMilliseconds = 30_000;
const orientationMilliseconds = 2_000;
const millisecondsPerVisibleCharacter = 120;

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
      copy={authOutcomeNoticeCopy(notice)}
      onDismiss={onDismiss}
    />
  );
}

function AuthOutcomeNoticeContent({
  copy,
  onDismiss,
}: {
  copy: string;
  onDismiss: () => void;
}) {
  const [isExiting, setIsExiting] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const [hasFocusWithin, setHasFocusWithin] = useState(false);
  const [activePointerId, setActivePointerId] = useState<number | null>(null);
  const [pageReadable, setPageReadable] = useState(isPageReadable);
  const onDismissRef = useRef(onDismiss);
  const surfaceRef = useRef<HTMLDivElement>(null);

  onDismissRef.current = onDismiss;

  useEffect(() => {
    const syncPageReadable = () => {
      const readable = isPageReadable();
      setPageReadable(readable);
      if (!readable) setActivePointerId(null);
    };
    const pauseForBlur = () => {
      setPageReadable(false);
      setActivePointerId(null);
    };
    const finishPointer = (event: globalThis.PointerEvent) => {
      setActivePointerId((active) =>
        active === event.pointerId ? null : active,
      );
    };
    const surface = surfaceRef.current;
    const handlePointerEnter = (event: globalThis.PointerEvent) => {
      if (event.pointerType === "mouse") setIsHovered(true);
    };
    const handlePointerLeave = (event: globalThis.PointerEvent) => {
      if (event.pointerType === "mouse") setIsHovered(false);
    };
    const handlePointerDown = (event: globalThis.PointerEvent) => {
      setActivePointerId((active) => active ?? event.pointerId);
    };
    const handleFocusIn = () => setHasFocusWithin(true);
    const handleFocusOut = (event: globalThis.FocusEvent) => {
      if (!surface || !containsRelatedTarget(surface, event.relatedTarget)) {
        setHasFocusWithin(false);
      }
    };

    document.addEventListener("visibilitychange", syncPageReadable);
    window.addEventListener("focus", syncPageReadable);
    window.addEventListener("blur", pauseForBlur);
    window.addEventListener("pointerup", finishPointer);
    window.addEventListener("pointercancel", finishPointer);
    window.addEventListener("lostpointercapture", finishPointer);
    surface?.addEventListener("pointerenter", handlePointerEnter);
    surface?.addEventListener("pointerleave", handlePointerLeave);
    surface?.addEventListener("pointerdown", handlePointerDown);
    surface?.addEventListener("focusin", handleFocusIn);
    surface?.addEventListener("focusout", handleFocusOut);

    return () => {
      document.removeEventListener("visibilitychange", syncPageReadable);
      window.removeEventListener("focus", syncPageReadable);
      window.removeEventListener("blur", pauseForBlur);
      window.removeEventListener("pointerup", finishPointer);
      window.removeEventListener("pointercancel", finishPointer);
      window.removeEventListener("lostpointercapture", finishPointer);
      surface?.removeEventListener("pointerenter", handlePointerEnter);
      surface?.removeEventListener("pointerleave", handlePointerLeave);
      surface?.removeEventListener("pointerdown", handlePointerDown);
      surface?.removeEventListener("focusin", handleFocusIn);
      surface?.removeEventListener("focusout", handleFocusOut);
    };
  }, []);

  useEffect(() => {
    if (
      isExiting ||
      isHovered ||
      hasFocusWithin ||
      activePointerId !== null ||
      !pageReadable
    ) {
      return;
    }
    const timeout = window.setTimeout(
      () => setIsExiting(true),
      authOutcomeNoticeReadingMilliseconds(copy),
    );
    return () => window.clearTimeout(timeout);
  }, [
    activePointerId,
    copy,
    hasFocusWithin,
    isExiting,
    isHovered,
    pageReadable,
  ]);

  useEffect(() => {
    if (!isExiting) return;
    const timeout = window.setTimeout(
      () => onDismissRef.current(),
      authOutcomeNoticeExitMilliseconds,
    );
    return () => window.clearTimeout(timeout);
  }, [isExiting]);

  return (
    <div
      ref={surfaceRef}
      data-testid="auth-outcome-notice"
      data-exiting={isExiting || undefined}
      className={`fixed inset-x-4 top-4 z-50 mx-auto flex max-w-md items-center gap-2.5 rounded-md border border-border/70 bg-background/95 px-3 py-2.5 text-[13px] leading-5 shadow-sm backdrop-blur-sm transition-opacity duration-150 ease-out motion-reduce:transition-none ${
        isExiting ? "pointer-events-none opacity-0" : "opacity-100"
      }`}
    >
      <Check
        className="size-4 shrink-0 text-emerald-600 dark:text-emerald-500"
        aria-hidden="true"
      />
      <span className="min-w-0 flex-1">{copy}</span>
      <button
        type="button"
        onClick={() => setIsExiting(true)}
        aria-label="通知を閉じる"
        className="-mr-1 shrink-0 rounded-sm p-1 text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <X className="size-4" aria-hidden="true" />
      </button>
    </div>
  );
}

export function authOutcomeNoticeReadingMilliseconds(copy: string): number {
  // Give every notice an orientation window, then extend it for the actual
  // terminal-outcome copy. The cap keeps this status transient.
  const visibleCharacters = Array.from(copy).filter(
    (character) => !/\s/u.test(character),
  ).length;
  return Math.min(
    maximumReadingMilliseconds,
    Math.max(
      minimumReadingMilliseconds,
      orientationMilliseconds +
        visibleCharacters * millisecondsPerVisibleCharacter,
    ),
  );
}

function isPageReadable(): boolean {
  return document.visibilityState === "visible" && document.hasFocus();
}

function containsRelatedTarget(
  container: HTMLElement,
  relatedTarget: EventTarget | null,
): boolean {
  return relatedTarget instanceof Node && container.contains(relatedTarget);
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
