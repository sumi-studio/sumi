import { Check, X } from "lucide-react";
import { useEffect } from "react";
import type { AuthOutcomeNotice as AuthOutcomeNoticeState } from "./auth-outcome-notice-state";

// Outcome notices are useful confirmation, not persistent status. Keep them
// long enough to read the transition copy while avoiding a notification that
// blocks the workspace until it is manually closed.
export const authOutcomeNoticeAutoDismissMilliseconds = 5_000;

export function AuthOutcomeNotice({
  notice,
  onDismiss,
}: {
  notice: AuthOutcomeNoticeState;
  onDismiss: () => void;
}) {
  const noticeReceiptId = notice.receiptId;

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      // Keep the timer scoped to this terminal receipt. A new notice should
      // get a fresh reading window even when the component remains mounted.
      onDismiss();
    }, authOutcomeNoticeAutoDismissMilliseconds);
    return () => window.clearTimeout(timeoutId);
  }, [noticeReceiptId, onDismiss]);

  return (
    <div
      role="status"
      className="fixed inset-x-4 top-4 z-50 mx-auto flex max-w-md items-center gap-2 rounded-lg border bg-background px-3 py-2.5 text-sm shadow-sm"
    >
      <Check className="size-4 shrink-0 text-emerald-600" aria-hidden="true" />
      <span className="flex-1">{authOutcomeNoticeCopy(notice)}</span>
      <button
        type="button"
        onClick={onDismiss}
        aria-label="通知を閉じる"
        className="rounded-sm p-1 text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <X className="size-4" aria-hidden="true" />
      </button>
    </div>
  );
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
