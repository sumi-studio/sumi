import { FirebaseError } from "firebase/app";
import {
  RedirectSignInAbandonedError,
  RedirectSignInExpiredError,
} from "./redirect-sign-in";
import { AuthAPIError } from "./session-client";

const firebaseErrorMessages: Record<string, string> = {
  "auth/account-exists-with-different-credential":
    "このメールアドレスは別のログイン方法で登録されています。",
  "auth/invalid-credential": "ログイン情報を確認して、もう一度お試しください。",
  "auth/operation-not-allowed": "このログイン方法は現在利用できません。",
  "auth/popup-blocked":
    "ポップアップがブロックされました。ブラウザの設定を確認してください。",
  "auth/popup-closed-by-user": "ログインがキャンセルされました。",
  "auth/redirect-cancelled-by-user": "ログインがキャンセルされました。",
  "auth/redirect-operation-pending":
    "別のログインを処理しています。少し待ってから、もう一度お試しください。",
  "auth/unauthorized-domain":
    "このドメインからはログインできません。別のURLからお試しください。",
  "auth/web-storage-unsupported":
    "ブラウザがログイン情報を保存できないため、ログインを完了できません。プライベートモードやCookieのブロックを解除してお試しください。",
  "auth/network-request-failed":
    "通信できませんでした。接続を確認して、もう一度お試しください。",
  "auth/too-many-requests":
    "試行回数が多すぎます。しばらく時間をおいてください。",
  "auth/user-disabled": "このアカウントは現在利用できません。",
};

export function getAuthErrorMessage(error: unknown): string {
  if (error instanceof RedirectSignInExpiredError) {
    return "ログインの有効期限が切れました。もう一度お試しください。";
  }
  if (error instanceof RedirectSignInAbandonedError) {
    return "ログインは完了しませんでした。キャンセルされたか、ブラウザがログイン状態を保持できませんでした。もう一度お試しください。";
  }
  if (error instanceof FirebaseError) {
    return (
      firebaseErrorMessages[error.code] ??
      "Firebase ログインを完了できませんでした。"
    );
  }
  if (error instanceof AuthAPIError) {
    const emailMessage = emailAuthErrorMessage(error);
    if (emailMessage) return emailMessage;
    if (error.status === 410) {
      // flow_expired: the provider return outlived the server-side flow TTL.
      return "ログインの有効期限が切れました。もう一度お試しください。";
    }
    if (error.status === 403) {
      return "このアカウントは Sumi の利用対象に登録されていません。";
    }
    if (error.status === 404 || error.status === 503) {
      return "Sumi のログイン機能は現在利用できません。";
    }
    return "Sumi のセッションを開始できませんでした。";
  }
  return "ログイン処理を完了できませんでした。もう一度お試しください。";
}

const emailErrorMessages: Record<string, string> = {
  code_locked:
    "入力回数の上限に達しました。メール内のリンクを開くか、コードを再送信してください。",
  email_superseded:
    "新しいコードを送信済みです。最新のメールに記載されたコードを入力してください。",
  email_expired:
    "確認コードの有効期限が切れました。コードを再送信してください。",
  email_consumed: "このメールのコードとリンクはすでに使用されています。",
  link_invalid:
    "このリンクは無効です。最新のメールのリンクを開くか、コードを入力してください。",
  link_adoption_required: "このブラウザで続けるかを選択してください。",
  continued_in_other_browser:
    "このログインは別のブラウザで続行されました。こちらで続ける場合は、もう一度メールアドレスを入力してください。",
  email_unavailable: "メールでのログインは現在利用できません。",
  session_active:
    "別のアカウントでログインしています。切り替える場合は、現在のセッションを終了してください。",
  flow_consumed: "このログインはすでに完了しています。",
};

function emailAuthErrorMessage(error: AuthAPIError): string | null {
  if (error.message === "code_mismatch") {
    const remaining = error.details.attemptsRemaining;
    if (remaining === undefined) return "6桁の数字を入力してください。";
    return remaining > 0
      ? `確認コードが正しくありません。あと${remaining}回入力できます。`
      : "確認コードが正しくありません。入力回数の上限に達したため、メール内のリンクを開くか、コードを再送信してください。";
  }
  if (error.message === "email_send_limited") {
    const retryAt = error.details.retryAt
      ? new Date(error.details.retryAt)
      : null;
    const stillUsable =
      "最新のメールのリンクは有効期限内なら使えます。コードは送信を始めた画面で入力してください。";
    return retryAt && Number.isFinite(retryAt.getTime())
      ? `このメールアドレスへの送信回数が上限に達しました。${formatRetryTime(retryAt)}以降にもう一度お試しください。${stillUsable}`
      : `このメールアドレスへの送信回数が上限に達しました。しばらくしてからもう一度お試しください。${stillUsable}`;
  }
  if (error.message === "email_unverified_account") {
    const providers = error.details.signInProviders;
    if (!providers) {
      return "このメールアドレスは確認済みではないため、メールではログインできません。以前使用したGoogleまたはGitHubでログインしてください。";
    }
    if (providers.length === 0) {
      return "このアカウントはメールアドレスが確認済みではないため、メールではログインできません。連携済みのGoogleやGitHubも見つからないため、管理者にお問い合わせください。";
    }
    const names = providers
      .map((provider) => (provider === "google.com" ? "Google" : "GitHub"))
      .join("または");
    return `このアカウントはメールアドレスが確認済みではないため、メールではログインできません。このアカウントに連携済みの${names}でログインしてください。`;
  }
  return emailErrorMessages[error.message] ?? null;
}

function formatRetryTime(date: Date): string {
  const seconds = Math.ceil((date.getTime() - Date.now()) / 1000);
  if (seconds <= 90) return `${Math.max(1, seconds)}秒後`;
  return date.toLocaleTimeString("ja-JP", {
    hour: "2-digit",
    minute: "2-digit",
  });
}
