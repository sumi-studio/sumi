import { FirebaseError } from "firebase/app";
import { AuthAPIError } from "./session-client";

const firebaseErrorMessages: Record<string, string> = {
  "auth/account-exists-with-different-credential":
    "このメールアドレスは別のログイン方法で登録されています。",
  "auth/invalid-credential": "ログイン情報を確認して、もう一度お試しください。",
  "auth/operation-not-allowed": "このログイン方法は現在利用できません。",
  "auth/popup-blocked":
    "ポップアップがブロックされました。ブラウザの設定を確認してください。",
  "auth/popup-closed-by-user": "ログインがキャンセルされました。",
  "auth/network-request-failed":
    "通信できませんでした。接続を確認して、もう一度お試しください。",
  "auth/too-many-requests":
    "試行回数が多すぎます。しばらく時間をおいてください。",
  "auth/user-disabled": "このアカウントは現在利用できません。",
};

export function getAuthErrorMessage(error: unknown): string {
  if (error instanceof FirebaseError) {
    return (
      firebaseErrorMessages[error.code] ??
      "Firebase ログインを完了できませんでした。"
    );
  }
  if (error instanceof AuthAPIError) {
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
