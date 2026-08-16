import { MessagingAPIError } from "./api-backend";
import type { Attachment } from "./model";

export type DraftAttachmentStatus = "uploading" | "ready" | "failed";

/**
 * composerに積まれた1ファイル。clientNonceはファイルごとに安定で、再送は
 * 同じnonceで同じ受領を得る。bytesはstoreのモジュール内にあり、ここには
 * 表示と送信判断に要るものだけを置く。
 */
export interface DraftAttachment {
  clientNonce: string;
  filename: string;
  sizeBytes: number;
  contentType: string;
  status: DraftAttachmentStatus;
  errorCode?: string;
  /** upload完了後の受領。sendMessageのattachmentsへこのIDを順に載せる。 */
  attachment?: Attachment;
}

export function attachmentUploadFailureCode(error: unknown): string {
  if (error instanceof MessagingAPIError) return error.code;
  if (error instanceof DOMException && error.name === "TimeoutError") {
    return "upload_timeout";
  }
  return "attachment_upload_failed";
}

const FAILURE_LABELS: Record<string, string> = {
  attachment_too_large: "20 MiBを超えています",
  attachment_empty: "空のファイルは送れません",
  attachment_quota_exceeded: "ワークスペースの添付容量が上限です",
  attachment_draft_limit: "未送信の添付が多すぎます",
  attachment_upload_conflict: "同じファイルの別内容が先に送られています",
  attachment_upload_expired:
    "アップロードの予約が切れました。もう一度お試しください",
  attachment_upload_retired:
    "削除済みのファイルです。新しく添付し直してください",
  attachment_upload_in_progress:
    "同じファイルをアップロード中です。少し待ってから再試行してください",
  attachments_unavailable: "このサーバーでは添付を受け付けていません",
  invalid_session: "サインインし直してください",
  app_disabled: "Messagingが無効化されています",
  upload_timeout: "時間内に送り切れませんでした",
};

export function attachmentFailureLabel(code: string | undefined): string {
  return (code && FAILURE_LABELS[code]) || "アップロードに失敗しました";
}

export function formatAttachmentSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024)
    return `${(bytes / 1024).toFixed(bytes < 10240 ? 1 : 0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
