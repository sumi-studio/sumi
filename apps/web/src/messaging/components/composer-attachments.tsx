import {
  AttachmentAction,
  Attachment as AttachmentCard,
  AttachmentContent,
  AttachmentGroup,
  AttachmentMedia,
  AttachmentTitle,
} from "@sumi/ui/components/attachment";
import {
  Eye,
  EyeOff,
  FileArchive,
  FileAudio,
  File as FileIcon,
  FileImage,
  FileText,
  FileVideo,
  Loader2,
  Pencil,
  RotateCw,
  TriangleAlert,
  X,
} from "lucide-react";
import { useState } from "react";
import type { DraftAttachment } from "../draft-attachments";
import {
  attachmentFailureLabel,
  formatAttachmentSize,
} from "../draft-attachments";
import type { AttachmentDraftPatch } from "../model";
import { isInlineImageMime } from "../model";
import { AttachmentEditDialog } from "./attachment-edit-dialog";

/**
 * 送信前の添付。ファイル名の羅列ではなく中身の見えるカードで出す——送る前に
 * 確かめられることが、送信前の添付という状態の存在理由なので。画像は手元の
 * bytesから作ったサムネイル、それ以外は形式アイコンで「何のファイルか」を出す。
 *
 * PDFの先頭ページプレビューは描画エンジンを丸ごと抱えることになるので採らず、
 * 形式アイコンと拡張子で代替する。
 */

/** 拡張子（先頭の . は含まない、最大5文字）。無ければ空。 */
export function fileExtension(filename: string): string {
  const dot = filename.lastIndexOf(".");
  if (dot <= 0 || dot === filename.length - 1) return "";
  const extension = filename.slice(dot + 1).toUpperCase();
  return extension.length <= 5 ? extension : "";
}

function FormatIcon({ mime, filename }: { mime: string; filename: string }) {
  const type = mime.toLowerCase();
  const extension = fileExtension(filename);
  const Icon = isInlineImageMime(type)
    ? FileImage
    : type.startsWith("audio/")
      ? FileAudio
      : type.startsWith("video/")
        ? FileVideo
        : type.includes("zip") ||
            type.includes("tar") ||
            type.includes("compressed")
          ? FileArchive
          : type === "application/pdf" ||
              type.startsWith("text/") ||
              type.includes("document")
            ? FileText
            : FileIcon;
  return (
    <span className="flex size-full flex-col items-center justify-center gap-1 text-muted-foreground">
      <Icon className="size-6" />
      {extension ? (
        <span className="font-medium text-[10px] tracking-wide">
          {extension}
        </span>
      ) : null}
    </span>
  );
}

function statusLine(draft: DraftAttachment): string {
  if (draft.status === "failed") return attachmentFailureLabel(draft.errorCode);
  if (draft.status === "editing") return "保存中";
  return formatAttachmentSize(draft.sizeBytes);
}

/** 再送しても同じ結果にしかならない失敗には再送を出さない。 */
function retryable(draft: DraftAttachment): boolean {
  return (
    draft.status === "failed" &&
    draft.errorCode !== "attachment_too_large" &&
    draft.errorCode !== "attachment_empty"
  );
}

export function ComposerAttachments({
  drafts,
  onRemove,
  onRetry,
  onEdit,
}: {
  drafts: DraftAttachment[];
  onRemove: (clientNonce: string) => void;
  onRetry: (clientNonce: string) => void;
  onEdit: (clientNonce: string, patch: AttachmentDraftPatch) => void;
}) {
  const [editingNonce, setEditingNonce] = useState<string | null>(null);
  const editing = drafts.find((entry) => entry.clientNonce === editingNonce);
  if (drafts.length === 0) return null;
  return (
    <AttachmentGroup className="px-3 pt-2.5" data-testid="composer-attachments">
      {drafts.map((draft) => {
        const spoiler = draft.attachment?.spoiler ?? false;
        // 宣言は預かりが済んでから付ける。まだ受領が無いあいだは操作を出さない。
        const declarable = draft.attachment !== undefined;
        const busy = draft.status === "uploading" || draft.status === "editing";
        return (
          <AttachmentCard
            key={draft.clientNonce}
            orientation="vertical"
            state={draft.status === "failed" ? "error" : "done"}
            data-status={draft.status}
            className={
              draft.status === "failed"
                ? "border-rose-500/40 bg-rose-500/8"
                : undefined
            }
          >
            <AttachmentMedia className="aspect-4/3">
              {draft.previewUrl ? (
                <img
                  src={draft.previewUrl}
                  alt={`${draft.filename} のプレビュー`}
                  className={spoiler ? "blur-md" : undefined}
                />
              ) : (
                <FormatIcon
                  mime={draft.contentType}
                  filename={draft.filename}
                />
              )}
              {spoiler ? (
                <span className="absolute bottom-1 left-1 rounded-full bg-background/85 px-1.5 py-px font-medium text-[10px] text-muted-foreground">
                  ネタバレ
                </span>
              ) : null}
              {busy ? (
                <span className="absolute inset-0 flex items-center justify-center bg-background/60">
                  <Loader2 className="size-4 animate-spin text-muted-foreground" />
                </span>
              ) : null}
              {draft.status === "failed" ? (
                <span className="absolute inset-0 flex items-center justify-center bg-background/60 text-rose-600 dark:text-rose-400">
                  <TriangleAlert className="size-4" />
                </span>
              ) : null}
              {/* 操作は常に置いた上でホバーで前に出す。隠してしまうとキーボード
                  から辿れなくなるので、変えるのは見え方だけ。 */}
              <div className="absolute top-1 right-1 flex items-center gap-1">
                {declarable ? (
                  <AttachmentAction
                    aria-label={`${draft.filename}の${
                      spoiler ? "ネタバレを解除" : "ネタバレをマーク"
                    }`}
                    aria-pressed={spoiler}
                    title={spoiler ? "ネタバレを解除" : "ネタバレとしてマーク"}
                    disabled={busy}
                    onClick={() =>
                      onEdit(draft.clientNonce, { spoiler: !spoiler })
                    }
                    className={`bg-background/80 opacity-60 focus-visible:opacity-100 group-hover/attachment:opacity-100 ${
                      spoiler ? "opacity-100" : ""
                    }`}
                  >
                    {spoiler ? <EyeOff /> : <Eye />}
                  </AttachmentAction>
                ) : null}
                {declarable ? (
                  <AttachmentAction
                    aria-label={`${draft.filename}を編集`}
                    title="名前と説明を編集"
                    disabled={busy}
                    onClick={() => setEditingNonce(draft.clientNonce)}
                    className="bg-background/80 opacity-60 focus-visible:opacity-100 group-hover/attachment:opacity-100"
                  >
                    <Pencil />
                  </AttachmentAction>
                ) : null}
                {retryable(draft) ? (
                  <AttachmentAction
                    aria-label={`${draft.filename}を再送`}
                    title="もう一度送る"
                    onClick={() => onRetry(draft.clientNonce)}
                    className="bg-background/80 opacity-60 focus-visible:opacity-100 group-hover/attachment:opacity-100"
                  >
                    <RotateCw />
                  </AttachmentAction>
                ) : null}
                <AttachmentAction
                  aria-label={`${draft.filename}を外す`}
                  title="添付を取り消す"
                  onClick={() => onRemove(draft.clientNonce)}
                  className="bg-background/80 opacity-60 hover:text-rose-600 focus-visible:opacity-100 group-hover/attachment:opacity-100 dark:hover:text-rose-400"
                >
                  <X />
                </AttachmentAction>
              </div>
            </AttachmentMedia>
            <AttachmentContent>
              <AttachmentTitle title={draft.filename}>
                {draft.filename}
              </AttachmentTitle>
              <span
                className={`block truncate tabular-nums ${
                  draft.status === "failed"
                    ? "text-rose-600 dark:text-rose-400"
                    : "text-muted-foreground"
                }`}
              >
                {statusLine(draft)}
              </span>
              {draft.attachment?.alt ? (
                <span
                  className="block truncate text-muted-foreground"
                  title={draft.attachment.alt}
                >
                  {draft.attachment.alt}
                </span>
              ) : null}
            </AttachmentContent>
          </AttachmentCard>
        );
      })}
      {editing?.attachment ? (
        <AttachmentEditDialog
          filename={editing.filename}
          alt={editing.attachment.alt}
          spoiler={editing.attachment.spoiler}
          previewUrl={editing.previewUrl}
          onCancel={() => setEditingNonce(null)}
          onApply={(patch) => {
            setEditingNonce(null);
            if (Object.keys(patch).length > 0) {
              onEdit(editing.clientNonce, patch);
            }
          }}
        />
      ) : null}
    </AttachmentGroup>
  );
}
