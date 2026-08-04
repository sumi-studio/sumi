import { Paperclip } from "lucide-react";
import type { Attachment } from "../model";
import { isImageAttachment } from "../model";

/**
 * 添付の表示。画像はその場で見えることに意味があるのでインラインに置き、
 * それ以外は「何が届いたか」が分かるファイルカードにする。
 * 原寸で見たいときは新規タブへ（サーバーは画像だけをinlineで配信する）。
 */

export function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const rounded = value < 10 ? value.toFixed(1) : String(Math.round(value));
  return `${rounded} ${units[unit]}`;
}

export function MessageAttachments({
  attachments,
}: {
  attachments: Attachment[];
}) {
  if (attachments.length === 0) return null;
  return (
    <div className="mt-1 flex flex-wrap items-start gap-2">
      {attachments.map((attachment) =>
        isImageAttachment(attachment) ? (
          <a
            key={attachment.attachmentId}
            href={attachment.url}
            target="_blank"
            rel="noreferrer"
            title={`${attachment.filename}・${formatFileSize(attachment.size)}`}
            className="block overflow-hidden rounded-lg border border-border"
          >
            <img
              src={attachment.url}
              alt={attachment.filename}
              className="max-h-80 max-w-full object-contain"
            />
          </a>
        ) : (
          <a
            key={attachment.attachmentId}
            href={attachment.url}
            target="_blank"
            rel="noreferrer"
            download={attachment.filename}
            className="flex max-w-full items-center gap-2 rounded-lg border border-border bg-muted/30 px-2.5 py-1.5 transition-colors hover:bg-accent"
          >
            <Paperclip className="size-3.5 shrink-0 text-muted-foreground" />
            <span className="truncate font-medium text-[12.5px]">
              {attachment.filename}
            </span>
            <span className="shrink-0 text-[11px] text-muted-foreground tabular-nums">
              {formatFileSize(attachment.size)}
            </span>
          </a>
        ),
      )}
    </div>
  );
}
