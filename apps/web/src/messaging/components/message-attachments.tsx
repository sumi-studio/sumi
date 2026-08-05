import { EyeOff, Paperclip } from "lucide-react";
import { useState } from "react";
import type { Attachment } from "../model";
import { isImageAttachment } from "../model";
import { ImageViewer } from "./image-viewer";

/**
 * 添付の表示。画像はその場で見えることに意味があるのでインラインに置き、
 * それ以外は「何が届いたか」が分かるファイルカードにする。
 * 画像を大きく見たいときはアプリ内のビューアーを開く（会話から離れない）。
 * 画像以外は新規タブへ（サーバーはそれをdownloadとして配信する）。
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

/**
 * ネタバレ付きの添付。送り手が「先に中身を見せない」と言ったものは、
 * 受け手が開ける操作をするまで隠す。概要（alt）はぼかしの上でも読める:
 * 何かは分かった上で、見るかどうかを受け手が決められるようにする。
 */
function SpoilerCover({
  alt,
  onReveal,
}: {
  alt: string;
  onReveal: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onReveal}
      className="absolute inset-0 flex flex-col items-center justify-center gap-1 bg-background/40 backdrop-blur-none transition-colors hover:bg-background/25"
    >
      <span className="flex items-center gap-1 rounded-full bg-background/90 px-2.5 py-1 font-medium text-[11px] shadow-xs">
        <EyeOff className="size-3" />
        ネタバレ
      </span>
      {alt ? (
        <span className="max-w-[80%] truncate text-[11px] text-foreground/70">
          {alt}
        </span>
      ) : null}
    </button>
  );
}

export function MessageAttachments({
  attachments,
  authorName,
  createdAt,
}: {
  attachments: Attachment[];
  /** ビューアーの左上に出す投稿者名と時刻。無ければ出さない。 */
  authorName?: string;
  createdAt?: number;
}) {
  const [viewing, setViewing] = useState<string | null>(null);
  const [revealed, setRevealed] = useState<string[]>([]);
  if (attachments.length === 0) return null;
  const viewed = attachments.find(
    (attachment) => attachment.attachmentId === viewing,
  );
  return (
    <div className="mt-1 flex flex-wrap items-start gap-2">
      {attachments.map((attachment) =>
        isImageAttachment(attachment) ? (
          <span
            key={attachment.attachmentId}
            className="relative block overflow-hidden rounded-lg border border-border"
          >
            <button
              type="button"
              onClick={() => setViewing(attachment.attachmentId)}
              title={`${attachment.filename}・${formatFileSize(attachment.size)}`}
              aria-label={`${attachment.filename} を開く`}
              className="block cursor-zoom-in transition-colors hover:border-ring/60"
            >
              <img
                src={attachment.url}
                alt={attachment.alt || attachment.filename}
                className={`max-h-80 max-w-full object-contain ${
                  attachment.spoiler &&
                  !revealed.includes(attachment.attachmentId)
                    ? "blur-xl"
                    : ""
                }`}
              />
            </button>
            {attachment.spoiler &&
            !revealed.includes(attachment.attachmentId) ? (
              <SpoilerCover
                alt={attachment.alt}
                onReveal={() =>
                  setRevealed((current) => [
                    ...current,
                    attachment.attachmentId,
                  ])
                }
              />
            ) : null}
          </span>
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
            {attachment.spoiler ? (
              <span className="flex shrink-0 items-center gap-1 rounded-full bg-muted px-1.5 py-px text-[10px] text-muted-foreground">
                <EyeOff className="size-3" />
                ネタバレ
              </span>
            ) : null}
            <span className="shrink-0 text-[11px] text-muted-foreground tabular-nums">
              {formatFileSize(attachment.size)}
            </span>
          </a>
        ),
      )}
      {viewed ? (
        <ImageViewer
          attachment={viewed}
          authorName={authorName}
          createdAt={createdAt}
          onClose={() => setViewing(null)}
        />
      ) : null}
    </div>
  );
}
