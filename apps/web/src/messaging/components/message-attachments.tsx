import { Download, EyeOff, FileText, ImageOff } from "lucide-react";
import { useState } from "react";
import { formatAttachmentSize } from "../draft-attachments";
import type { Attachment } from "../model";
import { isInlineImageMime } from "../model";
import { useMessaging } from "../store";
import { ImageViewer } from "./image-viewer";

/**
 * メッセージが運ぶ添付の描画。画像はサーバーが安全と判定した型だけをinlineで
 * 出し、それ以外はファイルカードとしてダウンロードさせる。bytesのURLは常に
 * 現在のexact scopeから引く: 古いWorkspaceのURLを描画に残さない。
 *
 * 送り手がネタバレと宣言した添付は覆ったまま出す。開くのは受け手の操作で、
 * 開いたかどうかはどこにも記録しない（この描画だけの状態）。
 */
export function MessageAttachments({
  attachments,
  authorName,
  createdAt,
}: {
  attachments: Attachment[];
  authorName?: string;
  createdAt?: number;
}) {
  const attachmentURL = useMessaging((state) => state.attachmentURL);
  const [viewing, setViewing] = useState<Attachment | null>(null);
  if (attachments.length === 0) return null;
  const images = attachments.filter((entry) => isInlineImageMime(entry.mime));
  const files = attachments.filter((entry) => !isInlineImageMime(entry.mime));
  return (
    <div
      className="mt-1.5 flex flex-col gap-1.5"
      data-testid="message-attachments"
    >
      {images.length > 0 ? (
        <div className="flex flex-wrap gap-1.5">
          {images.map((entry) => (
            <ImageAttachment
              key={entry.attachmentId}
              attachment={entry}
              href={attachmentURL(entry.attachmentId)}
              onOpen={() => setViewing(entry)}
            />
          ))}
        </div>
      ) : null}
      {files.map((entry) => (
        <FileAttachment
          key={entry.attachmentId}
          attachment={entry}
          href={attachmentURL(entry.attachmentId)}
        />
      ))}
      {viewing ? (
        <ImageViewer
          attachment={viewing}
          href={attachmentURL(viewing.attachmentId)}
          authorName={authorName}
          createdAt={createdAt}
          onClose={() => setViewing(null)}
        />
      ) : null}
    </div>
  );
}

function ImageAttachment({
  attachment,
  href,
  onOpen,
}: {
  attachment: Attachment;
  href: string;
  onOpen: () => void;
}) {
  const [broken, setBroken] = useState(false);
  const [revealed, setRevealed] = useState(false);
  if (broken) {
    return <FileAttachment attachment={attachment} href={href} icon="image" />;
  }
  const covered = attachment.spoiler && !revealed;
  const label = attachment.alt || attachment.filename;
  return (
    <div className="relative w-fit max-w-full">
      <button
        type="button"
        onClick={() => (covered ? setRevealed(true) : onOpen())}
        aria-label={
          covered
            ? `${label}のネタバレを開く`
            : `${label}を大きく表示（${formatAttachmentSize(attachment.sizeBytes)}）`
        }
        title={
          covered
            ? "ネタバレ。クリックで表示"
            : `${attachment.filename}（${formatAttachmentSize(attachment.sizeBytes)}）`
        }
        className="block overflow-hidden rounded-lg border border-border bg-muted/40"
      >
        {covered ? null : (
          <img
            src={href}
            alt={label}
            loading="lazy"
            decoding="async"
            onError={() => setBroken(true)}
            className="block max-h-72 max-w-full object-contain sm:max-w-md"
          />
        )}
      </button>
      {covered ? (
        <span className="flex min-h-28 min-w-48 flex-col items-center justify-center gap-1 px-3 text-center">
          <span className="flex items-center gap-1 rounded-full bg-background/85 px-2 py-0.5 font-medium text-[12px]">
            <EyeOff className="size-3.5" />
            ネタバレ
          </span>
          {attachment.alt ? (
            <span className="max-w-[80%] truncate rounded bg-background/70 px-1.5 text-[11px] text-muted-foreground">
              {attachment.alt}
            </span>
          ) : null}
        </span>
      ) : null}
      {!covered && attachment.alt ? (
        <p className="mt-0.5 max-w-full truncate text-[11px] text-muted-foreground">
          {attachment.alt}
        </p>
      ) : null}
    </div>
  );
}

function FileAttachment({
  attachment,
  href,
  icon = "file",
}: {
  attachment: Attachment;
  href: string;
  icon?: "file" | "image";
}) {
  const Icon = attachment.spoiler
    ? EyeOff
    : icon === "image"
      ? ImageOff
      : FileText;
  return (
    <div className="w-fit max-w-full">
      <a
        href={href}
        download={attachment.filename}
        className="flex w-fit max-w-full items-center gap-2 rounded-lg border border-border bg-muted/40 px-2.5 py-1.5 text-[12.5px] hover:bg-accent"
        title={`${attachment.filename}をダウンロード`}
      >
        <Icon className="size-4 shrink-0 text-muted-foreground" />
        <span className="truncate font-medium">{attachment.filename}</span>
        <span className="shrink-0 text-muted-foreground text-xs">
          {formatAttachmentSize(attachment.sizeBytes)}
        </span>
        <Download className="size-3.5 shrink-0 text-muted-foreground" />
      </a>
      {attachment.alt ? (
        <p className="mt-0.5 max-w-full truncate text-[11px] text-muted-foreground">
          {attachment.alt}
        </p>
      ) : null}
    </div>
  );
}
