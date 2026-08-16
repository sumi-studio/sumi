import { Download, FileText, ImageOff } from "lucide-react";
import { useState } from "react";
import { formatAttachmentSize } from "../draft-attachments";
import type { Attachment } from "../model";
import { isInlineImageMime } from "../model";
import { useMessaging } from "../store";

/**
 * メッセージが運ぶ添付の描画。画像はサーバーが安全と判定した型だけをinlineで
 * 出し、それ以外はファイルカードとしてダウンロードさせる。bytesのURLは常に
 * 現在のexact scopeから引く: 古いWorkspaceのURLを描画に残さない。
 */
export function MessageAttachments({
  attachments,
}: {
  attachments: Attachment[];
}) {
  const attachmentURL = useMessaging((state) => state.attachmentURL);
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
    </div>
  );
}

function ImageAttachment({
  attachment,
  href,
}: {
  attachment: Attachment;
  href: string;
}) {
  const [broken, setBroken] = useState(false);
  if (broken) {
    return <FileAttachment attachment={attachment} href={href} icon="image" />;
  }
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="block overflow-hidden rounded-lg border border-border bg-muted/40"
      title={`${attachment.filename}（${formatAttachmentSize(attachment.sizeBytes)}）`}
    >
      <img
        src={href}
        alt={attachment.filename}
        loading="lazy"
        decoding="async"
        onError={() => setBroken(true)}
        className="block max-h-72 max-w-full object-contain sm:max-w-md"
      />
    </a>
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
  const Icon = icon === "image" ? ImageOff : FileText;
  return (
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
  );
}
