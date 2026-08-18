import {
  Download,
  ExternalLink,
  Info,
  Link as LinkIcon,
  X,
  ZoomIn,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { isImeComposing } from "../../lib/ime";
import { formatAttachmentSize } from "../draft-attachments";
import type { Attachment } from "../model";

/**
 * アプリ内の画像ビューアー。添付画像を新規タブで開くと会話から離れてしまい、
 * 「見てすぐ戻る」ができない。背後の会話を残したまま画像だけを前に出し、Escか
 * 外側のクリックで元の位置へ戻れるようにする。
 *
 * bytesのURLは呼び出し側が現在のexact scopeから渡す。ここでURLを組み立てない
 * のは、古いWorkspaceのURLを描画に残さないため。
 */

const FULL_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "long",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

/** 「コピーしました」のような一瞬の合図を出す時間。 */
const NOTICE_MS = 1_800;

function ToolbarButton({
  label,
  onClick,
  children,
}: {
  label: string;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      className="flex size-8 items-center justify-center rounded-md border border-transparent bg-black/40 text-white/80 backdrop-blur-xs transition-colors hover:border-white/25 hover:bg-black/70 hover:text-white"
    >
      {children}
    </button>
  );
}

export function ImageViewer({
  attachment,
  href,
  authorName,
  createdAt,
  onClose,
}: {
  attachment: Attachment;
  href: string;
  authorName?: string;
  createdAt?: number;
  onClose: () => void;
}) {
  const [zoomed, setZoomed] = useState(false);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const closeRef = useRef<HTMLButtonElement>(null);
  const noticeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    closeRef.current?.focus();
  }, []);

  useEffect(
    () => () => {
      if (noticeTimer.current) clearTimeout(noticeTimer.current);
    },
    [],
  );

  const announce = useCallback((message: string) => {
    setNotice(message);
    if (noticeTimer.current) clearTimeout(noticeTimer.current);
    noticeTimer.current = setTimeout(() => setNotice(null), NOTICE_MS);
  }, []);

  // ビューアーは会話の上に重なっているので、Escは下のUIへ渡さず自分で止める。
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape" || isImeComposing(event)) return;
      event.stopPropagation();
      onClose();
    };
    document.addEventListener("keydown", onKeyDown, true);
    return () => document.removeEventListener("keydown", onKeyDown, true);
  }, [onClose]);

  const copyLink = useCallback(async () => {
    let absolute = href;
    try {
      absolute = new URL(href, window.location.origin).href;
    } catch {
      // 相対のまま渡す。組み立てられないURLでも、写せる文字列ではある。
    }
    try {
      await navigator.clipboard.writeText(absolute);
      announce("リンクをコピーしました");
    } catch {
      announce("コピーできませんでした");
    }
  }, [href, announce]);

  return createPortal(
    // biome-ignore lint/a11y/useKeyWithClickEvents: 背景クリックは閉じるための冗長な手段。キーボードからはEscと✕で閉じられる
    <div
      role="dialog"
      aria-modal="true"
      aria-label={`${attachment.filename} の画像ビューアー`}
      data-testid="image-viewer"
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-[2px]"
      onClick={(event) => {
        // 画像やツールバーの上のクリックは閉じない。
        if (event.target !== event.currentTarget) return;
        onClose();
      }}
    >
      {zoomed ? null : (
        <div className="pointer-events-none absolute top-3 left-4 z-10 max-w-[50vw] text-white/90">
          {authorName ? (
            <p className="truncate font-semibold text-[13px]">{authorName}</p>
          ) : null}
          {createdAt ? (
            <p className="text-[11px] text-white/60 tabular-nums">
              {FULL_FORMAT.format(createdAt)}
            </p>
          ) : null}
        </div>
      )}
      <div className="absolute top-3 right-4 z-10 flex items-start gap-1.5">
        {zoomed ? null : (
          <>
            <ToolbarButton label="ズーム" onClick={() => setZoomed(true)}>
              <ZoomIn className="size-4" />
            </ToolbarButton>
            <ToolbarButton
              label="詳細"
              onClick={() => setDetailsOpen((open) => !open)}
            >
              <Info className="size-4" />
            </ToolbarButton>
            <ToolbarButton label="リンクをコピー" onClick={copyLink}>
              <LinkIcon className="size-4" />
            </ToolbarButton>
            <a
              href={href}
              download={attachment.filename}
              title="保存"
              aria-label="保存"
              className="flex size-8 items-center justify-center rounded-md border border-transparent bg-black/40 text-white/80 backdrop-blur-xs transition-colors hover:border-white/25 hover:bg-black/70 hover:text-white"
            >
              <Download className="size-4" />
            </a>
            <a
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              title="ブラウザで開く"
              aria-label="ブラウザで開く"
              className="flex size-8 items-center justify-center rounded-md border border-transparent bg-black/40 text-white/80 backdrop-blur-xs transition-colors hover:border-white/25 hover:bg-black/70 hover:text-white"
            >
              <ExternalLink className="size-4" />
            </a>
          </>
        )}
        <button
          type="button"
          ref={closeRef}
          onClick={onClose}
          title="閉じる"
          aria-label="画像ビューアーを閉じる"
          className="flex size-8 items-center justify-center rounded-md border border-transparent bg-black/40 text-white/80 backdrop-blur-xs transition-colors hover:border-white/25 hover:bg-black/70 hover:text-white"
        >
          <X className="size-4" />
        </button>
      </div>

      {/* 画像そのものがワンクリックの拡大トグル。カーソルで今どちらかを示す。 */}
      <button
        type="button"
        onClick={() => setZoomed((on) => !on)}
        aria-label={zoomed ? "通常サイズに戻す" : "最大表示にする"}
        aria-pressed={zoomed}
        className={
          zoomed
            ? "flex h-screen w-screen cursor-zoom-out items-center justify-center"
            : "flex max-h-[82vh] max-w-[86vw] cursor-zoom-in items-center justify-center"
        }
      >
        <img
          src={href}
          alt={attachment.alt || attachment.filename}
          className={
            zoomed
              ? "max-h-screen max-w-screen object-contain"
              : "max-h-[82vh] max-w-[86vw] rounded-lg object-contain shadow-2xl"
          }
        />
      </button>

      {detailsOpen && !zoomed ? (
        <div className="absolute bottom-4 left-4 z-10 max-w-[70vw] rounded-lg border border-white/15 bg-black/60 px-3 py-2 text-[12px] text-white/85 backdrop-blur-xs">
          <p className="truncate font-medium">{attachment.filename}</p>
          <p className="text-white/60">
            {formatAttachmentSize(attachment.sizeBytes)}・{attachment.mime}
          </p>
          {attachment.alt ? (
            <p className="mt-1 text-white/70">{attachment.alt}</p>
          ) : null}
        </div>
      ) : null}

      <output
        aria-live="polite"
        className={`absolute bottom-4 left-1/2 -translate-x-1/2 rounded-full bg-black/70 px-3 py-1 text-[12px] text-white transition-opacity ${
          notice ? "opacity-100" : "pointer-events-none opacity-0"
        }`}
      >
        {notice}
      </output>
    </div>,
    document.body,
  );
}
