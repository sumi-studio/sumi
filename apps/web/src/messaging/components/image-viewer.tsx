import {
  Copy,
  Download,
  ExternalLink,
  Info,
  Link as LinkIcon,
  MoreHorizontal,
  X,
  ZoomIn,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { Attachment } from "../model";
import { formatFileSize } from "./message-attachments";

/**
 * アプリ内の画像ビューアー。添付画像を新規タブで開くと会話から離れてしまい、
 * 「見てすぐ戻る」ができない。背後の会話をうっすら残したまま画像だけを前に
 * 出し、Escか外側クリックで元の位置に戻れるようにする。
 *
 * 開閉は自前で持つ（オーバーレイのプリミティブが揃うまでの暫定）。
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
  authorName,
  createdAt,
  onClose,
}: {
  attachment: Attachment;
  authorName?: string;
  createdAt?: number;
  onClose: () => void;
}) {
  const [zoomed, setZoomed] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
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

  // Escは常に閉じる。メニューが開いていればまずメニューだけを畳む。
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.stopPropagation();
      if (menuOpen) {
        setMenuOpen(false);
        return;
      }
      onClose();
    };
    document.addEventListener("keydown", onKeyDown, true);
    return () => document.removeEventListener("keydown", onKeyDown, true);
  }, [menuOpen, onClose]);

  const absoluteUrl = useCallback(() => {
    try {
      return new URL(attachment.url, window.location.origin).href;
    } catch {
      return attachment.url;
    }
  }, [attachment.url]);

  const copyText = useCallback(
    async (text: string, done: string) => {
      try {
        await navigator.clipboard.writeText(text);
        announce(done);
      } catch {
        announce("コピーできませんでした");
      }
    },
    [announce],
  );

  const copyImage = useCallback(async () => {
    try {
      const response = await fetch(attachment.url, { credentials: "include" });
      const blob = await response.blob();
      if (typeof ClipboardItem !== "function" || !navigator.clipboard?.write) {
        throw new Error("clipboard image unsupported");
      }
      await navigator.clipboard.write([
        new ClipboardItem({ [blob.type]: blob }),
      ]);
      announce("画像をコピーしました");
    } catch {
      announce("画像をコピーできませんでした");
    }
  }, [attachment.url, announce]);

  const openInBrowser = useCallback(() => {
    window.open(attachment.url, "_blank", "noopener,noreferrer");
  }, [attachment.url]);

  const save = useCallback(() => {
    const anchor = document.createElement("a");
    anchor.href = attachment.url;
    anchor.download = attachment.filename;
    anchor.rel = "noreferrer";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
  }, [attachment.url, attachment.filename]);

  const menuItems = [
    { label: "画像をコピー", icon: Copy, run: copyImage },
    {
      label: "メディアリンクをコピー",
      icon: LinkIcon,
      run: () => copyText(absoluteUrl(), "リンクをコピーしました"),
    },
    {
      label: detailsOpen ? "詳細を隠す" : "詳細",
      icon: Info,
      run: () => setDetailsOpen((open) => !open),
    },
    {
      label: "添付IDをコピー",
      icon: Copy,
      run: () => copyText(attachment.attachmentId, "添付IDをコピーしました"),
    },
  ];

  return createPortal(
    // biome-ignore lint/a11y/useKeyWithClickEvents: 背景クリックは閉じるための冗長な手段。キーボードからはEscと✕で閉じられる
    <div
      role="dialog"
      aria-modal="true"
      aria-label={`${attachment.filename} の画像ビューアー`}
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
            <ToolbarButton label="保存" onClick={save}>
              <Download className="size-4" />
            </ToolbarButton>
            <ToolbarButton label="ブラウザで開く" onClick={openInBrowser}>
              <ExternalLink className="size-4" />
            </ToolbarButton>
            <div className="relative">
              <ToolbarButton
                label="その他"
                onClick={() => setMenuOpen((open) => !open)}
              >
                <MoreHorizontal className="size-4" />
              </ToolbarButton>
              {menuOpen ? (
                <div className="absolute top-full right-0 mt-1 w-52 overflow-hidden rounded-lg border border-white/15 bg-neutral-900/95 py-1 text-white shadow-lg">
                  {menuItems.map((item) => (
                    <button
                      key={item.label}
                      type="button"
                      onClick={() => {
                        setMenuOpen(false);
                        void item.run();
                      }}
                      className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12.5px] transition-colors hover:bg-white/10"
                    >
                      <item.icon className="size-3.5 text-white/70" />
                      {item.label}
                    </button>
                  ))}
                </div>
              ) : null}
            </div>
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
          src={attachment.url}
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
            {formatFileSize(attachment.size)}・{attachment.mime}
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
