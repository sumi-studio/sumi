import {
  FileArchive,
  FileAudio,
  File as FileIcon,
  FileImage,
  FileText,
  FileVideo,
  Loader2,
  TriangleAlert,
  X,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { secureRandomUUID } from "../../lib/random-uuid";
import type { Attachment } from "../model";
import {
  isImageMime,
  MAX_ATTACHMENT_BYTES,
  MAX_ATTACHMENTS_PER_MESSAGE,
} from "../model";
import { formatFileSize } from "./message-attachments";

/**
 * 送信前の添付UI。composer本体から切り出してあるのは、送信前の添付が
 * 「ファイル名の羅列」ではなく中身の見える下書きだからで、その表示と
 * 取り回し（サムネイル・object URLの後始末）をここに閉じ込める。
 *
 * アップロードは送信より前に済ませ、送信時にはidを渡すだけ。送る前なら
 * いつでも取り消せる。
 */
export interface DraftAttachment {
  localId: string;
  filename: string;
  size: number;
  mime: string;
  status: "uploading" | "ready" | "failed";
  attachment?: Attachment;
  /** 画像のみ。ローカルのFileから作ったサムネイル用object URL。 */
  previewUrl?: string;
}

/** 拡張子（先頭の . は含まない、最大5文字）。無ければ空。 */
export function fileExtension(filename: string): string {
  const dot = filename.lastIndexOf(".");
  if (dot <= 0 || dot === filename.length - 1) return "";
  const extension = filename.slice(dot + 1).toUpperCase();
  return extension.length <= 5 ? extension : "";
}

/**
 * 中身のサムネイルが出せない形式に、せめて「何のファイルか」を出す。
 * PDFの先頭ページプレビューは描画エンジン（pdf.js）を丸ごと抱えることに
 * なるため採らず、形式アイコン＋拡張子で代替する。
 */
function FormatIcon({ mime, filename }: { mime: string; filename: string }) {
  const type = mime.toLowerCase();
  const extension = fileExtension(filename);
  const Icon = isImageMime(type)
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
    <span className="flex size-full flex-col items-center justify-center gap-1 bg-muted/50 text-muted-foreground">
      <Icon className="size-6" />
      {extension ? (
        <span className="font-medium text-[10px] tracking-wide">
          {extension}
        </span>
      ) : null}
    </span>
  );
}

function statusLabel(entry: DraftAttachment): string {
  if (entry.status === "failed") {
    return entry.size > MAX_ATTACHMENT_BYTES ? "大きすぎます" : "失敗";
  }
  return formatFileSize(entry.size);
}

/**
 * 送信前の添付カード。画像は中身の見えるサムネイル、それ以外は形式アイコン。
 * 操作アイコンは常に置いた上でホバーで前に出す（隠すとキーボードから
 * 辿れなくなるため、見え方だけを変える）。
 */
export function ComposerAttachments({
  items,
  onRemove,
  actions,
}: {
  items: DraftAttachment[];
  onRemove: (localId: string) => void;
  /** カードのホバー操作に足す追加ボタン（レイヤー3の編集・ネタバレ）。 */
  actions?: (entry: DraftAttachment) => React.ReactNode;
}) {
  if (items.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-2 px-2.5 pt-2.5">
      {items.map((entry) => (
        <div
          key={entry.localId}
          className={`group relative w-32 overflow-hidden rounded-lg border ${
            entry.status === "failed"
              ? "border-rose-500/40 bg-rose-500/8"
              : "border-border bg-muted/30"
          }`}
        >
          <div className="relative h-20 w-full overflow-hidden">
            {entry.previewUrl ? (
              <img
                src={entry.previewUrl}
                alt={`${entry.filename} のプレビュー`}
                className="size-full object-cover"
              />
            ) : (
              <FormatIcon mime={entry.mime} filename={entry.filename} />
            )}
            {entry.status === "uploading" ? (
              <span className="absolute inset-0 flex items-center justify-center bg-background/60">
                <Loader2 className="size-4 animate-spin text-muted-foreground" />
              </span>
            ) : null}
            {entry.status === "failed" ? (
              <span className="absolute inset-0 flex items-center justify-center bg-background/60 text-rose-600 dark:text-rose-400">
                <TriangleAlert className="size-4" />
              </span>
            ) : null}
            <div className="absolute top-1 right-1 flex items-center gap-1">
              {actions?.(entry)}
              <button
                type="button"
                onClick={() => onRemove(entry.localId)}
                title="添付ファイルを削除"
                aria-label={`${entry.filename} の添付を取り消す`}
                className="flex size-6 items-center justify-center rounded-md border border-transparent bg-background/80 text-muted-foreground opacity-60 shadow-xs transition-colors hover:border-rose-500/50 hover:bg-rose-500/15 hover:text-rose-600 focus-visible:opacity-100 group-hover:opacity-100 dark:hover:text-rose-400"
              >
                <X className="size-3.5" />
              </button>
            </div>
          </div>
          <div className="px-2 py-1.5">
            <p
              className="truncate font-medium text-[12px]"
              title={entry.filename}
            >
              {entry.filename}
            </p>
            <p
              className={`text-[11px] tabular-nums ${
                entry.status === "failed"
                  ? "text-rose-600 dark:text-rose-400"
                  : "text-muted-foreground"
              }`}
            >
              {statusLabel(entry)}
            </p>
          </div>
        </div>
      ))}
    </div>
  );
}

/**
 * 送信前の添付の受け皿。ファイルを預かってアップロードし、送信できる
 * Attachmentだけを取り出せるようにする。object URLはこのフックが後始末する。
 */
export function useDraftAttachments(
  uploadAttachment: (file: File) => Promise<Attachment>,
) {
  const [items, setItems] = useState<DraftAttachment[]>([]);
  // 生成したobject URLは必ず解放する。stateを辿るとremove後に取り逃すため
  // 別に持つ。
  const previewUrls = useRef(new Map<string, string>());
  const filesRef = useRef(new Map<string, File>());

  useEffect(
    () => () => {
      for (const url of previewUrls.current.values()) URL.revokeObjectURL(url);
      previewUrls.current.clear();
      filesRef.current.clear();
    },
    [],
  );

  const releasePreview = useCallback((localId: string) => {
    const url = previewUrls.current.get(localId);
    if (url) {
      URL.revokeObjectURL(url);
      previewUrls.current.delete(localId);
    }
    filesRef.current.delete(localId);
  }, []);

  // 大きすぎるファイルはサーバーへ運ばずここで落とす（上限は契約と同値）。
  const addFiles = useCallback(
    (files: FileList | File[] | null | undefined) => {
      const chosen = Array.from(files ?? []);
      if (chosen.length === 0) return;
      const room = Math.max(0, MAX_ATTACHMENTS_PER_MESSAGE - items.length);
      const accepted = chosen.slice(0, room);
      if (accepted.length === 0) return;
      const drafts: DraftAttachment[] = accepted.map((file) => {
        const localId = secureRandomUUID();
        const mime = file.type || "application/octet-stream";
        let previewUrl: string | undefined;
        if (isImageMime(mime) && typeof URL.createObjectURL === "function") {
          previewUrl = URL.createObjectURL(file);
          previewUrls.current.set(localId, previewUrl);
        }
        filesRef.current.set(localId, file);
        return {
          localId,
          filename: file.name || "file",
          size: file.size,
          mime,
          status: file.size > MAX_ATTACHMENT_BYTES ? "failed" : "uploading",
          previewUrl,
        };
      });
      setItems((current) => [...current, ...drafts]);
      drafts.forEach((draft, index) => {
        if (draft.status !== "uploading") return;
        uploadAttachment(accepted[index])
          .then((attachment) => {
            setItems((current) =>
              current.map((entry) =>
                entry.localId === draft.localId
                  ? { ...entry, status: "ready" as const, attachment }
                  : entry,
              ),
            );
          })
          .catch(() => {
            setItems((current) =>
              current.map((entry) =>
                entry.localId === draft.localId
                  ? { ...entry, status: "failed" as const }
                  : entry,
              ),
            );
          });
      });
    },
    [items.length, uploadAttachment],
  );

  const remove = useCallback(
    (localId: string) => {
      releasePreview(localId);
      setItems((current) =>
        current.filter((entry) => entry.localId !== localId),
      );
    },
    [releasePreview],
  );

  const clear = useCallback(() => {
    for (const url of previewUrls.current.values()) URL.revokeObjectURL(url);
    previewUrls.current.clear();
    filesRef.current.clear();
    setItems([]);
  }, []);

  /** 差し替え（レイヤー3の画像編集が新しい実体を作ったとき）。 */
  const replace = useCallback(
    (localId: string, next: Partial<DraftAttachment>, file?: File) => {
      if (file) {
        const previous = previewUrls.current.get(localId);
        if (previous) URL.revokeObjectURL(previous);
        filesRef.current.set(localId, file);
        if (
          isImageMime(file.type) &&
          typeof URL.createObjectURL === "function"
        ) {
          const url = URL.createObjectURL(file);
          previewUrls.current.set(localId, url);
          next = { ...next, previewUrl: url };
        }
      }
      setItems((current) =>
        current.map((entry) =>
          entry.localId === localId ? { ...entry, ...next } : entry,
        ),
      );
    },
    [],
  );

  const fileFor = useCallback(
    (localId: string) => filesRef.current.get(localId),
    [],
  );

  const uploading = items.some((entry) => entry.status === "uploading");
  const ready = items.flatMap((entry) =>
    entry.attachment ? [entry.attachment] : [],
  );

  return {
    items,
    addFiles,
    remove,
    clear,
    replace,
    fileFor,
    uploading,
    ready,
  };
}
