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
  TriangleAlert,
  X,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { secureRandomUUID } from "../../lib/random-uuid";
import type { Attachment, AttachmentDraftPatch } from "../model";
import {
  isImageMime,
  MAX_ATTACHMENT_BYTES,
  MAX_ATTACHMENTS_PER_MESSAGE,
} from "../model";
import {
  type AttachmentEdit,
  AttachmentEditModal,
} from "./attachment-edit-modal";
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
  onToggleSpoiler,
  onEdit,
  fileFor,
}: {
  items: DraftAttachment[];
  onRemove: (localId: string) => void;
  /** ホバーからのワンタッチのネタバレ切替。 */
  onToggleSpoiler?: (localId: string) => void;
  onEdit?: (localId: string, edit: AttachmentEdit) => void;
  /** 画像加工の元になるローカルのFile。 */
  fileFor?: (localId: string) => File | undefined;
}) {
  const [editingId, setEditingId] = useState<string | null>(null);
  const editing = items.find((entry) => entry.localId === editingId);
  if (items.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-2 px-2.5 pt-2.5">
      {items.map((entry) => {
        const spoiler = entry.attachment?.spoiler ?? false;
        return (
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
                  className={`size-full object-cover ${
                    spoiler ? "blur-md" : ""
                  }`}
                />
              ) : (
                <FormatIcon mime={entry.mime} filename={entry.filename} />
              )}
              {spoiler ? (
                <span className="absolute bottom-1 left-1 rounded-full bg-background/85 px-1.5 py-px font-medium text-[10px] text-muted-foreground">
                  ネタバレ
                </span>
              ) : null}
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
                {onToggleSpoiler && entry.attachment ? (
                  <button
                    type="button"
                    onClick={() => onToggleSpoiler(entry.localId)}
                    title={spoiler ? "ネタバレを解除" : "ネタバレとしてマーク"}
                    aria-label={`${entry.filename} の${
                      spoiler ? "ネタバレを解除" : "ネタバレをマーク"
                    }`}
                    aria-pressed={spoiler}
                    className={`flex size-6 items-center justify-center rounded-md border bg-background/80 shadow-xs transition-colors focus-visible:opacity-100 group-hover:opacity-100 ${
                      spoiler
                        ? "border-ring/60 text-foreground opacity-100"
                        : "border-transparent text-muted-foreground opacity-60 hover:border-border hover:bg-accent hover:text-foreground"
                    }`}
                  >
                    {spoiler ? (
                      <EyeOff className="size-3.5" />
                    ) : (
                      <Eye className="size-3.5" />
                    )}
                  </button>
                ) : null}
                {onEdit && entry.attachment ? (
                  <button
                    type="button"
                    onClick={() => setEditingId(entry.localId)}
                    title="添付ファイルを編集"
                    aria-label={`${entry.filename} を編集`}
                    className="flex size-6 items-center justify-center rounded-md border border-transparent bg-background/80 text-muted-foreground opacity-60 shadow-xs transition-colors hover:border-border hover:bg-accent hover:text-foreground focus-visible:opacity-100 group-hover:opacity-100"
                  >
                    <Pencil className="size-3.5" />
                  </button>
                ) : null}
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
        );
      })}
      {editing && onEdit ? (
        <AttachmentEditModal
          filename={editing.filename}
          alt={editing.attachment?.alt ?? ""}
          spoiler={editing.attachment?.spoiler ?? false}
          file={fileFor?.(editing.localId)}
          imageUrl={editing.previewUrl}
          onCancel={() => setEditingId(null)}
          onApply={(edit) => {
            setEditingId(null);
            onEdit(editing.localId, edit);
          }}
        />
      ) : null}
    </div>
  );
}

/**
 * 送信前の添付の受け皿。ファイルを預かってアップロードし、送信できる
 * Attachmentだけを取り出せるようにする。object URLはこのフックが後始末する。
 */
export function useDraftAttachments({
  upload,
  update,
}: {
  upload: (file: File) => Promise<Attachment>;
  update: (
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ) => Promise<Attachment>;
}) {
  const [items, setItems] = useState<DraftAttachment[]>([]);
  // 生成したobject URLは必ず解放する。stateを辿るとremove後に取り逃すため
  // 別に持つ。
  const previewUrls = useRef(new Map<string, string>());
  const filesRef = useRef(new Map<string, File>());
  // 非同期の編集が終わったときの最新の下書き（stale closureを避ける）。
  const itemsRef = useRef(items);
  itemsRef.current = items;

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
        upload(accepted[index])
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
    [items.length, upload],
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

  /**
   * 送信前の編集を適用する。画像を加工したときは中身が別物になるので
   * 預け直し、宣言（名前・概要・ネタバレ）はサーバー側の添付へ反映する。
   * 加工前の預かりは束ねられないまま残る（誰にも見えない）。
   */
  const applyEdit = useCallback(
    async (localId: string, edit: AttachmentEdit) => {
      const current = itemsRef.current.find(
        (entry) => entry.localId === localId,
      );
      if (!current) return;
      let attachment = current.attachment;
      if (edit.editedFile) {
        replace(localId, { status: "uploading" }, edit.editedFile);
        try {
          attachment = await upload(edit.editedFile);
        } catch {
          replace(localId, { status: "failed" });
          return;
        }
      }
      if (!attachment) return;
      const patch = edit.patch;
      const needsPatch =
        patch.filename !== undefined ||
        patch.alt !== undefined ||
        patch.spoiler !== undefined;
      if (needsPatch) {
        // 宣言だけの更新も送信と競合する。PATCH が終わるまで composer の
        // 送信ゲートを閉じ、束ねた後の添付へ更新が落ちるのを防ぐ。
        replace(localId, { status: "uploading" });
        try {
          attachment = await update(attachment.attachmentId, patch);
        } catch {
          replace(localId, { status: "failed" });
          return;
        }
      }
      replace(localId, {
        status: "ready",
        attachment,
        filename: attachment.filename,
        size: attachment.size,
        mime: attachment.mime,
      });
    },
    [replace, upload, update],
  );

  /** ホバーからのワンタッチ。宣言だけを切り替える。 */
  const toggleSpoiler = useCallback(
    (localId: string) => {
      const current = itemsRef.current.find(
        (entry) => entry.localId === localId,
      );
      if (!current?.attachment) return;
      void applyEdit(localId, {
        patch: { spoiler: !current.attachment.spoiler },
      });
    },
    [applyEdit],
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
    applyEdit,
    toggleSpoiler,
    uploading,
    ready,
  };
}
