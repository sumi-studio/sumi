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
import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { secureRandomUUID } from "../../lib/random-uuid";
import type { Attachment, AttachmentDraftPatch, PlaceKey } from "../model";
import {
  isImageMime,
  MAX_ATTACHMENT_BYTES,
  MAX_ATTACHMENTS_PER_MESSAGE,
} from "../model";
import { useMessaging } from "../store";
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

const NO_DRAFT_ATTACHMENTS: DraftAttachment[] = [];

type UploadAttachment = (file: File) => Promise<Attachment>;
type UpdateAttachment = (
  attachmentId: string,
  patch: AttachmentDraftPatch,
) => Promise<Attachment>;
type RenewReadyAttachments = (attachmentIds: string[]) => Promise<void>;

export const DRAFT_ATTACHMENT_RENEW_INTERVAL_MS = 5 * 60_000;
export const DRAFT_ATTACHMENT_RENEW_BATCH_SIZE = MAX_ATTACHMENTS_PER_MESSAGE;

interface DraftAttachmentsOwner {
  itemsByPlace: Record<PlaceKey, DraftAttachment[]>;
  addFiles(
    placeKey: PlaceKey,
    files: FileList | File[] | null | undefined,
  ): void;
  remove(placeKey: PlaceKey, localId: string): void;
  clear(placeKey: PlaceKey): void;
  replace(
    placeKey: PlaceKey,
    localId: string,
    next: Partial<DraftAttachment>,
    file?: File,
  ): void;
  fileFor(placeKey: PlaceKey, localId: string): File | undefined;
  applyEdit(
    placeKey: PlaceKey,
    localId: string,
    edit: AttachmentEdit,
  ): Promise<void>;
  toggleSpoiler(placeKey: PlaceKey, localId: string): void;
}

const DraftAttachmentsContext = createContext<DraftAttachmentsOwner | null>(
  null,
);

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
                    disabled={entry.status === "uploading"}
                    title={spoiler ? "ネタバレを解除" : "ネタバレとしてマーク"}
                    aria-label={`${entry.filename} の${
                      spoiler ? "ネタバレを解除" : "ネタバレをマーク"
                    }`}
                    aria-pressed={spoiler}
                    className={`flex size-6 items-center justify-center rounded-md border bg-background/80 shadow-xs transition-colors focus-visible:opacity-100 group-hover:opacity-100 disabled:cursor-not-allowed disabled:opacity-30 ${
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
                    disabled={entry.status === "uploading"}
                    title="添付ファイルを編集"
                    aria-label={`${entry.filename} を編集`}
                    className="flex size-6 items-center justify-center rounded-md border border-transparent bg-background/80 text-muted-foreground opacity-60 shadow-xs transition-colors hover:border-border hover:bg-accent hover:text-foreground focus-visible:opacity-100 group-hover:opacity-100 disabled:cursor-not-allowed disabled:opacity-30"
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
 * 添付下書きの真の所有者。route配下のComposerより長く生き、messaging sessionが
 * 終わった時だけ全File/object URLを解放する。
 */
export function DraftAttachmentsProvider({
  children,
  upload,
  update,
  renewReadyAttachments,
}: {
  children: ReactNode;
  upload: UploadAttachment;
  update: UpdateAttachment;
  /** Transport seam for renewing ready, still-unbound attachment leases. */
  renewReadyAttachments?: RenewReadyAttachments;
}) {
  const owner = useDraftAttachmentsOwner({ upload, update });
  useReadyAttachmentRenewal(owner.itemsByPlace, renewReadyAttachments);
  return (
    <DraftAttachmentsContext.Provider value={owner}>
      {children}
    </DraftAttachmentsContext.Provider>
  );
}

function useReadyAttachmentRenewal(
  itemsByPlace: Record<PlaceKey, DraftAttachment[]>,
  renewReadyAttachments: RenewReadyAttachments | undefined,
) {
  const readyIds = useMemo(() => {
    const ids = new Set<string>();
    for (const items of Object.values(itemsByPlace)) {
      for (const entry of items) {
        if (entry.status === "ready" && entry.attachment) {
          ids.add(entry.attachment.attachmentId);
        }
      }
    }
    return [...ids].sort();
  }, [itemsByPlace]);
  const readyIdsRef = useRef(readyIds);
  readyIdsRef.current = readyIds;
  const requestRenewalRef = useRef<() => void>(() => {});

  useEffect(() => {
    if (!renewReadyAttachments) {
      requestRenewalRef.current = () => {};
      return;
    }

    let active = true;
    let inFlight: Promise<void> | null = null;
    const requestRenewal = () => {
      if (
        !active ||
        inFlight ||
        (typeof navigator !== "undefined" && navigator.onLine === false)
      ) {
        return;
      }
      const ids = [...readyIdsRef.current];
      if (ids.length === 0) return;

      // callbackをmicrotaskへ送ってからinFlightを立て、同期的な再入も二重化しない。
      const task = Promise.resolve().then(async () => {
        for (
          let start = 0;
          active && start < ids.length;
          start += DRAFT_ATTACHMENT_RENEW_BATCH_SIZE
        ) {
          try {
            await renewReadyAttachments(
              ids.slice(start, start + DRAFT_ATTACHMENT_RENEW_BATCH_SIZE),
            );
          } catch {
            // このbatchは次回再試行する。後続batchのleaseまで飢えさせない。
          }
        }
      });
      inFlight = task;
      void task.then(() => {
        if (inFlight === task) inFlight = null;
      });
    };
    const renewWhenVisible = () => {
      if (document.visibilityState === "visible") requestRenewal();
    };

    requestRenewalRef.current = requestRenewal;
    requestRenewal();
    const timer = window.setInterval(
      requestRenewal,
      DRAFT_ATTACHMENT_RENEW_INTERVAL_MS,
    );
    window.addEventListener("focus", requestRenewal);
    window.addEventListener("online", requestRenewal);
    window.addEventListener("pageshow", requestRenewal);
    document.addEventListener("visibilitychange", renewWhenVisible);
    return () => {
      active = false;
      requestRenewalRef.current = () => {};
      window.clearInterval(timer);
      window.removeEventListener("focus", requestRenewal);
      window.removeEventListener("online", requestRenewal);
      window.removeEventListener("pageshow", requestRenewal);
      document.removeEventListener("visibilitychange", renewWhenVisible);
    };
  }, [renewReadyAttachments]);

  // 新しくreadyになったrowも次の長いintervalを待たず一度更新する。
  useEffect(() => {
    if (readyIds.length > 0) requestRenewalRef.current();
  }, [readyIds]);
}

/**
 * sibling routeをまたぐmessaging session境界。selfKeyが変わる（logout/account
 * switchを含む）とownerごと破棄し、前sessionのローカルFileを残さない。
 */
export function MessagingDraftAttachmentsSession({
  children,
}: {
  children: ReactNode;
}) {
  const sessionKey = useMessaging(
    (state) => state.selfKey || "no-messaging-session",
  );
  const upload = useMessaging((state) => state.uploadAttachment);
  const update = useMessaging((state) => state.updateAttachment);
  const renewReadyAttachments = useMessaging((state) => state.renewAttachments);
  return (
    <DraftAttachmentsProvider
      key={sessionKey}
      upload={upload}
      update={update}
      renewReadyAttachments={renewReadyAttachments}
    >
      {children}
    </DraftAttachmentsProvider>
  );
}

function useDraftAttachmentsOwner({
  upload,
  update,
}: {
  upload: UploadAttachment;
  update: UpdateAttachment;
}): DraftAttachmentsOwner {
  const [itemsByPlace, setItemsByPlace] = useState<
    Record<PlaceKey, DraftAttachment[]>
  >({});
  // 非同期処理の完了先は、その時点のactive placeではなく開始時のplace。
  // stateと同じ形のrefを同期的な正本にして、place切替や連続操作に耐える。
  const itemsByPlaceRef = useRef<Record<PlaceKey, DraftAttachment[]>>({});
  const mountedRef = useRef(false);
  // object URLとFileもplaceごとに持つ。localIdだけに頼ると、別placeの操作が
  // 同じ資源を解放・参照できてしまうため。
  const previewUrlsByPlace = useRef(new Map<PlaceKey, Map<string, string>>());
  const filesByPlace = useRef(new Map<PlaceKey, Map<string, File>>());
  // React の再描画より先に同じ添付への二重操作を拒む同期的な占有権。
  const editsInFlightByPlace = useRef(new Map<PlaceKey, Set<string>>());

  const setPlaceItems = useCallback(
    (
      targetPlaceKey: PlaceKey,
      updateItems: (current: DraftAttachment[]) => DraftAttachment[],
    ) => {
      if (!mountedRef.current) return;
      const currentByPlace = itemsByPlaceRef.current;
      const current = currentByPlace[targetPlaceKey] ?? NO_DRAFT_ATTACHMENTS;
      const nextItems = updateItems(current);
      if (nextItems === current) return;

      let nextByPlace: Record<PlaceKey, DraftAttachment[]>;
      if (nextItems.length === 0) {
        if (!(targetPlaceKey in currentByPlace)) return;
        nextByPlace = { ...currentByPlace };
        delete nextByPlace[targetPlaceKey];
      } else {
        nextByPlace = { ...currentByPlace, [targetPlaceKey]: nextItems };
      }
      // Reactのcommitを待たず、後続のイベント・Promiseも最新値から更新する。
      itemsByPlaceRef.current = nextByPlace;
      setItemsByPlace(nextByPlace);
    },
    [],
  );

  const releaseResources = useCallback(
    (targetPlaceKey: PlaceKey, localId: string) => {
      const previews = previewUrlsByPlace.current.get(targetPlaceKey);
      const previewUrl = previews?.get(localId);
      if (previewUrl && typeof URL.revokeObjectURL === "function") {
        URL.revokeObjectURL(previewUrl);
      }
      previews?.delete(localId);
      if (previews?.size === 0) {
        previewUrlsByPlace.current.delete(targetPlaceKey);
      }

      const files = filesByPlace.current.get(targetPlaceKey);
      files?.delete(localId);
      if (files?.size === 0) filesByPlace.current.delete(targetPlaceKey);
    },
    [],
  );

  const releasePlaceResources = useCallback((targetPlaceKey: PlaceKey) => {
    const previews = previewUrlsByPlace.current.get(targetPlaceKey);
    if (typeof URL.revokeObjectURL === "function") {
      for (const url of previews?.values() ?? []) {
        URL.revokeObjectURL(url);
      }
    }
    previewUrlsByPlace.current.delete(targetPlaceKey);
    filesByPlace.current.delete(targetPlaceKey);
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (typeof URL.revokeObjectURL === "function") {
        for (const previews of previewUrlsByPlace.current.values()) {
          for (const url of previews.values()) URL.revokeObjectURL(url);
        }
      }
      previewUrlsByPlace.current.clear();
      filesByPlace.current.clear();
      editsInFlightByPlace.current.clear();
      itemsByPlaceRef.current = {};
    };
  }, []);

  // 大きすぎるファイルはサーバーへ運ばずここで落とす（上限は契約と同値）。
  const addFiles = useCallback(
    (targetPlaceKey: PlaceKey, files: FileList | File[] | null | undefined) => {
      if (!mountedRef.current) return;
      const chosen = Array.from(files ?? []);
      if (chosen.length === 0) return;
      const current =
        itemsByPlaceRef.current[targetPlaceKey] ?? NO_DRAFT_ATTACHMENTS;
      const room = Math.max(0, MAX_ATTACHMENTS_PER_MESSAGE - current.length);
      const accepted = chosen.slice(0, room);
      if (accepted.length === 0) return;
      const drafts: DraftAttachment[] = accepted.map((file) => {
        const localId = secureRandomUUID();
        const mime = file.type || "application/octet-stream";
        let previewUrl: string | undefined;
        if (isImageMime(mime) && typeof URL.createObjectURL === "function") {
          previewUrl = URL.createObjectURL(file);
          const previews =
            previewUrlsByPlace.current.get(targetPlaceKey) ?? new Map();
          previews.set(localId, previewUrl);
          previewUrlsByPlace.current.set(targetPlaceKey, previews);
        }
        const filesForPlace =
          filesByPlace.current.get(targetPlaceKey) ?? new Map();
        filesForPlace.set(localId, file);
        filesByPlace.current.set(targetPlaceKey, filesForPlace);
        return {
          localId,
          filename: file.name || "file",
          size: file.size,
          mime,
          status: file.size > MAX_ATTACHMENT_BYTES ? "failed" : "uploading",
          previewUrl,
        };
      });
      setPlaceItems(targetPlaceKey, (currentItems) => [
        ...currentItems,
        ...drafts,
      ]);
      drafts.forEach((draft, index) => {
        if (draft.status !== "uploading") return;
        upload(accepted[index])
          .then((attachment) => {
            setPlaceItems(targetPlaceKey, (currentItems) =>
              currentItems.map((entry) =>
                entry.localId === draft.localId
                  ? { ...entry, status: "ready" as const, attachment }
                  : entry,
              ),
            );
          })
          .catch(() => {
            setPlaceItems(targetPlaceKey, (currentItems) =>
              currentItems.map((entry) =>
                entry.localId === draft.localId
                  ? { ...entry, status: "failed" as const }
                  : entry,
              ),
            );
          });
      });
    },
    [setPlaceItems, upload],
  );

  const remove = useCallback(
    (targetPlaceKey: PlaceKey, localId: string) => {
      if (!mountedRef.current) return;
      releaseResources(targetPlaceKey, localId);
      setPlaceItems(targetPlaceKey, (current) =>
        current.filter((entry) => entry.localId !== localId),
      );
    },
    [releaseResources, setPlaceItems],
  );

  const clear = useCallback(
    (targetPlaceKey: PlaceKey) => {
      if (!mountedRef.current) return;
      releasePlaceResources(targetPlaceKey);
      setPlaceItems(targetPlaceKey, () => []);
    },
    [releasePlaceResources, setPlaceItems],
  );

  /** 差し替え（レイヤー3の画像編集が新しい実体を作ったとき）。 */
  const replaceAtPlace = useCallback(
    (
      targetPlaceKey: PlaceKey,
      localId: string,
      next: Partial<DraftAttachment>,
      file?: File,
    ) => {
      if (
        !mountedRef.current ||
        !itemsByPlaceRef.current[targetPlaceKey]?.some(
          (entry) => entry.localId === localId,
        )
      ) {
        return;
      }
      if (file) {
        const previews = previewUrlsByPlace.current.get(targetPlaceKey);
        const previous = previews?.get(localId);
        if (previous && typeof URL.revokeObjectURL === "function") {
          URL.revokeObjectURL(previous);
        }
        previews?.delete(localId);

        const filesForPlace =
          filesByPlace.current.get(targetPlaceKey) ?? new Map();
        filesForPlace.set(localId, file);
        filesByPlace.current.set(targetPlaceKey, filesForPlace);

        let previewUrl: string | undefined;
        if (
          isImageMime(file.type) &&
          typeof URL.createObjectURL === "function"
        ) {
          previewUrl = URL.createObjectURL(file);
          const nextPreviews = previews ?? new Map();
          nextPreviews.set(localId, previewUrl);
          previewUrlsByPlace.current.set(targetPlaceKey, nextPreviews);
        }
        // 画像以外への差し替えでも、解放済みの古いURLをstateへ残さない。
        next = { ...next, previewUrl };
      }
      setPlaceItems(targetPlaceKey, (current) =>
        current.map((entry) =>
          entry.localId === localId ? { ...entry, ...next } : entry,
        ),
      );
    },
    [setPlaceItems],
  );

  const fileFor = useCallback(
    (targetPlaceKey: PlaceKey, localId: string) =>
      filesByPlace.current.get(targetPlaceKey)?.get(localId),
    [],
  );

  /**
   * 送信前の編集を適用する。画像を加工したときは中身が別物になるので
   * 預け直し、宣言（名前・概要・ネタバレ）はサーバー側の添付へ反映する。
   * 加工前の預かりは束ねられないまま残る（誰にも見えない）。
   */
  const applyEdit = useCallback(
    async (targetPlaceKey: PlaceKey, localId: string, edit: AttachmentEdit) => {
      // owner teardown後のmodal callbackは、upload/PATCHを始める前に止める。
      if (!mountedRef.current) return;
      const current = itemsByPlaceRef.current[targetPlaceKey]?.find(
        (entry) => entry.localId === localId,
      );
      const editsInFlight =
        editsInFlightByPlace.current.get(targetPlaceKey) ?? new Set();
      if (
        !current ||
        current.status === "uploading" ||
        editsInFlight.has(localId)
      ) {
        return;
      }
      const patch = edit.patch;
      const needsPatch =
        patch.filename !== undefined ||
        patch.alt !== undefined ||
        patch.spoiler !== undefined;
      if (!edit.editedFile && !needsPatch) return;
      if (!current.attachment && !edit.editedFile) return;

      editsInFlight.add(localId);
      editsInFlightByPlace.current.set(targetPlaceKey, editsInFlight);
      try {
        // Local File/previewを差し替えた瞬間から旧server rowは対応する実体では
        // なくなる。upload失敗時に後続操作が旧idへ戻れないよう、先に切り離す。
        replaceAtPlace(
          targetPlaceKey,
          localId,
          {
            status: "uploading",
            ...(edit.editedFile ? { attachment: undefined } : {}),
          },
          edit.editedFile,
        );
        let attachment = current.attachment;
        if (edit.editedFile) {
          attachment = await upload(edit.editedFile);
          // session teardown中にuploadが返っても、その後のPATCHを始めない。
          if (!mountedRef.current) return;
          // metadata PATCHが失敗しても、新しいpreview/Fileと同じupload idだけを
          // recovery元にする。readyにはまだせず送信ゲートは閉じたまま。
          replaceAtPlace(targetPlaceKey, localId, {
            status: "uploading",
            attachment,
            filename: attachment.filename,
            size: attachment.size,
            mime: attachment.mime,
          });
        }
        if (!attachment) throw new Error("attachment upload returned no row");
        // 宣言だけの更新も送信と競合する。PATCH が終わるまで composer の
        // 送信ゲートを閉じ、束ねた後の添付へ更新が落ちるのを防ぐ。
        if (needsPatch) {
          attachment = await update(attachment.attachmentId, patch);
        }
        replaceAtPlace(targetPlaceKey, localId, {
          status: "ready",
          attachment,
          filename: attachment.filename,
          size: attachment.size,
          mime: attachment.mime,
        });
      } catch {
        replaceAtPlace(targetPlaceKey, localId, { status: "failed" });
      } finally {
        editsInFlight.delete(localId);
        if (editsInFlight.size === 0) {
          editsInFlightByPlace.current.delete(targetPlaceKey);
        }
      }
    },
    [replaceAtPlace, upload, update],
  );

  /** ホバーからのワンタッチ。宣言だけを切り替える。 */
  const toggleSpoiler = useCallback(
    (targetPlaceKey: PlaceKey, localId: string) => {
      if (!mountedRef.current) return;
      const current = itemsByPlaceRef.current[targetPlaceKey]?.find(
        (entry) => entry.localId === localId,
      );
      if (!current?.attachment) return;
      void applyEdit(targetPlaceKey, localId, {
        patch: { spoiler: !current.attachment.spoiler },
      });
    },
    [applyEdit],
  );

  return {
    itemsByPlace,
    addFiles,
    remove,
    clear,
    replace: replaceAtPlace,
    fileFor,
    applyEdit,
    toggleSpoiler,
  };
}

/**
 * Composerから見た現在place用の窓口。状態と資源の所有権はproviderに残し、
 * callbackだけをこのrender時点のplaceへ束縛する。
 */
export function useDraftAttachments({
  placeKey,
}: {
  placeKey: PlaceKey | null;
}) {
  const owner = useContext(DraftAttachmentsContext);
  if (!owner) {
    throw new Error(
      "useDraftAttachments must be used within DraftAttachmentsProvider",
    );
  }

  const items = placeKey
    ? (owner.itemsByPlace[placeKey] ?? NO_DRAFT_ATTACHMENTS)
    : NO_DRAFT_ATTACHMENTS;
  const addFiles = useCallback(
    (files: FileList | File[] | null | undefined) => {
      if (placeKey) owner.addFiles(placeKey, files);
    },
    [owner.addFiles, placeKey],
  );
  const remove = useCallback(
    (localId: string) => {
      if (placeKey) owner.remove(placeKey, localId);
    },
    [owner.remove, placeKey],
  );
  const clear = useCallback(() => {
    if (placeKey) owner.clear(placeKey);
  }, [owner.clear, placeKey]);
  const replace = useCallback(
    (localId: string, next: Partial<DraftAttachment>, file?: File) => {
      if (placeKey) owner.replace(placeKey, localId, next, file);
    },
    [owner.replace, placeKey],
  );
  const fileFor = useCallback(
    (localId: string) =>
      placeKey ? owner.fileFor(placeKey, localId) : undefined,
    [owner.fileFor, placeKey],
  );
  const applyEdit = useCallback(
    (localId: string, edit: AttachmentEdit) =>
      placeKey ? owner.applyEdit(placeKey, localId, edit) : Promise.resolve(),
    [owner.applyEdit, placeKey],
  );
  const toggleSpoiler = useCallback(
    (localId: string) => {
      if (placeKey) owner.toggleSpoiler(placeKey, localId);
    },
    [owner.toggleSpoiler, placeKey],
  );

  const uploading = items.some((entry) => entry.status === "uploading");
  // attachment objectが残っていても、失敗・処理中のidはsendへ出さない。
  const ready = items.flatMap((entry) =>
    entry.status === "ready" && entry.attachment ? [entry.attachment] : [],
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
