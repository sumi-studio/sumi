import { EyeOff, X } from "lucide-react";
import { useRef, useState } from "react";
import { clampCodePoints } from "../../lib/text-length";
import { sanitizeAttachmentDisplayText } from "../attachment-display";
import type { AttachmentDraftPatch } from "../model";
import { MAX_ATTACHMENT_ALT_LENGTH } from "../model";
import { ModalDialog } from "./modal-dialog";

/**
 * 送信前の添付に付ける三つの宣言——表示名・説明・ネタバレ——を決めるダイアログ。
 *
 * ここで決めるのは「送る前に決められること」だけで、送ったあとの書き換えでは
 * ない（サーバーも送信済みの添付の編集を拒む）。変えなかった項目はpatchに
 * 載せない: 一つを触ったせいで他が黙って戻る、が一番困る。
 */
export function AttachmentEditDialog({
  filename,
  alt,
  spoiler,
  previewUrl,
  onCancel,
  onApply,
}: {
  filename: string;
  alt: string;
  spoiler: boolean;
  previewUrl?: string;
  onCancel: () => void;
  onApply: (patch: AttachmentDraftPatch) => void;
}) {
  const [nextFilename, setNextFilename] = useState(filename);
  const [nextAlt, setNextAlt] = useState(alt);
  const [nextSpoiler, setNextSpoiler] = useState(spoiler);
  const filenameRef = useRef<HTMLInputElement>(null);

  const apply = () => {
    const patch: AttachmentDraftPatch = {};
    const trimmed = sanitizeAttachmentDisplayText(nextFilename).trim();
    const clampedAlt = clampCodePoints(
      sanitizeAttachmentDisplayText(nextAlt),
      MAX_ATTACHMENT_ALT_LENGTH,
    );
    if (trimmed && trimmed !== filename) patch.filename = trimmed;
    if (clampedAlt !== alt) patch.alt = clampedAlt;
    if (nextSpoiler !== spoiler) patch.spoiler = nextSpoiler;
    onApply(patch);
  };

  return (
    <ModalDialog
      label="添付ファイルを編集"
      onClose={onCancel}
      initialFocusRef={filenameRef}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
    >
      <div className="flex max-h-full w-full max-w-md flex-col overflow-hidden rounded-xl border border-border bg-background shadow-xl">
        <div className="flex items-center justify-between border-border border-b px-4 py-3">
          <h2 className="font-semibold text-[14px]">添付ファイルを編集</h2>
          <button
            type="button"
            onClick={onCancel}
            aria-label="編集を閉じる"
            title="閉じる"
            className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <X className="size-4" />
          </button>
        </div>

        <div className="flex-1 space-y-4 overflow-y-auto px-4 py-4">
          {previewUrl ? (
            <div className="flex justify-center rounded-md border border-border bg-muted/20 p-2">
              <img
                src={previewUrl}
                alt={`${filename} のプレビュー`}
                className={`max-h-40 max-w-full object-contain ${
                  nextSpoiler ? "blur-md" : ""
                }`}
              />
            </div>
          ) : null}

          <label className="block">
            <span className="mb-1 block font-medium text-[12px]">
              ファイル名
            </span>
            <input
              ref={filenameRef}
              value={nextFilename}
              onChange={(event) => setNextFilename(event.target.value)}
              className="w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none focus:border-ring/60"
            />
          </label>

          <label className="block">
            <span className="mb-1 block font-medium text-[12px]">説明</span>
            <textarea
              value={nextAlt}
              onChange={(event) =>
                setNextAlt(
                  clampCodePoints(
                    event.target.value,
                    MAX_ATTACHMENT_ALT_LENGTH,
                  ),
                )
              }
              rows={2}
              placeholder="中身を見なくても何か分かる説明"
              className="w-full resize-none rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/70 focus:border-ring/60"
            />
          </label>

          <label className="flex items-center gap-2 text-[13px]">
            <input
              type="checkbox"
              checked={nextSpoiler}
              onChange={(event) => setNextSpoiler(event.target.checked)}
              className="size-4 accent-current"
            />
            <EyeOff className="size-3.5 text-muted-foreground" />
            ネタバレとしてマークする
          </label>
        </div>

        <div className="flex items-center justify-end gap-2 border-border border-t px-4 py-3">
          <button
            type="button"
            onClick={onCancel}
            className="rounded-md border border-border px-3 py-1.5 text-[13px] transition-colors hover:bg-accent"
          >
            キャンセル
          </button>
          <button
            type="button"
            onClick={apply}
            className="rounded-md bg-foreground px-3 py-1.5 font-medium text-[13px] text-background transition-opacity hover:opacity-90"
          >
            保存
          </button>
        </div>
      </div>
    </ModalDialog>
  );
}
