import {
  Crop,
  EyeOff,
  PenLine,
  RotateCcw,
  SlidersHorizontal,
  Square,
  X,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { AttachmentDraftPatch } from "../model";

/**
 * 送信前の添付を整えるモーダル。ここで決めるのは「送る前に決められること」
 * だけで、送った後の書き換えではない（サーバーも送信済みの添付の編集を
 * 拒む）。
 *
 * 3つの宣言（ファイル名・概要・ネタバレ）に加えて、画像には軽い加工を持つ。
 * 「送る前に一部を隠す」「不要な周りを落とす」は送信の一部であって、外部の
 * 画像編集アプリへ往復させる作業ではない、という判断。
 */

type Tool = "pen" | "redact" | "crop" | "tone";

export interface AttachmentEdit {
  patch: AttachmentDraftPatch;
  /** 画像加工の結果。加工していなければ undefined。 */
  editedFile?: File;
}

const TOOLS: { value: Tool; label: string; icon: typeof PenLine }[] = [
  { value: "pen", label: "ペン", icon: PenLine },
  { value: "redact", label: "黒塗り", icon: Square },
  { value: "crop", label: "トリミング", icon: Crop },
  { value: "tone", label: "色調", icon: SlidersHorizontal },
];

const PEN_COLOR = "#ef4444";
const PEN_WIDTH = 4;

interface Point {
  x: number;
  y: number;
}

/** キャンバス座標の矩形（ドラッグの始点と現在地）。 */
interface Rect {
  from: Point;
  to: Point;
}

/** キャンバス実寸の矩形を、表示中のキャンバスに重ねる % 位置へ。 */
function overlayStyle(canvas: HTMLCanvasElement | null, rect: Rect) {
  if (!canvas || canvas.width === 0 || canvas.height === 0) return undefined;
  const left = Math.min(rect.from.x, rect.to.x) / canvas.width;
  const top = Math.min(rect.from.y, rect.to.y) / canvas.height;
  const width = Math.abs(rect.to.x - rect.from.x) / canvas.width;
  const height = Math.abs(rect.to.y - rect.from.y) / canvas.height;
  return {
    left: `${left * 100}%`,
    top: `${top * 100}%`,
    width: `${width * 100}%`,
    height: `${height * 100}%`,
  };
}

/** キャンバス座標へ。表示は縮小されているので比率で戻す。 */
function canvasPoint(canvas: HTMLCanvasElement, event: React.PointerEvent) {
  const rect = canvas.getBoundingClientRect();
  return {
    x: ((event.clientX - rect.left) / rect.width) * canvas.width,
    y: ((event.clientY - rect.top) / rect.height) * canvas.height,
  };
}

export function AttachmentEditModal({
  filename,
  alt,
  spoiler,
  file,
  imageUrl,
  onCancel,
  onApply,
}: {
  filename: string;
  alt: string;
  spoiler: boolean;
  /** 画像加工の元。画像でなければ渡さない。 */
  file?: File;
  imageUrl?: string;
  onCancel: () => void;
  onApply: (edit: AttachmentEdit) => void;
}) {
  const [nextFilename, setNextFilename] = useState(filename);
  const [nextAlt, setNextAlt] = useState(alt);
  const [nextSpoiler, setNextSpoiler] = useState(spoiler);
  const [tool, setTool] = useState<Tool>("pen");
  const [brightness, setBrightness] = useState(100);
  const [contrast, setContrast] = useState(100);
  const [dirty, setDirty] = useState(false);
  const [cropRect, setCropRect] = useState<Rect | null>(null);
  const [redactPreview, setRedactPreview] = useState<Rect | null>(null);

  const canvasRef = useRef<HTMLCanvasElement>(null);
  const sourceRef = useRef<HTMLImageElement | null>(null);
  const drawing = useRef(false);
  // 押した点（黒塗り・トリミングの始点）と直前の点（ペンの線分）は別物。
  const start = useRef<Point | null>(null);
  const last = useRef<Point | null>(null);
  const editable = Boolean(imageUrl);

  // 元画像を読み込んでキャンバスの初期状態にする。取り消しはここへ戻る。
  const reset = useCallback(() => {
    const canvas = canvasRef.current;
    const source = sourceRef.current;
    if (!canvas || !source) return;
    canvas.width = source.naturalWidth;
    canvas.height = source.naturalHeight;
    const context = canvas.getContext("2d");
    if (!context) return;
    context.filter = "none";
    context.clearRect(0, 0, canvas.width, canvas.height);
    context.drawImage(source, 0, 0);
    setBrightness(100);
    setContrast(100);
    setCropRect(null);
    setDirty(false);
  }, []);

  useEffect(() => {
    if (!imageUrl) return;
    const image = new Image();
    image.onload = () => {
      sourceRef.current = image;
      reset();
    };
    image.src = imageUrl;
    return () => {
      image.onload = null;
    };
  }, [imageUrl, reset]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.stopPropagation();
      onCancel();
    };
    document.addEventListener("keydown", onKeyDown, true);
    return () => document.removeEventListener("keydown", onKeyDown, true);
  }, [onCancel]);

  /** 色調は元画像から描き直す（適用の重ねがけで潰れないように）。 */
  const applyTone = useCallback(
    (nextBrightness: number, nextContrast: number) => {
      const canvas = canvasRef.current;
      const source = sourceRef.current;
      if (!canvas || !source) return;
      const context = canvas.getContext("2d");
      if (!context) return;
      context.filter = `brightness(${nextBrightness}%) contrast(${nextContrast}%)`;
      context.clearRect(0, 0, canvas.width, canvas.height);
      context.drawImage(source, 0, 0);
      context.filter = "none";
      setDirty(true);
    },
    [],
  );

  const onPointerDown = (event: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas || tool === "tone") return;
    canvas.setPointerCapture(event.pointerId);
    drawing.current = true;
    const point = canvasPoint(canvas, event);
    start.current = point;
    last.current = point;
    if (tool === "crop") setCropRect({ from: point, to: point });
  };

  const onPointerMove = (event: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas || !drawing.current) return;
    const point = canvasPoint(canvas, event);
    if (tool === "crop") {
      setCropRect((current) =>
        current ? { from: current.from, to: point } : null,
      );
      return;
    }
    if (tool === "redact") {
      // 黒塗りは離した時点の矩形を一度だけ塗る（途中経過は塗らない）。
      setRedactPreview({ from: start.current ?? point, to: point });
      return;
    }
    const context = canvas.getContext("2d");
    if (!context || !last.current) return;
    if (tool === "pen") {
      context.strokeStyle = PEN_COLOR;
      context.lineWidth = PEN_WIDTH;
      context.lineCap = "round";
      context.beginPath();
      context.moveTo(last.current.x, last.current.y);
      context.lineTo(point.x, point.y);
      context.stroke();
    }
    last.current = point;
    setDirty(true);
  };

  const onPointerUp = (event: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas || !drawing.current) return;
    drawing.current = false;
    canvas.releasePointerCapture(event.pointerId);
    const point = canvasPoint(canvas, event);
    if (tool === "redact" && start.current) {
      const context = canvas.getContext("2d");
      if (context) {
        const from = start.current;
        context.fillStyle = "#000000";
        context.fillRect(
          Math.min(from.x, point.x),
          Math.min(from.y, point.y),
          Math.abs(point.x - from.x),
          Math.abs(point.y - from.y),
        );
        setDirty(true);
      }
      setRedactPreview(null);
    }
    start.current = null;
    last.current = null;
  };

  const applyCrop = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas || !cropRect) return;
    const x = Math.round(Math.min(cropRect.from.x, cropRect.to.x));
    const y = Math.round(Math.min(cropRect.from.y, cropRect.to.y));
    const width = Math.round(Math.abs(cropRect.to.x - cropRect.from.x));
    const height = Math.round(Math.abs(cropRect.to.y - cropRect.from.y));
    if (width < 8 || height < 8) return;
    const context = canvas.getContext("2d");
    if (!context) return;
    const slice = context.getImageData(x, y, width, height);
    canvas.width = width;
    canvas.height = height;
    const cropped = canvas.getContext("2d");
    if (!cropped) return;
    cropped.putImageData(slice, 0, 0);
    // 切り抜いた結果を以後の「元画像」にする（色調はここから掛け直す）。
    const snapshot = new Image();
    snapshot.onload = () => {
      sourceRef.current = snapshot;
    };
    snapshot.src = canvas.toDataURL("image/png");
    setCropRect(null);
    setDirty(true);
  }, [cropRect]);

  const apply = useCallback(() => {
    const patch: AttachmentDraftPatch = {};
    const trimmedName = nextFilename.trim();
    if (trimmedName && trimmedName !== filename) patch.filename = trimmedName;
    if (nextAlt !== alt) patch.alt = nextAlt;
    if (nextSpoiler !== spoiler) patch.spoiler = nextSpoiler;
    const canvas = canvasRef.current;
    if (!dirty || !canvas || !file) {
      onApply({ patch });
      return;
    }
    canvas.toBlob((blob) => {
      if (!blob) {
        onApply({ patch });
        return;
      }
      // 加工結果はPNGで送る。元がJPEGでも、塗り潰しの縁が滲まない。
      const name = (patch.filename ?? trimmedName ?? filename).replace(
        /\.[^.]+$/,
        ".png",
      );
      onApply({
        patch: { ...patch, filename: name },
        editedFile: new File([blob], name, { type: "image/png" }),
      });
    }, "image/png");
  }, [
    nextFilename,
    filename,
    nextAlt,
    alt,
    nextSpoiler,
    spoiler,
    dirty,
    file,
    onApply,
  ]);

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4">
      <div
        role="dialog"
        aria-modal="true"
        aria-label="添付ファイルを編集"
        className="flex max-h-full w-full max-w-2xl flex-col overflow-hidden rounded-xl border border-border bg-background shadow-xl"
      >
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
          <label className="block">
            <span className="mb-1 block font-medium text-[12px]">
              ファイル名
            </span>
            <input
              value={nextFilename}
              onChange={(event) => setNextFilename(event.target.value)}
              className="w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none focus:border-ring/60"
            />
          </label>

          <label className="block">
            <span className="mb-1 block font-medium text-[12px]">概要</span>
            <textarea
              value={nextAlt}
              onChange={(event) => setNextAlt(event.target.value)}
              maxLength={1000}
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
            スポイラーとしてマーク
          </label>

          {editable ? (
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-1">
                {TOOLS.map((entry) => (
                  <button
                    key={entry.value}
                    type="button"
                    onClick={() => setTool(entry.value)}
                    aria-pressed={tool === entry.value}
                    className={`flex items-center gap-1 rounded-md border px-2 py-1 text-[12px] transition-colors ${
                      tool === entry.value
                        ? "border-ring/60 bg-accent"
                        : "border-border hover:bg-accent"
                    }`}
                  >
                    <entry.icon className="size-3.5" />
                    {entry.label}
                  </button>
                ))}
                <button
                  type="button"
                  onClick={reset}
                  className="ml-auto flex items-center gap-1 rounded-md border border-border px-2 py-1 text-[12px] transition-colors hover:bg-accent"
                >
                  <RotateCcw className="size-3.5" />
                  加工を取り消す
                </button>
              </div>

              {tool === "tone" ? (
                <div className="flex flex-wrap items-center gap-4 rounded-md border border-border bg-muted/30 px-3 py-2 text-[12px]">
                  <label className="flex items-center gap-2">
                    明度
                    <input
                      type="range"
                      min={50}
                      max={150}
                      value={brightness}
                      onChange={(event) => {
                        const value = Number(event.target.value);
                        setBrightness(value);
                        applyTone(value, contrast);
                      }}
                    />
                  </label>
                  <label className="flex items-center gap-2">
                    コントラスト
                    <input
                      type="range"
                      min={50}
                      max={150}
                      value={contrast}
                      onChange={(event) => {
                        const value = Number(event.target.value);
                        setContrast(value);
                        applyTone(brightness, value);
                      }}
                    />
                  </label>
                </div>
              ) : null}

              {tool === "crop" ? (
                <div className="flex items-center gap-2 text-[12px] text-muted-foreground">
                  残す範囲をドラッグして選ぶ
                  <button
                    type="button"
                    onClick={applyCrop}
                    disabled={!cropRect}
                    className="rounded-md border border-border px-2 py-0.5 transition-colors hover:bg-accent disabled:opacity-40"
                  >
                    切り抜く
                  </button>
                </div>
              ) : null}

              <div className="flex justify-center rounded-md border border-border bg-muted/20 p-2">
                <span className="relative inline-block">
                  <canvas
                    ref={canvasRef}
                    aria-label="画像の加工"
                    onPointerDown={onPointerDown}
                    onPointerMove={onPointerMove}
                    onPointerUp={onPointerUp}
                    className="block max-h-[40vh] max-w-full touch-none cursor-crosshair object-contain"
                  />
                  {cropRect ? (
                    <span
                      className="pointer-events-none absolute border-2 border-ring border-dashed bg-background/20"
                      style={overlayStyle(canvasRef.current, cropRect)}
                    />
                  ) : null}
                  {redactPreview ? (
                    <span
                      className="pointer-events-none absolute bg-black/70"
                      style={overlayStyle(canvasRef.current, redactPreview)}
                    />
                  ) : null}
                </span>
              </div>
            </div>
          ) : null}
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
    </div>,
    document.body,
  );
}
