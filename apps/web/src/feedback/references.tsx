import { Camera, Crosshair, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { type FeedbackAttachment, uploadFeedbackAttachment } from "./api";
import { canCaptureRegionScreenshot, captureRegionScreenshot } from "./capture";
import type { DiagnosticAnnotation } from "./diagnostics";
import "./references.css";

const pendingRegionFiles = new Map<string, File>();

export function AnnotationReferences({
  annotations,
  idPrefix = "feedback-reference",
  draftStorageKey,
  resolveAnnotation,
  disabled = false,
  attachmentCount = 0,
  onMention,
  onRemove,
  onReveal,
  onAttachment,
  onBusyChange,
  onCaptureChange,
}: {
  annotations: DiagnosticAnnotation[];
  idPrefix?: string;
  draftStorageKey?: string;
  resolveAnnotation?(
    annotation: DiagnosticAnnotation,
  ): { x: number; y: number; width: number; height: number } | undefined;
  disabled?: boolean;
  attachmentCount?: number;
  onMention?(annotation: DiagnosticAnnotation): void;
  onRemove?(annotation: DiagnosticAnnotation): void;
  onReveal?(annotation: DiagnosticAnnotation): void;
  onAttachment?(attachment: FeedbackAttachment): void;
  onBusyChange?(busy: boolean): void;
  onCaptureChange?(active: boolean): void;
}) {
  const [active, setActive] = useState(false);
  const [pending, setPendingState] = useState<File | undefined>(() =>
    draftStorageKey ? pendingRegionFiles.get(draftStorageKey) : undefined,
  );
  function setPending(file: File | undefined) {
    if (draftStorageKey) {
      if (file) pendingRegionFiles.set(draftStorageKey, file);
      else pendingRegionFiles.delete(draftStorageKey);
    }
    setPendingState(file);
  }
  const [error, setError] = useState("");
  const operation = useRef<AbortController | undefined>(undefined);
  const latest = useRef({
    onAttachment,
    onBusyChange,
    onCaptureChange,
    resolveAnnotation,
  });
  latest.current = {
    onAttachment,
    onBusyChange,
    onCaptureChange,
    resolveAnnotation,
  };
  useEffect(() => {
    latest.current.onBusyChange?.(Boolean(pending));
  }, [pending]);
  useEffect(
    () => () => {
      operation.current?.abort();
      latest.current.onCaptureChange?.(false);
      latest.current.onBusyChange?.(false);
    },
    [],
  );
  const busy = active || Boolean(pending);
  async function upload(file: File, controller: AbortController) {
    setPending(file);
    latest.current.onCaptureChange?.(false);
    const attachment = await uploadFeedbackAttachment(file, controller.signal);
    if (controller.signal.aborted) return;
    latest.current.onAttachment?.(attachment);
    setPending(undefined);
  }
  function finish(controller: AbortController, hasPending: boolean) {
    if (controller.signal.aborted) return;
    operation.current = undefined;
    setActive(false);
    latest.current.onBusyChange?.(hasPending);
    latest.current.onCaptureChange?.(false);
  }
  async function capture(annotation: DiagnosticAnnotation) {
    if (disabled || busy || attachmentCount >= 5) return;
    const isCurrent = () =>
      Boolean(latest.current.resolveAnnotation?.(annotation)) &&
      annotation.path === window.location.pathname &&
      annotation.viewport_width === window.innerWidth &&
      annotation.viewport_height === window.innerHeight &&
      annotation.scroll_x === window.scrollX &&
      annotation.scroll_y === window.scrollY;
    if (!isCurrent()) {
      setError(
        "画面の位置が変わっています。範囲を選び直してから撮影してください。",
      );
      return;
    }
    const controller = new AbortController();
    operation.current = controller;
    setActive(true);
    setError("");
    const target = document.createElement("div");
    target.dataset.feedbackUi = "";
    Object.assign(target.style, {
      position: "fixed",
      pointerEvents: "none",
      zIndex: "2147483647",
      left: `${annotation.rect.x}px`,
      top: `${annotation.rect.y}px`,
      width: `${annotation.rect.width}px`,
      height: `${annotation.rect.height}px`,
    });
    document.body.append(target);
    let file: File | undefined;
    try {
      flushSync(() => {
        latest.current.onBusyChange?.(true);
        latest.current.onCaptureChange?.(true);
      });
      file = await captureRegionScreenshot(
        target,
        annotation.number,
        controller.signal,
        isCurrent,
      );
      target.remove();
      await upload(file, controller);
      finish(controller, false);
    } catch (cause) {
      if (controller.signal.aborted) return;
      const name = cause instanceof Error ? cause.name : "";
      if (file)
        setError(
          "画像は残っています。送信を再試行するか、画像を取り除いてください。",
        );
      else if (name !== "AbortError" && name !== "NotAllowedError")
        setError(
          "この範囲を撮影できませんでした。画面共有ではこのSumiのタブを選んでください。通常の画像添付も使えます。",
        );
      finish(controller, Boolean(file));
    } finally {
      target.remove();
    }
  }
  async function retry() {
    if (!pending || active || disabled || attachmentCount >= 5) return;
    const controller = new AbortController();
    operation.current = controller;
    setActive(true);
    setError("");
    try {
      await upload(pending, controller);
      finish(controller, false);
    } catch {
      if (controller.signal.aborted) return;
      setError("画像を添付できませんでした。もう一度お試しください。");
      finish(controller, true);
    }
  }
  if (!annotations.length && !pending) return null;
  return (
    <section className="feedback-references" aria-label="言及する場所">
      <p className="feedback-references-heading">
        {onMention ? "この場所について" : "言及された場所"}
      </p>
      <ol>
        {annotations.map((annotation) => (
          <li
            key={annotation.number}
            id={`${idPrefix}-${annotation.number}`}
            tabIndex={-1}
          >
            <button
              type="button"
              className="feedback-reference-main"
              disabled={!onMention || disabled || active}
              aria-label={`場所 ${annotation.number}: ${annotation.label}`}
              title={
                onMention ? `本文に [${annotation.number}] を挿入` : undefined
              }
              onClick={() => onMention?.(annotation)}
            >
              <span className="feedback-reference-number">
                {annotation.number}
              </span>
              <span className="feedback-reference-label">
                {annotation.label ||
                  (annotation.kind === "region"
                    ? "選択した範囲"
                    : "選択した要素")}
                <small>
                  {annotation.kind === "region"
                    ? `${annotation.rect.width} × ${annotation.rect.height} px の範囲`
                    : annotation.tag}
                </small>
              </span>
            </button>
            {onReveal && (
              <button
                type="button"
                className="feedback-reference-action"
                title="画面上の場所を確認"
                aria-label={`場所 ${annotation.number} を画面で確認`}
                onClick={() => onReveal(annotation)}
                disabled={active}
              >
                <Crosshair size={15} />
              </button>
            )}
            {onAttachment &&
              annotation.kind === "region" &&
              canCaptureRegionScreenshot() && (
                <button
                  type="button"
                  className="feedback-reference-action"
                  title="この範囲を画像にする"
                  aria-label={`範囲 ${annotation.number} を画像にする`}
                  onClick={() => void capture(annotation)}
                  disabled={disabled || busy || attachmentCount >= 5}
                >
                  <Camera size={15} />
                </button>
              )}
            {onRemove && (
              <button
                type="button"
                className="feedback-reference-action"
                title="参照を取り除く"
                aria-label={`場所 ${annotation.number} の参照を取り除く`}
                onClick={() => onRemove(annotation)}
                disabled={disabled || busy}
              >
                <X size={14} />
              </button>
            )}
          </li>
        ))}
      </ol>
      {active && (
        <p role="status" className="feedback-reference-note">
          画像を準備しています…
        </p>
      )}
      {error && (
        <p role="alert" className="feedback-reference-note">
          {error}
        </p>
      )}
      {pending && !active && (
        <div className="feedback-reference-retry">
          <button
            type="button"
            disabled={disabled || attachmentCount >= 5}
            onClick={() => void retry()}
          >
            画像の添付を再試行
          </button>
          <button
            type="button"
            onClick={() => {
              setPending(undefined);
              setError("");
              latest.current.onBusyChange?.(false);
            }}
          >
            画像を取り除く
          </button>
        </div>
      )}
    </section>
  );
}
