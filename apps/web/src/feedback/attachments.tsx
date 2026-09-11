import { useEffect, useEffectEvent, useRef, useState } from "react";
import { createPortal, flushSync } from "react-dom";
import { type FeedbackAttachment, uploadFeedbackAttachment } from "./api";
import {
  canCaptureScreenshot,
  canRecordScreen,
  captureScreenshot,
  MAX_RECORDING_BYTES,
  type ScreenRecording,
  startRecording,
} from "./capture";
import "./attachments.css";

const MAX_ATTACHMENTS = 5;
const ACCEPT = "image/png,image/jpeg,image/webp,video/webm,video/mp4";

interface Props {
  attachments: FeedbackAttachment[];
  onChange(attachments: FeedbackAttachment[]): void;
  disabled?: boolean;
  onBusyChange?(busy: boolean): void;
  onCaptureChange?(capturing: boolean): void;
}

function mediaURL(url: string): string | undefined {
  try {
    const parsed = new URL(url, window.location.href);
    return ["https:", "http:"].includes(parsed.protocol)
      ? parsed.href
      : undefined;
  } catch {
    return undefined;
  }
}

export function MediaPreview({
  attachments,
}: {
  attachments: FeedbackAttachment[];
}) {
  if (!attachments.length) return null;
  return (
    <div className="feedback-media-list">
      {attachments.map((attachment) => {
        const url = mediaURL(attachment.url);
        return (
          <figure className="feedback-media" key={attachment.id}>
            {url && attachment.mime_type.startsWith("image/") ? (
              <a href={url} target="_blank" rel="noopener noreferrer">
                <img src={url} alt={attachment.name} loading="lazy" />
              </a>
            ) : url && attachment.mime_type.startsWith("video/") ? (
              // biome-ignore lint/a11y/useMediaCaption: Screen recordings have no audio; uploaded videos have no supplied caption track to render.
              <video
                src={url}
                controls
                playsInline
                preload="metadata"
                aria-label={attachment.name}
              />
            ) : null}
            <figcaption>
              {url ? (
                <a href={url} target="_blank" rel="noopener noreferrer">
                  {attachment.name}
                </a>
              ) : (
                attachment.name
              )}
            </figcaption>
          </figure>
        );
      })}
    </div>
  );
}

export function FeedbackAttachments(props: Props) {
  const { attachments, disabled = false } = props;
  const latest = useRef(props);
  latest.current = props;
  const input = useRef<HTMLInputElement>(null);
  const root = useRef<HTMLElement>(null);
  const operation = useRef<AbortController | null>(null);
  const recording = useRef<ScreenRecording | null>(null);
  const mounted = useRef(true);
  const [busy, setBusy] = useState(false);
  const [phase, setPhase] = useState<
    "idle" | "choosing" | "recording" | "uploading"
  >("idle");
  const [error, setError] = useState("");
  const [retryFiles, setRetryFiles] = useState<File[]>([]);
  const pendingFiles = useRef<File[]>([]);
  const pasteFiles = useEffectEvent((files: File[]) => addFiles(files));

  function rememberFiles(files: File[]) {
    pendingFiles.current = files;
    setRetryFiles(files);
  }

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      operation.current?.abort();
      operation.current = null;
      recording.current?.cancel();
      recording.current = null;
      latest.current.onBusyChange?.(false);
      latest.current.onCaptureChange?.(false);
    };
  }, []);

  useEffect(() => {
    const target = root.current?.closest("form") ?? root.current;
    const paste = (event: ClipboardEvent) => {
      const files = Array.from(event.clipboardData?.files ?? []);
      if (
        !files.length ||
        operation.current ||
        latest.current.disabled ||
        latest.current.attachments.length + pendingFiles.current.length >=
          MAX_ATTACHMENTS
      )
        return;
      event.preventDefault();
      pasteFiles(files);
    };
    target?.addEventListener("paste", paste as EventListener);
    return () => target?.removeEventListener("paste", paste as EventListener);
  }, []);

  function begin(capture = false) {
    if (
      operation.current ||
      latest.current.disabled ||
      latest.current.attachments.length >= MAX_ATTACHMENTS ||
      (capture &&
        latest.current.attachments.length + pendingFiles.current.length >=
          MAX_ATTACHMENTS)
    )
      return null;
    const controller = new AbortController();
    operation.current = controller;
    setError("");
    setBusy(true);
    latest.current.onBusyChange?.(true);
    // Commit composer hiding before invoking getDisplayMedia in this same click.
    if (capture) flushSync(() => latest.current.onCaptureChange?.(true));
    return controller;
  }

  function finish(controller: AbortController) {
    if (!mounted.current || operation.current !== controller) return;
    operation.current = null;
    recording.current = null;
    setBusy(false);
    setPhase("idle");
    latest.current.onBusyChange?.(false);
    latest.current.onCaptureChange?.(false);
  }

  function report(reason: unknown) {
    if (!mounted.current) return;
    const name = reason instanceof Error ? reason.name : "";
    if (name === "AbortError" || name === "NotAllowedError") return;
    const message = reason instanceof Error ? reason.message : "";
    setError(
      message && message !== "attachment_upload_failed" && name !== "TypeError"
        ? message
        : "添付できませんでした。ファイルは残っています。接続を確認して再試行してください。",
    );
  }

  async function upload(files: File[], controller: AbortController) {
    if (controller.signal.aborted) return;
    files = [...new Set([...pendingFiles.current, ...files])];
    setPhase("uploading");
    latest.current.onCaptureChange?.(false);
    const remaining = MAX_ATTACHMENTS - latest.current.attachments.length;
    if (files.length > remaining) {
      setError("添付できるファイルは5件までです。ファイルを減らしてください。");
      finish(controller);
      return;
    }
    const saved = [...latest.current.attachments];
    for (let index = 0; index < files.length; index++) {
      const file = files[index];
      try {
        if (!ACCEPT.split(",").includes(file.type.split(";")[0]))
          throw new Error(
            `${file.name}: PNG・JPEG・WebP画像、またはWebM・MP4動画を選んでください。`,
          );
        if (file.size === 0 || file.size > MAX_RECORDING_BYTES)
          throw new Error(
            `${file.name}: ファイルは20 MiB以内で、空でないものを選んでください。`,
          );
        const attachment = await uploadFeedbackAttachment(
          file,
          controller.signal,
        );
        if (controller.signal.aborted || !mounted.current) return;
        saved.push(attachment);
        latest.current.onChange([...saved]);
      } catch (reason) {
        if (controller.signal.aborted || !mounted.current) return;
        rememberFiles(files.slice(index));
        report(reason);
        finish(controller);
        return;
      }
    }
    rememberFiles([]);
    finish(controller);
  }

  function addFiles(files: File[], retry = false) {
    if (!files.length) return;
    if (
      !retry &&
      latest.current.attachments.length + pendingFiles.current.length >=
        MAX_ATTACHMENTS
    )
      return;
    const controller = begin();
    if (controller) void upload(files, controller);
  }

  function screenshot() {
    const controller = begin(true);
    if (!controller) return;
    // This call must remain before any await, timer, or animation frame.
    void captureScreenshot(controller.signal)
      .then((file) => upload([file], controller))
      .catch((reason) => {
        if (!controller.signal.aborted) report(reason);
        finish(controller);
      });
  }

  function record() {
    const controller = begin(true);
    if (!controller) return;
    setPhase("choosing");
    void startRecording(
      (file) => {
        recording.current = null;
        void upload([file], controller);
      },
      (reason) => {
        report(reason);
        finish(controller);
      },
      controller.signal,
    )
      .then((control) => {
        if (
          controller.signal.aborted ||
          !mounted.current ||
          operation.current !== controller
        ) {
          control.cancel();
          return;
        }
        recording.current = control;
        setPhase("recording");
      })
      .catch((reason) => {
        if (!controller.signal.aborted) report(reason);
        finish(controller);
      });
  }

  const blocked = disabled || busy || attachments.length >= MAX_ATTACHMENTS;
  const additionsBlocked =
    blocked || attachments.length + retryFiles.length >= MAX_ATTACHMENTS;
  return (
    <section
      ref={root}
      className="feedback-attachments"
      aria-label="添付ファイル"
      onDragOver={(event) => {
        if (event.dataTransfer.types.includes("Files")) event.preventDefault();
      }}
      onDrop={(event) => {
        event.preventDefault();
        if (!additionsBlocked) addFiles(Array.from(event.dataTransfer.files));
      }}
    >
      <fieldset
        className="feedback-attachment-actions"
        aria-label="画像と動画の添付"
      >
        <input
          ref={input}
          type="file"
          accept={ACCEPT}
          multiple
          hidden
          disabled={additionsBlocked}
          onChange={(event) => {
            addFiles(Array.from(event.currentTarget.files ?? []));
            event.currentTarget.value = "";
          }}
        />
        <button
          type="button"
          disabled={additionsBlocked}
          onClick={() => input.current?.click()}
        >
          画像・動画を追加
        </button>
        {canCaptureScreenshot() ? (
          <button
            type="button"
            disabled={additionsBlocked}
            onClick={screenshot}
          >
            画面を撮影
          </button>
        ) : null}
        {canRecordScreen() ? (
          <button type="button" disabled={additionsBlocked} onClick={record}>
            画面を録画
          </button>
        ) : null}
        <span>{attachments.length}/5 · 各20 MiBまで</span>
      </fieldset>
      <div className="feedback-attachment-edit-list">
        {attachments.map((attachment) => (
          <div className="feedback-attachment-edit" key={attachment.id}>
            <MediaPreview attachments={[attachment]} />
            <button
              type="button"
              className="feedback-attachment-remove"
              disabled={disabled || busy}
              aria-label={`${attachment.name}を削除`}
              onClick={() =>
                props.onChange(
                  attachments.filter((item) => item.id !== attachment.id),
                )
              }
            >
              ×
            </button>
          </div>
        ))}
      </div>
      {phase === "uploading" ? (
        <p className="feedback-attachment-status" role="status">
          添付ファイルをアップロード中…
        </p>
      ) : null}
      {error ? (
        <p className="feedback-attachment-error" role="alert">
          {error}
        </p>
      ) : null}
      {retryFiles.length ? (
        <div className="feedback-attachment-retry">
          <span>未添付: {retryFiles.map((file) => file.name).join("、")}</span>
          <button
            type="button"
            disabled={blocked}
            onClick={() => addFiles(retryFiles, true)}
          >
            再試行
          </button>
          <button
            type="button"
            disabled={busy || disabled}
            onClick={() => {
              rememberFiles([]);
              setError("");
            }}
          >
            取り消す
          </button>
        </div>
      ) : null}
      {phase === "recording" || phase === "choosing"
        ? createPortal(
            <section
              className="feedback-recording-controls"
              aria-label="画面録画の操作"
            >
              <span role="status">
                {phase === "recording"
                  ? "画面を録画中 · 最大60秒"
                  : "録画する画面を選択してください"}
              </span>
              {phase === "recording" ? (
                <button type="button" onClick={() => recording.current?.stop()}>
                  停止して添付
                </button>
              ) : null}
              <button
                type="button"
                onClick={() => {
                  const controller = operation.current;
                  controller?.abort();
                  recording.current?.cancel();
                  if (controller) finish(controller);
                }}
              >
                取り消す
              </button>
            </section>,
            document.body,
          )
        : null}
    </section>
  );
}
