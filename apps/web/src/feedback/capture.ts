export const MAX_RECORDING_DURATION_MS = 60_000;
export const MAX_RECORDING_BYTES = 20 * 1024 * 1024;

export interface ScreenRecording {
  stop(): void;
  cancel(): void;
}

export function canCaptureScreenshot(): boolean {
  return (
    typeof navigator !== "undefined" &&
    typeof navigator.mediaDevices?.getDisplayMedia === "function"
  );
}

export function canRecordScreen(): boolean {
  return canCaptureScreenshot() && typeof MediaRecorder !== "undefined";
}

// Region Capture is not yet declared by the project's DOM TypeScript library.
type RegionTrack = MediaStreamTrack & { cropTo(target: object): Promise<void> };
function regionCaptureAPI() {
  return globalThis as typeof globalThis & {
    CropTarget?: { fromElement(element: HTMLElement): Promise<object> };
    BrowserCaptureMediaStreamTrack?: { prototype: Partial<RegionTrack> };
  };
}

export function canCaptureRegionScreenshot(): boolean {
  const api = regionCaptureAPI();
  return (
    canCaptureScreenshot() &&
    typeof api.CropTarget?.fromElement === "function" &&
    typeof api.BrowserCaptureMediaStreamTrack?.prototype.cropTo === "function"
  );
}

function stopTracks(stream: MediaStream): void {
  for (const track of stream.getTracks()) track.stop();
}

function requireVideo(stream: MediaStream): MediaStreamTrack {
  // Audio is never requested; discard it defensively if a browser supplies it.
  for (const track of stream.getAudioTracks()) {
    track.stop();
    stream.removeTrack(track);
  }
  const track = stream.getVideoTracks()[0];
  if (!track || track.readyState === "ended") {
    throw new Error("Screen sharing ended before capture could start.");
  }
  return track;
}

function waitForFrame(
  video: HTMLVideoElement,
  track: MediaStreamTrack,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timeout);
      video.removeEventListener("loadeddata", ready);
      video.removeEventListener("error", failed);
      track.removeEventListener("ended", ended);
      signal?.removeEventListener("abort", aborted);
    };
    const fail = (error: Error) => {
      cleanup();
      reject(error);
    };
    const ready = () => {
      if (
        video.readyState >= 2 &&
        video.videoWidth > 0 &&
        video.videoHeight > 0
      ) {
        cleanup();
        resolve();
      }
    };
    const failed = () =>
      fail(new Error("The shared screen could not be read."));
    const ended = () =>
      fail(
        new Error("Screen sharing ended before the screenshot was captured."),
      );
    const aborted = () =>
      fail(new DOMException("Screen capture was canceled.", "AbortError"));
    const timeout = setTimeout(
      () => fail(new Error("Timed out waiting for the shared screen.")),
      10_000,
    );
    video.addEventListener("loadeddata", ready);
    video.addEventListener("error", failed);
    track.addEventListener("ended", ended);
    signal?.addEventListener("abort", aborted, { once: true });
    if (signal?.aborted) {
      aborted();
      return;
    }
    void video.play().then(ready, failed);
    ready();
  });
}

/** Call directly from a user click handler to preserve browser activation. */
export async function captureScreenshot(signal?: AbortSignal): Promise<File> {
  signal?.throwIfAborted();
  if (!canCaptureScreenshot())
    throw new Error("Screen capture is unavailable. Upload an image instead.");
  // Do not put an await before getDisplayMedia: the browser requires a user gesture.
  const stream = await navigator.mediaDevices.getDisplayMedia({
    video: true,
    audio: false,
  });
  return screenshotFile(stream, `sumi-feedback-${Date.now()}.png`, signal);
}

function validateRegionTarget(target: HTMLElement): void {
  const rect = target.getBoundingClientRect();
  if (
    !target.isConnected ||
    target.ownerDocument !== document ||
    rect.width <= 0 ||
    rect.height <= 0 ||
    rect.left < 0 ||
    rect.top < 0 ||
    rect.right > window.innerWidth ||
    rect.bottom > window.innerHeight
  ) {
    throw new Error(
      "選択した範囲が画面内にありません。範囲を選び直してください。",
    );
  }
}

function regionStep<T>(
  pending: Promise<T>,
  track: MediaStreamTrack,
  signal?: AbortSignal,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", aborted);
      track.removeEventListener("ended", ended);
    };
    const fail = (error: unknown) => {
      cleanup();
      reject(error);
    };
    const aborted = () =>
      fail(new DOMException("範囲の撮影を取り消しました。", "AbortError"));
    const ended = () =>
      fail(new Error("範囲の撮影前に画面共有が終了しました。"));
    const timeout = setTimeout(
      () =>
        fail(
          new Error(
            "範囲の撮影が完了しませんでした。画像を添付するか、もう一度お試しください。",
          ),
        ),
      10_000,
    );
    signal?.addEventListener("abort", aborted, { once: true });
    track.addEventListener("ended", ended);
    pending.then((value) => {
      cleanup();
      resolve(value);
    }, fail);
    if (signal?.aborted) aborted();
    else if (track.readyState === "ended") ended();
  });
}

/**
 * Call directly from a click. The caller owns a connected, transparent, empty,
 * fixed-position target with pointer-events:none and keeps it rendered until settled.
 * Only native Region Capture is accepted: no viewport/video coordinate guessing.
 * validate checks the caller's saved page, viewport, and scroll context after
 * asynchronous browser steps and immediately before pixels are read.
 */
export async function captureRegionScreenshot(
  target: HTMLElement,
  annotationNumber: number,
  signal?: AbortSignal,
  validate?: () => boolean,
): Promise<File> {
  signal?.throwIfAborted();
  const cropAPI = regionCaptureAPI().CropTarget;
  if (!canCaptureRegionScreenshot() || !cropAPI) {
    throw new Error(
      "このブラウザーでは選択範囲を撮影できません。範囲の情報を送るか、画像を添付してください。",
    );
  }
  if (!Number.isSafeInteger(annotationNumber) || annotationNumber < 1)
    throw new Error("範囲の番号が正しくありません。");
  validateRegionTarget(target);
  const assertSource = () => {
    if (validate && !validate()) {
      throw new Error(
        "撮影元のページや表示位置が変わりました。範囲を選び直してから撮影してください。",
      );
    }
    validateRegionTarget(target);
  };
  // No await before the native chooser. These are hints; cropTo provides the proof.
  const options: DisplayMediaStreamOptions & {
    preferCurrentTab: boolean;
    surfaceSwitching: "exclude";
  } = {
    video: { displaySurface: "browser" },
    audio: false,
    preferCurrentTab: true,
    surfaceSwitching: "exclude",
  };
  const stream = await navigator.mediaDevices.getDisplayMedia(options);
  const release = () => stopTracks(stream);
  try {
    signal?.throwIfAborted();
    signal?.addEventListener("abort", release, { once: true });
    assertSource();
    const track = requireVideo(stream) as Partial<RegionTrack> &
      MediaStreamTrack;
    if (typeof track.cropTo !== "function") {
      throw new Error(
        "共有する画面には、このSumiのタブを選んでください。ウィンドウや画面全体では範囲を撮影できません。",
      );
    }
    const cropTarget = await regionStep(
      cropAPI.fromElement(target),
      track,
      signal,
    );
    try {
      // A CropTarget minted in this document cannot crop another tab. Resolution
      // guarantees subsequent frames are cropped; attach the video only afterward.
      await regionStep(track.cropTo(cropTarget), track, signal);
    } catch (error) {
      signal?.throwIfAborted();
      throw new Error(
        "選択範囲を撮影できませんでした。共有する画面に、このSumiのタブを選んでください。",
        { cause: error },
      );
    }
    signal?.throwIfAborted();
    assertSource();
    return await screenshotFile(
      stream,
      `sumi-feedback-region-${annotationNumber}-${Date.now()}.png`,
      signal,
      annotationNumber,
      assertSource,
    );
  } finally {
    signal?.removeEventListener("abort", release);
    release();
  }
}

async function screenshotFile(
  stream: MediaStream,
  filename: string,
  signal?: AbortSignal,
  annotationNumber?: number,
  beforeRead?: () => void,
): Promise<File> {
  let video: HTMLVideoElement | undefined;
  try {
    signal?.throwIfAborted();
    const track = requireVideo(stream);
    video = document.createElement("video");
    video.muted = true;
    video.playsInline = true;
    video.srcObject = stream;
    await waitForFrame(video, track, signal);
    signal?.throwIfAborted();
    const canvas = document.createElement("canvas");
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    const context = canvas.getContext("2d");
    if (!context) throw new Error("The screenshot could not be created.");
    beforeRead?.();
    context.drawImage(video, 0, 0);
    if (annotationNumber !== undefined) {
      const unit = Math.min(canvas.width, canvas.height);
      const border = Math.min(3, unit / 12);
      const fontSize = Math.min(20, unit / 3);
      const label = String(annotationNumber);
      const badgeWidth = Math.min(
        canvas.width - border * 2,
        fontSize * (label.length + 1),
      );
      const badgeHeight = Math.min(canvas.height - border * 2, fontSize * 1.4);
      context.strokeStyle = "#e5484d";
      context.lineWidth = border;
      context.strokeRect(
        border / 2,
        border / 2,
        canvas.width - border,
        canvas.height - border,
      );
      context.fillStyle = "#e5484d";
      context.fillRect(border, border, badgeWidth, badgeHeight);
      context.fillStyle = "#ffffff";
      context.font = `bold ${fontSize}px sans-serif`;
      context.textAlign = "center";
      context.textBaseline = "middle";
      context.fillText(
        label,
        border + badgeWidth / 2,
        border + badgeHeight / 2,
        badgeWidth,
      );
    }
    // The frame is now copied; release sharing before PNG encoding finishes.
    stopTracks(stream);
    const blob = await new Promise<Blob>((resolve, reject) => {
      canvas.toBlob(
        (result) =>
          result
            ? resolve(result)
            : reject(new Error("The screenshot could not be encoded.")),
        "image/png",
      );
    });
    signal?.throwIfAborted();
    return new File([blob], filename, {
      type: "image/png",
    });
  } finally {
    stopTracks(stream);
    if (video) {
      video.pause();
      video.srcObject = null;
    }
  }
}

/** Setup failures reject; later failures call onError. cancel() discards all data. */
export async function startRecording(
  onStop: (file: File) => void,
  onError: (error: Error) => void,
  signal?: AbortSignal,
): Promise<ScreenRecording> {
  signal?.throwIfAborted();
  if (!canRecordScreen())
    throw new Error("Screen recording is unavailable. Upload a video instead.");
  const stream = await navigator.mediaDevices.getDisplayMedia({
    video: true,
    audio: false,
  });
  let recorder: MediaRecorder | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let terminal = false;
  let stopping = false;
  let bytes = 0;
  let chunks: Blob[] = [];

  const release = () => {
    clearTimeout(timer);
    for (const track of stream.getVideoTracks())
      track.removeEventListener("ended", stop);
    stopTracks(stream);
  };
  const discard = () => {
    terminal = true;
    signal?.removeEventListener("abort", discard);
    chunks = [];
    if (recorder) {
      recorder.ondataavailable = null;
      recorder.onstop = null;
      recorder.onerror = null;
      try {
        if (recorder.state !== "inactive") recorder.stop();
      } catch {
        // Cancellation still releases the tracks when the recorder itself fails.
      } finally {
        release();
      }
    } else release();
  };
  const fail = (error: Error) => {
    if (terminal) return;
    discard();
    onError(error);
  };
  function stop() {
    if (terminal || stopping) return;
    stopping = true;
    try {
      if (recorder && recorder.state !== "inactive") recorder.stop();
    } catch {
      fail(new Error("The screen recording could not be finished."));
    } finally {
      release();
    }
  }

  try {
    signal?.throwIfAborted();
    requireVideo(stream);
    const mimeType = [
      "video/webm;codecs=vp9",
      "video/webm;codecs=vp8",
      "video/webm",
      "video/mp4",
    ].find((type) => MediaRecorder.isTypeSupported(type));
    recorder = new MediaRecorder(stream, {
      ...(mimeType ? { mimeType } : {}),
      videoBitsPerSecond: 2_000_000,
    });
    recorder.ondataavailable = (event) => {
      if (terminal || event.data.size === 0) return;
      bytes += event.data.size;
      // Chunks can arrive late or exceed the timeslice size. Never emit an oversized file
      // or truncate container bytes, which would create an invalid video.
      if (bytes > MAX_RECORDING_BYTES) {
        fail(
          new Error("The recording exceeded 20 MiB. Record a shorter video."),
        );
        return;
      }
      chunks.push(event.data);
      if (bytes === MAX_RECORDING_BYTES) stop();
    };
    recorder.onerror = () =>
      fail(new Error("Screen recording failed. Try again or upload a video."));
    recorder.onstop = () => {
      release();
      if (terminal) return;
      if (bytes === 0) {
        fail(
          new Error("The recording was empty. Record again or upload a video."),
        );
        return;
      }
      terminal = true;
      signal?.removeEventListener("abort", discard);
      const type =
        recorder?.mimeType || chunks[0]?.type || mimeType || "video/webm";
      const extension = type.split(";")[0] === "video/mp4" ? "mp4" : "webm";
      const file = new File(
        chunks,
        `sumi-feedback-${Date.now()}.${extension}`,
        { type },
      );
      chunks = [];
      onStop(file);
    };
    for (const track of stream.getVideoTracks())
      track.addEventListener("ended", stop);
    recorder.start(250);
    timer = setTimeout(stop, MAX_RECORDING_DURATION_MS);
    signal?.addEventListener("abort", discard, { once: true });
    return { stop, cancel: discard };
  } catch (error) {
    discard();
    throw error;
  }
}
