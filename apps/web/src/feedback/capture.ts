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
    context.drawImage(video, 0, 0);
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
    return new File([blob], `sumi-feedback-${Date.now()}.png`, {
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
