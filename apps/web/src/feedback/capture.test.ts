// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  canCaptureRegionScreenshot,
  captureRegionScreenshot,
  captureScreenshot,
  MAX_RECORDING_BYTES,
  MAX_RECORDING_DURATION_MS,
  startRecording,
} from "./capture";

class Track extends EventTarget {
  readyState = "live";
  stop = vi.fn(() => {
    this.readyState = "ended";
  });
}

class Recorder {
  static latest: Recorder;
  static isTypeSupported = (type: string) => type === "video/webm";
  state = "inactive";
  mimeType = "video/webm";
  ondataavailable: ((event: { data: Blob }) => void) | null = null;
  onstop: (() => void) | null = null;
  onerror: (() => void) | null = null;
  constructor() {
    Recorder.latest = this;
  }
  start() {
    this.state = "recording";
  }
  stop = vi.fn(() => {
    this.state = "inactive";
  });
  data(size = 10) {
    this.ondataavailable?.({
      data: new Blob([new Uint8Array(size)], { type: this.mimeType }),
    });
  }
  finish() {
    this.data();
    this.onstop?.();
  }
}

let track: Track;
let stream: MediaStream;
let getDisplayMedia: ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.useFakeTimers();
  track = new Track();
  stream = {
    getTracks: () => [track],
    getVideoTracks: () => [track],
    getAudioTracks: () => [],
    removeTrack: vi.fn(),
  } as unknown as MediaStream;
  getDisplayMedia = vi.fn().mockResolvedValue(stream);
  vi.stubGlobal("navigator", { mediaDevices: { getDisplayMedia } });
  vi.stubGlobal("MediaRecorder", Recorder);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("screen recording lifecycle", () => {
  it("opens the video-only chooser synchronously and delivers final data only after stop", async () => {
    const done = vi.fn();
    const pending = startRecording(done, vi.fn());
    expect(getDisplayMedia).toHaveBeenCalledWith({ video: true, audio: false });
    const control = await pending;
    Recorder.latest.data();
    control.stop();
    expect(track.stop).toHaveBeenCalled();
    expect(done).not.toHaveBeenCalled();
    Recorder.latest.finish();
    expect(done).toHaveBeenCalledOnce();
    expect(done.mock.calls[0][0]).toMatchObject({
      size: 20,
      type: "video/webm",
    });
  });

  it("finishes when the user ends sharing", async () => {
    const done = vi.fn();
    await startRecording(done, vi.fn());
    track.dispatchEvent(new Event("ended"));
    expect(Recorder.latest.stop).toHaveBeenCalledOnce();
    Recorder.latest.finish();
    expect(done).toHaveBeenCalledOnce();
  });

  it("stops at the duration limit and clears the timer on cancel", async () => {
    const control = await startRecording(vi.fn(), vi.fn());
    vi.advanceTimersByTime(MAX_RECORDING_DURATION_MS - 1);
    expect(Recorder.latest.stop).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(Recorder.latest.stop).toHaveBeenCalledOnce();
    control.cancel();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects oversized data without returning a truncated recording", async () => {
    const done = vi.fn();
    const error = vi.fn();
    await startRecording(done, error);
    Recorder.latest.data(MAX_RECORDING_BYTES + 1);
    Recorder.latest.finish();
    expect(error).toHaveBeenCalledOnce();
    expect(done).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("cancels even after stop while final data is pending", async () => {
    const done = vi.fn();
    const error = vi.fn();
    const abort = new AbortController();
    const control = await startRecording(done, error, abort.signal);
    control.stop();
    abort.abort();
    Recorder.latest.finish();
    expect(done).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
  });

  it("cleans up when recording setup throws", async () => {
    vi.spyOn(Recorder.prototype, "start").mockImplementation(() => {
      throw new Error("unavailable");
    });
    await expect(startRecording(vi.fn(), vi.fn())).rejects.toThrow(
      "unavailable",
    );
    expect(track.stop).toHaveBeenCalled();
  });

  it("reports recording failure once and discards partial output", async () => {
    const done = vi.fn();
    const error = vi.fn();
    await startRecording(done, error);
    Recorder.latest.data();
    Recorder.latest.onerror?.();
    Recorder.latest.finish();
    expect(error).toHaveBeenCalledOnce();
    expect(done).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
  });

  it.each([
    "screenshot",
    "recording",
  ])("releases a late chooser result after %s cancellation", async (kind) => {
    let resolve!: (value: MediaStream) => void;
    getDisplayMedia.mockReturnValue(
      new Promise<MediaStream>((r) => {
        resolve = r;
      }),
    );
    const abort = new AbortController();
    const pending =
      kind === "screenshot"
        ? captureScreenshot(abort.signal)
        : startRecording(vi.fn(), vi.fn(), abort.signal);
    abort.abort();
    resolve(stream);
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(track.stop).toHaveBeenCalled();
  });
});

it("copies a screenshot frame and ends sharing before PNG encoding", async () => {
  vi.spyOn(HTMLMediaElement.prototype, "play").mockResolvedValue();
  vi.spyOn(HTMLMediaElement.prototype, "pause").mockImplementation(() => {});
  vi.spyOn(HTMLMediaElement.prototype, "readyState", "get").mockReturnValue(2);
  vi.spyOn(HTMLVideoElement.prototype, "videoWidth", "get").mockReturnValue(
    640,
  );
  vi.spyOn(HTMLVideoElement.prototype, "videoHeight", "get").mockReturnValue(
    480,
  );
  const drawImage = vi.fn();
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue({
    drawImage,
  } as unknown as CanvasRenderingContext2D);
  vi.spyOn(HTMLCanvasElement.prototype, "toBlob").mockImplementation(
    (callback) => {
      expect(track.stop).toHaveBeenCalled();
      callback(new Blob(["PNG"], { type: "image/png" }));
    },
  );
  const file = await captureScreenshot();
  expect(file).toMatchObject({ size: 3, type: "image/png" });
  expect(drawImage).toHaveBeenCalledOnce();
  expect(vi.getTimerCount()).toBe(0);
});

it("releases screenshot sharing if no first frame arrives", async () => {
  vi.spyOn(HTMLMediaElement.prototype, "play").mockResolvedValue();
  vi.spyOn(HTMLMediaElement.prototype, "pause").mockImplementation(() => {});
  const pending = captureScreenshot();
  const rejected = expect(pending).rejects.toThrow("Timed out");
  await vi.advanceTimersByTimeAsync(10_000);
  await rejected;
  expect(track.stop).toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
});

describe("current-tab region screenshot", () => {
  function setupRegion() {
    const target = document.createElement("div");
    target.style.cssText =
      "position:fixed;pointer-events:none;background:transparent;width:100px;height:80px";
    document.body.append(target);
    vi.spyOn(target, "getBoundingClientRect").mockReturnValue({
      left: 10,
      top: 20,
      right: 110,
      bottom: 100,
      width: 100,
      height: 80,
    } as DOMRect);
    const cropTarget = {};
    const fromElement = vi.fn().mockResolvedValue(cropTarget);
    const cropTo = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("CropTarget", { fromElement });
    vi.stubGlobal("BrowserCaptureMediaStreamTrack", { prototype: { cropTo } });
    Object.assign(track, { cropTo });
    vi.spyOn(HTMLMediaElement.prototype, "play").mockResolvedValue();
    vi.spyOn(HTMLMediaElement.prototype, "pause").mockImplementation(() => {});
    vi.spyOn(HTMLMediaElement.prototype, "readyState", "get").mockReturnValue(
      2,
    );
    vi.spyOn(HTMLVideoElement.prototype, "videoWidth", "get").mockReturnValue(
      200,
    );
    vi.spyOn(HTMLVideoElement.prototype, "videoHeight", "get").mockReturnValue(
      160,
    );
    const context = {
      drawImage: vi.fn(),
      strokeRect: vi.fn(),
      fillRect: vi.fn(),
      fillText: vi.fn(),
    };
    vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(
      context as unknown as CanvasRenderingContext2D,
    );
    vi.spyOn(HTMLCanvasElement.prototype, "toBlob").mockImplementation(
      (callback) => {
        expect(track.stop).toHaveBeenCalled();
        callback(new Blob(["PNG"], { type: "image/png" }));
      },
    );
    return { target, cropTarget, fromElement, cropTo, context };
  }

  afterEach(() => document.body.replaceChildren());

  it("feature detects both target creation and track cropping", () => {
    expect(canCaptureRegionScreenshot()).toBe(false);
    setupRegion();
    expect(canCaptureRegionScreenshot()).toBe(true);
    vi.stubGlobal("BrowserCaptureMediaStreamTrack", { prototype: {} });
    expect(canCaptureRegionScreenshot()).toBe(false);
  });

  it("opens the chooser synchronously but reads pixels only after native crop succeeds", async () => {
    const { target, cropTarget, fromElement, cropTo, context } = setupRegion();
    let finishCrop: (() => void) | undefined;
    cropTo.mockReturnValue(
      new Promise<void>((resolve) => {
        finishCrop = resolve;
      }),
    );
    const pending = captureRegionScreenshot(target, 13);
    expect(getDisplayMedia).toHaveBeenCalledWith({
      video: { displaySurface: "browser" },
      audio: false,
      preferCurrentTab: true,
      surfaceSwitching: "exclude",
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(fromElement).toHaveBeenCalledWith(target);
    expect(cropTo).toHaveBeenCalledWith(cropTarget);
    expect(context.drawImage).not.toHaveBeenCalled();
    finishCrop?.();
    const file = await pending;
    expect(file.name).toMatch(/^sumi-feedback-region-13-\d+\.png$/);
    expect(file.type).toBe("image/png");
    expect(context.drawImage).toHaveBeenCalledOnce();
    expect(context.strokeRect).toHaveBeenCalledWith(1.5, 1.5, 197, 157);
    expect(context.fillText).toHaveBeenCalledWith(
      "13",
      expect.any(Number),
      expect.any(Number),
      expect.any(Number),
    );
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects another tab without using its uncropped pixels", async () => {
    const { target, cropTo, context } = setupRegion();
    cropTo.mockRejectedValue(new DOMException("Wrong tab", "NotAllowedError"));
    await expect(captureRegionScreenshot(target, 1)).rejects.toThrow(
      "このSumiのタブ",
    );
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects source changes while the native chooser was open", async () => {
    const { target, cropTo, context } = setupRegion();
    let current = true;
    let choose: ((value: MediaStream) => void) | undefined;
    getDisplayMedia.mockReturnValue(
      new Promise<MediaStream>((resolve) => {
        choose = resolve;
      }),
    );
    const pending = captureRegionScreenshot(
      target,
      1,
      undefined,
      () => current,
    );
    current = false;
    choose?.(stream);
    await expect(pending).rejects.toThrow(
      "撮影元のページや表示位置が変わりました",
    );
    expect(cropTo).not.toHaveBeenCalled();
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("revalidates source context after frame readiness immediately before reading pixels", async () => {
    const { target, cropTo, context } = setupRegion();
    let current = true;
    const validate = vi.fn(() => current);
    vi.mocked(HTMLMediaElement.prototype.play).mockImplementation(() => {
      current = false;
      return Promise.resolve();
    });
    await expect(
      captureRegionScreenshot(target, 1, undefined, validate),
    ).rejects.toThrow("撮影元のページや表示位置が変わりました");
    expect(cropTo).toHaveBeenCalledOnce();
    expect(validate.mock.results.map((result) => result.value)).toEqual([
      true,
      true,
      false,
    ]);
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects a window track without region capture support", async () => {
    const { target, context } = setupRegion();
    Object.assign(track, { cropTo: undefined });
    await expect(captureRegionScreenshot(target, 1)).rejects.toThrow(
      "ウィンドウや画面全体",
    );
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
  });

  it("releases a chooser result that arrives after abort", async () => {
    const { target, cropTo } = setupRegion();
    let choose: ((value: MediaStream) => void) | undefined;
    getDisplayMedia.mockReturnValue(
      new Promise<MediaStream>((resolve) => {
        choose = resolve;
      }),
    );
    const abort = new AbortController();
    const pending = captureRegionScreenshot(target, 1, abort.signal);
    abort.abort();
    choose?.(stream);
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(cropTo).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
  });

  it("aborts a pending native crop immediately and releases tracks", async () => {
    const { target, cropTo, context } = setupRegion();
    cropTo.mockReturnValue(new Promise<void>(() => {}));
    const abort = new AbortController();
    const pending = captureRegionScreenshot(target, 1, abort.signal);
    const rejected = expect(pending).rejects.toMatchObject({
      name: "AbortError",
    });
    await vi.advanceTimersByTimeAsync(0);
    abort.abort();
    await rejected;
    expect(context.drawImage).not.toHaveBeenCalled();
    expect(track.stop).toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("times out pending crop setup without leaving sharing active", async () => {
    const { target, fromElement } = setupRegion();
    fromElement.mockReturnValue(new Promise<object>(() => {}));
    const pending = captureRegionScreenshot(target, 1);
    const rejected = expect(pending).rejects.toThrow(
      "範囲の撮影が完了しませんでした",
    );
    await vi.advanceTimersByTimeAsync(10_000);
    await rejected;
    expect(track.stop).toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rejects detached targets before requesting permission", async () => {
    const { target } = setupRegion();
    target.remove();
    await expect(captureRegionScreenshot(target, 1)).rejects.toThrow(
      "範囲を選び直して",
    );
    expect(getDisplayMedia).not.toHaveBeenCalled();
  });
});
