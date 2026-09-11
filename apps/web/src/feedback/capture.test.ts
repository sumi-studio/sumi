// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
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
