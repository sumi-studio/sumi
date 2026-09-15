import { readFile } from "node:fs/promises";

/**
 * The replaceable media adapter seam. CallSTT turns one detected speech
 * segment into text (or null for a non-speech segment); CallTTS renders one
 * utterance to PCM. The fixture implementations below make the slice run
 * without credentials — real engines (Deepgram/whisper/piper/etc.) plug in
 * behind these two interfaces later. This is a deployment decision, not a
 * product question.
 */

export interface AudioSegment {
  /** Raw LiveKit identity of the speaking participant. */
  speakerIdentity: string;
  /** Participant ref parsed from the identity ("human:<id>" etc). */
  speakerRef: string;
  samples: Int16Array;
  sampleRate: number;
  startedAt: Date;
  endedAt: Date;
}

export interface Transcript {
  text: string;
  /** Adapter provenance — recorded in the input payload. */
  engine: string;
}

export interface CallSTT {
  transcribe(segment: AudioSegment): Promise<Transcript | null>;
}

export interface RenderedSpeech {
  samples: Int16Array;
  sampleRate: number;
}

export interface CallTTS {
  render(text: string): Promise<RenderedSpeech>;
}

/**
 * FixtureSTT: maps each detected segment to the next scripted line for that
 * speaker. The script is data loaded from a file —
 * {"default": ["..."], "<identity>": ["..."]} — so no fixture phrase lives
 * in call logic, and an unscripted segment produces no transcript.
 */
export class FixtureSTT implements CallSTT {
  private scripts: Map<string, string[]>;
  private cursors = new Map<string, number>();

  static async fromFile(path: string): Promise<FixtureSTT> {
    const raw = JSON.parse(await readFile(path, "utf8")) as Record<
      string,
      string[]
    >;
    return new FixtureSTT(new Map(Object.entries(raw)));
  }

  constructor(scripts: Map<string, string[]>) {
    this.scripts = scripts;
  }

  async transcribe(segment: AudioSegment): Promise<Transcript | null> {
    const key = this.scripts.has(segment.speakerIdentity)
      ? segment.speakerIdentity
      : this.scripts.has(segment.speakerRef)
        ? segment.speakerRef
        : "default";
    const lines = this.scripts.get(key);
    if (!lines || lines.length === 0) return null;
    const at = this.cursors.get(key) ?? 0;
    const line = lines[at];
    if (line === undefined) return null;
    this.cursors.set(key, at + 1);
    return { text: line, engine: "fixture" };
  }
}

/**
 * FixtureTTS: renders text to an audible 16kHz PCM pattern — a per-utterance
 * tone sequence whose pitch varies with content, so "the secretary spoke" is
 * distinguishable from silence and from other utterances without embedding
 * any real speech. If SUMI_CALL_TTS_WAV names a WAV file, that prerecorded
 * clip is emitted instead (truncated or looped to fit), giving the fixture
 * real-spectrogram audio when a test supplies one.
 */
export class FixtureTTS implements CallTTS {
  private wavPcm: Int16Array | null = null;

  static async create(wavPath?: string): Promise<FixtureTTS> {
    const tts = new FixtureTTS();
    if (wavPath) {
      tts.wavPcm = decodeWavPcm16(await readFile(wavPath));
    }
    return tts;
  }

  async render(text: string): Promise<RenderedSpeech> {
    if (this.wavPcm) {
      return { samples: this.wavPcm, sampleRate: 16000 };
    }
    const sampleRate = 16000;
    // ~55ms per character, bounded; distinct enough per text to tell
    // different utterances apart by ear/level without claiming speech.
    const ms = Math.min(Math.max(400, text.length * 55), 12000);
    const total = Math.floor((sampleRate * ms) / 1000);
    const samples = new Int16Array(total);
    let seed = 0;
    for (const ch of text) seed = (seed * 31 + ch.charCodeAt(0)) >>> 0;
    const f1 = 300 + (seed % 500);
    const f2 = f1 * 1.5;
    for (let i = 0; i < total; i++) {
      const t = i / sampleRate;
      // 90ms tone bursts separated by short gaps — audible, clearly
      // synthetic, and measurably non-silent on the far side.
      const burst = Math.floor(t * 1000) % 160 < 100;
      const envelope = burst ? Math.sin(Math.PI * ((t * 1000) % 160) / 160) : 0;
      samples[i] = Math.round(
        9000 * envelope * (Math.sin(2 * Math.PI * f1 * t) + 0.5 * Math.sin(2 * Math.PI * f2 * t)) / 1.5,
      );
    }
    return { samples, sampleRate };
  }
}

/** Minimal WAV decoder: PCM16 mono; anything else rejects loudly. */
export function decodeWavPcm16(buf: Buffer): Int16Array {
  if (buf.length < 44 || buf.toString("ascii", 0, 4) !== "RIFF") {
    throw new Error("not a RIFF/WAVE file");
  }
  // Walk chunks to find fmt + data.
  let offset = 12;
  let channels = 0;
  let bits = 0;
  let data: Buffer | null = null;
  while (offset + 8 <= buf.length) {
    const id = buf.toString("ascii", offset, offset + 4);
    const size = buf.readUInt32LE(offset + 4);
    const body = buf.subarray(offset + 8, offset + 8 + size);
    if (id === "fmt ") {
      const format = body.readUInt16LE(0);
      channels = body.readUInt16LE(2);
      bits = body.readUInt16LE(14);
      if (format !== 1 || bits !== 16) {
        throw new Error("fixture WAV must be PCM16");
      }
    }
    if (id === "data") data = body;
    offset += 8 + size + (size % 2);
  }
  if (!data || channels === 0) throw new Error("WAV has no data chunk");
  const frames = new Int16Array(data.length / 2 / channels | 0);
  for (let i = 0; i < frames.length; i++) {
    // Downmix to mono by taking the first channel.
    frames[i] = data.readInt16LE(i * 2 * channels);
  }
  return frames;
}

/**
 * Energy VAD segmenter over 10ms/16kHz mono frames: a segment opens on
 * sustained energy, closes on ~500ms of silence, and is capped at 30s so a
 * talkative speaker still yields per-segment transcripts.
 */
export class VadSegmenter {
  private readonly threshold: number;
  private readonly hangoverFrames: number;
  private readonly maxFrames: number;
  private open = false;
  private silent = 0;
  private buf: Int16Array[] = [];
  private frames = 0;
  private startedAt: Date | null = null;

  constructor(opts?: { threshold?: number; hangoverMs?: number; maxMs?: number }) {
    this.threshold = opts?.threshold ?? 500;
    this.hangoverFrames = Math.round((opts?.hangoverMs ?? 500) / 10);
    this.maxFrames = Math.round((opts?.maxMs ?? 30_000) / 10);
  }

  /** Returns a completed segment's samples when the frame closes one. */
  push(frame: { data: Int16Array }, at: Date): Int16Array | null {
    let peak = 0;
    for (const v of frame.data) {
      const a = Math.abs(v);
      if (a > peak) peak = a;
    }
    if (!this.open) {
      if (peak <= this.threshold) return null;
      this.open = true;
      this.silent = 0;
      this.buf = [frame.data];
      this.frames = 1;
      this.startedAt = at;
      return null;
    }
    this.buf.push(frame.data);
    this.frames++;
    if (peak <= this.threshold) {
      this.silent++;
    } else {
      this.silent = 0;
    }
    if (this.silent >= this.hangoverFrames || this.frames >= this.maxFrames) {
      return this.close();
    }
    return null;
  }

  /** Force-close any open segment (e.g. track ended). */
  close(): Int16Array | null {
    if (!this.open || !this.startedAt) return null;
    const total = this.buf.reduce((n, b) => n + b.length, 0);
    const out = new Int16Array(total);
    let at = 0;
    for (const b of this.buf) {
      out.set(b, at);
      at += b.length;
    }
    this.open = false;
    this.buf = [];
    this.frames = 0;
    this.silent = 0;
    const started = this.startedAt;
    this.startedAt = null;
    void started;
    return out;
  }

  get segmentStartedAt(): Date | null {
    return this.startedAt;
  }
}
