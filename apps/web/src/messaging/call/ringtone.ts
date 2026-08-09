/**
 * 着信音。通知音（notifications.ts）と同じ理由で合成する——音声アセットを
 * 抱えるほどの表現ではない。通知の「一度鳴る二音」と違い、着信は応答か拒否が
 * あるまで繰り返す必要があるので、鳴り続ける口と止める口を持つ。
 */

type AudioContextConstructor = new () => AudioContext;

let sharedContext: AudioContext | null = null;
let ringTimer: ReturnType<typeof setInterval> | null = null;

function audioContext(): AudioContext | null {
  const Ctor = (globalThis as { AudioContext?: AudioContextConstructor })
    .AudioContext;
  if (!Ctor) return null;
  if (!sharedContext) {
    try {
      sharedContext = new Ctor();
    } catch {
      return null;
    }
  }
  return sharedContext;
}

function ring(): void {
  const context = audioContext();
  if (!context) return;
  try {
    void context.resume?.();
    const now = context.currentTime;
    const master = context.createGain();
    master.gain.value = 0.05;
    master.connect(context.destination);
    // 同じ高さの二拍。通知の上行二音と混ざらない、電話の呼び出しの語彙。
    for (const index of [0, 1]) {
      const startAt = now + index * 0.32;
      const oscillator = context.createOscillator();
      const envelope = context.createGain();
      oscillator.type = "sine";
      oscillator.frequency.setValueAtTime(587.33, startAt);
      envelope.gain.setValueAtTime(0.0001, startAt);
      envelope.gain.exponentialRampToValueAtTime(1, startAt + 0.02);
      envelope.gain.exponentialRampToValueAtTime(0.0001, startAt + 0.24);
      oscillator.connect(envelope);
      envelope.connect(master);
      oscillator.start(startAt);
      oscillator.stop(startAt + 0.26);
    }
  } catch {
    // 音が出せないのは着信の失敗ではない。画面には出ている。
  }
}

/** 着信中に鳴らし続ける。二重に呼んでも一つしか鳴らない。 */
export function startRingtone(): void {
  if (ringTimer !== null) return;
  ring();
  ringTimer = globalThis.setInterval?.(ring, 2_600) ?? null;
}

/** 応答・拒否・相手の切断で止める。 */
export function stopRingtone(): void {
  if (ringTimer === null) return;
  globalThis.clearInterval?.(ringTimer);
  ringTimer = null;
}
