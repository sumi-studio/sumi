/**
 * 通知の提示層。「呼ぶかどうか」はサーバーが送信時に評価済みで、ここに届く
 * `notify` は既にその答えである。この層が決めるのは「どう提示するか」だけ
 * ——今この画面を見ている人に音を重ねるか。OS の通知カードは Service
 * Worker だけが generic 文面で提示し、page と二重発火させない。
 *
 * 通知許可は端末の同意であってサーバーの設定ではない。localStorageに置くのは
 * 音のon/offとバナーを閉じた事実だけで、通知条件そのものは本人のNotificationSetting
 * （正本はサーバー）に一本化する。
 */

import type { NotificationLevel, NotifyReason, PlaceKey } from "./model";

const SOUND_STORAGE_KEY = "sumi.messaging.notification-sound";
const PROMPT_STORAGE_KEY = "sumi.messaging.notification-prompt";

/** テストとSSRのために、window不在でも壊れない読み書きにする。 */
function readStorage(key: string): string | null {
  try {
    return globalThis.localStorage?.getItem(key) ?? null;
  } catch {
    return null;
  }
}

function writeStorage(key: string, value: string): void {
  try {
    globalThis.localStorage?.setItem(key, value);
  } catch {
    // プライベートモード等で書けないだけ。通知そのものは動く。
  }
}

/** 既定はオン。鳴らしたくない人が一度切れば、その端末では二度と鳴らない。 */
export function isNotificationSoundEnabled(): boolean {
  return readStorage(SOUND_STORAGE_KEY) !== "off";
}

export function setNotificationSoundEnabled(enabled: boolean): void {
  writeStorage(SOUND_STORAGE_KEY, enabled ? "on" : "off");
}

/** 許可を促すバナーは一度閉じたら出し直さない（控えめに、が要件）。 */
export function isPermissionPromptDismissed(): boolean {
  return readStorage(PROMPT_STORAGE_KEY) === "dismissed";
}

export function dismissPermissionPrompt(): void {
  writeStorage(PROMPT_STORAGE_KEY, "dismissed");
}

export type NotificationPermissionState =
  | "default"
  | "granted"
  | "denied"
  | "unsupported";

export function notificationPermission(): NotificationPermissionState {
  if (typeof Notification === "undefined") return "unsupported";
  return Notification.permission;
}

export async function requestNotificationPermission(): Promise<NotificationPermissionState> {
  if (typeof Notification === "undefined") return "unsupported";
  try {
    return await Notification.requestPermission();
  } catch {
    return Notification.permission;
  }
}

/** タブが前面にあり、実際に見られている状態か。 */
export function isTabActive(): boolean {
  if (typeof document === "undefined") return false;
  return document.visibilityState === "visible" && document.hasFocus();
}

export interface PresentationInput {
  /** サーバーの判定結果。nullは「呼んでいない」。 */
  notify: { reason: NotifyReason } | null;
  /** 自分の発言では呼ばない（サーバーも除外するが、ここでも fail-closed）。 */
  authorIsSelf: boolean;
  tabActive: boolean;
  /** そのplaceを今まさに開いているか。開いていれば音も要らない。 */
  placeIsActive: boolean;
  soundEnabled: boolean;
}

export interface Presentation {
  sound: boolean;
}

/** タブタイトルに載せる「この place から呼ばれている未読」の件数。 */
export function notificationCountForPlace(
  key: PlaceKey,
  level: NotificationLevel,
  unread: number,
  mentions: number,
): number {
  if (level === "mute") return 0;
  if (key.startsWith("dm:") || key.startsWith("group_dm:")) return unread;
  return level === "all" ? unread : mentions;
}

/**
 * 提示の決定。判定条件（level・mention・keyword）はここに無い——それは
 * 受信側の設定としてサーバーが持っている。ここにあるのは端末の事情だけ。
 */
export function presentationFor(input: PresentationInput): Presentation {
  if (!input.notify || input.authorIsSelf) return { sound: false };
  // 見ている画面に通知を重ねない。見えているものを知らせ直す意味はない。
  const unseen = !input.tabActive || !input.placeIsActive;
  return { sound: unseen && input.soundEnabled };
}

type AudioContextConstructor = new () => AudioContext;

let sharedContext: AudioContext | null = null;

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

/**
 * 通知音は合成する。音声アセットを置くとライセンスの出所を抱えることになり、
 * 「短い二音」程度の表現にそれは見合わない。二音を上向きに重ねた、控えめな
 * ノックのような音。
 */
export function playNotificationSound(): void {
  const context = audioContext();
  if (!context) return;
  try {
    void context.resume?.();
    const now = context.currentTime;
    const master = context.createGain();
    master.gain.value = 0.06;
    master.connect(context.destination);
    // A5 → D6。完全四度の上行は「呼ばれた」と分かる最小の合図。
    for (const [index, frequency] of [880, 1174.66].entries()) {
      const startAt = now + index * 0.09;
      const oscillator = context.createOscillator();
      const envelope = context.createGain();
      oscillator.type = "sine";
      oscillator.frequency.setValueAtTime(frequency, startAt);
      envelope.gain.setValueAtTime(0.0001, startAt);
      envelope.gain.exponentialRampToValueAtTime(1, startAt + 0.012);
      envelope.gain.exponentialRampToValueAtTime(0.0001, startAt + 0.12);
      oscillator.connect(envelope);
      envelope.connect(master);
      oscillator.start(startAt);
      oscillator.stop(startAt + 0.14);
    }
  } catch {
    // 音が出せないのは通知の失敗ではない。
  }
}

/** テスト用: 合成に使ったAudioContextを捨てる。 */
export function resetNotificationAudio(): void {
  sharedContext = null;
}
