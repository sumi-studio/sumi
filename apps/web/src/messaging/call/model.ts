/**
 * 通話（RTC）のdomain型。ADR 0012: メディアはself-hosted LiveKit（SFU）が運び、
 * 「誰が今その場所で話しているか」はサーバーが持ってWSで配る。
 *
 * 人間と人格agentは通話でも同じ「参加者」。bot用の別枠を作らない。
 */

import type { ParticipantKey, ParticipantRef, Place } from "../model";

/** 通話に今いる一人。 */
export interface CallParticipant {
  participant: ParticipantRef;
  joinedAt: number;
  /** 画面共有を出しているか。人ではなく通話の属性。 */
  screenShare: boolean;
}

/** 1つのplaceの通話状態。volatileで、seqを持たない。 */
export interface CallState {
  place: Place;
  active: boolean;
  startedAt: number | null;
  participants: CallParticipant[];
}

/** placeの部屋へ入るための切符。tokenはLiveKitへ渡す以外の用途を持たない。 */
export interface CallTicket {
  url: string;
  token: string;
  room: string;
  identity: string;
}

/** 自分の通話セッションの段階。UIは「今どこにいるか」をここだけで判断する。 */
export type CallPhase = "idle" | "connecting" | "connected" | "failed";

/**
 * 通話中に相手から届く1本のメディア。tileはこれを並べるだけで、
 * transportの実装（LiveKitかfakeか）を知らない。
 */
export interface CallMediaTrack {
  participantKey: ParticipantKey;
  kind: "camera" | "screen";
  /** DOMへ差し込む実体。テストのfake transportではnullでよい。 */
  attach: ((element: HTMLVideoElement) => void) | null;
  detach: (() => void) | null;
}

/** 自分の入出力の状態。相手には音・映像そのものとして伝わる。 */
export interface CallLocalState {
  micEnabled: boolean;
  cameraEnabled: boolean;
  screenShareEnabled: boolean;
}
