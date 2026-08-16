import type { ParticipantKey, ParticipantRef, Place } from "../model";

export interface CallParticipant {
  participant: ParticipantRef;
  joinedAt: number;
  screenShare: boolean;
}

/** Volatile place presence has no durable Messaging sequence. */
export interface CallState {
  place: Place;
  active: boolean;
  startedAt: number | null;
  participants: CallParticipant[];
}

export interface CallTicket {
  url: string;
  token: string;
  room: string;
  identity: string;
}

export type CallPhase = "idle" | "connecting" | "connected" | "failed";

export type CallFailure =
  | "insecure_context"
  | "microphone_denied"
  | "mixed_content"
  | "unavailable"
  | "not_allowed"
  | "connection_failed";

export const CALL_FAILURE_MESSAGE: Record<CallFailure, string> = {
  insecure_context:
    "この画面では通話できません。マイクを使うには HTTPS のアドレスで開いてください。",
  microphone_denied:
    "マイクを使用できません。ブラウザのマイク権限を許可して、もう一度お試しください。",
  mixed_content:
    "この画面では通話できません。安全な通話接続（wss://）が設定された HTTPS の画面を開いてください。",
  unavailable: "この環境では通話が設定されていません。",
  not_allowed: "この通話に参加できません。場所へのアクセスを確認してください。",
  connection_failed:
    "通話サーバーに接続できません。しばらくしてからもう一度お試しください。",
};

export interface CallMediaTrack {
  participantKey: ParticipantKey;
  kind: "camera" | "screen";
  attach: ((element: HTMLVideoElement) => void) | null;
  detach: (() => void) | null;
}

export interface CallLocalState {
  micEnabled: boolean;
  cameraEnabled: boolean;
  screenShareEnabled: boolean;
}
