/**
 * 通話メディアの薄い抽象。storeはこのinterfaceだけを知り、LiveKitのSDKも
 * WebRTCも知らない——ADR 0012が言う「通話状態の判断とメディアの運搬を
 * 分ける」をコードの形でも保つため。テストはfake transportを差し込む。
 */

import type { Room } from "livekit-client";
import type { ParticipantKey } from "../model";
import type { CallMediaTrack, CallTicket } from "./model";

export interface CallTransportEvents {
  /** 相手から届いている映像（カメラ・画面共有）の現在の全量。 */
  onTracks(tracks: CallMediaTrack[]): void;
  /** いま発話している参加者。タイルのリング表示に使う。 */
  onSpeaking(participants: ParticipantKey[]): void;
  /** 部屋の実際の在室者。サーバーのcall_stateが遅れても画面が追いつく。 */
  onParticipants(participants: ParticipantKey[]): void;
  /** 相手都合・回線都合で切れた。自分から切った場合は呼ばれない。 */
  onDisconnected(): void;
}

export interface CallTransport {
  connect(ticket: CallTicket): Promise<void>;
  setMicrophoneEnabled(enabled: boolean): Promise<void>;
  setCameraEnabled(enabled: boolean): Promise<void>;
  setScreenShareEnabled(enabled: boolean): Promise<void>;
  disconnect(): Promise<void>;
}

export type CallTransportFactory = (
  events: CallTransportEvents,
) => CallTransport;

/**
 * LiveKit実装。SDKは動的importで、通話を始めるまで読み込まない
 * （メッセージングを開くだけの人にRTCのコードを配らない）。
 */
class LiveKitCallTransport implements CallTransport {
  private readonly events: CallTransportEvents;
  private room: Room | null = null;
  private readonly tracks = new Map<string, CallMediaTrack>();
  private readonly audioTracks = new Map<string, () => void>();
  private audioRecoveryRoom: Room | null = null;
  private audioRecoveryPending = false;

  constructor(events: CallTransportEvents) {
    this.events = events;
  }

  async connect(ticket: CallTicket): Promise<void> {
    const { Room, RoomEvent, Track } = await import("livekit-client");
    const room = new Room({ adaptiveStream: true, dynacast: true });
    this.room = room;

    room.on(RoomEvent.TrackSubscribed, (track, publication, participant) => {
      if (track.kind === "audio") {
        const element = document.createElement("audio");
        element.autoplay = true;
        element.hidden = true;
        track.attach(element);
        document.body.append(element);
        const key = publication.trackSid;
        this.removeAudioTrack(key);
        this.audioTracks.set(key, () => {
          track.detach(element);
          element.remove();
        });
        return;
      }
      if (track.kind !== "video") return;
      const screen = track.source === Track.Source.ScreenShare;
      this.tracks.set(trackKey(participant.identity, screen), {
        participantKey: participant.identity,
        kind: screen ? "screen" : "camera",
        attach: (element) => {
          track.attach(element);
        },
        detach: () => {
          track.detach();
        },
      });
      this.emitTracks();
    });
    room.on(RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
      if (track.kind === "audio") {
        this.removeAudioTrack(publication.trackSid);
        return;
      }
      if (track.kind !== "video") return;
      const screen = track.source === Track.Source.ScreenShare;
      this.tracks.delete(trackKey(participant.identity, screen));
      this.emitTracks();
    });
    room.on(RoomEvent.ActiveSpeakersChanged, (speakers) => {
      this.events.onSpeaking(speakers.map((speaker) => speaker.identity));
    });
    const emitParticipants = () => {
      this.events.onParticipants([
        room.localParticipant.identity,
        ...[...room.remoteParticipants.values()].map(
          (participant) => participant.identity,
        ),
      ]);
    };
    room.on(RoomEvent.ParticipantConnected, emitParticipants);
    room.on(RoomEvent.ParticipantDisconnected, emitParticipants);
    room.on(RoomEvent.AudioPlaybackStatusChanged, (canPlay) => {
      if (canPlay) {
        this.stopAudioRecovery();
      } else {
        this.startAudioRecovery(room);
      }
    });
    room.on(RoomEvent.Disconnected, () => {
      if (this.room === room) this.room = null;
      this.clearAudioTracks();
      this.stopAudioRecovery();
      this.tracks.clear();
      this.emitTracks();
      this.events.onDisconnected();
    });

    await room.connect(ticket.url, ticket.token);
    // 入った瞬間はマイクだけ。カメラは本人が押すまで開かない——通話に入る
    // ことと顔を出すことは別の意思である。
    await room.localParticipant.setMicrophoneEnabled(true);
    emitParticipants();
  }

  async setMicrophoneEnabled(enabled: boolean): Promise<void> {
    await this.room?.localParticipant.setMicrophoneEnabled(enabled);
  }

  async setCameraEnabled(enabled: boolean): Promise<void> {
    await this.room?.localParticipant.setCameraEnabled(enabled);
  }

  async setScreenShareEnabled(enabled: boolean): Promise<void> {
    await this.room?.localParticipant.setScreenShareEnabled(enabled);
  }

  async disconnect(): Promise<void> {
    const room = this.room;
    this.room = null;
    this.clearAudioTracks();
    this.stopAudioRecovery();
    this.tracks.clear();
    this.emitTracks();
    await room?.disconnect();
  }

  private emitTracks(): void {
    this.events.onTracks([...this.tracks.values()]);
  }

  private removeAudioTrack(key: string): void {
    const cleanup = this.audioTracks.get(key);
    if (!cleanup) return;
    this.audioTracks.delete(key);
    cleanup();
  }

  private clearAudioTracks(): void {
    for (const cleanup of this.audioTracks.values()) cleanup();
    this.audioTracks.clear();
  }

  /** Browserのautoplay拒否後、次の本人操作をLiveKitの再開gestureに使う。 */
  private startAudioRecovery(room: Room): void {
    if (this.audioRecoveryRoom === room) return;
    this.stopAudioRecovery();
    this.audioRecoveryRoom = room;
    document.addEventListener("click", this.recoverAudio, true);
    document.addEventListener("keydown", this.recoverAudio, true);
  }

  private stopAudioRecovery(): void {
    this.audioRecoveryRoom = null;
    this.audioRecoveryPending = false;
    document.removeEventListener("click", this.recoverAudio, true);
    document.removeEventListener("keydown", this.recoverAudio, true);
  }

  private readonly recoverAudio = (): void => {
    const room = this.audioRecoveryRoom;
    if (!room || this.audioRecoveryPending) return;
    this.audioRecoveryPending = true;
    void room.startAudio().finally(() => {
      if (this.audioRecoveryRoom === room) this.audioRecoveryPending = false;
    });
  };
}

function trackKey(participantKey: ParticipantKey, screen: boolean): string {
  return `${participantKey}|${screen ? "screen" : "camera"}`;
}

export const createLiveKitTransport: CallTransportFactory = (events) =>
  new LiveKitCallTransport(events);
