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

  constructor(events: CallTransportEvents) {
    this.events = events;
  }

  async connect(ticket: CallTicket): Promise<void> {
    const { Room, RoomEvent, Track } = await import("livekit-client");
    const room = new Room({ adaptiveStream: true, dynacast: true });
    this.room = room;

    room.on(RoomEvent.TrackSubscribed, (track, _publication, participant) => {
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
    room.on(RoomEvent.TrackUnsubscribed, (track, _publication, participant) => {
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
    room.on(RoomEvent.Disconnected, () => {
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
    this.tracks.clear();
    this.emitTracks();
    await room?.disconnect();
  }

  private emitTracks(): void {
    this.events.onTracks([...this.tracks.values()]);
  }
}

function trackKey(participantKey: ParticipantKey, screen: boolean): string {
  return `${participantKey}|${screen ? "screen" : "camera"}`;
}

export const createLiveKitTransport: CallTransportFactory = (events) =>
  new LiveKitCallTransport(events);
