import type { Room } from "livekit-client";
import type { ParticipantKey } from "../model";
import type { CallMediaTrack, CallTicket } from "./model";

export interface CallTransportEvents {
  onTracks(tracks: CallMediaTrack[]): void;
  onSpeaking(participants: ParticipantKey[]): void;
  onParticipants(participants: ParticipantKey[]): void;
  onAudioPlaybackBlocked(blocked: boolean): void;
  onDisconnected(): void;
}

export interface CallTransport {
  connect(ticket: CallTicket): Promise<void>;
  setMicrophoneEnabled(enabled: boolean): Promise<void>;
  setCameraEnabled(enabled: boolean): Promise<void>;
  setScreenShareEnabled(enabled: boolean): Promise<void>;
  resumeAudio(): Promise<void>;
  disconnect(): Promise<void>;
}

export type CallTransportFactory = (
  events: CallTransportEvents,
) => CallTransport;

class LiveKitCallTransport implements CallTransport {
  private readonly events: CallTransportEvents;
  private room: Room | null = null;
  private readonly tracks = new Map<string, CallMediaTrack>();
  private readonly audioTracks = new Map<string, () => void>();

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
      this.events.onAudioPlaybackBlocked(!canPlay);
    });
    room.on(RoomEvent.Disconnected, () => {
      if (this.room === room) this.room = null;
      this.clearAudioTracks();
      this.tracks.clear();
      this.emitTracks();
      this.events.onAudioPlaybackBlocked(false);
      this.events.onDisconnected();
    });

    await room.connect(ticket.url, ticket.token);
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

  async resumeAudio(): Promise<void> {
    const room = this.room;
    if (!room) return;
    await room.startAudio();
    if (this.room === room) this.events.onAudioPlaybackBlocked(false);
  }

  async disconnect(): Promise<void> {
    const room = this.room;
    this.room = null;
    this.clearAudioTracks();
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
}

function trackKey(participantKey: ParticipantKey, screen: boolean): string {
  return `${participantKey}|${screen ? "screen" : "camera"}`;
}

export const createLiveKitTransport: CallTransportFactory = (events) =>
  new LiveKitCallTransport(events);
