// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";

const liveKit = vi.hoisted(() => {
  const RoomEvent = {
    TrackSubscribed: "track-subscribed",
    TrackUnsubscribed: "track-unsubscribed",
    ActiveSpeakersChanged: "active-speakers-changed",
    ParticipantConnected: "participant-connected",
    ParticipantDisconnected: "participant-disconnected",
    Disconnected: "disconnected",
    AudioPlaybackStatusChanged: "audio-playback-status-changed",
  } as const;
  const Track = {
    Source: { ScreenShare: "screen-share" },
  } as const;

  class Room {
    static instances: Room[] = [];
    readonly handlers = new Map<string, Array<(...args: unknown[]) => void>>();
    readonly localParticipant = {
      identity: "human:alice",
      setMicrophoneEnabled: vi.fn(async () => undefined),
      setCameraEnabled: vi.fn(async () => undefined),
      setScreenShareEnabled: vi.fn(async () => undefined),
    };
    readonly remoteParticipants = new Map();
    readonly startAudio = vi.fn(async () => undefined);
    readonly connect = vi.fn(async () => undefined);
    readonly disconnect = vi.fn(async () => undefined);

    constructor(_options: unknown) {
      Room.instances.push(this);
    }

    on(event: string, handler: (...args: unknown[]) => void): this {
      const handlers = this.handlers.get(event) ?? [];
      handlers.push(handler);
      this.handlers.set(event, handlers);
      return this;
    }

    emit(event: string, ...args: unknown[]): void {
      for (const handler of this.handlers.get(event) ?? []) handler(...args);
    }
  }

  return { Room, RoomEvent, Track };
});

vi.mock("livekit-client", () => liveKit);

import { createLiveKitTransport } from "./call-transport";

describe("LiveKit call transport", () => {
  beforeEach(() => {
    liveKit.Room.instances = [];
    document.body.replaceChildren();
  });

  it("remote audioをmanaged elementへattachし、video mapと分離して片付ける", async () => {
    const onTracks = vi.fn();
    const transport = createLiveKitTransport({
      onTracks,
      onSpeaking: vi.fn(),
      onParticipants: vi.fn(),
      onAudioPlaybackBlocked: vi.fn(),
      onDisconnected: vi.fn(),
    });
    await transport.connect({
      url: "ws://livekit.test",
      token: "ticket",
      room: "c1",
      identity: "human:alice",
    });
    const room = liveKit.Room.instances[0];
    const participant = { identity: "human:bob" };
    const video = fakeTrack("video", "camera");
    const audio = fakeTrack("audio", "microphone");

    room.emit(
      liveKit.RoomEvent.TrackSubscribed,
      video,
      { trackSid: "video-1" },
      participant,
    );
    expect(onTracks.mock.lastCall?.[0]).toHaveLength(1);

    room.emit(
      liveKit.RoomEvent.TrackSubscribed,
      audio,
      { trackSid: "audio-1" },
      participant,
    );
    expect(audio.attach).toHaveBeenCalledOnce();
    const element = audio.attach.mock.calls[0][0] as HTMLAudioElement;
    expect(element).toBeInstanceOf(HTMLAudioElement);
    expect(element.autoplay).toBe(true);
    expect(element.hidden).toBe(true);
    expect(element.isConnected).toBe(true);
    expect(onTracks.mock.lastCall?.[0]).toHaveLength(1);

    room.emit(
      liveKit.RoomEvent.TrackUnsubscribed,
      audio,
      { trackSid: "audio-1" },
      participant,
    );
    expect(audio.detach).toHaveBeenCalledWith(element);
    expect(element.isConnected).toBe(false);
    expect(onTracks.mock.lastCall?.[0]).toHaveLength(1);

    room.emit(
      liveKit.RoomEvent.TrackUnsubscribed,
      video,
      { trackSid: "video-1" },
      participant,
    );
    expect(onTracks.mock.lastCall?.[0]).toEqual([]);
  });

  it("autoplay拒否を上へ伝え、明示的なresumeでRoom.startAudioを待つ", async () => {
    const onAudioPlaybackBlocked = vi.fn();
    const transport = createLiveKitTransport({
      onTracks: vi.fn(),
      onSpeaking: vi.fn(),
      onParticipants: vi.fn(),
      onAudioPlaybackBlocked,
      onDisconnected: vi.fn(),
    });
    await transport.connect({
      url: "ws://livekit.test",
      token: "ticket",
      room: "c1",
      identity: "human:alice",
    });
    const room = liveKit.Room.instances[0];

    room.emit(liveKit.RoomEvent.AudioPlaybackStatusChanged, false);
    expect(onAudioPlaybackBlocked).toHaveBeenLastCalledWith(true);
    document.body.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(room.startAudio).not.toHaveBeenCalled();

    await transport.resumeAudio();
    expect(room.startAudio).toHaveBeenCalledOnce();
    expect(onAudioPlaybackBlocked).toHaveBeenLastCalledWith(false);
  });

  it("resume失敗を呼び出し元へ返し、blocked状態を解除しない", async () => {
    const onAudioPlaybackBlocked = vi.fn();
    const transport = createLiveKitTransport({
      onTracks: vi.fn(),
      onSpeaking: vi.fn(),
      onParticipants: vi.fn(),
      onAudioPlaybackBlocked,
      onDisconnected: vi.fn(),
    });
    await transport.connect({
      url: "ws://livekit.test",
      token: "ticket",
      room: "c1",
      identity: "human:alice",
    });
    const room = liveKit.Room.instances[0];
    room.emit(liveKit.RoomEvent.AudioPlaybackStatusChanged, false);
    room.startAudio.mockRejectedValueOnce(new Error("still blocked"));

    await expect(transport.resumeAudio()).rejects.toThrow("still blocked");

    expect(room.startAudio).toHaveBeenCalledOnce();
    expect(onAudioPlaybackBlocked).toHaveBeenLastCalledWith(true);
  });
});

function fakeTrack(kind: "audio" | "video", source: string) {
  return {
    kind,
    source,
    attach: vi.fn((element: HTMLMediaElement) => element),
    detach: vi.fn((_element?: HTMLMediaElement) => undefined),
  };
}
