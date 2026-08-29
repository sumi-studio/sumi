// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  dismissPermissionPrompt,
  isNotificationSoundEnabled,
  isPermissionPromptDismissed,
  notificationCountForPlace,
  playNotificationSound,
  presentationFor,
  resetNotificationAudio,
  setNotificationSoundEnabled,
} from "./notifications";

const called = { notify: null, authorIsSelf: false } as const;

function input(overrides: Partial<Parameters<typeof presentationFor>[0]> = {}) {
  return {
    notify: { reason: "mention" as const },
    authorIsSelf: false,
    tabActive: false,
    placeIsActive: false,
    soundEnabled: true,
    ...overrides,
  };
}

afterEach(() => {
  localStorage.clear();
  resetNotificationAudio();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("presentationFor", () => {
  it("stays silent when the server did not call this person", () => {
    expect(presentationFor(input({ notify: called.notify }))).toEqual({
      sound: false,
    });
  });

  it("never calls someone for their own message", () => {
    expect(presentationFor(input({ authorIsSelf: true }))).toEqual({
      sound: false,
    });
  });

  it("does not stack a notification on the screen being looked at", () => {
    expect(
      presentationFor(input({ tabActive: true, placeIsActive: true })),
    ).toEqual({ sound: false });
  });

  it("still makes a sound for another place while the tab is in front", () => {
    expect(
      presentationFor(input({ tabActive: true, placeIsActive: false })),
    ).toEqual({ sound: true });
  });

  it("keeps the in-page sound when the tab is in the background", () => {
    expect(presentationFor(input())).toEqual({ sound: true });
  });

  it("respects a device that asked for silence", () => {
    expect(presentationFor(input({ soundEnabled: false }))).toEqual({
      sound: false,
    });
  });
});

describe("notification badge aggregation", () => {
  it("suppresses muted places and follows each channel level", () => {
    expect(notificationCountForPlace("channel:c1", "mute", 8, 3)).toBe(0);
    expect(notificationCountForPlace("channel:c1", "all", 8, 3)).toBe(8);
    expect(notificationCountForPlace("channel:c1", "mentions", 8, 3)).toBe(3);
  });

  it("counts every direct message unless the conversation is muted", () => {
    expect(notificationCountForPlace("dm:d1", "mentions", 4, 0)).toBe(4);
    expect(notificationCountForPlace("group_dm:g1", "mute", 4, 0)).toBe(0);
  });
});

describe("device preferences", () => {
  it("defaults the sound on and remembers being switched off", () => {
    expect(isNotificationSoundEnabled()).toBe(true);
    setNotificationSoundEnabled(false);
    expect(isNotificationSoundEnabled()).toBe(false);
    setNotificationSoundEnabled(true);
    expect(isNotificationSoundEnabled()).toBe(true);
  });

  it("never re-asks once the permission banner was closed", () => {
    expect(isPermissionPromptDismissed()).toBe(false);
    dismissPermissionPrompt();
    expect(isPermissionPromptDismissed()).toBe(true);
  });
});

describe("playNotificationSound", () => {
  it("synthesises two short tones rather than loading an audio asset", () => {
    const oscillators: { start: unknown; stop: unknown }[] = [];
    const gainNode = () => ({
      gain: {
        value: 0,
        setValueAtTime: vi.fn(),
        exponentialRampToValueAtTime: vi.fn(),
      },
      connect: vi.fn(),
    });
    class FakeAudioContext {
      currentTime = 0;
      destination = {};
      resume = vi.fn();
      createGain = vi.fn(gainNode);
      createOscillator = vi.fn(() => {
        const oscillator = {
          type: "",
          frequency: { setValueAtTime: vi.fn() },
          connect: vi.fn(),
          start: vi.fn(),
          stop: vi.fn(),
        };
        oscillators.push(oscillator);
        return oscillator;
      });
    }
    vi.stubGlobal("AudioContext", FakeAudioContext);

    playNotificationSound();

    expect(oscillators).toHaveLength(2);
    for (const oscillator of oscillators) {
      expect(oscillator.start).toHaveBeenCalledOnce();
      expect(oscillator.stop).toHaveBeenCalledOnce();
    }
  });

  it("is a no-op where Web Audio is unavailable", () => {
    vi.stubGlobal("AudioContext", undefined);
    expect(() => playNotificationSound()).not.toThrow();
  });
});
