// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  dismissPermissionPrompt,
  isNotificationSoundEnabled,
  isPermissionPromptDismissed,
  MAX_SNIPPET_CHARS,
  notificationBody,
  notificationCountForPlace,
  notificationTitle,
  playNotificationSound,
  presentationFor,
  presentDesktopNotification,
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
    permission: "granted" as const,
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
      desktop: false,
      sound: false,
    });
  });

  it("never calls someone for their own message", () => {
    expect(presentationFor(input({ authorIsSelf: true }))).toEqual({
      desktop: false,
      sound: false,
    });
  });

  it("does not stack a notification on the screen being looked at", () => {
    expect(
      presentationFor(input({ tabActive: true, placeIsActive: true })),
    ).toEqual({ desktop: false, sound: false });
  });

  it("still makes a sound for another place while the tab is in front", () => {
    expect(
      presentationFor(input({ tabActive: true, placeIsActive: false })),
    ).toEqual({ desktop: false, sound: true });
  });

  it("shows a desktop notification only when the tab is not in front", () => {
    expect(presentationFor(input())).toEqual({ desktop: true, sound: true });
  });

  it("needs granted permission for the desktop half, and keeps the sound", () => {
    expect(presentationFor(input({ permission: "default" }))).toEqual({
      desktop: false,
      sound: true,
    });
    expect(presentationFor(input({ permission: "unsupported" }))).toEqual({
      desktop: false,
      sound: true,
    });
  });

  it("respects a device that asked for silence", () => {
    expect(presentationFor(input({ soundEnabled: false }))).toEqual({
      desktop: true,
      sound: false,
    });
  });
});

describe("notification text", () => {
  it("names the place and the speaker, and falls back to the speaker alone", () => {
    expect(notificationTitle("#dev", "Kuro")).toBe("#dev — Kuro");
    expect(notificationTitle("", "Kuro")).toBe("Kuro");
  });

  it("collapses whitespace and truncates a long body", () => {
    expect(notificationBody("  改行を\n  含む  文  ")).toBe("改行を 含む 文");
    const long = "あ".repeat(MAX_SNIPPET_CHARS + 40);
    const body = notificationBody(long);
    expect(body).toHaveLength(MAX_SNIPPET_CHARS);
    expect(body.endsWith("…")).toBe(true);
  });

  it("names attachment-only messages instead of showing an empty notification", () => {
    expect(
      notificationBody("", [
        {
          attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
          filename: "plan.pdf",
          mime: "application/pdf",
          sizeBytes: 1,
          sha256: "00",
          position: 0,
          spoiler: false,
          alt: "",
        },
      ]),
    ).toBe("📎 plan.pdf");
    expect(
      notificationBody("", [
        {
          attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaac",
          filename: "ending_dies.png",
          mime: "image/png",
          sizeBytes: 1,
          sha256: "00",
          position: 0,
          spoiler: true,
          alt: "",
        },
      ]),
    ).toBe("📎 添付（ネタバレ）");
    expect(
      notificationBody("", [
        {
          attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
          filename: "one.txt",
          mime: "text/plain",
          sizeBytes: 1,
          sha256: "00",
          position: 0,
          spoiler: false,
          alt: "",
        },
        {
          attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaab",
          filename: "two.txt",
          mime: "text/plain",
          sizeBytes: 1,
          sha256: "00",
          position: 1,
          spoiler: false,
          alt: "",
        },
      ]),
    ).toBe("📎 2件のファイル");
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

describe("presentDesktopNotification", () => {
  it("tags by place, and activating it focuses the conversation", () => {
    const constructed: { title: string; options: NotificationOptions }[] = [];
    class FakeNotification {
      static permission = "granted";
      onclick: (() => void) | null = null;
      close = vi.fn();
      constructor(title: string, options: NotificationOptions) {
        constructed.push({ title, options });
        instances.push(this);
      }
    }
    const instances: FakeNotification[] = [];
    vi.stubGlobal("Notification", FakeNotification);
    // jsdom has no real window focus; the click still has to reach the place.
    vi.stubGlobal("focus", vi.fn());
    const onActivate = vi.fn();

    presentDesktopNotification({
      title: "#dev — Kuro",
      body: "レビューお願いします",
      placeKey: "channel:c1",
      onActivate,
    });

    expect(constructed).toHaveLength(1);
    expect(constructed[0]?.title).toBe("#dev — Kuro");
    expect(constructed[0]?.options.tag).toBe("sumi:channel:c1");
    instances[0]?.onclick?.();
    expect(onActivate).toHaveBeenCalledOnce();
    expect(instances[0]?.close).toHaveBeenCalledOnce();
  });

  it("does nothing without granted permission", () => {
    class DeniedNotification {
      static permission = "denied";
      constructor() {
        throw new Error("must not construct a notification without permission");
      }
    }
    vi.stubGlobal("Notification", DeniedNotification);
    expect(() =>
      presentDesktopNotification({
        title: "t",
        body: "b",
        placeKey: "channel:c1",
        onActivate: () => undefined,
      }),
    ).not.toThrow();
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
