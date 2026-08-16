import { beforeEach, describe, expect, it, vi } from "vitest";
import type { PlaceKey } from "../model";
import { CallAPIError, fetchCallStates, fetchCallTicket } from "./call-api";
import { installCallTransportFactory, useCall } from "./call-store";
import type { CallTransport } from "./call-transport";

vi.mock("./call-api", async () => {
  const actual =
    await vi.importActual<typeof import("./call-api")>("./call-api");
  return {
    ...actual,
    fetchCallStates: vi.fn(),
    fetchCallTicket: vi.fn(),
  };
});

const PLACE: PlaceKey = "dm:dm-1";
const ticket = {
  url: "wss://livekit.example.test",
  token: "signed-token",
  room: "dm_dm-1",
  identity: "human:h-1",
};

function transport(): CallTransport {
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    setMicrophoneEnabled: vi.fn().mockResolvedValue(undefined),
    setCameraEnabled: vi.fn().mockResolvedValue(undefined),
    setScreenShareEnabled: vi.fn().mockResolvedValue(undefined),
    resumeAudio: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
  };
}

function mediaPermission(result: "allowed" | "denied" = "allowed") {
  const stop = vi.fn();
  const getUserMedia =
    result === "allowed"
      ? vi.fn().mockResolvedValue({ getTracks: () => [{ stop }] })
      : vi
          .fn()
          .mockRejectedValue(new DOMException("denied", "NotAllowedError"));
  Object.defineProperty(navigator, "mediaDevices", {
    configurable: true,
    value: { getUserMedia },
  });
  return { getUserMedia, stop };
}

beforeEach(() => {
  useCall.getState().reset();
  vi.mocked(fetchCallStates).mockReset().mockResolvedValue([]);
  vi.mocked(fetchCallTicket).mockReset().mockResolvedValue(ticket);
  installCallTransportFactory(() => transport());
  Object.defineProperty(globalThis, "isSecureContext", {
    configurable: true,
    value: true,
  });
  mediaPermission();
});

describe("call degradation", () => {
  it("explains a plain-http origin without requesting a token or leaving a spinner", async () => {
    Object.defineProperty(globalThis, "isSecureContext", {
      configurable: true,
      value: false,
    });
    await useCall.getState().join(PLACE);
    expect(useCall.getState()).toMatchObject({
      phase: "failed",
      failure: "insecure_context",
      activePlaceKey: null,
    });
    expect(fetchCallTicket).not.toHaveBeenCalled();
  });

  it("handles browsers where mediaDevices is absent", async () => {
    Object.defineProperty(navigator, "mediaDevices", {
      configurable: true,
      value: undefined,
    });
    await useCall.getState().join(PLACE);
    expect(useCall.getState().failure).toBe("insecure_context");
    expect(useCall.getState().phase).not.toBe("connecting");
  });

  it("stops a permission probe and reports a denied microphone", async () => {
    mediaPermission("denied");
    await useCall.getState().join(PLACE);
    expect(useCall.getState()).toMatchObject({
      phase: "failed",
      failure: "microphone_denied",
      activePlaceKey: null,
    });
  });

  it("does not attempt mixed-content signalling from HTTPS", async () => {
    vi.mocked(fetchCallTicket).mockResolvedValue({
      ...ticket,
      url: "ws://livekit.internal:7880",
    });
    const created = transport();
    installCallTransportFactory(() => created);
    await useCall.getState().join(PLACE);
    expect(useCall.getState().failure).toBe("mixed_content");
    expect(created.connect).not.toHaveBeenCalled();
  });

  it("reports LiveKit/API downtime without affecting messaging state", async () => {
    vi.mocked(fetchCallTicket).mockRejectedValue(
      new CallAPIError("call_request_failed", 503),
    );
    await useCall.getState().join(PLACE);
    expect(useCall.getState()).toMatchObject({
      phase: "failed",
      failure: "unavailable",
      activePlaceKey: null,
    });
  });
});

it("hydrates volatile state and accepts a newer websocket projection", async () => {
  vi.mocked(fetchCallStates).mockResolvedValue([
    {
      place: { kind: "dm", dmId: "dm-1" },
      active: true,
      startedAt: 1,
      participants: [],
    },
  ]);
  await useCall.getState().hydrate();
  useCall.getState().applyCallState({
    place: { kind: "dm", dmId: "dm-1" },
    active: true,
    startedAt: 2,
    participants: [
      {
        participant: { kind: "human", humanId: "h-2" },
        joinedAt: 2,
        screenShare: false,
      },
    ],
  });
  expect(useCall.getState().stateByPlace[PLACE]?.startedAt).toBe(2);
});
// @vitest-environment jsdom
