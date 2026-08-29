// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useMessaging } from "../store";
import { CallBanner } from "./call-banner";
import { CallStage } from "./call-stage";
import { useCall } from "./call-store";

const SELF = { kind: "human", humanId: "self" } as const;
const OTHER = { kind: "human", humanId: "other" } as const;
const PLACE = "dm:dm-1" as const;
const resumeAudio = useCall.getState().resumeAudio;
type CallStateOverride = Partial<ReturnType<typeof useCall.getState>>;

beforeEach(() => {
  useCall.getState().reset();
  useCall.setState({ resumeAudio });
  useMessaging.setState({
    self: SELF,
    selfKey: "human:self",
    membersByKey: {
      "human:self": {
        participant: SELF,
        displayName: "余白",
        tagline: "",
      },
      "human:other": {
        participant: OTHER,
        displayName: "はる",
        tagline: "",
      },
    },
  });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function setConnectedCall(overrides: CallStateOverride = {}) {
  useCall.setState({
    activePlaceKey: PLACE,
    phase: "connected",
    stateByPlace: {
      [PLACE]: {
        place: { kind: "dm", dmId: "dm-1" },
        active: true,
        startedAt: 1,
        participants: [SELF, OTHER].map((participant) => ({
          participant,
          joinedAt: 1,
          screenShare: false,
        })),
      },
    },
    tracks: [],
    ...overrides,
  });
}

describe("audio-only call UI", () => {
  it("keeps DM participants visible without video tracks", () => {
    setConnectedCall({
      speakingUntil: { "human:other": Date.now() + 10_000 },
    });

    render(<CallStage />);

    expect(screen.getByRole("region", { name: "通話参加者" })).toBeVisible();
    expect(screen.getByText("余白")).toBeVisible();
    expect(screen.getByText("はる")).toHaveClass("ring-emerald-500");
  });

  it("catches blocked audio retry failures and keeps recovery available", async () => {
    const rejectedResume = vi
      .fn<() => Promise<void>>()
      .mockRejectedValue(new Error("still blocked"));
    setConnectedCall({
      audioPlaybackBlocked: true,
      resumeAudio: rejectedResume,
    });

    render(<CallBanner />);
    fireEvent.click(screen.getByRole("button", { name: "音声を再生" }));

    await waitFor(() => expect(rejectedResume).toHaveBeenCalledOnce());
    expect(screen.getByRole("button", { name: "音声を再生" })).toBeVisible();
  });
});

describe("call media controls", () => {
  it("exposes stable labels and pressed state for each toggle", () => {
    setConnectedCall();
    render(<CallBanner />);

    expect(screen.getByRole("button", { name: "マイク" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByRole("button", { name: "カメラ" })).toHaveAttribute(
      "aria-pressed",
      "false",
    );
    expect(screen.getByRole("button", { name: "画面共有" })).toHaveAttribute(
      "aria-pressed",
      "false",
    );

    act(() =>
      useCall.setState({
        local: {
          micEnabled: false,
          cameraEnabled: true,
          screenShareEnabled: true,
        },
      }),
    );

    expect(screen.getByRole("button", { name: "マイク" })).toHaveAttribute(
      "aria-pressed",
      "false",
    );
    expect(screen.getByRole("button", { name: "カメラ" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
    expect(screen.getByRole("button", { name: "画面共有" })).toHaveAttribute(
      "aria-pressed",
      "true",
    );
  });
});
