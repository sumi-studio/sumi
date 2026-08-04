// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef, Place } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { CallBanner } from "./call-banner";
import { CallStage } from "./call-stage";
import { CallStartButtons } from "./call-start-buttons";
import { useCall } from "./call-store";
import { IncomingCallModal } from "./incoming-call";
import type { CallState } from "./model";

const me: ParticipantRef = { kind: "human", humanId: "h1" };
const haru: ParticipantRef = { kind: "human", humanId: "h2" };
const kuro: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};
const meKey = participantKey(me);

const members: MemberProfile[] = [
  { participant: me, displayName: "余白", tagline: "" },
  { participant: haru, displayName: "はる", tagline: "" },
  { participant: kuro, displayName: "墨", tagline: "秘書" },
];

const DM: Place = { kind: "dm", dmId: "d1" };

function callWith(place: Place, participants: ParticipantRef[]): CallState {
  return {
    place,
    active: true,
    startedAt: 1_000,
    participants: participants.map((participant) => ({
      participant,
      joinedAt: 1_000,
      screenShare: false,
    })),
  };
}

beforeEach(() => {
  useMessaging.setState({
    ready: true,
    self: me,
    selfKey: meKey,
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
  });
  useCall.setState({
    stateByPlace: {},
    activePlaceKey: null,
    phase: "idle",
    failure: null,
    local: {
      micEnabled: true,
      cameraEnabled: false,
      screenShareEnabled: false,
    },
    tracks: [],
    speakingUntil: {},
    dismissedPlaces: {},
  });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("通話バナー", () => {
  it("通話が無ければ何も出さない", () => {
    const { container } = render(<CallBanner placeKey="dm:d1" />);
    expect(container).toBeEmptyDOMElement();
  });

  it("通話中は参加者と参加の導線を出す", () => {
    useCall.setState({ stateByPlace: { "dm:d1": callWith(DM, [haru, kuro]) } });
    const join = vi.fn();
    useCall.setState({ join });
    render(<CallBanner placeKey="dm:d1" />);

    expect(screen.getByText("現在通話中")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "参加" }));
    expect(join).toHaveBeenCalledWith("dm:d1");
  });

  it("自分が入っている通話ではバナーを畳む（CallStageに替わる）", () => {
    useCall.setState({
      stateByPlace: { "dm:d1": callWith(DM, [me, haru]) },
      activePlaceKey: "dm:d1",
    });
    const { container } = render(<CallBanner placeKey="dm:d1" />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("通話画面", () => {
  it("参加者のタイルと自分のコントロールを出す", () => {
    useCall.setState({
      stateByPlace: { "dm:d1": callWith(DM, [me, kuro]) },
      activePlaceKey: "dm:d1",
      phase: "connected",
    });
    render(<CallStage placeKey="dm:d1" />);

    // 人間と人格agentが同じタイルの文法で並ぶ（1文字の名前はアバターの
    // イニシャルとも一致するのでgetAllByTextで受ける）。
    expect(screen.getAllByText("余白").length).toBeGreaterThan(0);
    expect(screen.getAllByText("墨").length).toBeGreaterThan(0);
    for (const label of [
      "ミュートする",
      "カメラを入れる",
      "画面を共有する",
      "通話を終える",
    ]) {
      expect(screen.getByRole("button", { name: label })).toBeInTheDocument();
    }
  });

  it("コントロールはstoreの操作をそのまま呼ぶ", () => {
    const toggleMicrophone = vi.fn();
    const toggleCamera = vi.fn();
    const toggleScreenShare = vi.fn();
    const leave = vi.fn();
    useCall.setState({
      stateByPlace: { "dm:d1": callWith(DM, [me]) },
      activePlaceKey: "dm:d1",
      phase: "connected",
      toggleMicrophone,
      toggleCamera,
      toggleScreenShare,
      leave,
    });
    render(<CallStage placeKey="dm:d1" />);

    fireEvent.click(screen.getByRole("button", { name: "ミュートする" }));
    fireEvent.click(screen.getByRole("button", { name: "カメラを入れる" }));
    fireEvent.click(screen.getByRole("button", { name: "画面を共有する" }));
    fireEvent.click(screen.getByRole("button", { name: "通話を終える" }));
    expect(toggleMicrophone).toHaveBeenCalled();
    expect(toggleCamera).toHaveBeenCalled();
    expect(toggleScreenShare).toHaveBeenCalled();
    expect(leave).toHaveBeenCalled();
  });

  it("入っていないplaceには出ない", () => {
    useCall.setState({ activePlaceKey: "dm:other", phase: "connected" });
    const { container } = render(<CallStage placeKey="dm:d1" />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe("着信", () => {
  it("相手の名前と応答・拒否を出す", () => {
    useCall.setState({ stateByPlace: { "dm:d1": callWith(DM, [haru]) } });
    const join = vi.fn();
    useCall.setState({ join });
    render(<IncomingCallModal />);

    expect(screen.getByText("はる")).toBeInTheDocument();
    expect(screen.getByText("着信中…")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "応答" }));
    expect(join).toHaveBeenCalledWith("dm:d1");
  });

  it("拒否は自分の画面を閉じるだけ（相手の通話は残る）", () => {
    useCall.setState({ stateByPlace: { "dm:d1": callWith(DM, [haru]) } });
    render(<IncomingCallModal />);

    fireEvent.click(screen.getByRole("button", { name: "拒否" }));
    expect(screen.queryByText("着信中…")).not.toBeInTheDocument();
    expect(useCall.getState().stateByPlace["dm:d1"]).toBeDefined();
  });
});

describe("通話開始ボタン", () => {
  it("音声とビデオの両方から同じ通話へ入る", async () => {
    const join = vi.fn().mockResolvedValue(undefined);
    useCall.setState({ join });
    render(<CallStartButtons placeKey="dm:d1" />);

    fireEvent.click(screen.getByRole("button", { name: "音声通話を開始" }));
    fireEvent.click(screen.getByRole("button", { name: "ビデオ通話を開始" }));
    await Promise.resolve();
    expect(join).toHaveBeenCalledTimes(2);
    expect(join).toHaveBeenCalledWith("dm:d1");
  });

  it("SFUが無い環境では理由を言い、ボタンは残す", () => {
    useCall.setState({ failure: "unavailable" });
    render(<CallStartButtons placeKey="dm:d1" />);
    expect(screen.getByText("通話は未設定です")).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "音声通話を開始" }),
    ).toBeEnabled();
  });
});
