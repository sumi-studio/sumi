// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { Sidebar } from "./sidebar";

const mocks = vi.hoisted(() => ({ navigate: vi.fn() }));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

const human: ParticipantRef = { kind: "human", humanId: "h1" };
const humanKey = participantKey(human);
const self: MemberProfile = {
  participant: human,
  displayName: "余白",
  tagline: "創業・デザイン",
};

const refreshRoles = vi.fn();

beforeEach(() => {
  refreshRoles.mockResolvedValue(undefined);
  useMessaging.setState({
    ready: true,
    self: human,
    selfKey: humanKey,
    membersByKey: { [humanKey]: self },
    workspaces: [{ workspaceId: "w1", name: "Sumi" }],
    channels: [
      {
        channelId: "c1",
        workspaceId: "w1",
        name: "dev",
        topic: "",
        visibility: "public",
        voice: false,
      },
    ],
    dms: [],
    statusByKey: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    notificationLevelByPlace: {},
    notificationDefaultLevel: "all",
    capabilities: {
      status: true,
      replyLater: true,
      reactions: true,
      notifications: true,
      threads: true,
      polls: true,
    },
    permissions: {},
    roles: [],
    roleAssignments: [],
    refreshRoles,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("Sidebar", () => {
  it("placeごとの通知レベルはコンテキストメニューの通知設定から選べ、選択中が分かる", () => {
    render(<Sidebar />);

    // 行のメニュー → 通知設定サブメニュー（UI-CHN-01でここに一本化された）。
    fireEvent.click(screen.getByRole("button", { name: "この場所のメニュー" }));
    fireEvent.click(screen.getByRole("menuitem", { name: /通知設定/ }));
    expect(
      screen.getByRole("menuitemradio", { name: /すべて通知/ }),
    ).toHaveAttribute("aria-checked", "true");

    fireEvent.click(screen.getByRole("menuitemradio", { name: /ミュート/ }));

    expect(
      screen.queryByRole("menuitemradio", { name: /ミュート/ }),
    ).not.toBeInTheDocument();
    expect(useMessaging.getState().notificationLevelByPlace["channel:c1"]).toBe(
      "mute",
    );
  });

  it("ステータスメニューは外側を押したら閉じ、トリガーの押し直しでも閉じる", () => {
    render(<Sidebar />);

    const trigger = screen.getByLabelText("アカウントとステータス");
    fireEvent.click(trigger);
    expect(
      screen.getByRole("menu", { name: "ステータス" }),
    ).toBeInTheDocument();

    // 外側のmousedownで閉じる（StatusMenuの外側クリック規律）。
    fireEvent.mouseDown(screen.getByRole("navigation"));
    expect(
      screen.queryByRole("menu", { name: "ステータス" }),
    ).not.toBeInTheDocument();

    // トリガーで開き直し、トリガーをもう一度押すと（mousedown→clickの順でも）閉じる。
    fireEvent.click(trigger);
    fireEvent.mouseDown(trigger);
    fireEvent.click(trigger);
    expect(
      screen.queryByRole("menu", { name: "ステータス" }),
    ).not.toBeInTheDocument();
  });

  it("manage_channelsが無ければチャンネルを増やす導線を出さない", () => {
    render(<Sidebar />);

    // 押せば必ず断られるボタンは出さない。DMは自分の会話なので残る。
    expect(
      screen.queryByRole("button", { name: "チャンネルを作成" }),
    ).toBeNull();
    expect(
      screen.getByRole("button", { name: "ダイレクトメッセージを開始" }),
    ).toBeInTheDocument();
  });

  it("manage_channelsを持つ人にはチャンネル作成が現れる", () => {
    useMessaging.setState({ permissions: { manage_channels: true } });
    render(<Sidebar />);

    fireEvent.click(screen.getByRole("button", { name: "チャンネルを作成" }));

    expect(screen.getByPlaceholderText("例: dev")).toBeInTheDocument();
  });
});
