// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
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
const agent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};
const secondAgent: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a2",
};
const humanKey = participantKey(human);
const agentKey = participantKey(agent);
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

  it("開いた後のステータス更新は入力中のひとことを上書きしない", () => {
    useMessaging.setState({
      statusByKey: {
        "human:h1": {
          participant: human,
          status: "available",
          note: "最初の宣言",
          expiresAt: null,
          baseStatus: null,
          baseNote: "",
        },
      },
    });
    render(<Sidebar />);

    fireEvent.click(screen.getByLabelText("アカウントとステータス"));
    const note = screen.getByLabelText("ステータスのひとこと");
    expect(note).toHaveValue("最初の宣言");
    fireEvent.change(note, { target: { value: "入力途中" } });

    act(() => {
      useMessaging.setState({
        statusByKey: {
          "human:h1": {
            participant: human,
            status: "away",
            note: "別クライアントからの更新",
            expiresAt: null,
            baseStatus: null,
            baseNote: "",
          },
        },
      });
    });

    expect(note).toHaveValue("入力途中");
  });

  it("1対1 DMのアバターはプロフィール、名前はplace遷移を開く", () => {
    useMessaging.setState({
      membersByKey: {
        [humanKey]: self,
        [agentKey]: {
          participant: agent,
          displayName: "墨",
          tagline: "秘書",
        },
      },
      dms: [{ dmId: "d1", kind: "dm", participants: [human, agent] }],
    });
    render(<Sidebar />);

    fireEvent.click(screen.getByRole("button", { name: "墨のプロフィール" }));
    expect(screen.getByText("秘書")).toBeInTheDocument();
    expect(mocks.navigate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "墨" }));
    expect(mocks.navigate).toHaveBeenCalledWith({
      to: "/dm/$dmId",
      params: { dmId: "d1" },
    });
  });

  it("group DMは先頭参加者のプロフィールと誤認させない", () => {
    const secondKey = participantKey(secondAgent);
    useMessaging.setState({
      membersByKey: {
        [humanKey]: self,
        [agentKey]: {
          participant: agent,
          displayName: "墨",
          tagline: "秘書",
        },
        [secondKey]: {
          participant: secondAgent,
          displayName: "筆",
          tagline: "編集",
        },
      },
      dms: [
        {
          dmId: "group-1",
          kind: "group_dm",
          participants: [human, agent, secondAgent],
        },
      ],
    });
    render(<Sidebar />);

    expect(
      screen.queryByRole("button", { name: "墨のプロフィール" }),
    ).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "墨、筆" })).toBeInTheDocument();
  });

  it("DMの未読数ではなくメンション数だけをurgent表示する", () => {
    useMessaging.setState({
      membersByKey: {
        [humanKey]: self,
        [agentKey]: { participant: agent, displayName: "墨", tagline: "" },
      },
      dms: [{ dmId: "d1", kind: "dm", participants: [human, agent] }],
      unreadCountByPlace: { "dm:d1": 3 },
      mentionCountByPlace: { "dm:d1": 1 },
    });
    render(<Sidebar />);

    const badge = screen.getByText("1");
    expect(badge).toHaveClass("bg-rose-500");
    expect(screen.queryByText("3")).not.toBeInTheDocument();
  });

  it("自分のプロフィールとステータス操作を別buttonに保つ", () => {
    const { container } = render(<Sidebar />);

    fireEvent.click(screen.getByRole("button", { name: "余白のプロフィール" }));
    expect(screen.getByText("創業・デザイン")).toBeInTheDocument();
    expect(
      screen.queryByRole("menu", { name: "ステータス" }),
    ).not.toBeInTheDocument();
    expect(container.querySelectorAll("button button")).toHaveLength(0);
  });
});
