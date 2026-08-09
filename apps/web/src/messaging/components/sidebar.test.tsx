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
import type { ParticipantRef } from "../model";
import { useMessaging } from "../store";
import { Sidebar } from "./sidebar";

vi.mock("@tanstack/react-router", () => ({ useNavigate: () => vi.fn() }));

const human: ParticipantRef = { kind: "human", humanId: "h1" };

beforeEach(() => {
  useMessaging.setState({
    ready: true,
    self: human,
    selfKey: "human:h1",
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
    membersByKey: {
      "human:h1": { participant: human, displayName: "余白", tagline: "" },
    },
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
    },
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
});
