// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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
  it("placeごとの通知レベルは選択中が✓で分かり、選ぶと閉じる", () => {
    render(<Sidebar />);

    fireEvent.click(screen.getByRole("button", { name: "通知設定" }));
    expect(screen.getByRole("radio", { name: /すべて通知/ })).toBeChecked();

    fireEvent.click(screen.getByRole("radio", { name: /ミュート/ }));

    expect(
      screen.queryByRole("radio", { name: /ミュート/ }),
    ).not.toBeInTheDocument();
    expect(useMessaging.getState().notificationLevelByPlace["channel:c1"]).toBe(
      "mute",
    );
  });

  it("ステータスメニューは外側を押したら閉じる", () => {
    render(<Sidebar />);

    fireEvent.click(screen.getByRole("button", { name: /余白/ }));
    expect(
      screen.getByRole("radio", { name: "取り込み中" }),
    ).toBeInTheDocument();

    fireEvent.pointerDown(screen.getByRole("navigation"));

    expect(
      screen.queryByRole("radio", { name: "取り込み中" }),
    ).not.toBeInTheDocument();
  });
});
