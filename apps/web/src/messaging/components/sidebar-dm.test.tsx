// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { PlaceKey } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { Sidebar } from "./sidebar";

const navigation = vi.hoisted(() => ({ navigate: vi.fn() }));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => navigation.navigate,
}));

const SELF = { kind: "human", humanId: "human-a" } as const;
const realStartDM = useMessaging.getState().startDM;

const startDM = vi.fn(async (): Promise<PlaceKey> => "dm:dm-new");

function member(id: string, displayName: string, tagline: string) {
  return {
    participant: { kind: "human", humanId: id } as const,
    displayName,
    tagline,
  };
}

function openDialog() {
  render(<Sidebar selectedPlaceKey={null} workspaceId="workspace-a" />);
  fireEvent.click(screen.getByTitle("ダイレクトメッセージを開始"));
  return screen.getByRole("dialog", {
    name: "ダイレクトメッセージを開始",
  });
}

beforeEach(() => {
  bindMessagingSessionIdentity(null);
  bindMessagingSessionIdentity("human-a");
  navigation.navigate.mockReset();
  useMessaging.setState({
    ready: true,
    self: SELF,
    selfKey: "human:human-a",
    workspaces: [{ workspaceId: "workspace-a", name: "Workspace A" }],
    channels: [],
    dms: [],
    membersByKey: {
      "human:human-a": member("human-a", "Alice", ""),
      "human:human-b": member("human-b", "KURO", "デプロイ担当"),
      "human:human-c": member("human-c", "みどり", "設計レビュー"),
    },
    statusByKey: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    activePlaceKey: null,
    startDM,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  useMessaging.setState({ startDM: realStartDM });
  bindMessagingSessionIdentity(null);
});

describe("StartDMDialog の絞り込み", () => {
  it("半角カナや大小文字の違いで相手が隠れない", () => {
    const dialog = openDialog();
    const search = within(dialog).getByRole("textbox", {
      name: "名前で絞り込む",
    });

    fireEvent.change(search, { target: { value: "ｋｕｒｏ" } });

    expect(within(dialog).getByText("KURO")).toBeVisible();
    expect(within(dialog).queryByText("みどり")).toBeNull();
  });

  it("taglineでも当たる", () => {
    const dialog = openDialog();
    fireEvent.change(
      within(dialog).getByRole("textbox", { name: "名前で絞り込む" }),
      { target: { value: "レビュー" } },
    );

    expect(within(dialog).getByText("みどり")).toBeVisible();
    expect(within(dialog).queryByText("KURO")).toBeNull();
  });

  it("絞り込みを変えても選択済みの相手は消えず、選択中として読める", async () => {
    const dialog = openDialog();
    const search = within(dialog).getByRole("textbox", {
      name: "名前で絞り込む",
    });

    fireEvent.click(within(dialog).getByText("KURO"));
    fireEvent.change(search, { target: { value: "みどり" } });

    // 選んだ相手はリストに残り、下の行にも名前が出ている。
    expect(within(dialog).getByText("KURO")).toBeVisible();
    expect(within(dialog).getByText(/KURO を選択中/)).toBeVisible();

    fireEvent.click(within(dialog).getByText("みどり"));
    fireEvent.click(
      within(dialog).getByRole("button", { name: "グループDMを作成" }),
    );

    await waitFor(() =>
      expect(startDM).toHaveBeenCalledWith([
        { kind: "human", humanId: "human-b" },
        { kind: "human", humanId: "human-c" },
      ]),
    );
    expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-new");
  });

  it("誰にも当たらない絞り込みは、空欄ではなくそう言う", () => {
    const dialog = openDialog();
    fireEvent.change(
      within(dialog).getByRole("textbox", { name: "名前で絞り込む" }),
      { target: { value: "ぬけがら" } },
    );

    expect(
      within(dialog).getByText(/「ぬけがら」に当たる相手がいません/),
    ).toBeVisible();
  });
});
