// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { Sidebar } from "./sidebar";

const mocks = vi.hoisted(() => ({
  navigate: vi.fn(),
  startDM: vi.fn(),
  updateChannel: vi.fn(),
  duplicateChannel: vi.fn(),
  setPlaceNotificationLevel: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

const self: ParticipantRef = { kind: "human", humanId: "h1" };
const haru: ParticipantRef = { kind: "human", humanId: "h2" };
const kuro: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a1",
};

const members: MemberProfile[] = [
  { participant: self, displayName: "余白", tagline: "創業・デザイン" },
  { participant: haru, displayName: "Haru", tagline: "エンジニア" },
  { participant: kuro, displayName: "Kuro", tagline: "開発" },
];

beforeEach(() => {
  mocks.startDM.mockResolvedValue("dm:d1");
  mocks.updateChannel.mockResolvedValue(undefined);
  mocks.duplicateChannel.mockResolvedValue("channel:c1-copy");
  useMessaging.setState({
    ready: true,
    self,
    selfKey: participantKey(self),
    workspaces: [{ workspaceId: "ws", name: "Sumi" }],
    channels: [
      {
        channelId: "c1",
        workspaceId: "ws",
        name: "dev",
        topic: "開発の相談",
        visibility: "public",
      },
    ],
    dms: [],
    membersByKey: Object.fromEntries(
      members.map((member) => [participantKey(member.participant), member]),
    ),
    statusByKey: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    startDM: mocks.startDM,
    updateChannel: mocks.updateChannel,
    duplicateChannel: mocks.duplicateChannel,
    setPlaceNotificationLevel: mocks.setPlaceNotificationLevel,
    notificationDefaultLevel: "all",
    notificationLevelByPlace: {},
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function openDMDialog() {
  render(<Sidebar />);
  fireEvent.click(screen.getByTitle("ダイレクトメッセージを開始"));
}

describe("StartDMDialog", () => {
  it("入力で候補を絞り込み、自分は候補に出ない", () => {
    openDMDialog();
    expect(screen.getByText("Haru")).toBeInTheDocument();
    expect(screen.getByText("Kuro")).toBeInTheDocument();
    // 自分は宛先候補に出ない（左下のアカウント欄にだけ居る）。
    const dialog = screen.getByRole("dialog");
    expect(within(dialog).queryByText("余白")).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("宛先を検索"), {
      target: { value: "har" },
    });
    expect(screen.getByText("Haru")).toBeInTheDocument();
    expect(screen.queryByText("Kuro")).not.toBeInTheDocument();

    // 肩書きでも引ける。
    fireEvent.change(screen.getByLabelText("宛先を検索"), {
      target: { value: "開発" },
    });
    expect(screen.getByText("Kuro")).toBeInTheDocument();
    expect(screen.queryByText("Haru")).not.toBeInTheDocument();
  });

  it("選んだ相手は絞り込みから外れても消えない", () => {
    openDMDialog();
    fireEvent.click(screen.getByText("Haru"));
    fireEvent.change(screen.getByLabelText("宛先を検索"), {
      target: { value: "kuro" },
    });
    expect(screen.getByText("Haru")).toBeInTheDocument();
    expect(screen.getByText("Kuro")).toBeInTheDocument();
  });

  it("1人ならDM、2人以上ならグループDMの文言になる", () => {
    openDMDialog();
    expect(
      screen.getByRole("button", { name: "DMを開始" }),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByText("Haru"));
    expect(
      screen.getByRole("button", { name: "DMを開始" }),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByText("Kuro"));
    expect(
      screen.getByRole("button", { name: "グループDMを作成" }),
    ).toBeInTheDocument();
  });

  it("キャンセルは主操作の文言が伸びても動かない", () => {
    openDMDialog();
    const cancel = screen.getByRole("button", { name: "キャンセル" });
    const footer = cancel.parentElement;
    // 主操作は右端、キャンセルは左端に固定（両端揃え）。文言の伸縮は右側だけを
    // 動かすので、ポインタの下にあるキャンセルは逃げない。
    expect(footer).toHaveClass("justify-between");
    expect(footer?.firstElementChild).toBe(cancel);

    fireEvent.click(screen.getByText("Haru"));
    fireEvent.click(screen.getByText("Kuro"));
    expect(footer?.firstElementChild).toBe(
      screen.getByRole("button", { name: "キャンセル" }),
    );
  });

  it("選んだ相手でDMを開始できる", async () => {
    openDMDialog();
    fireEvent.click(screen.getByText("Haru"));
    fireEvent.click(screen.getByRole("button", { name: "DMを開始" }));
    expect(mocks.startDM).toHaveBeenCalledWith([haru]);
  });
});

function openChannelMenu() {
  render(<Sidebar />);
  fireEvent.contextMenu(screen.getByText("dev"));
  return within(screen.getByRole("menu", { name: "この場所のメニュー" }));
}

describe("チャンネルのコンテキストメニュー", () => {
  it("右クリックで編集・複製・作成が開く", () => {
    const menu = openChannelMenu();
    expect(
      menu.getByRole("menuitem", { name: "チャンネルを編集" }),
    ).toBeInTheDocument();
    expect(menu.getByRole("menuitem", { name: "複製" })).toBeInTheDocument();
    expect(
      menu.getByRole("menuitem", { name: "チャンネルを作成" }),
    ).toBeInTheDocument();
  });

  it("三点メニューからも同じ項目が開く", () => {
    render(<Sidebar />);
    fireEvent.click(screen.getAllByLabelText("この場所のメニュー")[0]);
    expect(
      screen.getByRole("menuitem", { name: "チャンネルを編集" }),
    ).toBeInTheDocument();
  });

  it("編集は名前とトピックを一緒に保存する", async () => {
    const menu = openChannelMenu();
    fireEvent.click(menu.getByRole("menuitem", { name: "チャンネルを編集" }));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを編集" });
    const [name, topic] = within(dialog).getAllByRole("textbox");
    expect(name).toHaveValue("dev");
    expect(topic).toHaveValue("開発の相談");
    fireEvent.change(name, { target: { value: "design" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "保存" }));
    expect(mocks.updateChannel).toHaveBeenCalledWith("c1", {
      name: "design",
      topic: "開発の相談",
    });
  });

  it("複製はそのまま実行して新しい場所へ移る", async () => {
    const menu = openChannelMenu();
    fireEvent.click(menu.getByRole("menuitem", { name: "複製" }));
    expect(mocks.duplicateChannel).toHaveBeenCalledWith("c1");
  });

  it("通知設定は横に開くサブメニューに入る", () => {
    const menu = openChannelMenu();
    // 主メニューには通知レベルが直接並ばない。
    expect(screen.queryByText("メンションのみ")).not.toBeInTheDocument();
    const submenuTrigger = menu.getByRole("menuitem", { name: /通知設定/ });
    expect(submenuTrigger).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(submenuTrigger);
    expect(submenuTrigger).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(screen.getByText("メンションのみ"));
    expect(mocks.setPlaceNotificationLevel).toHaveBeenCalledWith(
      "channel:c1",
      "mentions",
    );
  });
});
