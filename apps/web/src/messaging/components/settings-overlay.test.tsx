// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantRef } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import {
  SettingsOverlay,
  SettingsTrigger,
  useSettingsOverlay,
} from "./settings-overlay";

const SELF: ParticipantRef = {
  kind: "human",
  humanId: "0199aaaa-0000-7000-8000-000000000001",
};
const SELF_KEY = participantKey(SELF);

const AGENT: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "0199aaaa-0000-7000-8000-000000000002",
};

const updateProfile = vi.fn();
const auth = vi.hoisted(() => ({
  syncDisplayName: vi.fn(),
  user: {
    id: "0199aaaa-0000-7000-8000-000000000001",
    displayName: "yohaku",
  },
}));

vi.mock("../../auth/auth-context", () => ({
  useAuth: () => auth,
}));

function seed(
  member: Partial<MemberProfile> = {},
  self: ParticipantRef = SELF,
) {
  const key = participantKey(self);
  useMessaging.setState({
    self,
    selfKey: key,
    membersByKey: {
      [key]: {
        participant: self,
        displayName: "yohaku",
        tagline: "デザイン",
        revision: 1,
        ...member,
      },
    },
    updateProfile,
  });
}

beforeEach(() => {
  updateProfile.mockResolvedValue({
    participant: SELF,
    displayName: "yohaku",
    tagline: "デザイン",
    revision: 2,
  });
  seed();
  useSettingsOverlay.setState({ open: true, section: "profile" });
});

afterEach(() => {
  cleanup();
  useSettingsOverlay.setState({ open: false, section: "profile" });
  vi.clearAllMocks();
});

describe("個人設定", () => {
  it("サイドバーの導線からプロフィールが開く", () => {
    useSettingsOverlay.setState({ open: false });
    render(
      <>
        <SettingsTrigger />
        <SettingsOverlay />
      </>,
    );
    expect(screen.queryByRole("dialog", { name: "個人設定" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "個人設定" }));

    expect(
      screen.getByRole("dialog", { name: "個人設定" }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("表示名")).toHaveValue("yohaku");
    expect(screen.getByLabelText("ひとこと")).toHaveValue("デザイン");
  });

  it("変更していないうちは保存できない", () => {
    render(<SettingsOverlay />);
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();

    fireEvent.change(screen.getByLabelText("ひとこと"), {
      target: { value: "開発" },
    });
    expect(screen.getByRole("button", { name: "保存" })).toBeEnabled();
  });

  it("保存は変更した値だけを前後の空白を落として送る", async () => {
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: "  余白  " },
    });
    fireEvent.change(screen.getByLabelText("ひとこと"), {
      target: { value: " 開発 " },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(updateProfile).toHaveBeenCalledWith({
        displayName: "余白",
        tagline: "開発",
      }),
    );
  });

  it("保存後はサーバー確定値を入力と基準値にして、同じ内容を再送しない", async () => {
    updateProfile.mockResolvedValueOnce({
      participant: SELF,
      displayName: "行内 空白",
      tagline: "デザイン",
      revision: 2,
    });
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: "  行内   空白  " },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(updateProfile).toHaveBeenCalledWith({
        displayName: "行内   空白",
      }),
    );
    await waitFor(() =>
      expect(screen.getByLabelText("表示名")).toHaveValue("行内 空白"),
    );
    expect(screen.getByText("保存しました")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    expect(updateProfile).toHaveBeenCalledTimes(1);
  });

  it("別経路で変わった未編集のひとことを保存時に上書きしない", async () => {
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: "余白" },
    });
    act(() => {
      useMessaging.setState({
        membersByKey: {
          [SELF_KEY]: {
            participant: SELF,
            displayName: "yohaku",
            tagline: "別タブの更新",
            revision: 2,
          },
        },
      });
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(updateProfile).toHaveBeenCalledWith({ displayName: "余白" }),
    );
  });

  it("Humanの保存後は確定した表示名をAuthContextにも同期する", async () => {
    updateProfile.mockResolvedValueOnce({
      participant: SELF,
      displayName: "確定名",
      tagline: "デザイン",
      revision: 2,
    });
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: "入力値" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(auth.syncDisplayName).toHaveBeenCalledWith(SELF.humanId, "確定名"),
    );
  });

  it("名乗れない表示名では保存を出さない", () => {
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: "   " },
    });

    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
    expect(updateProfile).not.toHaveBeenCalled();
  });

  it("非BMP文字をcode pointで数え、80文字までは入力できる", () => {
    render(<SettingsOverlay />);
    const eighty = "😀".repeat(80);
    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: eighty },
    });
    expect(screen.getByLabelText("表示名")).toHaveValue(eighty);
    expect(
      screen.getByText("他の参加者に見える名前です（80 / 80）"),
    ).toBeInTheDocument();
  });

  it("保存が拒まれたら理由を出し、入力を捨てない", async () => {
    updateProfile.mockRejectedValueOnce(new Error("invalid_display_name"));
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("表示名"), {
      target: { value: "余白" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(screen.getByText(/保存できませんでした/)).toBeInTheDocument(),
    );
    expect(screen.getByLabelText("表示名")).toHaveValue("余白");
  });

  it("他の経路で名乗りが変わったら、触っていない欄だけ追従する", async () => {
    render(<SettingsOverlay />);

    fireEvent.change(screen.getByLabelText("ひとこと"), {
      target: { value: "書きかけ" },
    });
    act(() => {
      // PA の道具や別タブから profile_updated が届いた状態。
      useMessaging.setState({
        membersByKey: {
          [SELF_KEY]: {
            participant: SELF,
            displayName: "余白",
            tagline: "秘書",
          },
        },
      });
    });

    expect(screen.getByLabelText("表示名")).toHaveValue("余白");
    expect(screen.getByLabelText("ひとこと")).toHaveValue("書きかけ");
  });

  it("人格agentも同じ画面で同じ名乗り方をする", () => {
    seed({ displayName: "Kuro", tagline: "調べもの" }, AGENT);
    render(<SettingsOverlay />);

    expect(screen.getByLabelText("表示名")).toHaveValue("Kuro");
    expect(screen.getByLabelText("ひとこと")).toHaveValue("調べもの");
  });

  it("アカウントには参加者IDを出す。名前の代わりにはしない", () => {
    useSettingsOverlay.setState({ section: "account" });
    render(<SettingsOverlay />);

    expect(screen.getByText(SELF.humanId)).toBeInTheDocument();
    expect(screen.queryByLabelText("表示名")).toBeNull();
  });

  it("小画面でも表示されるセクションナビからアカウントへ移れる", () => {
    render(<SettingsOverlay />);

    const navigation = screen.getByRole("navigation", {
      name: "設定セクション",
    });
    expect(navigation).not.toHaveClass("hidden");

    fireEvent.click(
      within(navigation).getByRole("button", { name: "アカウント" }),
    );

    expect(screen.getByText(SELF.humanId)).toBeInTheDocument();
    expect(
      within(navigation).getByRole("button", { name: "アカウント" }),
    ).toHaveAttribute("aria-current", "page");
    expect(screen.queryByLabelText("表示名")).toBeNull();
  });

  it("Escで閉じる", () => {
    render(<SettingsOverlay />);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByRole("dialog", { name: "個人設定" })).toBeNull();
  });
});
