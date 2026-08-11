// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useMessaging } from "../store";
import { NotificationSettingsMenu } from "./notification-settings";

const mocks = vi.hoisted(() => ({
  setDefaultLevel: vi.fn(),
  setKeywords: vi.fn(),
  setSoundEnabled: vi.fn(),
}));

beforeEach(() => {
  mocks.setDefaultLevel.mockResolvedValue("confirmed");
  mocks.setKeywords.mockResolvedValue("confirmed");
  mocks.setSoundEnabled.mockResolvedValue("confirmed");
  useMessaging.setState({
    notificationDefaultLevel: "mentions",
    notificationKeywords: [],
    notificationSoundEnabled: false,
    setNotificationDefaultLevel: mocks.setDefaultLevel,
    setNotificationKeywords: mocks.setKeywords,
    setNotificationSoundEnabled: mocks.setSoundEnabled,
  });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

function open() {
  fireEvent.click(screen.getByRole("button", { name: "通知設定" }));
}

describe("NotificationSettingsMenu", () => {
  it("選択中のレベルをradioと形で示す", () => {
    render(<NotificationSettingsMenu />);
    open();

    expect(screen.getByRole("radio", { name: "メンションのみ" })).toBeChecked();
    expect(screen.getByRole("radio", { name: "すべて通知" })).not.toBeChecked();
  });

  it("レベル変更は確定後にだけstatusで返す", async () => {
    let confirm!: (result: "confirmed") => void;
    mocks.setDefaultLevel.mockReturnValueOnce(
      new Promise((resolve) => {
        confirm = resolve;
      }),
    );
    render(<NotificationSettingsMenu />);
    open();

    fireEvent.click(screen.getByRole("radio", { name: "ミュート" }));

    expect(mocks.setDefaultLevel).toHaveBeenCalledWith("mute");
    expect(screen.getByRole("status")).toBeEmptyDOMElement();

    confirm("confirmed");
    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "既定を「ミュート」にしました",
      ),
    );
  });

  it("レベル変更の失敗とロールバックをstatusで返す", async () => {
    mocks.setDefaultLevel.mockResolvedValueOnce("failed");
    render(<NotificationSettingsMenu />);
    open();

    fireEvent.click(screen.getByRole("radio", { name: "ミュート" }));

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "既定の通知を保存できず、元に戻しました",
      ),
    );
  });

  it("通知音の状態をswitchで示す", async () => {
    render(<NotificationSettingsMenu />);
    open();

    const toggle = screen.getByRole("switch", { name: /通知音/ });
    expect(toggle).toHaveAttribute("aria-checked", "false");
    fireEvent.click(toggle);

    expect(mocks.setSoundEnabled).toHaveBeenCalledWith(true);
    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        "通知音を鳴らします",
      ),
    );
  });

  it("IME変換確定のEnterではキーワードを追加しない", () => {
    render(<NotificationSettingsMenu />);
    open();
    const input = screen.getByLabelText("通知キーワードを追加");
    fireEvent.change(input, { target: { value: "設計" } });

    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229 });
    expect(mocks.setKeywords).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: "Enter" });
    expect(mocks.setKeywords).toHaveBeenCalledWith(["設計"]);
  });
});
