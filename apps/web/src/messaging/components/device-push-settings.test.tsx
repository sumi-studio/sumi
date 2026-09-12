// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { DevicePushSettings } from "./device-push-settings";

const mocks = vi.hoisted(() => ({
  enable: vi.fn(),
  disable: vi.fn(),
  state: "idle",
  supported: true,
}));
vi.mock("../push", () => ({
  enablePushSubscription: mocks.enable,
  disablePushSubscription: mocks.disable,
  getDevicePushState: () => mocks.state,
  isPushSupported: () => mocks.supported,
  subscribeDevicePush: () => () => undefined,
}));
beforeEach(() => {
  mocks.state = "idle";
  mocks.supported = true;
  mocks.enable.mockReset().mockResolvedValue(true);
  mocks.disable.mockReset().mockResolvedValue(true);
  vi.stubGlobal("Notification", {
    permission: "default",
    requestPermission: vi.fn().mockResolvedValue("granted"),
  });
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
it("requests permission only from the action and reports registration failure", async () => {
  mocks.enable.mockResolvedValue(false);
  render(<DevicePushSettings />);
  expect(Notification.requestPermission).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "オンにする" }));
  await waitFor(() => expect(mocks.enable).toHaveBeenCalledWith(true));
  expect(await screen.findByText(/通知を登録できませんでした/)).toBeVisible();
});
it("retries a failed disable without enabling again", async () => {
  mocks.state = "disable-error";
  render(<DevicePushSettings />);
  fireEvent.click(screen.getByRole("button", { name: "再試行" }));
  await waitFor(() => expect(mocks.disable).toHaveBeenCalledOnce());
  expect(mocks.enable).not.toHaveBeenCalled();
});
it("explains OS-denied permission rather than repeatedly requesting it", () => {
  vi.stubGlobal("Notification", {
    permission: "denied",
    requestPermission: vi.fn(),
  });
  render(<DevicePushSettings />);
  expect(screen.getByText(/ブラウザまたは端末の設定/)).toBeVisible();
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
});
it("provides Home Screen guidance when browser push is unavailable", () => {
  mocks.supported = false;
  render(<DevicePushSettings />);
  expect(screen.getByText(/ホーム画面に追加/)).toBeVisible();
});
