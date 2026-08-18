// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ThreadPanel } from "./thread-panel";

const mocks = vi.hoisted(() => ({
  createThread: vi.fn(),
  navigate: vi.fn(),
}));
const state = {
  threadsById: {},
  createThread: mocks.createThread,
};

vi.mock("../store", () => ({
  useMessaging: (selector: (value: typeof state) => unknown) => selector(state),
}));
vi.mock("../place-route", () => ({ usePlaceNavigate: () => mocks.navigate }));

afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});

describe("ThreadPanel", () => {
  it("creates only one thread for rapid submissions while the request is in flight", async () => {
    let resolveCreate: (key: string) => void;
    mocks.createThread.mockReturnValue(
      new Promise<string>((resolve) => {
        resolveCreate = resolve;
      }),
    );
    const onClose = vi.fn();
    render(<ThreadPanel parentKey="channel:channel-1" onClose={onClose} />);

    fireEvent.change(screen.getByPlaceholderText("スレッド名"), {
      target: { value: "認証リダイレクト" },
    });
    const create = screen.getByRole("button", { name: "スレッドを作成" });
    fireEvent.click(create);
    fireEvent.click(create);

    expect(mocks.createThread).toHaveBeenCalledTimes(1);
    expect(mocks.createThread).toHaveBeenCalledWith(
      "channel:channel-1",
      "認証リダイレクト",
      null,
      expect.any(String),
    );
    expect(create).toBeDisabled();

    await act(async () => resolveCreate?.("thread:thread-1"));

    expect(mocks.navigate).toHaveBeenCalledWith("thread:thread-1");
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
