// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Message } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { MessageThreadAction } from "./message-thread";

const mocks = vi.hoisted(() => ({
  createThread: vi.fn(),
  navigate: vi.fn(),
}));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => mocks.navigate,
}));

const message: Message = {
  messageId: "message-1",
  place: { kind: "channel", channelId: "channel-1" },
  seq: 1,
  author: { kind: "human", humanId: "human-a" },
  content: "スレッドにします",
  mentions: [],
  urgency: "normal",
  reactions: [],
  attachments: [],
  poll: null,
  replyTo: null,
  createdAt: 1,
  editedAt: null,
  deleted: false,
};

const realCreateThread = useMessaging.getState().createThread;

describe("MessageThreadAction", () => {
  beforeEach(() => {
    bindMessagingSessionIdentity("thread-action-test");
    mocks.createThread.mockReset();
    mocks.navigate.mockReset();
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: true,
      },
      activePlaceKey: "channel:channel-1",
      threadsById: {},
      createThread: mocks.createThread,
    });
  });

  afterEach(() => {
    cleanup();
    useMessaging.setState({ createThread: realCreateThread });
    bindMessagingSessionIdentity(null);
  });

  it("does not expose thread creation when threads are unavailable", () => {
    useMessaging.setState((state) => ({
      capabilities: { ...state.capabilities, threads: false },
    }));

    render(<MessageThreadAction message={message} />);

    expect(screen.queryByLabelText("スレッドを作成")).not.toBeInTheDocument();
  });

  it("focuses the named input and closes through Escape or an outside pointer", async () => {
    render(<MessageThreadAction message={message} />);
    const trigger = screen.getByRole("button", { name: "スレッドを作成" });

    expect(trigger).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(trigger);

    const input = screen.getByRole("textbox", { name: "スレッドの名前" });
    expect(trigger).toHaveAttribute("aria-expanded", "true");
    expect(input).toHaveFocus();
    expect(input).toHaveValue("スレッドにします");
    expect(
      screen.getByText("スレッドを作成 — この発言から枝分かれします"),
    ).toBeInTheDocument();

    fireEvent.keyDown(input, { key: "Escape" });
    await waitFor(() =>
      expect(trigger).toHaveAttribute("aria-expanded", "false"),
    );
    expect(trigger).toHaveFocus();

    fireEvent.click(trigger);
    expect(
      screen.getByRole("textbox", { name: "スレッドの名前" }),
    ).toHaveFocus();
    fireEvent.pointerDown(document.body);
    await waitFor(() =>
      expect(trigger).toHaveAttribute("aria-expanded", "false"),
    );
  });

  it("ignores IME Enter and retries with the same input", async () => {
    mocks.createThread
      .mockRejectedValueOnce(new Error("temporary"))
      .mockResolvedValueOnce("thread:thread-1");
    render(<MessageThreadAction message={message} />);
    fireEvent.click(screen.getByRole("button", { name: "スレッドを作成" }));
    const input = screen.getByRole("textbox", { name: "スレッドの名前" });
    fireEvent.change(input, { target: { value: "認証リダイレクト" } });

    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229 });
    expect(mocks.createThread).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "スレッドを作成できませんでした",
      ),
    );
    expect(input).toHaveValue("認証リダイレクト");
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() =>
      expect(mocks.navigate).toHaveBeenCalledWith("thread:thread-1"),
    );
    expect(mocks.createThread).toHaveBeenCalledTimes(2);
    expect(mocks.createThread).toHaveBeenLastCalledWith(
      "channel:channel-1",
      "認証リダイレクト",
      message.messageId,
    );
  });

  it("blocks duplicate submissions and clamps names by Unicode code point", async () => {
    let resolveCreate: ((key: string) => void) | undefined;
    mocks.createThread.mockReturnValue(
      new Promise<string>((resolve) => {
        resolveCreate = resolve;
      }),
    );
    render(<MessageThreadAction message={message} />);
    fireEvent.click(screen.getByRole("button", { name: "スレッドを作成" }));
    const input = screen.getByRole("textbox", { name: "スレッドの名前" });
    fireEvent.change(input, { target: { value: "😀".repeat(101) } });

    expect(Array.from((input as HTMLInputElement).value)).toHaveLength(100);
    fireEvent.keyDown(input, { key: "Enter" });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(mocks.createThread).toHaveBeenCalledTimes(1);
    expect(
      Array.from(mocks.createThread.mock.calls[0]?.[1] as string),
    ).toHaveLength(100);

    await act(async () => resolveCreate?.("thread:thread-1"));
    expect(mocks.navigate).toHaveBeenCalledWith("thread:thread-1");
  });

  it("disables creation for a blank name", () => {
    render(<MessageThreadAction message={message} />);
    fireEvent.click(screen.getByRole("button", { name: "スレッドを作成" }));
    fireEvent.change(screen.getByRole("textbox", { name: "スレッドの名前" }), {
      target: { value: "   " },
    });

    expect(screen.getByRole("button", { name: "作成" })).toBeDisabled();
  });
});
