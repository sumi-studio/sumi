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
import type { MemberProfile, ParticipantRef, ThreadSummary } from "../model";
import { participantKey } from "../model";
import { ThreadPanel } from "./thread-panel";

const mocks = vi.hoisted(() => ({
  createThread: vi.fn(),
  loadThreads: vi.fn(),
  navigate: vi.fn(),
}));

const PARENT_KEY = "channel:channel-1";
const PEOPLE: ParticipantRef[] = [
  { kind: "human", humanId: "human-a" },
  { kind: "human", humanId: "human-b" },
  { kind: "human", humanId: "human-c" },
  { kind: "personality_agent", personalityAgentId: "agent-a" },
  { kind: "personality_agent", personalityAgentId: "agent-b" },
];

const state: {
  threadsById: Record<string, ThreadSummary>;
  threadsLoadedForPlace: Record<string, boolean>;
  createThread: typeof mocks.createThread;
  loadThreads: typeof mocks.loadThreads;
  membersByKey: Record<string, MemberProfile>;
  unreadCountByPlace: Record<string, number>;
} = {
  threadsById: {},
  threadsLoadedForPlace: {},
  createThread: mocks.createThread,
  loadThreads: mocks.loadThreads,
  membersByKey: {},
  unreadCountByPlace: {},
};

vi.mock("../store", () => ({
  useMessaging: (selector: (value: typeof state) => unknown) => selector(state),
}));
vi.mock("../place-route", () => ({ usePlaceNavigate: () => mocks.navigate }));

function thread(
  threadId: string,
  overrides: Partial<ThreadSummary> = {},
): ThreadSummary {
  return {
    threadId,
    parentPlace: { kind: "channel", channelId: "channel-1" },
    parentMessageId: null,
    workspaceId: "workspace-1",
    name: threadId,
    messageCount: 0,
    lastMessageAt: null,
    lastMessage: "",
    participants: [],
    latestSeq: 0,
    ...overrides,
    revision: overrides.revision ?? 1,
  };
}

describe("ThreadPanel", () => {
  beforeEach(() => {
    mocks.createThread.mockReset();
    mocks.loadThreads.mockReset();
    mocks.loadThreads.mockResolvedValue(undefined);
    mocks.navigate.mockReset();
    state.threadsById = {};
    state.threadsLoadedForPlace = {};
    state.membersByKey = Object.fromEntries(
      PEOPLE.map((participant, index) => [
        participantKey(participant),
        {
          participant,
          displayName: `Person ${index + 1}`,
          tagline: "",
        },
      ]),
    );
    state.unreadCountByPlace = {};
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("distinguishes loading, failure, retry, and a successful empty list", async () => {
    let rejectLoad: ((error: Error) => void) | undefined;
    mocks.loadThreads.mockReturnValueOnce(
      new Promise<void>((_resolve, reject) => {
        rejectLoad = reject;
      }),
    );
    const onClose = vi.fn();
    render(<ThreadPanel parentKey={PARENT_KEY} onClose={onClose} />);

    expect(screen.getByRole("status")).toHaveTextContent(
      "スレッドを読み込み中",
    );
    await act(async () => rejectLoad?.(new Error("offline")));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "スレッドを読み込めませんでした",
    );

    state.threadsLoadedForPlace[PARENT_KEY] = true;
    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    expect(await screen.findByText("スレッドはありません")).toBeInTheDocument();
    expect(
      screen.getAllByRole("button", { name: "スレッドを作成" }),
    ).toHaveLength(2);
    expect(mocks.loadThreads).toHaveBeenCalledTimes(2);
  });

  it("searches rows and preserves unread, preview, participants, count, and activity", async () => {
    state.threadsLoadedForPlace[PARENT_KEY] = true;
    state.threadsById = {
      auth: thread("auth", {
        name: "Auth Flow",
        messageCount: 12,
        lastMessage: "修正しました",
        lastMessageAt: Date.now() - 5 * 60_000,
        participants: PEOPLE,
      }),
      empty: thread("empty", { name: "Empty Thread" }),
    };
    state.unreadCountByPlace = { "thread:auth": 120 };
    const onClose = vi.fn();
    render(<ThreadPanel parentKey={PARENT_KEY} onClose={onClose} />);

    expect(await screen.findByText("Auth Flow")).toBeInTheDocument();
    expect(screen.getByText("99+")).toBeInTheDocument();
    expect(screen.getByText("修正しました")).toBeInTheDocument();
    expect(screen.getByText(/12件 · 5分前/)).toBeInTheDocument();
    expect(screen.getByTitle("参加者 4人を表示")).toBeInTheDocument();
    expect(screen.getByText("まだ発言はありません")).toBeInTheDocument();

    const search = screen.getByRole("textbox", {
      name: "スレッドを名前で探す",
    });
    fireEvent.change(search, { target: { value: "AUTH" } });
    expect(screen.getByText("Auth Flow")).toBeInTheDocument();
    expect(screen.queryByText("Empty Thread")).not.toBeInTheDocument();

    fireEvent.change(search, { target: { value: "missing" } });
    expect(
      screen.getByText("一致するスレッドはありません"),
    ).toBeInTheDocument();

    fireEvent.change(search, { target: { value: "auth" } });
    fireEvent.click(screen.getByText("Auth Flow").closest("button") as Element);
    expect(mocks.navigate).toHaveBeenCalledWith("thread:auth");
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("focuses its dedicated form, ignores IME Enter, and supports Escape and cancel", async () => {
    state.threadsLoadedForPlace[PARENT_KEY] = true;
    render(<ThreadPanel parentKey={PARENT_KEY} onClose={vi.fn()} />);
    await screen.findByText("スレッドはありません");
    const createToggle = screen.getAllByRole("button", {
      name: "スレッドを作成",
    })[0];
    if (!createToggle) throw new Error("create toggle missing");

    fireEvent.click(createToggle);
    const input = screen.getByRole("textbox", { name: "スレッドの名前" });
    expect(input).toHaveFocus();
    expect(createToggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByRole("button", { name: "作成" })).toBeDisabled();

    fireEvent.change(input, { target: { value: "変換中" } });
    fireEvent.keyDown(input, { key: "Enter", isComposing: true });
    fireEvent.keyDown(input, { key: "Enter", keyCode: 229 });
    expect(mocks.createThread).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: "Escape" });
    expect(
      screen.queryByRole("textbox", { name: "スレッドの名前" }),
    ).not.toBeInTheDocument();
    expect(createToggle).toHaveAttribute("aria-expanded", "false");
    expect(createToggle).toHaveFocus();

    fireEvent.click(createToggle);
    fireEvent.click(screen.getByRole("button", { name: "キャンセル" }));
    expect(
      screen.queryByRole("textbox", { name: "スレッドの名前" }),
    ).not.toBeInTheDocument();
    expect(createToggle).toHaveFocus();
  });

  it("retries one store-owned gesture while blocking duplicate Enter submissions", async () => {
    state.threadsLoadedForPlace[PARENT_KEY] = true;
    let resolveCreate: ((key: string) => void) | undefined;
    mocks.createThread
      .mockRejectedValueOnce(new Error("temporary"))
      .mockReturnValueOnce(
        new Promise<string>((resolve) => {
          resolveCreate = resolve;
        }),
      );
    const onClose = vi.fn();
    render(<ThreadPanel parentKey={PARENT_KEY} onClose={onClose} />);
    await screen.findByText("スレッドはありません");
    const createToggle = screen.getAllByRole("button", {
      name: "スレッドを作成",
    })[0];
    if (!createToggle) throw new Error("create toggle missing");
    fireEvent.click(createToggle);
    const input = screen.getByRole("textbox", { name: "スレッドの名前" });
    fireEvent.change(input, { target: { value: "😀".repeat(101) } });
    expect(Array.from((input as HTMLInputElement).value)).toHaveLength(100);

    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "スレッドを作成できませんでした",
      ),
    );
    expect(Array.from((input as HTMLInputElement).value)).toHaveLength(100);

    fireEvent.keyDown(input, { key: "Enter" });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(mocks.createThread).toHaveBeenCalledTimes(2);
    expect(mocks.createThread).toHaveBeenLastCalledWith(
      PARENT_KEY,
      "😀".repeat(100),
      null,
    );

    await act(async () => resolveCreate?.("thread:thread-1"));
    expect(mocks.navigate).toHaveBeenCalledWith("thread:thread-1");
    expect(onClose).toHaveBeenCalledOnce();
  });
});
