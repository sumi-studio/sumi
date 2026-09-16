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
import { EMPTY_COMPOSER_DRAFT } from "../composer-draft";
import type { Message, PlaceKey } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { MessagingScreen } from "./messaging-screen";

const mocks = vi.hoisted(() => ({
  placeNavigate: vi.fn(),
  jumpToSeq: vi.fn(),
  jumpToMessage: vi.fn(),
  jump: { placeKey: "channel:channel-b", seq: 1 } as {
    placeKey: PlaceKey;
    seq: number;
  },
}));

vi.mock("../../shell/app-rail", () => ({
  AppRail: () => <div data-testid="app-rail" />,
}));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => mocks.placeNavigate,
}));

vi.mock("./message-search", () => ({
  MessageSearch: ({ onJump }: { onJump: (jump: unknown) => void }) => (
    <button type="button" onClick={() => onJump(mocks.jump)}>
      jump to old result
    </button>
  ),
}));

vi.mock("./sidebar", () => ({
  Sidebar: ({ selectedPlaceKey }: { selectedPlaceKey: PlaceKey | null }) => (
    <aside data-testid="sidebar-selection">
      {selectedPlaceKey ?? "unselected"}
    </aside>
  ),
}));

vi.mock("./connection-banner", () => ({ ConnectionBanner: () => null }));
vi.mock("./member-list", () => ({
  MemberList: () => <aside data-testid="member-list" />,
}));
vi.mock("./message-list", () => ({
  MessageList: ({ handleRef }: { handleRef?: { current: unknown } }) => {
    if (handleRef) {
      handleRef.current = {
        jumpToSeq: mocks.jumpToSeq,
        jumpToMessage: mocks.jumpToMessage,
      };
    }
    return null;
  },
}));
vi.mock("./composer", () => ({ Composer: () => null }));

const SELF = { kind: "human", humanId: "human-a" } as const;
const CHANNEL_A: PlaceKey = "channel:channel-a";
const CHANNEL_B: PlaceKey = "channel:channel-b";
const realInit = useMessaging.getState().init;
const realSelectPlace = useMessaging.getState().selectPlace;
const realLoadPlaceAround = useMessaging.getState().loadPlaceAround;
const realLoadThread = useMessaging.getState().loadThread;
const realLoadThreads = useMessaging.getState().loadThreads;

function seedCurrentPlace() {
  useMessaging.setState({
    init: vi.fn(),
    selectPlace: (key) =>
      useMessaging.setState({
        activePlaceKey: key,
        editingMessageId: null,
      }),
    ready: true,
    capabilities: {
      status: false,
      replyLater: false,
      reactions: false,
      notifications: false,
      threads: false,
    },
    self: SELF,
    selfKey: "human:human-a",
    workspaces: [
      { workspaceId: "workspace-a", name: "Workspace A" },
      { workspaceId: "workspace-b", name: "Workspace B" },
    ],
    channels: [
      {
        channelId: "channel-a",
        workspaceId: "workspace-a",
        revision: 1,
        name: "alpha",
        topic: "",
        visibility: "public",
        voice: false,
      },
      {
        channelId: "channel-b",
        workspaceId: "workspace-b",
        revision: 1,
        name: "beta",
        topic: "",
        visibility: "public",
        voice: false,
      },
    ],
    dms: [],
    threadsById: {},
    threadsLoadedForPlace: {},
    threadLoadErrorsById: {},
    membersByKey: {
      "human:human-a": {
        participant: SELF,
        displayName: "Alice",
        tagline: "",
      },
    },
    activePlaceKey: CHANNEL_A,
    editingMessageId: "editing-a",
    draftByPlace: {
      [CHANNEL_A]: {
        ...EMPTY_COMPOSER_DRAFT,
        replyTarget: {
          messageId: "reply-a",
          authorLabel: "Alice",
          preview: "先ほどの相談",
        },
      },
    },
    connection: "connected",
  });
}

describe("MessagingScreen route-owned current place", () => {
  beforeEach(() => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    mocks.jump = { placeKey: CHANNEL_B, seq: 1 };
    seedCurrentPlace();
  });

  afterEach(() => {
    cleanup();
    mocks.placeNavigate.mockReset();
    mocks.jumpToSeq.mockReset();
    mocks.jumpToMessage.mockReset();
    vi.unstubAllGlobals();
    Reflect.deleteProperty(navigator, "serviceWorker");
    useMessaging.setState({
      init: realInit,
      selectPlace: realSelectPlace,
      loadPlaceAround: realLoadPlaceAround,
      loadThread: realLoadThread,
      loadThreads: realLoadThreads,
    });
    bindMessagingSessionIdentity(null);
  });

  it("clears the current place and its edit context on channel to home", () => {
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);
    expect(screen.getByTestId("sidebar-selection")).toHaveTextContent(
      CHANNEL_A,
    );

    view.rerender(<MessagingScreen />);

    expect(screen.getByTestId("sidebar-selection")).toHaveTextContent(
      "unselected",
    );
    expect(useMessaging.getState()).toMatchObject({
      activePlaceKey: null,
      editingMessageId: null,
    });
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_A].replyTarget?.messageId,
    ).toBe("reply-a");
  });

  it("selects an explicit second place and clears a later unknown URL", () => {
    const view = render(<MessagingScreen placeKey={CHANNEL_B} />);

    expect(useMessaging.getState().activePlaceKey).toBe(CHANNEL_B);
    expect(screen.getByTestId("sidebar-selection")).toHaveTextContent(
      CHANNEL_B,
    );

    useMessaging.setState({
      editingMessageId: "editing-b",
    });
    view.rerender(<MessagingScreen placeKey="channel:left-or-unknown" />);

    expect(useMessaging.getState()).toMatchObject({
      activePlaceKey: null,
      editingMessageId: null,
    });
    expect(screen.getByTestId("sidebar-selection")).toHaveTextContent(
      "unselected",
    );
  });

  it("loads an old search result only after its route has selected and held the place", () => {
    const loadPlaceAround = vi.fn();
    useMessaging.setState({ loadPlaceAround });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);

    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    expect(mocks.placeNavigate).toHaveBeenCalledWith(CHANNEL_B);
    expect(loadPlaceAround).not.toHaveBeenCalled();

    view.rerender(<MessagingScreen placeKey={CHANNEL_B} />);
    expect(useMessaging.getState().activePlaceKey).toBe(CHANNEL_B);
    expect(loadPlaceAround).toHaveBeenCalledWith(CHANNEL_B, 1);
  });

  it("hydrates an unknown thread hit before loading its old message", async () => {
    const target = "thread:thread-search-hit" as PlaceKey;
    mocks.jump = { placeKey: target, seq: 7 };
    const loadPlaceAround = vi.fn();
    let resolveLoad!: (loaded: boolean) => void;
    const loadThread = vi.fn(
      () =>
        new Promise<boolean>((resolve) => {
          resolveLoad = resolve;
        }),
    );
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: true,
      },
      loadPlaceAround,
      loadThread,
    });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);

    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    view.rerender(<MessagingScreen placeKey={target} />);
    expect(loadPlaceAround).not.toHaveBeenCalled();

    useMessaging.setState({
      threadsById: {
        "thread-search-hit": {
          threadId: "thread-search-hit",
          revision: 1,
          parentPlace: { kind: "channel", channelId: "channel-a" },
          parentMessageId: null,
          workspaceId: "workspace-a",
          name: "検索から開いた枝",
          messageCount: 7,
          lastMessageAt: null,
          lastMessage: "",
          participants: [],
          latestSeq: 7,
        },
      },
    });
    await act(async () => resolveLoad(true));

    await waitFor(() =>
      expect(useMessaging.getState().activePlaceKey).toBe(target),
    );
    expect(loadPlaceAround).toHaveBeenCalledWith(target, 7);
  });

  it("cancels an observed unknown-thread jump after navigating away", async () => {
    const target = "thread:thread-delayed-hit" as PlaceKey;
    mocks.jump = { placeKey: target, seq: 9 };
    const loadPlaceAround = vi.fn();
    let resolveLoad!: (loaded: boolean) => void;
    const loadThread = vi.fn(
      () =>
        new Promise<boolean>((resolve) => {
          resolveLoad = resolve;
        }),
    );
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: true,
      },
      loadPlaceAround,
      loadThread,
    });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);
    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    view.rerender(<MessagingScreen placeKey={target} />);
    expect(loadThread).toHaveBeenCalledWith("thread-delayed-hit");

    view.rerender(<MessagingScreen placeKey={CHANNEL_A} />);
    useMessaging.setState({
      threadsById: {
        "thread-delayed-hit": {
          threadId: "thread-delayed-hit",
          revision: 1,
          parentPlace: { kind: "channel", channelId: "channel-a" },
          parentMessageId: null,
          workspaceId: "workspace-a",
          name: "遅れて解決した枝",
          messageCount: 9,
          lastMessageAt: null,
          lastMessage: "",
          participants: [],
          latestSeq: 9,
        },
      },
    });
    await act(async () => resolveLoad(true));
    view.rerender(<MessagingScreen placeKey={target} />);

    await waitFor(() =>
      expect(useMessaging.getState().activePlaceKey).toBe(target),
    );
    expect(loadPlaceAround).not.toHaveBeenCalled();
  });

  it("opens a known thread route with its parent channel context", () => {
    useMessaging.setState({
      threadsById: {
        "thread-a": {
          threadId: "thread-a",
          revision: 1,
          parentPlace: { kind: "channel", channelId: "channel-a" },
          parentMessageId: "message-a",
          workspaceId: "workspace-a",
          name: "認証リダイレクト",
          messageCount: 2,
          lastMessageAt: null,
          lastMessage: "",
          participants: [SELF],
          latestSeq: 2,
        },
      },
    });

    render(<MessagingScreen placeKey="thread:thread-a" />);

    expect(screen.getByTestId("sidebar-selection")).toHaveTextContent(
      "thread:thread-a",
    );
    expect(screen.getByText("認証リダイレクト")).toBeInTheDocument();
    expect(screen.getByText("親: #alpha")).toBeInTheDocument();
    expect(useMessaging.getState().activePlaceKey).toBe("thread:thread-a");

    fireEvent.click(screen.getByTitle("親チャンネルへ戻る"));
    expect(mocks.placeNavigate).toHaveBeenCalledWith(CHANNEL_A);
  });

  it("shows direct thread loading explicitly before a not-found result", async () => {
    let resolveLoad: ((loaded: boolean) => void) | undefined;
    const loadThread = vi.fn(
      () =>
        new Promise<boolean>((resolve) => {
          resolveLoad = resolve;
        }),
    );
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: true,
      },
      loadThread,
    });

    render(<MessagingScreen placeKey="thread:thread-missing" />);

    expect(screen.getByRole("status")).toHaveTextContent(
      "スレッドを読み込み中",
    );
    useMessaging.setState({
      threadLoadErrorsById: { "thread-missing": "not_found" },
    });
    await act(async () => resolveLoad?.(false));
    expect(screen.getByText("スレッドが見つかりません")).toBeInTheDocument();
    expect(
      screen.getByText(/存在しないか、アクセスできません/),
    ).toBeInTheDocument();
  });

  it("shows a direct thread load failure and retries it into the existing thread", async () => {
    const recovered = {
      threadId: "thread-retry",
      revision: 1,
      parentPlace: { kind: "channel", channelId: "channel-a" } as const,
      parentMessageId: "message-a",
      workspaceId: "workspace-a",
      name: "回復したスレッド",
      messageCount: 0,
      lastMessageAt: null,
      lastMessage: "",
      participants: [SELF],
      latestSeq: 0,
    };
    let attempts = 0;
    const loadThread = vi.fn(async () => {
      attempts += 1;
      if (attempts === 1) {
        useMessaging.setState({
          threadLoadErrorsById: { "thread-retry": "failed" },
        });
        return false;
      }
      useMessaging.setState({
        threadsById: { "thread-retry": recovered },
        threadLoadErrorsById: {},
      });
      return true;
    });
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: true,
      },
      loadThread,
    });

    render(<MessagingScreen placeKey="thread:thread-retry" />);

    await waitFor(() =>
      expect(
        screen.getByText("スレッドを開けませんでした"),
      ).toBeInTheDocument(),
    );
    expect(loadThread).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: "再試行" }));

    await waitFor(() =>
      expect(screen.getByText("回復したスレッド")).toBeInTheDocument(),
    );
    expect(loadThread).toHaveBeenCalledTimes(2);
    expect(useMessaging.getState().activePlaceKey).toBe("thread:thread-retry");
  });

  it("hides thread controls when the backend does not support threads", () => {
    render(<MessagingScreen placeKey={CHANNEL_A} />);

    expect(screen.queryByTitle("スレッド")).not.toBeInTheDocument();
  });

  it("marks the thread toggle active and places its panel beside the conversation", async () => {
    const loadThreads = vi.fn().mockRejectedValue(new Error("offline"));
    useMessaging.setState({
      capabilities: {
        status: false,
        replyLater: false,
        reactions: false,
        notifications: false,
        threads: true,
      },
      loadThreads,
    });
    render(<MessagingScreen placeKey={CHANNEL_A} />);
    const toggle = screen.getByRole("button", { name: "スレッド" });

    expect(toggle).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(toggle).toHaveClass("bg-accent", "text-foreground");
    const close = screen.getByRole("button", {
      name: "スレッド一覧を閉じる",
    });
    const panel = close.closest("aside");
    if (!panel) throw new Error("thread panel missing");
    const members = screen.getByTestId("member-list");
    expect(
      panel.compareDocumentPosition(members) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    await waitFor(() => expect(loadThreads).toHaveBeenCalledWith(CHANNEL_A));

    fireEvent.click(close);
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(toggle).toHaveFocus();
  });

  it("hides the notification permission prompt when Web Push is unsupported", () => {
    vi.stubGlobal("Notification", { permission: "default" });
    useMessaging.setState({
      capabilities: {
        ...useMessaging.getState().capabilities,
        notifications: true,
      },
    });

    render(<MessagingScreen />);

    expect(
      screen.queryByRole("button", { name: "許可する" }),
    ).not.toBeInTheDocument();
  });

  it("shows the notification permission prompt when Web Push is supported", () => {
    vi.stubGlobal("Notification", { permission: "default" });
    vi.stubGlobal("PushManager", class FakePushManager {});
    Object.defineProperty(navigator, "serviceWorker", {
      configurable: true,
      value: {},
    });
    useMessaging.setState({
      capabilities: {
        ...useMessaging.getState().capabilities,
        notifications: true,
      },
    });

    render(<MessagingScreen />);

    expect(
      screen.getByRole("button", { name: "許可する" }),
    ).toBeInTheDocument();
  });
});

describe("MessagingScreen jump outcome", () => {
  const TARGET_MESSAGE: Message = {
    messageId: "jump-target-9",
    place: { kind: "channel", channelId: "channel-b" },
    seq: 9,
    author: SELF,
    content: "対象メッセージ",
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

  beforeEach(() => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    mocks.jump = { placeKey: CHANNEL_B, seq: 9 };
    seedCurrentPlace();
  });

  afterEach(() => {
    cleanup();
    mocks.placeNavigate.mockReset();
    mocks.jumpToSeq.mockReset();
    mocks.jumpToMessage.mockReset();
    useMessaging.setState({
      init: realInit,
      selectPlace: realSelectPlace,
      loadPlaceAround: realLoadPlaceAround,
      loadThread: realLoadThread,
      loadThreads: realLoadThreads,
    });
    bindMessagingSessionIdentity(null);
  });

  it("reports a failed jump in context and retries to the same target", async () => {
    const loadPlaceAround = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValue("found" as const);
    useMessaging.setState({
      loadPlaceAround,
      messagesByPlace: { [CHANNEL_B]: [{ ...TARGET_MESSAGE }] },
    });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);

    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    view.rerender(<MessagingScreen placeKey={CHANNEL_B} />);

    // 旧実装はrejectionを握り潰してpendingJumpを解決しなかった。
    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "そのメッセージへ移動できませんでした",
      ),
    );

    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    await waitFor(() =>
      expect(loadPlaceAround).toHaveBeenCalledWith(CHANNEL_B, 9),
    );
    await waitFor(() => expect(mocks.jumpToSeq).toHaveBeenCalledWith(9));
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("reports a genuinely missing target without offering a retry", async () => {
    const loadPlaceAround = vi.fn().mockResolvedValue("missing" as const);
    useMessaging.setState({ loadPlaceAround });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);

    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    view.rerender(<MessagingScreen placeKey={CHANNEL_B} />);

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        "そのメッセージは存在しないか、アクセスできません",
      ),
    );
    expect(
      screen.queryByRole("button", { name: "再試行" }),
    ).not.toBeInTheDocument();
  });

  it("ignores a stale failure after a newer jump replaced it", async () => {
    let rejectFirst!: (reason: unknown) => void;
    const loadPlaceAround = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<"found">((_, no) => {
            rejectFirst = no;
          }),
      )
      .mockResolvedValue("found" as const);
    useMessaging.setState({
      loadPlaceAround,
      messagesByPlace: { [CHANNEL_B]: [{ ...TARGET_MESSAGE }] },
    });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);

    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    view.rerender(<MessagingScreen placeKey={CHANNEL_B} />);
    await waitFor(() =>
      expect(loadPlaceAround).toHaveBeenCalledWith(CHANNEL_B, 9),
    );

    // 同じ場所へ別のjumpを要求してから、古いfetchを失敗させる。
    mocks.jump = { placeKey: CHANNEL_B, seq: 5 };
    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    await waitFor(() =>
      expect(loadPlaceAround).toHaveBeenCalledWith(CHANNEL_B, 5),
    );
    await act(async () => rejectFirst(new Error("late failure")));

    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    // 新しいjumpはseq5を待ち続ける。後続で到着すれば普通にjumpする。
    act(() => {
      useMessaging.setState({
        messagesByPlace: {
          [CHANNEL_B]: [
            { ...TARGET_MESSAGE },
            { ...TARGET_MESSAGE, messageId: "jump-target-5", seq: 5 },
          ],
        },
      });
    });
    await waitFor(() => expect(mocks.jumpToSeq).toHaveBeenCalledWith(5));
  });

  it("drops the outcome when the transport generation replaced the jump", async () => {
    let rejectFirst!: (reason: unknown) => void;
    const loadPlaceAround = vi.fn().mockImplementationOnce(
      () =>
        new Promise<"found">((_, no) => {
          rejectFirst = no;
        }),
    );
    useMessaging.setState({ loadPlaceAround });
    const view = render(<MessagingScreen placeKey={CHANNEL_A} />);

    fireEvent.click(screen.getByRole("button", { name: "jump to old result" }));
    view.rerender(<MessagingScreen placeKey={CHANNEL_B} />);
    await waitFor(() =>
      expect(loadPlaceAround).toHaveBeenCalledWith(CHANNEL_B, 9),
    );

    const generation = useMessaging.getState().transportGeneration;
    act(() => {
      useMessaging.setState({ transportGeneration: generation + 1 });
    });
    await act(async () => rejectFirst(new Error("stale transport")));

    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
