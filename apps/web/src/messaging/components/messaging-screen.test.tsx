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
import type { PlaceKey } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { MessagingScreen } from "./messaging-screen";

const mocks = vi.hoisted(() => ({ placeNavigate: vi.fn() }));

vi.mock("../../shell/app-rail", () => ({
  AppRail: () => <div data-testid="app-rail" />,
}));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => mocks.placeNavigate,
}));

vi.mock("./message-search", () => ({
  MessageSearch: ({ onJump }: { onJump: (jump: unknown) => void }) => (
    <button
      type="button"
      onClick={() => onJump({ placeKey: CHANNEL_B, seq: 1 })}
    >
      jump to old result
    </button>
  ),
}));

vi.mock("./sidebar", () => ({
  NOTIFICATION_LEVEL_LABEL: {
    all: "すべて通知",
    mentions: "メンションのみ",
    mute: "ミュート",
  },
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
vi.mock("./thread-panel", () => ({
  ThreadPanel: () => <aside data-testid="thread-panel" />,
}));
vi.mock("./message-list", () => ({ MessageList: () => null }));
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
        replyTargetId: null,
      }),
    ready: true,
    capabilities: {
      status: false,
      replyLater: false,
      reactions: false,
      notifications: false,
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
        name: "alpha",
        topic: "",
        visibility: "public",
        voice: false,
      },
      {
        channelId: "channel-b",
        workspaceId: "workspace-b",
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
    replyTargetId: "reply-a",
    connection: "connected",
  });
}

describe("MessagingScreen route-owned current place", () => {
  beforeEach(() => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    seedCurrentPlace();
  });

  afterEach(() => {
    cleanup();
    mocks.placeNavigate.mockReset();
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
      replyTargetId: null,
    });
  });

  it("selects an explicit second place and clears a later unknown URL", () => {
    const view = render(<MessagingScreen placeKey={CHANNEL_B} />);

    expect(useMessaging.getState().activePlaceKey).toBe(CHANNEL_B);
    expect(screen.getByTestId("sidebar-selection")).toHaveTextContent(
      CHANNEL_B,
    );

    useMessaging.setState({
      editingMessageId: "editing-b",
      replyTargetId: "reply-b",
    });
    view.rerender(<MessagingScreen placeKey="channel:left-or-unknown" />);

    expect(useMessaging.getState()).toMatchObject({
      activePlaceKey: null,
      editingMessageId: null,
      replyTargetId: null,
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

  it("opens a known thread route with its parent channel context", () => {
    useMessaging.setState({
      threadsById: {
        "thread-a": {
          threadId: "thread-a",
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
    const panel = screen.getByTestId("thread-panel");
    const members = screen.getByTestId("member-list");
    expect(
      panel.compareDocumentPosition(members) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
    await waitFor(() => expect(loadThreads).toHaveBeenCalledWith(CHANNEL_A));
  });
});
