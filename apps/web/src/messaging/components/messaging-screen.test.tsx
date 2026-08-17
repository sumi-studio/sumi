// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { PlaceKey } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { MessagingScreen } from "./messaging-screen";

vi.mock("../../shell/app-rail", () => ({
  AppRail: () => <div data-testid="app-rail" />,
}));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => vi.fn(),
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
vi.mock("./member-list", () => ({ MemberList: () => null }));
vi.mock("./message-list", () => ({ MessageList: () => null }));
vi.mock("./composer", () => ({ Composer: () => null }));

const SELF = { kind: "human", humanId: "human-a" } as const;
const CHANNEL_A: PlaceKey = "channel:channel-a";
const CHANNEL_B: PlaceKey = "channel:channel-b";
const realInit = useMessaging.getState().init;
const realSelectPlace = useMessaging.getState().selectPlace;

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
    useMessaging.setState({ init: realInit, selectPlace: realSelectPlace });
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
  });

  it("hides thread controls when the backend does not support threads", () => {
    render(<MessagingScreen placeKey={CHANNEL_A} />);

    expect(screen.queryByTitle("スレッド")).not.toBeInTheDocument();
  });
});
