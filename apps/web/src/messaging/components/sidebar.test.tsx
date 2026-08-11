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
import type { PlaceKey } from "../model";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  useMessaging,
} from "../store";
import { Badge, Sidebar } from "./sidebar";

const navigation = vi.hoisted(() => ({ navigate: vi.fn() }));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => navigation.navigate,
}));

const SELF = { kind: "human", humanId: "human-a" } as const;
const realCreateChannel = useMessaging.getState().createChannel;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function setTwoWorkspaceState(
  createChannel: (
    workspaceId: string,
    name: string,
    topic: string,
  ) => Promise<PlaceKey>,
) {
  useMessaging.setState({
    ready: true,
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
      },
      {
        channelId: "channel-b",
        workspaceId: "workspace-b",
        name: "beta",
        topic: "",
        visibility: "public",
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
    statusByKey: {},
    activePlaceKey: "channel:channel-a",
    createChannel,
  });
}

function resolveThenSwitchIdentity(): Promise<PlaceKey> {
  return Promise.resolve<PlaceKey>("channel:created-for-b").then((place) => {
    queueMicrotask(() => {
      bindMessagingSessionIdentity(null);
      bindMessagingSessionIdentity("human-b");
    });
    return place;
  });
}

describe("sidebar unread badge", () => {
  afterEach(cleanup);

  it("renders a visible unread count for an unmuted place", () => {
    render(<Badge count={7} urgent={false} muted={false} />);
    expect(screen.getByText("7")).toBeTruthy();
  });

  it("renders nothing when there is no count", () => {
    const { container } = render(
      <Badge count={0} urgent={false} muted={false} />,
    );
    expect(container.textContent).toBe("");
  });

  it("suppresses the count for a muted place", () => {
    const { container } = render(
      <Badge count={7} urgent={true} muted={true} />,
    );
    expect(container.textContent).toBe("");
  });
});

describe("Sidebar route authority", () => {
  beforeEach(() => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    navigation.navigate.mockReset();
  });

  afterEach(() => {
    cleanup();
    useMessaging.setState({ createChannel: realCreateChannel });
    bindMessagingSessionIdentity(null);
    vi.restoreAllMocks();
  });

  it("uses the route-selected Workspace, then targets nothing on home", async () => {
    const createChannel = vi.fn(
      async (): Promise<PlaceKey> => "channel:created-for-b",
    );
    setTwoWorkspaceState(createChannel);

    const view = render(<Sidebar selectedPlaceKey="channel:channel-b" />);

    expect(screen.getByText("Workspace B")).toBeVisible();
    expect(screen.getByRole("button", { name: "beta" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(screen.getByRole("button", { name: "alpha" })).not.toHaveAttribute(
      "aria-current",
    );

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    fireEvent.change(within(dialog).getByRole("textbox", { name: "名前" }), {
      target: { value: "new-b" },
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "作成" }));

    await waitFor(() =>
      expect(createChannel).toHaveBeenCalledWith("workspace-b", "new-b", ""),
    );
    expect(navigation.navigate).toHaveBeenCalledWith("channel:created-for-b");

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    expect(
      screen.getByRole("dialog", { name: "チャンネルを作成" }),
    ).toBeVisible();
    view.rerender(<Sidebar selectedPlaceKey={null} />);

    expect(screen.getByText("場所を選択")).toBeVisible();
    expect(screen.queryByRole("button", { current: "page" })).toBeNull();
    expect(screen.queryByTitle("チャンネルを作成")).toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("rechecks identity after store completion before channel navigation", async () => {
    const createChannel = vi.fn(resolveThenSwitchIdentity);
    setTwoWorkspaceState(createChannel);
    render(<Sidebar selectedPlaceKey="channel:channel-b" />);

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    fireEvent.change(within(dialog).getByRole("textbox", { name: "名前" }), {
      target: { value: "private-b" },
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "作成" }));

    await waitFor(() => expect(getMessagingSessionIdentity()).toBe("human-b"));
    expect(createChannel).toHaveBeenCalledWith("workspace-b", "private-b", "");
    expect(navigation.navigate).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("does not navigate a deferred B creation after the route selects A", async () => {
    const pending = deferred<PlaceKey>();
    const createChannel = vi.fn(() => pending.promise);
    setTwoWorkspaceState(createChannel);
    const view = render(<Sidebar selectedPlaceKey="channel:channel-b" />);

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    fireEvent.change(within(dialog).getByRole("textbox", { name: "名前" }), {
      target: { value: "late-b" },
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "作成" }));
    expect(createChannel).toHaveBeenCalledWith("workspace-b", "late-b", "");

    view.rerender(<Sidebar selectedPlaceKey="channel:channel-a" />);
    expect(screen.getByText("Workspace A")).toBeVisible();
    expect(screen.queryByRole("dialog")).toBeNull();

    await act(async () => {
      pending.resolve("channel:late-b");
      await pending.promise;
      await Promise.resolve();
    });

    expect(navigation.navigate).not.toHaveBeenCalled();
    expect(screen.getByText("Workspace A")).toBeVisible();
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});

describe("Sidebar overlay and IME behavior", () => {
  const setPlaceNotificationLevel = vi.fn();
  const setStatus = vi.fn();
  const createChannel = vi.fn(async (): Promise<PlaceKey> => "channel:created");

  beforeEach(() => {
    navigation.navigate.mockReset();
    setTwoWorkspaceState(createChannel);
    useMessaging.setState({
      statusByKey: {
        "human:human-a": {
          participant: SELF,
          status: "available",
          note: "",
          expiresAt: null,
        },
      },
      unreadCountByPlace: {},
      mentionCountByPlace: {},
      notificationLevelByPlace: {},
      notificationDefaultLevel: "all",
      capabilities: {
        status: true,
        replyLater: true,
        reactions: true,
        notifications: true,
      },
      setPlaceNotificationLevel,
      setStatus,
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    useMessaging.setState({ createChannel: realCreateChannel });
  });

  it("shows the selected place notification level as a radio and closes after selection", () => {
    render(<Sidebar selectedPlaceKey="channel:channel-a" />);

    fireEvent.click(screen.getAllByRole("button", { name: "通知設定" })[0]);
    expect(screen.getByRole("radio", { name: /すべて通知/ })).toBeChecked();
    fireEvent.click(screen.getByRole("radio", { name: /ミュート/ }));

    expect(setPlaceNotificationLevel).toHaveBeenCalledWith(
      "channel:channel-a",
      "mute",
    );
    expect(screen.queryByRole("radio", { name: /ミュート/ })).toBeNull();
  });

  it("closes the status menu on an outside pointerdown", () => {
    render(<Sidebar selectedPlaceKey="channel:channel-a" />);
    fireEvent.click(screen.getByRole("button", { name: /Alice/ }));
    expect(screen.getByRole("radio", { name: "取り込み中" })).toBeVisible();

    fireEvent.pointerDown(screen.getByRole("navigation"));

    expect(screen.queryByRole("radio", { name: "取り込み中" })).toBeNull();
  });

  it("does not implicitly submit channel creation on an IME Enter", () => {
    render(<Sidebar selectedPlaceKey="channel:channel-a" />);
    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    const name = within(dialog).getByRole("textbox", { name: "名前" });
    fireEvent.change(name, { target: { value: "設計" } });

    fireEvent.keyDown(name, { key: "Enter", isComposing: true });
    fireEvent.keyDown(name, { key: "Enter", keyCode: 229 });

    expect(createChannel).not.toHaveBeenCalled();
  });
});
