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
const realUpdateChannel = useMessaging.getState().updateChannel;
const realDuplicateChannel = useMessaging.getState().duplicateChannel;

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

  it("uses the route-selected Workspace, then retains its exact scope on home", async () => {
    const createChannel = vi.fn(
      async (): Promise<PlaceKey> => "channel:created-for-b",
    );
    setTwoWorkspaceState(createChannel);

    const view = render(
      <Sidebar
        selectedPlaceKey="channel:channel-b"
        workspaceId="workspace-b"
      />,
    );

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
      expect(createChannel).toHaveBeenCalledWith(
        "workspace-b",
        "new-b",
        "",
        false,
      ),
    );
    expect(navigation.navigate).toHaveBeenCalledWith("channel:created-for-b");

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    expect(
      screen.getByRole("dialog", { name: "チャンネルを作成" }),
    ).toBeVisible();
    view.rerender(
      <Sidebar selectedPlaceKey={null} workspaceId="workspace-b" />,
    );

    expect(screen.getByText("Workspace B")).toBeVisible();
    expect(screen.queryByRole("button", { current: "page" })).toBeNull();
    expect(screen.getByTitle("チャンネルを作成")).toBeVisible();
    expect(
      screen.getByRole("dialog", { name: "チャンネルを作成" }),
    ).toBeVisible();
  });

  it("creates the first channel from the exact bound Workspace", async () => {
    const createChannel = vi.fn(
      async (): Promise<PlaceKey> => "channel:first-b",
    );
    setTwoWorkspaceState(createChannel);
    useMessaging.setState({ channels: [], activePlaceKey: null });

    render(<Sidebar selectedPlaceKey={null} workspaceId="workspace-b" />);

    expect(screen.getByText("Workspace B")).toBeVisible();
    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    fireEvent.change(within(dialog).getByRole("textbox", { name: "名前" }), {
      target: { value: "first-b" },
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "作成" }));

    await waitFor(() =>
      expect(createChannel).toHaveBeenCalledWith(
        "workspace-b",
        "first-b",
        "",
        false,
      ),
    );
    expect(navigation.navigate).toHaveBeenCalledWith("channel:first-b");
  });

  it("rechecks identity after store completion before channel navigation", async () => {
    const createChannel = vi.fn(resolveThenSwitchIdentity);
    setTwoWorkspaceState(createChannel);
    render(
      <Sidebar
        selectedPlaceKey="channel:channel-b"
        workspaceId="workspace-b"
      />,
    );

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    fireEvent.change(within(dialog).getByRole("textbox", { name: "名前" }), {
      target: { value: "private-b" },
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "作成" }));

    await waitFor(() => expect(getMessagingSessionIdentity()).toBe("human-b"));
    expect(createChannel).toHaveBeenCalledWith(
      "workspace-b",
      "private-b",
      "",
      false,
    );
    expect(navigation.navigate).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("does not navigate a deferred B creation after the route selects A", async () => {
    const pending = deferred<PlaceKey>();
    const createChannel = vi.fn(() => pending.promise);
    setTwoWorkspaceState(createChannel);
    const view = render(
      <Sidebar
        selectedPlaceKey="channel:channel-b"
        workspaceId="workspace-b"
      />,
    );

    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    fireEvent.change(within(dialog).getByRole("textbox", { name: "名前" }), {
      target: { value: "late-b" },
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "作成" }));
    expect(createChannel).toHaveBeenCalledWith(
      "workspace-b",
      "late-b",
      "",
      false,
    );

    view.rerender(
      <Sidebar
        selectedPlaceKey="channel:channel-a"
        workspaceId="workspace-a"
      />,
    );
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
          baseStatus: null,
          baseNote: "",
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

  it("shows the selected place notification level inside the place menu and closes after selection", () => {
    render(
      <Sidebar
        selectedPlaceKey="channel:channel-a"
        workspaceId="workspace-a"
      />,
    );

    fireEvent.click(
      screen.getAllByRole("button", { name: "この場所のメニュー" })[0],
    );
    fireEvent.click(screen.getByRole("menuitem", { name: /通知設定/ }));
    expect(
      screen.getByRole("menuitemradio", { name: /すべて通知/ }),
    ).toBeChecked();
    fireEvent.click(screen.getByRole("menuitemradio", { name: /ミュート/ }));

    expect(setPlaceNotificationLevel).toHaveBeenCalledWith(
      "channel:channel-a",
      "mute",
    );
    expect(
      screen.queryByRole("menuitemradio", { name: /ミュート/ }),
    ).toBeNull();
  });

  it("closes the status menu on an outside pointerdown", () => {
    render(
      <Sidebar
        selectedPlaceKey="channel:channel-a"
        workspaceId="workspace-a"
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Alice 対応可能/ }));
    expect(screen.getByRole("button", { name: /取り込み中/ })).toBeVisible();

    fireEvent.pointerDown(screen.getByRole("navigation"));

    expect(screen.queryByRole("button", { name: /取り込み中/ })).toBeNull();
  });

  it("does not implicitly submit channel creation on an IME Enter", () => {
    render(
      <Sidebar
        selectedPlaceKey="channel:channel-a"
        workspaceId="workspace-a"
      />,
    );
    fireEvent.click(screen.getByTitle("チャンネルを作成"));
    const dialog = screen.getByRole("dialog", { name: "チャンネルを作成" });
    const name = within(dialog).getByRole("textbox", { name: "名前" });
    fireEvent.change(name, { target: { value: "設計" } });

    fireEvent.keyDown(name, { key: "Enter", isComposing: true });
    fireEvent.keyDown(name, { key: "Enter", keyCode: 229 });

    expect(createChannel).not.toHaveBeenCalled();
  });
});

describe("place menu channel actions", () => {
  const updateChannel = vi.fn(async () => undefined);
  const duplicateChannel = vi.fn(
    async (): Promise<PlaceKey> => "channel:alpha-copy",
  );
  const createChannel = vi.fn(async (): Promise<PlaceKey> => "channel:created");

  beforeEach(() => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    navigation.navigate.mockReset();
    setTwoWorkspaceState(createChannel);
    useMessaging.setState({
      capabilities: {
        status: true,
        replyLater: true,
        reactions: true,
        notifications: true,
      },
      updateChannel,
      duplicateChannel,
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
    useMessaging.setState({
      createChannel: realCreateChannel,
      updateChannel: realUpdateChannel,
      duplicateChannel: realDuplicateChannel,
    });
    bindMessagingSessionIdentity(null);
  });

  function openAlphaMenu() {
    render(
      <Sidebar
        selectedPlaceKey="channel:channel-a"
        workspaceId="workspace-a"
      />,
    );
    fireEvent.click(
      screen.getAllByRole("button", { name: "この場所のメニュー" })[0],
    );
  }

  it("refuses to save an edit that names no change, then sends only what changed", async () => {
    openAlphaMenu();
    fireEvent.click(screen.getByRole("menuitem", { name: "チャンネルを編集" }));

    const dialog = screen.getByRole("dialog", { name: "チャンネルを編集" });
    expect(within(dialog).getByRole("textbox", { name: "名前" })).toHaveValue(
      "alpha",
    );
    expect(within(dialog).getByRole("button", { name: "保存" })).toBeDisabled();

    fireEvent.change(
      within(dialog).getByRole("textbox", { name: "トピック" }),
      { target: { value: "設計の話" } },
    );
    fireEvent.click(within(dialog).getByRole("button", { name: "保存" }));

    // 名前は触っていないので送らない——空文字で上書きしてトピックを消さない。
    await waitFor(() =>
      expect(updateChannel).toHaveBeenCalledWith("channel-a", {
        topic: "設計の話",
      }),
    );
  });

  it("keeps the opening snapshot when a place update arrives before a topic-only save", async () => {
    openAlphaMenu();
    fireEvent.click(screen.getByRole("menuitem", { name: "チャンネルを編集" }));

    // 自分の編集とは別の更新が、ダイアログを開いた後に届く。
    useMessaging.setState((state) => ({
      channels: state.channels.map((channel) =>
        channel.channelId === "channel-a"
          ? { ...channel, name: "別の名前", topic: "別の話題" }
          : channel,
      ),
    }));

    const dialog = screen.getByRole("dialog", { name: "チャンネルを編集" });
    fireEvent.change(
      within(dialog).getByRole("textbox", { name: "トピック" }),
      {
        target: { value: "自分の話題" },
      },
    );
    fireEvent.click(within(dialog).getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(updateChannel).toHaveBeenCalledWith("channel-a", {
        topic: "自分の話題",
      }),
    );
  });

  it("duplicates without asking for a name and moves to the copy", async () => {
    openAlphaMenu();
    fireEvent.click(screen.getByRole("menuitem", { name: "複製" }));

    await waitFor(() =>
      expect(duplicateChannel).toHaveBeenCalledWith("channel-a"),
    );
    expect(navigation.navigate).toHaveBeenCalledWith("channel:alpha-copy");
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("offers no channel-only actions on a direct message", () => {
    useMessaging.setState({
      dms: [
        {
          kind: "dm",
          dmId: "dm-1",
          participants: [SELF, { kind: "human", humanId: "human-b" }],
        },
      ],
      membersByKey: {
        ...useMessaging.getState().membersByKey,
        "human:human-b": {
          participant: { kind: "human", humanId: "human-b" },
          displayName: "Bob",
          tagline: "",
        },
      },
    });
    render(
      <Sidebar
        selectedPlaceKey="channel:channel-a"
        workspaceId="workspace-a"
      />,
    );

    const menus = screen.getAllByRole("button", { name: "この場所のメニュー" });
    fireEvent.click(menus[menus.length - 1]);

    expect(
      screen.queryByRole("menuitem", { name: "チャンネルを編集" }),
    ).toBeNull();
    expect(screen.queryByRole("menuitem", { name: "複製" })).toBeNull();
    expect(screen.getByRole("menuitem", { name: /通知設定/ })).toBeVisible();
  });
});
