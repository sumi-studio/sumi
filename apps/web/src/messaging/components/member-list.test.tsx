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
import { MockMessagingServer } from "../mock-server";
import type { DmSummary, PlaceKey } from "../model";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../store";
import { MemberList } from "./member-list";
import { Sidebar } from "./sidebar";

const navigation = vi.hoisted(() => ({ navigate: vi.fn() }));

vi.mock("../place-route", () => ({
  usePlaceNavigate: () => navigation.navigate,
}));

const SELF = { kind: "human", humanId: "human-a" } as const;
const BOB = { kind: "human", humanId: "human-b" } as const;
const CAROL = { kind: "human", humanId: "human-c" } as const;
const realStartDM = useMessaging.getState().startDM;

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function setMembers() {
  useMessaging.setState({
    ready: true,
    self: SELF,
    selfKey: "human:human-a",
    membersByKey: {
      "human:human-a": {
        participant: SELF,
        displayName: "Alice",
        tagline: "",
      },
      "human:human-b": {
        participant: BOB,
        displayName: "Bob",
        tagline: "Designer",
      },
      "human:human-c": {
        participant: CAROL,
        displayName: "Carol",
        tagline: "Engineer",
      },
    },
    statusByKey: {},
    dms: [],
  });
}

function resolveThenSwitchIdentity(): Promise<PlaceKey> {
  return Promise.resolve<PlaceKey>("dm:dm-bob").then((place) => {
    queueMicrotask(() => {
      bindMessagingSessionIdentity(null);
      bindMessagingSessionIdentity("human-b");
    });
    return place;
  });
}

describe("MemberList DM action", () => {
  beforeEach(() => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    navigation.navigate.mockReset();
    useMessaging.setState({ startDM: realStartDM });
    setMembers();
  });

  afterEach(() => {
    cleanup();
    bindMessagingSessionIdentity(null);
    vi.restoreAllMocks();
  });

  it("starts a DM from a non-self member and navigates to its place", async () => {
    const startDM = vi.fn(async (): Promise<PlaceKey> => "dm:dm-bob");
    useMessaging.setState({ startDM });
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: "BobにDMを送る" }));

    expect(startDM).toHaveBeenCalledWith([BOB]);
    await waitFor(() =>
      expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-bob"),
    );
  });

  it("exposes one pending action and blocks concurrent member actions", async () => {
    const pending = deferred<PlaceKey>();
    const startDM = vi.fn(() => pending.promise);
    useMessaging.setState({ startDM });
    render(<MemberList />);

    const bob = screen.getByRole("button", { name: "BobにDMを送る" });
    const carol = screen.getByRole("button", { name: "CarolにDMを送る" });
    fireEvent.click(bob);

    expect(bob).toHaveAttribute("aria-busy", "true");
    expect(bob).toBeDisabled();
    expect(carol).toBeDisabled();
    expect(screen.getByText("DMを開始しています…")).toBeVisible();
    fireEvent.click(carol);
    expect(startDM).toHaveBeenCalledTimes(1);

    await act(async () => pending.resolve("dm:dm-bob"));
    await waitFor(() =>
      expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-bob"),
    );
    expect(bob).toHaveAttribute("aria-busy", "false");
  });

  it("announces a failure and retries the same action", async () => {
    const startDM = vi
      .fn<() => Promise<PlaceKey>>()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce("dm:dm-bob");
    useMessaging.setState({ startDM });
    render(<MemberList />);

    const bob = screen.getByRole("button", { name: "BobにDMを送る" });
    fireEvent.click(bob);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveAttribute("aria-live", "assertive");
    expect(bob).not.toContainElement(alert);
    expect(alert).toHaveTextContent(
      "DMを開始できませんでした。もう一度押してください",
    );
    expect(bob).toBeEnabled();

    fireEvent.click(bob);
    await waitFor(() =>
      expect(navigation.navigate).toHaveBeenCalledWith("dm:dm-bob"),
    );
    expect(startDM).toHaveBeenCalledTimes(2);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("rechecks identity after store completion before member-row navigation", async () => {
    const startDM = vi.fn(resolveThenSwitchIdentity);
    useMessaging.setState({ startDM });
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: "BobにDMを送る" }));

    await waitFor(() => expect(getMessagingSessionIdentity()).toBe("human-b"));
    expect(startDM).toHaveBeenCalledTimes(1);
    expect(navigation.navigate).not.toHaveBeenCalled();
  });

  it("rechecks identity after store completion before dialog navigation", async () => {
    const startDM = vi.fn(resolveThenSwitchIdentity);
    useMessaging.setState({ startDM });
    render(<Sidebar />);

    fireEvent.click(screen.getByTitle("ダイレクトメッセージを開始"));
    const dialog = screen.getByRole("dialog", {
      name: "ダイレクトメッセージを開始",
    });
    fireEvent.click(within(dialog).getByText("Bob", { exact: true }));
    fireEvent.click(within(dialog).getByRole("button", { name: "DMを開始" }));

    await waitFor(() => expect(getMessagingSessionIdentity()).toBe("human-b"));
    expect(startDM).toHaveBeenCalledTimes(1);
    expect(navigation.navigate).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("does not navigate when a deferred ensureDM crosses an identity switch", async () => {
    const pending = deferred<DmSummary>();
    const server = new MockMessagingServer();
    vi.spyOn(server, "ensureDM").mockReturnValue(pending.promise);
    installMessagingBackend(server);
    setMembers();
    render(<MemberList />);

    fireEvent.click(screen.getByRole("button", { name: "BobにDMを送る" }));
    expect(server.ensureDM).toHaveBeenCalledWith(BOB);

    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");
    await act(async () => {
      pending.resolve({
        dmId: "stale-dm",
        kind: "dm",
        participants: [SELF, BOB],
      });
      await pending.promise;
      await Promise.resolve();
    });

    expect(navigation.navigate).not.toHaveBeenCalled();
    expect(useMessaging.getState().dms).toEqual([]);
  });
});
