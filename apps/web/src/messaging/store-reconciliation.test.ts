// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend } from "./api-backend";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const CHANNEL_KEY = "channel:channel-1";

describe("messaging catch-up reconciliation", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("refreshes a loaded old poll when its offline vote is outside message replay", async () => {
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") return json(bootstrapWire());
      if (path.startsWith("/messaging/places/channel-1/messages")) {
        historyReads += 1;
        return json({
          messages: [pollMessageWire(historyReads === 1 ? [] : ["human-2"])],
        });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("poll-reconciliation");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));

    useMessaging.getState().selectPlace(CHANNEL_KEY);
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.poll
          ?.options[0]?.voters,
      ).toEqual([]),
    );
    const socket = FakeWebSocket.instance;
    socket?.open();
    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });

    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.poll
          ?.options[0]?.voters,
      ).toEqual([{ kind: "human", humanId: "human-2" }]),
    );
    expect(historyReads).toBe(3);
    const historyPaths = fetchMock.mock.calls.map(([input]) => String(input));
    expect(
      historyPaths.filter(
        (path) =>
          path === "/messaging/places/channel-1/messages?before_seq=3&limit=1",
      ),
    ).toHaveLength(2);
  });

  it("applies a poll frame without reverting a later edit or reviving a tombstone", async () => {
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") return json(bootstrapWire());
      if (path.startsWith("/messaging/places/channel-1/messages")) {
        historyReads += 1;
        return json({ messages: [pollMessageWire([])] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("poll-field-merge");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace(CHANNEL_KEY);
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.poll,
      ).toBeTruthy(),
    );
    const socket = FakeWebSocket.instance;
    socket?.open();
    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });
    // Poll and reaction projections have independent durable snapshots. Both
    // must reconcile the loaded message after catch-up.
    await vi.waitFor(() => expect(historyReads).toBe(3));

    socket?.message({
      type: "event",
      event: {
        type: "message_edited",
        place_id: "channel-1",
        message: { ...pollMessageWire([]), content: "edited" },
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "poll_updated",
        place_id: "channel-1",
        message: pollMessageWire(["human-2"]),
      },
    });
    expect(
      useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0],
    ).toMatchObject({
      content: "edited",
      deleted: false,
      poll: {
        options: [
          { voters: [{ kind: "human", humanId: "human-2" }] },
          { voters: [] },
        ],
      },
    });

    socket?.message({
      type: "event",
      event: {
        type: "message_deleted",
        place_id: "channel-1",
        message: {
          ...pollMessageWire([]),
          content: "",
          poll: null,
          deleted: true,
        },
      },
    });
    socket?.message({
      type: "event",
      event: {
        type: "poll_updated",
        place_id: "channel-1",
        message: pollMessageWire(["human-3"]),
      },
    });
    expect(
      useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0],
    ).toMatchObject({ content: "", deleted: true, poll: null });
  });

  it("keeps a live tombstone terminal when an older poll snapshot returns", async () => {
    let releaseSnapshot: () => void = () => {};
    let noteSnapshotStarted: () => void = () => {};
    const snapshotGate = new Promise<void>((resolve) => {
      releaseSnapshot = resolve;
    });
    const snapshotStarted = new Promise<void>((resolve) => {
      noteSnapshotStarted = resolve;
    });
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") return json(bootstrapWire());
      if (path.startsWith("/messaging/places/channel-1/messages")) {
        historyReads += 1;
        if (historyReads === 2) {
          noteSnapshotStarted();
          await snapshotGate;
        }
        return json({ messages: [pollMessageWire(["human-2"])] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("poll-delete-race");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace(CHANNEL_KEY);
    await vi.waitFor(() =>
      expect(useMessaging.getState().messagesByPlace[CHANNEL_KEY]).toHaveLength(
        1,
      ),
    );
    const socket = FakeWebSocket.instance;
    socket?.open();
    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });
    await snapshotStarted;
    socket?.message({
      type: "event",
      event: {
        type: "message_deleted",
        place_id: "channel-1",
        message: {
          ...pollMessageWire([]),
          content: "",
          poll: null,
          deleted: true,
        },
      },
    });
    releaseSnapshot();
    await nextTask();

    expect(
      useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0],
    ).toMatchObject({ content: "", deleted: true, poll: null });
  });

  it("adopts an authoritative tombstone found during poll reconciliation", async () => {
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") return json(bootstrapWire());
      if (path.startsWith("/messaging/places/channel-1/messages")) {
        historyReads += 1;
        const message = pollMessageWire([]);
        return json({
          messages: [
            historyReads === 1
              ? message
              : { ...message, content: "", poll: null, deleted: true },
          ],
        });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("poll-offline-delete");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace(CHANNEL_KEY);
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.deleted,
      ).toBe(false),
    );
    const socket = FakeWebSocket.instance;
    socket?.open();
    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });

    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0],
      ).toMatchObject({ content: "", deleted: true, poll: null }),
    );
  });

  it("keeps a live poll update newer than an in-flight initial history load", async () => {
    let releaseHistory: () => void = () => {};
    let noteHistoryStarted: () => void = () => {};
    const historyGate = new Promise<void>((resolve) => {
      releaseHistory = resolve;
    });
    const historyStarted = new Promise<void>((resolve) => {
      noteHistoryStarted = resolve;
    });
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") return json(bootstrapWire());
      if (path.startsWith("/messaging/places/channel-1/messages")) {
        historyReads += 1;
        if (historyReads === 1) {
          noteHistoryStarted();
          await historyGate;
          return json({ messages: [pollMessageWire([])] });
        }
        return json({ messages: [pollMessageWire(["human-2"])] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("poll-initial-load-race");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace(CHANNEL_KEY);
    await historyStarted;

    const socket = FakeWebSocket.instance;
    socket?.open();
    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });
    socket?.message({
      type: "event",
      event: {
        type: "poll_updated",
        place_id: "channel-1",
        message: pollMessageWire(["human-3"]),
      },
    });
    releaseHistory();

    // Initial history, poll reconciliation, and reaction reconciliation all
    // complete after the held load. The live poll frame must still win.
    await vi.waitFor(() => expect(historyReads).toBe(3));
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.poll
          ?.options[0]?.voters,
      ).toEqual([{ kind: "human", humanId: "human-3" }]),
    );
  });

  it("discards an older overlapping poll reconciliation", async () => {
    let releaseOlderSnapshot: () => void = () => {};
    let noteOlderSnapshotStarted: () => void = () => {};
    const olderSnapshotGate = new Promise<void>((resolve) => {
      releaseOlderSnapshot = resolve;
    });
    const olderSnapshotStarted = new Promise<void>((resolve) => {
      noteOlderSnapshotStarted = resolve;
    });
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") return json(bootstrapWire());
      if (path.startsWith("/messaging/places/channel-1/messages")) {
        historyReads += 1;
        if (historyReads === 2) {
          noteOlderSnapshotStarted();
          await olderSnapshotGate;
          return json({ messages: [pollMessageWire(["human-2"])] });
        }
        return json({
          messages: [pollMessageWire(historyReads === 1 ? [] : ["human-3"])],
        });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("poll-overlapping-reconciliation");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace(CHANNEL_KEY);
    await vi.waitFor(() => expect(historyReads).toBe(1));
    const socket = FakeWebSocket.instance;
    socket?.open();

    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });
    await olderSnapshotStarted;
    socket?.message({
      type: "caught_up",
      place_id: "channel-1",
      latest_seq: 4,
    });
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.poll
          ?.options[0]?.voters,
      ).toEqual([{ kind: "human", humanId: "human-3" }]),
    );

    releaseOlderSnapshot();
    await nextTask();
    expect(
      useMessaging.getState().messagesByPlace[CHANNEL_KEY]?.[0]?.poll
        ?.options[0]?.voters,
    ).toEqual([{ kind: "human", humanId: "human-3" }]);
  });
});

describe("thread summary reconciliation", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("refreshes the latest preview without incrementing count after an edit", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        return json({ ...bootstrapWire(), threads: [threadWire("before", 2)] });
      }
      if (path === "/messaging/places/channel-1/threads") {
        return json({ threads: [threadWire("after", 2)] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-edit");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const socket = FakeWebSocket.instance;
    socket?.open();

    socket?.message({
      type: "event",
      event: {
        type: "message_edited",
        place_id: "thread-1",
        message: threadMessageWire(2, "after"),
      },
    });

    await vi.waitFor(() =>
      expect(useMessaging.getState().threadsById["thread-1"]).toMatchObject({
        lastMessage: "after",
        messageCount: 2,
      }),
    );
    expect(fetchMock.mock.calls.map(([input]) => String(input))).toContain(
      "/messaging/places/channel-1/threads",
    );
  });

  it("decrements count without replacing the preview when an older reply is deleted", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        return json({
          ...bootstrapWire(),
          threads: [threadWire("latest", 3, 3)],
        });
      }
      if (path === "/messaging/places/channel-1/threads") {
        return json({ threads: [threadWire("latest", 2, 3)] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-delete-older");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const socket = FakeWebSocket.instance;
    socket?.open();

    socket?.message({
      type: "event",
      event: {
        type: "message_deleted",
        place_id: "thread-1",
        message: threadMessageWire(2, "", true),
      },
    });

    await vi.waitFor(() =>
      expect(useMessaging.getState().threadsById["thread-1"]).toMatchObject({
        lastMessage: "latest",
        messageCount: 2,
        latestSeq: 3,
      }),
    );
  });

  it("finds the previous preview from the server when the latest reply is deleted", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        return json({
          ...bootstrapWire(),
          threads: [threadWire("latest", 3, 3, "2026-08-01T11:00:00Z")],
        });
      }
      if (path === "/messaging/places/channel-1/threads") {
        return json({
          threads: [threadWire("older", 2, 3, "2026-08-01T09:00:00Z")],
        });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-delete-latest");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const socket = FakeWebSocket.instance;
    socket?.open();

    socket?.message({
      type: "event",
      event: {
        type: "message_deleted",
        place_id: "thread-1",
        message: threadMessageWire(3, "", true),
      },
    });

    await vi.waitFor(() =>
      expect(useMessaging.getState().threadsById["thread-1"]).toMatchObject({
        lastMessage: "older",
        lastMessageAt: Date.parse("2026-08-01T09:00:00Z"),
        messageCount: 2,
        latestSeq: 3,
      }),
    );
    // The previous reply was not in the local timeline; the refreshed
    // projection, not a local guess, supplied its preview.
    expect(useMessaging.getState().messagesByPlace["thread:thread-1"]).toEqual([
      expect.objectContaining({ messageId: "thread-message-3", deleted: true }),
    ]);
  });

  it("reconciles the thread summary when an offline poll deletion is discovered", async () => {
    let historyReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        const snapshot = bootstrapWire();
        return json({
          ...snapshot,
          threads: [threadWire("poll", 3, 3)],
          unread_summaries: [
            ...snapshot.unread_summaries,
            {
              place: { kind: "thread", thread_id: "thread-1" },
              latest_seq: 3,
              unread_count: 0,
              mention_count: 0,
            },
          ],
        });
      }
      if (path.startsWith("/messaging/places/thread-1/messages")) {
        historyReads += 1;
        const message = threadPollMessageWire([]);
        return json({
          messages: [
            historyReads === 1
              ? message
              : { ...message, content: "", poll: null, deleted: true },
          ],
        });
      }
      if (path === "/messaging/places/channel-1/threads") {
        return json({ threads: [threadWire("older", 2, 3)] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-poll-offline-delete");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace("thread:thread-1");
    await vi.waitFor(() => expect(historyReads).toBe(1));

    const socket = FakeWebSocket.instance;
    socket?.open();
    socket?.message({
      type: "caught_up",
      place_id: "thread-1",
      latest_seq: 3,
    });

    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace["thread:thread-1"]?.[0],
      ).toMatchObject({ content: "", deleted: true, poll: null }),
    );
    await vi.waitFor(() =>
      expect(useMessaging.getState().threadsById["thread-1"]).toMatchObject({
        lastMessage: "older",
        messageCount: 2,
        latestSeq: 3,
      }),
    );
  });

  it("does not let an older edit refresh overwrite newer thread activity", async () => {
    let releaseRefresh: () => void = () => {};
    let noteRefreshStarted: () => void = () => {};
    const refreshGate = new Promise<void>((resolve) => {
      releaseRefresh = resolve;
    });
    const refreshStarted = new Promise<void>((resolve) => {
      noteRefreshStarted = resolve;
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        return json({ ...bootstrapWire(), threads: [threadWire("before", 2)] });
      }
      if (path === "/messaging/places/channel-1/threads") {
        noteRefreshStarted();
        await refreshGate;
        return json({ threads: [threadWire("edited", 2)] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-refresh-race");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const socket = FakeWebSocket.instance;
    socket?.open();

    socket?.message({
      type: "event",
      event: {
        type: "message_edited",
        place_id: "thread-1",
        message: threadMessageWire(2, "edited"),
      },
    });
    await refreshStarted;
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        place_id: "thread-1",
        message: threadMessageWire(3, "newer"),
        notify: null,
      },
    });
    releaseRefresh();
    await nextTask();

    expect(useMessaging.getState().threadsById["thread-1"]).toMatchObject({
      lastMessage: "newer",
      messageCount: 3,
      latestSeq: 3,
    });
  });

  it("keeps the count exact when a create overtakes a delete refresh", async () => {
    let releaseDeleteRefresh: () => void = () => {};
    let noteDeleteRefreshStarted: () => void = () => {};
    const deleteRefreshGate = new Promise<void>((resolve) => {
      releaseDeleteRefresh = resolve;
    });
    const deleteRefreshStarted = new Promise<void>((resolve) => {
      noteDeleteRefreshStarted = resolve;
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        return json({
          ...bootstrapWire(),
          threads: [threadWire("latest", 3, 3)],
        });
      }
      if (path === "/messaging/places/channel-1/threads") {
        noteDeleteRefreshStarted();
        await deleteRefreshGate;
        return json({ threads: [threadWire("older", 2, 3)] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-delete-create-race");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const socket = FakeWebSocket.instance;
    socket?.open();

    // History may independently observe the tombstone before its live event;
    // summary accounting must not depend on which projection arrived first.
    useMessaging.setState((state) => ({
      messagesByPlace: {
        ...state.messagesByPlace,
        "thread:thread-1": [threadMessage(3, "", true)],
      },
    }));

    socket?.message({
      type: "event",
      event: {
        type: "message_deleted",
        place_id: "thread-1",
        message: threadMessageWire(3, "", true),
      },
    });
    await deleteRefreshStarted;
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        place_id: "thread-1",
        message: threadMessageWire(4, "newer"),
        notify: null,
      },
    });
    releaseDeleteRefresh();
    await nextTask();

    expect(useMessaging.getState().threadsById["thread-1"]).toMatchObject({
      lastMessage: "newer",
      messageCount: 3,
      latestSeq: 4,
    });
  });

  it("does not let an older panel load overwrite an event refresh", async () => {
    let releasePanelLoad: () => void = () => {};
    let notePanelLoadStarted: () => void = () => {};
    const panelLoadGate = new Promise<void>((resolve) => {
      releasePanelLoad = resolve;
    });
    const panelLoadStarted = new Promise<void>((resolve) => {
      notePanelLoadStarted = resolve;
    });
    let threadReads = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input);
      if (path === "/messaging/bootstrap") {
        return json({ ...bootstrapWire(), threads: [threadWire("before", 2)] });
      }
      if (path === "/messaging/places/channel-1/threads") {
        threadReads += 1;
        if (threadReads === 1) {
          notePanelLoadStarted();
          await panelLoadGate;
          return json({ threads: [threadWire("before", 2)] });
        }
        return json({ threads: [threadWire("after", 2)] });
      }
      throw new Error(`unexpected request ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("WebSocket", FakeWebSocket);
    bindMessagingSessionIdentity("thread-panel-race");
    installMessagingBackend(new ApiMessagingBackend());
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const socket = FakeWebSocket.instance;
    socket?.open();

    const panelLoad = useMessaging.getState().loadThreads(CHANNEL_KEY);
    await panelLoadStarted;
    socket?.message({
      type: "event",
      event: {
        type: "message_edited",
        place_id: "thread-1",
        message: threadMessageWire(2, "after"),
      },
    });
    await vi.waitFor(() =>
      expect(useMessaging.getState().threadsById["thread-1"]?.lastMessage).toBe(
        "after",
      ),
    );
    releasePanelLoad();
    await panelLoad;

    expect(useMessaging.getState().threadsById["thread-1"]?.lastMessage).toBe(
      "after",
    );
  });
});

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 3;
  static instance: FakeWebSocket | null = null;
  readyState = FakeWebSocket.CONNECTING;
  readonly sent: string[] = [];
  readonly url: string | URL;

  constructor(url: string | URL) {
    super();
    this.url = url;
    FakeWebSocket.instance = this;
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
  }

  message(value: unknown): void {
    this.dispatchEvent(
      new MessageEvent("message", { data: JSON.stringify(value) }),
    );
  }
}

function bootstrapWire() {
  return {
    self: { kind: "human", human_id: "human-1" },
    workspaces: [{ workspace_id: "workspace-1", name: "Sumi" }],
    channels: [
      {
        channel_id: "channel-1",
        workspace_id: "workspace-1",
        name: "general",
        topic: "",
        visibility: "public",
      },
    ],
    dms: [],
    threads: [],
    members: [],
    statuses: [],
    read_markers: [],
    unread_summaries: [
      {
        place: { kind: "channel", channel_id: "channel-1" },
        latest_seq: 4,
        unread_count: 0,
        mention_count: 0,
      },
    ],
    reply_later_markers: [],
    notification_setting: {
      owner: { kind: "human", human_id: "human-1" },
      defaults: { level: "all" },
      per_place: [],
      keywords: [],
    },
  };
}

function pollMessageWire(voterIDs: string[]) {
  return {
    message_id: "message-2",
    place: { kind: "channel", channel_id: "channel-1" },
    seq: 2,
    author: { kind: "human", human_id: "human-1" },
    content: "リリースはいつ？",
    mentions: [],
    attachments: [],
    urgency: "normal",
    reactions: [],
    poll: {
      question: "リリースはいつ？",
      allow_multi: false,
      closes_at: null,
      options: [
        {
          option_id: "option-1",
          text: "今日",
          voters: voterIDs.map((humanID) => ({
            kind: "human",
            human_id: humanID,
          })),
        },
        { option_id: "option-2", text: "明日", voters: [] },
      ],
    },
    reply_to: null,
    client_nonce: "poll-message",
    created_at: "2026-08-01T10:00:00Z",
    edited_at: null,
    deleted: false,
  };
}

function threadPollMessageWire(voterIDs: string[]) {
  return {
    ...pollMessageWire(voterIDs),
    message_id: "thread-message-3",
    place: { kind: "thread", thread_id: "thread-1" },
    seq: 3,
    client_nonce: "thread-poll-message",
  };
}

function threadWire(
  lastMessage: string,
  messageCount: number,
  latestSeq = 2,
  lastMessageAt = "2026-08-01T10:00:00Z",
) {
  return {
    thread_id: "thread-1",
    parent_place: { kind: "channel", channel_id: "channel-1" },
    parent_message_id: "message-1",
    workspace_id: "workspace-1",
    name: "脇道",
    message_count: messageCount,
    last_message_at: lastMessageAt,
    last_message: lastMessage,
    participants: [{ kind: "human", human_id: "human-1" }],
    latest_seq: latestSeq,
  };
}

function threadMessageWire(seq: number, content: string, deleted = false) {
  return {
    message_id: `thread-message-${seq}`,
    place: { kind: "thread", thread_id: "thread-1" },
    seq,
    author: { kind: "human", human_id: "human-1" },
    content: deleted ? "" : content,
    mentions: [],
    attachments: [],
    urgency: "normal",
    reactions: [],
    reply_to: null,
    client_nonce: `thread-${seq}`,
    created_at: "2026-08-01T10:00:00Z",
    edited_at: deleted ? null : "2026-08-01T11:00:00Z",
    deleted,
  };
}

function threadMessage(seq: number, content: string, deleted = false) {
  return {
    messageId: `thread-message-${seq}`,
    place: { kind: "thread" as const, threadId: "thread-1" },
    seq,
    author: { kind: "human" as const, humanId: "human-1" },
    content,
    mentions: [],
    urgency: "normal" as const,
    reactions: [],
    attachments: [],
    replyTo: null,
    createdAt: Date.parse("2026-08-01T10:00:00Z"),
    editedAt: null,
    deleted,
  };
}

function json(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function nextTask(): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, 0));
}
