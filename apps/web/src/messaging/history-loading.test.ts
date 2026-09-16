import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { Message, ServerEvent } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";
import { buildRows } from "./timeline";

const key = "thread:history-thread" as const;
const place = { kind: "thread", threadId: "history-thread" } as const;

function message(seq: number, deleted = false): Message {
  return {
    messageId: `history-${seq}`,
    place,
    seq,
    author: { kind: "human", humanId: "self" },
    content: deleted ? "" : `Message ${seq}`,
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: seq,
    editedAt: null,
    deleted,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

let session = 0;

async function openHistory(newestPage?: Message[]) {
  bindMessagingSessionIdentity(`history-test-${++session}`);
  const server = new MockMessagingServer();
  const snapshot = await server.bootstrap();
  vi.spyOn(server, "bootstrap").mockResolvedValue({
    ...snapshot,
    threads: [
      {
        threadId: place.threadId,
        revision: 1,
        workspaceId: "workspace-1",
        parentPlace: { kind: "channel", channelId: "ch-general" },
        parentMessageId: null,
        name: "History",
        messageCount: 100,
        lastMessageAt: 100,
        lastMessage: "Message 100",
        participants: [],
        latestSeq: 100,
      },
    ],
  });
  const page =
    newestPage ?? Array.from({ length: 50 }, (_, index) => message(index + 51));
  const fetch = vi.spyOn(server, "fetchMessages").mockResolvedValue(page);
  // Avoid unrelated connection resyncs; this test drives the history API only.
  vi.spyOn(server, "subscribeConnection").mockReturnValue(() => {});
  let emit: (event: ServerEvent) => void = () => {};
  const realSubscribe = server.subscribe.bind(server);
  vi.spyOn(server, "subscribe").mockImplementation((listener, options) => {
    emit = listener;
    return realSubscribe(listener, options);
  });
  installMessagingBackend(server);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
  useMessaging.getState().selectPlace(key);
  await vi.waitFor(() =>
    expect(useMessaging.getState().messagesByPlace[key]).toHaveLength(
      page.length,
    ),
  );
  fetch.mockClear();
  return { fetch, page, emit: (event: ServerEvent) => emit(event) };
}

afterEach(() => {
  bindMessagingSessionIdentity(null);
  vi.restoreAllMocks();
});

describe("older message recovery", () => {
  it("allows retry after a failed page without losing history or duplicating rows", async () => {
    const { fetch, page } = await openHistory();
    fetch.mockRejectedValueOnce(new Error("temporary network failure"));
    await expect(
      useMessaging.getState().loadOlder(key),
    ).resolves.toBeUndefined();
    expect(useMessaging.getState().messagesByPlace[key]).toEqual(page);

    const retry = deferred<Message[]>();
    fetch.mockReturnValueOnce(retry.promise);
    const loading = useMessaging.getState().loadOlder(key);
    await useMessaging.getState().loadOlder(key);
    expect(fetch).toHaveBeenCalledTimes(2);
    retry.resolve([message(49), message(50), message(51)]);
    await loading;
    expect(useMessaging.getState().messagesByPlace[key]).toEqual([
      message(49),
      message(50),
      ...page,
    ]);
    expect(useMessaging.getState().loadingOlderByPlace[key]).toBe(false);
  });

  it.each([
    { transition: "session", outcome: "resolve" },
    { transition: "session", outcome: "reject" },
    { transition: "place", outcome: "resolve" },
    { transition: "place", outcome: "reject" },
  ])("ignores an old $outcome after replacing the $transition while a new page is pending", async ({
    transition,
    outcome,
  }) => {
    const old = await openHistory();
    const stale = deferred<Message[]>();
    old.fetch.mockReturnValueOnce(stale.promise);
    const oldLoad = useMessaging.getState().loadOlder(key);

    let current = old;
    if (transition === "session") {
      current = await openHistory();
    } else {
      // Leaving a thread we have not joined releases its history and ownership.
      useMessaging.getState().clearPlaceSelection();
      old.fetch.mockResolvedValueOnce(old.page);
      useMessaging.getState().selectPlace(key);
      await vi.waitFor(() =>
        expect(useMessaging.getState().messagesByPlace[key]).toHaveLength(50),
      );
      old.fetch.mockClear();
    }
    const fresh = deferred<Message[]>();
    current.fetch.mockReturnValueOnce(fresh.promise);
    const currentLoad = useMessaging.getState().loadOlder(key);
    if (outcome === "resolve") stale.resolve([message(1)]);
    else stale.reject(new Error("old request failed"));
    await oldLoad;
    await useMessaging.getState().loadOlder(key);
    expect(current.fetch).toHaveBeenCalledTimes(1);
    expect(useMessaging.getState().messagesByPlace[key]).toEqual(current.page);
    fresh.resolve([message(50)]);
    await currentLoad;
    expect(useMessaging.getState().messagesByPlace[key]).toEqual([
      message(50),
      ...current.page,
    ]);
  });
});

function range(from: number, to: number): Message[] {
  return Array.from({ length: to - from + 1 }, (_, index) =>
    message(from + index),
  );
}

function timelineRows() {
  return buildRows({
    messages: useMessaging.getState().messagesByPlace[key] ?? [],
    pending: [],
    selfKey: "human:self",
    unreadLineSeq: null,
    self: { kind: "human", humanId: "self" },
    now: 1_000,
  });
}

describe("search-result jump windows", () => {
  it("loads both sides of an old target and keeps the missing range reachable", async () => {
    const { fetch, page } = await openHistory();
    // 100-seq thread, newest page 51..100 loaded. A search hit at seq 20 must
    // land in a window that has context on both sides, and the remaining
    // unloaded range must be surfaced — not silently adjacent.
    fetch.mockResolvedValueOnce(range(1, 44));
    await expect(
      useMessaging.getState().loadPlaceAround(key, 20),
    ).resolves.toBe(true);
    // Centered on the target: 25 older + target + 24 newer.
    expect(fetch).toHaveBeenCalledWith(place, { beforeSeq: 45, limit: 50 });

    const loaded = useMessaging.getState().messagesByPlace[key];
    expect(loaded.map((entry) => entry.seq)).toEqual([
      ...range(1, 44).map((entry) => entry.seq),
      ...page.map((entry) => entry.seq),
    ]);
    // The window reached the history floor; the top loader is done.
    expect(useMessaging.getState().hasMoreByPlace[key]).toBe(false);

    // The internal missing range 45..50 is a truthful gap row, not a seam.
    const gaps = timelineRows().filter((row) => row.kind === "gap");
    expect(gaps).toEqual([
      expect.objectContaining({
        kind: "gap",
        afterSeq: 44,
        beforeSeq: 51,
        missingCount: 6,
      }),
    ]);

    // The gap is reachable: filling it joins the windows into one contiguous
    // history and removes the gap row.
    fetch.mockResolvedValueOnce(range(45, 50));
    await useMessaging.getState().loadGap(key, 51);
    expect(fetch).toHaveBeenCalledWith(place, { beforeSeq: 51, limit: 50 });
    expect(useMessaging.getState().messagesByPlace[key]).toHaveLength(100);
    expect(timelineRows().filter((row) => row.kind === "gap")).toHaveLength(0);

    // A repeated jump into the now-cached target does not refetch.
    fetch.mockClear();
    await expect(
      useMessaging.getState().loadPlaceAround(key, 20),
    ).resolves.toBe(true);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("keeps a failed gap fill retryable without losing either window", async () => {
    const { fetch, page } = await openHistory();
    fetch.mockResolvedValueOnce(range(1, 44));
    await expect(
      useMessaging.getState().loadPlaceAround(key, 20),
    ).resolves.toBe(true);

    fetch.mockRejectedValueOnce(new Error("temporary network failure"));
    await useMessaging.getState().loadGap(key, 51);
    // Nothing merged, both windows intact, marker cleared, gap still shown.
    expect(useMessaging.getState().messagesByPlace[key]).toEqual([
      ...range(1, 44),
      ...page,
    ]);
    expect(useMessaging.getState().loadingGapsByPlace[key]).toEqual([]);
    expect(
      timelineRows().some((row) => row.kind === "gap" && row.beforeSeq === 51),
    ).toBe(true);

    fetch.mockResolvedValueOnce(range(45, 50));
    await useMessaging.getState().loadGap(key, 51);
    expect(useMessaging.getState().messagesByPlace[key]).toHaveLength(100);
  });

  it("keeps the omitted range reachable when a tombstone lands inside it live", async () => {
    const { fetch, emit } = await openHistory();
    fetch.mockResolvedValueOnce(range(1, 44));
    await useMessaging.getState().loadPlaceAround(key, 20);
    expect(
      timelineRows()
        .filter((row) => row.kind === "gap")
        .map((row) => row.kind === "gap" && [row.afterSeq, row.beforeSeq]),
    ).toEqual([[44, 51]]);

    // Someone deletes seq 47 — a message this client never loaded. The event
    // upserts a tombstone into the middle of the unloaded range.
    emit({ type: "message_deleted", message: message(47, true) });
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[key].map((m) => m.seq),
      ).toContain(47),
    );

    // Both halves of the interrupted range must stay reachable: two gap rows,
    // not silently adjacent windows around an invisible tombstone.
    expect(
      timelineRows()
        .filter((row) => row.kind === "gap")
        .map((row) => row.kind === "gap" && [row.afterSeq, row.beforeSeq]),
    ).toEqual([
      [44, 47],
      [47, 51],
    ]);
  });

  it("surfaces the omitted range when the newest page starts with a tombstone", async () => {
    // Newest page's first row was deleted before it was ever loaded: the
    // window boundary itself is a tombstone. The gap row must still be
    // emitted — this is the boundary case that hid 45..50 entirely.
    const { fetch } = await openHistory([message(51, true), ...range(52, 100)]);
    fetch.mockResolvedValueOnce(range(1, 44));
    await expect(
      useMessaging.getState().loadPlaceAround(key, 20),
    ).resolves.toBe(true);

    expect(
      timelineRows()
        .filter((row) => row.kind === "gap")
        .map((row) => row.kind === "gap" && [row.afterSeq, row.beforeSeq]),
    ).toEqual([[44, 51]]);

    // And the range is actually reachable through the boundary tombstone.
    fetch.mockResolvedValueOnce(range(45, 50));
    await useMessaging.getState().loadGap(key, 51);
    expect(
      useMessaging.getState().messagesByPlace[key].map((m) => m.seq),
    ).toEqual(range(1, 100).map((m) => m.seq));
  });

  it("still fetches context for a search target that is already a tombstone", async () => {
    // The hit at seq 20 was deleted between search and click; its tombstone
    // arrived live. Treating the tombstone as "already loaded" would skip the
    // fetch entirely and leave the jump with no context and no outcome.
    const { fetch, emit } = await openHistory();
    emit({ type: "message_deleted", message: message(20, true) });
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[key].map((m) => m.seq),
      ).toContain(20),
    );
    fetch.mockClear();

    fetch.mockResolvedValueOnce(
      range(1, 44).map((m) => (m.seq === 20 ? message(20, true) : m)),
    );
    await expect(
      useMessaging.getState().loadPlaceAround(key, 20),
    ).resolves.toBe(true);
    // Context was fetched even though the tombstone was already present.
    expect(fetch).toHaveBeenCalledWith(place, { beforeSeq: 45, limit: 50 });
    // The tombstone's neighbors are loaded; a deleted-target marker can be
    // rendered at seq 20 and the jump settles on it.
    const rows = buildRows({
      messages: useMessaging.getState().messagesByPlace[key],
      pending: [],
      selfKey: "human:self",
      unreadLineSeq: null,
      self: { kind: "human", humanId: "self" },
      now: 1_000,
      deletedTargetSeq: 20,
    });
    expect(rows.some((row) => row.id === "deleted:20")).toBe(true);
  });
});
