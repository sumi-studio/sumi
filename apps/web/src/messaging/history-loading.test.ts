import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { Message } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const key = "thread:history-thread" as const;
const place = { kind: "thread", threadId: "history-thread" } as const;

function message(seq: number): Message {
  return {
    messageId: `history-${seq}`,
    place,
    seq,
    author: { kind: "human", humanId: "self" },
    content: `Message ${seq}`,
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: seq,
    editedAt: null,
    deleted: false,
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

async function openHistory() {
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
  const page = Array.from({ length: 50 }, (_, index) => message(index + 51));
  const fetch = vi.spyOn(server, "fetchMessages").mockResolvedValue(page);
  // Avoid unrelated connection resyncs; this test drives the history API only.
  vi.spyOn(server, "subscribeConnection").mockReturnValue(() => {});
  installMessagingBackend(server);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
  useMessaging.getState().selectPlace(key);
  await vi.waitFor(() =>
    expect(useMessaging.getState().messagesByPlace[key]).toHaveLength(50),
  );
  fetch.mockClear();
  return { fetch, page };
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
