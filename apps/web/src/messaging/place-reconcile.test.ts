// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type { ConnectionState, MessagingBackend } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

type Snapshot = Awaited<ReturnType<MessagingBackend["bootstrap"]>>;

class ReconnectingMockServer extends MockMessagingServer {
  connectionListener: ((state: ConnectionState) => void) | null = null;

  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    this.connectionListener = listener;
    listener("reconnecting");
    return () => {
      this.connectionListener = null;
    };
  }

  emitConnection(state: ConnectionState): void {
    this.connectionListener?.(state);
  }
}

function deferred<T>() {
  let resolve: (value: T) => void = () => {};
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function withChannel(
  snapshot: Snapshot,
  channelId: string,
  topic: string,
  latestSeq = 0,
): Snapshot {
  const nextChannel = {
    channelId,
    workspaceId: snapshot.workspaces[0]?.workspaceId ?? "ws-sumi",
    name: channelId,
    topic,
    visibility: "public" as const,
    voice: false,
  };
  const place = { kind: "channel" as const, channelId };
  return {
    ...snapshot,
    channels: [
      snapshot.channels[0] as Snapshot["channels"][number],
      nextChannel,
    ],
    readMarkers: [...snapshot.readMarkers, { place, lastReadSeq: 0 }],
    unreadSummaries: [
      ...snapshot.unreadSummaries,
      {
        place,
        latestSeq,
        unreadCount: latestSeq,
        mentionCount: 0,
      },
    ],
  };
}

async function setup(
  configure: (server: ReconnectingMockServer, initial: Snapshot) => void,
) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () =>
      Response.json(
        { calls: [] },
        { headers: { "Content-Type": "application/json" } },
      ),
    ),
  );
  const server = new ReconnectingMockServer();
  const initial = await server.bootstrap();
  configure(server, initial);
  bindMessagingSessionIdentity("place-reconcile-human");
  installMessagingBackend(server);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
  server.emitConnection("connected");
  return { server, initial };
}

afterEach(() => {
  bindMessagingSessionIdentity(null);
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("place lifecycle reconnect reconciliation", () => {
  it("adopts newly durable places without rolling back known local state", async () => {
    let second: Snapshot | null = null;
    const subscribeSpy = vi.fn();
    const { server, initial } = await setup((candidate, snapshot) => {
      second = withChannel(snapshot, "channel-new", "reconnected", 2);
      vi.spyOn(candidate, "bootstrap")
        .mockResolvedValueOnce(snapshot)
        .mockImplementation(async () => second as Snapshot);
      const originalSubscribe = candidate.subscribe.bind(candidate);
      vi.spyOn(candidate, "subscribe").mockImplementation(
        (listener, options) => {
          subscribeSpy(options?.sinceByPlace);
          return originalSubscribe(listener, options);
        },
      );
    });

    const knownKey = `channel:${initial.channels[0]?.channelId}` as const;
    useMessaging.getState().selectPlace(knownKey);
    useMessaging.getState().setDraft(knownKey, "書きかけ");
    useMessaging.getState().noteReadUpTo(knownKey, 99);

    server.emitConnection("reconnecting");
    server.emitConnection("connected");

    await vi.waitFor(() =>
      expect(useMessaging.getState().channels).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            channelId: "channel-new",
            topic: "reconnected",
          }),
        ]),
      ),
    );
    const state = useMessaging.getState();
    expect(state.activePlaceKey).toBe(knownKey);
    expect(state.draftByPlace[knownKey]).toBe("書きかけ");
    expect(state.lastReadByPlace[knownKey]).toBe(99);
    expect(state.unreadCountByPlace["channel:channel-new"]).toBe(2);
    expect(subscribeSpy).toHaveBeenLastCalledWith({
      "channel:channel-new": 2,
    });
  });

  it("ignores an older reconciliation response after a newer reconnect", async () => {
    const slow = deferred<Snapshot>();
    let initialSnapshot!: Snapshot;
    const { server } = await setup((candidate, initial) => {
      initialSnapshot = initial;
      vi.spyOn(candidate, "bootstrap")
        .mockResolvedValueOnce(initial)
        .mockReturnValueOnce(slow.promise)
        .mockResolvedValueOnce(withChannel(initial, "latest", "newest"));
    });

    server.emitConnection("reconnecting");
    server.emitConnection("connected");
    await vi.waitFor(() => expect(server.bootstrap).toHaveBeenCalledTimes(2));
    server.emitConnection("reconnecting");
    server.emitConnection("connected");
    await vi.waitFor(() =>
      expect(useMessaging.getState().channels).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ channelId: "latest" }),
        ]),
      ),
    );

    slow.resolve(withChannel(initialSnapshot, "stale", "older"));
    await Promise.resolve();
    await Promise.resolve();

    expect(
      useMessaging
        .getState()
        .channels.some((entry) => entry.channelId === "stale"),
    ).toBe(false);
    expect(
      useMessaging
        .getState()
        .channels.some((entry) => entry.channelId === "latest"),
    ).toBe(true);
  });

  it("keeps known places when the reconnect bootstrap fails", async () => {
    const { server, initial } = await setup((candidate, snapshot) => {
      vi.spyOn(candidate, "bootstrap")
        .mockResolvedValueOnce(snapshot)
        .mockRejectedValueOnce(new Error("offline"));
    });

    server.emitConnection("reconnecting");
    server.emitConnection("connected");
    await vi.waitFor(() => expect(server.bootstrap).toHaveBeenCalledTimes(2));

    expect(useMessaging.getState().channels).toEqual(initial.channels);
    expect(useMessaging.getState().connection).toBe("connected");
  });
});
