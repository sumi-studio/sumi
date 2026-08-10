// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type {
  Message,
  ParticipantRef,
  Place,
  PlaceKey,
  ReactionMutationResult,
  ServerEvent,
} from "./model";
import { MAX_SEQ } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const SELF: ParticipantRef = { kind: "human", humanId: "h-yohaku" };
const OTHER: ParticipantRef = { kind: "human", humanId: "human-2" };
const PLACE: Place = { kind: "channel", channelId: "ch-general" };
const PLACE_KEY: PlaceKey = "channel:ch-general";

function message(
  messageId: string,
  seq: number,
  reactions: Message["reactions"] = [],
): Message {
  return {
    messageId,
    place: PLACE,
    seq,
    author: OTHER,
    content: `message ${seq}`,
    mentions: [],
    urgency: "normal",
    reactions,
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: seq,
    editedAt: null,
    deleted: false,
  };
}

interface PendingSet {
  messageId: string;
  emoji: string;
  reacted: boolean;
  resolve: (result: ReactionMutationResult) => void;
}

class ControlledReactionBackend extends MockMessagingServer {
  authoritativeHistory: Message[] = [];
  holdFetch = false;
  fetches: { beforeSeq?: number; limit?: number }[] = [];
  setCalls: { messageId: string; emoji: string; reacted: boolean }[] = [];
  pendingSets: PendingSet[] = [];
  private eventListener: ((event: ServerEvent) => void) | null = null;
  private catchUpListener:
    ((place: Place, latestSeq: number) => void | Promise<void>) | null = null;
  private pendingFetch: {
    resolve: (messages: Message[]) => void;
    promise: Promise<Message[]>;
  } | null = null;

  override async fetchMessages(
    _place: Place,
    options: { beforeSeq?: number; limit?: number } = {},
  ): Promise<Message[]> {
    this.fetches.push(options);
    if (this.holdFetch) {
      if (!this.pendingFetch) {
        let resolve!: (messages: Message[]) => void;
        const promise = new Promise<Message[]>((done) => {
          resolve = done;
        });
        this.pendingFetch = { resolve, promise };
      }
      return this.pendingFetch.promise;
    }
    return this.page(options);
  }

  override async setReaction(
    _place: Place,
    messageId: string,
    emoji: string,
    reacted: boolean,
  ): Promise<ReactionMutationResult> {
    this.setCalls.push({ messageId, emoji, reacted });
    return new Promise((resolve) => {
      this.pendingSets.push({ messageId, emoji, reacted, resolve });
    });
  }

  override subscribe(listener: (event: ServerEvent) => void): () => void {
    this.eventListener = listener;
    return () => {
      this.eventListener = null;
    };
  }

  subscribeCatchUp(
    listener: (place: Place, latestSeq: number) => void | Promise<void>,
  ): () => void {
    this.catchUpListener = listener;
    return () => {
      this.catchUpListener = null;
    };
  }

  override dispose(): void {
    super.dispose();
    this.eventListener = null;
    this.catchUpListener = null;
  }

  pushEvent(event: ServerEvent): void {
    this.eventListener?.(event);
  }

  async catchUp(latestSeq = 100): Promise<void> {
    await this.catchUpListener?.(PLACE, latestSeq);
  }

  resolveFetch(messages = this.page({})): void {
    const pending = this.pendingFetch;
    if (!pending) throw new Error("no pending reaction fetch");
    this.pendingFetch = null;
    this.holdFetch = false;
    pending.resolve(messages);
  }

  resolveSet(messageId: string, reactions: Message["reactions"]): void {
    const index = this.pendingSets.findIndex(
      (pending) => pending.messageId === messageId,
    );
    if (index < 0) throw new Error(`no pending reaction set for ${messageId}`);
    const [pending] = this.pendingSets.splice(index, 1);
    pending.resolve({
      messageId,
      reactions,
      reacted: pending.reacted,
    });
  }

  private page(options: { beforeSeq?: number; limit?: number }): Message[] {
    const beforeSeq = options.beforeSeq ?? Number.POSITIVE_INFINITY;
    const limit = options.limit ?? 50;
    const eligible = this.authoritativeHistory.filter(
      (entry) => entry.seq < beforeSeq,
    );
    return eligible.slice(Math.max(0, eligible.length - limit));
  }
}

async function start(
  backend: ControlledReactionBackend,
  identity = "reaction-human",
): Promise<void> {
  bindMessagingSessionIdentity(identity);
  installMessagingBackend(backend);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
}

describe("reaction projection convergence", () => {
  afterEach(() => bindMessagingSessionIdentity(null));

  it("re-reads loaded reactions after catch-up", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const loaded = [message("m1", 1), message("m2", 2)];
    const authoritative = [
      message("m1", 1, [{ emoji: "👀", participants: [OTHER] }]),
      message("m2", 2),
    ];
    backend.authoritativeHistory = authoritative;
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: loaded } });

    await backend.catchUp(2);

    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
    ).toEqual(authoritative[0].reactions);
  });

  it("waits for an initial place load before reconciling catch-up", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const stale = message("m1", 1);
    const fresh = {
      ...stale,
      reactions: [{ emoji: "👀", participants: [OTHER] }],
    };
    backend.holdFetch = true;

    useMessaging.getState().selectPlace(PLACE_KEY);
    await vi.waitFor(() => expect(backend.fetches).toHaveLength(1));
    const catchUp = backend.catchUp(1);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(backend.fetches).toHaveLength(1);

    backend.authoritativeHistory = [fresh];
    backend.resolveFetch([stale]);
    await catchUp;

    expect(backend.fetches).toHaveLength(2);
    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
    ).toEqual(fresh.reactions);
  });

  it("replays an unknown live reaction after the initial history snapshot", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const stale = message("m1", 1);
    const live = [{ emoji: "👍", participants: [OTHER] }];
    backend.holdFetch = true;

    useMessaging.getState().selectPlace(PLACE_KEY);
    await vi.waitFor(() => expect(backend.fetches).toHaveLength(1));
    backend.pushEvent({
      type: "reaction_updated",
      place: PLACE,
      messageId: "m1",
      reactions: live,
    });
    backend.resolveFetch([stale]);

    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
      ).toEqual(live),
    );
  });

  it("replays an unknown live reaction after an older-page snapshot", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const current = message("m51", 51);
    const older = message("m1", 1);
    const live = [{ emoji: "🎉", participants: [OTHER] }];
    backend.holdFetch = true;
    useMessaging.setState({
      messagesByPlace: { [PLACE_KEY]: [current] },
      hasMoreByPlace: { [PLACE_KEY]: true },
    });

    const load = useMessaging.getState().loadOlder(PLACE_KEY);
    await vi.waitFor(() => expect(backend.fetches).toHaveLength(1));
    backend.pushEvent({
      type: "reaction_updated",
      place: PLACE,
      messageId: "m1",
      reactions: live,
    });
    backend.resolveFetch([older]);
    await load;

    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
    ).toEqual(live);
    expect(useMessaging.getState().loadingOlderByPlace[PLACE_KEY]).toBe(false);
  });

  it("covers paginated and maximum-sequence loaded windows", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const loaded = Array.from({ length: 205 }, (_, index) =>
      message(`m${index + 1}`, index + 1),
    );
    backend.authoritativeHistory = loaded.map((entry) =>
      entry.seq === 1
        ? { ...entry, reactions: [{ emoji: "👀", participants: [OTHER] }] }
        : entry,
    );
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: loaded } });

    await backend.catchUp(205);

    expect(backend.fetches).toEqual([
      { beforeSeq: 206, limit: 200 },
      { beforeSeq: 6, limit: 5 },
    ]);
    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
    ).toEqual([{ emoji: "👀", participants: [OTHER] }]);

    const atMax = message("m-max", MAX_SEQ);
    backend.fetches.length = 0;
    backend.authoritativeHistory = [
      { ...atMax, reactions: [{ emoji: "🔥", participants: [OTHER] }] },
    ];
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: [atMax] } });

    await backend.catchUp(MAX_SEQ);

    expect(backend.fetches).toEqual([{ limit: 1 }]);
    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
    ).toEqual([{ emoji: "🔥", participants: [OTHER] }]);
  });

  it("replays a live update that races an older reconnect snapshot", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const loaded = message("m1", 1);
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: [loaded] } });
    backend.holdFetch = true;
    const catchUp = backend.catchUp(1);
    await vi.waitFor(() => expect(backend.fetches).toHaveLength(1));

    const live = [{ emoji: "👍", participants: [OTHER] }];
    backend.pushEvent({
      type: "reaction_updated",
      place: PLACE,
      messageId: "m1",
      reactions: live,
    });
    backend.resolveFetch([loaded]);
    await catchUp;

    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
    ).toEqual(live);
  });

  it("runs mutations for different messages concurrently", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const first = message("m1", 1);
    const second = message("m2", 2);
    useMessaging.setState({
      messagesByPlace: { [PLACE_KEY]: [first, second] },
    });

    useMessaging.getState().toggleReaction(first, "👍");
    useMessaging.getState().toggleReaction(second, "🎉");
    await vi.waitFor(() => expect(backend.setCalls).toHaveLength(2));

    backend.resolveSet("m2", [{ emoji: "🎉", participants: [SELF] }]);
    backend.resolveSet("m1", [{ emoji: "👍", participants: [SELF] }]);
    await vi.waitFor(() => {
      const current = useMessaging.getState().messagesByPlace[PLACE_KEY] ?? [];
      expect(current[0]?.reactions[0]?.emoji).toBe("👍");
      expect(current[1]?.reactions[0]?.emoji).toBe("🎉");
    });
    expect(backend.setCalls.every((call) => call.reacted)).toBe(true);
  });

  it("serializes canonical snapshots for the same message", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const target = message("m1", 1);
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: [target] } });

    useMessaging.getState().toggleReaction(target, "👍");
    useMessaging.getState().toggleReaction(target, "🎉");
    await vi.waitFor(() => expect(backend.setCalls).toHaveLength(1));

    backend.resolveSet("m1", [{ emoji: "👍", participants: [SELF] }]);
    await vi.waitFor(() => expect(backend.setCalls).toHaveLength(2));
    backend.resolveSet("m1", [
      { emoji: "👍", participants: [SELF] },
      { emoji: "🎉", participants: [SELF] },
    ]);

    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0]?.reactions,
      ).toHaveLength(2),
    );
  });

  it("discards delayed snapshots and queued mutations from an old identity", async () => {
    const oldBackend = new ControlledReactionBackend();
    await start(oldBackend, "reaction-old");
    const oldTarget = message("m1", 1);
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: [oldTarget] } });
    oldBackend.holdFetch = true;

    const oldCatchUp = oldBackend.catchUp(1);
    useMessaging.getState().toggleReaction(oldTarget, "👍");
    useMessaging.getState().toggleReaction(oldTarget, "🎉");
    await vi.waitFor(() => {
      expect(oldBackend.fetches).toHaveLength(1);
      expect(oldBackend.setCalls).toHaveLength(1);
    });

    bindMessagingSessionIdentity("reaction-new");
    const newBackend = new ControlledReactionBackend();
    installMessagingBackend(newBackend);
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    const newTarget = {
      ...message("m1", 1),
      content: "new identity",
      reactions: [{ emoji: "🔥", participants: [OTHER] }],
    };
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: [newTarget] } });

    oldBackend.resolveFetch([
      {
        ...oldTarget,
        reactions: [{ emoji: "👀", participants: [OTHER] }],
      },
    ]);
    oldBackend.resolveSet("m1", [{ emoji: "👍", participants: [SELF] }]);
    await oldCatchUp;
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(oldBackend.setCalls).toHaveLength(1);
    expect(
      useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0],
    ).toMatchObject({
      content: "new identity",
      reactions: [{ emoji: "🔥", participants: [OTHER] }],
    });
  });

  it("preserves independent edits and lets tombstones defeat late updates", async () => {
    const backend = new ControlledReactionBackend();
    await start(backend);
    const reactions = [{ emoji: "👀", participants: [OTHER] }];
    const original = message("m1", 1, reactions);
    useMessaging.setState({ messagesByPlace: { [PLACE_KEY]: [original] } });

    backend.pushEvent({
      type: "message_edited",
      message: { ...original, content: "edited", reactions: [] },
    });
    let projected = useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0];
    expect(projected?.content).toBe("edited");
    expect(projected?.reactions).toEqual(reactions);

    backend.pushEvent({
      type: "message_deleted",
      message: {
        ...original,
        content: "",
        reactions: [],
        deleted: true,
      },
    });
    backend.pushEvent({
      type: "reaction_updated",
      place: PLACE,
      messageId: "m1",
      reactions: [{ emoji: "🔥", participants: [OTHER] }],
    });
    projected = useMessaging.getState().messagesByPlace[PLACE_KEY]?.[0];
    expect(projected?.deleted).toBe(true);
    expect(projected?.reactions).toEqual([]);
  });
});
