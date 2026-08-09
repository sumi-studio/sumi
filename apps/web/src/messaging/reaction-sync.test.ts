import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  Place,
  ServerEvent,
} from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const place: Place = { kind: "channel", channelId: "channel-1" };
const placeKey = "channel:channel-1" as const;

/**
 * reaction eventはmessage全体を運ばない、という契約のstore側の裏。編集を
 * 巻き戻さないこと、切断中に落ちたreactionが再接続で戻ることを見る。
 */
describe("reaction convergence in the messaging store", () => {
  let session = 0;

  beforeEach(() => {
    bindMessagingSessionIdentity(`reaction-test-${++session}`);
  });

  afterEach(() => {
    bindMessagingSessionIdentity(null);
  });

  it("patches only reactions and re-reads the loaded window on caught_up", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "編集前"), message(2, "隣")] },
    });

    // 編集がcommit/publishしたあとに、編集前のcontentを見ていたreaction eventが
    // 遅れて届く。reactionだけが乗るので編集は生き残る。
    harness.emit({ type: "message_edited", message: message(1, "編集後", 99) });
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-1",
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    const afterEdit = useMessaging.getState().messagesByPlace[placeKey] ?? [];
    expect(afterEdit[0]).toMatchObject({
      content: "編集後",
      editedAt: 99,
      reactions: [{ emoji: "👍", participants: [self] }],
    });

    // 未ロードのmessageへのreactionはstateを触らない。
    const untouched = useMessaging.getState().messagesByPlace;
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-404",
      reactions: [{ emoji: "🎉", participants: [self] }],
    });
    expect(useMessaging.getState().messagesByPlace).toBe(untouched);

    // 切断中に落ちたreactionはcatch-upでreplayされない（seqが進まないので）。
    // caught_upでロード済みwindowを読み直して収束する。
    harness.history = [
      { ...message(1, "編集後", 99), reactions: [] },
      {
        ...message(2, "隣"),
        reactions: [{ emoji: "🎉", participants: [self] }],
      },
    ];
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(harness.fetches).toEqual([{ beforeSeq: 3, limit: 2 }]);
    const resynced = useMessaging.getState().messagesByPlace[placeKey] ?? [];
    expect(resynced[0]).toMatchObject({ content: "編集後", reactions: [] });
    expect(resynced[1]?.reactions).toEqual([
      { emoji: "🎉", participants: [self] },
    ]);

    // 変化がなければmessageの参照は保たれる（無駄な再描画を作らない）。
    const stable = useMessaging.getState().messagesByPlace[placeKey];
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(useMessaging.getState().messagesByPlace[placeKey]).toBe(stable);

    // loadOlderで200件を超えていても、最古のロード済みmessageまで
    // before_seqで遡り、切断中にreactionを落としたままにしない。
    const loaded = Array.from({ length: 205 }, (_, index) =>
      message(index + 1, `message ${index + 1}`),
    );
    harness.history = loaded.map((entry) =>
      entry.seq === 1
        ? {
            ...entry,
            reactions: [{ emoji: "👀", participants: [self] }],
          }
        : entry,
    );
    harness.fetches.length = 0;
    useMessaging.setState({ messagesByPlace: { [placeKey]: loaded } });
    harness.emit({ type: "caught_up", place });
    await harness.settle();

    expect(harness.fetches).toEqual([
      { beforeSeq: 206, limit: 200 },
      { beforeSeq: 6, limit: 5 },
    ]);
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "👀", participants: [self] }]);
  });

  it("re-reads each loaded range without traversing unloaded gaps", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;

    const loaded = [
      ...Array.from({ length: 50 }, (_, index) =>
        message(index + 1, `old ${index + 1}`),
      ),
      ...Array.from({ length: 50 }, (_, index) =>
        message(index + 951, `new ${index + 951}`),
      ),
    ];
    harness.history = loaded.map((entry) => {
      if (entry.seq === 1) {
        return {
          ...entry,
          reactions: [{ emoji: "👀", participants: [self] }],
        };
      }
      if (entry.seq === 951) {
        return {
          ...entry,
          reactions: [{ emoji: "🎉", participants: [self] }],
        };
      }
      return entry;
    });
    useMessaging.setState({ messagesByPlace: { [placeKey]: loaded } });

    harness.emit({ type: "caught_up", place });
    await harness.settle();

    expect(harness.fetches).toEqual([
      { beforeSeq: 1001, limit: 50 },
      { beforeSeq: 51, limit: 50 },
    ]);
    const resynced = useMessaging.getState().messagesByPlace[placeKey] ?? [];
    expect(resynced.find((entry) => entry.seq === 1)?.reactions).toEqual([
      { emoji: "👀", participants: [self] },
    ]);
    expect(resynced.find((entry) => entry.seq === 951)?.reactions).toEqual([
      { emoji: "🎉", participants: [self] },
    ]);
  });

  it("replays live reaction updates over an older resync snapshot", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "message")] },
    });
    harness.history = [message(1, "message")];
    harness.holdFetches = true;

    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(harness.heldFetchCount).toBe(1);

    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-1",
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    harness.releaseFetches();
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "👍", participants: [self] }]);
  });

  it("discards a reaction snapshot from an earlier session", async () => {
    const oldHarness = new StubBackend();
    installMessagingBackend(oldHarness);
    useMessaging.getState().init();
    await oldHarness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "old session")] },
    });
    oldHarness.history = [
      {
        ...message(1, "old session"),
        reactions: [{ emoji: "👀", participants: [self] }],
      },
    ];
    oldHarness.holdFetches = true;
    oldHarness.emit({ type: "caught_up", place });
    await oldHarness.settle();
    expect(oldHarness.heldFetchCount).toBe(1);

    bindMessagingSessionIdentity("reaction-test-new-session");
    const newHarness = new StubBackend();
    installMessagingBackend(newHarness);
    useMessaging.getState().init();
    await newHarness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: {
        [placeKey]: [
          {
            ...message(1, "new session"),
            reactions: [{ emoji: "🎉", participants: [self] }],
          },
        ],
      },
    });

    oldHarness.releaseFetches();
    await oldHarness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({
      content: "new session",
      reactions: [{ emoji: "🎉", participants: [self] }],
    });
  });
});

const self = { kind: "human", humanId: "human-1" } as const;

function message(
  seq: number,
  content: string,
  editedAt: number | null = null,
): Message {
  return {
    messageId: `message-${seq}`,
    place,
    seq,
    author: self,
    content,
    mentions: [],
    urgency: "normal",
    reactions: [],
    replyTo: null,
    createdAt: 1,
    editedAt,
    deleted: false,
  };
}

class StubBackend implements MessagingBackend {
  readonly capabilities = {
    status: false,
    replyLater: false,
    reactions: true,
  } as const;
  history: Message[] = [];
  readonly fetches: { beforeSeq?: number; limit?: number }[] = [];
  holdFetches = false;
  private heldFetches: {
    response: Message[];
    resolve: (messages: Message[]) => void;
  }[] = [];
  private listener: ((event: ServerEvent) => void) | null = null;
  private resolveBootstrapped!: () => void;
  readonly bootstrapped = new Promise<void>((resolve) => {
    this.resolveBootstrapped = resolve;
  });

  emit(event: ServerEvent): void {
    this.listener?.(event);
  }

  get heldFetchCount(): number {
    return this.heldFetches.length;
  }

  releaseFetches(): void {
    for (const fetch of this.heldFetches.splice(0)) {
      fetch.resolve(fetch.response);
    }
  }

  /** resyncのawait chain（fetch → set）を流し切る。 */
  async settle(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    queueMicrotask(() => this.resolveBootstrapped());
    return {
      self,
      workspaces: [],
      channels: [
        {
          channelId: "channel-1",
          workspaceId: "workspace-1",
          name: "general",
          topic: "",
          visibility: "public",
        },
      ],
      dms: [],
      members: [],
      statuses: [],
      readMarkers: [],
      unreadSummaries: [],
      replyLaterMarkers: [],
      employedAgents: [],
    };
  }

  async fetchMessages(
    _place: Place,
    options: { beforeSeq?: number; limit?: number } = {},
  ): Promise<Message[]> {
    this.fetches.push(options);
    const beforeSeq = options.beforeSeq ?? Number.POSITIVE_INFINITY;
    const limit = options.limit ?? 50;
    const eligible = this.history.filter((entry) => entry.seq < beforeSeq);
    const response = eligible.slice(Math.max(0, eligible.length - limit));
    if (!this.holdFetches) return response;
    return new Promise((resolve) => {
      this.heldFetches.push({ response, resolve });
    });
  }

  sendMessage = vi.fn();
  editMessage = vi.fn(async () => undefined);
  deleteMessage = vi.fn(async () => undefined);
  markRead = vi.fn(async () => undefined);
  setStatus = vi.fn(async () => undefined);
  createReplyLater = vi.fn(async () => undefined);
  resolveReplyLater = vi.fn(async () => undefined);
  toggleReaction = vi.fn(async () => undefined);
  sendTyping = vi.fn();

  subscribe(listener: (event: ServerEvent) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }

  subscribeConnection(_listener: (state: ConnectionState) => void): () => void {
    return () => undefined;
  }

  dispose(): void {
    this.listener = null;
  }
}
