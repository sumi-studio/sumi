import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  Message,
  MessagingBackend,
  Place,
  PlaceKey,
  SendMessageInput,
  SendReceipt,
  ServerEvent,
  ThreadSummary,
} from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

type BootstrapSnapshot = Awaited<ReturnType<MessagingBackend["bootstrap"]>>;

const SELF = { kind: "human", humanId: "human-a" } as const;
const OTHER = { kind: "human", humanId: "human-b" } as const;

function channel(channelId: string, topic: string): ChannelSummary {
  return {
    channelId,
    workspaceId: "workspace-1",
    name: channelId,
    topic,
    visibility: "public",
    voice: false,
  };
}

function dm(dmId: string): DmSummary {
  return { dmId, kind: "dm", participants: [SELF, OTHER] };
}

function thread(threadId: string): ThreadSummary {
  return {
    threadId,
    workspaceId: "workspace-1",
    parentPlace: { kind: "channel", channelId: "channel-1" },
    parentMessageId: "message-1",
    name: threadId,
    messageCount: 1,
    lastMessageAt: 1,
    lastMessage: "参加してください",
    participants: [SELF, OTHER],
    latestSeq: 1,
  };
}

function place(key: PlaceKey): Place {
  const [kind, id] = key.split(":");
  return kind === "channel"
    ? { kind: "channel", channelId: id }
    : kind === "thread"
      ? { kind: "thread", threadId: id }
      : { kind: kind as "dm" | "group_dm", dmId: id };
}

function threadMessage(threadId: string, seq: number): Message {
  return {
    messageId: `message-${threadId}-${seq}`,
    place: { kind: "thread", threadId },
    seq,
    author: OTHER,
    content: "新しい返信です",
    mentions: [SELF],
    urgency: "normal",
    reactions: [],
    attachments: [],
    replyTo: null,
    createdAt: seq,
    editedAt: null,
    deleted: false,
  };
}

function snapshot(options: {
  channels: ChannelSummary[];
  dms: DmSummary[];
  unread: Record<PlaceKey, { latest: number; unread: number; mention: number }>;
  lastRead?: Record<PlaceKey, number>;
  threads?: ThreadSummary[];
}): BootstrapSnapshot {
  const keys = Object.keys(options.unread) as PlaceKey[];
  return {
    self: SELF,
    workspaces: [{ workspaceId: "workspace-1", name: "Sumi" }],
    channels: options.channels,
    dms: options.dms,
    threads: options.threads,
    members: [
      { participant: SELF, displayName: "Yohaku", tagline: "" },
      { participant: OTHER, displayName: "Aoi", tagline: "" },
    ],
    statuses: [],
    readMarkers: keys.map((key) => ({
      place: place(key),
      lastReadSeq: options.lastRead?.[key] ?? 0,
    })),
    unreadSummaries: keys.map((key) => ({
      place: place(key),
      latestSeq: options.unread[key].latest,
      unreadCount: options.unread[key].unread,
      mentionCount: options.unread[key].mention,
    })),
    replyLaterMarkers: [],
    notificationSetting: {
      owner: SELF,
      defaults: { level: "all" },
      perPlace: [],
      keywords: [],
    },
    employedAgents: [],
  };
}

/** connectionの上げ下げを外から駆動できる最小backend。 */
class FakeBackend implements MessagingBackend {
  readonly capabilities = {
    status: false,
    replyLater: false,
    reactions: false,
    notifications: false,
    threads: true,
  } as const;
  next: BootstrapSnapshot;
  bootstrapCalls = 0;
  readonly cursorCalls: (Record<PlaceKey, number> | undefined)[] = [];
  readonly listeners = new Set<(event: ServerEvent) => void>();
  private connectionListener: ((state: ConnectionState) => void) | null = null;

  constructor(initial: BootstrapSnapshot) {
    this.next = initial;
  }

  async bootstrap(): Promise<BootstrapSnapshot> {
    this.bootstrapCalls += 1;
    return this.next;
  }

  fetchMessages = vi.fn(
    async (
      _place: Place,
      _options?: { beforeSeq?: number; limit?: number },
    ): Promise<Message[]> => [],
  );
  async searchMessages() {
    return [];
  }
  fetchThread = vi.fn(async (_threadId: string): Promise<ThreadSummary> => {
    throw new Error("unused");
  });
  fetchThreads = vi.fn(async (_parent: Place): Promise<ThreadSummary[]> => []);
  createThread = vi.fn(
    async (
      _parent: Place,
      _name: string,
      _originMessageId: string | null,
      _clientNonce: string,
    ): Promise<ThreadSummary> => {
      throw new Error("unused");
    },
  );
  async fetchPresence(): ReturnType<MessagingBackend["fetchPresence"]> {
    return { statuses: [], replyLaterMarkers: [] };
  }
  async createChannel(): Promise<ChannelSummary> {
    throw new Error("unused");
  }
  async ensureDM(): Promise<DmSummary> {
    throw new Error("unused");
  }
  async createGroupDM(): Promise<DmSummary> {
    throw new Error("unused");
  }
  async updateChannelTopic(): Promise<ChannelSummary> {
    throw new Error("unused");
  }
  async uploadAttachment(): Promise<never> {
    throw new Error("uploadAttachment is not part of this test");
  }
  attachmentURL(attachmentId: string): string {
    return `/test/attachments/${attachmentId}`;
  }
  sendMessage = vi.fn(
    async (input: SendMessageInput): Promise<SendReceipt> => ({
      clientNonce: input.clientNonce,
      messageId: "unused",
      seq: 1,
      created: true,
    }),
  );
  async editMessage() {}
  async deleteMessage() {}
  async markRead() {}
  async setStatus(): ReturnType<MessagingBackend["setStatus"]> {
    throw new Error("unused");
  }
  async createReplyLater(): ReturnType<MessagingBackend["createReplyLater"]> {
    throw new Error("unused");
  }
  async resolveReplyLater(): ReturnType<MessagingBackend["resolveReplyLater"]> {
    throw new Error("unused");
  }
  async toggleReaction() {
    return { messageId: "unused", reactions: [] };
  }
  async setNotificationSetting(): ReturnType<
    MessagingBackend["setNotificationSetting"]
  > {
    throw new Error("unused");
  }
  sendTyping() {}

  subscribe(
    listener: (event: ServerEvent) => void,
    options: { sinceByPlace?: Record<PlaceKey, number> } = {},
  ): () => void {
    this.listeners.add(listener);
    this.cursorCalls.push(options.sinceByPlace);
    return () => {
      this.listeners.delete(listener);
    };
  }

  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    this.connectionListener = listener;
    listener("reconnecting");
    return () => {
      this.connectionListener = null;
    };
  }

  dispose(): void {
    this.listeners.clear();
    this.connectionListener = null;
  }

  emitConnection(state: ConnectionState): void {
    this.connectionListener?.(state);
  }

  emit(event: ServerEvent): void {
    for (const listener of this.listeners) listener(event);
  }
}

/** initとreconcileはmicrotask境界を跨ぐので、決着まで待つ。 */
async function settle(): Promise<void> {
  for (let i = 0; i < 8; i += 1) await Promise.resolve();
}

const CHANNEL_1: PlaceKey = "channel:channel-1";
const CHANNEL_2: PlaceKey = "channel:channel-2";
const DM_9: PlaceKey = "dm:dm-9";
const THREAD_LIVE: PlaceKey = "thread:thread-live";
const THREAD_KNOWN: PlaceKey = "thread:thread-known";

const FIRST = snapshot({
  channels: [channel("channel-1", "旧トピック")],
  dms: [],
  unread: { [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 } },
});

let backend: FakeBackend;

describe("place lifecycleの再接続突き合わせ", () => {
  beforeEach(async () => {
    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-a");
    backend = new FakeBackend(FIRST);
    installMessagingBackend(backend);
    useMessaging.getState().init();
    await settle();
  });

  afterEach(() => bindMessagingSessionIdentity(null));

  it("最初のconnectedでもbootstrap-to-subscribe gapを再検証する", async () => {
    backend.emitConnection("connected");
    await settle();
    expect(backend.bootstrapCalls).toBe(2);
    expect(useMessaging.getState().ready).toBe(true);
  });

  it("初回接続の継ぎ目で作られた非参加threadを親一覧へ取り込む", async () => {
    const beforeConnect = thread("thread-before-connect");
    const createdInGap = thread("thread-created-in-gap");
    backend.fetchThreads.mockResolvedValueOnce([beforeConnect]);
    await useMessaging.getState().loadThreads(CHANNEL_1);
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);

    // The bootstrap projection remains participation-scoped, so only the
    // parent list fetched after the first hello ACK can reveal this thread.
    backend.fetchThreads.mockResolvedValueOnce([beforeConnect, createdInGap]);
    backend.emitConnection("connected");
    await settle();

    expect(backend.bootstrapCalls).toBe(2);
    expect(backend.fetchThreads).toHaveBeenCalledTimes(2);
    expect(useMessaging.getState().threadsById).toMatchObject({
      [beforeConnect.threadId]: beforeConnect,
      [createdInGap.threadId]: createdInGap,
    });
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);
  });

  it("liveイベントで初めて知ったthreadを一度だけhydrateして選べる", async () => {
    const live = thread("thread-live");
    backend.fetchThread.mockResolvedValue(live);

    backend.emit({
      type: "message_created",
      message: threadMessage(live.threadId, 2),
      notify: null,
    });
    await settle();

    expect(backend.fetchThread).toHaveBeenCalledTimes(1);
    expect(backend.fetchThread).toHaveBeenCalledWith(live.threadId);
    expect(useMessaging.getState().threadsById[live.threadId]).toEqual(live);

    useMessaging.getState().selectPlace(THREAD_LIVE);
    expect(useMessaging.getState().activePlaceKey).toBe(THREAD_LIVE);
  });

  it("URLで開いたbootstrap未取得threadでもbackendのthisを保つ", async () => {
    const unopened = thread("thread-direct-link");
    backend.fetchThread.mockImplementation(async function (this: FakeBackend) {
      // ApiMessagingBackend.fetchThread also calls this.request(...).
      expect(this).toBe(backend);
      return unopened;
    });

    await expect(
      useMessaging.getState().loadThread(unopened.threadId),
    ).resolves.toBe(true);
    expect(backend.fetchThread).toHaveBeenCalledWith(unopened.threadId);
    expect(useMessaging.getState().threadsById[unopened.threadId]).toEqual(
      unopened,
    );
  });

  it("未知threadのhydrate中の新活動で古いGET summaryを戻さない", async () => {
    const stale = thread("thread-hydration-race");
    let resolveFetch!: (summary: ThreadSummary) => void;
    const fresh = {
      ...stale,
      messageCount: 3,
      latestSeq: 3,
      lastMessageAt: 3,
      lastMessage: "新しい返信です",
    };
    backend.fetchThread
      .mockImplementationOnce(
        () =>
          new Promise<ThreadSummary>((resolve) => {
            resolveFetch = resolve;
          }),
      )
      .mockResolvedValueOnce(fresh);

    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 2),
      notify: null,
    });
    await settle();
    expect(backend.fetchThread).toHaveBeenCalledWith(stale.threadId);

    // The GET snapshot was already taken. A later event advances the
    // projection before that stale response can be applied.
    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 3),
      notify: null,
    });
    resolveFetch(stale);
    await settle();

    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);
    expect(backend.fetchThread).toHaveBeenCalledTimes(2);
  });

  it("重複したthread message_createdを一度の権威あるsummary取得に合流する", async () => {
    const known = thread("thread-known");
    useMessaging.setState({ threadsById: { [known.threadId]: known } });
    const incoming = threadMessage(known.threadId, 2);
    const aggregate = {
      ...known,
      messageCount: known.messageCount + 1,
      latestSeq: incoming.seq,
      lastMessageAt: incoming.createdAt,
      lastMessage: incoming.content,
    };
    backend.fetchThread.mockResolvedValue(aggregate);

    backend.emit({ type: "message_created", message: incoming, notify: null });
    backend.emit({ type: "message_created", message: incoming, notify: null });
    await settle();

    expect(backend.fetchThread).toHaveBeenCalledTimes(1);
    expect(useMessaging.getState().threadsById[known.threadId]).toEqual(
      aggregate,
    );
  });

  it("順不同のthread作成イベントでもserver aggregateの件数と参加者を採用する", async () => {
    const known = thread("thread-authoritative");
    const mentioned = { kind: "human", humanId: "human-c" } as const;
    const aggregate = {
      ...known,
      messageCount: 3,
      latestSeq: 3,
      lastMessageAt: 3,
      participants: [...known.participants, mentioned],
    };
    backend.fetchThread.mockResolvedValue(aggregate);
    useMessaging.setState({ threadsById: { [known.threadId]: known } });

    backend.emit({
      type: "message_created",
      message: threadMessage(known.threadId, 3),
      notify: null,
    });
    backend.emit({
      type: "message_created",
      message: threadMessage(known.threadId, 2),
      notify: null,
    });
    await settle();

    expect(useMessaging.getState().threadsById[known.threadId]).toEqual(
      aggregate,
    );
  });

  it("遅れて届いたthread作成eventで新しいserver aggregateを巻き戻さない", async () => {
    const stale = thread("thread-late-create");
    const fresh = {
      ...stale,
      messageCount: 2,
      latestSeq: 2,
      lastMessageAt: 2,
      lastMessage: "作成後の返信です",
      participants: [
        ...stale.participants,
        { kind: "human", humanId: "human-c" } as const,
      ],
    };
    useMessaging.setState({ threadsById: { [stale.threadId]: stale } });
    backend.fetchThread.mockResolvedValue(fresh);

    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 2),
      notify: null,
    });
    await settle();
    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);

    backend.emit({ type: "place_created", thread: stale });
    // place_created is synchronous; a stale write here would be visible even
    // if the follow-up authoritative GET happened to complete immediately.
    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);
    await settle();

    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);
  });

  it("作成応答がlive refresh済みのthread aggregateを巻き戻さない", async () => {
    const stale = thread("thread-create-response-race");
    const fresh = {
      ...stale,
      messageCount: 2,
      latestSeq: 2,
      lastMessageAt: 2,
      lastMessage: "作成応答より新しい返信です",
    };
    let resolveCreate!: (summary: ThreadSummary) => void;
    backend.createThread.mockImplementationOnce(
      () =>
        new Promise<ThreadSummary>((resolve) => {
          resolveCreate = resolve;
        }),
    );
    backend.fetchThread.mockResolvedValue(fresh);

    const creating = useMessaging
      .getState()
      .createThread(
        CHANNEL_1,
        stale.name,
        stale.parentMessageId,
        "create-race",
      );
    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 2),
      notify: null,
    });
    await settle();
    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);

    resolveCreate(stale);
    await expect(creating).resolves.toBe(`thread:${stale.threadId}`);
    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);
  });

  it("再接続bootstrapがlive refresh済みのthread aggregateを巻き戻さない", async () => {
    const stale = thread("thread-reconnect-bootstrap-race");
    const fresh = {
      ...stale,
      messageCount: 2,
      latestSeq: 2,
      lastMessageAt: 2,
      lastMessage: "bootstrapより新しい返信です",
    };
    useMessaging.setState({ threadsById: { [stale.threadId]: stale } });
    let resolveBootstrap!: (snapshot: BootstrapSnapshot) => void;
    backend.bootstrap = vi.fn(
      () =>
        new Promise<BootstrapSnapshot>((resolve) => {
          resolveBootstrap = resolve;
        }),
    );
    backend.fetchThread.mockResolvedValue(fresh);

    backend.emitConnection("connected");
    await Promise.resolve();
    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 2),
      notify: null,
    });
    await settle();
    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);

    resolveBootstrap(
      snapshot({
        channels: [channel("channel-1", "旧トピック")],
        dms: [],
        threads: [stale],
        unread: { [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 } },
      }),
    );
    await settle();

    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);
  });

  it("thread summary再取得が一度失敗しても親一覧を開き直せば回復する", async () => {
    const stale = thread("thread-refresh-retry");
    const fresh = {
      ...stale,
      messageCount: 2,
      latestSeq: 2,
      lastMessageAt: 2,
      lastMessage: "再取得後の返信です",
    };
    useMessaging.setState({
      threadsById: { [stale.threadId]: stale },
      threadsLoadedForPlace: { [CHANNEL_1]: true },
    });
    backend.fetchThread.mockRejectedValueOnce(new Error("timeout"));

    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 2),
      notify: null,
    });
    await settle();

    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(
      false,
    );
    backend.fetchThreads.mockResolvedValueOnce([fresh]);
    await useMessaging.getState().loadThreads(CHANNEL_1);

    expect(useMessaging.getState().threadsById[stale.threadId]).toEqual(fresh);
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);
  });

  it("一覧の1件がfenceで弾かれても残りのthreadを取り込む", async () => {
    const raced = thread("thread-raced");
    const other = thread("thread-other");
    const fresh = {
      ...raced,
      messageCount: 2,
      latestSeq: 5,
      lastMessageAt: 5,
      lastMessage: "一覧の取得中に届いた返信です",
    };
    useMessaging.setState({ threadsById: { [raced.threadId]: raced } });
    let resolveList!: (threads: ThreadSummary[]) => void;
    backend.fetchThreads.mockImplementationOnce(
      () =>
        new Promise<ThreadSummary[]>((resolve) => {
          resolveList = resolve;
        }),
    );
    const loading = useMessaging.getState().loadThreads(CHANNEL_1);
    await settle();

    // 一覧のsnapshotを撮ったあとに、その1件だけ新しい権威ある投影が始まる。
    backend.fetchThread.mockResolvedValue(fresh);
    backend.emit({
      type: "message_created",
      message: threadMessage(raced.threadId, 5),
      notify: null,
    });
    resolveList([raced, other]);
    await loading;
    await settle();

    // 追い越された1件はGET結果が勝つ。残りはその巻き添えにしない。
    expect(useMessaging.getState().threadsById[raced.threadId]).toEqual(fresh);
    expect(useMessaging.getState().threadsById[other.threadId]).toEqual(other);
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);
  });

  it("live echoを取り逃してACKだけで確定した送信でもsummaryを取り直す", async () => {
    const known = thread("thread-ack-only");
    const refreshed = {
      ...known,
      messageCount: 2,
      latestSeq: 2,
      lastMessageAt: 2,
      lastMessage: "送りました",
    };
    const committed: Message = {
      ...threadMessage(known.threadId, 2),
      messageId: "message-ack-only",
      author: SELF,
      content: "送りました",
      mentions: [],
    };
    useMessaging.setState({
      threadsById: { [known.threadId]: known },
      threadsLoadedForPlace: { [CHANNEL_1]: true },
    });
    backend.sendMessage.mockResolvedValueOnce({
      clientNonce: "unused",
      messageId: committed.messageId,
      seq: committed.seq,
      created: true,
    });
    // echoが落ちた送信は、ACKのseqで取り直したときにだけ履歴へ現れる。
    backend.fetchMessages.mockImplementation(async (_place, options) =>
      options?.beforeSeq === committed.seq + 1 ? [committed] : [],
    );
    backend.fetchThread.mockResolvedValue(refreshed);

    useMessaging.getState().selectPlace(`thread:${known.threadId}` as PlaceKey);
    useMessaging.getState().send("送りました", "normal");
    await settle();
    await settle();

    // 自分の発言でthreadは進んでいる。親一覧に古い件数を残さない。
    expect(backend.fetchThread).toHaveBeenCalledWith(known.threadId);
    expect(useMessaging.getState().threadsById[known.threadId]).toEqual(
      refreshed,
    );
  });

  it("bootstrapが先に取り込んだthread messageをcatch-upで再加算しない", async () => {
    const known = thread("thread-known");
    const incoming = threadMessage(known.threadId, 2);
    useMessaging.setState({ threadsById: { [known.threadId]: known } });
    backend.next = snapshot({
      channels: [channel("channel-1", "旧トピック")],
      dms: [],
      threads: [
        {
          ...known,
          messageCount: known.messageCount + 1,
          latestSeq: incoming.seq,
          lastMessageAt: incoming.createdAt,
          lastMessage: incoming.content,
        },
      ],
      unread: { [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 } },
    });

    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();
    backend.emit({ type: "message_created", message: incoming, notify: null });
    await settle();

    expect(useMessaging.getState().threadsById[known.threadId]).toMatchObject({
      messageCount: known.messageCount + 1,
      latestSeq: incoming.seq,
    });
  });

  it("bootstrap済みのthread tombstoneをcatch-upで二重減算しない", async () => {
    const known = { ...thread("thread-known"), messageCount: 2 };
    const tombstone = {
      ...threadMessage(known.threadId, 2),
      content: "",
      deleted: true,
      mentions: [],
    };
    useMessaging.setState({ threadsById: { [known.threadId]: known } });
    backend.next = snapshot({
      channels: [channel("channel-1", "旧トピック")],
      dms: [],
      threads: [
        {
          ...known,
          messageCount: 1,
          latestSeq: tombstone.seq,
          lastMessageAt: null,
          lastMessage: "",
        },
      ],
      unread: { [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 } },
    });

    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();
    expect(useMessaging.getState().threadsById[known.threadId]).toMatchObject({
      messageCount: 1,
      latestSeq: tombstone.seq,
    });

    // The summary re-fetch may fail while offline. The bootstrap count remains
    // authoritative until a later successful refresh, rather than decrementing
    // a second time when the catch-up tombstone is replayed.
    backend.fetchThread.mockRejectedValueOnce(new Error("offline"));
    backend.emit({
      type: "message_created",
      message: tombstone,
      notify: null,
    });
    await settle();

    expect(useMessaging.getState().threadsById[known.threadId]).toMatchObject({
      messageCount: 1,
      latestSeq: tombstone.seq,
    });
  });

  it("切断中に作られたplaceとtopic編集を再接続で取り込む", async () => {
    useMessaging.getState().selectPlace(CHANNEL_1);
    useMessaging.getState().setDraft(CHANNEL_1, "書きかけ");
    // 切断前にここまで読んだ。serverのread markerはまだ追いついていない。
    useMessaging.getState().noteReadUpTo(CHANNEL_1, 5);
    await settle();

    backend.next = snapshot({
      channels: [channel("channel-1", "新トピック"), channel("channel-2", "")],
      dms: [dm("dm-9")],
      unread: {
        [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 },
        [CHANNEL_2]: { latest: 2, unread: 2, mention: 0 },
        [DM_9]: { latest: 4, unread: 4, mention: 0 },
      },
    });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    const state = useMessaging.getState();
    expect(backend.bootstrapCalls).toBe(2);
    expect(state.channels.map((entry) => entry.channelId)).toEqual([
      "channel-1",
      "channel-2",
    ]);
    expect(state.channels[0]?.topic).toBe("新トピック");
    expect(state.dms.map((entry) => entry.dmId)).toEqual(["dm-9"]);
    // 切断中に届いていた分は、新しく見つかったplaceにだけ未読として載る。
    expect(state.unreadCountByPlace[CHANNEL_2]).toBe(2);
    expect(state.unreadCountByPlace[DM_9]).toBe(4);
    // 次の切断でもこのplaceがreplay対象になるようcursorを登録する。
    expect(backend.cursorCalls.at(-1)).toEqual({
      [CHANNEL_2]: 2,
      [DM_9]: 4,
    });
    expect(backend.listeners.size).toBe(1);
  });

  it("再接続snapshotで切断中に参加したthreadを既知threadを残して取り込む", async () => {
    useMessaging.setState({
      threadsById: { "thread-known": thread("thread-known") },
    });
    backend.next = snapshot({
      channels: [channel("channel-1", "旧トピック")],
      dms: [],
      threads: [thread("thread-admitted-offline")],
      unread: { [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 } },
    });

    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    expect(useMessaging.getState().threadsById).toMatchObject({
      "thread-known": { threadId: "thread-known" },
      "thread-admitted-offline": { threadId: "thread-admitted-offline" },
    });
  });

  it("切断中に作られた非参加threadを再接続後に親一覧から取り込む", async () => {
    const existing = thread("thread-existing");
    const offline = thread("thread-created-offline");
    backend.fetchThreads.mockResolvedValueOnce([existing]);
    await useMessaging.getState().loadThreads(CHANNEL_1);
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);

    // Bootstrap carries only participating threads, so this new thread must
    // come from the parent list fetch that reconnect invalidates and repeats.
    backend.fetchThreads.mockResolvedValueOnce([existing, offline]);
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    expect(backend.fetchThreads).toHaveBeenCalledTimes(2);
    expect(useMessaging.getState().threadsById).toMatchObject({
      [existing.threadId]: existing,
      [offline.threadId]: offline,
    });
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);
  });

  it("進行中の未読・既読・ローカルstateを突き合わせで壊さない", async () => {
    useMessaging.getState().selectPlace(CHANNEL_1);
    useMessaging.getState().setDraft(CHANNEL_1, "書きかけ");
    useMessaging.getState().noteReadUpTo(CHANNEL_1, 5);
    await settle();
    const before = useMessaging.getState();
    expect(before.lastReadByPlace[CHANNEL_1]).toBe(5);
    expect(before.unreadCountByPlace[CHANNEL_1]).toBe(0);

    // serverのsnapshotはまだ既読前（未読3・メンション1、read marker 0）。
    backend.next = snapshot({
      channels: [channel("channel-1", "新トピック"), channel("channel-2", "")],
      dms: [],
      unread: {
        [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 },
        [CHANNEL_2]: { latest: 2, unread: 2, mention: 0 },
      },
    });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    const state = useMessaging.getState();
    expect(state.lastReadByPlace[CHANNEL_1]).toBe(5);
    expect(state.unreadCountByPlace[CHANNEL_1]).toBe(0);
    expect(state.mentionCountByPlace[CHANNEL_1]).toBe(0);
    expect(state.draftByPlace[CHANNEL_1]).toBe("書きかけ");
    expect(state.activePlaceKey).toBe(CHANNEL_1);
    expect(state.unreadLineByPlace[CHANNEL_1]).toBe(0);
    expect(state.messagesByPlace[CHANNEL_1]).toEqual([]);
  });

  it("既知threadの既読・未読を再接続snapshotで巻き戻さない", async () => {
    useMessaging.setState((state) => ({
      threadsById: { "thread-known": thread("thread-known") },
      lastReadByPlace: { ...state.lastReadByPlace, [THREAD_KNOWN]: 5 },
      unreadCountByPlace: { ...state.unreadCountByPlace, [THREAD_KNOWN]: 0 },
      mentionCountByPlace: { ...state.mentionCountByPlace, [THREAD_KNOWN]: 0 },
    }));
    backend.next = snapshot({
      channels: [channel("channel-1", "旧トピック")],
      dms: [],
      threads: [thread("thread-known")],
      unread: {
        [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 },
        [THREAD_KNOWN]: { latest: 5, unread: 3, mention: 1 },
      },
      lastRead: { [CHANNEL_1]: 0, [THREAD_KNOWN]: 0 },
    });

    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    const state = useMessaging.getState();
    expect(state.lastReadByPlace[THREAD_KNOWN]).toBe(5);
    expect(state.unreadCountByPlace[THREAD_KNOWN]).toBe(0);
    expect(state.mentionCountByPlace[THREAD_KNOWN]).toBe(0);
    expect(backend.cursorCalls).toHaveLength(1);
  });

  it("再接続で現れたplaceをURL経由でも選べる", async () => {
    // 切断中に作られたplaceのURLを踏んでも、その時点では未知なので選べない。
    useMessaging.getState().selectPlace(CHANNEL_2);
    expect(useMessaging.getState().activePlaceKey).toBeNull();

    backend.next = snapshot({
      channels: [channel("channel-1", "旧トピック"), channel("channel-2", "")],
      dms: [],
      unread: {
        [CHANNEL_1]: { latest: 5, unread: 3, mention: 1 },
        [CHANNEL_2]: { latest: 0, unread: 0, mention: 0 },
      },
    });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    useMessaging.getState().selectPlace(CHANNEL_2);
    expect(useMessaging.getState().activePlaceKey).toBe(CHANNEL_2);
  });

  it("bootstrapが失敗しても既知のplaceを落とさない", async () => {
    backend.bootstrap = async () => {
      backend.bootstrapCalls += 1;
      throw new Error("offline");
    };
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    const state = useMessaging.getState();
    expect(backend.bootstrapCalls).toBe(2);
    expect(state.channels.map((entry) => entry.channelId)).toEqual([
      "channel-1",
    ]);
    expect(state.connection).toBe("connected");
  });
});
