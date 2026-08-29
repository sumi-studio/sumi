import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MessagingAPIError } from "./api-backend";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  Message,
  MessagingBackend,
  Place,
  PlaceKey,
  ReactionMutationResult,
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

function channel(
  channelId: string,
  topic: string,
  revision = 1,
): ChannelSummary {
  return {
    channelId,
    workspaceId: "workspace-1",
    revision,
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
    revision: 1,
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

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  return { promise, resolve, reject };
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
  async updateChannel(): Promise<ChannelSummary> {
    throw new Error("unused");
  }
  async duplicateChannel(): Promise<ChannelSummary> {
    throw new Error("unused");
  }
  async uploadAttachment(): Promise<never> {
    throw new Error("uploadAttachment is not part of this test");
  }

  async updateDraftAttachment(): Promise<never> {
    throw new Error("updateDraftAttachment is not part of this test");
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
  async editMessage(): ReturnType<MessagingBackend["editMessage"]> {
    throw new Error("unused");
  }
  async deleteMessage(): ReturnType<MessagingBackend["deleteMessage"]> {
    throw new Error("unused");
  }
  markRead = vi.fn(async () => undefined);
  async setStatus(): ReturnType<MessagingBackend["setStatus"]> {
    throw new Error("unused");
  }
  async createReplyLater(): ReturnType<MessagingBackend["createReplyLater"]> {
    throw new Error("unused");
  }
  async resolveReplyLater(): ReturnType<MessagingBackend["resolveReplyLater"]> {
    throw new Error("unused");
  }
  async toggleReaction(): Promise<ReactionMutationResult> {
    return { messageId: "unused", reactions: [] };
  }
  async setNotificationSetting(): ReturnType<
    MessagingBackend["setNotificationSetting"]
  > {
    throw new Error("unused");
  }
  sendTyping() {}
  openPlace = vi.fn((_place: Place | null, _sinceSeq?: number): void => {});
  releasePlace = vi.fn((_place: Place): void => {});

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

  it("direct thread load failure remains visible until a retry recovers", async () => {
    const unopened = thread("thread-retry-after-failure");
    backend.fetchThread
      .mockRejectedValueOnce(new Error("temporary network failure"))
      .mockResolvedValueOnce(unopened);

    await expect(
      useMessaging.getState().loadThread(unopened.threadId),
    ).resolves.toBe(false);
    expect(
      useMessaging.getState().threadLoadErrorsById[unopened.threadId],
    ).toBe("failed");

    await expect(
      useMessaging.getState().loadThread(unopened.threadId),
    ).resolves.toBe(true);
    expect(
      useMessaging.getState().threadLoadErrorsById[unopened.threadId],
    ).toBeUndefined();
    expect(useMessaging.getState().threadsById[unopened.threadId]).toEqual(
      unopened,
    );
  });

  it("未知threadのhydrate中の新活動で古いGET summaryを戻さない", async () => {
    const stale = thread("thread-hydration-race");
    let resolveFetch!: (summary: ThreadSummary) => void;
    const fresh = {
      ...stale,
      revision: 3,
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

  it("未知threadのstale hydrate後のauthoritative refresh失敗をloadingのままにしない", async () => {
    const stale = thread("thread-hydration-refresh-failure");
    const initial = deferred<ThreadSummary>();
    backend.fetchThread
      .mockReturnValueOnce(initial.promise)
      .mockRejectedValueOnce(new Error("authoritative refresh failed"))
      .mockRejectedValue(new Error("retry refresh failed"));

    const loading = useMessaging.getState().loadThread(stale.threadId);
    backend.emit({
      type: "message_created",
      message: threadMessage(stale.threadId, 2),
      notify: null,
    });
    await settle();
    initial.resolve(stale);

    await expect(loading).resolves.toBe(false);
    await settle();
    expect(useMessaging.getState().threadsById[stale.threadId]).toBeUndefined();
    expect(useMessaging.getState().threadLoadErrorsById[stale.threadId]).toBe(
      "failed",
    );
  });

  it("重複したthread message_createdを一度の権威あるsummary取得に合流する", async () => {
    const known = thread("thread-known");
    useMessaging.setState({ threadsById: { [known.threadId]: known } });
    const incoming = threadMessage(known.threadId, 2);
    const aggregate = {
      ...known,
      revision: 2,
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

  it("履歴を保持していないplaceでもcreated・edited・replay重複を一度だけ未読にする", () => {
    const created: Message = {
      ...threadMessage("unused", 6),
      place: { kind: "channel", channelId: "channel-1" },
      messageId: "message-unheld-dedupe",
      mentions: [SELF],
    };

    backend.emit({ type: "message_created", message: created, notify: null });
    expect(useMessaging.getState().messagesByPlace[CHANNEL_1]).toBeUndefined();
    expect(useMessaging.getState().unreadCountByPlace[CHANNEL_1]).toBe(4);
    expect(useMessaging.getState().mentionCountByPlace[CHANNEL_1]).toBe(2);

    backend.emit({
      type: "message_edited",
      message: { ...created, content: "編集後の本文", editedAt: 7 },
    });
    backend.emit({ type: "message_created", message: created, notify: null });

    expect(useMessaging.getState().unreadCountByPlace[CHANNEL_1]).toBe(4);
    expect(useMessaging.getState().mentionCountByPlace[CHANNEL_1]).toBe(2);
  });

  it("順不同のthread作成イベントでもserver aggregateの件数と参加者を採用する", async () => {
    const known = thread("thread-authoritative");
    const mentioned = { kind: "human", humanId: "human-c" } as const;
    const aggregate = {
      ...known,
      revision: 3,
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
      revision: 2,
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
      revision: 2,
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
      .createThread(CHANNEL_1, stale.name, stale.parentMessageId);
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
      revision: 2,
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
      revision: 2,
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
      revision: 2,
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

  it("親一覧の取得中に届いたthread_createdをsnapshotの後に取り込む", async () => {
    const listed = thread("thread-listed-before-response");
    const created = {
      ...thread("thread-created-during-list"),
      participants: [OTHER],
    };
    let resolveList!: (threads: ThreadSummary[]) => void;
    backend.fetchThreads.mockImplementationOnce(
      () =>
        new Promise<ThreadSummary[]>((resolve) => {
          resolveList = resolve;
        }),
    );

    const loading = useMessaging.getState().loadThreads(CHANNEL_1);
    await settle();
    backend.emit({ type: "place_created", thread: created });

    // The event is held behind the still-in-flight snapshot, rather than
    // being lost because this parent is not loaded yet.
    expect(
      useMessaging.getState().threadsById[created.threadId],
    ).toBeUndefined();
    resolveList([listed]);
    await loading;

    expect(useMessaging.getState().threadsById).toMatchObject({
      [listed.threadId]: listed,
      [created.threadId]: created,
    });
    expect(useMessaging.getState().threadsLoadedForPlace[CHANNEL_1]).toBe(true);
  });

  it("live echoを取り逃してACKだけで確定した送信でもsummaryを取り直す", async () => {
    const known = thread("thread-ack-only");
    const refreshed = {
      ...known,
      revision: 2,
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

  it.each([
    "success",
    "conflict",
  ] as const)("live echoを取り逃したedit %sでもpreviewとparticipantsを取り直す", async (disposition) => {
    const known = {
      ...thread(`thread-edit-${disposition}`),
      participants: [SELF],
    };
    const key = `thread:${known.threadId}` as PlaceKey;
    const target: Message = {
      ...threadMessage(known.threadId, 1),
      author: SELF,
      content: "編集前",
      mentions: [],
      revision: 1,
    };
    const committed: Message = {
      ...target,
      content: "編集後",
      mentions: [OTHER],
      revision: 2,
    };
    const refreshed: ThreadSummary = {
      ...known,
      revision: 2,
      lastMessage: committed.content,
      participants: [SELF, OTHER],
    };
    backend.fetchMessages.mockResolvedValueOnce([target]);
    backend.fetchThread.mockResolvedValueOnce(refreshed);
    backend.editMessage = vi.fn(async () => {
      if (disposition === "success") return committed;
      const conflict = new MessagingAPIError("edit_conflict", 409);
      Object.defineProperty(conflict, "currentMessage", {
        value: committed,
      });
      throw conflict;
    });
    useMessaging.setState({ threadsById: { [known.threadId]: known } });

    useMessaging.getState().selectPlace(key);
    await settle();
    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft(committed.content);
    useMessaging.getState().submitEdit();
    await settle();
    await settle();

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
          revision: 2,
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
          revision: 2,
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
      channels: [
        channel("channel-1", "新トピック", 2),
        channel("channel-2", ""),
      ],
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
    // cursorを配るのは履歴を持っている場所だけ。新しく見つかったplaceの未読は
    // このsnapshotが直したので、replayを頼む相手は増えていない。
    expect(backend.cursorCalls.at(-1)).toEqual({ [CHANNEL_1]: 0 });
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

  it("再接続snapshotから消えたchannelを保持しない", async () => {
    backend.next = snapshot({ channels: [], dms: [], unread: {} });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    expect(useMessaging.getState().channels).toEqual([]);
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
      channels: [
        channel("channel-1", "新トピック", 2),
        channel("channel-2", ""),
      ],
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

  it("履歴を持っていない場所の未読は再接続のsnapshotが直す", async () => {
    // 一覧にはあるが開いていないchannel。cursorを配っていないのでreplayは
    // 来ない——切断中に届いた分はsnapshotがそのまま正本になる。
    useMessaging.setState((state) => ({
      unreadCountByPlace: { ...state.unreadCountByPlace, [CHANNEL_1]: 0 },
      mentionCountByPlace: { ...state.mentionCountByPlace, [CHANNEL_1]: 0 },
      lastReadByPlace: { ...state.lastReadByPlace, [CHANNEL_1]: 5 },
    }));
    backend.next = snapshot({
      channels: [channel("channel-1", "旧トピック")],
      dms: [],
      unread: { [CHANNEL_1]: { latest: 9, unread: 4, mention: 2 } },
      lastRead: { [CHANNEL_1]: 5 },
    });

    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await settle();

    const state = useMessaging.getState();
    expect(state.unreadCountByPlace[CHANNEL_1]).toBe(4);
    expect(state.mentionCountByPlace[CHANNEL_1]).toBe(2);
  });

  it("開いている間だけ見ていたthreadは閉じるときに履歴もcursorも手放す", async () => {
    const visiting: ThreadSummary = {
      ...thread("thread-visiting"),
      participants: [OTHER],
    };
    const key = `thread:${visiting.threadId}` as PlaceKey;
    useMessaging.setState({ threadsById: { [visiting.threadId]: visiting } });
    backend.fetchMessages.mockResolvedValueOnce([
      threadMessage(visiting.threadId, 2),
    ]);

    useMessaging.getState().selectPlace(key);
    await settle();
    expect(useMessaging.getState().messagesByPlace[key]).toHaveLength(1);

    useMessaging.getState().clearPlaceSelection();

    // cursorを返した場所の履歴を抱えたままだと、再接続を跨いだ穴がそのまま
    // 残る。開き直したときにRESTで取り直せるよう、両方まとめて手放す。
    expect(useMessaging.getState().messagesByPlace[key]).toBeUndefined();
    expect(backend.releasePlace).toHaveBeenCalledWith({
      kind: "thread",
      threadId: visiting.threadId,
    });
  });

  it("閉じたwatch-only threadへ遅い履歴応答がholdとcursorを復活させない", async () => {
    const visiting: ThreadSummary = {
      ...thread("thread-late-history"),
      participants: [OTHER],
    };
    const key = `thread:${visiting.threadId}` as PlaceKey;
    let resolveHistory!: (messages: Message[]) => void;
    backend.fetchMessages.mockImplementationOnce(
      () =>
        new Promise<Message[]>((resolve) => {
          resolveHistory = resolve;
        }),
    );
    useMessaging.setState({ threadsById: { [visiting.threadId]: visiting } });

    useMessaging.getState().selectPlace(key);
    await settle();
    useMessaging.getState().clearPlaceSelection();
    const cursorsBeforeResponse = [...backend.cursorCalls];

    resolveHistory([threadMessage(visiting.threadId, 2)]);
    await settle();

    expect(useMessaging.getState().messagesByPlace[key]).toBeUndefined();
    expect(backend.releasePlace).toHaveBeenCalledWith({
      kind: "thread",
      threadId: visiting.threadId,
    });
    // A late completion must not call holdPlace again, which is the path that
    // registers this place in the next hello cursor collection.
    expect(backend.cursorCalls).toEqual(cursorsBeforeResponse);
  });

  it("watch-only thread release後の遅いedit success/409が履歴を復活させない", async () => {
    for (const disposition of ["success", "conflict"] as const) {
      const visiting: ThreadSummary = {
        ...thread(`thread-late-edit-${disposition}`),
        participants: [OTHER],
      };
      const key = `thread:${visiting.threadId}` as PlaceKey;
      const target: Message = {
        ...threadMessage(visiting.threadId, 1),
        author: SELF,
        content: "R",
        mentions: [],
        revision: 1,
      };
      const response = deferred<Message>();
      backend.editMessage = vi.fn(async () => response.promise);
      backend.fetchMessages.mockResolvedValueOnce([target]);
      useMessaging.setState({ threadsById: { [visiting.threadId]: visiting } });

      useMessaging.getState().selectPlace(key);
      await settle();
      useMessaging.getState().startEdit(target.messageId);
      useMessaging.getState().setEditDraft("R+1");
      useMessaging.getState().submitEdit();
      useMessaging.getState().clearPlaceSelection();

      const committed = { ...target, content: "R+1", revision: 2 };
      if (disposition === "success") {
        response.resolve(committed);
      } else {
        const conflict = new MessagingAPIError("edit_conflict", 409);
        Object.defineProperty(conflict, "currentMessage", {
          value: committed,
        });
        response.reject(conflict);
      }
      await settle();
      expect(useMessaging.getState().messagesByPlace[key]).toBeUndefined();
    }
  });

  it("reopened watch-only thread accepts a useful old edit ACK without rolling back a newer page", async () => {
    for (const reopenedRevision of [1, 3]) {
      const visiting: ThreadSummary = {
        ...thread(`thread-reopened-edit-${reopenedRevision}`),
        participants: [OTHER],
      };
      const key = `thread:${visiting.threadId}` as PlaceKey;
      const target: Message = {
        ...threadMessage(visiting.threadId, 1),
        author: SELF,
        content: "R",
        mentions: [],
        revision: 1,
      };
      const response = deferred<Message>();
      backend.editMessage = vi.fn(async () => response.promise);
      backend.fetchMessages
        .mockResolvedValueOnce([target])
        .mockResolvedValueOnce([
          reopenedRevision === 1
            ? target
            : { ...target, content: "R+2", revision: reopenedRevision },
        ]);
      useMessaging.setState({ threadsById: { [visiting.threadId]: visiting } });

      useMessaging.getState().selectPlace(key);
      await settle();
      useMessaging.getState().startEdit(target.messageId);
      useMessaging.getState().setEditDraft("R+1");
      useMessaging.getState().submitEdit();
      useMessaging.getState().clearPlaceSelection();
      useMessaging.getState().selectPlace(key);
      await settle();

      response.resolve({ ...target, content: "R+1", revision: 2 });
      await settle();
      expect(useMessaging.getState().messagesByPlace[key]?.[0]).toMatchObject(
        reopenedRevision === 1
          ? { content: "R+1", revision: 2 }
          : { content: "R+2", revision: 3 },
      );
      useMessaging.getState().clearPlaceSelection();
    }
  });

  it("release後の遅いread observerは未読もcursorも変更しない", async () => {
    const visiting: ThreadSummary = {
      ...thread("thread-late-read"),
      participants: [OTHER],
    };
    const key = `thread:${visiting.threadId}` as PlaceKey;
    backend.fetchMessages.mockResolvedValueOnce([
      threadMessage(visiting.threadId, 2),
    ]);
    useMessaging.setState({
      threadsById: { [visiting.threadId]: visiting },
      lastReadByPlace: { [key]: 1 },
      unreadCountByPlace: { [key]: 1 },
      mentionCountByPlace: { [key]: 1 },
    });
    useMessaging.getState().selectPlace(key);
    await settle();
    useMessaging.getState().clearPlaceSelection();
    backend.markRead.mockClear();

    useMessaging.getState().noteReadUpTo(key, 2);

    expect(useMessaging.getState().lastReadByPlace[key]).toBe(1);
    expect(useMessaging.getState().unreadCountByPlace[key]).toBe(1);
    expect(useMessaging.getState().mentionCountByPlace[key]).toBe(1);
    expect(backend.markRead).not.toHaveBeenCalled();
  });

  it("release/reopen後に古いreaction ACKやresyncを新しいsnapshotへ適用しない", async () => {
    const visiting: ThreadSummary = {
      ...thread("thread-old-reaction-projection"),
      participants: [OTHER],
    };
    const key = `thread:${visiting.threadId}` as PlaceKey;
    const target: Message = {
      ...threadMessage(visiting.threadId, 1),
      reactions: [],
    };
    const newer: Message = {
      ...target,
      reactions: [{ emoji: "🎉", participants: [OTHER] }],
    };
    const toggle = deferred<ReactionMutationResult>();
    backend.toggleReaction = vi.fn(async () => toggle.promise);
    backend.fetchMessages
      .mockResolvedValueOnce([target])
      .mockResolvedValueOnce([newer]);
    useMessaging.setState({ threadsById: { [visiting.threadId]: visiting } });

    useMessaging.getState().selectPlace(key);
    await settle();
    useMessaging.getState().toggleReaction(target, "👍");
    useMessaging.getState().clearPlaceSelection();
    useMessaging.getState().selectPlace(key);
    await settle();
    toggle.resolve({
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [SELF] }],
    });
    await settle();
    expect(
      useMessaging.getState().messagesByPlace[key]?.[0]?.reactions,
    ).toEqual(newer.reactions);

    const resync = deferred<Message[]>();
    backend.fetchMessages
      .mockReturnValueOnce(resync.promise)
      .mockResolvedValueOnce([newer]);
    backend.emit({
      type: "caught_up",
      place: { kind: "thread", threadId: visiting.threadId },
    });
    await settle();
    useMessaging.getState().clearPlaceSelection();
    useMessaging.getState().selectPlace(key);
    await settle();
    resync.resolve([
      {
        ...target,
        reactions: [{ emoji: "👀", participants: [SELF] }],
      },
    ]);
    await settle();
    expect(
      useMessaging.getState().messagesByPlace[key]?.[0]?.reactions,
    ).toEqual(newer.reactions);
  });

  it("activeなplaceの履歴取得失敗も保持とcursorをまとめて手放す", async () => {
    backend.fetchMessages.mockRejectedValueOnce(new Error("history timed out"));

    useMessaging.getState().selectPlace(CHANNEL_1);
    await settle();

    expect(useMessaging.getState().activePlaceKey).toBe(CHANNEL_1);
    expect(useMessaging.getState().messagesByPlace[CHANNEL_1]).toBeUndefined();
    expect(backend.releasePlace).toHaveBeenCalledWith({
      kind: "channel",
      channelId: "channel-1",
    });
  });

  it("初めて開く場所の宣言は知っている最新seqを名乗る", async () => {
    // 宣言のcursorはserverのreplay開始点。持っていないからと0を名乗ると、
    // 画面を開くたびにその場所の先頭から流れてくる。欲しいのは、いま取りに
    // 行くpageとこの宣言の隙間だけ。
    useMessaging.getState().selectPlace(CHANNEL_1);
    await settle();
    expect(backend.openPlace).toHaveBeenCalledWith(
      { kind: "channel", channelId: "channel-1" },
      5,
    );

    // liveで先へ進んだあとに開き直しても、宣言は進んだ分から頼む。
    useMessaging.getState().clearPlaceSelection();
    backend.emit({
      type: "message_created",
      message: {
        ...threadMessage("unused", 7),
        place: { kind: "channel", channelId: "channel-1" },
      },
      notify: null,
    });
    useMessaging.getState().selectPlace(CHANNEL_1);
    await settle();
    expect(backend.openPlace).toHaveBeenLastCalledWith(
      { kind: "channel", channelId: "channel-1" },
      7,
    );
  });

  it("手放した後に届いた遅延frameは履歴を作らず、開き直せば全部揃う", async () => {
    const visiting: ThreadSummary = {
      ...thread("thread-late-frame"),
      participants: [OTHER],
    };
    const key = `thread:${visiting.threadId}` as PlaceKey;
    useMessaging.setState({ threadsById: { [visiting.threadId]: visiting } });
    backend.fetchMessages.mockResolvedValueOnce([
      threadMessage(visiting.threadId, 2),
    ]);

    useMessaging.getState().selectPlace(key);
    await settle();
    useMessaging.getState().clearPlaceSelection();
    expect(useMessaging.getState().messagesByPlace[key]).toBeUndefined();

    // 手放した時点でHubが既にenqueueしていた分。持っていない場所の履歴を
    // ここで作ると、次に開いたときそれが「読み込み済み」に見えて穴が残る。
    backend.emit({
      type: "message_created",
      message: threadMessage(visiting.threadId, 3),
      notify: null,
    });
    expect(useMessaging.getState().messagesByPlace[key]).toBeUndefined();
    // 数え上げは別の台帳。開いていない場所のバッジは進み続ける。
    expect(useMessaging.getState().unreadCountByPlace[key]).toBe(1);

    backend.fetchMessages.mockResolvedValueOnce([
      threadMessage(visiting.threadId, 2),
      threadMessage(visiting.threadId, 3),
    ]);
    useMessaging.getState().selectPlace(key);
    await settle();

    expect(backend.fetchMessages).toHaveBeenCalledTimes(2);
    expect(
      useMessaging.getState().messagesByPlace[key]?.map((m) => m.seq),
    ).toEqual([2, 3]);
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

  it("遅れて届く古いplace_updatedで新しい表示へ戻さない", () => {
    backend.listeners.forEach((listener) => {
      listener({
        type: "place_updated",
        channel: channel("channel-1", "新しいtopic", 3),
      });
    });
    backend.listeners.forEach((listener) => {
      listener({
        type: "place_updated",
        channel: channel("channel-1", "古いtopic", 2),
      });
    });

    expect(useMessaging.getState().channels[0]).toMatchObject({
      topic: "新しいtopic",
      revision: 3,
    });
  });

  it("未知のplace_updatedを挿入し、後着の古いplace_createdを退ける", () => {
    backend.listeners.forEach((listener) => {
      listener({
        type: "place_updated",
        channel: channel("channel-2", "編集済みtopic", 2),
      });
    });
    backend.listeners.forEach((listener) => {
      listener({
        type: "place_created",
        channel: channel("channel-2", "作成時topic", 1),
      });
    });

    expect(useMessaging.getState().channels).toContainEqual(
      expect.objectContaining({
        channelId: "channel-2",
        topic: "編集済みtopic",
        revision: 2,
      }),
    );
  });
});
