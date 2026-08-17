import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  Message,
  MessagingBackend,
  Place,
  PlaceKey,
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

  async fetchMessages() {
    return [];
  }
  async searchMessages() {
    return [];
  }
  fetchThread = vi.fn(async (_threadId: string): Promise<ThreadSummary> => {
    throw new Error("unused");
  });
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
  async sendMessage() {
    return {
      clientNonce: "unused",
      messageId: "unused",
      seq: 1,
      created: true,
    };
  }
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
    backend.emitConnection("connected");
    await settle();
  });

  afterEach(() => bindMessagingSessionIdentity(null));

  it("最初のconnectedではbootstrapを読み直さない", () => {
    expect(backend.bootstrapCalls).toBe(1);
    expect(useMessaging.getState().ready).toBe(true);
  });

  it("liveイベントで初めて知ったthreadを一度だけhydrateして選べる", async () => {
    const live = thread("thread-live");
    backend.fetchThread.mockResolvedValue(live);

    backend.emit({
      type: "message_created",
      message: threadMessage(live.threadId, 2),
      notify: null,
    });
    backend.emit({
      type: "message_created",
      message: threadMessage(live.threadId, 3),
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

  it("重複したthread message_createdで件数を二重加算しない", async () => {
    const known = thread("thread-known");
    useMessaging.setState({ threadsById: { [known.threadId]: known } });
    const incoming = threadMessage(known.threadId, 2);

    backend.emit({ type: "message_created", message: incoming, notify: null });
    backend.emit({ type: "message_created", message: incoming, notify: null });
    await settle();

    expect(useMessaging.getState().threadsById[known.threadId]).toMatchObject({
      messageCount: known.messageCount + 1,
      latestSeq: incoming.seq,
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
