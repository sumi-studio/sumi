import { create } from "zustand";
import { secureRandomUUID } from "../lib/random-uuid";
import { ApiMessagingBackend } from "./api-backend";
import { hasDisplayMention } from "./mention";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  MemberProfile,
  Message,
  MessagingBackend,
  MessagingCapabilities,
  ParticipantKey,
  ParticipantRef,
  ParticipantStatus,
  Place,
  PlaceKey,
  ReactionSummary,
  ReplyLaterMarker,
  ServerEvent,
  StatusKind,
  Urgency,
  WorkspaceSummary,
} from "./model";
import { parsePlaceKey, participantKey, placeKey } from "./model";
import type { PendingMessage } from "./timeline";
import { mergeMessages, upsertMessage } from "./timeline";

const TYPING_TTL_MS = 4_500;
const DEFAULT_REPLY_LATER_REMIND_MS = 30 * 60_000;
/** reaction再同期の1ページ上限（serverの上限と同じ）。 */
const REACTION_RESYNC_LIMIT = 200;

/** reactionの同一判定用。無変化ならmessageの参照を保って再描画を避ける。 */
function reactionsFingerprint(reactions: readonly ReactionSummary[]): string {
  return reactions
    .map(
      (entry) =>
        `${entry.emoji}:${entry.participants.map(participantKey).join(",")}`,
    )
    .join("|");
}

let backend: MessagingBackend = new ApiMessagingBackend();

/** Tests and explicit development harnesses may replace the transport before init. */
export function installMessagingBackend(override: MessagingBackend): void {
  if (initialized) throw new Error("Messaging backend is already initialized");
  backend = override;
}

interface MessagingState {
  capabilities: MessagingCapabilities;
  ready: boolean;
  self: ParticipantRef | null;
  selfKey: ParticipantKey;
  workspaces: WorkspaceSummary[];
  channels: ChannelSummary[];
  dms: DmSummary[];
  membersByKey: Record<ParticipantKey, MemberProfile>;
  statusByKey: Record<ParticipantKey, ParticipantStatus>;
  messagesByPlace: Record<PlaceKey, Message[]>;
  pendingByPlace: Record<PlaceKey, PendingMessage[]>;
  lastReadByPlace: Record<PlaceKey, number>;
  unreadCountByPlace: Record<PlaceKey, number>;
  mentionCountByPlace: Record<PlaceKey, number>;
  /** placeへ入った時点のlastReadのスナップショット。離れるまで動かさない。 */
  unreadLineByPlace: Record<PlaceKey, number | null>;
  draftByPlace: Record<PlaceKey, string>;
  typingByPlace: Record<PlaceKey, Record<ParticipantKey, number>>;
  replyLaterById: Record<string, ReplyLaterMarker>;
  employedAgents: ParticipantRef[];
  hasMoreByPlace: Record<PlaceKey, boolean>;
  loadingOlderByPlace: Record<PlaceKey, boolean>;
  activePlaceKey: PlaceKey | null;
  editingMessageId: string | null;
  replyTargetId: string | null;
  connection: ConnectionState;

  init(): void;
  selectPlace(key: PlaceKey): void;
  createChannel(name: string, topic: string): Promise<PlaceKey>;
  /** 1人ならDM（既存があれば再利用）、複数人ならグループDMを開く。 */
  startDM(participants: ParticipantRef[]): Promise<PlaceKey>;
  updateChannelTopic(channelId: string, topic: string): Promise<void>;
  loadPlaceAround(key: PlaceKey, seq: number): Promise<boolean>;
  setDraft(key: PlaceKey, draft: string): void;
  send(content: string, urgency: Urgency): void;
  retrySend(clientNonce: string): void;
  startEdit(messageId: string): void;
  cancelEdit(): void;
  submitEdit(content: string): void;
  deleteMessage(messageId: string): void;
  setReplyTarget(messageId: string | null): void;
  noteReadUpTo(key: PlaceKey, seq: number): void;
  setStatus(status: StatusKind, note: string): void;
  createReplyLater(message: Message, delayMs?: number): void;
  toggleReaction(message: Message, emoji: string): void;
  loadOlder(key: PlaceKey): Promise<void>;
  resolveReplyLater(markerId: string): void;
  sendTyping(): void;
}

function resolveMentions(
  content: string,
  members: Record<ParticipantKey, MemberProfile>,
  selfKey: ParticipantKey,
): ParticipantRef[] {
  const mentions: ParticipantRef[] = [];
  for (const member of Object.values(members)) {
    const key = participantKey(member.participant);
    if (key === selfKey) continue;
    if (hasDisplayMention(content, member.displayName)) {
      mentions.push(member.participant);
    }
  }
  return mentions;
}

function unreadContribution(
  message: Message,
  lastReadSeq: number,
  selfKey: ParticipantKey,
): { unread: number; mentions: number } {
  if (
    message.deleted ||
    message.seq <= lastReadSeq ||
    participantKey(message.author) === selfKey
  ) {
    return { unread: 0, mentions: 0 };
  }
  return {
    unread: 1,
    mentions:
      message.urgency !== "fyi" &&
      message.mentions.some((ref) => participantKey(ref) === selfKey)
        ? 1
        : 0,
  };
}

let initialized = false;
let messagingSessionGeneration = 0;

type ReactionUpdatedEvent = Extract<ServerEvent, { type: "reaction_updated" }>;

interface ReactionProjectionOperation {
  epoch: number;
  journal: ReactionUpdatedEvent[];
}

interface ReactionProjectionCoordinator {
  active: ReactionProjectionOperation | null;
  backend: MessagingBackend;
  epoch: number;
  pending: number;
  sessionGeneration: number;
  tail: Promise<void>;
}

const reactionProjectionByPlace = new Map<
  PlaceKey,
  ReactionProjectionCoordinator
>();

export const useMessaging = create<MessagingState>((set, get) => {
  if (import.meta.env.DEV) {
    // 開発時のデバッグ・E2E検証用のstate参照口。
    (globalThis as Record<string, unknown>).__sumiMessaging = () => get();
  }
  const applyReactionUpdateRaw = (event: ReactionUpdatedEvent) => {
    const key = placeKey(event.place);
    // reactionだけを差し替える。message全体を置き換えると、同時に届いた
    // 編集をこのeventが巻き戻す。未ロードのmessageは無視でよい（後で読めば
    // 現在のreactionが付いてくる）。
    set((state) => {
      const current = state.messagesByPlace[key];
      if (!current) return {};
      let changed = false;
      const next = current.map((message) => {
        if (message.messageId !== event.messageId) return message;
        // tombstoneはreactionを持たない。削除より前に始まったtoggleの遅い
        // ACK/echoをreplayしても、削除済みmessageを復活させない。
        if (message.deleted) return message;
        if (
          reactionsFingerprint(message.reactions) ===
          reactionsFingerprint(event.reactions)
        ) {
          return message;
        }
        changed = true;
        return { ...message, reactions: event.reactions };
      });
      if (!changed) return {};
      return { messagesByPlace: { ...state.messagesByPlace, [key]: next } };
    });
  };

  /**
   * Absolute reaction snapshotを作る処理はplaceごとのFIFOに置く。resyncと
   * local toggle ACKが独立に完了すると、遅い古いsnapshotが新しいstateを
   * 巻き戻せるため。active operation中のWSは即時反映しつつjournalへ一度だけ
   * 記録し、snapshot適用後に受信順でreplayする。wire revisionがないため
   * cross-transportの瞬時線形化は主張しない。request前にpublish済みのframeが
   * request中に届いても、serverがown echoを同じWS FIFOへenqueueしてからHTTPを
   * 返すことで収束し、切断・overflow時は後続のcaught_up resyncで収束する。
   */
  const enqueueReactionProjection = (
    place: Place,
    produce: (
      operationBackend: MessagingBackend,
      isCurrent: () => boolean,
    ) => Promise<() => void>,
  ): Promise<void> => {
    const key = placeKey(place);
    const operationBackend = backend;
    const sessionGeneration = messagingSessionGeneration;
    let coordinator = reactionProjectionByPlace.get(key);
    if (
      !coordinator ||
      coordinator.backend !== operationBackend ||
      coordinator.sessionGeneration !== sessionGeneration
    ) {
      coordinator = {
        active: null,
        backend: operationBackend,
        epoch: 0,
        pending: 0,
        sessionGeneration,
        tail: Promise.resolve(),
      };
      reactionProjectionByPlace.set(key, coordinator);
    }
    const target = coordinator;
    target.pending += 1;
    const task = target.tail.then(async () => {
      const isCurrent = () =>
        backend === operationBackend &&
        messagingSessionGeneration === sessionGeneration &&
        reactionProjectionByPlace.get(key) === target;
      if (!isCurrent()) return;
      const operation: ReactionProjectionOperation = {
        epoch: ++target.epoch,
        journal: [],
      };
      target.active = operation;
      try {
        const applySnapshot = await produce(operationBackend, isCurrent);
        if (
          !isCurrent() ||
          target.active !== operation ||
          target.epoch !== operation.epoch
        ) {
          return;
        }
        applySnapshot();
        for (const event of operation.journal) {
          applyReactionUpdateRaw(event);
        }
      } finally {
        if (target.active === operation) target.active = null;
      }
    });
    const settled = task
      .catch(() => undefined)
      .finally(() => {
        target.pending -= 1;
        if (
          target.pending === 0 &&
          target.active === null &&
          reactionProjectionByPlace.get(key) === target
        ) {
          reactionProjectionByPlace.delete(key);
        }
      });
    target.tail = settled;
    return task;
  };

  /**
   * ロード済み範囲のreactionを読み直して収束させる。catch-upはcursorより後の
   * messageしかreplayしないので、切断中やHub overflowで落ちたreaction eventは
   * 二度と届かない。再接続のたびにロード済みwindow全体を
   * serverの上限ごとに遡って読み直す。
   */
  const resyncReactions = (place: Place): Promise<void> =>
    enqueueReactionProjection(place, async (resyncBackend, isCurrent) => {
      const key = placeKey(place);
      const loaded = get().messagesByPlace[key];
      if (!loaded || loaded.length === 0) return () => undefined;
      const reactionsById = new Map<string, ReactionSummary[]>();
      const ranges: { oldestSeq: number; newestSeq: number }[] = [];
      for (const message of [...loaded].sort(
        (left, right) => left.seq - right.seq,
      )) {
        const current = ranges[ranges.length - 1];
        if (current && message.seq <= current.newestSeq + 1) {
          current.newestSeq = Math.max(current.newestSeq, message.seq);
        } else {
          ranges.push({ oldestSeq: message.seq, newestSeq: message.seq });
        }
      }

      // loadPlaceAroundで離れたwindowが併存し得るため、件数ではなく連続seq範囲
      // ごとに取得する。gapをページ送りで横断せず、各windowを確実に覆う。
      for (const range of ranges.reverse()) {
        let beforeSeq = range.newestSeq + 1;
        while (beforeSeq > range.oldestSeq) {
          const limit = Math.min(
            beforeSeq - range.oldestSeq,
            REACTION_RESYNC_LIMIT,
          );
          const fresh = await resyncBackend.fetchMessages(place, {
            beforeSeq,
            limit,
          });
          if (!isCurrent()) return () => undefined;
          if (fresh.length === 0) break;
          for (const message of fresh) {
            reactionsById.set(message.messageId, message.reactions);
          }
          const nextBeforeSeq = Math.min(
            ...fresh.map((message) => message.seq),
          );
          if (nextBeforeSeq >= beforeSeq) break;
          beforeSeq = nextBeforeSeq;
        }
      }

      return () => {
        set((state) => {
          const current = state.messagesByPlace[key];
          if (!current) return {};
          let changed = false;
          const next = current.map((message) => {
            const reactions = reactionsById.get(message.messageId);
            if (
              message.deleted ||
              !reactions ||
              reactionsFingerprint(reactions) ===
                reactionsFingerprint(message.reactions)
            ) {
              return message;
            }
            changed = true;
            return { ...message, reactions };
          });
          if (!changed) return {};
          return {
            messagesByPlace: { ...state.messagesByPlace, [key]: next },
          };
        });
      };
    });

  const applyReactionUpdate = (event: ReactionUpdatedEvent) => {
    const coordinator = reactionProjectionByPlace.get(placeKey(event.place));
    if (
      coordinator?.backend === backend &&
      coordinator.sessionGeneration === messagingSessionGeneration
    ) {
      coordinator.active?.journal.push(event);
    }
    applyReactionUpdateRaw(event);
  };

  const applyEvent = (
    event: Parameters<Parameters<MessagingBackend["subscribe"]>[0]>[0],
  ) => {
    if (event.type === "reaction_updated") {
      applyReactionUpdate(event);
      return;
    }
    if (event.type === "caught_up") {
      void resyncReactions(event.place).catch(() => undefined);
      return;
    }
    if (event.type === "message_created" || event.type === "message_edited") {
      const key = placeKey(event.message.place);
      set((state) => {
        const existing = (state.messagesByPlace[key] ?? []).find(
          (message) => message.messageId === event.message.messageId,
        );
        const messages = upsertMessage(
          state.messagesByPlace[key] ?? [],
          event.message,
        );
        const nonce = event.message.clientNonce;
        const pending = nonce
          ? (state.pendingByPlace[key] ?? []).filter(
              (entry) => entry.clientNonce !== nonce,
            )
          : (state.pendingByPlace[key] ?? []);
        const authorKey = participantKey(event.message.author);
        const typing = { ...(state.typingByPlace[key] ?? {}) };
        delete typing[authorKey];
        const lastRead =
          authorKey === state.selfKey
            ? Math.max(state.lastReadByPlace[key] ?? 0, event.message.seq)
            : (state.lastReadByPlace[key] ?? 0);
        const previousContribution = existing
          ? unreadContribution(existing, lastRead, state.selfKey)
          : { unread: 0, mentions: 0 };
        const nextContribution = unreadContribution(
          event.message,
          lastRead,
          state.selfKey,
        );
        return {
          messagesByPlace: { ...state.messagesByPlace, [key]: messages },
          pendingByPlace: { ...state.pendingByPlace, [key]: pending },
          typingByPlace: { ...state.typingByPlace, [key]: typing },
          lastReadByPlace: { ...state.lastReadByPlace, [key]: lastRead },
          unreadCountByPlace: {
            ...state.unreadCountByPlace,
            [key]: Math.max(
              0,
              (state.unreadCountByPlace[key] ?? 0) -
                previousContribution.unread +
                nextContribution.unread,
            ),
          },
          mentionCountByPlace: {
            ...state.mentionCountByPlace,
            [key]: Math.max(
              0,
              (state.mentionCountByPlace[key] ?? 0) -
                previousContribution.mentions +
                nextContribution.mentions,
            ),
          },
        };
      });
      return;
    }
    if (event.type === "message_deleted") {
      const key = placeKey(event.message.place);
      set((state) => {
        const existing = (state.messagesByPlace[key] ?? []).find(
          (message) => message.messageId === event.message.messageId,
        );
        const previous = existing ?? { ...event.message, deleted: false };
        const contribution = unreadContribution(
          previous,
          state.lastReadByPlace[key] ?? 0,
          state.selfKey,
        );
        return {
          messagesByPlace: {
            ...state.messagesByPlace,
            [key]: upsertMessage(
              state.messagesByPlace[key] ?? [],
              event.message,
            ),
          },
          unreadCountByPlace: {
            ...state.unreadCountByPlace,
            [key]: Math.max(
              0,
              (state.unreadCountByPlace[key] ?? 0) - contribution.unread,
            ),
          },
          mentionCountByPlace: {
            ...state.mentionCountByPlace,
            [key]: Math.max(
              0,
              (state.mentionCountByPlace[key] ?? 0) - contribution.mentions,
            ),
          },
        };
      });
      return;
    }
    if (event.type === "typing") {
      const key = placeKey(event.place);
      const typerKey = participantKey(event.participant);
      if (typerKey === get().selfKey) return;
      set((state) => ({
        typingByPlace: {
          ...state.typingByPlace,
          [key]: {
            ...(state.typingByPlace[key] ?? {}),
            [typerKey]: Date.now() + TYPING_TTL_MS,
          },
        },
      }));
      return;
    }
    if (event.type === "status_updated") {
      set((state) => ({
        statusByKey: {
          ...state.statusByKey,
          [participantKey(event.status.participant)]: event.status,
        },
      }));
      return;
    }
    if (event.type === "reply_later_created") {
      set((state) => ({
        replyLaterById: {
          ...state.replyLaterById,
          [event.marker.markerId]: event.marker,
        },
      }));
      return;
    }
    if (event.type === "reply_later_resolved") {
      set((state) => {
        const marker = state.replyLaterById[event.markerId];
        if (!marker) return {};
        return {
          replyLaterById: {
            ...state.replyLaterById,
            [event.markerId]: { ...marker, resolved: true },
          },
        };
      });
      return;
    }
    if (event.type === "place_created") {
      const { channel, dm } = event;
      set((state) => {
        if (channel) {
          return state.channels.some(
            (entry) => entry.channelId === channel.channelId,
          )
            ? {}
            : { channels: [...state.channels, channel] };
        }
        if (dm) {
          return state.dms.some((entry) => entry.dmId === dm.dmId)
            ? {}
            : { dms: [...state.dms, dm] };
        }
        return {};
      });
      return;
    }
    if (event.type === "place_updated") {
      const { channel } = event;
      set((state) => ({
        channels: state.channels.map((entry) =>
          entry.channelId === channel.channelId ? channel : entry,
        ),
      }));
    }
  };

  /**
   * 再接続時のplace突き合わせ。place lifecycle event（place_created /
   * place_updated）はcursor replayの対象外で、durableな正本はplacesそのもの。
   * 切断中に作られたchannel/DMや編集されたtopicはeventとして二度と届かないため、
   * 再接続のたびにbootstrapを読み直してplace一覧を取り直す。
   *
   * 進行中のローカルstate（下書き・選択中のplace・読み込み済みメッセージ・
   * 送信待ち）は触らない。既知placeの既読と未読はcursor replayとローカルの
   * 既読進行が正本で、bootstrapのスナップショットで塗り直すと読んだはずの
   * メッセージが未読へ巻き戻る。未読を採用するのはこのクライアントがまだ
   * 知らなかったplaceだけにする。
   */
  const reconcilePlaces = async () => {
    const currentBackend = backend;
    const currentIdentity = getMessagingSessionIdentity();
    const expectedSelfKey = get().selfKey;
    const snapshot = await currentBackend.bootstrap();
    if (
      backend !== currentBackend ||
      getMessagingSessionIdentity() !== currentIdentity ||
      get().selfKey !== expectedSelfKey ||
      participantKey(snapshot.self) !== expectedSelfKey
    ) {
      // セッション境界を越えた応答は別人のsnapshotなので捨てる。
      return;
    }
    const state = get();
    const known = new Set<PlaceKey>([
      ...state.channels.map((entry) =>
        placeKey({ kind: "channel", channelId: entry.channelId }),
      ),
      ...state.dms.map((entry) =>
        placeKey({ kind: entry.kind, dmId: entry.dmId }),
      ),
    ]);
    const membersByKey: Record<ParticipantKey, MemberProfile> = {};
    for (const member of snapshot.members) {
      membersByKey[participantKey(member.participant)] = member;
    }
    const lastReadByPlace = { ...state.lastReadByPlace };
    for (const marker of snapshot.readMarkers) {
      const key = placeKey(marker.place);
      if (!known.has(key)) lastReadByPlace[key] = marker.lastReadSeq;
    }
    const unreadCountByPlace = { ...state.unreadCountByPlace };
    const mentionCountByPlace = { ...state.mentionCountByPlace };
    const sinceByPlace: Record<PlaceKey, number> = {};
    for (const summary of snapshot.unreadSummaries) {
      const key = placeKey(summary.place);
      if (known.has(key)) continue;
      unreadCountByPlace[key] = summary.unreadCount;
      mentionCountByPlace[key] = summary.mentionCount;
      sinceByPlace[key] = summary.latestSeq;
    }
    set({
      workspaces: snapshot.workspaces,
      channels: snapshot.channels,
      dms: snapshot.dms,
      membersByKey,
      lastReadByPlace,
      unreadCountByPlace,
      mentionCountByPlace,
    });
    if (Object.keys(sinceByPlace).length > 0) {
      // 新しく見つかったplaceのcursorを登録し、次の切断でもそのplaceの
      // durable eventがreplayされるようにする。applyEventは同一参照なので
      // listenerは重複しない。
      currentBackend.subscribe(applyEvent, { sinceByPlace });
    }
  };

  const PAGE_SIZE = 50;

  const loadPlace = async (place: Place) => {
    const key = placeKey(place);
    if (get().messagesByPlace[key]) return;
    const messages = await backend.fetchMessages(place, { limit: PAGE_SIZE });
    set((state) => ({
      messagesByPlace: {
        ...state.messagesByPlace,
        [key]: mergeMessages(state.messagesByPlace[key] ?? [], messages),
      },
      hasMoreByPlace: {
        ...state.hasMoreByPlace,
        [key]: messages.length >= PAGE_SIZE,
      },
    }));
  };

  // 送信・再送の共通経路。ACK(receipt)はecho eventで照合されるため、
  // ここでは失敗時にpendingへfailedを立てて再送UIへ委ねるだけで良い。
  const dispatchSend = (key: PlaceKey, pending: PendingMessage) => {
    const place = parsePlaceKey(key);
    if (!place) return;
    backend
      .sendMessage({
        place,
        content: pending.content,
        urgency: pending.urgency,
        replyTo: pending.replyTo,
        clientNonce: pending.clientNonce,
      })
      .then(async (receipt) => {
        let confirmed = (get().messagesByPlace[key] ?? []).some(
          (message) =>
            message.messageId === receipt.messageId ||
            message.clientNonce === pending.clientNonce,
        );
        if (!confirmed) {
          // ACKだけ届き、live echoを取りこぼした再送もreceiptのseqから確定する。
          const messages = await backend.fetchMessages(place, {
            beforeSeq: receipt.seq + 1,
            limit: 1,
          });
          const committed = messages.find(
            (message) => message.messageId === receipt.messageId,
          );
          if (committed) {
            set((state) => ({
              messagesByPlace: {
                ...state.messagesByPlace,
                [key]: upsertMessage(
                  state.messagesByPlace[key] ?? [],
                  committed,
                ),
              },
            }));
            confirmed = true;
          }
        }
        if (!confirmed) throw new Error("Committed message was not found");
        set((state) => ({
          pendingByPlace: {
            ...state.pendingByPlace,
            [key]: (state.pendingByPlace[key] ?? []).filter(
              (entry) => entry.clientNonce !== pending.clientNonce,
            ),
          },
        }));
      })
      .catch(() => {
        set((state) => ({
          pendingByPlace: {
            ...state.pendingByPlace,
            [key]: (state.pendingByPlace[key] ?? []).map((entry) =>
              entry.clientNonce === pending.clientNonce
                ? { ...entry, failed: true }
                : entry,
            ),
          },
        }));
      });
  };

  return {
    capabilities: backend.capabilities,
    ready: false,
    self: null,
    selfKey: "",
    workspaces: [],
    channels: [],
    dms: [],
    membersByKey: {},
    statusByKey: {},
    messagesByPlace: {},
    pendingByPlace: {},
    lastReadByPlace: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    unreadLineByPlace: {},
    draftByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    employedAgents: [],
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "connected",

    init() {
      if (initialized) return;
      initialized = true;
      void backend
        .bootstrap()
        .then((snapshot) => {
          const membersByKey: Record<ParticipantKey, MemberProfile> = {};
          for (const member of snapshot.members) {
            membersByKey[participantKey(member.participant)] = member;
          }
          const statusByKey: Record<ParticipantKey, ParticipantStatus> = {};
          for (const status of snapshot.statuses) {
            statusByKey[participantKey(status.participant)] = status;
          }
          const lastReadByPlace: Record<PlaceKey, number> = {};
          for (const marker of snapshot.readMarkers) {
            lastReadByPlace[placeKey(marker.place)] = marker.lastReadSeq;
          }
          const replyLaterById: Record<string, ReplyLaterMarker> = {};
          for (const marker of snapshot.replyLaterMarkers) {
            replyLaterById[marker.markerId] = marker;
          }
          const unreadCountByPlace: Record<PlaceKey, number> = {};
          const mentionCountByPlace: Record<PlaceKey, number> = {};
          const sinceByPlace: Record<PlaceKey, number> = {};
          for (const summary of snapshot.unreadSummaries) {
            const key = placeKey(summary.place);
            unreadCountByPlace[key] = summary.unreadCount;
            mentionCountByPlace[key] = summary.mentionCount;
            sinceByPlace[key] = summary.latestSeq;
          }
          set({
            ready: true,
            capabilities: backend.capabilities,
            self: snapshot.self,
            selfKey: participantKey(snapshot.self),
            workspaces: snapshot.workspaces,
            channels: snapshot.channels,
            dms: snapshot.dms,
            membersByKey,
            statusByKey,
            lastReadByPlace,
            unreadCountByPlace,
            mentionCountByPlace,
            replyLaterById,
            employedAgents: snapshot.employedAgents,
          });
          backend.subscribe(applyEvent, { sinceByPlace });
          // 最初のconnectedはいま読んだこのbootstrapが正本。以降のconnectedは
          // 再接続なので、replayされないplace lifecycleを読み直す。
          let connectedOnce = false;
          backend.subscribeConnection((connection) => {
            set({ connection });
            if (connection !== "connected") return;
            if (!connectedOnce) {
              connectedOnce = true;
              return;
            }
            void reconcilePlaces().catch(() => undefined);
          });
        })
        .catch(() => {
          initialized = false;
          set({ connection: "disconnected" });
        });
    },

    selectPlace(key) {
      const place = parsePlaceKey(key);
      if (!place) return;
      const state = get();
      const known =
        place.kind === "channel"
          ? state.channels.some(
              (channel) => channel.channelId === place.channelId,
            )
          : state.dms.some(
              (dm) => dm.kind === place.kind && dm.dmId === place.dmId,
            );
      if (!known) return;
      set((state) => ({
        activePlaceKey: key,
        editingMessageId: null,
        replyTargetId: null,
        unreadLineByPlace: {
          ...state.unreadLineByPlace,
          [key]: state.lastReadByPlace[key] ?? 0,
        },
      }));
      void loadPlace(place);
    },

    async createChannel(name, topic) {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) throw new Error("workspace is not ready");
      const channel = await backend.createChannel(workspaceId, name, topic);
      set((state) =>
        state.channels.some((entry) => entry.channelId === channel.channelId)
          ? {}
          : { channels: [...state.channels, channel] },
      );
      return placeKey({ kind: "channel", channelId: channel.channelId });
    },

    async startDM(participants) {
      const [first] = participants;
      if (!first) throw new Error("participants are required");
      const currentBackend = backend;
      const currentIdentity = getMessagingSessionIdentity();
      const expectedSelfKey = get().selfKey;
      const dm =
        participants.length === 1
          ? await currentBackend.ensureDM(first)
          : await currentBackend.createGroupDM(participants);
      if (
        backend !== currentBackend ||
        getMessagingSessionIdentity() !== currentIdentity ||
        get().selfKey !== expectedSelfKey
      ) {
        throw new Error("Messaging session changed during DM start");
      }
      set((state) =>
        state.dms.some((entry) => entry.dmId === dm.dmId)
          ? {}
          : { dms: [...state.dms, dm] },
      );
      return placeKey({ kind: dm.kind, dmId: dm.dmId });
    },

    async updateChannelTopic(channelId, topic) {
      const channel = await backend.updateChannelTopic(channelId, topic);
      set((state) => ({
        channels: state.channels.map((entry) =>
          entry.channelId === channel.channelId ? channel : entry,
        ),
      }));
    },

    async loadPlaceAround(key, seq) {
      const place = parsePlaceKey(key);
      if (!place || !Number.isSafeInteger(seq) || seq < 1) return false;
      if (
        (get().messagesByPlace[key] ?? []).some(
          (message) => message.seq === seq,
        )
      ) {
        return true;
      }
      const messages = await backend.fetchMessages(place, {
        beforeSeq: seq + 1,
        limit: 50,
      });
      set((state) => ({
        messagesByPlace: {
          ...state.messagesByPlace,
          [key]: mergeMessages(state.messagesByPlace[key] ?? [], messages),
        },
      }));
      return messages.some((message) => message.seq === seq);
    },

    setDraft(key, draft) {
      set((state) => ({
        draftByPlace: { ...state.draftByPlace, [key]: draft },
      }));
    },

    send(content, urgency) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      const trimmed = content.trim();
      if (!key || !place || !trimmed || !state.self) return;
      const pending: PendingMessage = {
        clientNonce: secureRandomUUID(),
        content: trimmed,
        mentions: resolveMentions(trimmed, state.membersByKey, state.selfKey),
        urgency,
        replyTo: state.replyTargetId,
        createdAt: Date.now(),
      };
      set((current) => ({
        pendingByPlace: {
          ...current.pendingByPlace,
          [key]: [...(current.pendingByPlace[key] ?? []), pending],
        },
        draftByPlace: { ...current.draftByPlace, [key]: "" },
        replyTargetId: null,
      }));
      dispatchSend(key, pending);
    },

    retrySend(clientNonce) {
      const key = get().activePlaceKey;
      if (!key) return;
      const pending = (get().pendingByPlace[key] ?? []).find(
        (entry) => entry.clientNonce === clientNonce,
      );
      if (!pending) return;
      set((current) => ({
        pendingByPlace: {
          ...current.pendingByPlace,
          [key]: (current.pendingByPlace[key] ?? []).map((entry) =>
            entry.clientNonce === clientNonce
              ? { ...entry, failed: false }
              : entry,
          ),
        },
      }));
      dispatchSend(key, pending);
    },

    startEdit(messageId) {
      set({ editingMessageId: messageId, replyTargetId: null });
    },

    cancelEdit() {
      set({ editingMessageId: null });
    },

    submitEdit(content) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      const messageId = state.editingMessageId;
      const trimmed = content.trim();
      if (!key || !place || !messageId) return;
      if (trimmed) void backend.editMessage(place, messageId, trimmed);
      set({ editingMessageId: null });
    },

    deleteMessage(messageId) {
      const state = get();
      const place = state.activePlaceKey
        ? parsePlaceKey(state.activePlaceKey)
        : null;
      if (!place) return;
      void backend.deleteMessage(place, messageId);
    },

    setReplyTarget(messageId) {
      set({ replyTargetId: messageId, editingMessageId: null });
    },

    noteReadUpTo(key, seq) {
      const state = get();
      const current = state.lastReadByPlace[key] ?? 0;
      if (seq <= current) return;
      const place = parsePlaceKey(key);
      if (!place) return;
      set((entry) => ({
        lastReadByPlace: { ...entry.lastReadByPlace, [key]: seq },
        unreadCountByPlace: {
          ...entry.unreadCountByPlace,
          [key]: (entry.messagesByPlace[key] ?? []).filter(
            (message) =>
              unreadContribution(message, seq, entry.selfKey).unread > 0,
          ).length,
        },
        mentionCountByPlace: {
          ...entry.mentionCountByPlace,
          [key]: (entry.messagesByPlace[key] ?? []).filter(
            (message) =>
              unreadContribution(message, seq, entry.selfKey).mentions > 0,
          ).length,
        },
      }));
      void backend.markRead(place, seq);
    },

    setStatus(status, note) {
      void backend.setStatus(status, note).catch(() => undefined);
    },

    createReplyLater(message, delayMs = DEFAULT_REPLY_LATER_REMIND_MS) {
      void backend
        .createReplyLater(
          message.place,
          message.messageId,
          Date.now() + delayMs,
        )
        .catch(() => undefined);
    },

    toggleReaction(message, emoji) {
      const clientNonce = secureRandomUUID();
      void enqueueReactionProjection(
        message.place,
        async (operationBackend, isCurrent) => {
          const canonical = await operationBackend.toggleReaction(
            message.place,
            message.messageId,
            emoji,
            clientNonce,
          );
          if (!isCurrent()) return () => undefined;
          if (canonical.messageId !== message.messageId) {
            throw new Error("Reaction acknowledgement target mismatch");
          }
          const acknowledgement: ReactionUpdatedEvent = {
            type: "reaction_updated",
            place: message.place,
            messageId: canonical.messageId,
            reactions: canonical.reactions,
          };
          return () => applyReactionUpdateRaw(acknowledgement);
        },
      ).catch(() => undefined);
    },

    async loadOlder(key) {
      const state = get();
      const place = parsePlaceKey(key);
      const current = state.messagesByPlace[key];
      if (!place || !current || current.length === 0) return;
      if (state.loadingOlderByPlace[key] || !state.hasMoreByPlace[key]) return;
      set((entry) => ({
        loadingOlderByPlace: { ...entry.loadingOlderByPlace, [key]: true },
      }));
      const older = await backend.fetchMessages(place, {
        beforeSeq: current[0].seq,
        limit: PAGE_SIZE,
      });
      set((entry) => {
        const existing = entry.messagesByPlace[key] ?? [];
        const known = new Set(existing.map((m) => m.messageId));
        const fresh = older.filter((m) => !known.has(m.messageId));
        return {
          messagesByPlace: {
            ...entry.messagesByPlace,
            [key]: [...fresh, ...existing],
          },
          hasMoreByPlace: {
            ...entry.hasMoreByPlace,
            [key]: older.length >= PAGE_SIZE,
          },
          loadingOlderByPlace: { ...entry.loadingOlderByPlace, [key]: false },
        };
      });
    },

    resolveReplyLater(markerId) {
      void backend.resolveReplyLater(markerId).catch(() => undefined);
    },

    sendTyping() {
      const key = get().activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (place) backend.sendTyping(place);
    },
  };
});

let messagingSessionIdentity: string | null = null;

export function getMessagingSessionIdentity(): string | null {
  return messagingSessionIdentity;
}

/**
 * Re-read authoritative presentation profiles after a canonical Human rename.
 * A rename may also change contextual agent labels (for example `Sumi（たっけ）`),
 * so patching only the signed-in Human would leave the current view inconsistent.
 */
export async function refreshMessagingMemberProfiles(): Promise<void> {
  const state = useMessaging.getState();
  if (!state.ready || !state.selfKey) return;

  const currentBackend = backend;
  const currentIdentity = messagingSessionIdentity;
  const expectedSelfKey = state.selfKey;
  const snapshot = await currentBackend.bootstrap();
  if (
    backend !== currentBackend ||
    messagingSessionIdentity !== currentIdentity ||
    useMessaging.getState().selfKey !== expectedSelfKey ||
    participantKey(snapshot.self) !== expectedSelfKey
  ) {
    throw new Error("Messaging session changed during profile refresh");
  }

  const membersByKey: Record<ParticipantKey, MemberProfile> = {};
  for (const member of snapshot.members) {
    membersByKey[participantKey(member.participant)] = member;
  }
  useMessaging.setState({ membersByKey });
}

export function bindMessagingSessionIdentity(identity: string | null): void {
  if (identity === messagingSessionIdentity) return;
  messagingSessionIdentity = identity;
  messagingSessionGeneration += 1;
  reactionProjectionByPlace.clear();
  backend.dispose();
  backend = new ApiMessagingBackend();
  initialized = false;
  useMessaging.setState({
    capabilities: backend.capabilities,
    ready: false,
    self: null,
    selfKey: "",
    workspaces: [],
    channels: [],
    dms: [],
    membersByKey: {},
    statusByKey: {},
    messagesByPlace: {},
    pendingByPlace: {},
    lastReadByPlace: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    unreadLineByPlace: {},
    draftByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    employedAgents: [],
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "disconnected",
  });
}
