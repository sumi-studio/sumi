import { create } from "zustand";
import { secureRandomUUID } from "../lib/random-uuid";
import { ApiMessagingBackend } from "./api-backend";
import type { DraftAttachment } from "./draft-attachments";
import { attachmentUploadFailureCode } from "./draft-attachments";
import { useCall } from "./call/call-store";
import { hasDisplayMention } from "./mention";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  MemberProfile,
  Message,
  MessageSearchResult,
  MessagingBackend,
  MessagingCapabilities,
  NotificationLevel,
  NotificationSetting,
  ParticipantKey,
  ParticipantRef,
  ParticipantStatus,
  Place,
  PlaceKey,
  ReactionSummary,
  ReplyLaterMarker,
  ServerEvent,
  StatusKind,
  ThreadSummary,
  Urgency,
  WorkspaceSummary,
} from "./model";
import {
  MAX_ATTACHMENT_BYTES,
  MAX_ATTACHMENTS_PER_MESSAGE,
  parsePlaceKey,
  participantKey,
  placeKey,
} from "./model";
import {
  isNotificationSoundEnabled,
  isTabActive,
  notificationBody,
  notificationPermission,
  notificationTitle,
  setNotificationSoundEnabled as persistNotificationSound,
  playNotificationSound,
  presentationFor,
  presentDesktopNotification,
} from "./notifications";
import {
  getActiveMessagingScope,
  type MessagingScope,
  sameMessagingScope,
  setActiveMessagingScope,
  validateMessagingScope,
} from "./scope";
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

const UNBOUND_CAPABILITIES: MessagingCapabilities = {
  status: false,
  replyLater: false,
  reactions: false,
  notifications: false,
  threads: false,
};

function unboundMessagingBackend(): MessagingBackend {
  const target = {
    capabilities: UNBOUND_CAPABILITIES,
    dispose() {},
  } as Partial<MessagingBackend>;
  return new Proxy(target as MessagingBackend, {
    get(value, property, receiver) {
      if (property in value) return Reflect.get(value, property, receiver);
      return () => {
        throw new Error("Messaging scope is not bound");
      };
    },
  });
}

let backend: MessagingBackend = unboundMessagingBackend();

/**
 * draft添付のbytesと進行中のupload。zustand stateにはメタデータだけを置き、
 * Fileと AbortController はここで持つ。resetで必ず全部止めて捨てる。
 */
const draftFiles = new Map<
  string,
  { file: File; controller: AbortController | null }
>();

function rememberDraftFile(clientNonce: string, file: File): void {
  draftFiles.set(clientNonce, { file, controller: null });
}

function releaseDraftFile(clientNonce: string): void {
  const entry = draftFiles.get(clientNonce);
  if (!entry) return;
  entry.controller?.abort();
  draftFiles.delete(clientNonce);
}

function releaseAllDraftFiles(): void {
  for (const clientNonce of [...draftFiles.keys()])
    releaseDraftFile(clientNonce);
}

/** Tests and explicit development harnesses may replace the transport before init. */
export function installMessagingBackend(override: MessagingBackend): void {
  if (initialized) throw new Error("Messaging backend is already initialized");
  backend.dispose();
  backend = override;
}

export type NotificationWriteResult = "confirmed" | "superseded" | "failed";

interface MessagingState {
  capabilities: MessagingCapabilities;
  ready: boolean;
  self: ParticipantRef | null;
  selfKey: ParticipantKey;
  workspaces: WorkspaceSummary[];
  channels: ChannelSummary[];
  dms: DmSummary[];
  threadsById: Record<string, ThreadSummary>;
  threadsLoadedForPlace: Record<PlaceKey, boolean>;
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
  /**
   * composerに積まれた添付。placeごと・現在のscopeとsessionだけに属し、
   * scope/session切替のresetで消える。bytesはここではなくモジュール内に置く。
   */
  draftAttachmentsByPlace: Record<PlaceKey, DraftAttachment[]>;
  /** 直近の追加操作で上限により受け付けられなかった件数。無言で捨てない。 */
  draftAttachmentOverflowByPlace: Record<PlaceKey, number>;
  typingByPlace: Record<PlaceKey, Record<ParticipantKey, number>>;
  replyLaterById: Record<string, ReplyLaterMarker>;
  /** 自分の通知設定。正本はサーバーで、ここはその写し。 */
  notificationDefaultLevel: NotificationLevel;
  notificationLevelByPlace: Record<PlaceKey, NotificationLevel>;
  notificationKeywords: string[];
  /** 音は端末の都合なのでlocalStorageに置く（設定の正本には混ぜない）。 */
  notificationSoundEnabled: boolean;
  employedAgents: ParticipantRef[];
  hasMoreByPlace: Record<PlaceKey, boolean>;
  loadingOlderByPlace: Record<PlaceKey, boolean>;
  activePlaceKey: PlaceKey | null;
  editingMessageId: string | null;
  replyTargetId: string | null;
  connection: ConnectionState;
  /**
   * True once this transport authority has been connected at least once, so an
   * interruption can be told apart from the initial handshake even by UI that
   * mounts while the interruption is already under way. Reset with the scope.
   */
  everConnected: boolean;
  /** Changes synchronously whenever the exact transport authority is replaced. */
  transportGeneration: number;

  init(): void;
  selectPlace(key: PlaceKey): void;
  clearPlaceSelection(): void;
  createChannel(
    workspaceId: string,
    name: string,
    topic: string,
    voice: boolean,
  ): Promise<PlaceKey>;
  /** 1人ならDM（既存があれば再利用）、複数人ならグループDMを開く。 */
  startDM(participants: ParticipantRef[]): Promise<PlaceKey>;
  updateChannelTopic(channelId: string, topic: string): Promise<void>;
  loadThreads(parentKey: PlaceKey): Promise<void>;
  loadThread(threadId: string): Promise<boolean>;
  createThread(
    parentKey: PlaceKey,
    name: string,
    originMessageId: string | null,
  ): Promise<PlaceKey>;
  searchMessages(query: string): Promise<MessageSearchResult[]>;
  loadPlaceAround(key: PlaceKey, seq: number): Promise<boolean>;
  setDraft(key: PlaceKey, draft: string): void;
  /** 選択・貼り付け・ドロップされたファイルを現在のplaceのdraftへ積み、uploadを始める。 */
  addDraftAttachments(files: File[]): void;
  removeDraftAttachment(clientNonce: string): void;
  retryDraftAttachment(clientNonce: string): void;
  /** 添付付き送信の可否: 本文か添付があり、uploadが全部終わっているとき。 */
  send(content: string, urgency: Urgency): void;
  retrySend(clientNonce: string): void;
  attachmentURL(attachmentId: string): string;
  startEdit(messageId: string): void;
  cancelEdit(): void;
  submitEdit(content: string): void;
  deleteMessage(messageId: string): void;
  setReplyTarget(messageId: string | null): void;
  noteReadUpTo(key: PlaceKey, seq: number): void;
  setStatus(status: StatusKind, note: string): void;
  setPlaceNotificationLevel(
    key: PlaceKey,
    level: NotificationLevel,
  ): Promise<NotificationWriteResult>;
  setNotificationDefaultLevel(
    level: NotificationLevel,
  ): Promise<NotificationWriteResult>;
  setNotificationKeywords(keywords: string[]): Promise<NotificationWriteResult>;
  setNotificationSoundEnabled(
    enabled: boolean,
  ): Promise<NotificationWriteResult>;
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
let statusExpiryTimer: ReturnType<typeof setTimeout> | null = null;
type PresenceProjection =
  | { type: "status"; status: ParticipantStatus }
  | { type: "reply_later"; marker: ReplyLaterMarker }
  | { type: "reply_later_resolved"; markerId: string };
let presenceResyncGeneration = 0;
let pendingPresenceResync: {
  generation: number;
  projections: PresenceProjection[];
} | null = null;

/** 遠い期限でもtimerを一度に張らない上限。起きたら残りをもう一度張り直す。 */
const STATUS_EXPIRY_MAX_DELAY_MS = 60 * 60_000;

/** 通知設定のうち、サーバーと共有している部分だけを取り出した形。 */
type NotificationSettingState = Pick<
  MessagingState,
  | "notificationDefaultLevel"
  | "notificationLevelByPlace"
  | "notificationKeywords"
>;

function notificationSettingState(
  setting: NotificationSetting,
): NotificationSettingState {
  const notificationLevelByPlace: Record<PlaceKey, NotificationLevel> = {};
  for (const entry of setting.perPlace) {
    notificationLevelByPlace[placeKey(entry.place)] = entry.level;
  }
  return {
    notificationDefaultLevel: setting.defaults.level,
    notificationLevelByPlace,
    notificationKeywords: setting.keywords,
  };
}

/**
 * 通知設定のPUTは全置換なので、二本同時に飛ばすと着順しだいで古いsnapshotが
 * サーバーの正になる。書き込みは一本の列に並べ、送る番が来た時点でもっと新しい
 * 設定になっていたら古い方は送らずに畳む——全置換だから最新の一本で足りる。
 */
let notificationWriteChain: Promise<void> = Promise.resolve();
/** 手元の設定がどの書き込みのものか。追い越された書き込みは手元を動かさない。 */
let notificationWriteGeneration = 0;
/** サーバーが最後に確定を返した設定。失敗時に戻る先はここで、送信前の手元ではない。 */
let confirmedNotificationSetting: NotificationSettingState | null = null;

let notificationNavigate: ((key: PlaceKey) => void) | null = null;

/**
 * 通知をクリックした先の遷移。URLが現在地の正本なので、storeが自前で
 * activePlaceKeyを書き換えるのではなくrouterの遷移を借りる。
 */
export function setNotificationNavigator(
  navigate: ((key: PlaceKey) => void) | null,
): void {
  notificationNavigate = navigate;
}

/** placeに効いている通知レベル。place個別の指定が無ければ既定に落ちる。 */
export function notificationLevelFor(
  state: Pick<
    MessagingState,
    "notificationLevelByPlace" | "notificationDefaultLevel"
  >,
  key: PlaceKey,
): NotificationLevel {
  return state.notificationLevelByPlace[key] ?? state.notificationDefaultLevel;
}

/**
 * 通知の見出しに使う場所の名前。DMは相手の名前が発言者の名前と同じなので
 * 場所を名乗らせない（「Haru — Haru」は情報が無い）。
 */
function notificationPlaceLabel(state: MessagingState, key: PlaceKey): string {
  const place = parsePlaceKey(key);
  if (!place) return "";
  if (place.kind === "channel") {
    const channel = state.channels.find(
      (entry) => entry.channelId === place.channelId,
    );
    return channel ? `#${channel.name}` : "";
  }
  if (place.kind === "dm") return "";
  const dm = state.dms.find(
    (entry) => entry.kind === place.kind && entry.dmId === place.dmId,
  );
  if (!dm) return "";
  return dm.participants
    .filter((ref) => participantKey(ref) !== state.selfKey)
    .map(
      (ref) => state.membersByKey[participantKey(ref)]?.displayName ?? "不明",
    )
    .join("、");
}

export const useMessaging = create<MessagingState>((set, get) => {
  if (import.meta.env.DEV) {
    // 開発時のデバッグ・E2E検証用のstate参照口。
    (globalThis as Record<string, unknown>).__sumiMessaging = () => get();
  }
  const threadProjectionVersions = new Map<string, number>();
  const appliedThreadDeletions = new Set<string>();

  const advanceThreadProjection = (message: Message): number | null => {
    if (message.place.kind !== "thread") return null;
    const threadId = message.place.threadId;
    const version = (threadProjectionVersions.get(threadId) ?? 0) + 1;
    threadProjectionVersions.set(threadId, version);
    return version;
  };

  const refreshThreadSummary = async (message: Message, version: number) => {
    if (message.place.kind !== "thread") return;
    const threadId = message.place.threadId;
    const thread = get().threadsById[threadId];
    const fetchThreads = backend.fetchThreads;
    if (!thread || !fetchThreads) return;
    const currentBackend = backend;
    const sessionGeneration = messagingSessionGeneration;
    const parentKey = placeKey(thread.parentPlace);
    try {
      const summaries = await fetchThreads.call(
        currentBackend,
        thread.parentPlace,
      );
      if (
        backend !== currentBackend ||
        messagingSessionGeneration !== sessionGeneration ||
        threadProjectionVersions.get(threadId) !== version
      ) {
        return;
      }
      const refreshed = summaries.find((entry) => entry.threadId === threadId);
      if (!refreshed) return;
      set((state) => ({
        threadsById: { ...state.threadsById, [threadId]: refreshed },
      }));
    } catch {
      if (
        backend !== currentBackend ||
        messagingSessionGeneration !== sessionGeneration ||
        threadProjectionVersions.get(threadId) !== version
      ) {
        return;
      }
      set((state) => ({
        threadsLoadedForPlace: {
          ...state.threadsLoadedForPlace,
          [parentKey]: false,
        },
      }));
    }
  };

  const noteThreadActivity = (message: Message) => {
    if (message.place.kind !== "thread") return;
    const threadId = message.place.threadId;
    set((state) => {
      const thread = state.threadsById[threadId];
      if (!thread) return {};
      const known = thread.participants.some(
        (participant) =>
          participantKey(participant) === participantKey(message.author),
      );
      return {
        threadsById: {
          ...state.threadsById,
          [threadId]: {
            ...thread,
            messageCount: thread.messageCount + (message.deleted ? 0 : 1),
            lastMessageAt: message.createdAt,
            lastMessage: message.content,
            latestSeq: Math.max(thread.latestSeq, message.seq),
            participants: known
              ? thread.participants
              : [...thread.participants, message.author],
          },
        },
      };
    });
  };

  const applyThreadDeletionSummary = (message: Message) => {
    if (message.place.kind !== "thread") return;
    const threadId = message.place.threadId;
    const deletionKey = `${threadId}:${message.messageId}`;
    if (!appliedThreadDeletions.has(deletionKey)) {
      appliedThreadDeletions.add(deletionKey);
      set((state) => {
        const thread = state.threadsById[threadId];
        if (!thread) return {};
        return {
          threadsById: {
            ...state.threadsById,
            [threadId]: {
              ...thread,
              messageCount: Math.max(0, thread.messageCount - 1),
            },
          },
        };
      });
    }
    const version = advanceThreadProjection(message);
    if (version !== null) void refreshThreadSummary(message, version);
  };
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
   * 期限切れのstatusは「まだ取り込み中に見える」より「何も申告していない」が
   * 正しい。serverは読み出し時に落とすだけで失効eventを送らないので、
   * 期限に達した分はこちらで落とす。
   */
  const withoutExpired = (
    statuses: Record<ParticipantKey, ParticipantStatus>,
    now: number,
  ): Record<ParticipantKey, ParticipantStatus> => {
    const live: Record<ParticipantKey, ParticipantStatus> = {};
    let dropped = false;
    for (const [key, status] of Object.entries(statuses)) {
      if (status.expiresAt !== null && status.expiresAt <= now) {
        dropped = true;
        continue;
      }
      live[key] = status;
    }
    return dropped ? live : statuses;
  };

  const scheduleStatusExpiry = () => {
    if (statusExpiryTimer !== null) clearTimeout(statusExpiryTimer);
    statusExpiryTimer = null;
    let nearest: number | null = null;
    for (const status of Object.values(get().statusByKey)) {
      if (status.expiresAt === null) continue;
      if (nearest === null || status.expiresAt < nearest) {
        nearest = status.expiresAt;
      }
    }
    if (nearest === null) return;
    const delay = Math.min(
      Math.max(0, nearest - Date.now()),
      STATUS_EXPIRY_MAX_DELAY_MS,
    );
    statusExpiryTimer = setTimeout(() => {
      statusExpiryTimer = null;
      set((state) => ({
        statusByKey: withoutExpired(state.statusByKey, Date.now()),
      }));
      scheduleStatusExpiry();
    }, delay);
  };

  const applyStatuses = (statuses: ParticipantStatus[]) => {
    const statusByKey: Record<ParticipantKey, ParticipantStatus> = {};
    for (const status of statuses) {
      statusByKey[participantKey(status.participant)] = status;
    }
    return withoutExpired(statusByKey, Date.now());
  };

  /** 一人分の申告を置き換える。WS echoとRESTのACKはどちらが先でも同じ形。 */
  const applyStatus = (status: ParticipantStatus) => {
    set((state) => ({
      statusByKey: withoutExpired(
        { ...state.statusByKey, [participantKey(status.participant)]: status },
        Date.now(),
      ),
    }));
    scheduleStatusExpiry();
  };

  /**
   * markerの現在値を書き込む。RESTのACKとWS echoは同じmarkerを二度運ぶので、
   * 順序に関わらず収束するよう「解けた約束は解けたまま」「一度知った自分の
   * リマインド予定は他人向けwireのnullで消さない」を保つ。
   */
  const applyReplyLater = (marker: ReplyLaterMarker) => {
    set((state) => {
      const known = state.replyLaterById[marker.markerId];
      return {
        replyLaterById: {
          ...state.replyLaterById,
          [marker.markerId]: {
            ...marker,
            remindAt: marker.remindAt ?? known?.remindAt ?? null,
            resolved: marker.resolved || (known?.resolved ?? false),
          },
        },
      };
    });
  };

  const applyPresenceProjection = (
    projection: PresenceProjection,
    bufferDuringResync = true,
  ) => {
    if (bufferDuringResync) {
      pendingPresenceResync?.projections.push(projection);
    }
    if (projection.type === "status") {
      applyStatus(projection.status);
      return;
    }
    if (projection.type === "reply_later") {
      applyReplyLater(projection.marker);
      return;
    }
    set((state) => {
      const marker = state.replyLaterById[projection.markerId];
      if (!marker) return {};
      return {
        replyLaterById: {
          ...state.replyLaterById,
          [projection.markerId]: { ...marker, resolved: true },
        },
      };
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

  /**
   * 再接続時の再同期。status_updatedはvolatileでreplayされず、reply-laterの
   * eventもplaceのseq catch-upには載らない。切断中に他の参加者がstatusを
   * 変えたりmarkerを作った／解いたりした分は、cursorでは戻らない。
   */
  const resyncPresence = async () => {
    const currentBackend = backend;
    const sessionGeneration = messagingSessionGeneration;
    const resync = {
      generation: ++presenceResyncGeneration,
      // このfetchより前のprojectionはsnapshotに含まれる。先行generationの
      // queueを継ぐと、snapshot内の後続状態を古いprojectionで巻き戻し得る。
      projections: [] as PresenceProjection[],
    };
    pendingPresenceResync = resync;
    try {
      const presence = await currentBackend.fetchPresence();
      if (
        backend !== currentBackend ||
        pendingPresenceResync !== resync ||
        presenceResyncGeneration !== resync.generation ||
        messagingSessionGeneration !== sessionGeneration
      ) {
        return;
      }
      const replyLaterById: Record<string, ReplyLaterMarker> = {};
      for (const marker of presence.replyLaterMarkers) {
        replyLaterById[marker.markerId] = marker;
      }
      // Stop buffering before replaying, otherwise the replay would append to
      // its own queue forever. Events were already applied live; replaying them
      // now restores anything the older wholesale snapshot replaced.
      pendingPresenceResync = null;
      set({ statusByKey: applyStatuses(presence.statuses), replyLaterById });
      scheduleStatusExpiry();
      for (const projection of resync.projections) {
        applyPresenceProjection(projection, false);
      }
    } finally {
      if (pendingPresenceResync === resync) pendingPresenceResync = null;
    }
  };

  /**
   * 呼ばれたことの提示。「呼ぶかどうか」はサーバーが送信時に判定済みで、
   * ここに来る `notify` はその答えそのもの。クライアントは提示の仕方だけを
   * 決める——見ている画面に通知を重ねない、音を鳴らすか。
   */
  const presentNotification = (
    event: Extract<ServerEvent, { type: "message_created" }>,
  ) => {
    const state = get();
    if (!state.capabilities.notifications) return;
    const key = placeKey(event.message.place);
    const presentation = presentationFor({
      notify: event.notify,
      authorIsSelf: participantKey(event.message.author) === state.selfKey,
      tabActive: isTabActive(),
      placeIsActive: state.activePlaceKey === key,
      permission: notificationPermission(),
      soundEnabled: state.notificationSoundEnabled,
    });
    if (presentation.sound) playNotificationSound();
    if (!presentation.desktop) return;
    const authorName =
      state.membersByKey[participantKey(event.message.author)]?.displayName ??
      "誰か";
    presentDesktopNotification({
      title: notificationTitle(notificationPlaceLabel(state, key), authorName),
      body: notificationBody(event.message.content, event.message.attachments),
      placeKey: key,
      onActivate: () => notificationNavigate?.(key),
    });
  };

  const applyEvent = (
    event: Parameters<Parameters<MessagingBackend["subscribe"]>[0]>[0],
  ) => {
    if (event.type === "call_state") {
      useCall.getState().applyCallState(event.call);
      return;
    }
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
      if (event.type === "message_created") {
        advanceThreadProjection(event.message);
        noteThreadActivity(event.message);
        presentNotification(event);
      } else {
        const version = advanceThreadProjection(event.message);
        if (version !== null) void refreshThreadSummary(event.message, version);
      }
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
      applyThreadDeletionSummary(event.message);
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
      applyPresenceProjection({ type: "status", status: event.status });
      return;
    }
    if (event.type === "reply_later_created") {
      applyPresenceProjection({ type: "reply_later", marker: event.marker });
      return;
    }
    if (event.type === "reply_later_resolved") {
      applyPresenceProjection({
        type: "reply_later_resolved",
        markerId: event.markerId,
      });
      return;
    }
    if (event.type === "place_created") {
      const { channel, dm, thread } = event;
      set((state) => {
        if (thread)
          return {
            threadsById: { ...state.threadsById, [thread.threadId]: thread },
          };
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
        attachments: pending.attachments.map((entry) => entry.attachmentId),
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

  // upload結果はscope/session/backend世代とdraftの存在を全部確かめてから反映する。
  // 別のWorkspaceに切り替わったあとに戻ってきた受領は、前のplaceの添付として
  // 別のメッセージへ載る危険があるので捨てる。
  const dispatchUpload = (
    key: PlaceKey,
    place: Place,
    draft: DraftAttachment,
  ) => {
    const entry = draftFiles.get(draft.clientNonce);
    if (!entry) return;
    const controller = new AbortController();
    entry.controller = controller;
    const currentBackend = backend;
    const sessionGeneration = messagingSessionGeneration;
    const stillLive = () =>
      backend === currentBackend &&
      messagingSessionGeneration === sessionGeneration &&
      (get().draftAttachmentsByPlace[key] ?? []).some(
        (candidate) => candidate.clientNonce === draft.clientNonce,
      );
    const patch = (next: Partial<DraftAttachment>) =>
      set((current) => ({
        draftAttachmentsByPlace: {
          ...current.draftAttachmentsByPlace,
          [key]: (current.draftAttachmentsByPlace[key] ?? []).map(
            (candidate) =>
              candidate.clientNonce === draft.clientNonce
                ? { ...candidate, ...next }
                : candidate,
          ),
        },
      }));
    currentBackend
      .uploadAttachment({
        place,
        clientNonce: draft.clientNonce,
        filename: draft.filename,
        contentType: draft.contentType,
        body: entry.file,
        signal: controller.signal,
      })
      .then((receipt) => {
        if (!stillLive()) return;
        patch({
          status: "ready",
          attachment: receipt.attachment,
          errorCode: undefined,
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted || !stillLive()) return;
        patch({
          status: "failed",
          errorCode: attachmentUploadFailureCode(error),
        });
      });
  };

  /**
   * 通知設定は丸ごと置き換える。手元を先に動かして即座に反映し、失敗したら
   * 元に戻す——設定が効いたふりをして黙って効いていないのが一番困る。
   * 送信は列に並べる。手元だけ新しくてサーバーが古いままになる形の食い違いは、
   * 次に読み直すまで誰も気付けないので、着順に頼らない。
   */
  const pushNotificationSetting = (next: {
    defaultLevel: NotificationLevel;
    levelByPlace: Record<PlaceKey, NotificationLevel>;
    keywords: string[];
  }): Promise<NotificationWriteResult> => {
    const sessionBackend = backend;
    const state = get();
    const previous: NotificationSettingState = {
      notificationDefaultLevel: state.notificationDefaultLevel,
      notificationLevelByPlace: state.notificationLevelByPlace,
      notificationKeywords: state.notificationKeywords,
    };
    const generation = ++notificationWriteGeneration;
    set({
      notificationDefaultLevel: next.defaultLevel,
      notificationLevelByPlace: next.levelByPlace,
      notificationKeywords: next.keywords,
    });
    const result = notificationWriteChain.then(
      async (): Promise<NotificationWriteResult> => {
        // bindMessagingSessionIdentity resets the public chain, but a callback
        // already queued on the old promise still exists. Never let it use the
        // replacement backend or mutate the replacement session's confirmed
        // rollback point.
        if (backend !== sessionBackend) return "superseded";
        // 送る番が来るまでにもっと新しい設定になっていたら、この一本は要らない。
        if (generation !== notificationWriteGeneration) return "superseded";
        const perPlace: { place: Place; level: NotificationLevel }[] = [];
        for (const [key, level] of Object.entries(next.levelByPlace)) {
          const place = parsePlaceKey(key);
          if (place) perPlace.push({ place, level });
        }
        try {
          const confirmed = notificationSettingState(
            await sessionBackend.setNotificationSetting({
              defaults: { level: next.defaultLevel },
              perPlace,
              keywords: next.keywords,
            }),
          );
          if (backend !== sessionBackend) return "superseded";
          confirmedNotificationSetting = confirmed;
          // 追い越されていれば後続の書き込みが正。確定値は覚えるが手元は触らない。
          if (generation !== notificationWriteGeneration) return "superseded";
          set(confirmed);
          return "confirmed";
        } catch {
          if (
            backend !== sessionBackend ||
            generation !== notificationWriteGeneration
          ) {
            return "superseded";
          }
          set(confirmedNotificationSetting ?? previous);
          return "failed";
        }
      },
    );
    notificationWriteChain = result.then(() => undefined);
    return result;
  };

  return {
    capabilities: backend.capabilities,
    ready: false,
    self: null,
    selfKey: "",
    workspaces: [],
    channels: [],
    dms: [],
    threadsById: {},
    threadsLoadedForPlace: {},
    membersByKey: {},
    statusByKey: {},
    messagesByPlace: {},
    pendingByPlace: {},
    lastReadByPlace: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    unreadLineByPlace: {},
    draftByPlace: {},
    draftAttachmentsByPlace: {},
    draftAttachmentOverflowByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    notificationDefaultLevel: "all",
    notificationLevelByPlace: {},
    notificationKeywords: [],
    notificationSoundEnabled: isNotificationSoundEnabled(),
    employedAgents: [],
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "disconnected",
    everConnected: false,
    transportGeneration: 0,

    init() {
      if (initialized) return;
      initialized = true;
      threadProjectionVersions.clear();
      appliedThreadDeletions.clear();
      void backend
        .bootstrap()
        .then((snapshot) => {
          const membersByKey: Record<ParticipantKey, MemberProfile> = {};
          for (const member of snapshot.members) {
            membersByKey[participantKey(member.participant)] = member;
          }
          const statusByKey = applyStatuses(snapshot.statuses);
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
          // bootstrapが運ぶ設定はサーバーの確定値。書き込みが失敗したときの
          // 戻り先はここから始まる。
          confirmedNotificationSetting = notificationSettingState(
            snapshot.notificationSetting,
          );
          set({
            ready: true,
            capabilities: backend.capabilities,
            self: snapshot.self,
            selfKey: participantKey(snapshot.self),
            workspaces: snapshot.workspaces,
            channels: snapshot.channels,
            dms: snapshot.dms,
            threadsById: Object.fromEntries(
              (snapshot.threads ?? []).map((thread) => [
                thread.threadId,
                thread,
              ]),
            ),
            threadsLoadedForPlace: {},
            membersByKey,
            statusByKey,
            lastReadByPlace,
            unreadCountByPlace,
            mentionCountByPlace,
            replyLaterById,
            ...confirmedNotificationSetting,
            employedAgents: snapshot.employedAgents,
          });
          scheduleStatusExpiry();
          backend.subscribe(applyEvent, { sinceByPlace });
          // 最初のconnectedはいま読んだこのbootstrapが正本。以降のconnectedは
          // 再接続なので、replayされないplace lifecycleを読み直す。presenceは
          // bootstrap-to-subscribe gapも閉じるため初回を含む毎回で取り直す。
          let connectedOnce = false;
          backend.subscribeConnection((connection) => {
            set((state) => ({
              connection,
              everConnected: state.everConnected || connection === "connected",
            }));
            if (connection !== "connected") return;
            void useCall.getState().hydrate();
            if (connectedOnce) {
              void reconcilePlaces().catch(() => undefined);
            } else {
              connectedOnce = true;
            }
            void resyncPresence().catch(() => undefined);
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
          : place.kind === "thread"
            ? state.threadsById[place.threadId] !== undefined
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

    clearPlaceSelection() {
      const state = get();
      if (
        state.activePlaceKey === null &&
        state.editingMessageId === null &&
        state.replyTargetId === null
      ) {
        return;
      }
      set({
        activePlaceKey: null,
        editingMessageId: null,
        replyTargetId: null,
      });
    },

    async createChannel(workspaceId, name, topic, voice) {
      const currentBackend = backend;
      const currentIdentity = getMessagingSessionIdentity();
      const expectedSelfKey = get().selfKey;
      const channel = await currentBackend.createChannel(
        workspaceId,
        name,
        topic,
        voice,
      );
      if (
        backend !== currentBackend ||
        getMessagingSessionIdentity() !== currentIdentity ||
        get().selfKey !== expectedSelfKey
      ) {
        throw new Error("Messaging session changed during channel creation");
      }
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

    async loadThreads(parentKey) {
      if (!get().capabilities.threads) return;
      if (get().threadsLoadedForPlace[parentKey]) return;
      const parent = parsePlaceKey(parentKey);
      if (!parent || parent.kind !== "channel") return;
      const currentBackend = backend;
      const sessionGeneration = messagingSessionGeneration;
      const versions = new Map(threadProjectionVersions);
      const threads = (await currentBackend.fetchThreads?.(parent)) ?? [];
      if (
        backend !== currentBackend ||
        messagingSessionGeneration !== sessionGeneration
      ) {
        return;
      }
      let raced = false;
      set((state) => ({
        threadsById: {
          ...state.threadsById,
          ...Object.fromEntries(
            threads
              .filter((thread) => {
                const unchanged =
                  (threadProjectionVersions.get(thread.threadId) ?? 0) ===
                  (versions.get(thread.threadId) ?? 0);
                if (!unchanged) raced = true;
                return unchanged;
              })
              .map((thread) => [thread.threadId, thread]),
          ),
        },
        threadsLoadedForPlace: {
          ...state.threadsLoadedForPlace,
          [parentKey]: !raced,
        },
      }));
    },

    async loadThread(threadId) {
      if (get().threadsById[threadId]) return true;
      const currentBackend = backend;
      const sessionGeneration = messagingSessionGeneration;
      if (!get().capabilities.threads || !currentBackend.fetchThread) {
        return false;
      }
      try {
        const thread = await currentBackend.fetchThread(threadId);
        if (
          backend !== currentBackend ||
          messagingSessionGeneration !== sessionGeneration
        ) {
          return false;
        }
        set((state) => ({
          threadsById: { ...state.threadsById, [thread.threadId]: thread },
        }));
        return true;
      } catch {
        return false;
      }
    },

    async createThread(parentKey, name, originMessageId) {
      const parent = parsePlaceKey(parentKey);
      if (!parent || parent.kind !== "channel")
        throw new Error("Threads require a channel parent");
      if (!backend.createThread) throw new Error("Threads are unavailable");
      const thread = await backend.createThread(parent, name, originMessageId);
      set((state) => ({
        threadsById: { ...state.threadsById, [thread.threadId]: thread },
      }));
      return `thread:${thread.threadId}`;
    },

    async searchMessages(query) {
      return backend.searchMessages(query);
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
      if (!key || !place || !state.self) return;
      const drafts = state.draftAttachmentsByPlace[key] ?? [];
      // 添付が1件でも上がりきっていなければ送らない。半端な添付で送るくらいなら
      // 送信ボタンを押せない方が正直である。
      if (drafts.some((entry) => entry.status !== "ready")) return;
      const attachments = drafts.flatMap((entry) =>
        entry.attachment ? [entry.attachment] : [],
      );
      if (!trimmed && attachments.length === 0) return;
      const pending: PendingMessage = {
        clientNonce: secureRandomUUID(),
        content: trimmed,
        mentions: resolveMentions(trimmed, state.membersByKey, state.selfKey),
        urgency,
        replyTo: state.replyTargetId,
        attachments,
        createdAt: Date.now(),
      };
      for (const entry of drafts) releaseDraftFile(entry.clientNonce);
      set((current) => ({
        pendingByPlace: {
          ...current.pendingByPlace,
          [key]: [...(current.pendingByPlace[key] ?? []), pending],
        },
        draftByPlace: { ...current.draftByPlace, [key]: "" },
        draftAttachmentsByPlace: {
          ...current.draftAttachmentsByPlace,
          [key]: [],
        },
        draftAttachmentOverflowByPlace: {
          ...current.draftAttachmentOverflowByPlace,
          [key]: 0,
        },
        replyTargetId: null,
      }));
      dispatchSend(key, pending);
    },

    addDraftAttachments(files) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (!key || !place || files.length === 0) return;
      const existing = state.draftAttachmentsByPlace[key] ?? [];
      const room = MAX_ATTACHMENTS_PER_MESSAGE - existing.length;
      const accepted = files.slice(0, Math.max(0, room));
      const overflow = files.length - accepted.length;
      const added: DraftAttachment[] = accepted.map((file) => {
        const clientNonce = secureRandomUUID();
        const draft: DraftAttachment = {
          clientNonce,
          filename: file.name || "file",
          sizeBytes: file.size,
          contentType: file.type,
          status: "uploading",
        };
        if (file.size <= 0) {
          return { ...draft, status: "failed", errorCode: "attachment_empty" };
        }
        if (file.size > MAX_ATTACHMENT_BYTES) {
          return {
            ...draft,
            status: "failed",
            errorCode: "attachment_too_large",
          };
        }
        rememberDraftFile(clientNonce, file);
        return draft;
      });
      set((current) => ({
        draftAttachmentsByPlace: {
          ...current.draftAttachmentsByPlace,
          [key]: [...(current.draftAttachmentsByPlace[key] ?? []), ...added],
        },
        draftAttachmentOverflowByPlace: {
          ...current.draftAttachmentOverflowByPlace,
          [key]: overflow,
        },
      }));
      for (const draft of added) {
        if (draft.status === "uploading") dispatchUpload(key, place, draft);
      }
    },

    removeDraftAttachment(clientNonce) {
      const key = get().activePlaceKey;
      if (!key) return;
      releaseDraftFile(clientNonce);
      set((current) => ({
        draftAttachmentsByPlace: {
          ...current.draftAttachmentsByPlace,
          [key]: (current.draftAttachmentsByPlace[key] ?? []).filter(
            (entry) => entry.clientNonce !== clientNonce,
          ),
        },
      }));
    },

    retryDraftAttachment(clientNonce) {
      const key = get().activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (!key || !place) return;
      const draft = (get().draftAttachmentsByPlace[key] ?? []).find(
        (entry) => entry.clientNonce === clientNonce,
      );
      if (draft?.status !== "failed" || !draftFiles.has(clientNonce)) {
        return;
      }
      const retried: DraftAttachment = {
        ...draft,
        status: "uploading",
        errorCode: undefined,
      };
      set((current) => ({
        draftAttachmentsByPlace: {
          ...current.draftAttachmentsByPlace,
          [key]: (current.draftAttachmentsByPlace[key] ?? []).map((entry) =>
            entry.clientNonce === clientNonce ? retried : entry,
          ),
        },
      }));
      dispatchUpload(key, place, retried);
    },

    attachmentURL(attachmentId) {
      return backend.attachmentURL(attachmentId);
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

    // 成功ACKはserverが確定した値そのものなので、socketが再接続中でも
    // これだけで表示とリマインドは収束する。後着のechoは同じ形を上書きする。
    setStatus(status, note) {
      const currentBackend = backend;
      const sessionGeneration = messagingSessionGeneration;
      void currentBackend.setStatus(status, note).then(
        (canonical) => {
          if (
            backend !== currentBackend ||
            messagingSessionGeneration !== sessionGeneration
          ) {
            return;
          }
          applyPresenceProjection({ type: "status", status: canonical });
        },
        () => undefined,
      );
    },

    setPlaceNotificationLevel(key, level) {
      const state = get();
      return pushNotificationSetting({
        defaultLevel: state.notificationDefaultLevel,
        levelByPlace: { ...state.notificationLevelByPlace, [key]: level },
        keywords: state.notificationKeywords,
      });
    },

    setNotificationDefaultLevel(level) {
      const state = get();
      return pushNotificationSetting({
        defaultLevel: level,
        levelByPlace: state.notificationLevelByPlace,
        keywords: state.notificationKeywords,
      });
    },

    setNotificationKeywords(keywords) {
      const state = get();
      return pushNotificationSetting({
        defaultLevel: state.notificationDefaultLevel,
        levelByPlace: state.notificationLevelByPlace,
        keywords,
      });
    },

    // 音は端末の設定。サーバーへは送らないので、他の端末の鳴り方は変わらない。
    setNotificationSoundEnabled(enabled) {
      persistNotificationSound(enabled);
      set({ notificationSoundEnabled: enabled });
      return Promise.resolve("confirmed");
    },

    createReplyLater(message, delayMs = DEFAULT_REPLY_LATER_REMIND_MS) {
      const currentBackend = backend;
      const sessionGeneration = messagingSessionGeneration;
      void currentBackend
        .createReplyLater(
          message.place,
          message.messageId,
          Date.now() + delayMs,
        )
        .then(
          (canonical) => {
            if (
              backend !== currentBackend ||
              messagingSessionGeneration !== sessionGeneration
            ) {
              return;
            }
            applyPresenceProjection({ type: "reply_later", marker: canonical });
          },
          () => undefined,
        );
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
      const currentBackend = backend;
      const sessionGeneration = messagingSessionGeneration;
      void currentBackend.resolveReplyLater(markerId).then(
        (canonical) => {
          if (
            backend !== currentBackend ||
            messagingSessionGeneration !== sessionGeneration
          ) {
            return;
          }
          applyPresenceProjection({ type: "reply_later", marker: canonical });
        },
        () => undefined,
      );
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

export function getMessagingScope(): MessagingScope | null {
  return getActiveMessagingScope();
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
  setActiveMessagingScope(null);
  resetMessagingRuntime(unboundMessagingBackend());
}

/**
 * Binds Messaging to one exact Workspace installation. A scope switch replaces
 * the transport and all private projections synchronously before React can
 * expose the next Workspace subtree.
 */
export function bindMessagingScope(scope: MessagingScope | null): void {
  if (scope !== null && messagingSessionIdentity === null) {
    throw new Error("Messaging scope requires an authenticated Human");
  }
  const exact = scope === null ? null : validateMessagingScope(scope);
  if (sameMessagingScope(getActiveMessagingScope(), exact)) return;
  setActiveMessagingScope(exact);
  resetMessagingRuntime(
    exact === null ? unboundMessagingBackend() : new ApiMessagingBackend(exact),
  );
}

function resetMessagingRuntime(nextBackend: MessagingBackend): void {
  messagingSessionGeneration += 1;
  reactionProjectionByPlace.clear();
  releaseAllDraftFiles();
  backend.dispose();
  useCall.getState().reset();
  backend = nextBackend;
  initialized = false;
  presenceResyncGeneration += 1;
  pendingPresenceResync = null;
  if (statusExpiryTimer !== null) clearTimeout(statusExpiryTimer);
  statusExpiryTimer = null;
  // 前の人宛ての書き込みは、次の人の手元にも新しいbackendにも届かせない。
  notificationWriteChain = Promise.resolve();
  notificationWriteGeneration = 0;
  confirmedNotificationSetting = null;
  useMessaging.setState({
    capabilities: backend.capabilities,
    ready: false,
    self: null,
    selfKey: "",
    workspaces: [],
    channels: [],
    dms: [],
    threadsById: {},
    threadsLoadedForPlace: {},
    membersByKey: {},
    statusByKey: {},
    messagesByPlace: {},
    pendingByPlace: {},
    lastReadByPlace: {},
    unreadCountByPlace: {},
    mentionCountByPlace: {},
    unreadLineByPlace: {},
    draftByPlace: {},
    draftAttachmentsByPlace: {},
    draftAttachmentOverflowByPlace: {},
    typingByPlace: {},
    replyLaterById: {},
    notificationDefaultLevel: "all",
    notificationLevelByPlace: {},
    notificationKeywords: [],
    notificationSoundEnabled: isNotificationSoundEnabled(),
    employedAgents: [],
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "disconnected",
    everConnected: false,
    transportGeneration: messagingSessionGeneration,
  });
}
