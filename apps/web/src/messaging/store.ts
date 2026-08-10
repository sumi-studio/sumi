import { create } from "zustand";
import { secureRandomUUID } from "../lib/random-uuid";
import { ApiMessagingBackend } from "./api-backend";
import { useCall } from "./call/call-store";
import { hasDisplayMention } from "./mention";
import type {
  Attachment,
  AttachmentDraftPatch,
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
  Permission,
  PermissionSet,
  Place,
  PlaceKey,
  PollInput,
  ProfileInput,
  ReplyLaterMarker,
  RoleAssignment,
  RoleInput,
  ServerEvent,
  StatusKind,
  ThreadSummary,
  Urgency,
  WorkspaceRole,
  WorkspaceSummary,
} from "./model";
import { MAX_SEQ, parsePlaceKey, participantKey, placeKey } from "./model";
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
import type { PendingMessage } from "./timeline";
import { mergeMessages, upsertMessage } from "./timeline";

const TYPING_TTL_MS = 4_500;
const DEFAULT_REPLY_LATER_REMIND_MS = 30 * 60_000;

let backend: MessagingBackend = new ApiMessagingBackend();

type CatchUpAwareMessagingBackend = MessagingBackend & {
  subscribeCatchUp?: (
    listener: (place: Place, latestSeq: number) => void | Promise<void>,
  ) => () => void;
};

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
  /**
   * 見えているスレッド。bootstrapでは自分が参加しているものだけが載り、
   * 親チャンネルを開くとその配下が全部足される（閲覧は親のメンバー全員できる）。
   */
  threadsById: Record<string, ThreadSummary>;
  /** スレッド一覧を取り終えた親place。開くたびの再取得を避ける。 */
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
  typingByPlace: Record<PlaceKey, Record<ParticipantKey, number>>;
  replyLaterById: Record<string, ReplyLaterMarker>;
  /** 自分の通知設定。正本はサーバーで、ここはその写し。 */
  notificationDefaultLevel: NotificationLevel;
  notificationLevelByPlace: Record<PlaceKey, NotificationLevel>;
  notificationKeywords: string[];
  /** 音は端末の都合なのでlocalStorageに置く（設定の正本には混ぜない）。 */
  notificationSoundEnabled: boolean;
  employedAgents: ParticipantRef[];
  /** いま居るワークスペースのロールと、自分の権限（正本はサーバー）。 */
  roles: WorkspaceRole[];
  roleAssignments: RoleAssignment[];
  permissions: PermissionSet;
  hasMoreByPlace: Record<PlaceKey, boolean>;
  loadingOlderByPlace: Record<PlaceKey, boolean>;
  activePlaceKey: PlaceKey | null;
  editingMessageId: string | null;
  replyTargetId: string | null;
  connection: ConnectionState;

  init(): void;
  selectPlace(key: PlaceKey): void;
  createChannel(
    name: string,
    topic: string,
    voice?: boolean,
  ): Promise<PlaceKey>;
  /** 1人ならDM（既存があれば再利用）、複数人ならグループDMを開く。 */
  startDM(participants: ParticipantRef[]): Promise<PlaceKey>;
  updateChannel(
    channelId: string,
    input: { name?: string; topic?: string },
  ): Promise<void>;
  /** 同じ形の空のchannelを作り、そのPlaceKeyを返す。 */
  duplicateChannel(channelId: string): Promise<PlaceKey>;
  /** 親チャンネル配下のスレッド一覧を取り込む。取得済みなら何もしない。 */
  loadThreads(parentKey: PlaceKey): Promise<void>;
  /** スレッドを作る。返り値は開く先のPlaceKey。 */
  createThread(
    parentKey: PlaceKey,
    name: string,
    originMessageId: string | null,
  ): Promise<PlaceKey>;
  loadPlaceAround(key: PlaceKey, seq: number): Promise<boolean>;
  /** 可視なplace全体の本文検索。結果はUI局所状態で持ち、storeには残さない。 */
  searchMessages(query: string): Promise<MessageSearchResult[]>;
  setDraft(key: PlaceKey, draft: string): void;
  send(
    content: string,
    urgency: Urgency,
    attachments?: Attachment[],
    poll?: PollInput | null,
  ): void;
  /** 投票する。空配列は取り消し。押した結果はサーバーのechoで確定する。 */
  votePoll(message: Message, optionIds: string[]): void;
  /** 送信前にファイルを預ける。返ったAttachmentをsendへ渡すまで誰にも見えない。 */
  uploadAttachment(file: File): Promise<Attachment>;
  /** 送信前の添付を編集する（名前・概要・ネタバレ）。 */
  updateAttachment(
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ): Promise<Attachment>;
  retrySend(clientNonce: string): void;
  startEdit(messageId: string): void;
  cancelEdit(): void;
  submitEdit(content: string): void;
  deleteMessage(messageId: string): void;
  setReplyTarget(messageId: string | null): void;
  noteReadUpTo(key: PlaceKey, seq: number): void;
  setStatus(status: StatusKind, note: string, expiresAt?: number | null): void;
  /**
   * 自分の名乗りの更新。失敗はUIが伝えるべきことなので握り潰さず投げ返す
   * （設定が効いたふりをして黙って効いていないのが一番困る）。
   */
  updateProfile(input: ProfileInput): Promise<void>;
  /** ロールを取り直す。ライブ配信しない代わりに、設定画面を開いた時点で読む。 */
  refreshRoles(): Promise<void>;
  createRole(input: RoleInput): Promise<void>;
  updateRole(roleId: string, input: RoleInput): Promise<void>;
  deleteRole(roleId: string): Promise<void>;
  setMemberRoles(participant: ParticipantRef, roleIds: string[]): Promise<void>;
  setPlaceNotificationLevel(key: PlaceKey, level: NotificationLevel): void;
  setNotificationDefaultLevel(level: NotificationLevel): void;
  setNotificationKeywords(keywords: string[]): void;
  setNotificationSoundEnabled(enabled: boolean): void;
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
let statusExpiryTimer: ReturnType<typeof setTimeout> | null = null;
type PresenceServerEvent = Extract<
  ServerEvent,
  {
    type:
      | "status_updated"
      | "status_cleared"
      | "reply_later_created"
      | "reply_later_resolved";
  }
>;
let presenceResyncGeneration = 0;
let pendingPresenceResync: {
  generation: number;
  events: PresenceServerEvent[];
} | null = null;

/** Bound long-lived timers and re-evaluate the nearest deadline periodically. */
const STATUS_EXPIRY_MAX_DELAY_MS = 60 * 60_000;

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

// Notification settings are full-snapshot PUTs. Serializing them prevents an
// older, slower response from becoming the server's final state.
let notificationWriteChain: Promise<void> = Promise.resolve();
let notificationWriteGeneration = 0;
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

  const statusSnapshot = (statuses: ParticipantStatus[]) => {
    const statusByKey: Record<ParticipantKey, ParticipantStatus> = {};
    for (const status of statuses) {
      statusByKey[participantKey(status.participant)] = status;
    }
    return withoutExpired(statusByKey, Date.now());
  };

  const applyStatus = (status: ParticipantStatus) => {
    set((state) => ({
      statusByKey: withoutExpired(
        { ...state.statusByKey, [participantKey(status.participant)]: status },
        Date.now(),
      ),
    }));
    scheduleStatusExpiry();
  };

  const clearStatus = (participant: ParticipantRef) => {
    set((state) => {
      const key = participantKey(participant);
      if (!(key in state.statusByKey)) return {};
      const statusByKey = { ...state.statusByKey };
      delete statusByKey[key];
      return { statusByKey };
    });
    scheduleStatusExpiry();
  };

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

  const resyncPresence = async (
    sessionBackend: MessagingBackend,
    sessionIdentity: string | null,
  ) => {
    const resync = {
      generation: ++presenceResyncGeneration,
      events: [] as PresenceServerEvent[],
    };
    pendingPresenceResync = resync;
    try {
      const presence = await sessionBackend.fetchPresence();
      if (
        backend !== sessionBackend ||
        messagingSessionIdentity !== sessionIdentity ||
        pendingPresenceResync !== resync ||
        presenceResyncGeneration !== resync.generation
      ) {
        return;
      }
      const replyLaterById: Record<string, ReplyLaterMarker> = {};
      for (const marker of presence.replyLaterMarkers) {
        replyLaterById[marker.markerId] = marker;
      }
      // Stop buffering before replay, otherwise replay would append to itself.
      // Events were already shown live; replay restores anything an older
      // wholesale snapshot would otherwise overwrite.
      pendingPresenceResync = null;
      set({ statusByKey: statusSnapshot(presence.statuses), replyLaterById });
      scheduleStatusExpiry();
      for (const event of resync.events) applyEvent(event);
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
      body: notificationBody(event.message.content),
      placeKey: key,
      onActivate: () => notificationNavigate?.(key),
    });
  };

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
    if (!thread) return;
    const currentBackend = backend;
    const parentKey = placeKey(thread.parentPlace);
    try {
      const summaries = await currentBackend.fetchThreads(thread.parentPlace);
      if (
        backend !== currentBackend ||
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
        threadProjectionVersions.get(threadId) !== version
      ) {
        return;
      }
      // A later visit may retry the authoritative list instead of treating a
      // failed refresh as a permanently loaded projection.
      set((state) => ({
        threadsLoadedForPlace: {
          ...state.threadsLoadedForPlace,
          [parentKey]: false,
        },
      }));
    }
  };

  const applyThreadDeletionSummary = (message: Message) => {
    if (message.place.kind !== "thread") return;
    const threadId = message.place.threadId;
    const deletionKey = `${threadId}:${message.messageId}`;
    const applyDeletion = !appliedThreadDeletions.has(deletionKey);
    appliedThreadDeletions.add(deletionKey);
    if (applyDeletion) {
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
    if (version !== null) {
      void refreshThreadSummary(message, version);
    }
  };

  /**
   * スレッドに新しい発言が着いたときの一覧側の追従。件数と最新行は一覧を
   * 開き直さなくても正しくあってほしいので、eventから直接更新する。
   */
  const noteThreadActivity = (message: Message) => {
    if (message.place.kind !== "thread") return;
    const threadId = message.place.threadId;
    set((state) => {
      const thread = state.threadsById[threadId];
      if (!thread) return {};
      const known = thread.participants.some(
        (ref) => participantKey(ref) === participantKey(message.author),
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

  const applyEvent = (
    event: Parameters<Parameters<MessagingBackend["subscribe"]>[0]>[0],
  ) => {
    if (
      event.type === "message_created" ||
      event.type === "message_edited" ||
      event.type === "reaction_updated" ||
      event.type === "poll_updated"
    ) {
      const key = placeKey(event.message.place);
      set((state) => {
        const existing = (state.messagesByPlace[key] ?? []).find(
          (message) => message.messageId === event.message.messageId,
        );
        const projected =
          event.type === "poll_updated" && existing
            ? {
                ...existing,
                // A delayed poll frame is a delta for the poll projection. It
                // must not revert a later edit or revive a tombstone.
                poll: existing.deleted ? null : (event.message.poll ?? null),
              }
            : event.message;
        const messages = upsertMessage(
          state.messagesByPlace[key] ?? [],
          projected,
        );
        const nonce = projected.clientNonce;
        const pending = nonce
          ? (state.pendingByPlace[key] ?? []).filter(
              (entry) => entry.clientNonce !== nonce,
            )
          : (state.pendingByPlace[key] ?? []);
        const authorKey = participantKey(projected.author);
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
          projected,
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
      } else if (event.type === "message_edited") {
        const version = advanceThreadProjection(event.message);
        if (version !== null) {
          void refreshThreadSummary(event.message, version);
        }
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
    if (event.type === "profile_updated") {
      set((state) => ({
        membersByKey: {
          ...state.membersByKey,
          [participantKey(event.member.participant)]: event.member,
        },
      }));
      return;
    }
    if (event.type === "status_updated") {
      pendingPresenceResync?.events.push(event);
      applyStatus(event.status);
      return;
    }
    if (event.type === "status_cleared") {
      pendingPresenceResync?.events.push(event);
      clearStatus(event.participant);
      return;
    }
    if (event.type === "reply_later_created") {
      pendingPresenceResync?.events.push(event);
      applyReplyLater(event.marker);
      return;
    }
    if (event.type === "reply_later_resolved") {
      pendingPresenceResync?.events.push(event);
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
      const { channel, dm, thread } = event;
      set((state) => {
        if (thread) {
          return {
            threadsById: {
              ...state.threadsById,
              [thread.threadId]: thread,
            },
          };
        }
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
      return;
    }
    // 通話の在室（ADR 0012）はメッセージングのstateではなくcall storeが持つ。
    if (event.type === "call_state") {
      useCall.getState().applyCallState(event.call);
    }
  };

  let placeReconcileGeneration = 0;

  /**
   * Place lifecycle events are live-only, so a reconnect must compare the
   * durable bootstrap snapshot with the places this client already knows.
   * Existing local timelines, drafts, and read progress remain authoritative
   * for known places; bootstrap counters are adopted only for newly learned
   * places. A generation fence prevents an older REST response from replacing
   * a later reconnect's snapshot.
   */
  const reconcilePlaces = async (
    currentBackend: MessagingBackend,
    currentIdentity: string | null,
    expectedSelfKey: ParticipantKey,
    generation: number,
  ) => {
    const snapshot = await currentBackend.bootstrap();
    if (
      backend !== currentBackend ||
      messagingSessionIdentity !== currentIdentity ||
      get().selfKey !== expectedSelfKey ||
      participantKey(snapshot.self) !== expectedSelfKey ||
      placeReconcileGeneration !== generation
    ) {
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
      ...Object.keys(state.threadsById).map(
        (threadId) => `thread:${threadId}` as PlaceKey,
      ),
    ]);
    const discovered = new Set<PlaceKey>([
      ...snapshot.channels.map((entry) =>
        placeKey({ kind: "channel", channelId: entry.channelId }),
      ),
      ...snapshot.dms.map((entry) =>
        placeKey({ kind: entry.kind, dmId: entry.dmId }),
      ),
      ...snapshot.threads.map(
        (entry) => `thread:${entry.threadId}` as PlaceKey,
      ),
    ]);
    for (const key of known) discovered.delete(key);

    const membersByKey: Record<ParticipantKey, MemberProfile> = {};
    for (const member of snapshot.members) {
      membersByKey[participantKey(member.participant)] = member;
    }
    const threadsById = { ...state.threadsById };
    for (const thread of snapshot.threads) {
      threadsById[thread.threadId] = thread;
    }
    const lastReadByPlace = { ...state.lastReadByPlace };
    for (const marker of snapshot.readMarkers) {
      const key = placeKey(marker.place);
      if (!known.has(key)) lastReadByPlace[key] = marker.lastReadSeq;
    }
    const unreadCountByPlace = { ...state.unreadCountByPlace };
    const mentionCountByPlace = { ...state.mentionCountByPlace };
    const sinceByPlace: Record<PlaceKey, number> = {};
    for (const key of discovered) sinceByPlace[key] = 0;
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
      threadsById,
      membersByKey,
      lastReadByPlace,
      unreadCountByPlace,
      mentionCountByPlace,
    });
    if (Object.keys(sinceByPlace).length > 0) {
      // ApiMessagingBackend keeps listeners in a Set, so this updates cursors
      // without registering a duplicate event delivery path.
      currentBackend.subscribe(applyEvent, { sinceByPlace });
    }
  };

  const PAGE_SIZE = 50;
  const placeLoads = new Map<PlaceKey, Promise<void>>();
  const pollReconciliationVersions = new Map<PlaceKey, number>();

  const reconcileLoadedPolls = async (
    currentBackend: MessagingBackend,
    place: Place,
  ) => {
    const key = placeKey(place);
    const reconciliationVersion =
      (pollReconciliationVersions.get(key) ?? 0) + 1;
    pollReconciliationVersions.set(key, reconciliationVersion);
    const isCurrentReconciliation = () =>
      backend === currentBackend &&
      pollReconciliationVersions.get(key) === reconciliationVersion;
    const inFlightLoad = placeLoads.get(key);
    if (inFlightLoad) await inFlightLoad;
    if (!isCurrentReconciliation()) return;
    const targets = (get().messagesByPlace[key] ?? [])
      .filter((message) => !message.deleted && message.poll)
      .map((message) => ({ messageId: message.messageId, seq: message.seq }));
    if (targets.length === 0) return;

    const snapshots = await Promise.all(
      targets.map(async (target) => {
        const options =
          target.seq === MAX_SEQ
            ? { limit: 1 }
            : { beforeSeq: target.seq + 1, limit: 1 };
        const messages = await currentBackend.fetchMessages(place, options);
        return (
          messages.find(
            (message) =>
              message.messageId === target.messageId &&
              message.seq === target.seq,
          ) ?? null
        );
      }),
    );
    // A session switch owns a different store. Never let the old person's
    // catch-up response cross that boundary. A newer socket generation can
    // also reconcile through the same backend instance, so identity alone is
    // not enough to reject its predecessor's delayed REST response.
    if (!isCurrentReconciliation()) return;
    const discoveredThreadDeletions: Message[] = [];
    set((state) => {
      let messages = state.messagesByPlace[key] ?? [];
      let unreadCount = state.unreadCountByPlace[key] ?? 0;
      let mentionCount = state.mentionCountByPlace[key] ?? 0;
      let changed = false;
      for (const snapshot of snapshots) {
        if (!snapshot) continue;
        const current = messages.find(
          (message) => message.messageId === snapshot.messageId,
        );
        if (!current) continue;
        if (
          !current.deleted &&
          snapshot.deleted &&
          snapshot.place.kind === "thread"
        ) {
          discoveredThreadDeletions.push(snapshot);
        }
        const projected = snapshot.deleted
          ? { ...current, ...snapshot, poll: null }
          : current.deleted
            ? { ...current, poll: null }
            : { ...current, poll: snapshot.poll ?? null };
        const lastRead = state.lastReadByPlace[key] ?? 0;
        const previousContribution = unreadContribution(
          current,
          lastRead,
          state.selfKey,
        );
        const nextContribution = unreadContribution(
          projected,
          lastRead,
          state.selfKey,
        );
        unreadCount = Math.max(
          0,
          unreadCount - previousContribution.unread + nextContribution.unread,
        );
        mentionCount = Math.max(
          0,
          mentionCount -
            previousContribution.mentions +
            nextContribution.mentions,
        );
        messages = upsertMessage(messages, projected);
        changed = true;
      }
      return changed
        ? {
            messagesByPlace: { ...state.messagesByPlace, [key]: messages },
            unreadCountByPlace: {
              ...state.unreadCountByPlace,
              [key]: unreadCount,
            },
            mentionCountByPlace: {
              ...state.mentionCountByPlace,
              [key]: mentionCount,
            },
          }
        : {};
    });
    for (const deletion of discoveredThreadDeletions) {
      applyThreadDeletionSummary(deletion);
    }
  };

  const loadPlace = async (place: Place) => {
    const key = placeKey(place);
    if (get().messagesByPlace[key]) return;
    const existingLoad = placeLoads.get(key);
    if (existingLoad) return existingLoad;
    const currentBackend = backend;
    const load = currentBackend
      .fetchMessages(place, { limit: PAGE_SIZE })
      .then((messages) => {
        if (backend !== currentBackend) return;
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
      });
    placeLoads.set(key, load);
    try {
      await load;
    } finally {
      if (placeLoads.get(key) === load) placeLoads.delete(key);
    }
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
        attachments: pending.attachments.map(
          (attachment) => attachment.attachmentId,
        ),
        poll: pending.poll ?? null,
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

  /**
   * 通知設定は丸ごと置き換える。手元を先に動かして即座に反映し、失敗したら
   * 元に戻す——設定が効いたふりをして黙って効いていないのが一番困る。
   */
  const pushNotificationSetting = (next: {
    defaultLevel: NotificationLevel;
    levelByPlace: Record<PlaceKey, NotificationLevel>;
    keywords: string[];
  }) => {
    const sessionBackend = backend;
    const state = get();
    const previous: NotificationSettingState = {
      notificationDefaultLevel: state.notificationDefaultLevel,
      notificationLevelByPlace: state.notificationLevelByPlace,
      notificationKeywords: state.notificationKeywords,
    };
    set({
      notificationDefaultLevel: next.defaultLevel,
      notificationLevelByPlace: next.levelByPlace,
      notificationKeywords: next.keywords,
    });
    const generation = ++notificationWriteGeneration;
    notificationWriteChain = notificationWriteChain.then(async () => {
      if (
        backend !== sessionBackend ||
        generation !== notificationWriteGeneration
      ) {
        return;
      }
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
        if (backend !== sessionBackend) return;
        confirmedNotificationSetting = confirmed;
        if (generation === notificationWriteGeneration) set(confirmed);
      } catch {
        if (
          backend !== sessionBackend ||
          generation !== notificationWriteGeneration
        ) {
          return;
        }
        set(confirmedNotificationSetting ?? previous);
      }
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
    typingByPlace: {},
    replyLaterById: {},
    notificationDefaultLevel: "all",
    notificationLevelByPlace: {},
    notificationKeywords: [],
    notificationSoundEnabled: isNotificationSoundEnabled(),
    employedAgents: [],
    roles: [],
    roleAssignments: [],
    permissions: {},
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "connected",

    init() {
      if (initialized) return;
      initialized = true;
      threadProjectionVersions.clear();
      appliedThreadDeletions.clear();
      placeLoads.clear();
      pollReconciliationVersions.clear();
      placeReconcileGeneration += 1;
      const currentBackend = backend;
      const currentSessionIdentity = messagingSessionIdentity;
      void currentBackend
        .bootstrap()
        .then((snapshot) => {
          if (
            backend !== currentBackend ||
            messagingSessionIdentity !== currentSessionIdentity
          ) {
            return;
          }
          const membersByKey: Record<ParticipantKey, MemberProfile> = {};
          for (const member of snapshot.members) {
            membersByKey[participantKey(member.participant)] = member;
          }
          const statusByKey = statusSnapshot(snapshot.statuses);
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
          const threadsById: Record<string, ThreadSummary> = {};
          for (const thread of snapshot.threads) {
            threadsById[thread.threadId] = thread;
          }
          confirmedNotificationSetting = notificationSettingState(
            snapshot.notificationSetting,
          );
          set({
            ready: true,
            capabilities: currentBackend.capabilities,
            self: snapshot.self,
            selfKey: participantKey(snapshot.self),
            workspaces: snapshot.workspaces,
            channels: snapshot.channels,
            dms: snapshot.dms,
            threadsById,
            membersByKey,
            statusByKey,
            lastReadByPlace,
            unreadCountByPlace,
            mentionCountByPlace,
            replyLaterById,
            ...confirmedNotificationSetting,
            employedAgents: snapshot.employedAgents,
            roles: snapshot.roles,
            roleAssignments: snapshot.roleAssignments,
            permissions: snapshot.permissions,
          });
          scheduleStatusExpiry();
          (currentBackend as CatchUpAwareMessagingBackend).subscribeCatchUp?.(
            (place) => reconcileLoadedPolls(currentBackend, place),
          );
          currentBackend.subscribe(applyEvent, { sinceByPlace });
          let previousConnection: ConnectionState | null = null;
          let connectedOnce = false;
          currentBackend.subscribeConnection((state) => {
            set({ connection: state });
            if (state !== "connected") {
              // Invalidate a REST snapshot that belongs to the connection we
              // just lost, even before the next socket reaches hello_ack.
              placeReconcileGeneration += 1;
              presenceResyncGeneration += 1;
              pendingPresenceResync = null;
            }
            // call_stateはreplayされない。初回接続と再接続のどちらでも、WSが
            // live配送可能になった時点の全量を読み、取得中のeventはcall storeで
            // snapshotの後へreplayする。
            if (state === "connected" && previousConnection !== "connected") {
              void useCall.getState().hydrate();
              if (connectedOnce) {
                const generation = ++placeReconcileGeneration;
                void reconcilePlaces(
                  currentBackend,
                  currentSessionIdentity,
                  participantKey(snapshot.self),
                  generation,
                ).catch(() => undefined);
                void resyncPresence(
                  currentBackend,
                  currentSessionIdentity,
                ).catch(() => undefined);
              } else {
                connectedOnce = true;
              }
            }
            previousConnection = state;
          });
        })
        .catch(() => {
          if (
            backend !== currentBackend ||
            messagingSessionIdentity !== currentSessionIdentity
          ) {
            return;
          }
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

    async createChannel(name, topic, voice = false) {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) throw new Error("workspace is not ready");
      const channel = await backend.createChannel(
        workspaceId,
        name,
        topic,
        voice,
      );
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
      const dm =
        participants.length === 1
          ? await backend.ensureDM(first)
          : await backend.createGroupDM(participants);
      set((state) =>
        state.dms.some((entry) => entry.dmId === dm.dmId)
          ? {}
          : { dms: [...state.dms, dm] },
      );
      return placeKey({ kind: dm.kind, dmId: dm.dmId });
    },

    async loadThreads(parentKey) {
      const parent = parsePlaceKey(parentKey);
      if (parent?.kind !== "channel") return;
      if (get().threadsLoadedForPlace[parentKey]) return;
      const currentBackend = backend;
      const versionsAtStart = new Map(threadProjectionVersions);
      const threads = await currentBackend.fetchThreads(parent);
      if (backend !== currentBackend) return;
      set((state) => {
        const threadsById = { ...state.threadsById };
        let skippedStaleThread = false;
        for (const thread of threads) {
          if (
            (threadProjectionVersions.get(thread.threadId) ?? 0) !==
            (versionsAtStart.get(thread.threadId) ?? 0)
          ) {
            skippedStaleThread = true;
            continue;
          }
          threadsById[thread.threadId] = thread;
        }
        return {
          threadsById,
          threadsLoadedForPlace: {
            ...state.threadsLoadedForPlace,
            [parentKey]: !skippedStaleThread,
          },
        };
      });
    },

    async createThread(parentKey, name, originMessageId) {
      const parent = parsePlaceKey(parentKey);
      if (!parent) throw new Error("unknown place");
      const thread = await backend.createThread(parent, name, originMessageId);
      set((state) => ({
        threadsById: { ...state.threadsById, [thread.threadId]: thread },
      }));
      return placeKey({ kind: "thread", threadId: thread.threadId });
    },

    async updateChannel(channelId, input) {
      const channel = await backend.updateChannel(channelId, input);
      set((state) => ({
        channels: state.channels.map((entry) =>
          entry.channelId === channel.channelId ? channel : entry,
        ),
      }));
    },

    async duplicateChannel(channelId) {
      const channel = await backend.duplicateChannel(channelId);
      set((state) =>
        state.channels.some((entry) => entry.channelId === channel.channelId)
          ? {}
          : { channels: [...state.channels, channel] },
      );
      return placeKey({ kind: "channel", channelId: channel.channelId });
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

    searchMessages(query) {
      return backend.searchMessages(query, { limit: 20 });
    },

    setDraft(key, draft) {
      set((state) => ({
        draftByPlace: { ...state.draftByPlace, [key]: draft },
      }));
    },

    send(content, urgency, attachments = [], poll = null) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      const trimmed = content.trim();
      // 添付だけの送信は普通のこと。本文も添付も無いときだけ送らない。
      if (!key || !place || !state.self) return;
      // 問いだけの送信も普通のこと。本文も添付も投票も無いときだけ送らない。
      if (!trimmed && attachments.length === 0 && !poll) return;
      const pending: PendingMessage = {
        clientNonce: secureRandomUUID(),
        content: trimmed,
        mentions: resolveMentions(trimmed, state.membersByKey, state.selfKey),
        attachments,
        poll,
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

    uploadAttachment(file) {
      return backend.uploadAttachment(file);
    },

    updateAttachment(attachmentId, patch) {
      return backend.updateAttachment(attachmentId, patch);
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

    setStatus(status, note, expiresAt = null) {
      void backend
        .setStatus(status, note, expiresAt)
        .then(applyStatus, () => undefined);
    },

    async refreshRoles() {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) return;
      const snapshot = await backend.fetchRoles(workspaceId);
      set({
        roles: snapshot.roles,
        roleAssignments: snapshot.roleAssignments,
        permissions: snapshot.permissions,
      });
    },

    async createRole(input) {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) throw new Error("workspace is not ready");
      await backend.createRole(workspaceId, input);
      await get().refreshRoles();
    },

    async updateRole(roleId, input) {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) throw new Error("workspace is not ready");
      await backend.updateRole(workspaceId, roleId, input);
      await get().refreshRoles();
    },

    async deleteRole(roleId) {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) throw new Error("workspace is not ready");
      await backend.deleteRole(workspaceId, roleId);
      await get().refreshRoles();
    },

    async setMemberRoles(participant, roleIds) {
      const workspaceId = get().workspaces[0]?.workspaceId;
      if (!workspaceId) throw new Error("workspace is not ready");
      await backend.setMemberRoles(workspaceId, participant, roleIds);
      await get().refreshRoles();
    },

    async updateProfile(input) {
      const member = await backend.updateProfile(input);
      set((state) => ({
        membersByKey: {
          ...state.membersByKey,
          [participantKey(member.participant)]: member,
        },
      }));
    },

    setPlaceNotificationLevel(key, level) {
      const state = get();
      pushNotificationSetting({
        defaultLevel: state.notificationDefaultLevel,
        levelByPlace: { ...state.notificationLevelByPlace, [key]: level },
        keywords: state.notificationKeywords,
      });
    },

    setNotificationDefaultLevel(level) {
      const state = get();
      pushNotificationSetting({
        defaultLevel: level,
        levelByPlace: state.notificationLevelByPlace,
        keywords: state.notificationKeywords,
      });
    },

    setNotificationKeywords(keywords) {
      const state = get();
      pushNotificationSetting({
        defaultLevel: state.notificationDefaultLevel,
        levelByPlace: state.notificationLevelByPlace,
        keywords,
      });
    },

    // 音は端末の設定。サーバーへは送らないので、他の端末の鳴り方は変わらない。
    setNotificationSoundEnabled(enabled) {
      persistNotificationSound(enabled);
      set({ notificationSoundEnabled: enabled });
    },

    createReplyLater(message, delayMs = DEFAULT_REPLY_LATER_REMIND_MS) {
      void backend
        .createReplyLater(
          message.place,
          message.messageId,
          Date.now() + delayMs,
        )
        .then(applyReplyLater, () => undefined);
    },

    votePoll(message, optionIds) {
      void backend
        .votePoll(message.place, message.messageId, optionIds)
        .catch(() => undefined);
    },

    toggleReaction(message, emoji) {
      void backend
        .toggleReaction(message.place, message.messageId, emoji)
        .catch(() => undefined);
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
      void backend
        .resolveReplyLater(markerId)
        .then(applyReplyLater, () => undefined);
    },

    sendTyping() {
      const key = get().activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (place) backend.sendTyping(place);
    },
  };
});

/**
 * 自分の権限セット。並行して作られたUI（チャンネルメニュー等）へ権限ゲートを
 * 差し込みやすいよう、単純な述語として export する。
 *
 * 正本はサーバーで、ここはその写し。UIで隠すのは導線の整理であって、
 * 強制ではない——実際の拒否はサーバーの 403 が行う。
 */
export function usePermissions(): {
  can: (permission: Permission) => boolean;
  permissions: PermissionSet;
} {
  const permissions = useMessaging((state) => state.permissions);
  return {
    permissions,
    can: (permission: Permission) => permissions[permission] === true,
  };
}

/** フック外（イベントハンドラ等）から同じ判定を使うための口。 */
export function canDo(permission: Permission): boolean {
  return useMessaging.getState().permissions[permission] === true;
}

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
  // 人が入れ替わるなら通話も終わる。前の人の部屋に残らない。
  useCall.getState().reset();
  backend.dispose();
  backend = new ApiMessagingBackend();
  initialized = false;
  notificationWriteChain = Promise.resolve();
  notificationWriteGeneration = 0;
  confirmedNotificationSetting = null;
  presenceResyncGeneration += 1;
  pendingPresenceResync = null;
  if (statusExpiryTimer !== null) clearTimeout(statusExpiryTimer);
  statusExpiryTimer = null;
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
    typingByPlace: {},
    replyLaterById: {},
    notificationDefaultLevel: "all",
    notificationLevelByPlace: {},
    notificationKeywords: [],
    notificationSoundEnabled: isNotificationSoundEnabled(),
    employedAgents: [],
    roles: [],
    roleAssignments: [],
    permissions: {},
    hasMoreByPlace: {},
    loadingOlderByPlace: {},
    activePlaceKey: null,
    editingMessageId: null,
    replyTargetId: null,
    connection: "disconnected",
  });
}
