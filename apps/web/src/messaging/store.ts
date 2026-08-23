import { create } from "zustand";
import { secureRandomUUID } from "../lib/random-uuid";
import { ApiMessagingBackend, MessagingAPIError } from "./api-backend";
import { sanitizeAttachmentFilenameForDisplay } from "./attachment-display";
import { useCall } from "./call/call-store";
import type { DraftAttachment } from "./draft-attachments";
import { attachmentUploadFailureCode } from "./draft-attachments";
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
  Place,
  PlaceKey,
  ReactionSummary,
  ReplyLaterMarker,
  ServerEvent,
  StatusKind,
  Urgency,
  WorkspaceSummary,
} from "./model";
import {
  isInlineImageMime,
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
import type { MessageContentRevision, PendingMessage } from "./timeline";
import { applyMessageRevision, mergeMessages, upsertMessage } from "./timeline";

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
let nextDMStartToken = 0;
let nextEditSessionToken = 0;

/**
 * draft添付のbytesと進行中のupload。zustand stateにはメタデータだけを置き、
 * Fileと AbortController はここで持つ。resetで必ず全部止めて捨てる。
 */
const draftFiles = new Map<
  string,
  { file: File; controller: AbortController | null; previewUrl: string | null }
>();

/**
 * サムネイルのobject URLはbytesと同じ持ち主が管理する。作った側が解放まで
 * 責任を持たないと、composerの再描画やplace切替のたびに取り逃す。
 */
function createPreviewUrl(file: File): string | null {
  if (!isInlineImageMime(file.type)) return null;
  if (typeof URL.createObjectURL !== "function") return null;
  return URL.createObjectURL(file);
}

function rememberDraftFile(
  clientNonce: string,
  file: File,
  previewUrl: string | null,
): void {
  draftFiles.set(clientNonce, { file, controller: null, previewUrl });
}

function releaseDraftFile(clientNonce: string): void {
  const entry = draftFiles.get(clientNonce);
  if (!entry) return;
  entry.controller?.abort();
  if (entry.previewUrl) URL.revokeObjectURL(entry.previewUrl);
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

interface EditSession {
  placeKey: PlaceKey;
  messageId: string;
  revision: number;
  /** 送信中なら、そのPATCHへ切り出した編集欄のスナップショット。 */
  submittedDraft: string | null;
  token: number;
  /**
   * 編集欄を開いた（開き直した）回。startEdit と reloadEditConflict だけが
   * 更新し、保存の送受で派生する session は引き継ぐ。編集欄の初回フォーカスは
   * この単位で一度だけ——仮想リストの行が再マウントされても caret を奪わない。
   */
  openedToken: number;
}

type EditConflict = Required<MessageContentRevision>;

type EditResponseDisposition = "conflict" | "terminal" | "failure";

/**
 * PATCH /messages/{message_id} の失敗応答を、保存を続けられるかで分類する。
 *
 * | 応答 | 処理 |
 * | --- | --- |
 * | 2xx | 送信時のdraftと現在のdraftが同じ時だけ閉じる。違えば新revisionを基準に、追記を残す |
 * | 409 `edit_conflict` | 現在のdraftを残して競合本文を提示 |
 * | 409 `message_deleted` | tombstoneを投影して終了（対象自体がもう編集不能） |
 * | 404 `not_found` | 対象seqを再取得して反映し、編集を終了（対象自体がもう編集不能） |
 * | その他 | 現在のdraftを残して失敗を表示 |
 *
 * API が新しいコードを足しても無言で無視しないよう、既知の終端・競合以外は
 * 必ず failure に落とす。
 */
function editResponseDisposition(error: unknown): EditResponseDisposition {
  if (!(error instanceof MessagingAPIError)) return "failure";
  switch (`${error.status}:${error.code}`) {
    case "409:edit_conflict":
      return "conflict";
    case "409:message_deleted":
    case "404:not_found":
      return "terminal";
    default:
      return "failure";
  }
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
   * 進行中のDM開始。導線ごとにpendingを持つと、メンバーリストとプロフィール
   * カードから2本のstartDMが走り、完了順で意図しないplaceへ飛ぶ。保留は
   * 同時に一つで、その一つをここに置く——入口はここだけを見る。
   */
  startingDM: PendingDMStart | null;
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
  /**
   * 編集セッション。対象IDと書きかけの本文は仮想リストの行の外——ここ——に置く。
   * 行はいつでもアンマウントされうるので、行ローカルのstateに置くと
   * スクロールで書きかけが消える。
   */
  editingMessageId: string | null;
  editDraft: string;
  /** 編集開始時の版。外部更新との衝突判定の基準。 */
  editBaseRevision: number | null;
  /** 非同期保存の完了を発行元だけに閉じ込める編集セッション。 */
  editSession: EditSession | null;
  /** 書きかけを保持したまま保存を止めるための、受信済みの新しい本文。 */
  editConflict: EditConflict | null;
  /** 競合でも対象消滅でもない保存失敗。無言で失敗を捨てない。 */
  editFailure: string | null;
  /** 保存応答後も、その後の追記を編集欄に残していることを示す。 */
  editSavedWithPendingChanges: boolean;
  /**
   * DELETE が失敗したメッセージ。行に「削除できませんでした」を出す。
   * 再試行の要求で外し、tombstone が届けば（誰の削除でも）外す。
   */
  deleteFailedMessageIds: ReadonlySet<string>;
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
  searchMessages(query: string): Promise<MessageSearchResult[]>;
  loadPlaceAround(key: PlaceKey, seq: number): Promise<boolean>;
  setDraft(key: PlaceKey, draft: string): void;
  /** 選択・貼り付け・ドロップされたファイルを現在のplaceのdraftへ積み、uploadを始める。 */
  addDraftAttachments(files: File[]): void;
  removeDraftAttachment(clientNonce: string): void;
  retryDraftAttachment(clientNonce: string): void;
  /**
   * 送信前の添付に付ける宣言（名前・説明・ネタバレ）を変える。反映中は送信
   * ゲートを閉じ、束ねたあとの添付へ編集が落ちるのを防ぐ。
   */
  editDraftAttachment(clientNonce: string, patch: AttachmentDraftPatch): void;
  /** 添付付き送信の可否: 本文か添付があり、uploadが全部終わっているとき。 */
  send(content: string, urgency: Urgency): void;
  retrySend(clientNonce: string): void;
  attachmentURL(attachmentId: string): string;
  startEdit(messageId: string): void;
  setEditDraft(draft: string): void;
  cancelEdit(): void;
  reloadEditConflict(): void;
  /** 編集セッションのドラフトをそのまま送る。引数を取らないのは正本が1つだから。 */
  submitEdit(): void;
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

function isCurrentEditSession(
  state: MessagingState,
  session: EditSession,
): boolean {
  const current = state.editSession;
  return (
    current !== null &&
    current.placeKey === session.placeKey &&
    current.messageId === session.messageId &&
    current.revision === session.revision &&
    current.submittedDraft === session.submittedDraft &&
    current.token === session.token &&
    state.activePlaceKey === session.placeKey &&
    state.editingMessageId === session.messageId &&
    state.editBaseRevision === session.revision
  );
}

function clearedEditSession(): Pick<
  MessagingState,
  | "editingMessageId"
  | "editDraft"
  | "editBaseRevision"
  | "editSession"
  | "editConflict"
  | "editFailure"
  | "editSavedWithPendingChanges"
> {
  return {
    editingMessageId: null,
    editDraft: "",
    editBaseRevision: null,
    editSession: null,
    editConflict: null,
    editFailure: null,
    editSavedWithPendingChanges: false,
  };
}

function setWith(ids: ReadonlySet<string>, id: string): ReadonlySet<string> {
  return ids.has(id) ? ids : new Set(ids).add(id);
}

function setWithout(ids: ReadonlySet<string>, id: string): ReadonlySet<string> {
  if (!ids.has(id)) return ids;
  const next = new Set(ids);
  next.delete(id);
  return next;
}

/**
 * 画面上の本文、競合本文、今回の応答を一つのrevision規則で畳み込む。
 * 呼び出し元は「どの場所に書き戻すか」だけを決め、版の優劣を別実装しない。
 */
function latestMessageContent(
  ...versions: Array<MessageContentRevision | undefined | null>
): MessageContentRevision | undefined {
  let latest: MessageContentRevision | undefined;
  for (const version of versions) {
    if (version) latest = applyMessageRevision(latest, version, "revision");
  }
  return latest;
}

function latestEditConflict(
  ...versions: Array<MessageContentRevision | undefined | null>
): EditConflict | undefined {
  const latest = latestMessageContent(...versions);
  return latest
    ? { content: latest.content, revision: latest.revision ?? 1 }
    : undefined;
}

/**
 * この編集セッションにおける「競合」の唯一の定義。
 *
 * base より新しい revision を運ぶ message event のうち、このセッションが送信した
 * 厳密な次版ではないものだけが競合である。送信済み本文と一致しても +2 以上なら、
 * 間の編集者を Message から識別できないため自分の echo とは扱えない。
 *
 * 入口は live の `message_edited` に限らない。再接続の catch-up は現在版を
 * `message_created` として再生するので、切断中に別の場所で進んだ revision も
 * ここを通す。submit の 409 まで競合を知らずにいる窓を作らない。
 */
function conflictFromMessageEvent(
  state: MessagingState,
  existing: Message | undefined,
  message: Message,
): EditConflict | null {
  const session = state.editSession;
  const revision = message.revision ?? 1;
  if (
    !session ||
    state.editingMessageId !== message.messageId ||
    state.editBaseRevision === null ||
    revision <= state.editBaseRevision ||
    (session.submittedDraft !== null &&
      revision === session.revision + 1 &&
      message.content === session.submittedDraft.trim())
  )
    return state.editConflict;
  return (
    latestEditConflict(existing, state.editConflict, message) ??
    state.editConflict
  );
}

/**
 * サーバーは PATCH の base revision を検査してから revision を進める。成功 ACK が
 * R を返した時点で base..R に他者の編集は無かったことが確定するので、その範囲の
 * 競合は自分の echo か古い観測であり、追記を残す場合にも表示し続けてはならない。
 */
function clearConflictsThroughSuccessfulEditAck(
  conflict: EditConflict | null,
  acknowledgedRevision: number,
): EditConflict | null {
  return conflict && conflict.revision > acknowledgedRevision ? conflict : null;
}

/**
 * 保存中に取り消して同じメッセージを開き直した未送信セッションは、送った版の
 * ACK を知らないまま古い base（≤ R）を持つ。そのまま保存すると自分の確定版に
 * 409 で弾かれ、「別の場所で編集されました」が自分の保存に対して出る。
 *
 * 成功 ACK R は base..R に他者の編集が無かったことの確定なので、その base だけ
 * R へ進め、R 以下の競合表示を畳む。書きかけには触らない——開き直した欄に
 * 見えている本文がそのまま次の保存の対象で、送った版のセッションはもう無い。
 * 送信中（submittedDraft あり）のセッションは自分の ACK で決着するので対象外。
 */
function advanceUnsentEditSessionThroughAck(
  state: MessagingState,
  acknowledged: EditSession,
  acknowledgedRevision: number,
): Partial<MessagingState> {
  const session = state.editSession;
  if (
    !session ||
    session.submittedDraft !== null ||
    session.placeKey !== acknowledged.placeKey ||
    session.messageId !== acknowledged.messageId ||
    state.editingMessageId !== session.messageId ||
    state.editBaseRevision === null ||
    state.editBaseRevision > acknowledgedRevision
  )
    return {};
  return {
    editBaseRevision: acknowledgedRevision,
    editSession: { ...session, revision: acknowledgedRevision },
    editConflict: clearConflictsThroughSuccessfulEditAck(
      state.editConflict,
      acknowledgedRevision,
    ),
  };
}

/**
 * 成功応答と、成功応答を失った直後の同一 PATCH 再試行は、同じ確定遷移を通す。
 * 後者を認められるのは、サーバー正本が送信 base の厳密な次版で、かつ送信本文と
 * 同一のときだけである。それより先の版や別本文は他者編集と区別できない。
 */
function reduceSuccessfulEditAcknowledgement(
  state: MessagingState,
  submittedSession: EditSession,
  committed: Message,
): Partial<MessagingState> {
  const key = submittedSession.placeKey;
  if (
    committed.messageId !== submittedSession.messageId ||
    placeKey(committed.place) !== key
  )
    return {};
  const currentMessage = (state.messagesByPlace[key] ?? []).find(
    (message) => message.messageId === committed.messageId,
  );
  const base = latestMessageContent(currentMessage, committed);
  const acknowledgedRevision = committed.revision ?? submittedSession.revision;
  const messagesByPlace = {
    ...state.messagesByPlace,
    [key]: upsertMessage(
      state.messagesByPlace[key] ?? [],
      committed,
      "revision",
    ),
  };
  if (currentMessage && (currentMessage.revision ?? 1) > acknowledgedRevision) {
    if (!isCurrentEditSession(state, submittedSession)) {
      return {
        messagesByPlace,
        ...advanceUnsentEditSessionThroughAck(
          state,
          submittedSession,
          acknowledgedRevision,
        ),
      };
    }
    return {
      messagesByPlace,
      editBaseRevision: acknowledgedRevision,
      editSession: {
        ...submittedSession,
        revision: acknowledgedRevision,
        submittedDraft: null,
        token: ++nextEditSessionToken,
      },
      editConflict:
        latestEditConflict(state.editConflict, currentMessage) ??
        state.editConflict,
      editFailure: null,
      editSavedWithPendingChanges: false,
    };
  }
  return {
    messagesByPlace,
    ...(isCurrentEditSession(state, submittedSession)
      ? state.editDraft === submittedSession.submittedDraft
        ? clearedEditSession()
        : {
            editBaseRevision: base?.revision ?? submittedSession.revision,
            editSession: {
              ...submittedSession,
              revision: base?.revision ?? submittedSession.revision,
              submittedDraft: null,
              token: ++nextEditSessionToken,
            },
            editConflict: clearConflictsThroughSuccessfulEditAck(
              state.editConflict,
              acknowledgedRevision,
            ),
            editFailure: null,
            editSavedWithPendingChanges: true,
          }
      : advanceUnsentEditSessionThroughAck(
          state,
          submittedSession,
          acknowledgedRevision,
        )),
  };
}

function isLostEditAcknowledgement(
  current: Message,
  submittedSession: EditSession,
): boolean {
  return (
    !current.deleted &&
    current.revision === submittedSession.revision + 1 &&
    submittedSession.submittedDraft !== null &&
    current.content === submittedSession.submittedDraft.trim()
  );
}

/**
 * 進行中のDM開始。participantsはUIが「誰にDMを始めているか」を表示するための値で、
 * tokenは開始を一意に識別して、古い開始のfinallyが新しい保留を解放しないための値。
 */
interface PendingDMStart {
  participants: ParticipantRef[];
  token: number;
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

/**
 * A backend response belongs to the exact transport authority that issued the
 * request.  Do not let a completion from a disposed backend project into the
 * replacement session, even when both sessions happen to contain the same
 * place key.
 */
interface MessagingBackendRequest {
  backend: MessagingBackend;
  isCurrent(): boolean;
  wait<T>(
    operation: (backend: MessagingBackend) => Promise<T>,
  ): Promise<T | undefined>;
}

function beginMessagingBackendRequest(): MessagingBackendRequest {
  const requestBackend = backend;
  const sessionGeneration = messagingSessionGeneration;
  const isCurrent = () =>
    backend === requestBackend &&
    messagingSessionGeneration === sessionGeneration;
  return {
    backend: requestBackend,
    isCurrent,
    async wait<T>(operation: (requestBackend: MessagingBackend) => Promise<T>) {
      const response = await operation(requestBackend);
      return isCurrent() ? response : undefined;
    },
  };
}

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
  request: MessagingBackendRequest;
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
      request: MessagingBackendRequest,
    ) => Promise<() => void>,
  ): Promise<void> => {
    const key = placeKey(place);
    const request = beginMessagingBackendRequest();
    const operationBackend = request.backend;
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
        request,
        sessionGeneration,
        tail: Promise.resolve(),
      };
      reactionProjectionByPlace.set(key, coordinator);
    }
    const target = coordinator;
    target.pending += 1;
    const task = target.tail.then(async () => {
      const isCurrent = () =>
        request.isCurrent() && reactionProjectionByPlace.get(key) === target;
      if (!isCurrent()) return;
      const operation: ReactionProjectionOperation = {
        epoch: ++target.epoch,
        journal: [],
      };
      target.active = operation;
      try {
        const applySnapshot = await produce(
          operationBackend,
          isCurrent,
          target.request,
        );
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
    enqueueReactionProjection(
      place,
      async (_resyncBackend, isCurrent, request) => {
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
            const fresh = await request.wait((backend) =>
              backend.fetchMessages(place, { beforeSeq, limit }),
            );
            if (!fresh) return () => undefined;
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
      },
    );

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
    const request = beginMessagingBackendRequest();
    const resync = {
      generation: ++presenceResyncGeneration,
      // このfetchより前のprojectionはsnapshotに含まれる。先行generationの
      // queueを継ぐと、snapshot内の後続状態を古いprojectionで巻き戻し得る。
      projections: [] as PresenceProjection[],
    };
    pendingPresenceResync = resync;
    try {
      const presence = await request.wait((backend) => backend.fetchPresence());
      if (!presence) return;
      if (
        !request.isCurrent() ||
        pendingPresenceResync !== resync ||
        presenceResyncGeneration !== resync.generation
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

  // REST DELETE応答とlive/replayのmessage_deletedは、同じtombstone投影を通す。
  // これでsocketが不在でも集計を即時に収束させ、後着イベントも単調に扱える。
  const applyMessageDeleted = (message: Message) => {
    const key = placeKey(message.place);
    set((state) => {
      const existing = (state.messagesByPlace[key] ?? []).find(
        (entry) => entry.messageId === message.messageId,
      );
      if (applyMessageRevision(existing, message, "revision") !== message) {
        return {};
      }
      const previous = existing ?? { ...message, deleted: false };
      const contribution = unreadContribution(
        previous,
        state.lastReadByPlace[key] ?? 0,
        state.selfKey,
      );
      const editingDeleted = state.editingMessageId === message.messageId;
      return {
        messagesByPlace: {
          ...state.messagesByPlace,
          [key]: upsertMessage(
            state.messagesByPlace[key] ?? [],
            message,
            "revision",
          ),
        },
        ...(state.deleteFailedMessageIds.has(message.messageId)
          ? {
              deleteFailedMessageIds: setWithout(
                state.deleteFailedMessageIds,
                message.messageId,
              ),
            }
          : {}),
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
        ...(editingDeleted ? clearedEditSession() : {}),
      };
    });
  };

  const closeCurrentEditSession = (session: EditSession) => {
    set((state) =>
      isCurrentEditSession(state, session) ? clearedEditSession() : {},
    );
  };

  // 409 message_deleted は通常 revision付きtombstoneを返す。古いサーバーや
  // 404 not_found は本文を返さないため、編集中だったseqを一点だけ読み直して
  // 同じ tombstone 投影に載せる。終端応答を受けた以上、再取得失敗時も保存欄は
  // 開いたままにしない。
  const reconcileTerminalEditResponse = (
    session: EditSession,
    request: MessagingBackendRequest,
    responseMessage: Message | null,
  ) => {
    if (
      responseMessage?.deleted &&
      responseMessage.messageId === session.messageId &&
      placeKey(responseMessage.place) === session.placeKey
    ) {
      applyMessageDeleted(responseMessage);
      return;
    }
    const place = parsePlaceKey(session.placeKey);
    const cached = (get().messagesByPlace[session.placeKey] ?? []).find(
      (message) => message.messageId === session.messageId,
    );
    if (!place || !cached) {
      closeCurrentEditSession(session);
      return;
    }
    void request
      .wait((backend) =>
        backend.fetchMessages(place, { beforeSeq: cached.seq + 1, limit: 1 }),
      )
      .then(
        (messages) => {
          if (!messages || !request.isCurrent()) return;
          const refreshed = messages.find(
            (message) => message.messageId === session.messageId,
          );
          if (refreshed?.deleted) {
            applyMessageDeleted(refreshed);
            return;
          }
          set((state) => {
            if (!isCurrentEditSession(state, session)) return {};
            return {
              ...(refreshed
                ? {
                    messagesByPlace: {
                      ...state.messagesByPlace,
                      [session.placeKey]: upsertMessage(
                        state.messagesByPlace[session.placeKey] ?? [],
                        refreshed,
                        "snapshot",
                      ),
                    },
                  }
                : {}),
              ...clearedEditSession(),
            };
          });
        },
        () => {
          if (request.isCurrent()) closeCurrentEditSession(session);
        },
      );
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
        if (
          applyMessageRevision(existing, event.message, "revision") !==
          event.message
        ) {
          return {};
        }
        const messages = upsertMessage(
          state.messagesByPlace[key] ?? [],
          event.message,
          "revision",
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
        const editConflict = conflictFromMessageEvent(
          state,
          existing,
          event.message,
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
          editConflict,
        };
      });
      if (event.type === "message_created") presentNotification(event);
      return;
    }
    if (event.type === "message_deleted") {
      applyMessageDeleted(event.message);
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
    const request = beginMessagingBackendRequest();
    const currentIdentity = getMessagingSessionIdentity();
    const expectedSelfKey = get().selfKey;
    const snapshot = await request.wait((backend) => backend.bootstrap());
    if (!snapshot) return;
    if (
      !request.isCurrent() ||
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
      request.backend.subscribe(applyEvent, { sinceByPlace });
    }
  };

  const PAGE_SIZE = 50;

  const loadPlace = async (place: Place) => {
    const key = placeKey(place);
    if (get().messagesByPlace[key]) return;
    const request = beginMessagingBackendRequest();
    const messages = await request.wait((backend) =>
      backend.fetchMessages(place, { limit: PAGE_SIZE }),
    );
    if (!messages || !request.isCurrent()) return;
    set((state) => ({
      messagesByPlace: {
        ...state.messagesByPlace,
        [key]: mergeMessages(
          state.messagesByPlace[key] ?? [],
          messages,
          "snapshot",
        ),
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
    const request = beginMessagingBackendRequest();
    request
      .wait((backend) =>
        backend.sendMessage({
          place,
          content: pending.content,
          urgency: pending.urgency,
          replyTo: pending.replyTo,
          clientNonce: pending.clientNonce,
          attachments: pending.attachments.map((entry) => entry.attachmentId),
        }),
      )
      .then(async (receipt) => {
        if (!receipt || !request.isCurrent()) return;
        let confirmed = (get().messagesByPlace[key] ?? []).some(
          (message) =>
            message.messageId === receipt.messageId ||
            message.clientNonce === pending.clientNonce,
        );
        if (!confirmed) {
          // ACKだけ届き、live echoを取りこぼした再送もreceiptのseqから確定する。
          const messages = await request.wait((backend) =>
            backend.fetchMessages(place, {
              beforeSeq: receipt.seq + 1,
              limit: 1,
            }),
          );
          if (!messages || !request.isCurrent()) return;
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
                  "snapshot",
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
        if (!request.isCurrent()) return;
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
   * 送信前の宣言（名前・説明・ネタバレ）の反映。uploadと同じ関門を通す:
   * backend世代・session世代・そのdraftがまだ積まれていることを確かめてから
   * 反映し、そうでなければ黙って捨てる。戻ってきた受領は別のWorkspaceの
   * 添付かもしれない。
   */
  const dispatchDraftEdit = (
    key: PlaceKey,
    draft: DraftAttachment,
    attachment: Attachment,
    patch: AttachmentDraftPatch,
  ) => {
    const currentBackend = backend;
    const sessionGeneration = messagingSessionGeneration;
    const stillLive = () =>
      backend === currentBackend &&
      messagingSessionGeneration === sessionGeneration &&
      (get().draftAttachmentsByPlace[key] ?? []).some(
        (candidate) => candidate.clientNonce === draft.clientNonce,
      );
    const apply = (next: Partial<DraftAttachment>) =>
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
      .updateDraftAttachment(attachment.attachmentId, patch)
      .then((updated) => {
        if (!stillLive()) return;
        apply({
          status: "ready",
          attachment: updated,
          filename: updated.filename,
          errorCode: undefined,
          editPatch: undefined,
        });
      })
      .catch((error: unknown) => {
        if (!stillLive()) return;
        // bytesは預けたままだが、古い宣言で送れてしまうと保存済みと誤認する。
        // 直前のPATCHと理由を残し、再試行か破棄が済むまで送信を閉じる。
        apply({
          status: "edit_failed",
          errorCode: attachmentUploadFailureCode(error),
        });
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
    const request = beginMessagingBackendRequest();
    const stillLive = () =>
      request.isCurrent() &&
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
    request
      .wait((backend) =>
        backend.uploadAttachment({
          place,
          clientNonce: draft.clientNonce,
          filename: draft.filename,
          contentType: draft.contentType,
          body: entry.file,
          signal: controller.signal,
        }),
      )
      .then((receipt) => {
        if (!receipt || !stillLive()) return;
        patch({
          status: "ready",
          attachment: receipt.attachment,
          filename: receipt.attachment.filename,
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
    const request = beginMessagingBackendRequest();
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
        if (!request.isCurrent()) return "superseded";
        // 送る番が来るまでにもっと新しい設定になっていたら、この一本は要らない。
        if (generation !== notificationWriteGeneration) return "superseded";
        const perPlace: { place: Place; level: NotificationLevel }[] = [];
        for (const [key, level] of Object.entries(next.levelByPlace)) {
          const place = parsePlaceKey(key);
          if (place) perPlace.push({ place, level });
        }
        try {
          const response = await request.wait((backend) =>
            backend.setNotificationSetting({
              defaults: { level: next.defaultLevel },
              perPlace,
              keywords: next.keywords,
            }),
          );
          if (!response || !request.isCurrent()) return "superseded";
          const confirmed = notificationSettingState(response);
          confirmedNotificationSetting = confirmed;
          // 追い越されていれば後続の書き込みが正。確定値は覚えるが手元は触らない。
          if (generation !== notificationWriteGeneration) return "superseded";
          set(confirmed);
          return "confirmed";
        } catch {
          if (
            !request.isCurrent() ||
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
    startingDM: null,
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
    editDraft: "",
    editBaseRevision: null,
    editSession: null,
    editConflict: null,
    editFailure: null,
    editSavedWithPendingChanges: false,
    deleteFailedMessageIds: new Set(),
    replyTargetId: null,
    connection: "disconnected",
    everConnected: false,
    transportGeneration: 0,

    init() {
      if (initialized) return;
      initialized = true;
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) => backend.bootstrap())
        .then((snapshot) => {
          if (!snapshot || !request.isCurrent()) return;
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
            capabilities: request.backend.capabilities,
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
            ...confirmedNotificationSetting,
            employedAgents: snapshot.employedAgents,
          });
          scheduleStatusExpiry();
          request.backend.subscribe(applyEvent, { sinceByPlace });
          // 最初のconnectedはいま読んだこのbootstrapが正本。以降のconnectedは
          // 再接続なので、replayされないplace lifecycleを読み直す。presenceは
          // bootstrap-to-subscribe gapも閉じるため初回を含む毎回で取り直す。
          let connectedOnce = false;
          request.backend.subscribeConnection((connection) => {
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
          if (!request.isCurrent()) return;
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
        ...clearedEditSession(),
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
        ...clearedEditSession(),
        replyTargetId: null,
      });
    },

    async createChannel(workspaceId, name, topic, voice) {
      const request = beginMessagingBackendRequest();
      const currentIdentity = getMessagingSessionIdentity();
      const expectedSelfKey = get().selfKey;
      const channel = await request.wait((backend) =>
        backend.createChannel(workspaceId, name, topic, voice),
      );
      if (
        !channel ||
        !request.isCurrent() ||
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
      if (get().startingDM !== null) {
        throw new Error("A DM start is already pending");
      }
      const request = beginMessagingBackendRequest();
      const currentIdentity = getMessagingSessionIdentity();
      const expectedSelfKey = get().selfKey;
      const token = ++nextDMStartToken;
      set({ startingDM: { participants, token } });
      try {
        const dm =
          participants.length === 1
            ? await request.wait((backend) => backend.ensureDM(first))
            : await request.wait((backend) =>
                backend.createGroupDM(participants),
              );
        if (
          !dm ||
          !request.isCurrent() ||
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
      } finally {
        // session/scopeのreset後に新しい開始が同じparticipants配列を再利用しても、
        // 古いfinallyは自分のtoken以外の保留を解放しない。
        set((state) =>
          state.startingDM?.token === token ? { startingDM: null } : {},
        );
      }
    },

    async updateChannelTopic(channelId, topic) {
      const request = beginMessagingBackendRequest();
      const channel = await request.wait((backend) =>
        backend.updateChannelTopic(channelId, topic),
      );
      if (!channel || !request.isCurrent()) return;
      set((state) => ({
        channels: state.channels.map((entry) =>
          entry.channelId === channel.channelId ? channel : entry,
        ),
      }));
    },

    async searchMessages(query) {
      const request = beginMessagingBackendRequest();
      const messages = await request.wait((backend) =>
        backend.searchMessages(query),
      );
      if (!messages || !request.isCurrent()) {
        throw new Error("Messaging session changed during message search");
      }
      return messages;
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
      const request = beginMessagingBackendRequest();
      const messages = await request.wait((backend) =>
        backend.fetchMessages(place, { beforeSeq: seq + 1, limit: 50 }),
      );
      if (!messages || !request.isCurrent()) return false;
      set((state) => ({
        messagesByPlace: {
          ...state.messagesByPlace,
          [key]: mergeMessages(
            state.messagesByPlace[key] ?? [],
            messages,
            "snapshot",
          ),
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
          filename: sanitizeAttachmentFilenameForDisplay(file.name),
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
        // サムネイルは手元のFileから作る。uploadの完了を待たずに中身が見える
        // ことが、送る前に確かめるという操作の全部なので。
        const previewUrl = createPreviewUrl(file);
        rememberDraftFile(clientNonce, file, previewUrl);
        return previewUrl ? { ...draft, previewUrl } : draft;
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
      if (!draft) return;
      if (
        draft.status === "edit_failed" &&
        draft.attachment &&
        draft.editPatch
      ) {
        const retried: DraftAttachment = {
          ...draft,
          status: "editing",
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
        dispatchDraftEdit(key, retried, draft.attachment, draft.editPatch);
        return;
      }
      if (draft.status !== "failed" || !draftFiles.has(clientNonce)) {
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

    editDraftAttachment(clientNonce, patch) {
      const key = get().activePlaceKey;
      if (!key) return;
      const draft = (get().draftAttachmentsByPlace[key] ?? []).find(
        (entry) => entry.clientNonce === clientNonce,
      );
      const attachment = draft?.attachment;
      // 預かりが済んでいない添付には宣言を付けられない。"editing" を先に置く
      // ことが二重送信の関門でもある（zustandのsetは同期的なので、次の呼び
      // 出しはもう ready ではない）。編集失敗後は、同じ入口から宣言を直して
      // 新しいpatchへ置き換えられる。
      if (
        !draft ||
        !attachment ||
        (draft.status !== "ready" && draft.status !== "edit_failed")
      )
        return;
      set((current) => ({
        draftAttachmentsByPlace: {
          ...current.draftAttachmentsByPlace,
          [key]: (current.draftAttachmentsByPlace[key] ?? []).map((entry) =>
            entry.clientNonce === clientNonce
              ? {
                  ...entry,
                  status: "editing" as const,
                  errorCode: undefined,
                  editPatch: patch,
                }
              : entry,
          ),
        },
      }));
      dispatchDraftEdit(key, draft, attachment, patch);
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
      const state = get();
      const key = state.activePlaceKey;
      const message = key
        ? (state.messagesByPlace[key] ?? []).find(
            (entry) => entry.messageId === messageId,
          )
        : undefined;
      if (!key || !message) return;
      const revision = message.revision ?? 1;
      const token = ++nextEditSessionToken;
      set({
        editingMessageId: messageId,
        editDraft: message.content,
        editBaseRevision: revision,
        editSession: {
          placeKey: key,
          messageId,
          revision,
          submittedDraft: null,
          token,
          openedToken: token,
        },
        editConflict: null,
        editFailure: null,
        editSavedWithPendingChanges: false,
        replyTargetId: null,
      });
    },

    setEditDraft(draft) {
      if (get().editingMessageId === null) return;
      set({ editDraft: draft });
    },

    cancelEdit() {
      set(clearedEditSession());
    },

    reloadEditConflict() {
      const state = get();
      const session = state.editSession;
      if (!state.editConflict || !session) return;
      const current = (state.messagesByPlace[session.placeKey] ?? []).find(
        (message) => message.messageId === session.messageId,
      );
      const base = latestMessageContent(current, state.editConflict);
      if (!base) return;
      const token = ++nextEditSessionToken;
      set({
        editDraft: base.content,
        editBaseRevision: base.revision ?? 1,
        editSession: {
          ...session,
          revision: base.revision ?? 1,
          submittedDraft: null,
          token,
          openedToken: token,
        },
        editConflict: null,
        editFailure: null,
        editSavedWithPendingChanges: false,
      });
    },

    submitEdit() {
      const state = get();
      const session = state.editSession;
      const key = session?.placeKey;
      const place = key ? parsePlaceKey(key) : null;
      const trimmed = state.editDraft.trim();
      if (
        !key ||
        !place ||
        !session ||
        !isCurrentEditSession(state, session) ||
        session.submittedDraft !== null ||
        state.editConflict ||
        !trimmed
      )
        return;
      const submittedSession: EditSession = {
        ...session,
        submittedDraft: state.editDraft,
        token: ++nextEditSessionToken,
      };
      set({
        editSession: submittedSession,
        editFailure: null,
        editSavedWithPendingChanges: false,
      });
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) =>
          backend.editMessage(
            place,
            submittedSession.messageId,
            trimmed,
            submittedSession.revision,
          ),
        )
        .then(
          (committed) => {
            if (!committed || !request.isCurrent()) return;
            set((current) =>
              reduceSuccessfulEditAcknowledgement(
                current,
                submittedSession,
                committed,
              ),
            );
          },
          (error: unknown) => {
            if (!request.isCurrent()) return;
            const disposition = editResponseDisposition(error);
            if (disposition === "terminal") {
              reconcileTerminalEditResponse(
                submittedSession,
                request,
                error instanceof MessagingAPIError
                  ? error.responseMessage
                  : null,
              );
              return;
            }
            if (
              disposition !== "conflict" ||
              !(error instanceof MessagingAPIError) ||
              !error.currentMessage
            ) {
              set((current) =>
                isCurrentEditSession(current, submittedSession)
                  ? {
                      editSession: {
                        ...submittedSession,
                        submittedDraft: null,
                        token: ++nextEditSessionToken,
                      },
                      editFailure:
                        "保存できませんでした。もう一度お試しください。",
                      editSavedWithPendingChanges: false,
                    }
                  : {},
              );
              return;
            }
            const latest = error.currentMessage;
            set((current) => {
              if (
                latest.messageId !== submittedSession.messageId ||
                placeKey(latest.place) !== key
              )
                return {};
              if (isLostEditAcknowledgement(latest, submittedSession)) {
                return reduceSuccessfulEditAcknowledgement(
                  current,
                  submittedSession,
                  latest,
                );
              }
              const messagesByPlace = {
                ...current.messagesByPlace,
                [key]: upsertMessage(
                  current.messagesByPlace[key] ?? [],
                  latest,
                  "revision",
                ),
              };
              if (!isCurrentEditSession(current, submittedSession)) {
                return { messagesByPlace };
              }
              const currentMessage = (current.messagesByPlace[key] ?? []).find(
                (message) => message.messageId === latest.messageId,
              );
              const conflict = latestEditConflict(
                currentMessage,
                current.editConflict,
                latest,
              );
              if (!conflict) return { messagesByPlace };
              return {
                messagesByPlace,
                editConflict: conflict,
                editFailure: null,
                editSavedWithPendingChanges: false,
              };
            });
          },
        );
    },

    deleteMessage(messageId) {
      const state = get();
      const key = state.activePlaceKey;
      const place = key ? parsePlaceKey(key) : null;
      if (!key || !place) return;
      if (state.deleteFailedMessageIds.has(messageId)) {
        set((current) => ({
          deleteFailedMessageIds: setWithout(
            current.deleteFailedMessageIds,
            messageId,
          ),
        }));
      }
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) => backend.deleteMessage(place, messageId))
        .then(
          (committed) => {
            if (!committed || !request.isCurrent()) return;
            if (
              committed.messageId !== messageId ||
              placeKey(committed.place) !== key
            )
              return;
            applyMessageDeleted(committed);
          },
          // 403/404/ネットワーク断。無言で捨てると行は残ったまま何も起きない。
          // 送信失敗と同じく、その行に失敗を出して再試行の口を残す。
          () => {
            if (!request.isCurrent()) return;
            set((current) => ({
              deleteFailedMessageIds: setWith(
                current.deleteFailedMessageIds,
                messageId,
              ),
            }));
          },
        );
    },

    setReplyTarget(messageId) {
      set({
        replyTargetId: messageId,
        ...clearedEditSession(),
      });
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
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) => backend.markRead(place, seq))
        .catch(() => undefined);
    },

    // 成功ACKはserverが確定した値そのものなので、socketが再接続中でも
    // これだけで表示とリマインドは収束する。後着のechoは同じ形を上書きする。
    setStatus(status, note) {
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) => backend.setStatus(status, note))
        .then(
          (canonical) => {
            if (!canonical || !request.isCurrent()) return;
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
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) =>
          backend.createReplyLater(
            message.place,
            message.messageId,
            Date.now() + delayMs,
          ),
        )
        .then(
          (canonical) => {
            if (!canonical || !request.isCurrent()) return;
            applyPresenceProjection({ type: "reply_later", marker: canonical });
          },
          () => undefined,
        );
    },

    toggleReaction(message, emoji) {
      const clientNonce = secureRandomUUID();
      void enqueueReactionProjection(
        message.place,
        async (_operationBackend, isCurrent, request) => {
          const canonical = await request.wait((backend) =>
            backend.toggleReaction(
              message.place,
              message.messageId,
              emoji,
              clientNonce,
            ),
          );
          if (!canonical || !request.isCurrent()) return () => undefined;
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
      const request = beginMessagingBackendRequest();
      const older = await request.wait((backend) =>
        backend.fetchMessages(place, {
          beforeSeq: current[0].seq,
          limit: PAGE_SIZE,
        }),
      );
      if (!older || !request.isCurrent()) return;
      set((entry) => {
        const existing = entry.messagesByPlace[key] ?? [];
        return {
          messagesByPlace: {
            ...entry.messagesByPlace,
            [key]: mergeMessages(existing, older, "snapshot"),
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
      const request = beginMessagingBackendRequest();
      void request
        .wait((backend) => backend.resolveReplyLater(markerId))
        .then(
          (canonical) => {
            if (!canonical || !request.isCurrent()) return;
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

/**
 * 編集セッションは、現在開いているタイムラインに対象が表示可能な間だけ有効。
 *
 * メッセージの取り込み経路は live event、ページング、再同期、scope reset など複数ある。
 * それぞれで編集状態を片付けるのではなく、編集セッションが依存する三つの値が変わる
 * たびにここで不変条件を検査する。tombstone は配列に残るがタイムラインには出ないため、
 * deleted も対象不在として扱う。
 */
function hasEditingTarget(state: MessagingState): boolean {
  const key = state.activePlaceKey;
  const messageId = state.editingMessageId;
  return Boolean(
    key &&
      messageId &&
      state.messagesByPlace[key]?.some(
        (message) => message.messageId === messageId && !message.deleted,
      ),
  );
}

useMessaging.subscribe((state, previous) => {
  if (
    state.editingMessageId === previous.editingMessageId &&
    state.activePlaceKey === previous.activePlaceKey &&
    state.messagesByPlace === previous.messagesByPlace
  ) {
    return;
  }
  if (state.editingMessageId !== null && !hasEditingTarget(state)) {
    useMessaging.setState(clearedEditSession());
  }
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

  const request = beginMessagingBackendRequest();
  const currentIdentity = messagingSessionIdentity;
  const expectedSelfKey = state.selfKey;
  const snapshot = await request.wait((backend) => backend.bootstrap());
  if (!snapshot) {
    throw new Error("Messaging session changed during profile refresh");
  }
  if (
    !request.isCurrent() ||
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
    startingDM: null,
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
    editDraft: "",
    editBaseRevision: null,
    editSession: null,
    editConflict: null,
    editFailure: null,
    editSavedWithPendingChanges: false,
    deleteFailedMessageIds: new Set(),
    replyTargetId: null,
    connection: "disconnected",
    everConnected: false,
    transportGeneration: messagingSessionGeneration,
  });
}
