import type { CallState } from "./call/model";

/**
 * Messaging domain types, mirroring docs/messaging-contracts-draft.md.
 *
 * HumanとPersonalityAgentは同じ「参加者」。author・membership・mention・
 * read marker・通知設定・Status・ReplyLaterのすべてがParticipantRefを使う。
 * authorのkindは将来 "app"（人格agentではない道具・自動装置）へ拡張される
 * sum typeであり、consumerは未知のkindをfail-closedに無視できる必要がある。
 */

export type ParticipantRef =
  | { kind: "human"; humanId: string }
  // "agent"ではなく"personality_agent": worker/subagent/appとの混同を防ぐ（Codex合意）。
  | { kind: "personality_agent"; personalityAgentId: string };

/** Stable map key for a participant: `human:<id>` / `personality_agent:<id>`. */
export type ParticipantKey = string;

export function participantKey(ref: ParticipantRef): ParticipantKey {
  return ref.kind === "human"
    ? `human:${ref.humanId}`
    : `personality_agent:${ref.personalityAgentId}`;
}

export function sameParticipant(a: ParticipantRef, b: ParticipantRef): boolean {
  return participantKey(a) === participantKey(b);
}

export type Place =
  | { kind: "channel"; channelId: string }
  | { kind: "dm"; dmId: string }
  | { kind: "group_dm"; dmId: string };

/** Stable map key for a place: `channel:<id>` / `dm:<id>` / `group_dm:<id>`. */
export type PlaceKey = string;

export function placeKey(place: Place): PlaceKey {
  return place.kind === "channel"
    ? `channel:${place.channelId}`
    : `${place.kind}:${place.dmId}`;
}

export function parsePlaceKey(key: PlaceKey): Place | null {
  const separator = key.indexOf(":");
  if (separator < 0) return null;
  const kind = key.slice(0, separator);
  const id = key.slice(separator + 1);
  if (!id) return null;
  if (kind === "channel") return { kind, channelId: id };
  if (kind === "dm" || kind === "group_dm") return { kind, dmId: id };
  return null;
}

/**
 * メッセージ単位の緊急度。attentionがコストである世界の誠実なUI。
 * agentには覚醒トリガの優先度、人間には未読トリアージとして働く。
 */
export type Urgency = "urgent" | "normal" | "fyi";

/** seqはJSONで安全に運べる整数に収める（wire契約はJsonSafeInteger）。 */
export const MAX_SEQ = Number.MAX_SAFE_INTEGER;

/** 絵文字リアクションの集計。参加者は解決済みParticipantRef。 */
export interface ReactionSummary {
  emoji: string;
  participants: ParticipantRef[];
}

/** Canonical absolute reaction state returned by a successful mutation. */
export interface ReactionMutationResult {
  messageId: string;
  reactions: ReactionSummary[];
}

/** 1ファイルの上限（20 MiB）と1メッセージあたりの添付数。サーバーと同じ値。 */
export const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;
export const MAX_ATTACHMENTS_PER_MESSAGE = 10;

/**
 * メッセージが運ぶファイル。bytesは含まず、`MessagingBackend.attachmentURL`から
 * 現在のexact scopeで再認可された上で取りに行く。mimeはサーバーがバイト先頭を
 * sniffして決めた値で、inline表示できる画像型はサーバー側の許可リストに従う。
 */
export interface Attachment {
  attachmentId: string;
  filename: string;
  mime: string;
  sizeBytes: number;
  sha256: string;
  position: number;
  /**
   * 送り手が「開くまで中身を見せない」と宣言した添付。受け手の画面では覆って
   * おき、開示は受け手の操作に任せる。メッセージ本文ではなく添付の性質。
   */
  spoiler: boolean;
  /** 中身を見なくても何のファイルか分かる説明。無ければ空。 */
  alt: string;
}

/** 添付の説明の上限。サーバーのMaxAttachmentAltRunesと同値。 */
export const MAX_ATTACHMENT_ALT_LENGTH = 1000;

/** 送信前の添付への編集。省略した項目は「触らない」。 */
export interface AttachmentDraftPatch {
  filename?: string;
  alt?: string;
  spoiler?: boolean;
}

export function isInlineImageMime(mime: string): boolean {
  return (
    mime === "image/png" ||
    mime === "image/jpeg" ||
    mime === "image/gif" ||
    mime === "image/webp"
  );
}

export interface Message {
  messageId: string;
  place: Place;
  /** Place単位の単調増加seq。未読・replay・permalinkの基準。上限はMAX_SEQ。 */
  seq: number;
  author: ParticipantRef;
  content: string;
  /** Admission時に解決済みのmention先。raw文字列一致は判定に使わない。 */
  mentions: ParticipantRef[];
  urgency: Urgency;
  reactions: ReactionSummary[];
  /** 送信者が選んだ順序。tombstoneでは空。 */
  attachments: Attachment[];
  replyTo: string | null;
  createdAt: number;
  editedAt: number | null;
  /** 編集の compare-and-swap 用の単調増加版。 */
  revision?: number;
  /** 削除済みはtombstone: contentは空になり、消えた事実とseqだけが残る。 */
  deleted: boolean;
  /** 送信者自身の楽観的描画とACK/echoを照合するidempotency key。 */
  clientNonce?: string;
}

/** A bounded search projection; full message content is fetched only on jump. */
export interface MessageSearchResult {
  messageId: string;
  place: Place;
  seq: number;
  author: ParticipantRef;
  snippet: string;
  createdAt: number;
}

export interface WorkspaceSummary {
  workspaceId: string;
  name: string;
}

export interface ChannelSummary {
  channelId: string;
  workspaceId: string;
  /** Monotonic database projection revision for volatile lifecycle frames. */
  revision: number;
  name: string;
  topic: string;
  visibility: "public" | "private";
  /** Voice channels retain the same text timeline and unread semantics. */
  voice: boolean;
}

export interface DmSummary {
  dmId: string;
  kind: "dm" | "group_dm";
  participants: ParticipantRef[];
}

/**
 * 表示用のメンバー情報。人間とagentを同じ形で表す。
 * taglineは職務の説明（例: 秘書、開発）であって、bot badgeではない。
 */
export interface MemberProfile {
  participant: ParticipantRef;
  displayName: string;
  tagline: string;
}

export type StatusKind = "available" | "busy" | "away";

/**
 * 自己申告のステータス。監視による自動表示はしない。
 * 「オフライン」「非表示」が無いのは隠す手段が足りないからではなく、
 * Sumiが在席を観測しないから——隠すべき自動の表示がそもそも無い。
 */
export interface ParticipantStatus {
  participant: ParticipantRef;
  /** Monotonic database revision of this participant's status projection. */
  revision: number;
  status: StatusKind;
  note: string;
  expiresAt: number | null;
  /**
   * expiresAtが来たときに戻る先。nullなら戻る先が無く、期限で宣言そのものが
   * 終わる。期限なしのステータスでは常にnull。
   */
  baseStatus: StatusKind | null;
  baseNote: string;
}

/** A durable empty status projection. It still advances the participant's revision. */
export interface StatusCleared {
  participant: ParticipantRef;
  revision: number;
}

/** 一時ステータスの期間プリセット。nullは「解除するまで」。 */
export interface StatusDuration {
  label: string;
  minutes: number | null;
}

export const STATUS_DURATIONS: StatusDuration[] = [
  { label: "15分", minutes: 15 },
  { label: "1時間", minutes: 60 },
  { label: "8時間", minutes: 8 * 60 },
  { label: "24時間", minutes: 24 * 60 },
  { label: "3日間", minutes: 3 * 24 * 60 },
  { label: "解除するまで", minutes: null },
];

/**
 * 「後で返信します」の応答予約。相手には返信予定が見え、
 * 本人にはシステムがリマインドして返信忘れを防ぐ。
 */
export interface ReplyLaterMarker {
  markerId: string;
  participant: ParticipantRef;
  place: Place;
  messageId: string;
  note: string;
  /**
   * 本人だけのリマインド予定。相手のwireには載らないのでnullになる
   * （合意事項6: durableなmarkerとprivate reminderに二分する）。
   */
  remindAt: number | null;
  resolved: boolean;
}

export interface ReadMarker {
  place: Place;
  lastReadSeq: number;
}

/**
 * 通知の強さ。受信側が place ごとに決める。
 * mute は「件数は数えるが呼ばない」— 未読が消えるわけではない。
 */
export type NotificationLevel = "all" | "mentions" | "mute";

/**
 * なぜ今呼ばれたのか。サーバーが送信時に評価して、呼んだ相手のwireにだけ載せる。
 * 優先度は dm > mention > keyword > all で、mute はすべてを抑制する。
 */
export type NotifyReason = "dm" | "mention" | "keyword" | "all";

/**
 * 本人が所有し、本人だけが変更する通知設定（human/agent同型）。
 * agentにとっては覚醒トリガの発火条件でもあるので、UI専用の概念にしない。
 */
export interface NotificationSetting {
  owner: ParticipantRef;
  defaults: { level: NotificationLevel };
  perPlace: { place: Place; level: NotificationLevel }[];
  keywords: string[];
}

/** 設定の更新入力。ownerは認証済みsessionが決めるので載せない。 */
export interface NotificationSettingInput {
  defaults: { level: NotificationLevel };
  perPlace: { place: Place; level: NotificationLevel }[];
  keywords: string[];
}

/** 履歴をまだ取得していないplaceにも表示できる、認証済みparticipant向け集計。 */
export interface UnreadSummary {
  place: Place;
  latestSeq: number;
  unreadCount: number;
  mentionCount: number;
}

export type ServerEvent =
  /**
   * notifyは受信者ごとに異なる。nullは欠損ではなく「あなたを呼んではいない」
   * というサーバーの答えで、mute/mentionsの判定はここで既に済んでいる。
   */
  | {
      type: "message_created";
      message: Message;
      notify: { reason: NotifyReason } | null;
    }
  | { type: "message_edited"; message: Message }
  | { type: "message_deleted"; message: Message }
  | { type: "typing"; place: Place; participant: ParticipantRef }
  | { type: "status_updated"; status: ParticipantStatus }
  /**
   * 一時ステータスが戻る先を持たずに期限切れになった。「対応可能になった」
   * ではなく「何も言っていない状態に戻った」——サーバーが代わりに何かを
   * 名乗ることはしない。
   */
  | ({ type: "status_cleared" } & StatusCleared)
  | { type: "reply_later_created"; marker: ReplyLaterMarker }
  | { type: "reply_later_resolved"; markerId: string }
  /**
   * reactionだけの部分更新。message全体を運ばないのは、同時に走った編集より
   * 遅れて届いたreaction eventがcontentを巻き戻さないようにするため。
   */
  | {
      type: "reaction_updated";
      place: Place;
      messageId: string;
      reactions: ReactionSummary[];
    }
  /**
   * placeのcatch-up完了。cursorより手前のmessageに付いたreactionはreplayされ
   * ないので、受け手はロード済み範囲を読み直して収束させる。
   */
  | { type: "caught_up"; place: Place }
  /** placeの誕生。作成者以外のメンバーのサイドバーへ即時に現れる。 */
  | { type: "place_created"; channel?: ChannelSummary; dm?: DmSummary }
  /** channelのmutable属性（v0: topic）の変更。 */
  | { type: "place_updated"; channel: ChannelSummary }
  /** Volatile presence: reconnect repairs it through GET /messaging/calls. */
  | { type: "call_state"; call: CallState };

export interface SendMessageInput {
  place: Place;
  content: string;
  urgency: Urgency;
  replyTo: string | null;
  /** 必須のidempotency key。再送しても二重投稿にならない。 */
  clientNonce: string;
  /** upload済みattachmentのIDを送信者の順序で。contentが空でも1件あれば送れる。 */
  attachments: string[];
}

export interface UploadAttachmentInput {
  place: Place;
  /** ファイルごとに安定なnonce。再送は同じ受領を返す。 */
  clientNonce: string;
  filename: string;
  /** ブラウザのMIME候補。サーバーはバイトを見て決め直す。 */
  contentType: string;
  body: Blob;
  signal?: AbortSignal;
}

export interface UploadAttachmentReceipt {
  attachment: Attachment;
  created: boolean;
}

/** mutationのACK。serverが採番したidentityを返し、楽観的描画と照合する。 */
export interface SendReceipt {
  clientNonce: string;
  messageId: string;
  seq: number;
  created: boolean;
}

export type ConnectionState = "connected" | "reconnecting" | "disconnected";

export interface MessagingCapabilities {
  status: boolean;
  replyLater: boolean;
  reactions: boolean;
  notifications: boolean;
}

/**
 * メッセージングbackendの境界。モックと実API（REST: /messaging/…、
 * WS: /messaging/ws 1本で全place multiplex）が同じ形を実装する。
 * ここに載る操作はすべて「人間もagentも使える道具」で、agent側は同じ契約を
 * tool経由で使う（AX）。UIだけにある操作を作らない。
 */
export interface MessagingBackend {
  readonly capabilities: MessagingCapabilities;
  bootstrap(): Promise<{
    self: ParticipantRef;
    workspaces: WorkspaceSummary[];
    channels: ChannelSummary[];
    dms: DmSummary[];
    members: MemberProfile[];
    statuses: ParticipantStatus[];
    /** Empty status wires, split from statuses so UI state remains status-only. */
    clearedStatuses?: StatusCleared[];
    readMarkers: ReadMarker[];
    unreadSummaries: UnreadSummary[];
    replyLaterMarkers: ReplyLaterMarker[];
    /** 自分の通知設定。muteしたplaceを最初の描画から薄くするために要る。 */
    notificationSetting: NotificationSetting;
    /** 自分がEmployerである人格agent。直通（生の直接回線）の対象。 */
    employedAgents: ParticipantRef[];
  }>;
  fetchMessages(
    place: Place,
    options?: { beforeSeq?: number; limit?: number },
  ): Promise<Message[]>;
  searchMessages(
    query: string,
    options?: { place?: Place; limit?: number },
  ): Promise<MessageSearchResult[]>;
  createChannel(
    workspaceId: string,
    name: string,
    topic: string,
    voice: boolean,
    clientNonce: string,
  ): Promise<ChannelSummary>;
  /** 相手との唯一のDMを返す。既存があればそれを返し、無ければ作る（EnsureDM）。 */
  ensureDM(participant: ParticipantRef): Promise<DmSummary>;
  createGroupDM(
    participants: ParticipantRef[],
    clientNonce: string,
  ): Promise<DmSummary>;
  /**
   * channelのmutableな身元（名前・トピック）を書き換える。省いた項目は
   * そのまま残る——名前を変えただけでトピックが消えては困る。
   */
  updateChannel(
    channelId: string,
    input: { name?: string; topic?: string },
  ): Promise<ChannelSummary>;
  /**
   * 同じ形（名前・トピック）の空のchannelを新しく作る。中身は複製しない:
   * メッセージ・既読・通知設定は元のchannelのもの。nameを省くとサーバーが
   * 既定の名前（「〜 のコピー」）を決める。
   */
  duplicateChannel(
    channelId: string,
    clientNonce: string,
    name?: string,
  ): Promise<ChannelSummary>;
  sendMessage(input: SendMessageInput): Promise<SendReceipt>;
  /** メッセージより先にbytesを預ける。受領したIDをsendMessageのattachmentsへ。 */
  uploadAttachment(
    input: UploadAttachmentInput,
  ): Promise<UploadAttachmentReceipt>;
  /**
   * 送信前の添付を編集する（名前・説明・ネタバレ）。送ってしまった添付は
   * 受け手が見たものが正なので、サーバーが編集を拒む。
   */
  updateDraftAttachment(
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ): Promise<Attachment>;
  /**
   * 現在のexact scopeで再認可されるbytes取得URL。<img src>と<a download>が
   * そのまま使う。scopeが変われば別のURLになる。
   */
  attachmentURL(attachmentId: string): string;
  editMessage(
    place: Place,
    messageId: string,
    content: string,
    expectedRevision: number,
  ): Promise<Message>;
  deleteMessage(place: Place, messageId: string): Promise<Message>;
  markRead(place: Place, lastReadSeq: number): Promise<void>;
  /**
   * 自己申告のattentionの現在値。status_updatedはvolatileでreplayされず、
   * reply-laterのeventはplaceのseq catch-upにも載らない。切断中の変化は
   * cursorからは戻らないので、再接続のたびにここで取り直して置き換える。
   */
  fetchPresence(): Promise<{
    statuses: ParticipantStatus[];
    /** Empty status wires, split from statuses so UI state remains status-only. */
    clearedStatuses?: StatusCleared[];
    replyLaterMarkers: ReplyLaterMarker[];
  }>;
  /**
   * mutationはserverが確定した値を返す。socketが再接続中でも成功ACKだけで
   * 収束できるよう、呼び出し側はこの戻り値を状態に反映する。
   */
  /**
   * 自分のステータスだけを置き換える。expiresAtを渡すと一時ステータスになり、
   * 期限で「その前に言っていたこと」へ戻る（サーバーが解決する）。
   */
  setStatus(
    status: StatusKind,
    note: string,
    expiresAt: number | null,
  ): Promise<ParticipantStatus>;
  createReplyLater(
    place: Place,
    messageId: string,
    remindAt: number,
  ): Promise<ReplyLaterMarker>;
  resolveReplyLater(markerId: string): Promise<ReplyLaterMarker>;
  toggleReaction(
    place: Place,
    messageId: string,
    emoji: string,
    clientNonce: string,
  ): Promise<ReactionMutationResult>;
  /**
   * 自分の通知設定を丸ごと置き換える。ownerはsessionが決め、bodyに載せない。
   * 返すのはサーバーが正規化した確定値。手元がそれと食い違ったまま残らないよう、
   * 呼び出し側はこれを正本として取り込む。
   */
  setNotificationSetting(
    input: NotificationSettingInput,
  ): Promise<NotificationSetting>;
  /** best-effort。失敗しても会話は壊れないため受領確認しない。 */
  sendTyping(place: Place): void;
  /**
   * durable eventの購読。再接続時はplaceごとの消費済みseqをcursorとして渡し、
   * その次からcatch-upする（volatile eventはreplayしない）。
   */
  subscribe(
    listener: (event: ServerEvent) => void,
    options?: { sinceByPlace?: Record<PlaceKey, number> },
  ): () => void;
  subscribeConnection(listener: (state: ConnectionState) => void): () => void;
  dispose(): void;
}
