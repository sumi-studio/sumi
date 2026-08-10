/**
 * Messaging domain types, mirroring docs/messaging-contracts-draft.md.
 *
 * HumanとPersonalityAgentは同じ「参加者」。author・membership・mention・
 * read marker・通知設定・Status・ReplyLaterのすべてがParticipantRefを使う。
 * authorのkindは将来 "app"（人格agentではない道具・自動装置）へ拡張される
 * sum typeであり、consumerは未知のkindをfail-closedに無視できる必要がある。
 */

// 通話（ADR 0012）はメッセージングの上に乗る別の層なので、型は call/ に置き、
// ここではServerEventの一分岐として参照するだけにする（型のみのimport）。
import type { CallState } from "./call/model";

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

/**
 * 会話の起きる場所。threadは新種の入れ物ではなくplaceの一種——seq・冪等送信・
 * tombstone・既読・通知が既存の仕組みのまま効く（migration 0018）。
 */
export type Place =
  | { kind: "channel"; channelId: string }
  | { kind: "dm"; dmId: string }
  | { kind: "group_dm"; dmId: string }
  | { kind: "thread"; threadId: string };

/**
 * Stable map key for a place:
 * `channel:<id>` / `dm:<id>` / `group_dm:<id>` / `thread:<id>`.
 */
export type PlaceKey = string;

export function placeKey(place: Place): PlaceKey {
  if (place.kind === "channel") return `channel:${place.channelId}`;
  if (place.kind === "thread") return `thread:${place.threadId}`;
  return `${place.kind}:${place.dmId}`;
}

export function parsePlaceKey(key: PlaceKey): Place | null {
  const separator = key.indexOf(":");
  if (separator < 0) return null;
  const kind = key.slice(0, separator);
  const id = key.slice(separator + 1);
  if (!id) return null;
  if (kind === "channel") return { kind, channelId: id };
  if (kind === "thread") return { kind, threadId: id };
  if (kind === "dm" || kind === "group_dm") return { kind, dmId: id };
  return null;
}

/** placeの識別子。kindごとのフィールド名の違いを呼び出し側に漏らさない。 */
export function placeId(place: Place): string {
  if (place.kind === "channel") return place.channelId;
  if (place.kind === "thread") return place.threadId;
  return place.dmId;
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

/**
 * メッセージが運ぶ添付ファイル。実体はwireに載らず、urlから取得する。
 * urlはbackend境界が決める（実APIは同一originの `/messaging/attachments/<id>`、
 * モックはローカルのobject URL）。
 */
export interface Attachment {
  attachmentId: string;
  filename: string;
  mime: string;
  /** バイト数。 */
  size: number;
  url: string;
  /**
   * 送り手が「中身を先に見せない」と宣言した添付。受け手の画面ではぼかして
   * 隠し、開示は受け手の操作に任せる。
   */
  spoiler: boolean;
  /** 中身を見なくても何かが分かる概要（代替テキスト）。無ければ空。 */
  alt: string;
}

/** 送信前の添付に対する編集。省略した項目は「触らない」。 */
export interface AttachmentDraftPatch {
  filename?: string;
  alt?: string;
  spoiler?: boolean;
}

/**
 * インラインプレビューして良い画像MIME。サーバーが `inline` で配信するものと
 * 同じ集合に保つ（それ以外はdownloadとして配信されるためimgでは表示できない）。
 */
const INLINE_IMAGE_MIMES = new Set([
  "image/png",
  "image/jpeg",
  "image/gif",
  "image/webp",
]);

/** MIME単体の判定。送信前の下書き（まだAttachmentでない）でも使う。 */
export function isImageMime(mime: string): boolean {
  return INLINE_IMAGE_MIMES.has(mime.toLowerCase());
}

export function isImageAttachment(attachment: Attachment): boolean {
  return isImageMime(attachment.mime);
}

/** 添付できる1ファイルの上限（20MiB）。サーバーのMaxAttachmentBytesと同値。 */
export const MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024;

/** 1メッセージに添付できる件数の上限。サーバーの上限と同値。 */
export const MAX_ATTACHMENTS_PER_MESSAGE = 10;

/**
 * 投票の選択肢。誰が入れたかはreactionと同じく見える（v0に匿名投票はない）。
 * 票数はvotersの数から導く——別に数を持つと二つの真実ができる。
 */
export interface PollOption {
  optionId: string;
  text: string;
  voters: ParticipantRef[];
}

/**
 * メッセージが運ぶ問い。投票は別の入れ物ではなくメッセージの付属物で、
 * 発言と一緒にcommitされ、発言が消えれば票ごと消える。
 */
export interface MessagePoll {
  question: string;
  /** 複数選択可。falseなら「同一投票に1票」をサーバーが強制する。 */
  allowMulti: boolean;
  /** 締切。nullは締切なし。過ぎたら結果だけが見える。 */
  closesAt: number | null;
  options: PollOption[];
}

/** 送信時に述べる投票。選択肢のidはサーバーが採番するのでtextだけを運ぶ。 */
export interface PollInput {
  question: string;
  allowMulti: boolean;
  closesAt: number | null;
  options: string[];
}

/** 投票の上限。サーバーのMinPollOptions/MaxPollOptionsと同値。 */
export const MIN_POLL_OPTIONS = 2;
export const MAX_POLL_OPTIONS = 10;

/** 締切を過ぎた投票は結果だけ。押せるものが残っていると嘘になる。 */
export function isPollClosed(poll: MessagePoll, now: number): boolean {
  return poll.closesAt !== null && now >= poll.closesAt;
}

export function pollVoteCount(poll: MessagePoll): number {
  return poll.options.reduce(
    (total, option) => total + option.voters.length,
    0,
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
  /** 送信時に紐付いた添付。tombstoneは何も運ばない。 */
  attachments: Attachment[];
  /**
   * 問いを立てているメッセージだけが持つ。省略可にしてあるのは、
   * Messageを組み立てる側（モック・テスト・楽観的描画）に無関係な
   * nullを書かせないため。
   */
  poll?: MessagePoll | null;
  replyTo: string | null;
  createdAt: number;
  editedAt: number | null;
  /** 削除済みはtombstone: contentは空になり、消えた事実とseqだけが残る。 */
  deleted: boolean;
  /** 送信者自身の楽観的描画とACK/echoを照合するidempotency key。 */
  clientNonce?: string;
}

export interface WorkspaceSummary {
  workspaceId: string;
  name: string;
}

export interface ChannelSummary {
  channelId: string;
  workspaceId: string;
  name: string;
  topic: string;
  visibility: "public" | "private";
  /**
   * 「話す場所」として作られたchannel（ADR 0012）。別種のplaceではなく
   * channelの一属性なので、timelineも未読もmentionもそのまま乗る。
   */
  voice: boolean;
}

export interface DmSummary {
  dmId: string;
  kind: "dm" | "group_dm";
  participants: ParticipantRef[];
}

/**
 * チャンネル配下の脇道。閲覧は親チャンネルのメンバー全員、参加者
 * （= 未読と通知の対象）は書いた人と作成者。サイドバーには並べず、
 * 親チャンネルのスレッド一覧と起点メッセージのチップから辿る。
 */
export interface ThreadSummary {
  threadId: string;
  /** 親チャンネル。 */
  parentPlace: Place;
  /** 起点メッセージ。nullは「ゼロから作ったスレッド」。 */
  parentMessageId: string | null;
  name: string;
  messageCount: number;
  lastMessageAt: number | null;
  /** 一覧に出す最新発言の抜粋。全文はplace側にある。 */
  lastMessage: string;
  participants: ParticipantRef[];
  latestSeq: number;
}

/**
 * 表示用のメンバー情報。人間とagentを同じ形で表す。
 * taglineは職務の説明（例: 秘書、開発）であって、bot badgeではない。
 */
export interface MemberProfile {
  participant: ParticipantRef;
  displayName: string;
  tagline: string;
  /** プロフィール画像の添付id。未設定は「画像なし」。 */
  avatarAttachmentId?: string;
  bannerAttachmentId?: string;
  /**
   * 表示用URL。attachmentと同じくbackend境界が決める
   * （実APIは同一originの `/messaging/attachments/<id>`、モックはobject URL）。
   */
  avatarUrl?: string;
  bannerUrl?: string;
}

/**
 * 個人設定からの名乗りの更新。ownerは認証済みsessionが決めるので載せない。
 * 全置換: クライアントは常に現在値を持っているので差分は要らない。
 */
export interface ProfileInput {
  displayName: string;
  tagline: string;
  /** 空文字は「画像を外す」。 */
  avatarAttachmentId: string;
  bannerAttachmentId: string;
}

/**
 * 権限キー。最小の4つだけを持つ。誰も強制しない権限は、製品が守らない約束に
 * なるので増やさない。
 */
export type Permission =
  | "manage_channels"
  | "manage_roles"
  | "manage_members"
  | "mention_all";

export const PERMISSIONS: Permission[] = [
  "manage_channels",
  "manage_roles",
  "manage_members",
  "mention_all",
];

/** 自分が何をして良いか。未知のキーはfail-closedに「不可」として読む。 */
export type PermissionSet = Partial<Record<Permission, boolean>>;

/** ワークスペースの権限の束。人間にもagentにも同じ形で付く。 */
export interface WorkspaceRole {
  roleId: string;
  workspaceId: string;
  name: string;
  /** 空文字は「色を付けない」。 */
  color: string;
  position: number;
  permissions: PermissionSet;
}

/** 誰がどのロールを持つか。 */
export interface RoleAssignment {
  participant: ParticipantRef;
  roleIds: string[];
}

/** ロールの作成・編集の入力。 */
export interface RoleInput {
  name: string;
  color: string;
  permissions: PermissionSet;
}

export type StatusKind = "available" | "busy" | "away";

/**
 * 自己申告のステータス。監視による自動表示はしない。
 * 「オフライン」「非表示」が無いのは隠す手段が足りないからではなく、
 * Sumiが在席を観測しないから——隠すべき自動の表示がそもそも無い。
 */
export interface ParticipantStatus {
  participant: ParticipantRef;
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

/**
 * メッセージ検索の1件。permalink識別子（place + seq）と表示に必要な断片だけを
 * 運ぶ。全文はサーバー側に留まり、ジャンプは既存のplace遷移+seq経路に乗る。
 */
export interface MessageSearchResult {
  messageId: string;
  place: Place;
  seq: number;
  author: ParticipantRef;
  snippet: string;
  createdAt: number;
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
  | { type: "status_cleared"; participant: ParticipantRef }
  /** 本人が名乗りを変えた。member-list・プロフィール・発言者名に即時に効く。 */
  | { type: "profile_updated"; member: MemberProfile }
  | { type: "reply_later_created"; marker: ReplyLaterMarker }
  | { type: "reply_later_resolved"; markerId: string }
  | { type: "reaction_updated"; message: Message }
  /** 票の更新。reaction_updatedと同じくmessage全体を運び、seqは進めない。 */
  | { type: "poll_updated"; message: Message }
  /** placeの誕生。作成者以外のメンバーのサイドバーへ即時に現れる。 */
  | {
      type: "place_created";
      channel?: ChannelSummary;
      dm?: DmSummary;
      thread?: ThreadSummary;
    }
  /** channelのmutable属性（v0: topic）の変更。 */
  | { type: "place_updated"; channel: ChannelSummary }
  /**
   * placeの通話に今いる人（ADR 0012）。typingやstatusと同じくvolatileで
   * replayされない。再接続時の現在値は GET /messaging/calls から読む。
   */
  | { type: "call_state"; call: CallState };

export interface SendMessageInput {
  place: Place;
  content: string;
  urgency: Urgency;
  replyTo: string | null;
  /** 必須のidempotency key。再送しても二重投稿にならない。 */
  clientNonce: string;
  /** 先にアップロード済みの添付id。自分がアップロードしたものだけ紐付く。 */
  attachments: string[];
  /** 投票付きの送信。問いと、それを述べる発言は一つの出来事。 */
  poll?: PollInput | null;
}

/** mutationのACK。serverが採番したidentityを返し、楽観的描画と照合する。 */
export interface SendReceipt {
  messageId: string;
  seq: number;
}

export type ConnectionState = "connected" | "reconnecting" | "disconnected";

export interface MessagingCapabilities {
  status: boolean;
  replyLater: boolean;
  reactions: boolean;
  notifications: boolean;
  threads: boolean;
  polls: boolean;
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
    /** 自分が参加しているスレッド。未読を持つスレッドを親を開かずに出せる。 */
    threads: ThreadSummary[];
    members: MemberProfile[];
    statuses: ParticipantStatus[];
    readMarkers: ReadMarker[];
    unreadSummaries: UnreadSummary[];
    replyLaterMarkers: ReplyLaterMarker[];
    /** 自分の通知設定。muteしたplaceを最初の描画から薄くするために要る。 */
    notificationSetting: NotificationSetting;
    /** いま居るワークスペースのロールと、自分の権限。導線の出し分けに要る。 */
    roles: WorkspaceRole[];
    roleAssignments: RoleAssignment[];
    permissions: PermissionSet;
    /** 自分がEmployerである人格agent。直通（生の直接回線）の対象。 */
    employedAgents: ParticipantRef[];
  }>;
  fetchMessages(
    place: Place,
    options?: { beforeSeq?: number; limit?: number },
  ): Promise<Message[]>;
  /**
   * 可視なplace全体（またはplace指定）での本文検索。可視性はサーバーが強制し、
   * tombstoneは含まれない。
   */
  searchMessages(
    query: string,
    options?: { place?: Place; limit?: number },
  ): Promise<MessageSearchResult[]>;
  /** voiceは「話す場所」として作るかどうか。省略はテキストchannel。 */
  createChannel(
    workspaceId: string,
    name: string,
    topic: string,
    voice?: boolean,
  ): Promise<ChannelSummary>;
  /** 相手との唯一のDMを返す。既存があればそれを返し、無ければ作る（EnsureDM）。 */
  ensureDM(participant: ParticipantRef): Promise<DmSummary>;
  createGroupDM(participants: ParticipantRef[]): Promise<DmSummary>;
  /** 親チャンネル配下のスレッド一覧。可視性はサーバーが強制する。 */
  fetchThreads(parent: Place): Promise<ThreadSummary[]>;
  /**
   * スレッドを開く。originMessageIdを渡すとそのメッセージが起点になり、
   * 1メッセージにつきスレッドは1本だけ生える。
   */
  createThread(
    parent: Place,
    name: string,
    originMessageId: string | null,
  ): Promise<ThreadSummary>;

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
  duplicateChannel(channelId: string, name?: string): Promise<ChannelSummary>;
  sendMessage(input: SendMessageInput): Promise<SendReceipt>;
  /**
   * 送信前にファイルを預ける。返ったAttachmentのidをsendMessageへ渡すまで
   * どのメッセージにも属さず、アップロードした本人にしか見えない。
   */
  uploadAttachment(file: File): Promise<Attachment>;
  /**
   * 送信前の添付を編集する（名前・概要・ネタバレ）。送ってしまった添付は
   * 受け手が見たものが正なので、サーバーが編集を拒む。
   */
  updateAttachment(
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ): Promise<Attachment>;
  editMessage(place: Place, messageId: string, content: string): Promise<void>;
  deleteMessage(place: Place, messageId: string): Promise<void>;
  markRead(place: Place, lastReadSeq: number): Promise<void>;
  /**
   * 自分のステータスだけを置き換える。expiresAtを渡すと一時ステータスになり、
   * 期限で「その前に言っていたこと」へ戻る（サーバーが解決する）。
   */
  setStatus(
    status: StatusKind,
    note: string,
    expiresAt: number | null,
  ): Promise<ParticipantStatus>;
  /**
   * Volatile presence is not recovered by the per-place durable cursor.
   * Re-read the authoritative current values after a connection gap.
   */
  fetchPresence(): Promise<{
    statuses: ParticipantStatus[];
    replyLaterMarkers: ReplyLaterMarker[];
  }>;
  /**
   * 自分の名乗りを丸ごと置き換える。人間はこれを個人設定画面から、agentは
   * 同じ契約をtool経由で使う（AX: UIだけにある操作を作らない）。
   */
  updateProfile(input: ProfileInput): Promise<MemberProfile>;
  /** ロール・付与状況・自分の権限をまとめて読む。閲覧はメンバーなら誰でも。 */
  fetchRoles(workspaceId: string): Promise<{
    roles: WorkspaceRole[];
    roleAssignments: RoleAssignment[];
    permissions: PermissionSet;
  }>;
  createRole(workspaceId: string, input: RoleInput): Promise<WorkspaceRole>;
  updateRole(
    workspaceId: string,
    roleId: string,
    input: RoleInput,
  ): Promise<WorkspaceRole>;
  deleteRole(workspaceId: string, roleId: string): Promise<void>;
  /** メンバーの保持ロールを丸ごと置き換える。空配列は「ロールなし」。 */
  setMemberRoles(
    workspaceId: string,
    participant: ParticipantRef,
    roleIds: string[],
  ): Promise<RoleAssignment>;
  createReplyLater(
    place: Place,
    messageId: string,
    remindAt: number,
  ): Promise<ReplyLaterMarker>;
  resolveReplyLater(markerId: string): Promise<ReplyLaterMarker>;
  toggleReaction(place: Place, messageId: string, emoji: string): Promise<void>;
  /**
   * 投票の回答を丸ごと置き換える。空配列は取り消し——気が変わることと
   * 取り下げることを別の道具にしない。
   */
  votePoll(place: Place, messageId: string, optionIds: string[]): Promise<void>;
  /**
   * 自分の通知設定を丸ごと置き換える。ownerはsessionが決め、bodyに載せない。
   * サーバーが正規化した確定値を返し、クライアントはそれを正本にする。
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
