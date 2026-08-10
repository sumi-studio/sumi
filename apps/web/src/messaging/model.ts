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

/** 自己申告のステータス。監視による自動表示はしない。 */
export interface ParticipantStatus {
  participant: ParticipantRef;
  status: StatusKind;
  note: string;
  expiresAt: number | null;
}

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
  remindAt: number;
  resolved: boolean;
}

export interface ReadMarker {
  place: Place;
  lastReadSeq: number;
}

/** 履歴をまだ取得していないplaceにも表示できる、認証済みparticipant向け集計。 */
export interface UnreadSummary {
  place: Place;
  latestSeq: number;
  unreadCount: number;
  mentionCount: number;
}

export type ServerEvent =
  | { type: "message_created"; message: Message }
  | { type: "message_edited"; message: Message }
  | { type: "message_deleted"; message: Message }
  | { type: "typing"; place: Place; participant: ParticipantRef }
  | { type: "status_updated"; status: ParticipantStatus }
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
  | { type: "place_updated"; channel: ChannelSummary };

export interface SendMessageInput {
  place: Place;
  content: string;
  urgency: Urgency;
  replyTo: string | null;
  /** 必須のidempotency key。再送しても二重投稿にならない。 */
  clientNonce: string;
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
    readMarkers: ReadMarker[];
    unreadSummaries: UnreadSummary[];
    replyLaterMarkers: ReplyLaterMarker[];
    /** 自分がEmployerである人格agent。直通（生の直接回線）の対象。 */
    employedAgents: ParticipantRef[];
  }>;
  fetchMessages(
    place: Place,
    options?: { beforeSeq?: number; limit?: number },
  ): Promise<Message[]>;
  createChannel(
    workspaceId: string,
    name: string,
    topic: string,
  ): Promise<ChannelSummary>;
  /** 相手との唯一のDMを返す。既存があればそれを返し、無ければ作る（EnsureDM）。 */
  ensureDM(participant: ParticipantRef): Promise<DmSummary>;
  createGroupDM(participants: ParticipantRef[]): Promise<DmSummary>;
  updateChannelTopic(channelId: string, topic: string): Promise<ChannelSummary>;
  sendMessage(input: SendMessageInput): Promise<SendReceipt>;
  editMessage(place: Place, messageId: string, content: string): Promise<void>;
  deleteMessage(place: Place, messageId: string): Promise<void>;
  markRead(place: Place, lastReadSeq: number): Promise<void>;
  setStatus(status: StatusKind, note: string): Promise<void>;
  createReplyLater(
    place: Place,
    messageId: string,
    remindAt: number,
  ): Promise<void>;
  resolveReplyLater(markerId: string): Promise<void>;
  toggleReaction(
    place: Place,
    messageId: string,
    emoji: string,
    clientNonce: string,
  ): Promise<ReactionMutationResult>;
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
