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
  | { kind: "agent"; personalityAgentId: string };

/** Stable map key for a participant: `human:<id>` / `agent:<id>`. */
export type ParticipantKey = string;

export function participantKey(ref: ParticipantRef): ParticipantKey {
  return ref.kind === "human"
    ? `human:${ref.humanId}`
    : `agent:${ref.personalityAgentId}`;
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

export interface Message {
  messageId: string;
  place: Place;
  /** Place単位の単調増加seq。未読・replay・permalinkの基準。 */
  seq: number;
  author: ParticipantRef;
  content: string;
  /** Admission時に解決済みのmention先。raw文字列一致は判定に使わない。 */
  mentions: ParticipantRef[];
  urgency: Urgency;
  replyTo: string | null;
  createdAt: number;
  editedAt: number | null;
  deleted: boolean;
  /** この端末が送ったメッセージの楽観的描画との照合用。 */
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

export type ServerEvent =
  | { type: "message_created"; message: Message }
  | { type: "message_edited"; message: Message }
  | { type: "message_deleted"; place: Place; messageId: string; seq: number }
  | { type: "typing"; place: Place; participant: ParticipantRef }
  | { type: "status_updated"; status: ParticipantStatus }
  | { type: "reply_later_created"; marker: ReplyLaterMarker }
  | { type: "reply_later_resolved"; markerId: string };

export interface SendMessageInput {
  place: Place;
  content: string;
  mentions: ParticipantRef[];
  urgency: Urgency;
  replyTo: string | null;
  clientNonce: string;
}

/**
 * メッセージングbackendの境界。モックと実API（WS+REST）が同じ形を実装する。
 * ここに載る操作はすべて「人間もagentも使える道具」で、agent側は同じ契約を
 * tool経由で使う（AX）。
 */
export interface MessagingBackend {
  bootstrap(): Promise<{
    self: ParticipantRef;
    workspaces: WorkspaceSummary[];
    channels: ChannelSummary[];
    dms: DmSummary[];
    members: MemberProfile[];
    statuses: ParticipantStatus[];
    readMarkers: ReadMarker[];
    replyLaterMarkers: ReplyLaterMarker[];
  }>;
  fetchMessages(
    place: Place,
    options?: { beforeSeq?: number; limit?: number },
  ): Promise<Message[]>;
  sendMessage(input: SendMessageInput): void;
  editMessage(place: Place, messageId: string, content: string): void;
  deleteMessage(place: Place, messageId: string): void;
  markRead(place: Place, lastReadSeq: number): void;
  setStatus(status: StatusKind, note: string): void;
  createReplyLater(place: Place, messageId: string, remindAt: number): void;
  resolveReplyLater(markerId: string): void;
  sendTyping(place: Place): void;
  subscribe(listener: (event: ServerEvent) => void): () => void;
}
