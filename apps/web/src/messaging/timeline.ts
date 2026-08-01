import type { Message, ParticipantKey, Urgency } from "./model";
import { participantKey } from "./model";

/**
 * タイムライン投影の純関数層。ストアとリスト描画の両方から使い、
 * 単体テストの対象にする。
 */

/** 同一著者の連続投稿をまとめるグルーピング窓。 */
export const GROUPING_WINDOW_MS = 7 * 60_000;

export interface PendingMessage {
  clientNonce: string;
  content: string;
  mentions: Message["mentions"];
  urgency: Urgency;
  replyTo: string | null;
  createdAt: number;
  /** 送信失敗。UIは再送を促し、再送は同じclientNonceで冪等に行う。 */
  failed?: boolean;
}

export type TimelineRow =
  | { id: string; kind: "date"; label: string }
  | { id: string; kind: "unread" }
  | {
      id: string;
      kind: "message";
      message: Message;
      grouped: boolean;
      pending: boolean;
      failed: boolean;
    };

/**
 * seq昇順を保ってメッセージを挿入・置換する。
 * 同じmessageIdは置換（編集反映）、同じseqの別IDは後着を採用しない。
 */
export function upsertMessage(
  messages: readonly Message[],
  incoming: Message,
): Message[] {
  const byId = messages.findIndex(
    (entry) => entry.messageId === incoming.messageId,
  );
  if (byId >= 0) {
    const next = [...messages];
    next[byId] = incoming;
    return next;
  }
  const next = [...messages];
  let index = next.length;
  while (index > 0 && next[index - 1].seq > incoming.seq) index -= 1;
  if (index > 0 && next[index - 1].seq === incoming.seq) return next;
  next.splice(index, 0, incoming);
  return next;
}

export function removeMessage(
  messages: readonly Message[],
  messageId: string,
): Message[] {
  return messages.filter((entry) => entry.messageId !== messageId);
}

/** 他者からの未読件数。自分の発言と削除済みは数えない。 */
export function unreadCount(
  messages: readonly Message[],
  lastReadSeq: number,
  selfKey: ParticipantKey,
): number {
  let count = 0;
  for (const message of messages) {
    if (message.seq <= lastReadSeq) continue;
    if (message.deleted) continue;
    if (participantKey(message.author) === selfKey) continue;
    count += 1;
  }
  return count;
}

/** 未読のうち自分へのmention（急ぎ/普通のみ。FYIはバッジにしない）。 */
export function mentionCount(
  messages: readonly Message[],
  lastReadSeq: number,
  selfKey: ParticipantKey,
): number {
  let count = 0;
  for (const message of messages) {
    if (message.seq <= lastReadSeq) continue;
    if (message.deleted) continue;
    if (message.urgency === "fyi") continue;
    if (message.mentions.some((ref) => participantKey(ref) === selfKey)) {
      count += 1;
    }
  }
  return count;
}

function sameDay(a: number, b: number): boolean {
  const left = new Date(a);
  const right = new Date(b);
  return (
    left.getFullYear() === right.getFullYear() &&
    left.getMonth() === right.getMonth() &&
    left.getDate() === right.getDate()
  );
}

export function dateLabel(timestamp: number, now: number): string {
  if (sameDay(timestamp, now)) return "今日";
  if (sameDay(timestamp, now - 24 * 60 * 60_000)) return "昨日";
  const date = new Date(timestamp);
  return `${date.getMonth() + 1}月${date.getDate()}日`;
}

export interface BuildRowsInput {
  messages: readonly Message[];
  pending: readonly PendingMessage[];
  selfKey: ParticipantKey;
  /**
   * 未読ラインの位置（placeへ入った時点のlastReadSeqのスナップショット）。
   * 読んでいる間は動かさず、placeを離れるまで固定する。nullなら表示しない。
   */
  unreadLineSeq: number | null;
  self: Message["author"];
  now: number;
}

/**
 * 日付区切り・未読ライン・グルーピングを含む描画行を組み立てる。
 * グルーピングは「同一著者・7分以内・返信でない・区切りを挟まない」。
 */
export function buildRows(input: BuildRowsInput): TimelineRow[] {
  const rows: TimelineRow[] = [];
  let previous: Message | null = null;
  let previousBroken = true;

  const pushMessage = (message: Message, pending: boolean, failed = false) => {
    if (message.deleted) return;
    if (previous && !sameDay(previous.createdAt, message.createdAt)) {
      rows.push({
        id: `date:${message.createdAt}`,
        kind: "date",
        label: dateLabel(message.createdAt, input.now),
      });
      previousBroken = true;
    } else if (!previous) {
      rows.push({
        id: `date:${message.createdAt}`,
        kind: "date",
        label: dateLabel(message.createdAt, input.now),
      });
      previousBroken = true;
    }
    if (
      !pending &&
      input.unreadLineSeq !== null &&
      message.seq > input.unreadLineSeq &&
      participantKey(message.author) !== input.selfKey &&
      !rows.some((row) => row.kind === "unread")
    ) {
      rows.push({ id: "unread", kind: "unread" });
      previousBroken = true;
    }
    const grouped =
      !previousBroken &&
      previous !== null &&
      participantKey(previous.author) === participantKey(message.author) &&
      message.createdAt - previous.createdAt < GROUPING_WINDOW_MS &&
      message.replyTo === null;
    rows.push({
      id: pending ? `pending:${message.clientNonce}` : message.messageId,
      kind: "message",
      message,
      grouped,
      pending,
      failed,
    });
    previous = message;
    previousBroken = false;
  };

  for (const message of input.messages) pushMessage(message, false);
  for (const entry of input.pending) {
    pushMessage(
      {
        messageId: `pending:${entry.clientNonce}`,
        place: input.messages[0]?.place ?? {
          kind: "channel",
          channelId: "pending",
        },
        seq: Number.MAX_SAFE_INTEGER,
        author: input.self,
        content: entry.content,
        mentions: entry.mentions,
        urgency: entry.urgency,
        reactions: [],
        replyTo: entry.replyTo,
        createdAt: entry.createdAt,
        editedAt: null,
        deleted: false,
        clientNonce: entry.clientNonce,
      },
      true,
      entry.failed ?? false,
    );
  }
  return rows;
}
