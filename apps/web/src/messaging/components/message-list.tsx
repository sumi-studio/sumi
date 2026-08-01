import { ArrowDown } from "lucide-react";
import {
  type Ref,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  ConversationVirtualizer,
  type ConversationVirtualizerHandle,
} from "../../components/conversation-virtualizer";
import type { Message } from "../model";
import { useMessaging } from "../store";
import { buildRows, type PendingMessage, type TimelineRow } from "../timeline";
import { MessageItem } from "./message-item";

/** selectorは毎回同じ参照を返す必要がある（新しい[]を作ると無限再レンダー）。 */
const NO_PENDING: PendingMessage[] = [];

export interface MessageListHandle {
  jumpToMessage(messageId: string): void;
  jumpToSeq(seq: number): void;
}

function estimateRowSize(row: TimelineRow): number {
  if (row.kind === "date") return 36;
  if (row.kind === "unread") return 24;
  return row.grouped ? 30 : 62;
}

export function MessageList({
  handleRef,
}: {
  handleRef?: Ref<MessageListHandle>;
}) {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const messages = useMessaging((state) =>
    state.activePlaceKey
      ? (state.messagesByPlace[state.activePlaceKey] ?? null)
      : null,
  );
  const pending = useMessaging((state) =>
    state.activePlaceKey
      ? (state.pendingByPlace[state.activePlaceKey] ?? NO_PENDING)
      : NO_PENDING,
  );
  const unreadLineSeq = useMessaging((state) =>
    state.activePlaceKey
      ? (state.unreadLineByPlace[state.activePlaceKey] ?? null)
      : null,
  );
  const self = useMessaging((state) => state.self);
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const noteReadUpTo = useMessaging((state) => state.noteReadUpTo);
  const setReplyTarget = useMessaging((state) => state.setReplyTarget);
  const startEdit = useMessaging((state) => state.startEdit);
  const deleteMessage = useMessaging((state) => state.deleteMessage);
  const createReplyLater = useMessaging((state) => state.createReplyLater);

  const virtualizerRef = useRef<ConversationVirtualizerHandle>(null);
  const [atEnd, setAtEnd] = useState(true);
  const atEndRef = useRef(true);
  const [highlightedId, setHighlightedId] = useState<string | null>(null);
  const highlightTimer = useRef<number | null>(null);
  const visibleIdsRef = useRef<string[]>([]);
  const positionedPlaceRef = useRef<string | null>(null);

  const rows = useMemo(() => {
    if (!messages || !self) return [];
    return buildRows({
      messages,
      pending,
      selfKey,
      unreadLineSeq,
      self,
      now: Date.now(),
    });
  }, [messages, pending, selfKey, unreadLineSeq, self]);

  const messagesById = useMemo(() => {
    const map = new Map<string, Message>();
    for (const message of messages ?? []) map.set(message.messageId, message);
    return map;
  }, [messages]);

  const seqToId = useMemo(() => {
    const map = new Map<number, string>();
    for (const message of messages ?? [])
      map.set(message.seq, message.messageId);
    return map;
  }, [messages]);

  const flashMessage = useCallback((messageId: string) => {
    virtualizerRef.current?.scrollToMessage(messageId, {
      align: "center",
      behavior: "auto",
    });
    setHighlightedId(messageId);
    if (highlightTimer.current) window.clearTimeout(highlightTimer.current);
    highlightTimer.current = window.setTimeout(
      () => setHighlightedId(null),
      1_800,
    );
  }, []);

  useImperativeHandle(
    handleRef,
    () => ({
      jumpToMessage: flashMessage,
      jumpToSeq: (seq) => {
        const id = seqToId.get(seq);
        if (id) flashMessage(id);
      },
    }),
    [flashMessage, seqToId],
  );

  // placeを開いたとき: 未読ラインがあればそこへ、なければ最下部へ。
  useEffect(() => {
    if (!activePlaceKey || rows.length === 0) return;
    if (positionedPlaceRef.current === activePlaceKey) return;
    positionedPlaceRef.current = activePlaceKey;
    const frame = window.requestAnimationFrame(() => {
      const hasUnread = rows.some((row) => row.kind === "unread");
      if (hasUnread) {
        virtualizerRef.current?.scrollToMessage("unread", {
          align: "center",
          behavior: "auto",
        });
      } else {
        virtualizerRef.current?.scrollToEnd({ behavior: "auto" });
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [activePlaceKey, rows]);

  // 最下部にいるときだけ新着へ自動追従する（読んでいる視点は奪わない）。
  const lastRowId = rows.length > 0 ? rows[rows.length - 1].id : null;
  useEffect(() => {
    if (lastRowId === null) return;
    if (atEndRef.current) {
      virtualizerRef.current?.scrollToEnd({ behavior: "auto" });
    }
  }, [lastRowId]);

  const advanceRead = useCallback(
    (ids: string[]) => {
      visibleIdsRef.current = ids;
      if (!activePlaceKey || !document.hasFocus()) return;
      let maxSeq = 0;
      for (const id of ids) {
        const message = messagesById.get(id);
        if (message && message.seq > maxSeq) maxSeq = message.seq;
      }
      if (maxSeq > 0) noteReadUpTo(activePlaceKey, maxSeq);
    },
    [activePlaceKey, messagesById, noteReadUpTo],
  );

  // ウィンドウにフォーカスが戻った時点で、見えていた範囲を既読にする。
  useEffect(() => {
    const onFocus = () => advanceRead(visibleIdsRef.current);
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [advanceRead]);

  const copyLink = useCallback(
    (message: Message) => {
      if (!activePlaceKey) return;
      const url = `${window.location.origin}/talk?place=${encodeURIComponent(activePlaceKey)}&m=${message.seq}`;
      void navigator.clipboard.writeText(url);
    },
    [activePlaceKey],
  );

  const confirmDelete = useCallback(
    (message: Message) => {
      if (window.confirm("このメッセージを削除しますか？")) {
        deleteMessage(message.messageId);
      }
    },
    [deleteMessage],
  );

  const renderRow = useCallback(
    (row: TimelineRow) => {
      if (row.kind === "date") {
        return (
          <div className="flex items-center gap-3 px-4 pt-4 pb-1 sm:px-6">
            <span className="h-px flex-1 bg-border" />
            <span className="font-medium text-[11px] text-muted-foreground">
              {row.label}
            </span>
            <span className="h-px flex-1 bg-border" />
          </div>
        );
      }
      if (row.kind === "unread") {
        return (
          <div className="flex items-center gap-2 px-4 py-1 sm:px-6">
            <span className="h-px flex-1 bg-rose-500/60" />
            <span className="font-medium text-[10px] text-rose-500">新着</span>
          </div>
        );
      }
      return (
        <div
          className={
            highlightedId === row.message.messageId
              ? "rounded-md bg-primary/8 ring-1 ring-primary/25 transition-colors"
              : undefined
          }
        >
          <MessageItem
            message={row.message}
            grouped={row.grouped}
            pending={row.pending}
            selfKey={selfKey}
            membersByKey={membersByKey}
            findMessage={(id) => messagesById.get(id)}
            onReply={(message) => setReplyTarget(message.messageId)}
            onReplyLater={createReplyLater}
            onCopyLink={copyLink}
            onEdit={(message) => startEdit(message.messageId)}
            onDelete={confirmDelete}
            onJumpTo={flashMessage}
          />
        </div>
      );
    },
    [
      highlightedId,
      selfKey,
      membersByKey,
      messagesById,
      setReplyTarget,
      createReplyLater,
      copyLink,
      startEdit,
      confirmDelete,
      flashMessage,
    ],
  );

  return (
    <div className="relative min-h-0 flex-1">
      <ConversationVirtualizer
        ref={virtualizerRef}
        items={rows}
        renderItem={renderRow}
        estimateSize={estimateRowSize}
        ariaLabel="メッセージ"
        className="scrollbar-ui size-full min-h-0 overscroll-contain"
        contentClassName="pb-4"
        onAtEndChange={(next) => {
          atEndRef.current = next;
          setAtEnd(next);
        }}
        onVisibleMessageIdsChange={advanceRead}
      />
      {atEnd ? null : (
        <button
          type="button"
          onClick={() =>
            virtualizerRef.current?.scrollToEnd({ behavior: "smooth" })
          }
          className="absolute right-4 bottom-3 flex items-center gap-1.5 rounded-full border border-border bg-background px-3 py-1.5 text-muted-foreground text-xs shadow-sm transition-colors hover:text-foreground"
        >
          <ArrowDown className="size-3.5" />
          最新へ
        </button>
      )}
    </div>
  );
}
