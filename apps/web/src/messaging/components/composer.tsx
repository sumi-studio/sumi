import { CornerUpLeft, Pencil, X } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isInsideUnclosedCodeFence } from "../compose-fence";
import type { MemberProfile, Message, Urgency } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { ParticipantAvatar } from "./participant-avatar";

const MAX_HEIGHT_PX = 220;
const TYPING_THROTTLE_MS = 2_000;

/** selectorは毎回同じ参照を返す必要がある（新しい[]を作ると無限再レンダー）。 */
const NO_MESSAGES: Message[] = [];

const URGENCIES: { value: Urgency; label: string; hint: string }[] = [
  { value: "normal", label: "普通", hint: "通常の通知" },
  { value: "urgent", label: "急ぎ", hint: "相手へ強めに通知" },
  { value: "fyi", label: "FYI", hint: "返信不要。手すきで見て" },
];

interface MentionQuery {
  query: string;
  start: number;
  end: number;
}

function findMentionQuery(value: string, caret: number): MentionQuery | null {
  const before = value.slice(0, caret);
  const match = /(^|\s)@([^\s@]*)$/.exec(before);
  if (!match) return null;
  const start = match.index + match[1].length;
  return { query: match[2], start, end: caret };
}

export function Composer() {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const draft = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftByPlace[state.activePlaceKey] ?? "")
      : "",
  );
  const setDraft = useMessaging((state) => state.setDraft);
  const send = useMessaging((state) => state.send);
  const sendTyping = useMessaging((state) => state.sendTyping);
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const messages = useMessaging((state) =>
    state.activePlaceKey
      ? (state.messagesByPlace[state.activePlaceKey] ?? NO_MESSAGES)
      : NO_MESSAGES,
  );
  const editingMessageId = useMessaging((state) => state.editingMessageId);
  const cancelEdit = useMessaging((state) => state.cancelEdit);
  const submitEdit = useMessaging((state) => state.submitEdit);
  const startEdit = useMessaging((state) => state.startEdit);
  const replyTargetId = useMessaging((state) => state.replyTargetId);
  const setReplyTarget = useMessaging((state) => state.setReplyTarget);

  const display = usePlaceDisplay(activePlaceKey);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [urgency, setUrgency] = useState<Urgency>("normal");
  const [editValue, setEditValue] = useState("");
  const [mention, setMention] = useState<MentionQuery | null>(null);
  const [mentionIndex, setMentionIndex] = useState(0);
  const lastTypingAt = useRef(0);

  const editingMessage = editingMessageId
    ? messages.find((entry) => entry.messageId === editingMessageId)
    : undefined;
  const replyTarget = replyTargetId
    ? messages.find((entry) => entry.messageId === replyTargetId)
    : undefined;
  const replyAuthor = replyTarget
    ? membersByKey[participantKey(replyTarget.author)]
    : undefined;

  const editing = editingMessage !== undefined;
  const value = editing ? editValue : draft;

  useEffect(() => {
    if (editingMessage) setEditValue(editingMessage.content);
  }, [editingMessage]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: 編集開始・place切替・返信開始をフォーカスのトリガーにする
  useEffect(() => {
    textareaRef.current?.focus();
  }, [activePlaceKey, editingMessageId, replyTargetId]);

  // autogrow: 内容に合わせて高さを伸ばし、上限でスクロールへ切り替える。
  // biome-ignore lint/correctness/useExhaustiveDependencies: 入力値の変化を高さ再計算のトリガーにする
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "auto";
    const next = Math.min(textarea.scrollHeight, MAX_HEIGHT_PX);
    textarea.style.height = `${next}px`;
    textarea.style.overflowY =
      textarea.scrollHeight > MAX_HEIGHT_PX ? "auto" : "hidden";
  }, [value]);

  const candidates = useMemo(() => {
    if (!mention) return [];
    const query = mention.query.toLowerCase();
    return Object.values(membersByKey)
      .filter((member) => participantKey(member.participant) !== selfKey)
      .filter((member) => member.displayName.toLowerCase().includes(query))
      .slice(0, 6);
  }, [mention, membersByKey, selfKey]);

  const updateValue = useCallback(
    (next: string) => {
      if (editing) {
        setEditValue(next);
      } else if (activePlaceKey) {
        setDraft(activePlaceKey, next);
        const now = Date.now();
        if (next.trim() && now - lastTypingAt.current > TYPING_THROTTLE_MS) {
          lastTypingAt.current = now;
          sendTyping();
        }
      }
      const caret = textareaRef.current?.selectionStart ?? next.length;
      setMention(findMentionQuery(next, caret));
      setMentionIndex(0);
    },
    [editing, activePlaceKey, setDraft, sendTyping],
  );

  const applyMention = useCallback(
    (member: MemberProfile) => {
      if (!mention) return;
      const inserted = `@${member.displayName} `;
      const next =
        value.slice(0, mention.start) + inserted + value.slice(mention.end);
      updateValue(next);
      setMention(null);
      window.requestAnimationFrame(() => {
        const textarea = textareaRef.current;
        if (!textarea) return;
        const caret = mention.start + inserted.length;
        textarea.setSelectionRange(caret, caret);
        textarea.focus();
      });
    },
    [mention, value, updateValue],
  );

  const submit = useCallback(() => {
    if (editing) {
      submitEdit(editValue);
      return;
    }
    if (!value.trim()) return;
    send(value, urgency);
    setUrgency("normal");
    setMention(null);
  }, [editing, editValue, submitEdit, value, send, urgency]);

  const onKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (event.nativeEvent.isComposing) return;
      if (mention && candidates.length > 0) {
        if (event.key === "ArrowDown") {
          event.preventDefault();
          setMentionIndex((index) => (index + 1) % candidates.length);
          return;
        }
        if (event.key === "ArrowUp") {
          event.preventDefault();
          setMentionIndex(
            (index) => (index - 1 + candidates.length) % candidates.length,
          );
          return;
        }
        if (event.key === "Enter" || event.key === "Tab") {
          event.preventDefault();
          applyMention(candidates[mentionIndex]);
          return;
        }
        if (event.key === "Escape") {
          event.preventDefault();
          setMention(null);
          return;
        }
      }
      if (event.key === "Escape") {
        if (editing) {
          event.preventDefault();
          cancelEdit();
        } else if (replyTargetId) {
          event.preventDefault();
          setReplyTarget(null);
        }
        return;
      }
      if (event.key === "Enter" && !event.shiftKey) {
        // 未閉鎖の```コードブロック内では送信せず、デフォルトの改行に任せる。
        const caret = event.currentTarget.selectionStart ?? value.length;
        if (isInsideUnclosedCodeFence(value, caret)) return;
        event.preventDefault();
        submit();
        return;
      }
      // 空欄で↑ = 自分の直前のメッセージを編集（Discordと同じ手癖）。
      if (event.key === "ArrowUp" && !editing && value === "") {
        const own = [...messages]
          .reverse()
          .find((entry) => participantKey(entry.author) === selfKey);
        if (own) {
          event.preventDefault();
          startEdit(own.messageId);
        }
      }
    },
    [
      mention,
      candidates,
      mentionIndex,
      applyMention,
      editing,
      cancelEdit,
      replyTargetId,
      setReplyTarget,
      submit,
      value,
      messages,
      selfKey,
      startEdit,
    ],
  );

  if (!activePlaceKey || !display) return null;
  const placeholder =
    display.kind === "channel"
      ? `#${display.name} へメッセージ`
      : `${display.name} へメッセージ`;

  return (
    <div className="relative shrink-0 px-4 pb-4 sm:px-6">
      {mention && candidates.length > 0 ? (
        <div className="absolute bottom-full left-4 z-10 mb-1 w-64 overflow-hidden rounded-lg border border-border bg-background shadow-md sm:left-6">
          {candidates.map((member, index) => {
            const key = participantKey(member.participant);
            return (
              <button
                key={key}
                type="button"
                onMouseDown={(event) => {
                  event.preventDefault();
                  applyMention(member);
                }}
                className={`flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-[13px] ${
                  index === mentionIndex ? "bg-accent" : ""
                }`}
              >
                <ParticipantAvatar
                  participantKey={key}
                  name={member.displayName}
                  size={20}
                />
                <span className="font-medium">{member.displayName}</span>
                <span className="truncate text-muted-foreground text-xs">
                  {member.tagline}
                </span>
              </button>
            );
          })}
        </div>
      ) : null}
      {editing ? (
        <div className="mb-1 flex items-center gap-2 text-muted-foreground text-xs">
          <Pencil className="size-3" />
          メッセージを編集中
          <span className="text-muted-foreground/70">
            Enterで保存・Escでキャンセル
          </span>
          <button
            type="button"
            onClick={cancelEdit}
            className="ml-auto rounded p-0.5 hover:bg-accent"
            aria-label="編集をキャンセル"
          >
            <X className="size-3.5" />
          </button>
        </div>
      ) : null}
      {!editing && replyTarget && replyAuthor ? (
        <div className="mb-1 flex items-center gap-2 text-muted-foreground text-xs">
          <CornerUpLeft className="size-3" />
          <span className="font-medium text-foreground">
            {replyAuthor.displayName}
          </span>
          <span className="truncate">{replyTarget.content}</span>
          <button
            type="button"
            onClick={() => setReplyTarget(null)}
            className="ml-auto rounded p-0.5 hover:bg-accent"
            aria-label="返信をキャンセル"
          >
            <X className="size-3.5" />
          </button>
        </div>
      ) : null}
      <div className="rounded-xl border border-border bg-background shadow-xs transition-colors focus-within:border-ring/60">
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(event) => updateValue(event.target.value)}
          onKeyDown={onKeyDown}
          onClick={(event) => {
            const caret = event.currentTarget.selectionStart ?? 0;
            setMention(findMentionQuery(value, caret));
          }}
          rows={1}
          placeholder={placeholder}
          aria-label={placeholder}
          className="block w-full resize-none bg-transparent px-3.5 pt-3 pb-1.5 text-[13.5px] leading-6 outline-none placeholder:text-muted-foreground/70"
        />
        <div className="flex items-center gap-1 px-2.5 pb-2">
          {/* 編集中は緊急度セレクタを不可視にするだけで場所は保つ
              （編集開始でツールバー行の高さが変わり入力欄が跳ねないように）。 */}
          <div
            className={`flex items-center rounded-md bg-muted/60 p-0.5 ${
              editing ? "invisible" : ""
            }`}
          >
            {URGENCIES.map((entry) => (
              <button
                key={entry.value}
                type="button"
                title={entry.hint}
                onClick={() => setUrgency(entry.value)}
                className={`rounded px-2 py-0.5 font-medium text-[11px] transition-colors ${
                  urgency === entry.value
                    ? entry.value === "urgent"
                      ? "bg-background text-rose-500 shadow-xs"
                      : "bg-background text-foreground shadow-xs"
                    : "text-muted-foreground hover:text-foreground"
                }`}
              >
                {entry.label}
              </button>
            ))}
          </div>
          <span className="ml-auto text-[11px] text-muted-foreground/60">
            Enterで送信・Shift+Enterで改行
          </span>
        </div>
      </div>
    </div>
  );
}
