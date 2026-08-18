import {
  BarChart3,
  CornerUpLeft,
  FileText,
  Loader2,
  Paperclip,
  Pencil,
  RotateCw,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { isInsideUnclosedCodeFence } from "../compose-fence";
import type { DraftAttachment } from "../draft-attachments";
import {
  attachmentFailureLabel,
  formatAttachmentSize,
} from "../draft-attachments";
import type { MemberProfile, Message, Urgency } from "../model";
import { MAX_ATTACHMENTS_PER_MESSAGE, participantKey } from "../model";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { useWheelPassthrough } from "./overlay";
import { ParticipantAvatar } from "./participant-avatar";
import { PollCreateDialog } from "./poll-create-dialog";

const MAX_HEIGHT_PX = 220;
const TYPING_THROTTLE_MS = 2_000;

/** selectorは毎回同じ参照を返す必要がある（新しい[]を作ると無限再レンダー）。 */
const NO_MESSAGES: Message[] = [];
const NO_DRAFT_ATTACHMENTS: DraftAttachment[] = [];

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
  const pollsEnabled = useMessaging((state) => state.capabilities.polls);
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
  const draftAttachments = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftAttachmentsByPlace[state.activePlaceKey] ??
        NO_DRAFT_ATTACHMENTS)
      : NO_DRAFT_ATTACHMENTS,
  );
  const attachmentOverflow = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftAttachmentOverflowByPlace[state.activePlaceKey] ?? 0)
      : 0,
  );
  const addDraftAttachments = useMessaging(
    (state) => state.addDraftAttachments,
  );
  const removeDraftAttachment = useMessaging(
    (state) => state.removeDraftAttachment,
  );
  const retryDraftAttachment = useMessaging(
    (state) => state.retryDraftAttachment,
  );
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);
  const [pollOpen, setPollOpen] = useState(false);

  const display = usePlaceDisplay(activePlaceKey);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [urgency, setUrgency] = useState<Urgency>("normal");
  const [editValue, setEditValue] = useState("");
  const [mention, setMention] = useState<MentionQuery | null>(null);
  const [mentionIndex, setMentionIndex] = useState(0);
  const lastTypingAt = useRef(0);
  const mentionPassthroughRef = useWheelPassthrough<HTMLDivElement>();

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

  const attachmentsSettled = draftAttachments.every(
    (entry) => entry.status === "ready",
  );
  const readyAttachmentCount = draftAttachments.filter(
    (entry) => entry.status === "ready",
  ).length;
  const canSend =
    !editing &&
    attachmentsSettled &&
    (value.trim().length > 0 || readyAttachmentCount > 0);

  const submit = useCallback(() => {
    if (editing) {
      submitEdit(editValue);
      return;
    }
    if (!canSend) return;
    send(value, urgency);
    setUrgency("normal");
    setMention(null);
  }, [editing, editValue, submitEdit, value, send, urgency, canSend]);

  const acceptFiles = useCallback(
    (list: FileList | File[] | null | undefined) => {
      if (editing || !list) return;
      const files = Array.from(list).filter((file) => file.size >= 0);
      if (files.length > 0) addDraftAttachments(files);
    },
    [editing, addDraftAttachments],
  );

  const onPaste = useCallback(
    (event: React.ClipboardEvent<HTMLTextAreaElement>) => {
      const files = Array.from(event.clipboardData?.files ?? []);
      if (files.length === 0) return;
      // 画像の貼り付けはファイルとして積む。テキストは通常どおり入力に流す。
      event.preventDefault();
      acceptFiles(files);
    },
    [acceptFiles],
  );

  const onDrop = useCallback(
    (event: React.DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      setDragging(false);
      acceptFiles(event.dataTransfer?.files);
    },
    [acceptFiles],
  );

  const onKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (isImeComposing(event)) return;
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

  const attachmentsFull =
    draftAttachments.length >= MAX_ATTACHMENTS_PER_MESSAGE;

  return (
    <section
      aria-label="メッセージ入力"
      className="relative shrink-0 px-4 pb-4 sm:px-6"
      onDragOver={(event) => {
        if (editing) return;
        event.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={onDrop}
    >
      {mention && candidates.length > 0 ? (
        <div
          ref={mentionPassthroughRef}
          className="absolute bottom-full left-4 z-10 mb-1 w-64 overflow-hidden rounded-lg border border-border bg-background shadow-md sm:left-6"
        >
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
      <div
        className={`rounded-xl border bg-background shadow-xs transition-colors focus-within:border-ring/60 ${
          dragging ? "border-ring/80 bg-accent/30" : "border-border"
        }`}
      >
        {!editing && draftAttachments.length > 0 ? (
          <div
            className="flex flex-wrap gap-1.5 px-3 pt-2.5"
            data-testid="composer-attachments"
          >
            {draftAttachments.map((entry) => (
              <DraftAttachmentChip
                key={entry.clientNonce}
                draft={entry}
                onRemove={() => removeDraftAttachment(entry.clientNonce)}
                onRetry={() => retryDraftAttachment(entry.clientNonce)}
              />
            ))}
          </div>
        ) : null}
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(event) => updateValue(event.target.value)}
          onKeyDown={onKeyDown}
          onPaste={onPaste}
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
          {!editing ? (
            <>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                className="hidden"
                data-testid="composer-file-input"
                onChange={(event) => {
                  acceptFiles(event.currentTarget.files);
                  event.currentTarget.value = "";
                }}
              />
              <button
                type="button"
                title={
                  attachmentsFull
                    ? `添付は1件のメッセージにつき${MAX_ATTACHMENTS_PER_MESSAGE}件まで`
                    : "ファイルを添付（貼り付け・ドロップも可）"
                }
                aria-label="ファイルを添付"
                disabled={attachmentsFull}
                onClick={() => fileInputRef.current?.click()}
                className="rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground disabled:opacity-40"
              >
                <Paperclip className="size-4" />
              </button>
              {pollsEnabled ? (
                <button
                  type="button"
                  title="投票を作成"
                  aria-label="投票を作成"
                  onClick={() => setPollOpen(true)}
                  className="rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
                >
                  <BarChart3 className="size-4" />
                </button>
              ) : null}
            </>
          ) : null}
          <span className="ml-auto text-[11px] text-muted-foreground/60">
            {!editing && attachmentOverflow > 0
              ? `上限のため${attachmentOverflow}件のファイルを追加できませんでした`
              : !editing && draftAttachments.length > 0 && !attachmentsSettled
                ? "添付の準備ができると送信できます"
                : "Enterで送信・Shift+Enterで改行"}
          </span>
        </div>
      </div>
      {pollOpen ? (
        <PollCreateDialog onClose={() => setPollOpen(false)} />
      ) : null}
    </section>
  );
}

function DraftAttachmentChip({
  draft,
  onRemove,
  onRetry,
}: {
  draft: DraftAttachment;
  onRemove: () => void;
  onRetry: () => void;
}) {
  const failed = draft.status === "failed";
  return (
    <div
      className={`flex max-w-full items-center gap-1.5 rounded-md border px-2 py-1 text-[12px] ${
        failed
          ? "border-rose-500/40 bg-rose-500/5 text-rose-600"
          : "border-border bg-muted/50"
      }`}
      data-status={draft.status}
      title={failed ? attachmentFailureLabel(draft.errorCode) : draft.filename}
    >
      {draft.status === "uploading" ? (
        <Loader2 className="size-3.5 shrink-0 animate-spin text-muted-foreground" />
      ) : (
        <FileText className="size-3.5 shrink-0 text-muted-foreground" />
      )}
      <span className="max-w-40 truncate font-medium">{draft.filename}</span>
      <span className="shrink-0 text-muted-foreground">
        {formatAttachmentSize(draft.sizeBytes)}
      </span>
      {failed ? (
        <>
          <span className="shrink-0">
            {attachmentFailureLabel(draft.errorCode)}
          </span>
          {draft.errorCode !== "attachment_too_large" &&
          draft.errorCode !== "attachment_empty" ? (
            <button
              type="button"
              onClick={onRetry}
              className="rounded p-0.5 hover:bg-rose-500/10"
              aria-label={`${draft.filename}を再送`}
            >
              <RotateCw className="size-3" />
            </button>
          ) : null}
        </>
      ) : null}
      <button
        type="button"
        onClick={onRemove}
        className="rounded p-0.5 hover:bg-accent"
        aria-label={`${draft.filename}を外す`}
      >
        <X className="size-3" />
      </button>
    </div>
  );
}
