import {
  AtSign,
  CornerUpLeft,
  Paperclip,
  SendHorizontal,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { isInsideUnclosedCodeFence } from "../compose-fence";
import type { DraftAttachment } from "../draft-attachments";
import type { Message, Urgency } from "../model";
import { MAX_ATTACHMENTS_PER_MESSAGE, participantKey } from "../model";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { ComposerAttachments } from "./composer-attachments";
import type { ComposerPlusMenuItem } from "./composer-plus-menu";
import { ComposerPlusMenu } from "./composer-plus-menu";
import {
  MentionSuggestions,
  useMentionAutocomplete,
} from "./mention-autocomplete";
import { useImeCommittedTextarea } from "./use-ime-committed-textarea";

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
  // 編集はメッセージ本体の位置で行う（message-item のインライン編集）。
  // composerは編集を預からず、↑キーの入り口だけを持つ。
  const editingMessageId = useMessaging((state) => state.editingMessageId);
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
  const editDraftAttachment = useMessaging(
    (state) => state.editDraftAttachment,
  );
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const display = usePlaceDisplay(activePlaceKey);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [urgency, setUrgency] = useState<Urgency>("normal");
  const lastTypingAt = useRef(0);
  const ime = useImeCommittedTextarea(textareaRef);

  const replyTarget = replyTargetId
    ? messages.find((entry) => entry.messageId === replyTargetId)
    : undefined;
  const replyAuthor = replyTarget
    ? membersByKey[participantKey(replyTarget.author)]
    : undefined;

  const value = draft;

  // biome-ignore lint/correctness/useExhaustiveDependencies: place切替・返信開始・編集終了をフォーカスのトリガーにする
  useEffect(() => {
    // インライン編集中はキャレットが編集欄にある。奪い返さない。
    if (editingMessageId) return;
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

  const updateValue = useCallback(
    (next: string) => {
      if (activePlaceKey) {
        setDraft(activePlaceKey, next);
        const now = Date.now();
        if (next.trim() && now - lastTypingAt.current > TYPING_THROTTLE_MS) {
          lastTypingAt.current = now;
          sendTyping();
        }
      }
    },
    [activePlaceKey, setDraft, sendTyping],
  );
  const mentionAutocomplete = useMentionAutocomplete({
    value,
    onValueChange: updateValue,
    inputRef: textareaRef,
    membersByKey,
    selfKey,
  });

  const attachmentsSettled = draftAttachments.every(
    (entry) => entry.status === "ready",
  );
  const readyAttachmentCount = draftAttachments.filter(
    (entry) => entry.status === "ready",
  ).length;
  const attachmentsFull =
    draftAttachments.length >= MAX_ATTACHMENTS_PER_MESSAGE;
  const canSubmitText = useCallback(
    (text: string) =>
      attachmentsSettled &&
      (text.trim().length > 0 || readyAttachmentCount > 0),
    [attachmentsSettled, readyAttachmentCount],
  );
  const canSend = canSubmitText(value);

  const submit = useCallback(() => {
    const text = ime.committedValue(value);
    if (!canSubmitText(text)) return;
    send(text, urgency);
    setUrgency("normal");
    mentionAutocomplete.dismiss();
    textareaRef.current?.focus();
  }, [value, send, urgency, canSubmitText, mentionAutocomplete, ime]);

  const acceptFiles = useCallback(
    (list: FileList | File[] | null | undefined) => {
      if (!list) return;
      const files = Array.from(list).filter((file) => file.size >= 0);
      if (files.length > 0) addDraftAttachments(files);
    },
    [addDraftAttachments],
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

  const plusItems = useMemo<ComposerPlusMenuItem[]>(
    () => [
      {
        id: "attach",
        label: "ファイルを添付",
        hint: attachmentsFull
          ? `1通につき${MAX_ATTACHMENTS_PER_MESSAGE}件まで`
          : "貼り付け・ドロップも可",
        icon: Paperclip,
        disabled: attachmentsFull,
        onSelect: () => fileInputRef.current?.click(),
      },
      {
        id: "mention",
        label: "メンション",
        hint: "@ で相手を呼ぶ",
        icon: AtSign,
        onSelect: mentionAutocomplete.insertTrigger,
      },
    ],
    [attachmentsFull, mentionAutocomplete.insertTrigger],
  );

  const onKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (isImeComposing(event)) return;
      if (mentionAutocomplete.onKeyDown(event)) return;
      if (event.key === "Escape") {
        if (replyTargetId) {
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
      // 空欄で↑ = 自分の直前のメッセージをその場で編集し始める。
      if (event.key === "ArrowUp" && !editingMessageId && value === "") {
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
      mentionAutocomplete,
      editingMessageId,
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

  const blockedNotice =
    attachmentOverflow > 0
      ? `上限のため${attachmentOverflow}件のファイルを追加できませんでした`
      : draftAttachments.some((entry) => entry.status === "edit_failed")
        ? "添付の保存に失敗しました。鉛筆から内容を直すか、再送してください"
        : draftAttachments.length > 0 && !attachmentsSettled
          ? "添付の準備ができると送信できます"
          : null;

  return (
    <section
      aria-label="メッセージ入力"
      className="relative shrink-0 px-4 pb-4 sm:px-6"
      onDragOver={(event) => {
        event.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={onDrop}
    >
      <MentionSuggestions
        autocomplete={mentionAutocomplete}
        className="absolute bottom-full left-4 z-10 mb-1 w-64 overflow-hidden rounded-lg border border-border bg-background shadow-md sm:left-6"
      />
      {replyTarget && replyAuthor ? (
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
        <ComposerAttachments
          drafts={draftAttachments}
          onRemove={removeDraftAttachment}
          onRetry={retryDraftAttachment}
          onEdit={editDraftAttachment}
        />
        <textarea
          ref={textareaRef}
          value={value}
          onChange={mentionAutocomplete.onInputChange}
          onKeyDown={onKeyDown}
          onKeyUp={mentionAutocomplete.onKeyUp}
          onCompositionStart={ime.onCompositionStart}
          onCompositionEnd={(event) => {
            ime.onCompositionEnd();
            mentionAutocomplete.onCompositionEnd(event);
          }}
          onPaste={onPaste}
          onClick={mentionAutocomplete.onInputClick}
          onSelect={mentionAutocomplete.onSelectionChange}
          rows={1}
          placeholder={placeholder}
          aria-label={placeholder}
          className="block w-full resize-none bg-transparent px-3.5 pt-3 pb-1.5 text-[13.5px] leading-6 outline-none placeholder:text-muted-foreground/70"
        />
        <div className="flex items-center gap-1 px-2.5 pb-2">
          <div className="flex items-center rounded-md bg-muted/60 p-0.5">
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
          <ComposerPlusMenu items={plusItems} finalFocusRef={textareaRef} />
          <div className="ml-auto flex min-w-0 items-center gap-2">
            {blockedNotice ? (
              <span className="truncate text-[11px] text-muted-foreground/60">
                {blockedNotice}
              </span>
            ) : (
              <span className="hidden truncate text-[11px] text-muted-foreground/60 sm:inline">
                Enterで送信・Shift+Enterで改行
              </span>
            )}
            <button
              type="button"
              onClick={submit}
              disabled={!canSend}
              title="送信（Enter）"
              aria-label="送信"
              className="flex size-7 shrink-0 items-center justify-center rounded-md bg-primary text-primary-foreground transition-opacity enabled:hover:opacity-90 disabled:bg-muted disabled:text-muted-foreground/60"
            >
              <SendHorizontal className="size-3.5" />
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
